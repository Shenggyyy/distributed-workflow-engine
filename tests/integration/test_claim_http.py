"""Claim HTTP transactions, live replay, status mapping and commit-before-success."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
from threading import Barrier, Event
from uuid import UUID, uuid4

import pytest
from alembic import command
from fastapi.testclient import TestClient
from sqlalchemy import Connection, Engine, event, func, select, text

from tests.integration.migration_helpers import migration_config
from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.domain.lease import LeaseClockRegressionError
from workflow_engine.repositories.claim_requests import (
    _CLAIM_LOCK_NAMESPACE,
    ClaimRequestRepository,
    _claim_lock_key,
)
from workflow_engine.repositories.claims import AttemptNumberExhaustedError, TaskClaim
from workflow_engine.repositories.leases import LeaseRepository
from workflow_engine.repositories.workers import WorkerClockRegressionError
from workflow_engine.schema import (
    attempt_leases,
    claim_requests,
    task_attempts,
    task_runs,
    worker_sessions,
    workflow_runs,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def http_engine(engine: Engine, migration_schema: str) -> Engine:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return engine.execution_options(schema_translate_map={None: migration_schema})


@pytest.fixture
def client(http_engine: Engine) -> Iterator[TestClient]:
    with TestClient(
        create_app(
            Settings(
                environment="test",
                attempt_lease_seconds=600,
                worker_heartbeat_timeout_seconds=300,
            ),
            engine=http_engine,
        )
    ) as value:
        yield value


def setup(client: TestClient, *, capacity: int = 2) -> tuple[str, dict[str, str]]:
    version = client.post(
        "/workflows",
        json={
            "name": "http_claim_" + uuid4().hex,
            "tasks": [
                {"task_id": "A", "task_type": "demo.echo"},
                {"task_id": "B", "task_type": "demo.echo"},
            ],
        },
    )
    assert version.status_code == 201
    run = client.post(
        "/runs",
        json={"workflow_version_id": version.json()["id"]},
        headers={"Idempotency-Key": uuid4().hex},
    )
    assert run.status_code == 201
    session = str(uuid4())
    assert (
        client.put(
            f"/worker-sessions/{session}",
            json={"worker_name": "http_worker", "max_concurrency": capacity},
        ).status_code
        == 200
    )
    return session, {"run_id": run.json()["run_id"], "request_id": str(uuid4())}


def test_committed_claim_replay_and_server_policy(
    client: TestClient,
    http_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    session, body = setup(client, capacity=1)
    monkeypatch.setenv("DWE_ATTEMPT_LEASE_SECONDS", "1")
    path = f"/worker-sessions/{session}/claims"
    response = client.post(path, json=body)
    assert (
        response.status_code == 200 and response.headers["Cache-Control"] == "no-store"
    )
    assert "Location" not in response.headers
    first = response.json()
    assert set(first) == {"worker_session_id", "run_id", "request_id", "claim"}
    assert first["worker_session_id"] == session and first["run_id"] == body["run_id"]
    assert first["request_id"] == body["request_id"]
    grant = first["claim"]
    assert grant["task"]["task_key"] == "A" and grant["task"]["status"] == "RUNNING"
    assert grant["attempt"]["status"] == "RUNNING"
    assert grant["definition"]["task_type"] == "demo.echo"
    lease = grant["lease"]
    assert lease["attempt_id"] == grant["attempt"]["id"]
    assert lease["worker_session_id"] == session
    assert datetime.fromisoformat(lease["lease_expires_at"]) - datetime.fromisoformat(
        lease["acquired_at"]
    ) == timedelta(seconds=600)
    with http_engine.begin() as connection:
        assert connection.execute(
            select(claim_requests.c.attempt_id)
        ).scalar_one() == UUID(grant["attempt"]["id"])
        assert (
            str(connection.execute(select(attempt_leases.c.lease_token)).scalar_one())
            == lease["lease_token"]
        )
    replay = client.post(path, json=body)
    assert replay.status_code == 200 and replay.json() == first
    assert replay.headers["Cache-Control"] == "no-store"
    assert lease["lease_token"] not in capsys.readouterr().err


def test_no_work_is_retained_after_capacity_frees(
    client: TestClient, http_engine: Engine
) -> None:
    session, body = setup(client, capacity=1)
    path = f"/worker-sessions/{session}/claims"
    granted = client.post(path, json=body).json()["claim"]
    empty_body = {**body, "request_id": str(uuid4())}
    empty = client.post(path, json=empty_body)
    assert empty.status_code == 200 and empty.json()["claim"] is None
    assert empty.headers["Cache-Control"] == "no-store"
    with http_engine.begin() as connection:
        connection.execute(task_attempts.update().values(status="SUCCEEDED"))
        connection.execute(
            task_runs.update()
            .where(task_runs.c.id == UUID(granted["task"]["id"]))
            .values(status="SUCCEEDED")
        )
    replay = client.post(path, json=empty_body)
    assert replay.status_code == 200 and replay.json() == empty.json()
    fresh = client.post(path, json={**body, "request_id": str(uuid4())})
    assert fresh.status_code == 200 and fresh.json()["claim"]["task"]["task_key"] == "B"


@pytest.mark.parametrize("conflicting", [False, True])
def test_concurrent_http_requests(
    client: TestClient, http_engine: Engine, conflicting: bool
) -> None:
    session, body = setup(client)
    _, other = setup(client)
    barrier = Barrier(4, timeout=10)

    def submit(index: int) -> tuple[int, dict[str, object]]:
        barrier.wait()
        payload = (
            {**body, "run_id": other["run_id"]} if conflicting and index % 2 else body
        )
        result = client.post(f"/worker-sessions/{session}/claims", json=payload)
        return result.status_code, dict(result.json())

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(submit, i) for i in range(4)]
        results = [f.result(timeout=20) for f in futures]
    successes = [body for code, body in results if code == 200]
    assert len(successes) == (2 if conflicting else 4)
    assert all(body == successes[0] for body in successes)
    for code, result_body in results:
        if code != 200:
            assert code == 409 and isinstance(result_body["error"], dict)
            assert result_body["error"]["code"] == "claim_request_conflict"
    with http_engine.begin() as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(task_attempts)
            ).scalar_one()
            == 1
        )
        assert (
            connection.execute(
                select(func.count()).select_from(claim_requests)
            ).scalar_one()
            == 1
        )


@pytest.mark.parametrize("missing", ["run", "worker"])
def test_missing_parent(client: TestClient, missing: str) -> None:
    session, body = setup(client)
    if missing == "run":
        body["run_id"] = str(uuid4())
    else:
        session = str(uuid4())
    result = client.post(f"/worker-sessions/{session}/claims", json=body)
    assert result.status_code == 404
    assert result.json()["error"]["code"] == (
        "run_not_found" if missing == "run" else "worker_not_found"
    )


@pytest.mark.parametrize(
    "case,code",
    [
        ("run", "run_inactive"),
        ("worker", "worker_inactive"),
        ("expiry", "worker_expired"),
    ],
)
def test_new_claim_admission_failure(
    client: TestClient, http_engine: Engine, case: str, code: str
) -> None:
    session, body = setup(client)
    with http_engine.begin() as connection:
        if case == "run":
            connection.execute(workflow_runs.update().values(status="FAILED"))
        elif case == "worker":
            connection.execute(worker_sessions.update().values(status="LOST"))
        else:
            # A different historical session has already expired; no sleeps.
            session = str(uuid4())
            stamp = connection.execute(
                select(func.clock_timestamp())
            ).scalar_one() - timedelta(days=1)
            connection.execute(
                worker_sessions.insert().values(
                    id=UUID(session),
                    worker_name="expired",
                    max_concurrency=1,
                    created_at=stamp,
                    last_heartbeat_at=stamp,
                    heartbeat_expires_at=stamp + timedelta(seconds=30),
                )
            )
    result = client.post(f"/worker-sessions/{session}/claims", json=body)
    assert result.status_code == 409 and result.json()["error"]["code"] == code
    with http_engine.begin() as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(claim_requests)
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize("terminal", [False, True])
def test_unavailable_replay_is_not_empty_or_new_work(
    client: TestClient,
    http_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    terminal: bool,
) -> None:
    session, body = setup(client)
    path = f"/worker-sessions/{session}/claims"
    first = client.post(path, json=body).json()["claim"]
    if terminal:
        with http_engine.begin() as connection:
            connection.execute(task_attempts.update().values(status="LOST"))
    else:
        deadline = datetime.fromisoformat(first["lease"]["lease_expires_at"])
        monkeypatch.setattr(
            ClaimRequestRepository, "_database_now", lambda self: deadline
        )
    result = client.post(path, json=body)
    assert (
        result.status_code == 409
        and result.json()["error"]["code"] == "claim_replay_unavailable"
    )
    assert (
        "claim" not in result.json()
        and first["lease"]["lease_token"] not in result.text
    )
    conflict = client.post(path, json={**body, "run_id": str(uuid4())})
    assert (
        conflict.status_code == 409
        and conflict.json()["error"]["code"] == "claim_request_conflict"
    )
    with http_engine.begin() as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(task_attempts)
            ).scalar_one()
            == 1
        )


def test_replay_reflects_renewal_and_lost_worker(
    client: TestClient, http_engine: Engine
) -> None:
    session, body = setup(client)
    path = f"/worker-sessions/{session}/claims"
    first = client.post(path, json=body).json()["claim"]
    with http_engine.begin() as connection:
        renewed = LeaseRepository(connection, lease_seconds=900).renew(
            UUID(first["attempt"]["id"]),
            worker_session_id=UUID(session),
            lease_token=UUID(first["lease"]["lease_token"]),
        )
        connection.execute(worker_sessions.update().values(status="LOST"))
    result = client.post(path, json=body)
    assert result.status_code == 200
    assert result.json()["claim"]["attempt"] == first["attempt"]
    assert result.json()["claim"]["lease"] == renewed.model_dump(mode="json")
    with http_engine.begin() as connection:
        assert (
            connection.execute(select(attempt_leases.c.last_renewed_at)).scalar_one()
            == renewed.last_renewed_at
        )


@pytest.mark.parametrize(
    "error,status,code",
    [
        (WorkerClockRegressionError, 503, "worker_clock_regression"),
        (LeaseClockRegressionError, 503, "lease_clock_regression"),
        (AttemptNumberExhaustedError, 409, "attempt_number_exhausted"),
    ],
)
def test_domain_error_mapping(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    error: type[Exception],
    status: int,
    code: str,
) -> None:
    session, body = setup(client)

    def fail(*args: object, **kwargs: object) -> None:
        raise error("sentinel-private-detail")

    monkeypatch.setattr(ClaimRequestRepository, "claim_next", fail)
    result = client.post(f"/worker-sessions/{session}/claims", json=body)
    assert result.status_code == status and result.json()["error"]["code"] == code
    assert "sentinel-private-detail" not in result.text


@pytest.mark.parametrize("phase", ["insert", "commit"])
@pytest.mark.parametrize("empty", [False, True])
def test_storage_failure_never_returns_a_provisional_grant(
    client: TestClient,
    http_engine: Engine,
    migration_schema: str,
    phase: str,
    empty: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    session, body = setup(client, capacity=1)
    path = f"/worker-sessions/{session}/claims"
    if empty:
        assert (
            client.post(path, json={**body, "request_id": str(uuid4())}).status_code
            == 200
        )
    tables = (claim_requests, task_attempts, attempt_leases, task_runs)
    with http_engine.begin() as connection:
        before = {
            t.name: connection.execute(select(t).order_by(*t.primary_key.columns)).all()
            for t in tables
        }
        connection.exec_driver_sql(f"""
            CREATE FUNCTION "{migration_schema}".reject_claim_http() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='23514',
                MESSAGE='sentinel-private-database-detail'; END; $$""")
        kind = (
            "TRIGGER reject_claim_http BEFORE"
            if phase == "insert"
            else "CONSTRAINT TRIGGER reject_claim_http AFTER"
        )
        deferred = "" if phase == "insert" else "DEFERRABLE INITIALLY DEFERRED"
        connection.exec_driver_sql(
            f'CREATE {kind} INSERT ON "{migration_schema}".claim_requests {deferred} '
            f'FOR EACH ROW EXECUTE FUNCTION "{migration_schema}".reject_claim_http()'
        )
    result = client.post(path, json=body)
    assert (
        result.status_code == 500 and result.json()["error"]["code"] == "storage_error"
    )
    assert "claim" not in result.json() and "Location" not in result.headers
    assert (
        "sentinel-private-database-detail" not in result.text + capsys.readouterr().err
    )
    with http_engine.begin() as connection:
        for table in tables:
            assert (
                connection.execute(
                    select(table).order_by(*table.primary_key.columns)
                ).all()
                == before[table.name]
            )
        connection.exec_driver_sql(
            f'DROP TRIGGER reject_claim_http ON "{migration_schema}".claim_requests'
        )
    retry = client.post(path, json=body)
    assert retry.status_code == 200 and (retry.json()["claim"] is None) is empty


def test_response_validation_precedes_commit(
    client: TestClient,
    http_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    session, body = setup(client)
    original = ClaimRequestRepository.claim_next

    def invalid(
        self: ClaimRequestRepository,
        run_id: UUID,
        worker_session_id: UUID,
        *,
        request_id: UUID,
    ) -> TaskClaim | None:
        value = original(self, run_id, worker_session_id, request_id=request_id)
        assert value is not None
        return replace(
            value, task=value.task.model_copy(update={"task_key": "private/invalid"})
        )

    monkeypatch.setattr(ClaimRequestRepository, "claim_next", invalid)
    result = client.post(f"/worker-sessions/{session}/claims", json=body)
    assert (
        result.status_code == 500 and result.json()["error"]["code"] == "storage_error"
    )
    assert "private/invalid" not in result.text + capsys.readouterr().err
    with http_engine.begin() as connection:
        for table in (claim_requests, task_attempts, attempt_leases):
            assert (
                connection.execute(select(func.count()).select_from(table)).scalar_one()
                == 0
            )


def test_corrupt_binding_has_sanitized_storage_response(
    client: TestClient,
    http_engine: Engine,
    migration_schema: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    session, body = setup(client)
    path = f"/worker-sessions/{session}/claims"
    first = client.post(path, json=body).json()["claim"]
    _, other = setup(client)
    with http_engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE "{migration_schema}".claim_requests '
            "DISABLE TRIGGER claim_requests_append_only"
        )
        connection.execute(claim_requests.update().values(run_id=UUID(other["run_id"])))
    result = client.post(path, json={**body, "run_id": other["run_id"]})
    assert (
        result.status_code == 500 and result.json()["error"]["code"] == "storage_error"
    )
    assert first["lease"]["lease_token"] not in result.text + capsys.readouterr().err


def test_liveness_and_commit_boundary_while_claim_waits(
    client: TestClient, http_engine: Engine
) -> None:
    session, body = setup(client)
    entered = Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if "pg_advisory_xact_lock" in statement:
            entered.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        with http_engine.begin() as owner:
            first = ClaimRequestRepository(owner, lease_seconds=600).claim_next(
                UUID(body["run_id"]), UUID(session), request_id=UUID(body["request_id"])
            )
            assert first is not None
            event.listen(http_engine, "before_cursor_execute", before_execute)
            try:
                pending = executor.submit(
                    client.post, f"/worker-sessions/{session}/claims", json=body
                )
                assert entered.wait(timeout=5) and not pending.done()
                assert (
                    executor.submit(client.get, "/health/live")
                    .result(timeout=2)
                    .status_code
                    == 200
                )
            finally:
                event.remove(http_engine, "before_cursor_execute", before_execute)
        response = pending.result(timeout=10)
    assert response.status_code == 200 and response.json()["claim"]["attempt"][
        "id"
    ] == str(first.attempt.id)


def test_lock_timeout_returns_503_without_allocating(
    client: TestClient, http_engine: Engine
) -> None:
    session, body = setup(client)

    def short_timeout(connection: Connection) -> None:
        connection.exec_driver_sql("SET LOCAL lock_timeout = '100ms'")

    with http_engine.begin() as owner:
        owner.execute(
            text("SELECT pg_advisory_xact_lock(:ns, :key)"),
            {
                "ns": _CLAIM_LOCK_NAMESPACE,
                "key": _claim_lock_key(UUID(session), UUID(body["request_id"])),
            },
        )
        event.listen(http_engine, "begin", short_timeout)
        try:
            result = client.post(f"/worker-sessions/{session}/claims", json=body)
        finally:
            event.remove(http_engine, "begin", short_timeout)
    assert (
        result.status_code == 503
        and result.json()["error"]["code"] == "database_unavailable"
    )
    with http_engine.begin() as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(claim_requests)
            ).scalar_one()
            == 0
        )
    assert (
        client.post(f"/worker-sessions/{session}/claims", json=body).status_code == 200
    )
