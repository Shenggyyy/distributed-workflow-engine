"""Renewal HTTP ownership, timing, commit boundaries and existing claim replay."""

import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Barrier, Event
from uuid import UUID, uuid4

import pytest
from alembic import command
from fastapi.testclient import TestClient
from sqlalchemy import Connection, Engine, event, func, select

from tests.integration.migration_helpers import migration_config
from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.domain.lease import AttemptLease
from workflow_engine.repositories.leases import LeaseRepository
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


@pytest.fixture
def lease(client: TestClient) -> AttemptLease:
    version = client.post(
        "/workflows",
        json={
            "name": "renew_http_" + uuid4().hex,
            "schema_version": 2,
            "tasks": [
                {
                    "task_id": "A",
                    "task_type": "demo.echo",
                    "execution": {"timeout_seconds": 3600},
                }
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
            json={"worker_name": "renew_worker", "max_concurrency": 1},
        ).status_code
        == 200
    )
    response = client.post(
        f"/worker-sessions/{session}/claims",
        json={"run_id": run.json()["run_id"], "request_id": str(uuid4())},
    )
    assert response.status_code == 200
    return AttemptLease.model_validate_json(
        json.dumps(response.json()["claim"]["lease"])
    )


def path(lease: AttemptLease) -> str:
    return (
        f"/worker-sessions/{lease.worker_session_id}/attempts/{lease.attempt_id}/renew"
    )


def body(lease: AttemptLease) -> dict[str, str]:
    return {"lease_token": str(lease.lease_token)}


def test_committed_renewal_and_retry_use_new_observations(
    client: TestClient,
    http_engine: Engine,
    lease: AttemptLease,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DWE_ATTEMPT_LEASE_SECONDS", "1")
    tables = (workflow_runs, task_runs, task_attempts, worker_sessions, claim_requests)
    with http_engine.begin() as connection:
        before = {t.name: connection.execute(select(t)).all() for t in tables}
    for offset in (10, 20):
        observed = lease.acquired_at + timedelta(seconds=offset)
        monkeypatch.setattr(
            LeaseRepository, "_database_now", lambda self, stamp=observed: stamp
        )
        response = client.post(path(lease), json=body(lease))
        assert (
            response.status_code == 200
            and response.headers["Cache-Control"] == "no-store"
        )
        assert "Location" not in response.headers and set(response.json()) == {"lease"}
        current = AttemptLease.model_validate_json(json.dumps(response.json()["lease"]))
        assert current.last_renewed_at == observed
        assert current.lease_expires_at == observed + timedelta(seconds=600)
        assert current.model_dump(
            exclude={"last_renewed_at", "lease_expires_at"}
        ) == lease.model_dump(exclude={"last_renewed_at", "lease_expires_at"})
        with http_engine.begin() as connection:
            assert (
                connection.execute(
                    select(attempt_leases.c.lease_expires_at)
                ).scalar_one()
                == current.lease_expires_at
            )
            for table in tables:
                assert connection.execute(select(table)).all() == before[table.name]
    assert str(lease.lease_token) not in capsys.readouterr().err


def test_shorter_server_policy_cannot_shrink_deadline(
    http_engine: Engine, lease: AttemptLease
) -> None:
    with TestClient(
        create_app(
            Settings(environment="test", attempt_lease_seconds=1), engine=http_engine
        )
    ) as client:
        response = client.post(path(lease), json=body(lease))
    assert response.status_code == 200
    current = AttemptLease.model_validate_json(json.dumps(response.json()["lease"]))
    assert current.lease_expires_at == lease.lease_expires_at


@pytest.mark.parametrize("wrong_session", [False, True])
def test_wrong_ownership_does_not_mutate(
    client: TestClient,
    http_engine: Engine,
    lease: AttemptLease,
    wrong_session: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    uri = (
        f"/worker-sessions/{uuid4()}/attempts/{lease.attempt_id}/renew"
        if wrong_session
        else path(lease)
    )
    payload = body(lease) if wrong_session else {"lease_token": str(uuid4())}
    response = client.post(uri, json=payload)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "lease_ownership_mismatch"
    assert str(lease.lease_token) not in response.text + capsys.readouterr().err
    assert payload["lease_token"] not in response.text
    with http_engine.begin() as connection:
        assert (
            dict(connection.execute(select(attempt_leases)).mappings().one())
            == lease.model_dump()
        )


@pytest.mark.parametrize("unowned", [False, True])
def test_missing_or_unowned_attempt(
    client: TestClient, http_engine: Engine, lease: AttemptLease, unowned: bool
) -> None:
    attempt_id = uuid4()
    if unowned:
        with http_engine.begin() as connection:
            task_id = connection.execute(select(task_attempts.c.task_id)).scalar_one()
            connection.execute(
                task_attempts.insert().values(
                    id=attempt_id, task_id=task_id, attempt_number=2, status="FAILED"
                )
            )
    response = client.post(
        f"/worker-sessions/{lease.worker_session_id}/attempts/{attempt_id}/renew",
        json=body(lease),
    )
    assert (
        response.status_code == 404
        and response.json()["error"]["code"] == "lease_not_found"
    )


@pytest.mark.parametrize("offset", [-0.000001, 599.999999, 600, 600.000001])
def test_clock_and_exclusive_deadline(
    client: TestClient,
    http_engine: Engine,
    lease: AttemptLease,
    monkeypatch: pytest.MonkeyPatch,
    offset: float,
) -> None:
    observed = lease.acquired_at + timedelta(seconds=offset)
    monkeypatch.setattr(LeaseRepository, "_database_now", lambda self: observed)
    response = client.post(path(lease), json=body(lease))
    if 0 <= offset < 600:
        assert response.status_code == 200
    else:
        assert response.status_code == (503 if offset < 0 else 409)
        assert response.json()["error"]["code"] == (
            "lease_clock_regression" if offset < 0 else "lease_expired"
        )
        with http_engine.begin() as connection:
            assert (
                dict(connection.execute(select(attempt_leases)).mappings().one())
                == lease.model_dump()
            )


@pytest.mark.parametrize(
    "target,status",
    [
        ("workflow_runs", "FAILED"),
        ("task_runs", "SUCCEEDED"),
        ("task_attempts", "LOST"),
        ("task_attempts", "TIMED_OUT"),
        ("worker_sessions", "STOPPED"),
    ],
)
def test_inactive_execution_precedes_token_check(
    client: TestClient,
    http_engine: Engine,
    migration_schema: str,
    lease: AttemptLease,
    target: str,
    status: str,
) -> None:
    with http_engine.begin() as connection:
        # Constant test table/state cases simulate future completion/recovery.
        connection.exec_driver_sql(
            f"UPDATE \"{migration_schema}\".{target} SET status = '{status}'"
        )
    response = client.post(path(lease), json={"lease_token": str(uuid4())})
    assert (
        response.status_code == 409
        and response.json()["error"]["code"] == "lease_inactive"
    )
    with http_engine.begin() as connection:
        assert (
            dict(connection.execute(select(attempt_leases)).mappings().one())
            == lease.model_dump()
        )


@pytest.mark.parametrize("lost", [False, True])
def test_expired_heartbeat_does_not_revoke_live_lease(
    client: TestClient,
    http_engine: Engine,
    lease: AttemptLease,
    monkeypatch: pytest.MonkeyPatch,
    lost: bool,
) -> None:
    with http_engine.begin() as connection:
        if lost:
            connection.execute(worker_sessions.update().values(status="LOST"))
        worker = connection.execute(select(worker_sessions)).one()
    monkeypatch.setattr(
        LeaseRepository,
        "_database_now",
        lambda self: worker.heartbeat_expires_at + timedelta(seconds=1),
    )
    assert client.post(path(lease), json=body(lease)).status_code == 200
    with http_engine.begin() as connection:
        assert connection.execute(select(worker_sessions)).one() == worker


def test_claim_replay_observes_http_renewal(
    client: TestClient, http_engine: Engine, lease: AttemptLease
) -> None:
    renewed = client.post(path(lease), json=body(lease))
    assert renewed.status_code == 200
    with http_engine.begin() as connection:
        binding = connection.execute(select(claim_requests)).one()
    replay = client.post(
        f"/worker-sessions/{lease.worker_session_id}/claims",
        json={"run_id": str(binding.run_id), "request_id": str(binding.request_id)},
    )
    assert (
        replay.status_code == 200
        and replay.json()["claim"]["lease"] == renewed.json()["lease"]
    )


def test_concurrent_renewals_preserve_identity(
    client: TestClient, http_engine: Engine, lease: AttemptLease
) -> None:
    barrier = Barrier(4, timeout=10)

    def submit() -> AttemptLease:
        barrier.wait()
        response = client.post(path(lease), json=body(lease))
        assert response.status_code == 200
        return AttemptLease.model_validate_json(json.dumps(response.json()["lease"]))

    with ThreadPoolExecutor(max_workers=4) as executor:
        pending = [executor.submit(submit) for _ in range(4)]
        results = [f.result(timeout=20) for f in pending]
    assert all(
        r.lease_token == lease.lease_token and r.attempt_id == lease.attempt_id
        for r in results
    )
    with http_engine.begin() as connection:
        assert connection.execute(
            select(attempt_leases.c.last_renewed_at)
        ).scalar_one() == max(r.last_renewed_at for r in results)
        assert (
            connection.execute(
                select(func.count()).select_from(task_attempts)
            ).scalar_one()
            == 1
        )


@pytest.mark.parametrize("deferred", [False, True])
def test_failed_write_or_commit_returns_no_renewed_lease(
    client: TestClient,
    http_engine: Engine,
    migration_schema: str,
    lease: AttemptLease,
    deferred: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with http_engine.begin() as connection:
        connection.exec_driver_sql(f"""
            CREATE FUNCTION "{migration_schema}".reject_lease_http() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='23514',
                MESSAGE='sentinel-private-detail'; END; $$""")
        kind = (
            "CONSTRAINT TRIGGER reject_lease_http AFTER"
            if deferred
            else "TRIGGER reject_lease_http BEFORE"
        )
        timing = "DEFERRABLE INITIALLY DEFERRED" if deferred else ""
        connection.exec_driver_sql(
            f'CREATE {kind} UPDATE ON "{migration_schema}".attempt_leases {timing} '
            f'FOR EACH ROW EXECUTE FUNCTION "{migration_schema}".reject_lease_http()'
        )
    response = client.post(path(lease), json=body(lease))
    assert (
        response.status_code == 500
        and response.json()["error"]["code"] == "storage_error"
    )
    assert "lease" not in response.json()
    logs = capsys.readouterr().err
    assert "sentinel-private-detail" not in response.text + logs
    assert str(lease.lease_token) not in response.text + logs
    with http_engine.begin() as connection:
        assert (
            dict(connection.execute(select(attempt_leases)).mappings().one())
            == lease.model_dump()
        )
        connection.exec_driver_sql(
            f'DROP TRIGGER reject_lease_http ON "{migration_schema}".attempt_leases'
        )
    assert client.post(path(lease), json=body(lease)).status_code == 200


def test_invalid_response_rolls_back_before_success(
    client: TestClient,
    http_engine: Engine,
    lease: AttemptLease,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original = LeaseRepository.renew

    def invalid(
        self: LeaseRepository,
        attempt_id: UUID,
        *,
        worker_session_id: UUID,
        lease_token: UUID,
    ) -> AttemptLease:
        renewed = original(
            self,
            attempt_id,
            worker_session_id=worker_session_id,
            lease_token=lease_token,
        )
        return renewed.model_copy(update={"lease_token": "private-value"})

    monkeypatch.setattr(LeaseRepository, "renew", invalid)
    response = client.post(path(lease), json=body(lease))
    assert (
        response.status_code == 500
        and response.json()["error"]["code"] == "storage_error"
    )
    assert "private-value" not in response.text + capsys.readouterr().err
    with http_engine.begin() as connection:
        assert (
            dict(connection.execute(select(attempt_leases)).mappings().one())
            == lease.model_dump()
        )


def test_corrupt_lease_is_sanitized(
    client: TestClient,
    http_engine: Engine,
    migration_schema: str,
    lease: AttemptLease,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with http_engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE "{migration_schema}".attempt_leases '
            "DROP CONSTRAINT ck_attempt_leases_lease_order"
        )
        connection.execute(
            attempt_leases.update().values(last_renewed_at=lease.lease_expires_at)
        )
    response = client.post(path(lease), json=body(lease))
    assert (
        response.status_code == 500
        and response.json()["error"]["code"] == "storage_error"
    )
    assert str(lease.lease_token) not in response.text + capsys.readouterr().err


def test_lock_wait_rechecks_expiry_without_blocking_liveness(
    client: TestClient,
    http_engine: Engine,
    lease: AttemptLease,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, sampled = Event(), Event()

    def observe(self: LeaseRepository) -> datetime:
        sampled.set()
        return lease.lease_expires_at

    monkeypatch.setattr(LeaseRepository, "_database_now", observe)

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if "attempt_leases" in statement and "FOR UPDATE" in statement:
            entered.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        with http_engine.begin() as blocker:
            blocker.execute(select(attempt_leases).with_for_update()).all()
            event.listen(http_engine, "before_cursor_execute", before_execute)
            try:
                pending = executor.submit(client.post, path(lease), json=body(lease))
                assert (
                    entered.wait(timeout=5)
                    and not pending.done()
                    and not sampled.is_set()
                )
                assert (
                    executor.submit(client.get, "/health/live")
                    .result(timeout=2)
                    .status_code
                    == 200
                )
            finally:
                event.remove(http_engine, "before_cursor_execute", before_execute)
        response = pending.result(timeout=10)
    assert sampled.is_set() and response.status_code == 409
    assert response.json()["error"]["code"] == "lease_expired"


def test_lock_timeout_returns_503_and_preserves_lease(
    client: TestClient, http_engine: Engine, lease: AttemptLease
) -> None:
    def short_timeout(connection: Connection) -> None:
        connection.exec_driver_sql("SET LOCAL lock_timeout = '100ms'")

    with http_engine.begin() as blocker:
        blocker.execute(select(attempt_leases).with_for_update()).all()
        event.listen(http_engine, "begin", short_timeout)
        try:
            response = client.post(path(lease), json=body(lease))
        finally:
            event.remove(http_engine, "begin", short_timeout)
    assert (
        response.status_code == 503
        and response.json()["error"]["code"] == "database_unavailable"
    )
    with http_engine.begin() as connection:
        assert (
            dict(connection.execute(select(attempt_leases)).mappings().one())
            == lease.model_dump()
        )
    assert client.post(path(lease), json=body(lease)).status_code == 200
