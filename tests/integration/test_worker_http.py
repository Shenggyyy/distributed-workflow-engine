"""Worker HTTP contracts on PostgreSQL, including races and commit failures."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Barrier, Event
from uuid import uuid4

import pytest
from alembic import command
from fastapi.testclient import TestClient
from sqlalchemy import Engine, event, func, select

from tests.integration.migration_helpers import migration_config
from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.schema import worker_sessions

pytestmark = pytest.mark.integration
BODY = {"worker_name": "worker", "max_concurrency": 2}


@pytest.fixture
def http_engine(engine: Engine, migration_schema: str) -> Engine:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return engine.execution_options(schema_translate_map={None: migration_schema})


@pytest.fixture
def client(http_engine: Engine) -> Iterator[TestClient]:
    with TestClient(
        create_app(
            Settings(environment="test", worker_heartbeat_timeout_seconds=7),
            engine=http_engine,
        )
    ) as value:
        yield value


def test_registration_replay_and_heartbeat_commit(
    client: TestClient, http_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The factory settings remain authoritative after process environment changes.
    monkeypatch.setenv("DWE_WORKER_HEARTBEAT_TIMEOUT_SECONDS", "60")
    session_id = uuid4()
    path = f"/worker-sessions/{session_id}"
    first = client.put(path, json=BODY)
    assert first.status_code == 200 and "Location" not in first.headers
    original = first.json()
    assert original["session"] == {"id": str(session_id), **BODY, "status": "ACTIVE"}
    assert (
        datetime.fromisoformat(original["heartbeat_expires_at"])
        - datetime.fromisoformat(original["last_heartbeat_at"])
    ) == timedelta(seconds=7)
    with http_engine.begin() as connection:
        stored = connection.execute(select(worker_sessions)).one()
        assert stored.id == session_id
        assert stored.last_heartbeat_at == datetime.fromisoformat(
            original["created_at"]
        )
    assert client.put(path, json=BODY).json() == original
    renewed = client.post(path + "/heartbeat", json={})
    assert renewed.status_code == 200 and "Location" not in renewed.headers
    current = renewed.json()
    assert current["session"] == original["session"]
    assert current["created_at"] == original["created_at"]
    assert (
        datetime.fromisoformat(current["last_heartbeat_at"]) >= stored.last_heartbeat_at
    )
    assert (
        datetime.fromisoformat(current["heartbeat_expires_at"])
        - datetime.fromisoformat(current["last_heartbeat_at"])
    ) == timedelta(seconds=7)
    assert client.put(path, json=BODY).json() == current
    with http_engine.begin() as connection:
        committed = connection.execute(select(worker_sessions)).one()
        assert committed.last_heartbeat_at == datetime.fromisoformat(
            current["last_heartbeat_at"]
        )


@pytest.mark.parametrize("different", [False, True])
def test_concurrent_registration(
    client: TestClient, http_engine: Engine, different: bool
) -> None:
    path = f"/worker-sessions/{uuid4()}"
    barrier = Barrier(4, timeout=10)

    def submit(index: int) -> tuple[int, dict[str, object]]:
        barrier.wait()
        result = client.put(
            path,
            json={**BODY, "max_concurrency": 2 + index % 2 if different else 2},
        )
        return result.status_code, dict(result.json())

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(submit, index) for index in range(4)]
        results = [future.result(timeout=20) for future in futures]
    successes = [body for status, body in results if status == 200]
    assert len(successes) == (2 if different else 4)
    assert all(body == successes[0] for body in successes)
    for status, body in results:
        if status != 200:
            assert status == 409
            assert isinstance(body["error"], dict)
            assert body["error"]["code"] == "worker_registration_conflict"
    with http_engine.begin() as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(worker_sessions)
            ).scalar_one()
            == 1
        )


@pytest.mark.parametrize(
    ("status", "future", "expected", "code"),
    [
        ("ACTIVE", False, 409, "worker_expired"),
        ("LOST", False, 409, "worker_inactive"),
        ("STOPPED", False, 409, "worker_inactive"),
        ("ACTIVE", True, 503, "worker_clock_regression"),
    ],
)
def test_rejected_heartbeat_preserves_snapshot_and_registration_can_replay(
    client: TestClient,
    http_engine: Engine,
    status: str,
    future: bool,
    expected: int,
    code: str,
) -> None:
    session_id = uuid4()
    with http_engine.begin() as connection:
        now = connection.execute(select(func.clock_timestamp())).scalar_one()
        stamp = now + timedelta(days=1 if future else -1)
        connection.execute(
            worker_sessions.insert().values(
                id=session_id,
                **BODY,
                status=status,
                created_at=stamp,
                last_heartbeat_at=stamp,
                heartbeat_expires_at=stamp + timedelta(seconds=30),
            )
        )
        original = connection.execute(select(worker_sessions)).one()
    path = f"/worker-sessions/{session_id}"
    result = client.post(path + "/heartbeat", json={})
    assert result.status_code == expected
    assert result.json()["error"]["code"] == code
    replay = client.put(path, json=BODY)
    assert replay.status_code == 200
    assert replay.json()["session"]["status"] == status
    with http_engine.begin() as connection:
        assert connection.execute(select(worker_sessions)).one() == original


def test_missing_heartbeat(client: TestClient) -> None:
    result = client.post(f"/worker-sessions/{uuid4()}/heartbeat", json={})
    assert result.status_code == 404
    assert result.json()["error"]["code"] == "worker_not_found"


@pytest.mark.parametrize("phase", ["insert", "register_commit", "heartbeat_commit"])
def test_storage_failure_rolls_back_before_http_success(
    client: TestClient,
    http_engine: Engine,
    migration_schema: str,
    phase: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = f"/worker-sessions/{uuid4()}"
    if phase == "heartbeat_commit":
        assert client.put(path, json=BODY).status_code == 200
    with http_engine.begin() as connection:
        before = connection.execute(select(worker_sessions)).all()
        connection.exec_driver_sql(f"""
            CREATE FUNCTION "{migration_schema}".reject_worker_http() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN
                RAISE EXCEPTION USING ERRCODE='23514',
                    MESSAGE='sentinel-private-database-detail';
            END; $$""")
        trigger = (
            "TRIGGER reject_worker_http BEFORE INSERT"
            if phase == "insert"
            else "CONSTRAINT TRIGGER reject_worker_http AFTER "
            + ("UPDATE" if phase == "heartbeat_commit" else "INSERT")
        )
        deferred = "" if phase == "insert" else "DEFERRABLE INITIALLY DEFERRED"
        connection.exec_driver_sql(f"""
            CREATE {trigger} ON "{migration_schema}".worker_sessions
            {deferred} FOR EACH ROW
            EXECUTE FUNCTION "{migration_schema}".reject_worker_http()""")
    response = (
        client.post(path + "/heartbeat", json={})
        if phase == "heartbeat_commit"
        else client.put(path, json=BODY)
    )
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "storage_error"
    assert "Location" not in response.headers
    assert (
        "sentinel-private-database-detail"
        not in response.text + capsys.readouterr().err
    )
    with http_engine.begin() as connection:
        assert connection.execute(select(worker_sessions)).all() == before
        connection.exec_driver_sql(
            f'DROP TRIGGER reject_worker_http ON "{migration_schema}".worker_sessions'
        )
    retry = (
        client.post(path + "/heartbeat", json={})
        if phase == "heartbeat_commit"
        else client.put(path, json=BODY)
    )
    assert retry.status_code == 200


def test_corrupt_stored_session_is_sanitized(
    client: TestClient,
    http_engine: Engine,
    migration_schema: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    session_id = uuid4()
    with http_engine.begin() as connection:
        now = connection.execute(select(func.clock_timestamp())).scalar_one()
        connection.exec_driver_sql(
            f'ALTER TABLE "{migration_schema}".worker_sessions '
            "DROP CONSTRAINT ck_worker_sessions_status_values"
        )
        connection.execute(
            worker_sessions.insert().values(
                id=session_id,
                **BODY,
                status="PRIVATE_BAD",
                created_at=now,
                last_heartbeat_at=now,
                heartbeat_expires_at=now + timedelta(seconds=30),
            )
        )
    path = f"/worker-sessions/{session_id}"
    for response in (
        client.put(path, json=BODY),
        client.post(path + "/heartbeat", json={}),
    ):
        assert response.status_code == 500
        assert response.json()["error"]["code"] == "storage_error"
        assert "PRIVATE_BAD" not in response.text + capsys.readouterr().err


def test_liveness_while_registration_waits_for_lock(
    client: TestClient,
    http_engine: Engine,
) -> None:
    session_id = uuid4()
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
            WorkerRepository(owner).register(
                session_id, worker_name="worker", max_concurrency=2
            )
            event.listen(http_engine, "before_cursor_execute", before_execute)
            try:
                pending = executor.submit(
                    client.put, f"/worker-sessions/{session_id}", json=BODY
                )
                assert entered.wait(timeout=5)
                assert (
                    executor.submit(client.get, "/health/live")
                    .result(timeout=2)
                    .status_code
                    == 200
                )
                assert not pending.done()
            finally:
                event.remove(http_engine, "before_cursor_execute", before_execute)
        assert pending.result(timeout=5).status_code == 200
