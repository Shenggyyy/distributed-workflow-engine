"""Registration replay, locks, database time and rollback on real PostgreSQL."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from queue import Queue
from threading import Barrier
from time import monotonic, sleep
from uuid import UUID, uuid4

import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import Connection, Engine, event, func, select, text
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.worker import WorkerStatus
from workflow_engine.repositories import workers as worker_module
from workflow_engine.repositories.workers import (
    StoredWorkerError,
    StoredWorkerSession,
    WorkerRegistrationConflictError,
    WorkerRepository,
)
from workflow_engine.repositories.workflows import RepositoryTransactionError
from workflow_engine.schema import worker_sessions

pytestmark = pytest.mark.integration


@contextmanager
def transaction(engine: Engine, schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        migration_config(connection, schema)
        yield connection


@pytest.fixture
def registered_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


def register(connection: Connection, session_id: UUID) -> StoredWorkerSession:
    return WorkerRepository(connection).register(
        session_id, worker_name="worker", max_concurrency=2
    )


def count(connection: Connection) -> int:
    return connection.execute(
        select(func.count()).select_from(worker_sessions)
    ).scalar_one()


def test_creation_uses_database_clock_and_replay_preserves_every_field(
    engine: Engine, registered_schema: str
) -> None:
    session_id = uuid4()
    with transaction(engine, registered_schema) as connection:
        before = connection.execute(select(func.clock_timestamp())).scalar_one()
        first = register(connection, session_id)
        after = connection.execute(select(func.clock_timestamp())).scalar_one()
        assert before <= first.created_at <= after
        assert first.created_at == first.last_heartbeat_at
        assert first.heartbeat_expires_at - first.created_at == timedelta(seconds=30)
        assert first.session.status is WorkerStatus.ACTIVE
        assert register(connection, session_id) == first
        with transaction(engine, registered_schema) as observer:
            assert count(observer) == 0
    with transaction(engine, registered_schema) as connection:
        assert register(connection, session_id) == first
        assert count(connection) == 1


@pytest.mark.parametrize("status", list(WorkerStatus))
def test_expired_or_terminal_replay_never_renews_or_reopens(
    engine: Engine, registered_schema: str, status: WorkerStatus
) -> None:
    session_id, stamp = uuid4(), datetime(2000, 1, 1, tzinfo=UTC)
    with transaction(engine, registered_schema) as connection:
        connection.execute(
            worker_sessions.insert().values(
                id=session_id,
                worker_name="worker",
                max_concurrency=2,
                status=status.value,
                created_at=stamp,
                last_heartbeat_at=stamp,
                heartbeat_expires_at=stamp + timedelta(seconds=30),
            )
        )
    with transaction(engine, registered_schema) as connection:
        original = dict(connection.execute(select(worker_sessions)).mappings().one())
        result = register(connection, session_id)
        assert result.session.status is status and result.created_at == stamp
        assert result.heartbeat_expires_at == stamp + timedelta(seconds=30)
        assert (
            dict(connection.execute(select(worker_sessions)).mappings().one())
            == original
        )


def test_replay_returns_latest_snapshot_and_ignores_changed_server_timeout(
    engine: Engine, registered_schema: str
) -> None:
    session_id = uuid4()
    with transaction(engine, registered_schema) as connection:
        first = register(connection, session_id)
        connection.execute(
            worker_sessions.update().values(
                last_heartbeat_at=first.created_at + timedelta(seconds=10),
                heartbeat_expires_at=first.created_at + timedelta(seconds=40),
            )
        )
    with transaction(engine, registered_schema) as connection:
        replay = WorkerRepository(connection, heartbeat_timeout_seconds=90).register(
            session_id, worker_name="worker", max_concurrency=2
        )
        assert replay.session == first.session
        assert replay.created_at == first.created_at
        assert replay.last_heartbeat_at == first.created_at + timedelta(seconds=10)
        assert replay.heartbeat_expires_at == first.created_at + timedelta(seconds=40)


@pytest.mark.parametrize("changed", [{"worker_name": "Worker"}, {"max_concurrency": 3}])
def test_registration_conflict_preserves_original(
    engine: Engine, registered_schema: str, changed: dict[str, object]
) -> None:
    session_id = uuid4()
    with transaction(engine, registered_schema) as connection:
        original = register(connection, session_id)
    data = {"worker_name": "worker", "max_concurrency": 2, **changed}
    with transaction(engine, registered_schema) as connection:
        with pytest.raises(WorkerRegistrationConflictError) as error:
            WorkerRepository(connection).register(session_id, **data)  # type: ignore[arg-type]
        assert str(session_id) not in str(error.value)
        assert register(connection, session_id) == original and count(connection) == 1


@pytest.mark.parametrize("different", [False, True])
def test_four_concurrent_registrants_commit_one_session(
    engine: Engine, registered_schema: str, different: bool
) -> None:
    barrier, session_id = Barrier(4, timeout=10), uuid4()

    def submit(index: int) -> StoredWorkerSession | None:
        try:
            with transaction(engine, registered_schema) as connection:
                repo = WorkerRepository(connection)
                barrier.wait()
                return repo.register(
                    session_id,
                    worker_name="worker",
                    max_concurrency=(index % 2 + 1) if different else 2,
                )
        except WorkerRegistrationConflictError:
            return None

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(submit, i) for i in range(4)]
        results = [future.result(timeout=20) for future in futures]
    accepted = [r for r in results if r is not None]
    assert len(accepted) == (2 if different else 4)
    assert all(r == accepted[0] for r in accepted)
    with transaction(engine, registered_schema) as connection:
        assert count(connection) == 1


def wait_for_block(engine: Engine, waiter: int, owner: int) -> None:
    deadline = monotonic() + 5
    with engine.connect() as observer:
        while monotonic() < deadline:
            if observer.execute(
                text("SELECT :owner = ANY(pg_blocking_pids(:waiter))"),
                {"owner": owner, "waiter": waiter},
            ).scalar_one():
                return
            sleep(0.01)
    raise AssertionError("Contender never blocked on the owning transaction")


@pytest.mark.parametrize("commit", [False, True])
def test_waiter_replays_commit_or_gets_fresh_clock_after_rollback(
    engine: Engine, registered_schema: str, commit: bool
) -> None:
    session_id, waiter_pid = uuid4(), Queue[int]()

    def contend() -> StoredWorkerSession:
        with transaction(engine, registered_schema) as connection:
            waiter_pid.put(
                connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
            )
            return register(connection, session_id)

    with engine.connect() as owner:
        tx = owner.begin()
        migration_config(owner, registered_schema)
        owner_pid = owner.execute(text("SELECT pg_backend_pid()")).scalar_one()
        provisional = register(owner, session_id)
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(contend)
            try:
                wait_for_block(engine, waiter_pid.get(timeout=5), owner_pid)
                release_time = owner.execute(
                    select(func.clock_timestamp())
                ).scalar_one()
                tx.commit() if commit else tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
            result = pending.result(timeout=10)
    if commit:
        assert result == provisional
    else:
        assert result.created_at >= release_time
        assert result.created_at > provisional.created_at
        assert result.heartbeat_expires_at - result.created_at == timedelta(seconds=30)


def test_timeout_propagates_and_registration_lock_releases(
    engine: Engine, registered_schema: str
) -> None:
    session_id = uuid4()
    with transaction(engine, registered_schema) as owner:
        first = register(owner, session_id)
        with pytest.raises(DBAPIError) as error:
            with transaction(engine, registered_schema) as contender:
                contender.execute(text("SET LOCAL lock_timeout = '100ms'"))
                register(contender, session_id)
        assert getattr(error.value.orig, "sqlstate", None) == "55P03"
    with transaction(engine, registered_schema) as connection:
        assert register(connection, session_id) == first


def test_distinct_sessions_do_not_share_a_global_registration_lock(
    engine: Engine, registered_schema: str
) -> None:
    first_id, second_id = UUID(int=1), UUID(int=2)
    assert worker_module._registration_lock_key(
        first_id
    ) != worker_module._registration_lock_key(second_id)
    with transaction(engine, registered_schema) as owner:
        first = register(owner, first_id)
        with transaction(engine, registered_schema) as contender:
            contender.execute(text("SET LOCAL lock_timeout = '100ms'"))
            second = register(contender, second_id)
        assert first.session.worker_name == second.session.worker_name
        assert first.session.id != second.session.id


def test_hash_collision_serializes_but_does_not_merge_identities(
    engine: Engine, registered_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module, "_registration_lock_key", lambda value: 7)
    with transaction(engine, registered_schema) as owner:
        first = register(owner, uuid4())
        with pytest.raises(DBAPIError):
            with transaction(engine, registered_schema) as contender:
                contender.execute(text("SET LOCAL lock_timeout = '100ms'"))
                register(contender, uuid4())
    with transaction(engine, registered_schema) as connection:
        second = register(connection, uuid4())
        assert first.session.id != second.session.id and count(connection) == 2


@pytest.mark.parametrize("phase", ["insert", "commit"])
def test_injected_failure_rolls_back_and_uuid_can_be_retried(
    engine: Engine, registered_schema: str, phase: str
) -> None:
    with transaction(engine, registered_schema) as connection:
        connection.execute(
            text("""
            CREATE FUNCTION reject_registration() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='injected'; END; $$
        """)
        )
        ddl = (
            "CREATE CONSTRAINT TRIGGER reject_registration "
            "AFTER INSERT ON worker_sessions "
            "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
            if phase == "commit"
            else "CREATE TRIGGER reject_registration BEFORE INSERT "
            "ON worker_sessions FOR EACH ROW "
        )
        connection.execute(text(ddl + "EXECUTE FUNCTION reject_registration()"))
    session_id, returned = uuid4(), False
    with pytest.raises(DBAPIError):
        with transaction(engine, registered_schema) as connection:
            register(connection, session_id)
            returned = True
    assert returned == (phase == "commit")
    with transaction(engine, registered_schema) as connection:
        assert count(connection) == 0
        connection.execute(text("DROP TRIGGER reject_registration ON worker_sessions"))
        register(connection, session_id)
    with transaction(engine, registered_schema) as connection:
        assert count(connection) == 1


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE", "AUTOCOMMIT"])
def test_unsupported_transaction_modes(engine: Engine, isolation: str) -> None:
    with engine.connect().execution_options(isolation_level=isolation) as connection:
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                WorkerRepository(connection)


def test_original_transaction_required_even_for_replay(
    engine: Engine, registered_schema: str
) -> None:
    with engine.connect() as connection:
        with pytest.raises(RepositoryTransactionError):
            WorkerRepository(connection)
        with connection.begin():
            migration_config(connection, registered_schema)
            repo, session_id = WorkerRepository(connection), uuid4()
            repo.register(session_id, worker_name="worker", max_concurrency=2)
        with pytest.raises(RepositoryTransactionError):
            repo.register(session_id, worker_name="worker", max_concurrency=2)
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                repo.register(session_id, worker_name="worker", max_concurrency=2)


@pytest.mark.parametrize("timeout", [True, 0, -1, 86401, "30", 1.5])
def test_invalid_timeout_policy(engine: Engine, timeout: object) -> None:
    with engine.connect() as connection:
        with pytest.raises(ValueError, match="Heartbeat timeout"):
            WorkerRepository(connection, heartbeat_timeout_seconds=timeout)  # type: ignore[arg-type]


@pytest.mark.parametrize("timeout", [1, 86400])
def test_timeout_policy_boundaries(
    engine: Engine, registered_schema: str, timeout: int
) -> None:
    with transaction(engine, registered_schema) as connection:
        result = WorkerRepository(
            connection, heartbeat_timeout_seconds=timeout
        ).register(uuid4(), worker_name="worker", max_concurrency=2)
        assert result.heartbeat_expires_at - result.created_at == timedelta(
            seconds=timeout
        )


@pytest.mark.parametrize(
    "bad",
    [
        {"session_id": "bad-uuid"},
        {"worker_name": "bad name"},
        {"max_concurrency": True},
        {"max_concurrency": 0},
    ],
)
def test_invalid_request_is_rejected_before_lock_or_write(
    engine: Engine, registered_schema: str, bad: dict[str, object]
) -> None:
    with transaction(engine, registered_schema) as connection:
        repo = WorkerRepository(connection)
        statements: list[str] = []

        def before_execute(
            connection: object,
            cursor: object,
            statement: str,
            parameters: object,
            context: object,
            executemany: object,
        ) -> None:
            statements.append(statement)

        event.listen(connection, "before_cursor_execute", before_execute)
        try:
            data = {
                "session_id": uuid4(),
                "worker_name": "worker",
                "max_concurrency": 2,
                **bad,
            }
            with pytest.raises((TypeError, ValidationError)):
                repo.register(**data)  # type: ignore[arg-type]
            assert not statements
        finally:
            event.remove(connection, "before_cursor_execute", before_execute)


def test_corrupt_storage_is_not_reported_as_successful_registration(
    engine: Engine, registered_schema: str
) -> None:
    session_id, stamp = uuid4(), datetime(2000, 1, 1, tzinfo=UTC)
    with transaction(engine, registered_schema) as connection:
        connection.execute(
            text(
                "ALTER TABLE worker_sessions "
                "DROP CONSTRAINT ck_worker_sessions_status_values"
            )
        )
        connection.execute(
            worker_sessions.insert().values(
                id=session_id,
                worker_name="worker",
                max_concurrency=2,
                status="PRIVATE_BAD",
                created_at=stamp,
                last_heartbeat_at=stamp,
                heartbeat_expires_at=stamp + timedelta(seconds=30),
            )
        )
    with transaction(engine, registered_schema) as connection:
        with pytest.raises(StoredWorkerError) as error:
            register(connection, session_id)
        assert "PRIVATE_BAD" not in str(error.value)
