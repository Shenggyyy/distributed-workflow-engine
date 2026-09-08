"""Heartbeat/expiry boundaries and transaction races on migrated PostgreSQL."""

from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from queue import Queue
from threading import Barrier, Event
from time import monotonic, sleep
from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, func, select, text
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.worker import WorkerStatus
from workflow_engine.repositories.workers import (
    StoredWorkerError,
    StoredWorkerSession,
    WorkerClockRegressionError,
    WorkerRepository,
    WorkerSessionExpiredError,
    WorkerSessionInactiveError,
    WorkerSessionNotFoundError,
)
from workflow_engine.repositories.workflows import RepositoryTransactionError
from workflow_engine.schema import worker_sessions

pytestmark = pytest.mark.integration
STAMP = datetime(2000, 1, 1, tzinfo=UTC)


@contextmanager
def transaction(engine: Engine, schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        migration_config(connection, schema)
        yield connection


@pytest.fixture
def heartbeat_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


class ClockedRepository(WorkerRepository):
    """Control only the clock seam; all locks, reads, writes and commits are real."""

    def __init__(
        self,
        connection: Connection,
        seconds: float,
        *,
        timeout: int = 30,
        clock_read: Event | None = None,
    ) -> None:
        super().__init__(connection, heartbeat_timeout_seconds=timeout)
        self.observation = STAMP + timedelta(seconds=seconds)
        self.clock_read = clock_read

    def _database_now(self) -> datetime:
        if self.clock_read is not None:
            self.clock_read.set()
        return self.observation


def seed(connection: Connection, status: str = "ACTIVE") -> UUID:
    session_id = uuid4()
    connection.execute(
        worker_sessions.insert().values(
            id=session_id,
            worker_name="worker",
            max_concurrency=2,
            status=status,
            created_at=STAMP,
            last_heartbeat_at=STAMP,
            heartbeat_expires_at=STAMP + timedelta(seconds=30),
        )
    )
    return session_id


def action(repo: WorkerRepository, name: str) -> Callable[[UUID], StoredWorkerSession]:
    return repo.heartbeat if name == "heartbeat" else repo.expire


@pytest.mark.parametrize("seconds", [0, 10, 29.999999])
def test_live_heartbeat_preserves_identity_and_advances_times(
    engine: Engine, heartbeat_schema: str, seconds: float
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection)
        result = ClockedRepository(connection, seconds).heartbeat(session_id)
        assert (
            result.session.id == session_id
            and result.session.status is WorkerStatus.ACTIVE
        )
        assert (
            result.session.worker_name == "worker"
            and result.session.max_concurrency == 2
        )
        assert result.created_at == STAMP
        assert result.last_heartbeat_at == STAMP + timedelta(seconds=seconds)
        assert result.heartbeat_expires_at == STAMP + timedelta(seconds=seconds + 30)
        # A repeated observation is a permitted SQL no-op, not a second lifecycle event.
        assert ClockedRepository(connection, seconds).heartbeat(session_id) == result


@pytest.mark.parametrize("seconds", [30, 30.000001, 60])
def test_deadline_is_exclusive_and_rejected_heartbeat_writes_nothing(
    engine: Engine, heartbeat_schema: str, seconds: float
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection)
        original = connection.execute(select(worker_sessions)).one()
        with pytest.raises(WorkerSessionExpiredError):
            ClockedRepository(connection, seconds).heartbeat(session_id)
        assert connection.execute(select(worker_sessions)).one() == original


@pytest.mark.parametrize("status", ["LOST", "STOPPED"])
def test_terminal_heartbeat_is_rejected_and_expire_is_repeatable(
    engine: Engine, heartbeat_schema: str, status: str
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection, status)
        original = connection.execute(select(worker_sessions)).one()
        repo = ClockedRepository(connection, 100)
        with pytest.raises(WorkerSessionInactiveError):
            repo.heartbeat(session_id)
        assert repo.expire(session_id) == repo.expire(session_id)
        assert connection.execute(select(worker_sessions)).one() == original


@pytest.mark.parametrize("seconds", [29.999999, 30, 30.000001])
def test_expiry_boundary_preserves_all_timestamps(
    engine: Engine, heartbeat_schema: str, seconds: float
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection)
        result = ClockedRepository(connection, seconds).expire(session_id)
        assert result.session.status is (
            WorkerStatus.ACTIVE if seconds < 30 else WorkerStatus.LOST
        )
        assert result.created_at == result.last_heartbeat_at == STAMP
        assert result.heartbeat_expires_at == STAMP + timedelta(seconds=30)


def test_shorter_policy_never_shortens_existing_deadline(
    engine: Engine, heartbeat_schema: str
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection)
        first = ClockedRepository(connection, 10, timeout=1).heartbeat(session_id)
        assert first.last_heartbeat_at == STAMP + timedelta(seconds=10)
        assert first.heartbeat_expires_at == STAMP + timedelta(seconds=30)
        later = ClockedRepository(connection, 29.5, timeout=1).heartbeat(session_id)
        assert later.heartbeat_expires_at == STAMP + timedelta(seconds=30.5)


@pytest.mark.parametrize("name", ["heartbeat", "expire"])
def test_clock_regression_is_explicit_without_mutation(
    engine: Engine, heartbeat_schema: str, name: str
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection)
        original = connection.execute(select(worker_sessions)).one()
        with pytest.raises(WorkerClockRegressionError):
            action(ClockedRepository(connection, -1), name)(session_id)
        assert connection.execute(select(worker_sessions)).one() == original


@pytest.mark.parametrize("name", ["heartbeat", "expire"])
def test_missing_invalid_and_expired_transaction(
    engine: Engine, heartbeat_schema: str, name: str
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        repo = ClockedRepository(connection, 10)
        with pytest.raises(WorkerSessionNotFoundError):
            action(repo, name)(uuid4())
        with pytest.raises(TypeError):
            action(repo, name)("bad")  # type: ignore[arg-type]
    with pytest.raises(RepositoryTransactionError):
        action(repo, name)(uuid4())
    with engine.connect() as connection:
        with connection.begin():
            later_repo = WorkerRepository(connection)
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                action(later_repo, name)(uuid4())


@pytest.mark.parametrize("name", ["heartbeat", "expire"])
def test_caller_rollback_restores_original_session(
    engine: Engine, heartbeat_schema: str, name: str
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection)
    with pytest.raises(RuntimeError, match="abort"):
        with transaction(engine, heartbeat_schema) as connection:
            result = action(
                ClockedRepository(connection, 10 if name == "heartbeat" else 30), name
            )(session_id)
            assert result.session.id == session_id
            raise RuntimeError("abort")
    with transaction(engine, heartbeat_schema) as connection:
        row = connection.execute(select(worker_sessions)).one()
        assert row.status == "ACTIVE" and row.last_heartbeat_at == STAMP
        assert row.heartbeat_expires_at == STAMP + timedelta(seconds=30)


@pytest.mark.parametrize("name", ["heartbeat", "expire"])
def test_deferred_commit_failure_is_not_success(
    engine: Engine, heartbeat_schema: str, name: str
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection)
        connection.execute(
            text("""
            CREATE FUNCTION reject_liveness() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='injected'; END; $$
        """)
        )
        connection.execute(
            text("""
            CREATE CONSTRAINT TRIGGER reject_liveness AFTER UPDATE ON worker_sessions
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            EXECUTE FUNCTION reject_liveness()
        """)
        )
    returned = False
    with pytest.raises(DBAPIError):
        with transaction(engine, heartbeat_schema) as connection:
            action(
                ClockedRepository(connection, 10 if name == "heartbeat" else 30), name
            )(session_id)
            returned = True
    assert returned
    with transaction(engine, heartbeat_schema) as connection:
        row = connection.execute(select(worker_sessions)).one()
        assert row.status == "ACTIVE" and row.last_heartbeat_at == STAMP
        connection.execute(text("DROP TRIGGER reject_liveness ON worker_sessions"))
        action(ClockedRepository(connection, 10 if name == "heartbeat" else 30), name)(
            session_id
        )


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
    raise AssertionError("Contender never blocked")


@pytest.mark.parametrize("winner", ["heartbeat", "expire"])
@pytest.mark.parametrize("commit", [True, False])
def test_heartbeat_expiry_race_rechecks_locked_state_and_samples_after_wait(
    engine: Engine, heartbeat_schema: str, winner: str, commit: bool
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection)
    waiter_pid, clock_read = Queue[int](), Event()

    def contend() -> StoredWorkerSession | str:
        with transaction(engine, heartbeat_schema) as connection:
            waiter_pid.put(
                connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
            )
            repo = ClockedRepository(connection, 31, clock_read=clock_read)
            try:
                return action(repo, "expire" if winner == "heartbeat" else "heartbeat")(
                    session_id
                )
            except WorkerSessionInactiveError:
                return "inactive"
            except WorkerSessionExpiredError:
                return "expired"

    with engine.connect() as owner:
        tx = owner.begin()
        migration_config(owner, heartbeat_schema)
        owner_pid = owner.execute(text("SELECT pg_backend_pid()")).scalar_one()
        action(ClockedRepository(owner, 29 if winner == "heartbeat" else 30), winner)(
            session_id
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(contend)
            try:
                wait_for_block(engine, waiter_pid.get(timeout=5), owner_pid)
                assert not clock_read.is_set()
                tx.commit() if commit else tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
            result = pending.result(timeout=10)
    if winner == "heartbeat":
        assert isinstance(result, StoredWorkerSession) and clock_read.is_set()
        assert result.session.status is (
            WorkerStatus.ACTIVE if commit else WorkerStatus.LOST
        )
        assert result.heartbeat_expires_at == STAMP + timedelta(
            seconds=59 if commit else 30
        )
    else:
        assert result == ("inactive" if commit else "expired")
        assert clock_read.is_set() == (not commit)


@pytest.mark.parametrize("name", ["heartbeat", "expire"])
def test_timeout_propagates_without_hidden_retry(
    engine: Engine, heartbeat_schema: str, name: str
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection)
    with transaction(engine, heartbeat_schema) as owner:
        owner.execute(select(worker_sessions).with_for_update()).all()
        with pytest.raises(DBAPIError) as error:
            with transaction(engine, heartbeat_schema) as contender:
                contender.execute(text("SET LOCAL lock_timeout = '100ms'"))
                action(ClockedRepository(contender, 31), name)(session_id)
        assert getattr(error.value.orig, "sqlstate", None) == "55P03"
    with transaction(engine, heartbeat_schema) as connection:
        assert (
            connection.execute(select(worker_sessions.c.status)).scalar_one()
            == "ACTIVE"
        )


def test_native_database_clock_and_concurrent_heartbeats(
    engine: Engine, heartbeat_schema: str
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        before = connection.execute(select(func.clock_timestamp())).scalar_one()
        registered = WorkerRepository(connection).register(
            uuid4(), worker_name="live", max_concurrency=2
        )
    barrier = Barrier(4, timeout=10)

    def renew() -> StoredWorkerSession:
        with transaction(engine, heartbeat_schema) as connection:
            barrier.wait()
            return WorkerRepository(connection).heartbeat(registered.session.id)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(renew) for _ in range(4)]
        results = [f.result(timeout=20) for f in futures]
    with transaction(engine, heartbeat_schema) as connection:
        after = connection.execute(select(func.clock_timestamp())).scalar_one()
        row = connection.execute(select(worker_sessions)).one()
        assert row.last_heartbeat_at == max(r.last_heartbeat_at for r in results)
        assert row.heartbeat_expires_at == max(r.heartbeat_expires_at for r in results)
        assert all(before <= r.last_heartbeat_at <= after for r in results)
        assert all(
            r.heartbeat_expires_at - r.last_heartbeat_at == timedelta(seconds=30)
            for r in results
        )
        assert row.id == registered.session.id and row.status == "ACTIVE"


def test_concurrent_expirers_return_same_terminal_snapshot(
    engine: Engine, heartbeat_schema: str
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        session_id = seed(connection)
    barrier = Barrier(4, timeout=10)

    def expire() -> StoredWorkerSession:
        with transaction(engine, heartbeat_schema) as connection:
            barrier.wait()
            return ClockedRepository(connection, 30).expire(session_id)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(expire) for _ in range(4)]
        results = [f.result(timeout=20) for f in futures]
    assert all(r == results[0] for r in results)
    assert results[0].session.status is WorkerStatus.LOST


@pytest.mark.parametrize("name", ["heartbeat", "expire"])
def test_corrupt_session_is_rejected_without_overwriting_it(
    engine: Engine, heartbeat_schema: str, name: str
) -> None:
    with transaction(engine, heartbeat_schema) as connection:
        connection.execute(
            text(
                "ALTER TABLE worker_sessions "
                "DROP CONSTRAINT ck_worker_sessions_status_values"
            )
        )
        session_id = seed(connection, "PRIVATE_BAD")
    with transaction(engine, heartbeat_schema) as connection:
        with pytest.raises(StoredWorkerError) as error:
            action(ClockedRepository(connection, 30), name)(session_id)
        assert "PRIVATE_BAD" not in str(error.value)
        assert (
            connection.execute(select(worker_sessions.c.status)).scalar_one()
            == "PRIVATE_BAD"
        )
