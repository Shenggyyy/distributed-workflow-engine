"""Idempotent allocation, current replay, uncertain outcomes and ordered races."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import Barrier, Event
from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, event, func, select, text
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.lease import LeaseClockRegressionError
from workflow_engine.domain.retry import ExecutionPolicy
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.claim_requests import (
    _CLAIM_LOCK_NAMESPACE,
    ClaimReplayUnavailableError,
    ClaimRequestConflictError,
    ClaimRequestRepository,
    StoredClaimRequestError,
    _claim_lock_key,
)
from workflow_engine.repositories.claims import ClaimRunNotFoundError, TaskClaim
from workflow_engine.repositories.leases import LeaseRepository
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workers import (
    WorkerRepository,
    WorkerSessionNotFoundError,
)
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    WorkflowRepository,
)
from workflow_engine.schema import (
    attempt_leases,
    claim_requests,
    task_attempts,
    task_runs,
    worker_sessions,
    workflow_runs,
)

pytestmark = pytest.mark.integration


@contextmanager
def transaction(engine: Engine, schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        migration_config(connection, schema)
        yield connection


@pytest.fixture
def request_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


def seed(
    connection: Connection, *, roots: int = 2, capacity: int = 2
) -> tuple[UUID, UUID]:
    version = WorkflowRepository(connection).publish(
        WorkflowDefinition(
            name="keyed_" + uuid4().hex,
            schema_version=2,
            tasks=tuple(
                TaskDefinition(
                    task_id=chr(65 + i),
                    task_type="demo.echo",
                    execution=ExecutionPolicy(timeout_seconds=3600),
                )
                for i in range(roots)
            ),
        )
    )
    run = RunRepository(connection).create(version.id).run
    worker = WorkerRepository(connection, heartbeat_timeout_seconds=300).register(
        uuid4(),
        worker_name="worker",
        max_concurrency=capacity,
    )
    return run.id, worker.session.id


@pytest.fixture
def ids(engine: Engine, request_schema: str) -> tuple[UUID, UUID, UUID]:
    with transaction(engine, request_schema) as connection:
        run, worker = seed(connection)
    return run, worker, uuid4()


def poll(engine: Engine, schema: str, ids: tuple[UUID, UUID, UUID]) -> TaskClaim | None:
    with transaction(engine, schema) as connection:
        return ClaimRequestRepository(connection, lease_seconds=600).claim_next(
            ids[0], ids[1], request_id=ids[2]
        )


class ClockedRequests(ClaimRequestRepository):
    def __init__(
        self, connection: Connection, observed: datetime, sampled: Event | None = None
    ) -> None:
        super().__init__(connection)
        self.observed, self.sampled = observed, sampled

    def _database_now(self) -> datetime:
        if self.sampled:
            self.sampled.set()
        return self.observed


def test_lost_response_replays_original_allocation_without_writes(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID]
) -> None:
    with pytest.raises(ConnectionError):
        first = poll(engine, request_schema, ids)
        assert first is not None
        # Transport failure after COMMIT: a new client transaction retries the key.
        raise ConnectionError("simulated response loss")
    assert first is not None
    with transaction(engine, request_schema) as connection:
        original = WorkflowRepository(connection).get_version(first.workflow_version_id)
        assert original is not None
        WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name=original.definition.name,
                tasks=(TaskDefinition(task_id="A", task_type="new.handler"),),
            )
        )
        tables = (
            workflow_runs,
            worker_sessions,
            task_runs,
            task_attempts,
            attempt_leases,
            claim_requests,
        )
        before = {
            t.name: connection.execute(select(t).order_by(*t.primary_key.columns)).all()
            for t in tables
        }
    for _ in range(2):
        with transaction(engine, request_schema) as connection:
            replay = ClaimRequestRepository(connection, lease_seconds=1).claim_next(
                ids[0], ids[1], request_id=ids[2]
            )
        assert replay == first
    assert str(first.lease.lease_token) not in repr(replay)
    with transaction(engine, request_schema) as connection:
        for table in tables:
            assert (
                connection.execute(
                    select(table).order_by(*table.primary_key.columns)
                ).all()
                == before[table.name]
            )


@pytest.mark.parametrize("capacity_blocked", [False, True])
def test_no_work_is_sticky_after_work_appears(
    engine: Engine, request_schema: str, capacity_blocked: bool
) -> None:
    with transaction(engine, request_schema) as connection:
        run, worker = seed(
            connection,
            roots=2 if capacity_blocked else 1,
            capacity=1 if capacity_blocked else 2,
        )
    held = poll(engine, request_schema, (run, worker, uuid4()))
    assert held is not None
    key = uuid4()
    assert poll(engine, request_schema, (run, worker, key)) is None
    with transaction(engine, request_schema) as connection:
        connection.execute(task_attempts.update().values(status="LOST"))
        connection.execute(
            task_runs.update()
            .where(task_runs.c.id == held.task.id)
            .values(status="RETRY_WAIT")
        )
        connection.execute(
            task_runs.update()
            .where(task_runs.c.id == held.task.id)
            .values(status="READY")
        )
    assert poll(engine, request_schema, (run, worker, key)) is None
    fresh = poll(engine, request_schema, (run, worker, uuid4()))
    assert fresh is not None and fresh.attempt.id != held.attempt.id
    with transaction(engine, request_schema) as connection:
        connection.execute(worker_sessions.update().values(status="LOST"))
        connection.execute(workflow_runs.update().values(status="FAILED"))
    assert poll(engine, request_schema, (run, worker, key)) is None
    with pytest.raises(ClaimRequestConflictError):
        poll(engine, request_schema, (uuid4(), worker, key))


def test_new_request_and_new_session_have_distinct_allocations(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID]
) -> None:
    first = poll(engine, request_schema, ids)
    second = poll(engine, request_schema, (ids[0], ids[1], uuid4()))
    assert first is not None and second is not None and first.task.id != second.task.id
    # Replay bypasses new-claim capacity admission even when both slots are full.
    assert poll(engine, request_schema, ids) == first
    with transaction(engine, request_schema) as connection:
        other_run, other_worker = seed(connection)
    third = poll(engine, request_schema, (other_run, other_worker, ids[2]))
    assert third is not None and third.lease.worker_session_id == other_worker
    assert third.attempt.id not in (first.attempt.id, second.attempt.id)


def test_concurrent_duplicate_claims_allocate_once(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID]
) -> None:
    barrier = Barrier(4, timeout=10)

    def submit() -> TaskClaim | None:
        barrier.wait()
        return poll(engine, request_schema, ids)

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = [
            f.result(timeout=15) for f in [executor.submit(submit) for _ in range(4)]
        ]
    assert results[0] is not None and all(r == results[0] for r in results)
    with transaction(engine, request_schema) as connection:
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


def test_hash_collisions_serialize_but_never_alias_requests(
    engine: Engine,
    request_schema: str,
    ids: tuple[UUID, UUID, UUID],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "workflow_engine.repositories.claim_requests._claim_lock_key",
        lambda worker, request: 42,
    )
    barrier = Barrier(2, timeout=10)

    def submit(key: UUID) -> TaskClaim | None:
        barrier.wait()
        return poll(engine, request_schema, (ids[0], ids[1], key))

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(submit, key) for key in (ids[2], uuid4())]
        results = [f.result(timeout=15) for f in futures]
    assert all(r is not None for r in results)
    assert len({r.attempt.id for r in results if r is not None}) == 2


def test_concurrent_same_key_different_run_has_one_winner(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID]
) -> None:
    with transaction(engine, request_schema) as connection:
        other_run, _ = seed(connection)
    barrier = Barrier(2, timeout=10)

    def submit(run: UUID) -> str:
        barrier.wait()
        try:
            result = poll(engine, request_schema, (run, ids[1], ids[2]))
            assert result is not None
            return "claimed"
        except ClaimRequestConflictError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        pending = [executor.submit(submit, run) for run in (ids[0], other_run)]
        assert sorted(f.result(timeout=15) for f in pending) == ["claimed", "conflict"]
    with transaction(engine, request_schema) as connection:
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


@pytest.mark.parametrize("commit", [False, True])
def test_waiting_duplicate_after_owner_commit_or_abort(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID], commit: bool
) -> None:
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

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as owner:
            tx = owner.begin()
            try:
                migration_config(owner, request_schema)
                first = ClaimRequestRepository(owner, lease_seconds=600).claim_next(
                    ids[0], ids[1], request_id=ids[2]
                )
                assert first is not None
                with transaction(engine, request_schema) as observer:
                    for table in (claim_requests, task_attempts, attempt_leases):
                        assert (
                            observer.execute(
                                select(func.count()).select_from(table)
                            ).scalar_one()
                            == 0
                        )
                event.listen(engine, "before_cursor_execute", before_execute)
                try:
                    pending = executor.submit(poll, engine, request_schema, ids)
                    assert entered.wait(timeout=5) and not pending.done()
                finally:
                    event.remove(engine, "before_cursor_execute", before_execute)
                tx.commit() if commit else tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
        result = pending.result(timeout=10)
    assert result is not None
    assert (result.attempt.id == first.attempt.id) is commit
    assert result.attempt.attempt_number == 1
    with transaction(engine, request_schema) as connection:
        assert (
            connection.execute(select(claim_requests.c.attempt_id)).scalar_one()
            == result.attempt.id
        )


@pytest.mark.parametrize("offset", [0, 599.999999, 600, 600.000001, -0.000001])
def test_replay_deadline_and_clock_boundaries(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID], offset: float
) -> None:
    first = poll(engine, request_schema, ids)
    assert first is not None
    observed = first.lease.acquired_at + timedelta(seconds=offset)
    with transaction(engine, request_schema) as connection:
        if 0 <= offset < 600:
            assert (
                ClockedRequests(connection, observed).claim_next(
                    ids[0], ids[1], request_id=ids[2]
                )
                == first
            )
        else:
            with pytest.raises(
                LeaseClockRegressionError if offset < 0 else ClaimReplayUnavailableError
            ):
                ClockedRequests(connection, observed).claim_next(
                    ids[0], ids[1], request_id=ids[2]
                )
        assert (
            connection.execute(select(attempt_leases.c.lease_expires_at)).scalar_one()
            == first.lease.lease_expires_at
        )
        assert (
            connection.execute(
                select(func.count()).select_from(task_attempts)
            ).scalar_one()
            == 1
        )


@pytest.mark.parametrize(
    "table,status",
    [
        (workflow_runs, "FAILED"),
        (workflow_runs, "SUCCEEDED"),
        (task_runs, "SUCCEEDED"),
        (task_runs, "FAILED"),
        (task_runs, "RETRY_WAIT"),
        (task_attempts, "SUCCEEDED"),
        (task_attempts, "FAILED"),
        (task_attempts, "LOST"),
        (task_attempts, "TIMED_OUT"),
        (worker_sessions, "STOPPED"),
    ],
)
def test_inactive_execution_never_reallocates(
    engine: Engine,
    request_schema: str,
    ids: tuple[UUID, UUID, UUID],
    table: object,
    status: str,
) -> None:
    from sqlalchemy import Table

    assert isinstance(table, Table)
    first = poll(engine, request_schema, ids)
    assert first is not None
    with transaction(engine, request_schema) as connection:
        target = (
            first.task.id
            if table is task_runs
            else first.attempt.id
            if table is task_attempts
            else ids[1]
            if table is worker_sessions
            else ids[0]
        )
        connection.execute(
            table.update().where(table.c.id == target).values(status=status)
        )
    with pytest.raises(ClaimReplayUnavailableError) as error:
        poll(engine, request_schema, ids)
    assert str(first.lease.lease_token) not in str(error.value)
    with pytest.raises(ClaimRequestConflictError):
        poll(engine, request_schema, (uuid4(), ids[1], ids[2]))
    with transaction(engine, request_schema) as connection:
        assert (
            connection.execute(select(task_attempts.c.id)).scalar_one()
            == first.attempt.id
        )


@pytest.mark.parametrize("lost", [False, True])
def test_live_lease_can_replay_after_heartbeat_expiry(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID], lost: bool
) -> None:
    first = poll(engine, request_schema, ids)
    assert first is not None
    with transaction(engine, request_schema) as connection:
        if lost:
            connection.execute(worker_sessions.update().values(status="LOST"))
        deadline = connection.execute(
            select(worker_sessions.c.heartbeat_expires_at)
        ).scalar_one()
        before = connection.execute(select(worker_sessions)).one()
        assert (
            ClockedRequests(connection, deadline + timedelta(seconds=1)).claim_next(
                ids[0], ids[1], request_id=ids[2]
            )
            == first
        )
        assert connection.execute(select(worker_sessions)).one() == before


def test_replay_reads_renewal_without_extending_it(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID]
) -> None:
    first = poll(engine, request_schema, ids)
    assert first is not None
    with transaction(engine, request_schema) as connection:
        renewed = LeaseRepository(connection, lease_seconds=900).renew(
            first.attempt.id,
            worker_session_id=ids[1],
            lease_token=first.lease.lease_token,
        )
    replay = poll(engine, request_schema, ids)
    assert replay is not None and replay.lease == renewed
    assert (
        replay.attempt == first.attempt
        and replay.lease.lease_expires_at > first.lease.lease_expires_at
    )


def test_old_binding_does_not_borrow_retry_attempt(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID]
) -> None:
    first = poll(engine, request_schema, ids)
    assert first is not None
    with transaction(engine, request_schema) as connection:
        connection.execute(task_attempts.update().values(status="LOST"))
        connection.execute(
            task_runs.update()
            .where(task_runs.c.id == first.task.id)
            .values(status="RETRY_WAIT")
        )
        connection.execute(
            task_runs.update()
            .where(task_runs.c.id == first.task.id)
            .values(status="READY")
        )
    replacement = poll(engine, request_schema, (ids[0], ids[1], uuid4()))
    assert replacement is not None and replacement.attempt.attempt_number == 2
    assert replacement.lease.lease_token != first.lease.lease_token
    with pytest.raises(ClaimReplayUnavailableError):
        poll(engine, request_schema, ids)


def test_cross_run_binding_is_corruption_not_authority(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID]
) -> None:
    first = poll(engine, request_schema, ids)
    assert first is not None
    with transaction(engine, request_schema) as connection:
        other_run, _ = seed(connection)
        # Test-only bypass: schema intentionally leaves Run membership to readers.
        connection.exec_driver_sql(
            "ALTER TABLE claim_requests DISABLE TRIGGER claim_requests_append_only"
        )
        connection.execute(claim_requests.update().values(run_id=other_run))
    with pytest.raises(StoredClaimRequestError):
        poll(engine, request_schema, (other_run, ids[1], ids[2]))


@pytest.mark.parametrize("phase", ["write", "commit", "abort"])
@pytest.mark.parametrize("no_work", [False, True])
def test_failed_binding_rolls_back_whole_allocation(
    engine: Engine,
    request_schema: str,
    ids: tuple[UUID, UUID, UUID],
    phase: str,
    no_work: bool,
) -> None:
    if no_work:
        with transaction(engine, request_schema) as connection:
            # Simulate a Run awaiting dependency resolution rather than READY work.
            connection.execute(task_runs.update().values(status="RUNNING"))
    if phase != "abort":
        with transaction(engine, request_schema) as connection:
            connection.exec_driver_sql("""
                CREATE FUNCTION reject_binding() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='23514',
                MESSAGE='injected binding failure'; END; $$""")
            kind = (
                "CONSTRAINT TRIGGER reject_binding AFTER"
                if phase == "commit"
                else "TRIGGER reject_binding BEFORE"
            )
            deferred = "DEFERRABLE INITIALLY DEFERRED" if phase == "commit" else ""
            connection.exec_driver_sql(
                f"CREATE {kind} INSERT ON claim_requests {deferred} "
                "FOR EACH ROW EXECUTE FUNCTION reject_binding()"
            )
    with pytest.raises(RuntimeError if phase == "abort" else DBAPIError):
        with transaction(engine, request_schema) as connection:
            result = ClaimRequestRepository(connection).claim_next(
                ids[0], ids[1], request_id=ids[2]
            )
            assert (result is None) is no_work
            if phase == "abort":
                raise RuntimeError("abort")
    with transaction(engine, request_schema) as connection:
        for table in (claim_requests, task_attempts, attempt_leases):
            assert (
                connection.execute(select(func.count()).select_from(table)).scalar_one()
                == 0
            )
        assert set(connection.execute(select(task_runs.c.status)).scalars()) == {
            "RUNNING" if no_work else "READY"
        }
        if phase != "abort":
            connection.exec_driver_sql("DROP TRIGGER reject_binding ON claim_requests")
    assert (poll(engine, request_schema, ids) is None) is no_work


@pytest.mark.parametrize("missing", ["run", "worker"])
def test_admission_errors_do_not_store_no_work(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID], missing: str
) -> None:
    bad = (uuid4(), ids[1], ids[2]) if missing == "run" else (ids[0], uuid4(), ids[2])
    with pytest.raises(
        ClaimRunNotFoundError if missing == "run" else WorkerSessionNotFoundError
    ):
        poll(engine, request_schema, bad)
    with transaction(engine, request_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(claim_requests)
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize(
    "target",
    [
        "request",
        "workflow_runs",
        "worker_sessions",
        "task_runs",
        "task_attempts",
        "attempt_leases",
    ],
)
def test_replay_samples_clock_only_after_all_locks(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID], target: str
) -> None:
    first = poll(engine, request_schema, ids)
    assert first is not None
    entered, sampled = Event(), Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if (target == "request" and "pg_advisory_xact_lock" in statement) or (
            f"FROM {target}" in statement and "FOR UPDATE" in statement
        ):
            entered.set()

    def replay() -> None:
        with pytest.raises(ClaimReplayUnavailableError):
            with transaction(engine, request_schema) as connection:
                ClockedRequests(
                    connection, first.lease.lease_expires_at, sampled
                ).claim_next(ids[0], ids[1], request_id=ids[2])

    with ThreadPoolExecutor(max_workers=1) as executor:
        with transaction(engine, request_schema) as blocker:
            if target == "request":
                blocker.execute(
                    text("SELECT pg_advisory_xact_lock(:ns, :key)"),
                    {
                        "ns": _CLAIM_LOCK_NAMESPACE,
                        "key": _claim_lock_key(ids[1], ids[2]),
                    },
                )
            else:
                blocker.exec_driver_sql(f"SELECT * FROM {target} FOR UPDATE").all()
            event.listen(engine, "before_cursor_execute", before_execute)
            try:
                pending = executor.submit(replay)
                assert entered.wait(timeout=5)
                assert not sampled.is_set() and not pending.done()
            finally:
                event.remove(engine, "before_cursor_execute", before_execute)
        pending.result(timeout=10)
    assert sampled.is_set()


@pytest.mark.parametrize("commit", [False, True])
@pytest.mark.parametrize("terminal", [False, True])
def test_replay_rechecks_predecessor_renewal_or_terminal_commit(
    engine: Engine,
    request_schema: str,
    ids: tuple[UUID, UUID, UUID],
    commit: bool,
    terminal: bool,
) -> None:
    first = poll(engine, request_schema, ids)
    assert first is not None
    observed = first.lease.lease_expires_at + timedelta(seconds=1)
    entered = Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if "FROM workflow_runs" in statement and "FOR UPDATE" in statement:
            entered.set()

    def replay() -> TaskClaim | None:
        with transaction(engine, request_schema) as connection:
            return ClockedRequests(connection, observed).claim_next(
                ids[0], ids[1], request_id=ids[2]
            )

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as owner:
            tx = owner.begin()
            try:
                migration_config(owner, request_schema)
                renewed = LeaseRepository(owner, lease_seconds=900).renew(
                    first.attempt.id,
                    worker_session_id=ids[1],
                    lease_token=first.lease.lease_token,
                )
                if terminal:
                    owner.execute(task_attempts.update().values(status="LOST"))
                event.listen(engine, "before_cursor_execute", before_execute)
                try:
                    pending = executor.submit(replay)
                    assert entered.wait(timeout=5) and not pending.done()
                finally:
                    event.remove(engine, "before_cursor_execute", before_execute)
                tx.commit() if commit else tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
        if commit and not terminal:
            replayed = pending.result(timeout=10)
            assert replayed is not None and replayed.lease == renewed
        else:
            with pytest.raises(ClaimReplayUnavailableError):
                pending.result(timeout=10)


@pytest.mark.parametrize("target", ["request", "run"])
def test_lock_timeout_releases_request_lock_and_leaves_no_claim(
    engine: Engine, request_schema: str, ids: tuple[UUID, UUID, UUID], target: str
) -> None:
    lock = {"ns": _CLAIM_LOCK_NAMESPACE, "key": _claim_lock_key(ids[1], ids[2])}
    with transaction(engine, request_schema) as blocker:
        if target == "request":
            blocker.execute(text("SELECT pg_advisory_xact_lock(:ns, :key)"), lock)
        else:
            blocker.execute(select(workflow_runs).with_for_update()).all()
        with pytest.raises(DBAPIError) as error:
            with transaction(engine, request_schema) as connection:
                connection.exec_driver_sql("SET LOCAL lock_timeout = '100ms'")
                ClaimRequestRepository(connection).claim_next(
                    ids[0], ids[1], request_id=ids[2]
                )
        assert getattr(error.value.orig, "sqlstate", None) == "55P03"
    with transaction(engine, request_schema) as connection:
        assert connection.execute(
            text("SELECT pg_try_advisory_xact_lock(:ns, :key)"), lock
        ).scalar_one()
        assert (
            connection.execute(
                select(func.count()).select_from(claim_requests)
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                select(func.count()).select_from(task_attempts)
            ).scalar_one()
            == 0
        )
    assert poll(engine, request_schema, ids) is not None


def test_repository_transaction_lifetime(engine: Engine, request_schema: str) -> None:
    with engine.connect() as connection:
        with pytest.raises(RepositoryTransactionError):
            ClaimRequestRepository(connection)
        with connection.begin():
            repo = ClaimRequestRepository(connection)
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                repo.claim_next(uuid4(), uuid4(), request_id=uuid4())


@pytest.mark.parametrize("isolation", ["AUTOCOMMIT", "REPEATABLE READ", "SERIALIZABLE"])
def test_unsupported_transaction_modes(engine: Engine, isolation: str) -> None:
    with engine.connect().execution_options(isolation_level=isolation) as connection:
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                ClaimRequestRepository(connection)


@pytest.mark.parametrize("position", [0, 1, 2])
def test_invalid_ids_before_locking(
    engine: Engine, request_schema: str, position: int
) -> None:
    values: list[object] = [uuid4(), uuid4(), uuid4()]
    values[position] = "invalid"
    with transaction(engine, request_schema) as connection:
        with pytest.raises(TypeError):
            ClaimRequestRepository(connection).claim_next(
                values[0],  # type: ignore[arg-type]
                values[1],  # type: ignore[arg-type]
                request_id=values[2],  # type: ignore[arg-type]
            )
