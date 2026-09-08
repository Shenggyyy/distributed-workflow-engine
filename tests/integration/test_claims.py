"""Atomic claims, capacity serialization, post-lock clocks and failure rollback."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import Barrier, Event
from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, event, func, select
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.runtime import AttemptStatus, TaskStatus
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.claims import (
    AttemptNumberExhaustedError,
    ClaimRepository,
    ClaimRunInactiveError,
    ClaimRunNotFoundError,
    TaskClaim,
)
from workflow_engine.repositories.runs import RunRepository, StoredRuntimeError
from workflow_engine.repositories.workers import (
    WorkerClockRegressionError,
    WorkerRepository,
    WorkerSessionExpiredError,
    WorkerSessionInactiveError,
    WorkerSessionNotFoundError,
)
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    WorkflowRepository,
)
from workflow_engine.schema import (
    attempt_leases,
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
def claim_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


def seed(
    connection: Connection,
    *,
    capacity: int = 2,
    roots: int = 1,
    dependent: bool = False,
) -> tuple[UUID, UUID]:
    tasks = tuple(
        TaskDefinition(task_id=chr(65 + i), task_type="demo.echo") for i in range(roots)
    )
    if dependent:
        tasks += (
            TaskDefinition(task_id="Z", task_type="demo.next", depends_on=("A",)),
        )
    version = WorkflowRepository(connection).publish(
        WorkflowDefinition(name="claim_" + uuid4().hex, tasks=tasks)
    )
    run = RunRepository(connection).create(version.id).run
    worker = WorkerRepository(connection, heartbeat_timeout_seconds=300).register(
        uuid4(), worker_name="worker", max_concurrency=capacity
    )
    return run.id, worker.session.id


def claim(
    engine: Engine, schema: str, run_id: UUID, worker_id: UUID
) -> TaskClaim | None:
    with transaction(engine, schema) as connection:
        result = ClaimRepository(connection).claim_next(run_id, worker_id)
    return result


def test_claim_is_committed_with_pinned_definition_and_server_lease(
    engine: Engine, claim_schema: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection, dependent=True)
        worker_before = connection.execute(select(worker_sessions)).one()
    with transaction(engine, claim_schema) as connection:
        result = ClaimRepository(connection, lease_seconds=7).claim_next(
            run_id, worker_id
        )
        assert result is not None
        # A separate connection cannot observe an uncommitted execution grant.
        with transaction(engine, claim_schema) as reader:
            assert (
                reader.execute(
                    select(func.count()).select_from(task_attempts)
                ).scalar_one()
                == 0
            )
    assert result.task.status is TaskStatus.RUNNING
    assert (
        result.attempt.status is AttemptStatus.RUNNING
        and result.attempt.attempt_number == 1
    )
    assert (
        result.definition.task_id == "A" and result.definition.task_type == "demo.echo"
    )
    assert result.lease.worker_session_id == worker_id
    assert result.lease.attempt_id == result.attempt.id
    assert result.lease.acquired_at == result.lease.last_renewed_at
    assert result.lease.lease_expires_at - result.lease.acquired_at == timedelta(
        seconds=7
    )
    assert str(result.lease.lease_token) not in repr(result)
    with transaction(engine, claim_schema) as connection:
        assert (
            connection.execute(select(task_attempts.c.id)).scalar_one()
            == result.attempt.id
        )
        assert (
            connection.execute(select(attempt_leases.c.lease_token)).scalar_one()
            == result.lease.lease_token
        )
        assert (
            connection.execute(
                select(task_runs.c.status).where(task_runs.c.task_key == "Z")
            ).scalar_one()
            == "PENDING"
        )
        assert connection.execute(select(worker_sessions)).one() == worker_before
        assert (
            connection.execute(select(workflow_runs.c.workflow_version_id)).scalar_one()
            == result.workflow_version_id
        )
    assert claim(engine, claim_schema, run_id, worker_id) is None


@pytest.mark.parametrize("cross_run", [False, True])
def test_concurrent_claims_respect_run_exclusivity_and_worker_capacity(
    engine: Engine, claim_schema: str, cross_run: bool
) -> None:
    with transaction(engine, claim_schema) as connection:
        first, worker_id = seed(
            connection, capacity=1 if cross_run else 4, roots=1 if cross_run else 2
        )
        second = seed(connection)[0] if cross_run else first
    barrier = Barrier(4, timeout=10)

    def submit(index: int) -> TaskClaim | None:
        barrier.wait()
        return claim(
            engine, claim_schema, first if index % 2 == 0 else second, worker_id
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(submit, i) for i in range(4)]
        results = [f.result(timeout=20) for f in futures]
    grants = [r for r in results if r is not None]
    assert len(grants) == (1 if cross_run else 2)
    assert len({r.task.id for r in grants}) == len(grants)
    assert len({r.lease.lease_token for r in grants}) == len(grants)
    with transaction(engine, claim_schema) as connection:
        assert connection.execute(
            select(func.count()).select_from(attempt_leases)
        ).scalar_one() == len(grants)


def test_two_workers_cannot_claim_the_same_task(
    engine: Engine, claim_schema: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, first = seed(connection)
        second = (
            WorkerRepository(connection)
            .register(uuid4(), worker_name="worker", max_concurrency=2)
            .session.id
        )
    barrier = Barrier(2, timeout=10)

    def submit(worker_id: UUID) -> TaskClaim | None:
        barrier.wait()
        return claim(engine, claim_schema, run_id, worker_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(submit, w) for w in (first, second)]
        assert sum(f.result(timeout=20) is not None for f in futures) == 1


def test_retry_allocates_new_identity_and_sequential_number(
    engine: Engine, claim_schema: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection, capacity=1)
    first = claim(engine, claim_schema, run_id, worker_id)
    assert first is not None
    with transaction(engine, claim_schema) as connection:
        connection.execute(task_attempts.update().values(status="FAILED"))
        connection.execute(task_runs.update().values(status="RETRY_WAIT"))
        connection.execute(task_runs.update().values(status="READY"))
    second = claim(engine, claim_schema, run_id, worker_id)
    assert second is not None and second.attempt.attempt_number == 2
    assert second.task.id == first.task.id and second.attempt.id != first.attempt.id
    assert second.lease.lease_token != first.lease.lease_token
    with transaction(engine, claim_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_leases)
            ).scalar_one()
            == 2
        )


def test_expired_running_lease_still_consumes_capacity(
    engine: Engine, claim_schema: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        first_run, worker_id = seed(connection, capacity=1)
        next_run = seed(connection)[0]
        task_id = connection.execute(
            select(task_runs.c.id).where(task_runs.c.run_id == first_run)
        ).scalar_one()
        connection.execute(
            task_runs.update().where(task_runs.c.id == task_id).values(status="RUNNING")
        )
        attempt_id = uuid4()
        connection.execute(
            task_attempts.insert().values(
                id=attempt_id, task_id=task_id, attempt_number=1
            )
        )
        past = connection.execute(
            select(func.clock_timestamp())
        ).scalar_one() - timedelta(days=1)
        connection.execute(
            attempt_leases.insert().values(
                attempt_id=attempt_id,
                worker_session_id=worker_id,
                lease_token=uuid4(),
                acquired_at=past,
                last_renewed_at=past,
                lease_expires_at=past + timedelta(seconds=30),
            )
        )
    assert claim(engine, claim_schema, next_run, worker_id) is None
    with transaction(engine, claim_schema) as connection:
        connection.execute(task_attempts.update().values(status="LOST"))
        connection.execute(
            task_runs.update()
            .where(task_runs.c.id == task_id)
            .values(status="RETRY_WAIT")
        )
    assert claim(engine, claim_schema, next_run, worker_id) is not None


@pytest.mark.parametrize(
    "target",
    ["run_missing", "run_inactive", "worker_missing", "worker_lost", "worker_stopped"],
)
def test_admission_errors_write_nothing(
    engine: Engine, claim_schema: str, target: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection)
        if target == "run_inactive":
            connection.execute(workflow_runs.update().values(status="FAILED"))
        if target.startswith("worker_") and target != "worker_missing":
            connection.execute(
                worker_sessions.update().values(status=target.split("_")[1].upper())
            )
    expected = {
        "run_missing": ClaimRunNotFoundError,
        "run_inactive": ClaimRunInactiveError,
        "worker_missing": WorkerSessionNotFoundError,
        "worker_lost": WorkerSessionInactiveError,
        "worker_stopped": WorkerSessionInactiveError,
    }[target]
    with pytest.raises(expected):
        claim(
            engine,
            claim_schema,
            uuid4() if target == "run_missing" else run_id,
            uuid4() if target == "worker_missing" else worker_id,
        )
    with transaction(engine, claim_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(task_attempts)
            ).scalar_one()
            == 0
        )
        assert connection.execute(select(task_runs.c.status)).scalar_one() == "READY"


class ClockedClaims(ClaimRepository):
    def __init__(self, connection: Connection, observations: list[datetime]) -> None:
        super().__init__(connection)
        self.observations = iter(observations)

    def _database_now(self) -> datetime:
        return next(self.observations)


@pytest.mark.parametrize(
    "case", ["before", "exact", "backward", "late_final", "regress_final"]
)
def test_clock_boundaries_on_both_admission_checks(
    engine: Engine, claim_schema: str, case: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection)
        worker = connection.execute(select(worker_sessions)).one()
    start, end = worker.last_heartbeat_at, worker.heartbeat_expires_at
    observations = {
        "before": [end - timedelta(microseconds=1)] * 2,
        "exact": [end],
        "backward": [start - timedelta(microseconds=1)],
        "late_final": [start, end],
        "regress_final": [start + timedelta(seconds=1), start],
    }[case]
    if case == "before":
        with transaction(engine, claim_schema) as connection:
            assert (
                ClockedClaims(connection, observations).claim_next(run_id, worker_id)
                is not None
            )
    else:
        expected = (
            WorkerClockRegressionError
            if case in ("backward", "regress_final")
            else WorkerSessionExpiredError
        )
        with pytest.raises(expected):
            with transaction(engine, claim_schema) as connection:
                ClockedClaims(connection, observations).claim_next(run_id, worker_id)
        with transaction(engine, claim_schema) as connection:
            assert (
                connection.execute(select(task_runs.c.status)).scalar_one() == "READY"
            )


def test_task_lock_wait_rechecks_worker_deadline(
    engine: Engine, claim_schema: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection)
        worker = connection.execute(select(worker_sessions)).one()
    entered = Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if "FROM task_runs" in statement and "FOR UPDATE" in statement:
            entered.set()

    def submit() -> None:
        with pytest.raises(WorkerSessionExpiredError):
            with transaction(engine, claim_schema) as connection:
                ClockedClaims(
                    connection, [worker.last_heartbeat_at, worker.heartbeat_expires_at]
                ).claim_next(run_id, worker_id)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with transaction(engine, claim_schema) as blocker:
            blocker.execute(select(task_runs).with_for_update()).all()
            event.listen(engine, "before_cursor_execute", before_execute)
            try:
                pending = executor.submit(submit)
                assert entered.wait(timeout=5) and not pending.done()
            finally:
                event.remove(engine, "before_cursor_execute", before_execute)
        pending.result(timeout=5)
    with transaction(engine, claim_schema) as connection:
        assert connection.execute(select(task_runs.c.status)).scalar_one() == "READY"


@pytest.mark.parametrize("phase", ["task", "attempt", "lease", "commit"])
def test_failure_rolls_back_all_claim_changes(
    engine: Engine, claim_schema: str, phase: str
) -> None:
    table = {
        "task": "task_runs",
        "attempt": "task_attempts",
        "lease": "attempt_leases",
        "commit": "attempt_leases",
    }[phase]
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection)
        connection.exec_driver_sql(
            """CREATE FUNCTION reject_claim() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION USING ERRCODE='23514',
                    MESSAGE='injected claim failure';
            END; $$"""
        )
        kind = (
            "CONSTRAINT TRIGGER reject_claim AFTER INSERT"
            if phase == "commit"
            else "TRIGGER reject_claim BEFORE "
            + ("UPDATE" if phase == "task" else "INSERT")
        )
        deferred = "DEFERRABLE INITIALLY DEFERRED" if phase == "commit" else ""
        connection.exec_driver_sql(
            f"CREATE {kind} ON {table} {deferred} "
            "FOR EACH ROW EXECUTE FUNCTION reject_claim()"
        )
    with pytest.raises(DBAPIError):
        claim(engine, claim_schema, run_id, worker_id)
    with transaction(engine, claim_schema) as connection:
        assert connection.execute(select(task_runs.c.status)).scalar_one() == "READY"
        for stored in (task_attempts, attempt_leases):
            assert (
                connection.execute(
                    select(func.count()).select_from(stored)
                ).scalar_one()
                == 0
            )
        connection.exec_driver_sql(f"DROP TRIGGER reject_claim ON {table}")
    assert claim(engine, claim_schema, run_id, worker_id) is not None


@pytest.mark.parametrize(
    "case",
    [
        "dependency",
        "missing_dependency",
        "missing_node",
        "running_attempt",
        "exhausted",
    ],
)
def test_inconsistent_ready_task_and_number_exhaustion_are_rejected(
    engine: Engine, claim_schema: str, case: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection, dependent=True)
        task_id = connection.execute(
            select(task_runs.c.id).where(task_runs.c.task_key == "A")
        ).scalar_one()
        if case in ("dependency", "missing_dependency"):
            connection.execute(
                task_runs.update()
                .where(task_runs.c.task_key == "A")
                .values(status="RUNNING")
            )
            connection.execute(
                task_runs.update()
                .where(task_runs.c.task_key == "Z")
                .values(status="READY")
            )
            if case == "missing_dependency":
                # Corrupt test-only schema to represent a missing runtime DAG node.
                connection.exec_driver_sql(
                    "ALTER TABLE task_runs DISABLE TRIGGER task_runs_retain_history"
                )
                connection.execute(
                    task_runs.delete().where(task_runs.c.task_key == "A")
                )
        elif case == "missing_node":
            connection.execute(
                task_runs.insert().values(
                    id=uuid4(), run_id=run_id, task_key="AA", status="READY"
                )
            )
            connection.execute(
                task_runs.update()
                .where(task_runs.c.task_key == "A")
                .values(status="RUNNING")
            )
        else:
            connection.execute(
                task_attempts.insert().values(
                    id=uuid4(),
                    task_id=task_id,
                    attempt_number=2147483647 if case == "exhausted" else 1,
                    status="FAILED" if case == "exhausted" else "RUNNING",
                )
            )
    with pytest.raises(
        AttemptNumberExhaustedError if case == "exhausted" else StoredRuntimeError
    ):
        claim(engine, claim_schema, run_id, worker_id)


def test_explicit_abort_and_transaction_lifetime(
    engine: Engine, claim_schema: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection)
    with pytest.raises(RuntimeError, match="abort"):
        with transaction(engine, claim_schema) as connection:
            repo = ClaimRepository(connection)
            assert repo.claim_next(run_id, worker_id) is not None
            raise RuntimeError("abort")
    with pytest.raises(RepositoryTransactionError):
        repo.claim_next(run_id, worker_id)
    with engine.connect() as connection:
        with pytest.raises(RepositoryTransactionError):
            ClaimRepository(connection)
        with connection.begin():
            old = ClaimRepository(connection)
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                old.claim_next(run_id, worker_id)
    result = claim(engine, claim_schema, run_id, worker_id)
    assert result is not None and result.attempt.attempt_number == 1


@pytest.mark.parametrize("isolation", ["AUTOCOMMIT", "REPEATABLE READ", "SERIALIZABLE"])
def test_unsupported_transaction_modes(engine: Engine, isolation: str) -> None:
    with engine.connect().execution_options(isolation_level=isolation) as connection:
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                ClaimRepository(connection)


@pytest.mark.parametrize("duration", [0, 86401, True, "30", 30.0])
def test_invalid_policy_before_sql(engine: Engine, duration: object) -> None:
    with engine.connect() as connection:
        with pytest.raises(ValueError):
            ClaimRepository(connection, lease_seconds=duration)  # type: ignore[arg-type]


def test_invalid_ids_before_sql(engine: Engine, claim_schema: str) -> None:
    with transaction(engine, claim_schema) as connection:
        repo = ClaimRepository(connection)
        with pytest.raises(TypeError):
            repo.claim_next("bad", uuid4())  # type: ignore[arg-type]


def test_ready_child_uses_its_pinned_definition_after_parent_success(
    engine: Engine, claim_schema: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection, dependent=True)
    parent = claim(engine, claim_schema, run_id, worker_id)
    assert parent is not None
    with transaction(engine, claim_schema) as connection:
        connection.execute(task_attempts.update().values(status="SUCCEEDED"))
        connection.execute(
            task_runs.update()
            .where(task_runs.c.task_key == "A")
            .values(status="SUCCEEDED")
        )
        connection.execute(
            task_runs.update().where(task_runs.c.task_key == "Z").values(status="READY")
        )
        original = WorkflowRepository(connection).get_version(
            parent.workflow_version_id
        )
        assert original is not None
        newer = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name=original.definition.name,
                tasks=(TaskDefinition(task_id="Z", task_type="different.handler"),),
            )
        )
    child = claim(engine, claim_schema, run_id, worker_id)
    assert child is not None and child.task.task_key == "Z"
    assert child.definition.task_type == "demo.next"
    assert child.workflow_version_id == parent.workflow_version_id != newer.id


def test_waiting_claim_can_allocate_after_owner_rolls_back(
    engine: Engine, claim_schema: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection, capacity=1)
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

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as owner:
            tx = owner.begin()
            try:
                migration_config(owner, claim_schema)
                abandoned = ClaimRepository(owner).claim_next(run_id, worker_id)
                assert abandoned is not None
                event.listen(engine, "before_cursor_execute", before_execute)
                try:
                    waiting = executor.submit(
                        claim, engine, claim_schema, run_id, worker_id
                    )
                    assert entered.wait(timeout=5) and not waiting.done()
                finally:
                    event.remove(engine, "before_cursor_execute", before_execute)
            finally:
                tx.rollback()
        result = waiting.result(timeout=5)
    assert result is not None and result.attempt.attempt_number == 1
    assert result.task.id == abandoned.task.id
    assert result.attempt.id != abandoned.attempt.id
    with transaction(engine, claim_schema) as connection:
        assert (
            connection.execute(select(task_attempts.c.id)).scalar_one()
            == result.attempt.id
        )
        assert (
            connection.execute(select(attempt_leases.c.attempt_id)).scalar_one()
            == result.attempt.id
        )


def test_run_lock_timeout_leaves_no_partial_claim(
    engine: Engine, claim_schema: str
) -> None:
    with transaction(engine, claim_schema) as connection:
        run_id, worker_id = seed(connection)
    with transaction(engine, claim_schema) as owner:
        owner.execute(select(workflow_runs).with_for_update()).all()
        with pytest.raises(DBAPIError) as error:
            with transaction(engine, claim_schema) as contender:
                contender.exec_driver_sql("SET LOCAL lock_timeout = '100ms'")
                ClaimRepository(contender).claim_next(run_id, worker_id)
        assert getattr(error.value.orig, "sqlstate", None) == "55P03"
    assert claim(engine, claim_schema, run_id, worker_id) is not None
