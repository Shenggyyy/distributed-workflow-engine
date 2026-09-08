"""Readiness transactions, real completion ordering, rollback and concurrent scans."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from tests.integration.test_completion_races import watch_lock
from tests.integration.test_completions import claimed as claimed
from tests.integration.test_completions import completion_schema as completion_schema
from tests.integration.test_completions import report, transaction
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.claims import ClaimRepository, TaskClaim
from workflow_engine.repositories.completions import CompletionRepository
from workflow_engine.repositories.runs import RunRepository, StoredRuntimeError
from workflow_engine.repositories.scheduling import (
    SchedulingRepository,
    SchedulingRunNotFoundError,
)
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    WorkflowRepository,
)
from workflow_engine.schema import task_attempts, task_runs, workflow_runs

pytestmark = pytest.mark.integration


def test_claim_complete_reconcile_diamond(
    engine: Engine, completion_schema: str, claimed: TaskClaim
) -> None:
    with transaction(engine, completion_schema) as connection:
        version = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name="diamond",
                tasks=(
                    TaskDefinition(task_id="A", task_type="demo.echo"),
                    TaskDefinition(
                        task_id="B", task_type="demo.echo", depends_on=("A",)
                    ),
                    TaskDefinition(
                        task_id="C", task_type="demo.echo", depends_on=("A",)
                    ),
                    TaskDefinition(
                        task_id="D", task_type="demo.echo", depends_on=("B", "C")
                    ),
                ),
            )
        )
        run = RunRepository(connection).create(version.id)
        CompletionRepository(connection).complete(report(claimed))
    for expected_key, next_ready in (
        ("A", ("B", "C")),
        ("B", ()),
        ("C", ("D",)),
        ("D", ()),
    ):
        with transaction(engine, completion_schema) as connection:
            claim = ClaimRepository(connection, lease_seconds=600).claim_next(
                run.run.id, claimed.lease.worker_session_id
            )
            assert claim is not None and claim.task.task_key == expected_key
            CompletionRepository(connection).complete(report(claim))
        with transaction(engine, completion_schema) as connection:
            ready = SchedulingRepository(connection).reconcile(run.run.id)
            assert tuple(task.task_key for task in ready) == next_ready
        with transaction(engine, completion_schema) as connection:
            assert SchedulingRepository(connection).reconcile(run.run.id) == ()
    with transaction(engine, completion_schema) as connection:
        assert set(
            connection.scalars(
                select(task_runs.c.status).where(task_runs.c.run_id == run.run.id)
            )
        ) == {"SUCCEEDED"}
        assert (
            connection.scalar(
                select(workflow_runs.c.status).where(workflow_runs.c.id == run.run.id)
            )
            == "SUCCEEDED"
        )


@pytest.mark.parametrize("failed", [False, True])
def test_only_committed_success_unlocks_descendant(
    engine: Engine, completion_schema: str, claimed: TaskClaim, failed: bool
) -> None:
    with transaction(engine, completion_schema) as connection:
        assert SchedulingRepository(connection).reconcile(claimed.task.run_id) == ()
    with transaction(engine, completion_schema) as connection:
        CompletionRepository(connection).complete(report(claimed, failed=failed))
    with transaction(engine, completion_schema) as connection:
        changed = SchedulingRepository(connection).reconcile(claimed.task.run_id)
        assert tuple(task.task_key for task in changed) == (() if failed else ("C",))
        assert connection.scalar(select(func.count()).select_from(task_attempts)) == 1


@pytest.mark.parametrize("commit", [False, True])
def test_waits_for_completion_commit_or_rollback(
    engine: Engine, completion_schema: str, claimed: TaskClaim, commit: bool
) -> None:
    entered = Event()

    def reconcile() -> tuple[str, ...]:
        with transaction(engine, completion_schema) as connection:
            watch_lock(connection, workflow_runs, entered)
            return tuple(
                task.task_key
                for task in SchedulingRepository(connection).reconcile(
                    claimed.task.run_id
                )
            )

    with ThreadPoolExecutor(max_workers=1) as executor:
        with transaction(engine, completion_schema) as owner:
            CompletionRepository(owner).complete(report(claimed))
            pending = executor.submit(reconcile)
            assert entered.wait(5) and not pending.done()
            if not commit:
                owner.rollback()
        assert pending.result(timeout=10) == (("C",) if commit else ())


def test_concurrent_schedulers_apply_once(
    engine: Engine, completion_schema: str, claimed: TaskClaim
) -> None:
    with transaction(engine, completion_schema) as connection:
        CompletionRepository(connection).complete(report(claimed))
    barrier = Barrier(3, timeout=10)

    def reconcile() -> int:
        with transaction(engine, completion_schema) as connection:
            barrier.wait()
            return len(SchedulingRepository(connection).reconcile(claimed.task.run_id))

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(reconcile) for _ in range(3)]
        assert sorted(future.result(timeout=10) for future in futures) == [0, 0, 1]


def test_caller_rollback_preserves_pending(
    engine: Engine, completion_schema: str, claimed: TaskClaim
) -> None:
    with transaction(engine, completion_schema) as connection:
        CompletionRepository(connection).complete(report(claimed))
    with transaction(engine, completion_schema) as connection:
        assert len(SchedulingRepository(connection).reconcile(claimed.task.run_id)) == 1
        connection.rollback()
    with transaction(engine, completion_schema) as connection:
        assert len(SchedulingRepository(connection).reconcile(claimed.task.run_id)) == 1


@pytest.mark.parametrize("deferred", [False, True])
def test_write_and_commit_failure_roll_back_readiness(
    engine: Engine, completion_schema: str, claimed: TaskClaim, deferred: bool
) -> None:
    with transaction(engine, completion_schema) as connection:
        CompletionRepository(connection).complete(report(claimed))
        connection.exec_driver_sql(
            "CREATE FUNCTION reject_ready() RETURNS trigger LANGUAGE plpgsql AS $$ "
            "BEGIN IF NEW.status = 'READY' THEN RAISE EXCEPTION 'injected'; "
            "END IF; RETURN NEW; END; $$"
        )
        kind = (
            "CONSTRAINT TRIGGER reject_ready AFTER"
            if deferred
            else "TRIGGER reject_ready BEFORE"
        )
        timing = "DEFERRABLE INITIALLY DEFERRED" if deferred else ""
        connection.exec_driver_sql(
            f"CREATE {kind} UPDATE ON task_runs {timing} "
            "FOR EACH ROW EXECUTE FUNCTION reject_ready()"
        )
    with pytest.raises(DBAPIError):
        with transaction(engine, completion_schema) as connection:
            SchedulingRepository(connection).reconcile(claimed.task.run_id)
    with transaction(engine, completion_schema) as connection:
        assert (
            connection.scalar(
                select(task_runs.c.status).where(task_runs.c.task_key == "C")
            )
            == "PENDING"
        )


def test_run_lock_timeout_and_retry(
    engine: Engine, completion_schema: str, claimed: TaskClaim
) -> None:
    with transaction(engine, completion_schema) as owner:
        CompletionRepository(owner).complete(report(claimed))
        with pytest.raises(DBAPIError) as error:
            with transaction(engine, completion_schema) as contender:
                contender.exec_driver_sql("SET LOCAL lock_timeout = '100ms'")
                SchedulingRepository(contender).reconcile(claimed.task.run_id)
        assert getattr(error.value.orig, "sqlstate", None) == "55P03"
    with transaction(engine, completion_schema) as connection:
        assert len(SchedulingRepository(connection).reconcile(claimed.task.run_id)) == 1


def test_missing_node_rejected_before_changes(
    engine: Engine, completion_schema: str, claimed: TaskClaim
) -> None:
    incomplete_run = uuid4()
    with transaction(engine, completion_schema) as connection:
        # Construct an incomplete snapshot as a corrupt external writer would;
        # do not disable runtime deletion/history protections for this test.
        connection.execute(
            workflow_runs.insert().values(
                id=incomplete_run,
                workflow_version_id=claimed.workflow_version_id,
                status="PENDING",
            )
        )
        connection.execute(
            workflow_runs.update()
            .where(workflow_runs.c.id == incomplete_run)
            .values(status="RUNNING")
        )
        connection.execute(
            task_runs.insert().values(
                id=uuid4(), run_id=incomplete_run, task_key="A", status="PENDING"
            )
        )
    with pytest.raises(StoredRuntimeError):
        with transaction(engine, completion_schema) as connection:
            SchedulingRepository(connection).reconcile(incomplete_run)


def test_missing_run_and_original_transaction(
    engine: Engine, completion_schema: str
) -> None:
    with engine.connect() as connection:
        with connection.begin():
            migration_config(connection, completion_schema)
            repository = SchedulingRepository(connection)
            with pytest.raises(SchedulingRunNotFoundError):
                repository.reconcile(uuid4())
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                repository.reconcile(uuid4())


@pytest.mark.parametrize("commit", [False, True])
def test_claim_waits_for_scheduler_transaction(
    engine: Engine, completion_schema: str, claimed: TaskClaim, commit: bool
) -> None:
    with transaction(engine, completion_schema) as connection:
        CompletionRepository(connection).complete(report(claimed))
        other = ClaimRepository(connection, lease_seconds=600).claim_next(
            claimed.task.run_id, claimed.lease.worker_session_id
        )
        assert other is not None and other.task.task_key == "B"
        CompletionRepository(connection).complete(report(other))
    entered = Event()

    def claim_child() -> TaskClaim | None:
        with transaction(engine, completion_schema) as connection:
            watch_lock(connection, workflow_runs, entered)
            return ClaimRepository(connection, lease_seconds=600).claim_next(
                claimed.task.run_id, claimed.lease.worker_session_id
            )

    with ThreadPoolExecutor(max_workers=1) as executor:
        with transaction(engine, completion_schema) as owner:
            assert len(SchedulingRepository(owner).reconcile(claimed.task.run_id)) == 1
            pending = executor.submit(claim_child)
            assert entered.wait(5) and not pending.done()
            if not commit:
                owner.rollback()
        result = pending.result(timeout=10)
    assert (result.task.task_key if result is not None else None) == (
        "C" if commit else None
    )
