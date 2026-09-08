"""Atomic completion, durable replay, basic races and transaction rollback."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import uuid4

import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import Connection, Engine, Table, event, func, select
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.completion import (
    AttemptCompletion,
    CompletionConflictError,
    CompletionOutcome,
    CompletionReceipt,
    CompletionResult,
)
from workflow_engine.domain.lease import (
    LeaseClockRegressionError,
    LeaseExpiredError,
    LeaseOwnershipError,
)
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.claim_requests import (
    ClaimReplayUnavailableError,
    ClaimRequestRepository,
)
from workflow_engine.repositories.claims import TaskClaim
from workflow_engine.repositories.completions import (
    CompletionInactiveError,
    CompletionNotFoundError,
    CompletionRepository,
    StoredCompletionError,
)
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    WorkflowRepository,
)
from workflow_engine.schema import (
    attempt_completions,
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
def completion_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


@pytest.fixture
def claimed(engine: Engine, completion_schema: str) -> TaskClaim:
    with transaction(engine, completion_schema) as connection:
        version = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name="complete_" + uuid4().hex,
                tasks=(
                    TaskDefinition(task_id="A", task_type="demo.echo"),
                    TaskDefinition(task_id="B", task_type="demo.echo"),
                    TaskDefinition(
                        task_id="C", task_type="demo.echo", depends_on=("A",)
                    ),
                ),
            )
        )
        run = RunRepository(connection).create_idempotent(
            version.id, idempotency_key=uuid4().hex
        )
        session = uuid4()
        WorkerRepository(connection, heartbeat_timeout_seconds=300).register(
            session, worker_name="worker", max_concurrency=1
        )
        claim = ClaimRequestRepository(connection, lease_seconds=600).claim_next(
            run.run_id, session, request_id=uuid4()
        )
        assert claim is not None and claim.task.task_key == "A"
        return claim


def report(claimed: TaskClaim, *, failed: bool = False) -> AttemptCompletion:
    return AttemptCompletion(
        attempt_id=claimed.attempt.id,
        worker_session_id=claimed.lease.worker_session_id,
        lease_token=claimed.lease.lease_token,
        result=CompletionResult(
            outcome=CompletionOutcome.FAILED if failed else CompletionOutcome.SUCCEEDED,
            error_code="handler_failed" if failed else None,
        ),
    )


class ClockedCompletions(CompletionRepository):
    def __init__(self, connection: Connection, observed: datetime) -> None:
        super().__init__(connection)
        self.observed = observed

    def _database_now(self) -> datetime:
        return self.observed


class ReplayOnly(CompletionRepository):
    def _database_now(self) -> datetime:
        raise AssertionError("Replay must not sample a new clock.")


@pytest.mark.parametrize("failed", [False, True])
def test_commit_visibility_capacity_and_replay(
    engine: Engine, completion_schema: str, claimed: TaskClaim, failed: bool
) -> None:
    submitted = report(claimed, failed=failed)
    with transaction(engine, completion_schema) as connection:
        receipt = CompletionRepository(connection).complete(submitted)
        with transaction(engine, completion_schema) as observer:
            assert (
                observer.execute(
                    select(func.count()).select_from(attempt_completions)
                ).scalar_one()
                == 0
            )
            assert (
                observer.execute(select(task_attempts.c.status)).scalar_one()
                == "RUNNING"
            )
            assert (
                observer.execute(
                    select(task_runs.c.status).where(task_runs.c.id == claimed.task.id)
                ).scalar_one()
                == "RUNNING"
            )
    with transaction(engine, completion_schema) as connection:
        assert ReplayOnly(connection).complete(submitted) == receipt
        assert (
            connection.execute(select(task_attempts.c.status)).scalar_one()
            == submitted.result.outcome.value
        )
        tasks = dict(
            connection.execute(select(task_runs.c.task_key, task_runs.c.status))
            .tuples()
            .all()
        )
        assert tasks == {
            "A": submitted.result.outcome.value,
            "B": "READY",
            "C": "PENDING",
        }
        assert (
            connection.execute(select(workflow_runs.c.status)).scalar_one() == "RUNNING"
        )
        assert (
            dict(connection.execute(select(attempt_leases)).mappings().one())
            == claimed.lease.model_dump()
        )
        binding = connection.execute(select(claim_requests)).one()
    with pytest.raises(ClaimReplayUnavailableError):
        with transaction(engine, completion_schema) as connection:
            ClaimRequestRepository(connection).claim_next(
                claimed.task.run_id,
                claimed.lease.worker_session_id,
                request_id=binding.request_id,
            )
    with transaction(engine, completion_schema) as connection:
        next_claim = ClaimRequestRepository(connection).claim_next(
            claimed.task.run_id, claimed.lease.worker_session_id, request_id=uuid4()
        )
        assert next_claim is not None and next_claim.task.task_key == "B"
    # Replaying A must not release the slot now held by B.
    with transaction(engine, completion_schema) as connection:
        assert ReplayOnly(connection).complete(submitted) == receipt
        assert (
            connection.execute(
                select(func.count())
                .select_from(task_attempts)
                .where(task_attempts.c.status == "RUNNING")
            ).scalar_one()
            == 1
        )


@pytest.mark.parametrize(
    "seconds,error",
    [
        (-0.000001, LeaseClockRegressionError),
        (600, LeaseExpiredError),
        (600.000001, LeaseExpiredError),
    ],
)
def test_deadline_rejection(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
    seconds: float,
    error: type[ValueError],
) -> None:
    with pytest.raises(error):
        with transaction(engine, completion_schema) as connection:
            ClockedCompletions(
                connection, claimed.lease.last_renewed_at + timedelta(seconds=seconds)
            ).complete(report(claimed))
    with transaction(engine, completion_schema) as connection:
        assert (
            connection.execute(select(task_attempts.c.status)).scalar_one() == "RUNNING"
        )
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize("seconds", [0, 599.999999])
def test_live_boundary(
    engine: Engine, completion_schema: str, claimed: TaskClaim, seconds: float
) -> None:
    observed = claimed.lease.last_renewed_at + timedelta(seconds=seconds)
    with transaction(engine, completion_schema) as connection:
        receipt = ClockedCompletions(connection, observed).complete(report(claimed))
        assert receipt.accepted_at == observed


@pytest.mark.parametrize("field", ["worker_session_id", "lease_token"])
@pytest.mark.parametrize("completed", [False, True])
def test_wrong_owner_never_accepted(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
    field: str,
    completed: bool,
) -> None:
    submitted = report(claimed)
    if completed:
        with transaction(engine, completion_schema) as connection:
            CompletionRepository(connection).complete(submitted)
    wrong = AttemptCompletion.model_validate({**submitted.model_dump(), field: uuid4()})
    with pytest.raises(LeaseOwnershipError) as error:
        with transaction(engine, completion_schema) as connection:
            CompletionRepository(connection).complete(wrong)
    assert str(wrong.lease_token) not in str(error.value)


@pytest.mark.parametrize("unleased", [False, True])
def test_missing_owned_attempt(
    engine: Engine, completion_schema: str, claimed: TaskClaim, unleased: bool
) -> None:
    missing = uuid4()
    if unleased:
        with transaction(engine, completion_schema) as connection:
            connection.execute(
                task_attempts.insert().values(
                    id=missing, task_id=claimed.task.id, attempt_number=2, status="LOST"
                )
            )
    submitted = AttemptCompletion.model_validate(
        {**report(claimed).model_dump(), "attempt_id": missing}
    )
    with pytest.raises(CompletionNotFoundError):
        with transaction(engine, completion_schema) as connection:
            CompletionRepository(connection).complete(submitted)


@pytest.mark.parametrize(
    "table,status",
    [
        (workflow_runs, "FAILED"),
        (task_runs, "SUCCEEDED"),
        (task_attempts, "SUCCEEDED"),
        (task_attempts, "FAILED"),
        (task_attempts, "LOST"),
        (task_attempts, "TIMED_OUT"),
        (worker_sessions, "STOPPED"),
    ],
)
def test_inactive_without_receipt(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
    table: Table,
    status: str,
) -> None:
    with transaction(engine, completion_schema) as connection:
        query = table.update().values(status=status)
        if table is task_runs:
            query = query.where(task_runs.c.id == claimed.task.id)
        connection.execute(query)
    with pytest.raises(CompletionInactiveError):
        with transaction(engine, completion_schema) as connection:
            CompletionRepository(connection).complete(report(claimed))


@pytest.mark.parametrize("lost", [False, True])
def test_stale_heartbeat_does_not_revoke_completion(
    engine: Engine, completion_schema: str, claimed: TaskClaim, lost: bool
) -> None:
    with transaction(engine, completion_schema) as connection:
        if lost:
            connection.execute(worker_sessions.update().values(status="LOST"))
        worker = connection.execute(select(worker_sessions)).one()
    with transaction(engine, completion_schema) as connection:
        ClockedCompletions(
            connection, worker.heartbeat_expires_at + timedelta(seconds=1)
        ).complete(report(claimed))
        assert connection.execute(select(worker_sessions)).one() == worker


def test_lost_response_replays_after_expiry_and_shutdown(
    engine: Engine, completion_schema: str, claimed: TaskClaim
) -> None:
    with transaction(engine, completion_schema) as connection:
        receipt = CompletionRepository(connection).complete(report(claimed))
    with transaction(engine, completion_schema) as connection:
        connection.execute(worker_sessions.update().values(status="STOPPED"))
        connection.execute(workflow_runs.update().values(status="FAILED"))
    with transaction(engine, completion_schema) as connection:
        assert (
            ClockedCompletions(
                connection, claimed.lease.lease_expires_at + timedelta(days=1)
            ).complete(report(claimed))
            == receipt
        )
        assert ReplayOnly(connection).complete(report(claimed)) == receipt


@pytest.mark.parametrize("changed_code", [False, True])
def test_conflicting_result(
    engine: Engine, completion_schema: str, claimed: TaskClaim, changed_code: bool
) -> None:
    with transaction(engine, completion_schema) as connection:
        original = CompletionRepository(connection).complete(
            report(claimed, failed=True)
        )
    conflicting = report(claimed)
    if changed_code:
        conflicting = AttemptCompletion.model_validate(
            {
                **report(claimed, failed=True).model_dump(),
                "result": {
                    "outcome": CompletionOutcome.FAILED,
                    "error_code": "different",
                },
            }
        )
    with pytest.raises(CompletionConflictError):
        with transaction(engine, completion_schema) as connection:
            ReplayOnly(connection).complete(conflicting)
    with transaction(engine, completion_schema) as connection:
        assert ReplayOnly(connection).complete(report(claimed, failed=True)) == original


@pytest.mark.parametrize("conflicting", [False, True])
def test_concurrent_completions(
    engine: Engine, completion_schema: str, claimed: TaskClaim, conflicting: bool
) -> None:
    barrier = Barrier(2, timeout=10)

    def submit(failed: bool) -> CompletionReceipt | None:
        barrier.wait()
        try:
            with transaction(engine, completion_schema) as connection:
                return CompletionRepository(connection).complete(
                    report(claimed, failed=failed)
                )
        except CompletionConflictError:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        pending = [executor.submit(submit, failed) for failed in (False, conflicting)]
        results = [f.result(timeout=20) for f in pending]
    if conflicting:
        assert sum(r is None for r in results) == 1
    else:
        assert results[0] is not None and results[0] == results[1]
    with transaction(engine, completion_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
            ).scalar_one()
            == 1
        )
        winner = next(r for r in results if r is not None)
        assert ReplayOnly(connection).complete(winner.completion) == winner


@pytest.mark.parametrize(
    "phase",
    ["attempt", "task", "receipt", "commit", "abort", "wrong_task", "wrong_receipt"],
)
def test_failure_rolls_back_every_write(
    engine: Engine, completion_schema: str, claimed: TaskClaim, phase: str
) -> None:
    if phase != "abort":
        with transaction(engine, completion_schema) as connection:
            if phase == "wrong_task":
                body = "RETURN OLD;"
            elif phase == "wrong_receipt":
                body = (
                    "NEW.accepted_at := NEW.accepted_at + interval '1 second'; "
                    "RETURN NEW;"
                )
            else:
                body = (
                    "RAISE EXCEPTION USING ERRCODE='23514', "
                    "MESSAGE='private-database-detail';"
                )
            connection.exec_driver_sql(
                "CREATE FUNCTION reject_completion() RETURNS trigger "
                f"LANGUAGE plpgsql AS $$ BEGIN {body} END; $$"
            )
            table = (
                "task_attempts"
                if phase == "attempt"
                else "task_runs"
                if phase in ("task", "wrong_task")
                else "attempt_completions"
            )
            operation = "INSERT" if table == "attempt_completions" else "UPDATE"
            kind = (
                "CONSTRAINT TRIGGER reject_completion AFTER"
                if phase == "commit"
                else "TRIGGER reject_completion BEFORE"
            )
            deferred = "DEFERRABLE INITIALLY DEFERRED" if phase == "commit" else ""
            connection.exec_driver_sql(
                f"CREATE {kind} {operation} ON {table} {deferred} "
                "FOR EACH ROW EXECUTE FUNCTION reject_completion()"
            )
    expected = (
        RuntimeError
        if phase == "abort"
        else StoredCompletionError
        if phase.startswith("wrong_")
        else DBAPIError
    )
    with pytest.raises(expected):
        with transaction(engine, completion_schema) as connection:
            CompletionRepository(connection).complete(report(claimed))
            if phase == "abort":
                raise RuntimeError("abort")
    with transaction(engine, completion_schema) as connection:
        assert (
            connection.execute(select(task_attempts.c.status)).scalar_one() == "RUNNING"
        )
        assert (
            connection.execute(
                select(task_runs.c.status).where(task_runs.c.id == claimed.task.id)
            ).scalar_one()
            == "RUNNING"
        )
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
            ).scalar_one()
            == 0
        )
        assert (
            dict(connection.execute(select(attempt_leases)).mappings().one())
            == claimed.lease.model_dump()
        )


@pytest.mark.parametrize("corruption", ["time", "owner", "outcome"])
def test_corrupt_receipt_is_not_replayed(
    engine: Engine, completion_schema: str, claimed: TaskClaim, corruption: str
) -> None:
    with transaction(engine, completion_schema) as connection:
        CompletionRepository(connection).complete(report(claimed))
    with transaction(engine, completion_schema) as connection:
        connection.exec_driver_sql(
            "ALTER TABLE attempt_completions "
            "DISABLE TRIGGER attempt_completions_append_only"
        )
        if corruption == "time":
            connection.execute(
                attempt_completions.update().values(
                    accepted_at=datetime(2000, 1, 1, tzinfo=UTC)
                )
            )
        elif corruption == "owner":
            connection.exec_driver_sql(
                "ALTER TABLE attempt_completions "
                "DROP CONSTRAINT fk_attempt_completions_owner_lease"
            )
            connection.execute(attempt_completions.update().values(lease_token=uuid4()))
        else:
            connection.exec_driver_sql(
                "ALTER TABLE attempt_completions "
                "DROP CONSTRAINT fk_attempt_completions_attempt_outcome"
            )
            connection.execute(
                attempt_completions.update().values(outcome="FAILED", error_code="bad")
            )
    with pytest.raises(StoredCompletionError) as error:
        with transaction(engine, completion_schema) as connection:
            ReplayOnly(connection).complete(report(claimed))
    assert str(claimed.lease.lease_token) not in str(error.value)


def test_original_transaction_required(
    engine: Engine, completion_schema: str, claimed: TaskClaim
) -> None:
    with engine.connect() as connection:
        with pytest.raises(RepositoryTransactionError):
            CompletionRepository(connection)
        with connection.begin():
            repo = CompletionRepository(connection)
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                repo.complete(report(claimed))


@pytest.mark.parametrize("mode", ["AUTOCOMMIT", "REPEATABLE READ", "SERIALIZABLE"])
def test_transaction_modes(engine: Engine, mode: str) -> None:
    with engine.connect().execution_options(isolation_level=mode) as connection:
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                CompletionRepository(connection)


def test_invalid_submission_fails_before_sql(
    engine: Engine, claimed: TaskClaim
) -> None:
    def reject_sql(*args: object) -> None:
        raise AssertionError("Invalid report must not execute SQL.")

    invalid = report(claimed).model_copy(update={"lease_token": "private-value"})
    with engine.begin() as connection:
        repo = CompletionRepository(connection)
        event.listen(connection, "before_cursor_execute", reject_sql)
        try:
            with pytest.raises(ValidationError) as error:
                repo.complete(invalid)
            assert "private-value" not in str(error.value)
        finally:
            event.remove(connection, "before_cursor_execute", reject_sql)
