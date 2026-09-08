"""Shared ordered ownership reads for renewal and claim replay; no clock or writes."""

from dataclasses import dataclass, field
from uuid import UUID

from sqlalchemy import Connection, RowMapping, Table, select

from workflow_engine.domain.lease import AttemptLease
from workflow_engine.domain.runtime import (
    AttemptStatus,
    RunStatus,
    TaskAttempt,
    TaskRun,
    TaskStatus,
    WorkflowRun,
)
from workflow_engine.domain.worker import WorkerSession, WorkerStatus
from workflow_engine.schema import (
    attempt_leases,
    task_attempts,
    task_runs,
    worker_sessions,
    workflow_runs,
)


class LeaseNotFoundError(LookupError):
    """No visible lease exists for the requested Attempt."""


class LeaseInactiveError(ValueError):
    """Execution is no longer running, or the owner has stopped."""


class StoredLeaseError(ValueError):
    """Persisted ownership or its execution references are invalid."""


def _read_lease(row: RowMapping) -> AttemptLease:
    try:
        return AttemptLease.model_validate(dict(row))
    except (ValueError, TypeError):
        raise StoredLeaseError("Stored attempt lease is invalid.") from None


@dataclass(frozen=True, slots=True)
class LockedOwnership:
    run: WorkflowRun
    worker: WorkerSession
    task: TaskRun
    attempt: TaskAttempt
    lease: AttemptLease = field(repr=False)

    def require_running(self) -> None:
        # A RUNNING Attempt is current by the unique RUNNING-attempt-per-task
        # constraint. Never authorize a terminal Attempt using a replacement's lease.
        if (
            self.run.status is not RunStatus.RUNNING
            or self.task.status is not TaskStatus.RUNNING
            or self.attempt.status is not AttemptStatus.RUNNING
            or self.worker.status is WorkerStatus.STOPPED
        ):
            raise LeaseInactiveError("Attempt lease execution is inactive.")


def _lock(connection: Connection, table: Table, key: str, identity: UUID) -> RowMapping:
    row = (
        connection.execute(
            select(table).where(table.c[key] == identity).with_for_update()
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise StoredLeaseError("Stored lease reference was not found.")
    return row


def lock_ownership(connection: Connection, attempt_id: UUID) -> LockedOwnership:
    """Caller owns the transaction and must hold no locks that reverse this order."""
    # Discovery is non-locking and identifies only immutable lock targets.
    # No status, deadline or owner authorization is accepted from this hint.
    hint = connection.execute(
        select(
            task_runs.c.run_id,
            task_attempts.c.task_id,
            attempt_leases.c.worker_session_id,
        )
        .select_from(
            attempt_leases.join(
                task_attempts, attempt_leases.c.attempt_id == task_attempts.c.id
            ).join(task_runs, task_attempts.c.task_id == task_runs.c.id)
        )
        .where(attempt_leases.c.attempt_id == attempt_id)
    ).one_or_none()
    if hint is None:
        raise LeaseNotFoundError("Attempt lease was not found.")

    run_row = _lock(connection, workflow_runs, "id", hint.run_id)
    worker_row = _lock(connection, worker_sessions, "id", hint.worker_session_id)
    task_row = _lock(connection, task_runs, "id", hint.task_id)
    attempt_row = _lock(connection, task_attempts, "id", attempt_id)
    lease = _read_lease(_lock(connection, attempt_leases, "attempt_id", attempt_id))
    try:
        run = WorkflowRun(
            id=run_row["id"],
            workflow_version_id=run_row["workflow_version_id"],
            status=RunStatus(run_row["status"]),
        )
        worker = WorkerSession(
            id=worker_row["id"],
            worker_name=worker_row["worker_name"],
            max_concurrency=worker_row["max_concurrency"],
            status=WorkerStatus(worker_row["status"]),
        )
        task = TaskRun(
            id=task_row["id"],
            run_id=task_row["run_id"],
            task_key=task_row["task_key"],
            status=TaskStatus(task_row["status"]),
        )
        attempt = TaskAttempt(
            id=attempt_row["id"],
            task_id=attempt_row["task_id"],
            attempt_number=attempt_row["attempt_number"],
            status=AttemptStatus(attempt_row["status"]),
        )
    except (ValueError, TypeError):
        raise StoredLeaseError("Stored lease execution state is invalid.") from None
    if (
        task.run_id != run.id
        or attempt.task_id != task.id
        or lease.attempt_id != attempt.id
        or lease.worker_session_id != worker.id
    ):
        raise StoredLeaseError("Stored lease references do not match.")
    return LockedOwnership(run, worker, task, attempt, lease)
