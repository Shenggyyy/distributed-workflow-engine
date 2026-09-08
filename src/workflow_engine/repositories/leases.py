"""Renew current attempt ownership in short, ordered PostgreSQL transactions."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import Connection, RowMapping, Table, func, select

from workflow_engine.domain.lease import MAX_LEASE_SECONDS, AttemptLease
from workflow_engine.domain.runtime import (
    AttemptStatus,
    RunStatus,
    TaskAttempt,
    TaskRun,
    TaskStatus,
    WorkflowRun,
)
from workflow_engine.domain.worker import WorkerSession, WorkerStatus
from workflow_engine.repositories.workflows import RepositoryTransactionError
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


class LeaseRepository:
    """Renew one Attempt in one caller transaction; never commit or retry.

    Use a fresh transaction per operation. Do not share across threads or call
    while holding locks that reverse Run -> Worker -> Task -> Attempt -> lease.
    """

    def __init__(self, connection: Connection, *, lease_seconds: int = 30) -> None:
        if (
            type(lease_seconds) is not int
            or not 1 <= lease_seconds <= MAX_LEASE_SECONDS
        ):
            raise ValueError("Lease duration must be an integer from 1 through 86400.")
        self._connection = connection
        self._transaction = connection.get_transaction()
        self._require_transaction()
        if (
            connection.dialect.name != "postgresql"
            or getattr(connection.connection.driver_connection, "autocommit", True)
            or connection.get_isolation_level() != "READ COMMITTED"
        ):
            raise RepositoryTransactionError(
                "LeaseRepository requires PostgreSQL READ COMMITTED."
            )
        self._lease_seconds = lease_seconds

    def _require_transaction(self) -> None:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "LeaseRepository requires its original active transaction."
            )

    def _database_now(self) -> datetime:
        observed: datetime = self._connection.execute(
            select(func.clock_timestamp())
        ).scalar_one()
        return observed

    def _lock(self, table: Table, key: str, identity: UUID) -> RowMapping:
        row = (
            self._connection.execute(
                select(table).where(table.c[key] == identity).with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise StoredLeaseError("Stored lease reference was not found.")
        return row

    def renew(
        self, attempt_id: UUID, *, worker_session_id: UUID, lease_token: UUID
    ) -> AttemptLease:
        """Return a provisional renewal of current ownership, never an old receipt."""
        self._require_transaction()
        if any(
            type(value) is not UUID
            for value in (attempt_id, worker_session_id, lease_token)
        ):
            raise TypeError(
                "Attempt, Worker session and lease token must be UUID instances."
            )
        # Discovery is non-locking and identifies only immutable lock targets.
        # No status, deadline or owner authorization is accepted from this hint.
        hint = self._connection.execute(
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

        run_row = self._lock(workflow_runs, "id", hint.run_id)
        worker_row = self._lock(worker_sessions, "id", hint.worker_session_id)
        task_row = self._lock(task_runs, "id", hint.task_id)
        attempt_row = self._lock(task_attempts, "id", attempt_id)
        lease = _read_lease(self._lock(attempt_leases, "attempt_id", attempt_id))
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
        # A RUNNING Attempt is current by the unique RUNNING-attempt-per-task
        # constraint. Never authorize a terminal Attempt using a replacement's lease.
        if (
            run.status is not RunStatus.RUNNING
            or task.status is not TaskStatus.RUNNING
            or attempt.status is not AttemptStatus.RUNNING
            or worker.status is WorkerStatus.STOPPED
        ):
            raise LeaseInactiveError("Attempt lease execution is inactive.")
        renewed = lease.renew(
            attempt_id=attempt_id,
            worker_session_id=worker_session_id,
            lease_token=lease_token,
            observed_at=self._database_now(),
            lease_seconds=self._lease_seconds,
        )
        row = (
            self._connection.execute(
                attempt_leases.update()
                .where(attempt_leases.c.attempt_id == attempt_id)
                .values(
                    last_renewed_at=renewed.last_renewed_at,
                    lease_expires_at=renewed.lease_expires_at,
                )
                .returning(attempt_leases)
            )
            .mappings()
            .one()
        )
        return _read_lease(row)
