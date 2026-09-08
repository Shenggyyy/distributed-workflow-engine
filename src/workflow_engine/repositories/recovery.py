"""Settle one expired Attempt under the same lock order as Worker operations."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import Connection, func, select

from workflow_engine.domain.runtime import (
    AttemptEvent,
    AttemptStatus,
    RunStatus,
    TaskAttempt,
    TaskStatus,
)
from workflow_engine.domain.timeout import attempt_deadline
from workflow_engine.repositories._ownership import (
    LeaseNotFoundError,
    StoredLeaseError,
    lock_ownership,
)
from workflow_engine.repositories._retry import execution_policy, failed_task
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    WorkflowRepository,
)
from workflow_engine.schema import task_attempts, task_runs


class RecoveryRepository:
    """One Attempt per fresh READ COMMITTED transaction; never commit here.

    Lock Run -> Worker -> Task -> Attempt -> lease. Do not call after holding
    Scheduler Task locks or reuse the transaction for another Attempt.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._transaction = connection.get_transaction()
        # Reuse the common transaction and isolation contract.
        WorkflowRepository(connection)

    def _database_now(self) -> datetime:
        observed: datetime = self._connection.execute(
            select(func.clock_timestamp())
        ).scalar_one()
        return observed

    def recover(self, attempt_id: UUID) -> TaskAttempt | None:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "Recovery requires its original transaction."
            )
        if type(attempt_id) is not UUID:
            raise TypeError("Attempt ID must be a UUID.")
        try:
            owned = lock_ownership(self._connection, attempt_id)
        except LeaseNotFoundError:
            return None
        if owned.attempt.status is not AttemptStatus.RUNNING:
            return None
        if (
            owned.run.status is not RunStatus.RUNNING
            or owned.task.status is not TaskStatus.RUNNING
        ):
            raise StoredLeaseError("Running Attempt has inconsistent recovery state.")
        deadline = attempt_deadline(
            owned.lease, execution_policy(self._connection, owned)
        )
        observed = self._database_now()
        if observed < owned.lease.last_renewed_at or observed < min(
            deadline, owned.lease.lease_expires_at
        ):
            return None
        # Choose the earliest expiry, not the eventual scan time. Ties are timeout.
        event = (
            AttemptEvent.DEADLINE_EXCEEDED
            if deadline <= owned.lease.lease_expires_at
            else AttemptEvent.LEASE_EXPIRED
        )
        attempt = owned.attempt.transition(event)
        task = failed_task(self._connection, owned, attempt, observed)
        for table, snapshot in ((task_attempts, attempt), (task_runs, task)):
            expected = snapshot.model_dump()
            stored = (
                self._connection.execute(
                    table.update()
                    .where(table.c.id == snapshot.id)
                    .values(status=snapshot.status.value)
                    .returning(*(table.c[name] for name in expected))
                )
                .mappings()
                .one_or_none()
            )
            if stored is None or dict(stored) != expected:
                raise StoredLeaseError("Stored recovery transition differs.")
        return attempt
