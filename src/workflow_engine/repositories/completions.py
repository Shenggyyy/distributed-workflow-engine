"""Atomically settle one owned Attempt and retain its immutable completion."""

from datetime import datetime

from sqlalchemy import Connection, RowMapping, func, select

from workflow_engine.domain.completion import (
    AttemptCompletion,
    CompletionOutcome,
    CompletionReceipt,
    CompletionResult,
    accept_completion,
)
from workflow_engine.domain.runtime import TaskEvent
from workflow_engine.repositories._ownership import (
    LeaseInactiveError,
    LeaseNotFoundError,
    LockedOwnership,
    StoredLeaseError,
    lock_ownership,
)
from workflow_engine.repositories._retry import failed_task
from workflow_engine.repositories.workflows import RepositoryTransactionError
from workflow_engine.schema import attempt_completions, task_attempts, task_runs


class CompletionNotFoundError(LookupError):
    """No owned Attempt exists for this report, including unleased history."""


class CompletionInactiveError(ValueError):
    """No receipt exists and execution cannot accept a new completion."""


class StoredCompletionError(ValueError):
    """Stored completion or its ownership/execution references are inconsistent."""


def _read_receipt(row: RowMapping, owned: LockedOwnership) -> CompletionReceipt:
    try:
        completion = AttemptCompletion(
            attempt_id=row["attempt_id"],
            worker_session_id=row["worker_session_id"],
            lease_token=row["lease_token"],
            result=CompletionResult(
                outcome=CompletionOutcome(row["outcome"]), error_code=row["error_code"]
            ),
        )
        receipt = CompletionReceipt(
            completion=completion, attempt=owned.attempt, accepted_at=row["accepted_at"]
        )
        # Validate the historical observation, never sample a replay-time clock.
        owned.lease.require_valid_owner(
            attempt_id=completion.attempt_id,
            worker_session_id=completion.worker_session_id,
            lease_token=completion.lease_token,
            observed_at=receipt.accepted_at,
        )
        return receipt
    except (TypeError, ValueError):
        raise StoredCompletionError("Stored completion is invalid.") from None


class CompletionRepository:
    """One report per fresh caller-owned transaction; never commit or retry.

    Requires PostgreSQL READ COMMITTED. Do not share across threads or enter
    while holding locks that reverse Run -> Worker -> Task -> Attempt -> lease.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._transaction = connection.get_transaction()
        self._require_transaction()
        if (
            connection.dialect.name != "postgresql"
            or getattr(connection.connection.driver_connection, "autocommit", True)
            or connection.get_isolation_level() != "READ COMMITTED"
        ):
            raise RepositoryTransactionError(
                "CompletionRepository requires PostgreSQL READ COMMITTED."
            )

    def _require_transaction(self) -> None:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "CompletionRepository requires its original active transaction."
            )

    def _database_now(self) -> datetime:
        observed: datetime = self._connection.execute(
            select(func.clock_timestamp())
        ).scalar_one()
        return observed

    def complete(self, submitted: AttemptCompletion) -> CompletionReceipt:
        """Return a provisional receipt; publish it only after caller COMMIT."""
        self._require_transaction()
        proposed = AttemptCompletion.model_validate(submitted)
        try:
            owned = lock_ownership(self._connection, proposed.attempt_id)
        except LeaseNotFoundError:
            raise CompletionNotFoundError("Owned Attempt was not found.") from None
        except StoredLeaseError:
            raise StoredCompletionError(
                "Stored completion ownership is invalid."
            ) from None

        row = (
            self._connection.execute(
                select(attempt_completions).where(
                    attempt_completions.c.attempt_id == proposed.attempt_id
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            return _read_receipt(row, owned).replay(proposed)

        try:
            owned.require_running()
        except LeaseInactiveError:
            raise CompletionInactiveError("Attempt completion is inactive.") from None
        receipt = accept_completion(
            owned.attempt, owned.lease, proposed, observed_at=self._database_now()
        )
        if proposed.result.outcome is CompletionOutcome.SUCCEEDED:
            task = owned.task.transition(TaskEvent.ATTEMPT_SUCCEEDED)
        else:
            try:
                task = failed_task(
                    self._connection, owned, receipt.attempt, receipt.accepted_at
                )
            except StoredLeaseError:
                raise StoredCompletionError(
                    "Stored retry settlement is invalid."
                ) from None
        # Compare all returned snapshot fields: an unexpected write/trigger result
        # must abort instead of being represented as successful completion.
        for table, snapshot in ((task_attempts, receipt.attempt), (task_runs, task)):
            expected = snapshot.model_dump()
            persisted = (
                self._connection.execute(
                    table.update()
                    .where(table.c.id == snapshot.id)
                    .values(status=snapshot.status.value)
                    .returning(*(table.c[name] for name in expected))
                )
                .mappings()
                .one_or_none()
            )
            if persisted is None or dict(persisted) != expected:
                raise StoredCompletionError("Stored completion transition differs.")
        stored = (
            self._connection.execute(
                attempt_completions.insert()
                .values(
                    attempt_id=proposed.attempt_id,
                    worker_session_id=proposed.worker_session_id,
                    lease_token=proposed.lease_token,
                    outcome=proposed.result.outcome.value,
                    error_code=proposed.result.error_code,
                    accepted_at=receipt.accepted_at,
                )
                .returning(attempt_completions)
            )
            .mappings()
            .one()
        )
        settled = LockedOwnership(
            owned.run, owned.worker, task, receipt.attempt, owned.lease
        )
        confirmed = _read_receipt(stored, settled)
        if confirmed != receipt:
            raise StoredCompletionError("Stored completion receipt differs.")
        return confirmed
