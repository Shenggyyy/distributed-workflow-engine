"""Renew current attempt ownership in short, ordered PostgreSQL transactions."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import Connection, func, select

from workflow_engine.domain.lease import MAX_LEASE_SECONDS, AttemptLease
from workflow_engine.repositories._ownership import (
    LeaseInactiveError as LeaseInactiveError,
)
from workflow_engine.repositories._ownership import (
    LeaseNotFoundError as LeaseNotFoundError,
)
from workflow_engine.repositories._ownership import (
    StoredLeaseError as StoredLeaseError,
)
from workflow_engine.repositories._ownership import (
    _read_lease,
    lock_ownership,
)
from workflow_engine.repositories.workflows import RepositoryTransactionError
from workflow_engine.schema import attempt_leases


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
        owned = lock_ownership(self._connection, attempt_id)
        owned.require_running()
        lease = owned.lease
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
