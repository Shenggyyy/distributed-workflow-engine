"""Worker registration in caller-owned PostgreSQL READ COMMITTED transactions."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import blake2s
from uuid import UUID

from sqlalchemy import Connection, RowMapping, func, select, text

from workflow_engine.domain.worker import WorkerEvent, WorkerSession, WorkerStatus
from workflow_engine.repositories.workflows import RepositoryTransactionError
from workflow_engine.schema import worker_sessions

# Two-int advisory keys have a separate PostgreSQL key space from the bigint
# migration lock. Keep the namespace and hash stable across rolling deployments.
_REGISTRATION_LOCK_NAMESPACE = 0x44574557


def _registration_lock_key(session_id: UUID) -> int:
    return int.from_bytes(
        blake2s(session_id.bytes, digest_size=4).digest(), signed=True
    )


class WorkerRegistrationConflictError(ValueError):
    """The session UUID is already registered with different immutable fields."""


class StoredWorkerError(ValueError):
    """Persisted worker data does not satisfy the supported snapshot contract."""


class WorkerSessionNotFoundError(LookupError):
    """The requested session is not visible to this transaction."""


class WorkerSessionInactiveError(ValueError):
    """A terminal session cannot accept a heartbeat."""


class WorkerSessionExpiredError(ValueError):
    """An ACTIVE session has already reached its heartbeat deadline."""


class WorkerClockRegressionError(ValueError):
    """Database time precedes the last accepted heartbeat observation."""


@dataclass(frozen=True, slots=True)
class StoredWorkerSession:
    """Current persisted snapshot, provisional until the caller commits."""

    session: WorkerSession
    created_at: datetime
    last_heartbeat_at: datetime
    heartbeat_expires_at: datetime


def _read_session(row: RowMapping) -> StoredWorkerSession:
    try:
        session = WorkerSession(
            id=row["id"],
            worker_name=row["worker_name"],
            max_concurrency=row["max_concurrency"],
            status=WorkerStatus(row["status"]),
        )
        times = (
            row["created_at"],
            row["last_heartbeat_at"],
            row["heartbeat_expires_at"],
        )
        if not all(
            isinstance(t, datetime) and t.utcoffset() is not None for t in times
        ):
            raise ValueError
        if not times[0] <= times[1] < times[2]:
            raise ValueError
    except (ValueError, TypeError):
        raise StoredWorkerError("Stored worker session is invalid.") from None
    return StoredWorkerSession(session, times[0], times[1], times[2])


class WorkerRepository:
    """Register, renew or expire sessions; never commit, retry or run a loop.

    Reuse only within the original transaction, and never across threads.
    heartbeat_timeout_seconds is server policy, not part of request identity.
    """

    def __init__(
        self, connection: Connection, *, heartbeat_timeout_seconds: int = 30
    ) -> None:
        if (
            type(heartbeat_timeout_seconds) is not int
            or not 1 <= heartbeat_timeout_seconds <= 86400
        ):
            raise ValueError(
                "Heartbeat timeout must be an integer from 1 to 86400 seconds."
            )
        self._connection = connection
        self._transaction = connection.get_transaction()
        self._require_transaction()
        if (
            connection.dialect.name != "postgresql"
            or getattr(connection.connection.driver_connection, "autocommit", True)
            or connection.get_isolation_level() != "READ COMMITTED"
        ):
            raise RepositoryTransactionError(
                "WorkerRepository requires PostgreSQL READ COMMITTED."
            )
        self._heartbeat_timeout = timedelta(seconds=heartbeat_timeout_seconds)

    def _database_now(self) -> datetime:
        observed_at: datetime = self._connection.execute(
            select(func.clock_timestamp())
        ).scalar_one()
        return observed_at

    def _lock_session(self, session_id: UUID) -> StoredWorkerSession:
        self._require_transaction()
        if not isinstance(session_id, UUID):
            raise TypeError("session_id must be a UUID.")
        row = (
            self._connection.execute(
                select(worker_sessions)
                .where(worker_sessions.c.id == session_id)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise WorkerSessionNotFoundError("Worker session was not found.")
        return _read_session(row)

    def _observe_active(self, current: StoredWorkerSession) -> datetime:
        observed_at = self._database_now()
        if observed_at < current.last_heartbeat_at:
            raise WorkerClockRegressionError(
                "Database heartbeat clock moved backwards."
            )
        return observed_at

    def heartbeat(self, session_id: UUID) -> StoredWorkerSession:
        """Renew a live ACTIVE session; rejected heartbeats never mutate state."""
        current = self._lock_session(session_id)
        if current.session.is_terminal:
            raise WorkerSessionInactiveError("Worker session is not active.")
        observed_at = self._observe_active(current)
        if observed_at >= current.heartbeat_expires_at:
            raise WorkerSessionExpiredError("Worker heartbeat deadline has elapsed.")
        # A smaller server timeout must not shorten an already accepted deadline.
        expires_at = max(
            current.heartbeat_expires_at, observed_at + self._heartbeat_timeout
        )
        row = (
            self._connection.execute(
                worker_sessions.update()
                .where(worker_sessions.c.id == session_id)
                .values(last_heartbeat_at=observed_at, heartbeat_expires_at=expires_at)
                .returning(worker_sessions)
            )
            .mappings()
            .one()
        )
        return _read_session(row)

    def expire(self, session_id: UUID) -> StoredWorkerSession:
        """Mark a due ACTIVE session LOST; otherwise return its locked snapshot."""
        current = self._lock_session(session_id)
        if current.session.is_terminal:
            return current
        observed_at = self._observe_active(current)
        if observed_at < current.heartbeat_expires_at:
            return current
        expired = current.session.transition(WorkerEvent.HEARTBEAT_EXPIRED)
        row = (
            self._connection.execute(
                worker_sessions.update()
                .where(worker_sessions.c.id == session_id)
                .values(status=expired.status.value)
                .returning(worker_sessions)
            )
            .mappings()
            .one()
        )
        return _read_session(row)

    def _require_transaction(self) -> None:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "WorkerRepository requires its original active transaction."
            )

    def register(
        self, session_id: UUID, *, worker_name: str, max_concurrency: int
    ) -> StoredWorkerSession:
        """Create ACTIVE or return the existing snapshot without renewing it."""
        self._require_transaction()
        if not isinstance(session_id, UUID):
            raise TypeError("session_id must be a UUID.")
        requested = WorkerSession(
            id=session_id, worker_name=worker_name, max_concurrency=max_concurrency
        )
        self._connection.execute(
            text("SELECT pg_advisory_xact_lock(:namespace, :key)"),
            {
                "namespace": _REGISTRATION_LOCK_NAMESPACE,
                "key": _registration_lock_key(session_id),
            },
        )
        # A separate statement sees the preceding registrant's committed row.
        # The row lock also serializes this snapshot with future heartbeat writes.
        row = (
            self._connection.execute(
                select(worker_sessions)
                .where(worker_sessions.c.id == session_id)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            existing = _read_session(row)
            if (
                existing.session.worker_name != requested.worker_name
                or existing.session.max_concurrency != requested.max_concurrency
            ):
                raise WorkerRegistrationConflictError(
                    "Worker session is already registered with different fields."
                )
            return existing

        # Sample after registration/row locks, not transaction start. A waiting
        # contender whose owner rolled back gets a fresh full initial deadline.
        observed_at = self._database_now()
        row = (
            self._connection.execute(
                worker_sessions.insert()
                .values(
                    id=requested.id,
                    worker_name=requested.worker_name,
                    max_concurrency=requested.max_concurrency,
                    status=requested.status.value,
                    created_at=observed_at,
                    last_heartbeat_at=observed_at,
                    heartbeat_expires_at=observed_at + self._heartbeat_timeout,
                )
                .returning(worker_sessions)
            )
            .mappings()
            .one()
        )
        return _read_session(row)
