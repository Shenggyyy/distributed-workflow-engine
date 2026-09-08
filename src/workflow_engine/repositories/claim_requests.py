"""Bind one poll to an allocation and replay only its current valid ownership."""

from datetime import datetime
from hashlib import blake2s
from uuid import UUID

from sqlalchemy import Connection, RowMapping, func, select, text

from workflow_engine.domain.lease import LeaseExpiredError
from workflow_engine.domain.timeout import AttemptTimeoutError, require_before_timeout
from workflow_engine.repositories._ownership import (
    LeaseInactiveError,
    LeaseNotFoundError,
    StoredLeaseError,
    lock_ownership,
)
from workflow_engine.repositories.claims import ClaimRepository, TaskClaim
from workflow_engine.repositories.runs import StoredRuntimeError
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    WorkflowRepository,
)
from workflow_engine.schema import claim_requests

# Stable across processes/releases. Distinct from Worker registration's DWEW
# namespace (0x44574557) and PostgreSQL's separate bigint migration lock space.
_CLAIM_LOCK_NAMESPACE = 0x44574543


def _claim_lock_key(worker_session_id: UUID, request_id: UUID) -> int:
    return int.from_bytes(
        blake2s(worker_session_id.bytes + request_id.bytes, digest_size=4).digest(),
        signed=True,
    )


class ClaimRequestConflictError(ValueError):
    """This session/request identity already targets a different Run."""


class ClaimReplayUnavailableError(ValueError):
    """The bound allocation is no longer executable; never allocate in its place."""


class StoredClaimRequestError(ValueError):
    """Stored request data or its ownership relationship is invalid."""


def _validate_binding(row: RowMapping) -> None:
    if (
        any(
            type(row[key]) is not UUID
            for key in ("worker_session_id", "request_id", "run_id")
        )
        or (row["attempt_id"] is not None and type(row["attempt_id"]) is not UUID)
        or not isinstance(row["created_at"], datetime)
        or row["created_at"].utcoffset() is None
    ):
        raise StoredClaimRequestError("Stored claim request is invalid.")


class ClaimRequestRepository:
    """One request per fresh caller transaction; no commit, retry or renewal.

    Request lock precedes Run -> Worker -> Task -> Attempt -> lease. Do not call
    while holding other control locks or combine requests in one transaction.
    """

    def __init__(self, connection: Connection, *, lease_seconds: int = 30) -> None:
        # Reuse allocation admission, policy and transaction-mode validation.
        self._claims = ClaimRepository(connection, lease_seconds=lease_seconds)
        self._connection = connection
        self._transaction = connection.get_transaction()

    def _require_transaction(self) -> None:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "ClaimRequestRepository requires its original active transaction."
            )

    def _database_now(self) -> datetime:
        observed: datetime = self._connection.execute(
            select(func.clock_timestamp())
        ).scalar_one()
        return observed

    def claim_next(
        self, run_id: UUID, worker_session_id: UUID, *, request_id: UUID
    ) -> TaskClaim | None:
        """Return provisional live ownership or sticky no-work; COMMIT is required."""
        self._require_transaction()
        if any(
            type(value) is not UUID for value in (run_id, worker_session_id, request_id)
        ):
            raise TypeError(
                "Run, Worker session and request IDs must be UUID instances."
            )
        self._connection.execute(
            text("SELECT pg_advisory_xact_lock(:namespace, :key)"),
            {
                "namespace": _CLAIM_LOCK_NAMESPACE,
                "key": _claim_lock_key(worker_session_id, request_id),
            },
        )
        # Separate statement after a wait observes the predecessor's commit.
        # The full key determines identity; hash collisions only serialize work.
        row = (
            self._connection.execute(
                select(claim_requests).where(
                    claim_requests.c.worker_session_id == worker_session_id,
                    claim_requests.c.request_id == request_id,
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            _validate_binding(row)
            if row["run_id"] != run_id:
                raise ClaimRequestConflictError(
                    "Claim request targets a different Run."
                )
            if row["attempt_id"] is None:
                return None
            return self._replay(run_id, worker_session_id, row["attempt_id"])

        result = self._claims.claim_next(run_id, worker_session_id)
        # Store a completed result only, in the allocation's transaction. Even an
        # empty poll is immutable. Failure here must roll back the allocation too.
        self._connection.execute(
            claim_requests.insert().values(
                worker_session_id=worker_session_id,
                request_id=request_id,
                run_id=run_id,
                attempt_id=None if result is None else result.attempt.id,
                created_at=self._database_now(),
            )
        )
        return result

    def _replay(
        self, run_id: UUID, worker_session_id: UUID, attempt_id: UUID
    ) -> TaskClaim:
        try:
            owned = lock_ownership(self._connection, attempt_id)
        except (LeaseNotFoundError, StoredLeaseError):
            raise StoredClaimRequestError(
                "Stored claim ownership is invalid."
            ) from None
        if owned.run.id != run_id or owned.worker.id != worker_session_id:
            raise StoredClaimRequestError(
                "Stored claim ownership references do not match."
            )
        try:
            owned.require_running()
        except LeaseInactiveError:
            raise ClaimReplayUnavailableError(
                "Bound claim is no longer available."
            ) from None
        version = WorkflowRepository(self._connection).get_version(
            owned.run.workflow_version_id
        )
        if version is None:
            raise StoredRuntimeError("Run's Workflow version is missing.")
        definition = next(
            (
                node
                for node in version.definition.tasks
                if node.task_id == owned.task.task_key
            ),
            None,
        )
        if definition is None:
            raise StoredRuntimeError("Task is absent from its pinned Workflow.")
        try:
            # The binding authorizes retrieval, not a submitted renewal token.
            # Read the token only after validating that exact retained allocation.
            observed = self._database_now()
            owned.lease.require_valid_owner(
                attempt_id=attempt_id,
                worker_session_id=worker_session_id,
                lease_token=owned.lease.lease_token,
                observed_at=observed,
            )
            require_before_timeout(owned.lease, definition.execution_policy, observed)
        except (LeaseExpiredError, AttemptTimeoutError):
            raise ClaimReplayUnavailableError(
                "Bound claim is no longer available."
            ) from None
        return TaskClaim(version.id, owned.task, owned.attempt, owned.lease, definition)
