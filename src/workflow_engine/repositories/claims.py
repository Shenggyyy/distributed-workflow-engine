"""Single-run claims in caller-owned PostgreSQL READ COMMITTED transactions."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import Connection, func, select

from workflow_engine.domain.lease import MAX_LEASE_SECONDS, AttemptLease
from workflow_engine.domain.runtime import (
    AttemptStatus,
    RunStatus,
    TaskAttempt,
    TaskEvent,
    TaskRun,
    TaskStatus,
    WorkflowRun,
)
from workflow_engine.domain.worker import WorkerSession, WorkerStatus
from workflow_engine.domain.workflow import TaskDefinition
from workflow_engine.repositories.runs import StoredRuntimeError
from workflow_engine.repositories.workers import (
    StoredWorkerError,
    WorkerClockRegressionError,
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


class ClaimRunNotFoundError(LookupError):
    """The requested Run is not visible."""


class ClaimRunInactiveError(ValueError):
    """Only a RUNNING Run can issue claims."""


class AttemptNumberExhaustedError(ValueError):
    """Allocating another attempt would exceed PostgreSQL INTEGER."""


@dataclass(frozen=True, slots=True)
class TaskClaim:
    """Provisional claim; never execute or publish before caller COMMIT succeeds."""

    workflow_version_id: UUID
    task: TaskRun
    attempt: TaskAttempt
    lease: AttemptLease = field(repr=False)
    definition: TaskDefinition = field(repr=False)


class ClaimRepository:
    """Claim one task in one Run; never commit, retry, renew or execute it.

    Use a fresh transaction for each claim. Do not share instances across threads
    or acquire another Run after holding Worker/Task/Attempt locks.
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
                "ClaimRepository requires PostgreSQL READ COMMITTED."
            )
        self._lease_seconds = lease_seconds

    def _require_transaction(self) -> None:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "ClaimRepository requires its original active transaction."
            )

    def _database_now(self) -> datetime:
        observed: datetime = self._connection.execute(
            select(func.clock_timestamp())
        ).scalar_one()
        return observed

    @staticmethod
    def _check_time(
        observed: datetime, last_heartbeat: datetime, deadline: datetime
    ) -> None:
        if observed < last_heartbeat:
            raise WorkerClockRegressionError(
                "Database heartbeat clock moved backwards."
            )
        if observed >= deadline:
            raise WorkerSessionExpiredError("Worker heartbeat deadline has elapsed.")

    def claim_next(self, run_id: UUID, worker_session_id: UUID) -> TaskClaim | None:
        """Return one provisional claim, or None for capacity/no READY task.

        Repeated calls are new allocations, not idempotent request replays.
        All exceptions must roll back the caller's transaction or savepoint.
        """
        self._require_transaction()
        if not isinstance(run_id, UUID) or not isinstance(worker_session_id, UUID):
            raise TypeError("Run and Worker session IDs must be UUID instances.")
        row = (
            self._connection.execute(
                select(workflow_runs)
                .where(workflow_runs.c.id == run_id)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ClaimRunNotFoundError("Run was not found.")
        try:
            run = WorkflowRun(
                id=row["id"],
                workflow_version_id=row["workflow_version_id"],
                status=RunStatus(row["status"]),
            )
        except (ValueError, TypeError):
            raise StoredRuntimeError("Stored Run is invalid.") from None
        if run.status is not RunStatus.RUNNING:
            raise ClaimRunInactiveError("Run is not active.")

        row = (
            self._connection.execute(
                select(worker_sessions)
                .where(worker_sessions.c.id == worker_session_id)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise WorkerSessionNotFoundError("Worker session was not found.")
        try:
            worker = WorkerSession(
                id=row["id"],
                worker_name=row["worker_name"],
                max_concurrency=row["max_concurrency"],
                status=WorkerStatus(row["status"]),
            )
            created, last, deadline = (
                row["created_at"],
                row["last_heartbeat_at"],
                row["heartbeat_expires_at"],
            )
            if (
                not all(
                    isinstance(t, datetime) and t.utcoffset() is not None
                    for t in (created, last, deadline)
                )
                or not created <= last < deadline
            ):
                raise ValueError
        except (ValueError, TypeError):
            raise StoredWorkerError("Stored Worker session is invalid.") from None
        if worker.is_terminal:
            raise WorkerSessionInactiveError("Worker session is not active.")
        first_observed = self._database_now()
        self._check_time(first_observed, last, deadline)

        # Fresh statement after the Worker lock sees other Runs' committed claims.
        outstanding = self._connection.execute(
            select(func.count())
            .select_from(
                attempt_leases.join(
                    task_attempts, attempt_leases.c.attempt_id == task_attempts.c.id
                )
            )
            .where(
                attempt_leases.c.worker_session_id == worker_session_id,
                task_attempts.c.status == AttemptStatus.RUNNING.value,
            )
        ).scalar_one()
        if outstanding >= worker.max_concurrency:
            return None
        row = (
            self._connection.execute(
                select(task_runs)
                .where(
                    task_runs.c.run_id == run_id,
                    task_runs.c.status == TaskStatus.READY.value,
                )
                .order_by(task_runs.c.task_key, task_runs.c.id)
                .limit(1)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        try:
            task = TaskRun(
                id=row["id"],
                run_id=row["run_id"],
                task_key=row["task_key"],
                status=TaskStatus(row["status"]),
            )
        except (ValueError, TypeError):
            raise StoredRuntimeError("Stored Task is invalid.") from None
        version = WorkflowRepository(self._connection).get_version(
            run.workflow_version_id
        )
        if version is None:
            raise StoredRuntimeError("Run's Workflow version is missing.")
        definition = next(
            (
                node
                for node in version.definition.tasks
                if node.task_id == task.task_key
            ),
            None,
        )
        if definition is None:
            raise StoredRuntimeError("Task is absent from its pinned Workflow.")
        if definition.depends_on:
            states = dict(
                self._connection.execute(
                    select(task_runs.c.task_key, task_runs.c.status).where(
                        task_runs.c.run_id == run_id,
                        task_runs.c.task_key.in_(definition.depends_on),
                    )
                )
                .tuples()
                .all()
            )
            if any(
                states.get(key) != TaskStatus.SUCCEEDED.value
                for key in definition.depends_on
            ):
                raise StoredRuntimeError("READY Task dependencies are not satisfied.")
        prior = self._connection.execute(
            select(
                func.max(task_attempts.c.attempt_number),
                func.count().filter(
                    task_attempts.c.status == AttemptStatus.RUNNING.value
                ),
            ).where(task_attempts.c.task_id == task.id)
        ).one()
        if prior[1]:
            raise StoredRuntimeError("READY Task already has a RUNNING Attempt.")
        number = (prior[0] or 0) + 1
        if number > 2_147_483_647:
            raise AttemptNumberExhaustedError("Task attempt number limit was reached.")
        # Recheck after the Task lock and all potentially blocking reads.
        observed = self._database_now()
        self._check_time(observed, max(last, first_observed), deadline)
        claimed = task.transition(TaskEvent.CLAIM)
        attempt = TaskAttempt(id=uuid4(), task_id=task.id, attempt_number=number)
        lease = AttemptLease(
            attempt_id=attempt.id,
            worker_session_id=worker_session_id,
            lease_token=uuid4(),
            acquired_at=observed,
            last_renewed_at=observed,
            lease_expires_at=observed + timedelta(seconds=self._lease_seconds),
        )
        changed = self._connection.execute(
            task_runs.update()
            .where(
                task_runs.c.id == task.id, task_runs.c.status == TaskStatus.READY.value
            )
            .values(status=claimed.status.value)
        )
        if changed.rowcount != 1:
            raise StoredRuntimeError("Task claim state update did not affect one row.")
        self._connection.execute(
            task_attempts.insert().values(
                **{**attempt.model_dump(), "status": attempt.status.value}
            )
        )
        self._connection.execute(attempt_leases.insert().values(**lease.model_dump()))
        return TaskClaim(run.workflow_version_id, claimed, attempt, lease, definition)
