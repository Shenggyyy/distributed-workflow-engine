"""Bounded advisory expiry discovery; returned IDs never authorize settlement."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import Connection, func, literal_column, select

from workflow_engine.domain.dag import MAX_TASKS
from workflow_engine.domain.lease import AttemptLease
from workflow_engine.domain.timeout import attempt_deadline
from workflow_engine.repositories._ownership import StoredLeaseError
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


class RecoveryDiscoveryRepository:
    def __init__(self, connection: Connection) -> None:
        self._workflows = WorkflowRepository(connection)
        self._connection = connection
        self._transaction = connection.get_transaction()

    def _require_transaction(self) -> None:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "Recovery discovery requires its original transaction."
            )

    def _database_now(self) -> datetime:
        value: datetime = self._connection.execute(
            select(func.clock_timestamp())
        ).scalar_one()
        return value

    def workers(self, *, limit: int = 100) -> tuple[UUID, ...]:
        self._require_transaction()
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Worker recovery limit must be 1..100.")
        return tuple(
            self._connection.scalars(
                select(worker_sessions.c.id)
                .where(
                    worker_sessions.c.status == literal_column("'ACTIVE'"),
                    worker_sessions.c.heartbeat_expires_at <= self._database_now(),
                )
                .order_by(worker_sessions.c.heartbeat_expires_at, worker_sessions.c.id)
                .limit(limit)
            )
        )

    def attempts(self, run_id: UUID) -> tuple[UUID, ...]:
        self._require_transaction()
        if type(run_id) is not UUID:
            raise TypeError("Run ID must be a UUID.")
        version_id = self._connection.scalar(
            select(workflow_runs.c.workflow_version_id).where(
                workflow_runs.c.id == run_id,
                workflow_runs.c.status == literal_column("'RUNNING'"),
            )
        )
        if version_id is None:
            return ()
        version = self._workflows.get_version(version_id)
        if version is None:
            raise StoredLeaseError("Recovery Workflow version is missing.")
        policies = {
            task.task_id: task.execution_policy for task in version.definition.tasks
        }
        rows = (
            self._connection.execute(
                select(attempt_leases, task_runs.c.task_key)
                .select_from(
                    task_runs.join(
                        task_attempts, task_attempts.c.task_id == task_runs.c.id
                    ).join(
                        attempt_leases,
                        attempt_leases.c.attempt_id == task_attempts.c.id,
                    )
                )
                .where(
                    task_runs.c.run_id == run_id,
                    task_attempts.c.status == literal_column("'RUNNING'"),
                )
                .order_by(task_runs.c.task_key)
                .limit(MAX_TASKS + 1)
            )
            .mappings()
            .all()
        )
        if len(rows) > MAX_TASKS:
            raise StoredLeaseError("Recovery Run exceeds supported Task count.")
        observed = self._database_now()
        candidates = []
        for row in rows:
            try:
                lease = AttemptLease.model_validate(
                    {key: row[key] for key in attempt_leases.c.keys()}
                )
            except ValueError:
                raise StoredLeaseError("Recovery lease data is invalid.") from None
            policy = policies.get(row["task_key"])
            if policy is None:
                raise StoredLeaseError("Recovery Task is absent from its Workflow.")
            if observed >= min(lease.lease_expires_at, attempt_deadline(lease, policy)):
                candidates.append(lease.attempt_id)
        return tuple(candidates)
