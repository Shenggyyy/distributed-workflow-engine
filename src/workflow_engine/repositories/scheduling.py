"""Readiness reconciliation coordinated by the existing per-Run transaction lock."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import Connection, func, select

from workflow_engine.domain.dag import MAX_TASKS
from workflow_engine.domain.readiness import ready_transitions
from workflow_engine.domain.runtime import RunStatus, TaskRun, TaskStatus, WorkflowRun
from workflow_engine.repositories._retry import due_tasks
from workflow_engine.repositories.runs import StoredRuntimeError
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    WorkflowRepository,
)
from workflow_engine.schema import task_runs, workflow_runs


class SchedulingRunNotFoundError(LookupError):
    """Requested Run is absent."""


class SchedulingRepository:
    """Reconcile one Run per fresh READ COMMITTED transaction; never commit here.

    Lock Run -> Tasks in task-key order. No Worker/Attempt/lease lock is acquired
    afterwards. All execution writers hold the same Run first, so completion and
    readiness observe one coherent parent-state snapshot without reverse locking.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._workflows = WorkflowRepository(connection)
        self._transaction = connection.get_transaction()

    def _database_now(self) -> datetime:
        observed: datetime = self._connection.execute(
            select(func.clock_timestamp())
        ).scalar_one()
        return observed

    def reconcile(
        self, run_id: UUID, *, skip_locked: bool = False
    ) -> tuple[TaskRun, ...]:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "Scheduler requires its original active transaction."
            )
        if not isinstance(run_id, UUID):
            raise TypeError("run_id must be a UUID.")
        if type(skip_locked) is not bool:
            raise TypeError("skip_locked must be a boolean.")
        row = (
            self._connection.execute(
                select(workflow_runs)
                .where(workflow_runs.c.id == run_id)
                .with_for_update(skip_locked=skip_locked)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            if skip_locked:
                # Advisory scans treat missing and busy Runs as no work this pass.
                return ()
            raise SchedulingRunNotFoundError("Run was not found.")
        try:
            run = WorkflowRun(
                id=row["id"],
                workflow_version_id=row["workflow_version_id"],
                status=RunStatus(row["status"]),
            )
        except (TypeError, ValueError):
            raise StoredRuntimeError("Stored scheduling Run is invalid.") from None
        if run.status is not RunStatus.RUNNING:
            return ()
        version = self._workflows.get_version(run.workflow_version_id)
        if version is None:
            raise StoredRuntimeError("Pinned workflow version is missing.")
        rows = (
            self._connection.execute(
                select(task_runs)
                .where(task_runs.c.run_id == run_id)
                .order_by(task_runs.c.task_key)
                .limit(MAX_TASKS + 1)
                .with_for_update()
            )
            .mappings()
            .all()
        )
        if len(rows) > MAX_TASKS:
            raise StoredRuntimeError("Stored Run exceeds the supported task limit.")
        try:
            tasks = tuple(
                TaskRun(
                    id=task["id"],
                    run_id=task["run_id"],
                    task_key=task["task_key"],
                    status=TaskStatus(task["status"]),
                )
                for task in rows
            )
            ready = ready_transitions(version.definition, run, tasks)
            if any(task.status is TaskStatus.RETRY_WAIT for task in tasks):
                ready += due_tasks(
                    self._connection, version.definition, tasks, self._database_now()
                )
        except (TypeError, ValueError):
            raise StoredRuntimeError(
                "Stored scheduling snapshot is inconsistent."
            ) from None
        if not ready:
            return ()
        changed = (
            self._connection.execute(
                task_runs.update()
                .where(
                    task_runs.c.id.in_([task.id for task in ready]),
                    task_runs.c.status.in_(
                        [TaskStatus.PENDING.value, TaskStatus.RETRY_WAIT.value]
                    ),
                )
                .values(status=TaskStatus.READY.value)
                .returning(task_runs)
            )
            .mappings()
            .all()
        )
        expected = {task.id: task for task in ready}
        try:
            actual = {
                row["id"]: TaskRun(
                    id=row["id"],
                    run_id=row["run_id"],
                    task_key=row["task_key"],
                    status=TaskStatus(row["status"]),
                )
                for row in changed
            }
            if len(changed) != len(ready) or actual != expected:
                raise ValueError()
        except (TypeError, ValueError):
            raise StoredRuntimeError(
                "Readiness update did not match its proposal."
            ) from None
        return ready
