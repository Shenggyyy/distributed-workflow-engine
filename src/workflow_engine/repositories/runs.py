"""Initialize a pinned workflow run in one caller-owned transaction."""

from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy import Connection

from workflow_engine.domain.runtime import (
    RunEvent,
    TaskEvent,
    TaskRun,
    WorkflowRun,
)
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import task_runs, workflow_runs


class WorkflowVersionNotFoundError(LookupError):
    """The requested immutable version is not visible to this transaction."""


@dataclass(frozen=True, slots=True)
class CreatedRun:
    """Provisional initialization snapshots; durable only after caller commit."""

    run: WorkflowRun
    tasks: tuple[TaskRun, ...]


class RunRepository:
    """Create inside engine.begin(); propagate failures out of the transaction.

    There is no internal commit, retry, idempotency, dispatch or attempt creation.
    Each call creates fresh identities. Do not share instances across threads.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        # Reuse validated version reads and their original-transaction guard.
        # Every create starts with this repository read, before any runtime write.
        self._workflows = WorkflowRepository(connection)

    def create(self, workflow_version_id: UUID) -> CreatedRun:
        """Pin a version, persist all nodes and activate roots atomically."""
        version = self._workflows.get_version(workflow_version_id)
        if version is None:
            raise WorkflowVersionNotFoundError("Workflow version was not found.")
        graph = version.definition.dag()
        roots = frozenset(graph.roots)
        pending_run = WorkflowRun(id=uuid4(), workflow_version_id=version.id)
        pending_tasks = tuple(
            TaskRun(id=uuid4(), run_id=pending_run.id, task_key=key)
            for key in graph.topological_order
        )
        # Resolve all domain transitions before writing. These roots have no
        # prerequisites; no task completion or execution is inferred here.
        initialized_tasks = tuple(
            task.transition(TaskEvent.DEPENDENCIES_SUCCEEDED)
            if task.task_key in roots
            else task
            for task in pending_tasks
        )
        active_run = pending_run.transition(RunEvent.START)

        self._connection.execute(
            workflow_runs.insert().values(
                id=pending_run.id,
                workflow_version_id=version.id,
                status=pending_run.status.value,
            )
        )
        self._connection.execute(
            task_runs.insert(),
            [
                {
                    "id": task.id,
                    "run_id": task.run_id,
                    "task_key": task.task_key,
                    "status": task.status.value,
                }
                for task in pending_tasks
            ],
        )
        # Persist actual PENDING -> READY/RUNNING edges, checked again by the
        # database guards. New rows are private to this creating transaction.
        ready_roots = tuple(
            task for task in initialized_tasks if task.task_key in roots
        )
        self._connection.execute(
            task_runs.update()
            .where(task_runs.c.id.in_([task.id for task in ready_roots]))
            .values(status=ready_roots[0].status.value)
        )
        self._connection.execute(
            workflow_runs.update()
            .where(workflow_runs.c.id == active_run.id)
            .values(status=active_run.status.value)
        )
        return CreatedRun(run=active_run, tasks=initialized_tasks)
