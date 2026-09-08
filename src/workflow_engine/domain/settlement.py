"""Pure all-success dependency failure propagation and terminal Run aggregation."""

from dataclasses import dataclass

from workflow_engine.domain.readiness import validate_snapshot
from workflow_engine.domain.runtime import (
    RunEvent,
    RunStatus,
    TaskEvent,
    TaskRun,
    TaskStatus,
    WorkflowRun,
)
from workflow_engine.domain.workflow import WorkflowDefinition


@dataclass(frozen=True, slots=True)
class Settlement:
    skipped: tuple[TaskRun, ...]
    run: WorkflowRun


def settle(
    definition: WorkflowDefinition, run: WorkflowRun, tasks: tuple[TaskRun, ...]
) -> Settlement:
    """Cascade permanent failure, then wait for every independent branch to settle."""
    current, by_key, graph = validate_snapshot(definition, run, tasks)
    if current.status is not RunStatus.RUNNING:
        return Settlement((), current)
    skipped = []
    for key in graph.topological_order:
        task = by_key[key]
        if task.status is TaskStatus.PENDING and any(
            by_key[parent].status in {TaskStatus.FAILED, TaskStatus.SKIPPED}
            for parent in graph.dependencies[key]
        ):
            task = task.transition(TaskEvent.DEPENDENCY_FAILED)
            by_key[key] = task
            skipped.append(task)
    if all(task.is_terminal for task in by_key.values()):
        event = (
            RunEvent.ALL_TASKS_SUCCEEDED
            if all(task.status is TaskStatus.SUCCEEDED for task in by_key.values())
            else RunEvent.TASKS_SETTLED_WITH_FAILURE
        )
        current = current.transition(event)
    return Settlement(tuple(skipped), current)
