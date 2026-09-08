"""Pure readiness decisions from one consistent Run snapshot."""

from workflow_engine.domain.runtime import (
    RunStatus,
    TaskEvent,
    TaskRun,
    TaskStatus,
    WorkflowRun,
)
from workflow_engine.domain.workflow import WorkflowDefinition


def ready_transitions(
    definition: WorkflowDefinition, run: WorkflowRun, tasks: tuple[TaskRun, ...]
) -> tuple[TaskRun, ...]:
    """Return only PENDING -> READY proposals; never assume a parent executed."""
    specification = WorkflowDefinition.model_validate(definition)
    current_run = WorkflowRun.model_validate(run)
    current_tasks = tuple(TaskRun.model_validate(task) for task in tasks)
    by_key = {task.task_key: task for task in current_tasks}
    graph = specification.dag()
    if (
        len(by_key) != len(current_tasks)
        or len({task.id for task in current_tasks}) != len(current_tasks)
        or set(by_key) != set(graph.topological_order)
        or any(task.run_id != current_run.id for task in current_tasks)
    ):
        raise ValueError("Run tasks do not match the pinned DAG.")
    if current_run.status is not RunStatus.RUNNING:
        return ()
    return tuple(
        by_key[key].transition(TaskEvent.DEPENDENCIES_SUCCEEDED)
        for key in graph.topological_order
        if by_key[key].status is TaskStatus.PENDING
        and all(
            by_key[parent].status is TaskStatus.SUCCEEDED
            for parent in graph.dependencies[key]
        )
    )
