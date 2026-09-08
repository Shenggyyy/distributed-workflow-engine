"""All-parent success, non-cascading readiness and strict snapshot identity."""

from uuid import uuid4

import pytest

from workflow_engine.domain.readiness import ready_transitions
from workflow_engine.domain.runtime import RunStatus, TaskRun, TaskStatus, WorkflowRun
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition


def snapshot(
    parent_status: TaskStatus,
) -> tuple[WorkflowDefinition, WorkflowRun, tuple[TaskRun, ...]]:
    definition = WorkflowDefinition(
        name="diamond",
        tasks=(
            TaskDefinition(task_id="A", task_type="demo.echo"),
            TaskDefinition(task_id="B", task_type="demo.echo"),
            TaskDefinition(task_id="C", task_type="demo.echo", depends_on=("A", "B")),
            TaskDefinition(task_id="D", task_type="demo.echo", depends_on=("C",)),
        ),
    )
    run = WorkflowRun(id=uuid4(), workflow_version_id=uuid4(), status=RunStatus.RUNNING)
    tasks = tuple(
        TaskRun(
            id=uuid4(),
            run_id=run.id,
            task_key=key,
            status=parent_status
            if key == "A"
            else TaskStatus.SUCCEEDED
            if key == "B"
            else TaskStatus.PENDING,
        )
        for key in ("A", "B", "C", "D")
    )
    return definition, run, tasks


@pytest.mark.parametrize("status", list(TaskStatus))
def test_all_direct_parents_must_succeed(status: TaskStatus) -> None:
    definition, run, tasks = snapshot(status)
    ready = ready_transitions(definition, run, tasks)
    expected = (
        ("C",)
        if status is TaskStatus.SUCCEEDED
        else ("A",)
        if status is TaskStatus.PENDING
        else ()
    )
    assert tuple(task.task_key for task in ready) == expected
    assert all(task.status is TaskStatus.READY for task in ready)
    assert tasks[2].status is TaskStatus.PENDING


def test_reconciliation_is_idempotent() -> None:
    definition, run, tasks = snapshot(TaskStatus.SUCCEEDED)
    changes = {task.id: task for task in ready_transitions(definition, run, tasks)}
    assert (
        ready_transitions(
            definition, run, tuple(changes.get(task.id, task) for task in tasks)
        )
        == ()
    )


@pytest.mark.parametrize(
    "status", [RunStatus.PENDING, RunStatus.SUCCEEDED, RunStatus.FAILED]
)
def test_inactive_run_has_no_changes(status: RunStatus) -> None:
    definition, run, tasks = snapshot(TaskStatus.SUCCEEDED)
    assert (
        ready_transitions(definition, run.model_copy(update={"status": status}), tasks)
        == ()
    )


@pytest.mark.parametrize(
    "corruption", ["missing", "duplicate", "wrong_run", "duplicate_id"]
)
def test_inconsistent_snapshot_rejected(corruption: str) -> None:
    definition, run, tasks = snapshot(TaskStatus.SUCCEEDED)
    if corruption == "missing":
        tasks = tasks[:-1]
    elif corruption == "duplicate":
        tasks = (*tasks, tasks[0])
    elif corruption == "wrong_run":
        tasks = (tasks[0].model_copy(update={"run_id": uuid4()}), *tasks[1:])
    else:
        tasks = (tasks[0].model_copy(update={"id": tasks[1].id}), *tasks[1:])
    with pytest.raises(ValueError, match="pinned DAG"):
        ready_transitions(definition, run, tasks)
