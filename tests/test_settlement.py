"""Settlement preserves independent work, retries and immutable snapshots."""

from uuid import uuid4

import pytest

from tests.test_readiness import snapshot
from workflow_engine.domain.runtime import RunStatus, TaskStatus
from workflow_engine.domain.settlement import settle


@pytest.mark.parametrize("status", list(TaskStatus))
def test_only_permanent_failure_cascades(status: TaskStatus) -> None:
    definition, run, tasks = snapshot(status)
    result = settle(definition, run, tasks)
    failure = status in {TaskStatus.FAILED, TaskStatus.SKIPPED}
    assert tuple(task.task_key for task in result.skipped) == (
        ("C", "D") if failure else ()
    )
    assert result.run.status is (RunStatus.FAILED if failure else RunStatus.RUNNING)
    assert tasks[2].status is TaskStatus.PENDING
    updated = {task.id: task for task in result.skipped}
    assert (
        settle(
            definition, result.run, tuple(updated.get(t.id, t) for t in tasks)
        ).skipped
        == ()
    )


@pytest.mark.parametrize(
    "independent", [TaskStatus.READY, TaskStatus.RUNNING, TaskStatus.RETRY_WAIT]
)
def test_wait_for_independent_work(independent: TaskStatus) -> None:
    definition, run, tasks = snapshot(TaskStatus.FAILED)
    tasks = (tasks[0], tasks[1].model_copy(update={"status": independent}), *tasks[2:])
    result = settle(definition, run, tasks)
    assert len(result.skipped) == 2
    assert result.run == run


def test_all_success_and_shuffled_input() -> None:
    definition, run, tasks = snapshot(TaskStatus.SUCCEEDED)
    tasks = tuple(
        t.model_copy(update={"status": TaskStatus.SUCCEEDED}) for t in reversed(tasks)
    )
    result = settle(definition, run, tasks)
    assert result.run.status is RunStatus.SUCCEEDED
    assert (
        result.run.id == run.id
        and result.run.workflow_version_id == run.workflow_version_id
    )
    assert result.skipped == ()


@pytest.mark.parametrize(
    "corruption", ["missing", "duplicate", "wrong_run", "duplicate_id"]
)
def test_invalid_snapshot_rejected(corruption: str) -> None:
    definition, run, tasks = snapshot(TaskStatus.FAILED)
    if corruption == "missing":
        tasks = tasks[:-1]
    elif corruption == "duplicate":
        tasks = (*tasks, tasks[0])
    elif corruption == "wrong_run":
        tasks = (tasks[0].model_copy(update={"run_id": uuid4()}), *tasks[1:])
    else:
        tasks = (tasks[0].model_copy(update={"id": tasks[1].id}), *tasks[1:])
    with pytest.raises(ValueError, match="pinned DAG"):
        settle(definition, run, tasks)


@pytest.mark.parametrize(
    "status", [RunStatus.PENDING, RunStatus.SUCCEEDED, RunStatus.FAILED]
)
def test_inactive_run_unchanged(status: RunStatus) -> None:
    definition, run, tasks = snapshot(TaskStatus.FAILED)
    run = run.model_copy(update={"status": status})
    assert settle(definition, run, tasks).run == run
    assert settle(definition, run, tasks).skipped == ()
