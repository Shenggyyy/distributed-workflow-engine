"""Demonstrate immutable runtime transitions in memory; no task is executed."""

from uuid import uuid4

from workflow_engine.domain.runtime import (
    AttemptEvent,
    RunEvent,
    TaskAttempt,
    TaskEvent,
    TaskRun,
    WorkflowRun,
)


def main() -> None:
    # This UUID is illustrative; no stored workflow version is resolved here.
    run = WorkflowRun(id=uuid4(), workflow_version_id=uuid4()).transition(
        RunEvent.START
    )
    task = TaskRun(id=uuid4(), run_id=run.id, task_key="A")
    task = task.transition(TaskEvent.DEPENDENCIES_SUCCEEDED).transition(TaskEvent.CLAIM)
    first = TaskAttempt(id=uuid4(), task_id=task.id, attempt_number=1)
    first = first.transition(AttemptEvent.FAIL)
    task = task.transition(TaskEvent.RETRY_SCHEDULED)
    print(f"Attempt 1: {first.status}; task: {task.status}")
    task = task.transition(TaskEvent.RETRY_DUE).transition(TaskEvent.CLAIM)
    second = TaskAttempt(id=uuid4(), task_id=task.id, attempt_number=2)
    second = second.transition(AttemptEvent.SUCCEED)
    task = task.transition(TaskEvent.ATTEMPT_SUCCEEDED)
    run = run.transition(RunEvent.ALL_TASKS_SUCCEEDED)
    print(f"Attempt 2: {second.status}; task: {task.status}; run: {run.status}")
    print("In-memory lifecycle only; no scheduling, waiting or task execution.")


if __name__ == "__main__":
    main()
