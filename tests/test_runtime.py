"""Runtime lifecycle contracts; no database, clocks or worker processes."""

from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from workflow_engine.domain.runtime import (
    AttemptEvent,
    AttemptStatus,
    InvalidStateTransition,
    RunEvent,
    RunStatus,
    TaskAttempt,
    TaskEvent,
    TaskRun,
    TaskStatus,
    WorkflowRun,
)


def run() -> WorkflowRun:
    return WorkflowRun(id=uuid4(), workflow_version_id=uuid4())


def task(*, status: TaskStatus = TaskStatus.PENDING) -> TaskRun:
    return TaskRun(id=uuid4(), run_id=uuid4(), task_key="A", status=status)


def attempt(*, status: AttemptStatus = AttemptStatus.RUNNING) -> TaskAttempt:
    return TaskAttempt(id=uuid4(), task_id=uuid4(), attempt_number=1, status=status)


def test_successful_run_and_task_lifecycles_preserve_identity() -> None:
    initial_run = run()
    active_run = initial_run.transition(RunEvent.START)
    initial_task = TaskRun(id=uuid4(), run_id=initial_run.id, task_key="A")
    ready = initial_task.transition(TaskEvent.DEPENDENCIES_SUCCEEDED)
    running = ready.transition(TaskEvent.CLAIM)
    succeeded = running.transition(TaskEvent.ATTEMPT_SUCCEEDED)
    completed_run = active_run.transition(RunEvent.ALL_TASKS_SUCCEEDED)

    assert [value.status for value in (initial_task, ready, running, succeeded)] == [
        TaskStatus.PENDING,
        TaskStatus.READY,
        TaskStatus.RUNNING,
        TaskStatus.SUCCEEDED,
    ]
    assert [value.status for value in (initial_run, active_run, completed_run)] == [
        RunStatus.PENDING,
        RunStatus.RUNNING,
        RunStatus.SUCCEEDED,
    ]
    for value in (ready, running, succeeded):
        assert value.id == initial_task.id
        assert value.run_id == initial_task.run_id
        assert value.task_key == initial_task.task_key
        assert value is not initial_task
    assert completed_run.id == initial_run.id
    assert completed_run.workflow_version_id == initial_run.workflow_version_id
    assert not initial_task.is_terminal
    assert succeeded.is_terminal and completed_run.is_terminal


def test_retry_creates_new_attempt_without_reopening_old_attempt() -> None:
    current_task = task(status=TaskStatus.RUNNING)
    first = TaskAttempt(id=uuid4(), task_id=current_task.id, attempt_number=1)
    failed = first.transition(AttemptEvent.FAIL)
    waiting = current_task.transition(TaskEvent.RETRY_SCHEDULED)
    ready = waiting.transition(TaskEvent.RETRY_DUE)
    running_again = ready.transition(TaskEvent.CLAIM)
    second = TaskAttempt(id=uuid4(), task_id=current_task.id, attempt_number=2)
    success = second.transition(AttemptEvent.SUCCEED)
    finished = running_again.transition(TaskEvent.ATTEMPT_SUCCEEDED)

    assert waiting.status == TaskStatus.RETRY_WAIT and not waiting.is_terminal
    assert first.status == AttemptStatus.RUNNING  # The input snapshot is unchanged.
    assert failed.status == AttemptStatus.FAILED and failed.is_terminal
    assert success.id != failed.id
    assert success.task_id == failed.task_id == finished.id
    assert success.attempt_number == 2
    assert finished.status == TaskStatus.SUCCEEDED
    with pytest.raises(InvalidStateTransition):
        failed.transition(AttemptEvent.SUCCEED)


def test_permanent_failure_skips_blocked_task_and_settles_run() -> None:
    active = run().transition(RunEvent.START)
    failed = task(status=TaskStatus.RUNNING).transition(TaskEvent.FAIL_PERMANENTLY)
    blocked = task().transition(TaskEvent.DEPENDENCY_FAILED)
    finished = active.transition(RunEvent.TASKS_SETTLED_WITH_FAILURE)
    assert failed.status == TaskStatus.FAILED
    assert blocked.status == TaskStatus.SKIPPED
    assert finished.status == RunStatus.FAILED
    assert all(value.is_terminal for value in (failed, blocked, finished))


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        (AttemptEvent.SUCCEED, AttemptStatus.SUCCEEDED),
        (AttemptEvent.FAIL, AttemptStatus.FAILED),
        (AttemptEvent.DEADLINE_EXCEEDED, AttemptStatus.TIMED_OUT),
        (AttemptEvent.LEASE_EXPIRED, AttemptStatus.LOST),
    ],
)
def test_attempt_outcomes_preserve_attempt_identity(
    event: AttemptEvent, expected: AttemptStatus
) -> None:
    original = attempt()
    result = original.transition(event)
    assert result.status == expected
    assert result.is_terminal and not original.is_terminal
    assert result.model_dump(exclude={"status"}) == original.model_dump(
        exclude={"status"}
    )


@pytest.mark.parametrize("status", [RunStatus.SUCCEEDED, RunStatus.FAILED])
def test_terminal_runs_reject_every_event(status: RunStatus) -> None:
    snapshot = WorkflowRun(id=uuid4(), workflow_version_id=uuid4(), status=status)
    assert snapshot.is_terminal
    for event in RunEvent:
        with pytest.raises(InvalidStateTransition):
            snapshot.transition(event)


@pytest.mark.parametrize(
    "status", [TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.SKIPPED]
)
def test_terminal_tasks_reject_every_event(status: TaskStatus) -> None:
    snapshot = task(status=status)
    assert snapshot.is_terminal
    for event in TaskEvent:
        with pytest.raises(InvalidStateTransition):
            snapshot.transition(event)


@pytest.mark.parametrize(
    "status",
    [
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.TIMED_OUT,
        AttemptStatus.LOST,
    ],
)
def test_terminal_attempts_reject_every_event(status: AttemptStatus) -> None:
    snapshot = attempt(status=status)
    assert snapshot.is_terminal
    for event in AttemptEvent:
        with pytest.raises(InvalidStateTransition):
            snapshot.transition(event)


@pytest.mark.parametrize(
    ("status", "event"),
    [
        (TaskStatus.PENDING, TaskEvent.CLAIM),
        (TaskStatus.PENDING, TaskEvent.ATTEMPT_SUCCEEDED),
        (TaskStatus.PENDING, TaskEvent.RETRY_DUE),
        (TaskStatus.READY, TaskEvent.FAIL_PERMANENTLY),
        (TaskStatus.READY, TaskEvent.DEPENDENCY_FAILED),
        (TaskStatus.RUNNING, TaskEvent.CLAIM),
        (TaskStatus.RUNNING, TaskEvent.RETRY_DUE),
        (TaskStatus.RETRY_WAIT, TaskEvent.CLAIM),
        (TaskStatus.RETRY_WAIT, TaskEvent.ATTEMPT_SUCCEEDED),
    ],
)
def test_tasks_cannot_bypass_lifecycle_stages(
    status: TaskStatus, event: TaskEvent
) -> None:
    snapshot = task(status=status)
    with pytest.raises(InvalidStateTransition) as error:
        snapshot.transition(event)
    assert error.value.status is status
    assert error.value.event is event
    assert snapshot.status is status


def test_run_must_start_before_settling_and_cannot_start_twice() -> None:
    snapshot = run()
    for event in (RunEvent.ALL_TASKS_SUCCEEDED, RunEvent.TASKS_SETTLED_WITH_FAILURE):
        with pytest.raises(InvalidStateTransition):
            snapshot.transition(event)
    with pytest.raises(InvalidStateTransition):
        snapshot.transition(RunEvent.START).transition(RunEvent.START)


def test_repeated_delivery_is_not_a_second_state_transition() -> None:
    ready = task().transition(TaskEvent.DEPENDENCIES_SUCCEEDED)
    with pytest.raises(InvalidStateTransition):
        ready.transition(TaskEvent.DEPENDENCIES_SUCCEEDED)


@pytest.mark.parametrize("number", [True, False, 0, -1, 1.5, "1"])
def test_attempt_number_requires_positive_strict_integer(number: object) -> None:
    with pytest.raises(ValidationError):
        TaskAttempt.model_validate(
            {"id": uuid4(), "task_id": uuid4(), "attempt_number": number}
        )


@pytest.mark.parametrize("kind", ["run", "task", "attempt"])
def test_snapshot_round_trip_and_frozen_fields(kind: str) -> None:
    snapshots: dict[str, WorkflowRun | TaskRun | TaskAttempt] = {
        "run": run(),
        "task": task(),
        "attempt": attempt(),
    }
    snapshot = snapshots[kind]
    restored = type(snapshot).model_validate_json(snapshot.model_dump_json())
    assert restored == snapshot
    assert snapshot.model_dump(mode="json")["status"] == snapshot.status.value
    with pytest.raises(ValidationError):
        snapshot.__setattr__("status", snapshot.status)


def test_status_types_and_unknown_fields_are_rejected() -> None:
    for bad_status in ("RUNNING", TaskStatus.RUNNING, "CANCELLED"):
        with pytest.raises(ValidationError):
            WorkflowRun.model_validate(
                {"id": uuid4(), "workflow_version_id": uuid4(), "status": bad_status}
            )
    with pytest.raises(ValidationError):
        TaskRun.model_validate(
            {"id": "invalid-uuid", "run_id": uuid4(), "task_key": "A"}
        )
    with pytest.raises(ValidationError):
        TaskRun.model_validate(
            {"id": uuid4(), "run_id": uuid4(), "task_key": "A", "typo": True}
        )
    with pytest.raises(ValidationError):
        TaskAttempt.model_validate_json(
            '{"id":"00000000-0000-0000-0000-000000000001",'
            '"task_id":"00000000-0000-0000-0000-000000000002",'
            '"attempt_number":1,"status":"RETRY_WAIT"}'
        )


def test_validation_bypasses_are_rechecked_before_transitions() -> None:
    invalid = attempt().model_copy(update={"attempt_number": 0})
    with pytest.raises(ValidationError):
        invalid.transition(AttemptEvent.SUCCEED)
    invalid_task = task().model_copy(update={"task_key": "bad key"})
    with pytest.raises(ValidationError):
        invalid_task.transition(TaskEvent.DEPENDENCIES_SUCCEEDED)
    invalid_run = run().model_copy(update={"status": "PENDING"})
    with pytest.raises(ValidationError):
        invalid_run.transition(RunEvent.START)


def test_event_types_are_checked_at_runtime() -> None:
    # Any deliberately models an untyped caller supplying an invalid event.
    bad_event: Any
    for bad_event in ("START", TaskEvent.CLAIM):
        with pytest.raises(TypeError, match="Expected RunEvent"):
            run().transition(bad_event)
    for bad_event in ("CLAIM", AttemptEvent.SUCCEED):
        with pytest.raises(TypeError, match="Expected TaskEvent"):
            task(status=TaskStatus.READY).transition(bad_event)
    for bad_event in ("SUCCEED", RunEvent.START):
        with pytest.raises(TypeError, match="Expected AttemptEvent"):
            attempt().transition(bad_event)
