"""Runtime identity snapshots and explicit, structural lifecycle transitions.

These pure models do not check graph readiness, clocks, ownership or database
concurrency. The transaction layer must establish those preconditions.
"""

from collections.abc import Mapping
from enum import Enum, StrEnum
from types import MappingProxyType
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from workflow_engine.domain.workflow import Identifier


class RunStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class AttemptStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    LOST = "LOST"


class RunEvent(StrEnum):
    START = "START"
    ALL_TASKS_SUCCEEDED = "ALL_TASKS_SUCCEEDED"
    TASKS_SETTLED_WITH_FAILURE = "TASKS_SETTLED_WITH_FAILURE"


class TaskEvent(StrEnum):
    DEPENDENCIES_SUCCEEDED = "DEPENDENCIES_SUCCEEDED"
    DEPENDENCY_FAILED = "DEPENDENCY_FAILED"
    CLAIM = "CLAIM"
    ATTEMPT_SUCCEEDED = "ATTEMPT_SUCCEEDED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    FAIL_PERMANENTLY = "FAIL_PERMANENTLY"
    RETRY_DUE = "RETRY_DUE"


class AttemptEvent(StrEnum):
    SUCCEED = "SUCCEED"
    FAIL = "FAIL"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    LEASE_EXPIRED = "LEASE_EXPIRED"


_RUN_TERMINAL = frozenset({RunStatus.SUCCEEDED, RunStatus.FAILED})
_TASK_TERMINAL = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.SKIPPED}
)
_ATTEMPT_TERMINAL = frozenset(
    {
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.TIMED_OUT,
        AttemptStatus.LOST,
    }
)

_RUN_TRANSITIONS: Mapping[tuple[RunStatus, RunEvent], RunStatus] = MappingProxyType(
    {
        (RunStatus.PENDING, RunEvent.START): RunStatus.RUNNING,
        (RunStatus.RUNNING, RunEvent.ALL_TASKS_SUCCEEDED): RunStatus.SUCCEEDED,
        (RunStatus.RUNNING, RunEvent.TASKS_SETTLED_WITH_FAILURE): RunStatus.FAILED,
    }
)
_TASK_TRANSITIONS: Mapping[tuple[TaskStatus, TaskEvent], TaskStatus] = MappingProxyType(
    {
        (TaskStatus.PENDING, TaskEvent.DEPENDENCIES_SUCCEEDED): TaskStatus.READY,
        (TaskStatus.PENDING, TaskEvent.DEPENDENCY_FAILED): TaskStatus.SKIPPED,
        (TaskStatus.READY, TaskEvent.CLAIM): TaskStatus.RUNNING,
        (TaskStatus.RUNNING, TaskEvent.ATTEMPT_SUCCEEDED): TaskStatus.SUCCEEDED,
        (TaskStatus.RUNNING, TaskEvent.RETRY_SCHEDULED): TaskStatus.RETRY_WAIT,
        (TaskStatus.RUNNING, TaskEvent.FAIL_PERMANENTLY): TaskStatus.FAILED,
        (TaskStatus.RETRY_WAIT, TaskEvent.RETRY_DUE): TaskStatus.READY,
    }
)
_ATTEMPT_TRANSITIONS: Mapping[tuple[AttemptStatus, AttemptEvent], AttemptStatus] = (
    MappingProxyType(
        {
            (AttemptStatus.RUNNING, AttemptEvent.SUCCEED): AttemptStatus.SUCCEEDED,
            (AttemptStatus.RUNNING, AttemptEvent.FAIL): AttemptStatus.FAILED,
            (
                AttemptStatus.RUNNING,
                AttemptEvent.DEADLINE_EXCEEDED,
            ): AttemptStatus.TIMED_OUT,
            (AttemptStatus.RUNNING, AttemptEvent.LEASE_EXPIRED): AttemptStatus.LOST,
        }
    )
)


class InvalidStateTransition(ValueError):
    """A supported event is illegal for the snapshot's current status."""

    def __init__(self, status: Enum, event: Enum) -> None:
        self.status = status
        self.event = event
        super().__init__(
            f"{type(status).__name__}.{status.name} does not accept "
            f"{type(event).__name__}.{event.name}."
        )


def _next_status[S: Enum, E: Enum](
    status: S, event: E, event_type: type[E], transitions: Mapping[tuple[S, E], S]
) -> S:
    # StrEnum values can compare equal across enum classes or to raw strings.
    # Reject those inputs before looking up an edge.
    if type(event) is not event_type:
        raise TypeError(f"Expected {event_type.__name__}.")
    try:
        return transitions[status, event]
    except KeyError:
        raise InvalidStateTransition(status, event) from None


class _RuntimeSnapshot(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class WorkflowRun(_RuntimeSnapshot):
    """One invocation pinned to a concrete immutable workflow version UUID."""

    id: UUID
    workflow_version_id: UUID
    status: RunStatus = Field(default=RunStatus.PENDING, strict=True)

    @property
    def is_terminal(self) -> bool:
        return self.status in _RUN_TERMINAL

    def transition(self, event: RunEvent) -> "WorkflowRun":
        current = WorkflowRun.model_validate(self)
        status = _next_status(current.status, event, RunEvent, _RUN_TRANSITIONS)
        return WorkflowRun.model_validate({**current.model_dump(), "status": status})


class TaskRun(_RuntimeSnapshot):
    """One DAG node within a run; task_key refers to TaskDefinition.task_id."""

    id: UUID
    run_id: UUID
    task_key: Identifier
    status: TaskStatus = Field(default=TaskStatus.PENDING, strict=True)

    @property
    def is_terminal(self) -> bool:
        return self.status in _TASK_TERMINAL

    def transition(self, event: TaskEvent) -> "TaskRun":
        current = TaskRun.model_validate(self)
        status = _next_status(current.status, event, TaskEvent, _TASK_TRANSITIONS)
        return TaskRun.model_validate({**current.model_dump(), "status": status})


class TaskAttempt(_RuntimeSnapshot):
    """One claimed execution attempt; RUNNING does not prove a handler started."""

    id: UUID
    task_id: UUID
    attempt_number: int = Field(strict=True, ge=1)
    status: AttemptStatus = Field(default=AttemptStatus.RUNNING, strict=True)

    @property
    def is_terminal(self) -> bool:
        return self.status in _ATTEMPT_TERMINAL

    def transition(self, event: AttemptEvent) -> "TaskAttempt":
        current = TaskAttempt.model_validate(self)
        status = _next_status(current.status, event, AttemptEvent, _ATTEMPT_TRANSITIONS)
        return TaskAttempt.model_validate({**current.model_dump(), "status": status})
