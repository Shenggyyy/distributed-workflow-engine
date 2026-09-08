"""Worker session identity, strict fields and terminal lifecycle contracts."""

from enum import StrEnum
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from workflow_engine.domain.runtime import InvalidStateTransition, TaskStatus
from workflow_engine.domain.worker import (
    MAX_WORKER_CONCURRENCY,
    WorkerEvent,
    WorkerSession,
    WorkerStatus,
)


def session(*, status: WorkerStatus = WorkerStatus.ACTIVE) -> WorkerSession:
    return WorkerSession(
        id=uuid4(), worker_name="worker_1", max_concurrency=2, status=status
    )


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        (WorkerEvent.HEARTBEAT_EXPIRED, WorkerStatus.LOST),
        (WorkerEvent.SHUTDOWN, WorkerStatus.STOPPED),
    ],
)
def test_terminal_transition_preserves_registration_fields(
    event: WorkerEvent, expected: WorkerStatus
) -> None:
    original = session()
    result = original.transition(event)
    assert original.status is WorkerStatus.ACTIVE and not original.is_terminal
    assert result.status is expected and result.is_terminal
    assert result is not original
    assert result.model_dump(exclude={"status"}) == original.model_dump(
        exclude={"status"}
    )


@pytest.mark.parametrize("status", [WorkerStatus.LOST, WorkerStatus.STOPPED])
@pytest.mark.parametrize("event", list(WorkerEvent))
def test_terminal_session_rejects_every_event(
    status: WorkerStatus, event: WorkerEvent
) -> None:
    original = session(status=status)
    with pytest.raises(InvalidStateTransition) as error:
        original.transition(event)
    assert error.value.status is status
    assert error.value.event is event
    assert original.status is status


def test_process_restart_uses_distinct_identity_with_reusable_name() -> None:
    old = session().transition(WorkerEvent.HEARTBEAT_EXPIRED)
    restarted = WorkerSession(
        id=uuid4(), worker_name=old.worker_name, max_concurrency=old.max_concurrency
    )
    assert old.id != restarted.id
    assert old.worker_name == restarted.worker_name
    assert old.is_terminal and restarted.status is WorkerStatus.ACTIVE
    # Callers supply UUIDs; constructing a snapshot does not enforce uniqueness.
    assert WorkerSession.model_validate(old) == old


@pytest.mark.parametrize("capacity", [True, False, 0, -1, 1.0, "2", 2_147_483_648])
def test_capacity_rejects_coercion_and_out_of_range(capacity: object) -> None:
    with pytest.raises(ValidationError):
        WorkerSession.model_validate(
            {"id": uuid4(), "worker_name": "worker", "max_concurrency": capacity}
        )


@pytest.mark.parametrize("capacity", [1, MAX_WORKER_CONCURRENCY])
def test_capacity_boundaries(capacity: int) -> None:
    value = WorkerSession(id=uuid4(), worker_name="worker", max_concurrency=capacity)
    assert value.max_concurrency == capacity


@pytest.mark.parametrize("name", ["", "9worker", "with space", "x" * 65, "节点", 12])
def test_worker_name_uses_existing_identifier_contract(name: object) -> None:
    with pytest.raises(ValidationError):
        WorkerSession.model_validate(
            {"id": uuid4(), "worker_name": name, "max_concurrency": 1}
        )


def test_equal_names_do_not_merge_sessions() -> None:
    first = WorkerSession(id=uuid4(), worker_name="A" + "z" * 63, max_concurrency=1)
    second = WorkerSession(id=uuid4(), worker_name=first.worker_name, max_concurrency=4)
    assert first.id != second.id
    assert first.max_concurrency != second.max_concurrency


@pytest.mark.parametrize("field", ["id", "worker_name", "max_concurrency"])
def test_registration_fields_are_required(field: str) -> None:
    data = session().model_dump()
    data.pop(field)
    with pytest.raises(ValidationError):
        WorkerSession.model_validate(data)


def test_invalid_identity_and_unknown_fields_are_rejected() -> None:
    original = session().model_dump()
    with pytest.raises(ValidationError):
        WorkerSession.model_validate({**original, "id": "not-a-uuid"})
    with pytest.raises(ValidationError):
        WorkerSession.model_validate({**original, "lease_token": "private-value"})


@pytest.mark.parametrize("status", list(WorkerStatus))
def test_round_trip_and_frozen_registration(status: WorkerStatus) -> None:
    original = session(status=status)
    restored = WorkerSession.model_validate_json(original.model_dump_json())
    assert restored == original
    assert restored.model_dump(mode="json")["status"] == status.value
    for field, value in {
        "id": uuid4(),
        "worker_name": "other",
        "max_concurrency": 3,
        "status": WorkerStatus.STOPPED,
    }.items():
        with pytest.raises(ValidationError):
            original.__setattr__(field, value)


class OtherStatus(StrEnum):
    ACTIVE = "ACTIVE"


class OtherEvent(StrEnum):
    SHUTDOWN = "SHUTDOWN"


@pytest.mark.parametrize("status", ["ACTIVE", OtherStatus.ACTIVE, TaskStatus.RUNNING])
def test_python_status_requires_exact_enum(status: object) -> None:
    with pytest.raises(ValidationError):
        WorkerSession.model_validate({**session().model_dump(), "status": status})


def test_unknown_json_status_is_rejected() -> None:
    data = session().model_dump_json().replace('"ACTIVE"', '"PAUSED"')
    with pytest.raises(ValidationError):
        WorkerSession.model_validate_json(data)


@pytest.mark.parametrize("event", ["SHUTDOWN", OtherEvent.SHUTDOWN, None])
def test_event_requires_exact_enum(event: Any) -> None:
    with pytest.raises(TypeError, match="Expected WorkerEvent"):
        session().transition(event)


@pytest.mark.parametrize(
    "update",
    [
        {"max_concurrency": 0},
        {"status": "ACTIVE"},
        {"id": "not-a-uuid"},
        {"worker_name": "invalid name"},
    ],
)
def test_transition_revalidates_bypassed_snapshot(update: dict[str, object]) -> None:
    invalid = session().model_copy(update=update)
    with pytest.raises(ValidationError):
        invalid.transition(WorkerEvent.SHUTDOWN)
