"""First completion authority and replay after terminal state or lease expiry."""

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from workflow_engine.domain.completion import (
    AttemptCompletion,
    CompletionConflictError,
    CompletionOutcome,
    CompletionReceipt,
    CompletionResult,
    accept_completion,
)
from workflow_engine.domain.lease import (
    AttemptLease,
    LeaseClockRegressionError,
    LeaseExpiredError,
    LeaseOwnershipError,
)
from workflow_engine.domain.runtime import (
    AttemptStatus,
    InvalidStateTransition,
    TaskAttempt,
)

START = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def execution() -> tuple[TaskAttempt, AttemptLease, AttemptCompletion]:
    attempt = TaskAttempt(id=uuid4(), task_id=uuid4(), attempt_number=1)
    lease = AttemptLease(
        attempt_id=attempt.id,
        worker_session_id=uuid4(),
        lease_token=uuid4(),
        acquired_at=START,
        last_renewed_at=START + timedelta(seconds=10),
        lease_expires_at=START + timedelta(seconds=30),
    )
    submitted = AttemptCompletion(
        attempt_id=attempt.id,
        worker_session_id=lease.worker_session_id,
        lease_token=lease.lease_token,
        result=CompletionResult(outcome=CompletionOutcome.SUCCEEDED),
    )
    return attempt, lease, submitted


Execution = tuple[TaskAttempt, AttemptLease, AttemptCompletion]


@pytest.mark.parametrize("outcome", list(CompletionOutcome))
@pytest.mark.parametrize("seconds", [10, 29.999999])
def test_first_completion_preserves_identity_and_originals(
    execution: Execution, outcome: CompletionOutcome, seconds: float
) -> None:
    attempt, lease, submitted = execution
    submitted = AttemptCompletion.model_validate(
        {
            **submitted.model_dump(),
            "result": {
                "outcome": outcome,
                "error_code": "handler_failed"
                if outcome is CompletionOutcome.FAILED
                else None,
            },
        }
    )
    receipt = accept_completion(
        attempt, lease, submitted, observed_at=START + timedelta(seconds=seconds)
    )
    assert receipt.attempt.status.value == outcome.value
    assert receipt.attempt.model_dump(exclude={"status"}) == attempt.model_dump(
        exclude={"status"}
    )
    assert receipt.completion == submitted
    assert receipt.accepted_at == START + timedelta(seconds=seconds)
    assert attempt.status is AttemptStatus.RUNNING
    assert lease.last_renewed_at == START + timedelta(seconds=10)
    assert lease.lease_expires_at == START + timedelta(seconds=30)


@pytest.mark.parametrize("seconds", [30, 30.000001, 60])
def test_expired_first_completion_rejected(
    execution: Execution, seconds: float
) -> None:
    with pytest.raises(LeaseExpiredError):
        accept_completion(*execution, observed_at=START + timedelta(seconds=seconds))


@pytest.mark.parametrize("seconds", [0, 9.999999])
def test_regressed_observation_rejected(execution: Execution, seconds: float) -> None:
    with pytest.raises(LeaseClockRegressionError):
        accept_completion(*execution, observed_at=START + timedelta(seconds=seconds))


@pytest.mark.parametrize("field", ["attempt_id", "worker_session_id", "lease_token"])
def test_wrong_owner_on_first_report_and_replay(
    execution: Execution, field: str
) -> None:
    attempt, lease, submitted = execution
    wrong = AttemptCompletion.model_validate({**submitted.model_dump(), field: uuid4()})
    receipt = accept_completion(*execution, observed_at=START + timedelta(seconds=15))
    operations: tuple[Callable[[], CompletionReceipt], ...] = (
        lambda: accept_completion(
            attempt, lease, wrong, observed_at=START + timedelta(seconds=15)
        ),
        lambda: receipt.replay(wrong),
    )
    for operation in operations:
        with pytest.raises(LeaseOwnershipError) as error:
            operation()
        assert str(wrong.lease_token) not in str(error.value)


@pytest.mark.parametrize(
    "status", [s for s in AttemptStatus if s is not AttemptStatus.RUNNING]
)
def test_terminal_without_receipt_is_not_a_duplicate(
    execution: Execution, status: AttemptStatus
) -> None:
    attempt, lease, submitted = execution
    terminal = TaskAttempt.model_validate({**attempt.model_dump(), "status": status})
    with pytest.raises(InvalidStateTransition):
        accept_completion(
            terminal, lease, submitted, observed_at=START + timedelta(seconds=15)
        )


def test_replay_keeps_original_acceptance_after_deadline(execution: Execution) -> None:
    attempt, lease, submitted = execution
    receipt = accept_completion(*execution, observed_at=START + timedelta(seconds=15))
    # A new acceptance fails now, but a stored receipt needs no current clock/lease.
    with pytest.raises(LeaseExpiredError):
        accept_completion(
            attempt, lease, submitted, observed_at=START + timedelta(days=1)
        )
    restored = CompletionReceipt.model_validate_json(receipt.model_dump_json())
    assert restored.replay(submitted) == receipt
    assert restored.replay(submitted).accepted_at == START + timedelta(seconds=15)
    assert restored.attempt.status is AttemptStatus.SUCCEEDED


@pytest.mark.parametrize("changed_outcome", [False, True])
def test_different_result_conflicts(
    execution: Execution, changed_outcome: bool
) -> None:
    attempt, lease, submitted = execution
    failed = AttemptCompletion.model_validate(
        {
            **submitted.model_dump(),
            "result": {
                "outcome": CompletionOutcome.FAILED,
                "error_code": "first_failure",
            },
        }
    )
    receipt = accept_completion(
        attempt, lease, failed, observed_at=START + timedelta(seconds=15)
    )
    assert receipt.replay(failed) == receipt
    conflicting = (
        submitted
        if changed_outcome
        else AttemptCompletion.model_validate(
            {
                **failed.model_dump(),
                "result": {
                    "outcome": CompletionOutcome.FAILED,
                    "error_code": "private_detail",
                },
            }
        )
    )
    with pytest.raises(CompletionConflictError) as error:
        receipt.replay(conflicting)
    assert "private_detail" not in str(error.value)


@pytest.mark.parametrize(
    "value", ["SUCCEEDED", AttemptStatus.SUCCEEDED, "LOST", "TIMED_OUT", True, 1, None]
)
def test_python_outcome_is_strict(value: Any) -> None:
    with pytest.raises(ValidationError):
        CompletionResult(outcome=value)


@pytest.mark.parametrize(
    "payload",
    [
        {"outcome": "FAILED"},
        {"outcome": "FAILED", "error_code": None},
        {"outcome": "FAILED", "error_code": ""},
        {"outcome": "FAILED", "error_code": "x" * 65},
        {"outcome": "FAILED", "error_code": "bad code"},
        {"outcome": "FAILED", "error_code": 42},
        {"outcome": "SUCCEEDED", "error_code": "unexpected"},
        {"outcome": "LOST"},
        {"outcome": "TIMED_OUT"},
        {"outcome": "SUCCEEDED", "output": {"secret": "private"}},
        {"outcome": "FAILED", "error_code": "error", "retryable": True},
    ],
)
def test_invalid_result_json(payload: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        CompletionResult.model_validate_json(json.dumps(payload))


def test_error_code_boundary_and_null_success() -> None:
    failure = CompletionResult(outcome=CompletionOutcome.FAILED, error_code="x" * 64)
    assert CompletionResult.model_validate_json(failure.model_dump_json()) == failure
    assert CompletionResult.model_validate_json(
        '{"outcome":"SUCCEEDED"}'
    ) == CompletionResult.model_validate_json(
        '{"outcome":"SUCCEEDED","error_code":null}'
    )


@pytest.mark.parametrize("field", ["attempt_id", "worker_session_id", "lease_token"])
def test_submission_ids_are_strict_and_required(
    execution: Execution, field: str
) -> None:
    payload = execution[2].model_dump()
    with pytest.raises(ValidationError):
        AttemptCompletion.model_validate({**payload, field: str(uuid4())})
    payload.pop(field)
    with pytest.raises(ValidationError):
        AttemptCompletion.model_validate(payload)


@pytest.mark.parametrize("time", [START.replace(tzinfo=None), 0, "2026-01-01", None])
def test_invalid_observation(execution: Execution, time: Any) -> None:
    with pytest.raises(ValueError):
        accept_completion(*execution, observed_at=time)


def test_utc_normalization(execution: Execution) -> None:
    observed = (START + timedelta(seconds=15)).astimezone(timezone(timedelta(hours=8)))
    receipt = accept_completion(*execution, observed_at=observed)
    assert receipt.accepted_at == START + timedelta(seconds=15)
    assert receipt.accepted_at.tzinfo is UTC


def test_mismatched_storage_snapshots(execution: Execution) -> None:
    attempt, lease, submitted = execution
    unrelated = lease.model_copy(update={"attempt_id": uuid4()})
    with pytest.raises(ValueError, match="Attempt and lease identity"):
        accept_completion(
            attempt, unrelated, submitted, observed_at=START + timedelta(seconds=15)
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", uuid4()),
        ("status", AttemptStatus.FAILED),
        ("status", AttemptStatus.RUNNING),
    ],
)
def test_inconsistent_receipt_rejected(
    execution: Execution, field: str, value: Any
) -> None:
    receipt = accept_completion(*execution, observed_at=START + timedelta(seconds=15))
    with pytest.raises(ValidationError):
        CompletionReceipt.model_validate(
            {
                **receipt.model_dump(),
                "attempt": {**receipt.attempt.model_dump(), field: value},
            }
        )


def test_frozen_nested_values_and_safe_repr(execution: Execution) -> None:
    receipt = accept_completion(*execution, observed_at=START + timedelta(seconds=15))
    for model, field, value in (
        (receipt, "accepted_at", START),
        (receipt.completion, "lease_token", uuid4()),
        (receipt.completion.result, "outcome", CompletionOutcome.FAILED),
    ):
        with pytest.raises(ValidationError):
            setattr(model, field, value)
    assert str(execution[2].lease_token) not in repr(receipt) + repr(receipt.completion)


def test_validation_bypasses_rechecked(execution: Execution) -> None:
    attempt, lease, submitted = execution
    bad_result = submitted.result.model_copy(update={"outcome": "SUCCEEDED"})
    invalid = submitted.model_copy(update={"result": bad_result})
    receipt = accept_completion(*execution, observed_at=START + timedelta(seconds=15))
    with pytest.raises(ValidationError):
        accept_completion(
            attempt, lease, invalid, observed_at=START + timedelta(seconds=15)
        )
    with pytest.raises(ValidationError):
        receipt.replay(invalid)
    with pytest.raises(ValidationError):
        receipt.model_copy(update={"accepted_at": None}).replay(submitted)
    with pytest.raises(ValidationError):
        bad_result.attempt_event()


@pytest.mark.parametrize("field", ["request_id", "accepted_at", "retry_at"])
def test_submission_rejects_engine_fields(execution: Execution, field: str) -> None:
    with pytest.raises(ValidationError):
        AttemptCompletion.model_validate(
            {**execution[2].model_dump(), field: "private"}
        )
