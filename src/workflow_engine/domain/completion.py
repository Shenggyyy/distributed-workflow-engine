"""Pure completion proposals and receipts, without durable deduplication or I/O."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from workflow_engine.domain.lease import AttemptLease, LeaseOwnershipError
from workflow_engine.domain.runtime import AttemptEvent, TaskAttempt
from workflow_engine.domain.workflow import Identifier


class CompletionOutcome(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class CompletionConflictError(ValueError):
    """The same owned Attempt was submitted with a different result."""


class _CompletionModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class CompletionResult(_CompletionModel):
    """Worker-reported outcome; no engine timeout/loss or retry decision."""

    outcome: CompletionOutcome
    error_code: Identifier | None = Field(default=None, repr=False)

    @model_validator(mode="after")
    def validate_error_code(self) -> Self:
        if (self.outcome is CompletionOutcome.FAILED) != (self.error_code is not None):
            raise ValueError("Only failed completion requires an error code.")
        return self

    def attempt_event(self) -> AttemptEvent:
        current = CompletionResult.model_validate(self)
        return (
            AttemptEvent.SUCCEED
            if current.outcome is CompletionOutcome.SUCCEEDED
            else AttemptEvent.FAIL
        )


class AttemptCompletion(_CompletionModel):
    """One proposal, keyed by Attempt ID and bound to the original lease owner."""

    attempt_id: UUID
    worker_session_id: UUID
    lease_token: UUID = Field(repr=False)
    result: CompletionResult = Field(repr=False)


class CompletionReceipt(_CompletionModel):
    """Internal accepted snapshot; construction alone proves no database commit."""

    completion: AttemptCompletion = Field(repr=False)
    attempt: TaskAttempt
    accepted_at: AwareDatetime

    @field_validator("accepted_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        try:
            return value.astimezone(UTC)
        except (OverflowError, ValueError):
            raise ValueError(
                "Completion time is outside the supported UTC range."
            ) from None

    @model_validator(mode="after")
    def validate_attempt(self) -> Self:
        if self.attempt.id != self.completion.attempt_id:
            raise ValueError("Completion receipt Attempt identity does not match.")
        if self.attempt.status.value != self.completion.result.outcome.value:
            raise ValueError("Completion receipt Attempt outcome does not match.")
        return self

    def replay(self, submitted: AttemptCompletion) -> "CompletionReceipt":
        """Compare an existing receipt, without new transitions or lease checks."""
        current = CompletionReceipt.model_validate(self)
        proposed = AttemptCompletion.model_validate(submitted)
        expected = current.completion
        if (proposed.attempt_id, proposed.worker_session_id, proposed.lease_token) != (
            expected.attempt_id,
            expected.worker_session_id,
            expected.lease_token,
        ):
            raise LeaseOwnershipError("Completion ownership does not match.")
        if proposed.result != expected.result:
            raise CompletionConflictError("Attempt completion result conflicts.")
        return current


def accept_completion(
    attempt: TaskAttempt,
    lease: AttemptLease,
    submitted: AttemptCompletion,
    *,
    observed_at: datetime,
) -> CompletionReceipt:
    """Propose a first completion from snapshots already locked by the caller.

    The caller must check Run/Task/Worker state, receipt absence and authoritative
    ownership, then persist receipt, Attempt and Task outcome in one transaction.
    """
    current = TaskAttempt.model_validate(attempt)
    ownership = AttemptLease.model_validate(lease)
    proposed = AttemptCompletion.model_validate(submitted)
    if current.id != ownership.attempt_id:
        raise ValueError("Attempt and lease identity do not match.")
    terminal = current.transition(proposed.result.attempt_event())
    ownership.require_valid_owner(
        attempt_id=proposed.attempt_id,
        worker_session_id=proposed.worker_session_id,
        lease_token=proposed.lease_token,
        observed_at=observed_at,
    )
    return CompletionReceipt(
        completion=proposed, attempt=terminal, accepted_at=observed_at
    )
