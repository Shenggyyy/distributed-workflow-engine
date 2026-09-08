"""Immutable execution policy and bounded retry delay; no clocks or I/O."""

import math
from datetime import timedelta
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ExecutionPolicy(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    max_attempts: int = Field(default=1, ge=1, le=100)
    timeout_seconds: int = Field(default=300, ge=1, le=86400)
    initial_backoff_ms: int = Field(default=1000, ge=1, le=86400000)
    max_backoff_ms: int = Field(default=60000, ge=1, le=86400000)

    @model_validator(mode="after")
    def backoff_bounds(self) -> Self:
        if self.max_backoff_ms < self.initial_backoff_ms:
            raise ValueError("Maximum backoff must not precede initial backoff.")
        return self


def retry_delay(
    policy: ExecutionPolicy, attempt_number: int, *, jitter: float
) -> timedelta | None:
    """Equal jitter in [half-cap, cap]; None means the attempt budget is spent.

    attempt_number counts the failed Attempt, including the first execution.
    The caller supplies entropy and persists its resulting absolute deadline.
    """
    current = ExecutionPolicy.model_validate(policy)
    if type(attempt_number) is not int or not 1 <= attempt_number <= 2_147_483_647:
        raise ValueError("Attempt number must be a positive PostgreSQL integer.")
    if isinstance(jitter, bool) or not math.isfinite(jitter) or not 0 <= jitter <= 1:
        raise ValueError("Jitter must be a finite sample between zero and one.")
    if attempt_number >= current.max_attempts:
        return None
    cap = min(
        current.max_backoff_ms, current.initial_backoff_ms * 2 ** (attempt_number - 1)
    )
    # Round upwards to an integer millisecond, retaining a positive delay even
    # for a one-millisecond policy. All exponentiation is bounded by max_attempts.
    return timedelta(milliseconds=math.ceil(cap * (0.5 + jitter * 0.5)))
