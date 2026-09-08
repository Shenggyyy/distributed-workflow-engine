"""Retry budget boundaries, jitter extrema and immutable policy validation."""

from datetime import timedelta

import pytest
from pydantic import ValidationError

from workflow_engine.domain.retry import ExecutionPolicy, retry_delay


@pytest.mark.parametrize(
    "changes",
    [
        {"max_attempts": 0},
        {"max_attempts": 101},
        {"max_attempts": True},
        {"max_attempts": "2"},
        {"timeout_seconds": 0},
        {"timeout_seconds": 86401},
        {"timeout_seconds": 1.5},
        {"initial_backoff_ms": 0},
        {"max_backoff_ms": 86400001},
        {"initial_backoff_ms": 20, "max_backoff_ms": 10},
        {"unknown": 1},
    ],
)
def test_invalid_policy(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ExecutionPolicy.model_validate(changes)


def test_budget_includes_first_attempt_and_defaults_to_no_retry() -> None:
    assert retry_delay(ExecutionPolicy(), 1, jitter=0.5) is None
    policy = ExecutionPolicy(max_attempts=3)
    assert retry_delay(policy, 1, jitter=1) == timedelta(seconds=1)
    assert retry_delay(policy, 2, jitter=1) == timedelta(seconds=2)
    assert retry_delay(policy, 3, jitter=1) is None
    assert retry_delay(policy, 2_147_483_647, jitter=1) is None


@pytest.mark.parametrize("attempt,cap", [(1, 1000), (2, 2000), (3, 2500), (99, 2500)])
def test_exponential_delay_caps_before_jitter(attempt: int, cap: int) -> None:
    policy = ExecutionPolicy(max_attempts=100, max_backoff_ms=2500)
    assert retry_delay(policy, attempt, jitter=0) == timedelta(milliseconds=cap // 2)
    assert retry_delay(policy, attempt, jitter=1) == timedelta(milliseconds=cap)
    middle = retry_delay(policy, attempt, jitter=0.5)
    assert middle is not None and timedelta(
        milliseconds=cap // 2
    ) <= middle <= timedelta(milliseconds=cap)


def test_smallest_delay_is_positive() -> None:
    policy = ExecutionPolicy(max_attempts=2, initial_backoff_ms=1, max_backoff_ms=1)
    assert retry_delay(policy, 1, jitter=0) == timedelta(milliseconds=1)


@pytest.mark.parametrize("jitter", [-0.1, 1.1, float("nan"), float("inf"), True])
def test_invalid_entropy(jitter: float) -> None:
    with pytest.raises(ValueError):
        retry_delay(ExecutionPolicy(max_attempts=2), 1, jitter=jitter)


@pytest.mark.parametrize("attempt", [0, -1, True, 2_147_483_648])
def test_invalid_attempt(attempt: int) -> None:
    with pytest.raises(ValueError):
        retry_delay(ExecutionPolicy(max_attempts=2), attempt, jitter=0.5)


def test_frozen_policy_roundtrip_and_revalidation() -> None:
    policy = ExecutionPolicy(max_attempts=4)
    assert ExecutionPolicy.model_validate_json(policy.model_dump_json()) == policy
    with pytest.raises(ValidationError):
        policy.max_attempts = 9  # type: ignore[misc]
    with pytest.raises(ValidationError):
        retry_delay(policy.model_copy(update={"max_attempts": 0}), 1, jitter=0.5)
