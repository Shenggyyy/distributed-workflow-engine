"""Fixed per-Attempt deadline, independent of renewable ownership."""

from datetime import datetime, timedelta

from workflow_engine.domain.lease import AttemptLease
from workflow_engine.domain.retry import ExecutionPolicy


class AttemptTimeoutError(ValueError):
    """The fixed execution deadline has elapsed."""


def attempt_deadline(lease: AttemptLease, policy: ExecutionPolicy) -> datetime:
    current = AttemptLease.model_validate(lease)
    settings = ExecutionPolicy.model_validate(policy)
    try:
        return current.acquired_at + timedelta(seconds=settings.timeout_seconds)
    except OverflowError:
        raise ValueError("Attempt deadline is outside the supported range.") from None


def require_before_timeout(
    lease: AttemptLease, policy: ExecutionPolicy, observed_at: datetime
) -> None:
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise ValueError("Attempt observation must be an aware datetime.")
    if observed_at >= attempt_deadline(lease, policy):
        raise AttemptTimeoutError("Attempt execution deadline elapsed.")
