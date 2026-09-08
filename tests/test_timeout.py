"""Fixed deadline derivation, default duration and exact admission boundary."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from workflow_engine.domain.lease import AttemptLease
from workflow_engine.domain.retry import ExecutionPolicy
from workflow_engine.domain.timeout import (
    AttemptTimeoutError,
    attempt_deadline,
    require_before_timeout,
)


def test_deadline_uses_acquisition_even_after_renewal() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    lease = AttemptLease(
        attempt_id=uuid4(),
        worker_session_id=uuid4(),
        lease_token=uuid4(),
        acquired_at=start,
        last_renewed_at=start + timedelta(seconds=299),
        lease_expires_at=start + timedelta(seconds=600),
    )
    policy = ExecutionPolicy()
    deadline = start + timedelta(seconds=300)
    assert attempt_deadline(lease, policy) == deadline
    require_before_timeout(lease, policy, deadline - timedelta(microseconds=1))
    with pytest.raises(AttemptTimeoutError):
        require_before_timeout(lease, policy, deadline)
    with pytest.raises(ValueError):
        require_before_timeout(lease, policy, deadline.replace(tzinfo=None))
