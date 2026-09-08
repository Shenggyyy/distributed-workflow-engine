"""Demonstrate lease boundaries with explicit times; no database or execution."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from workflow_engine.domain.lease import (
    AttemptLease,
    LeaseExpiredError,
    LeaseOwnershipError,
)


def main() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    lease = AttemptLease(
        attempt_id=uuid4(),
        worker_session_id=uuid4(),
        lease_token=uuid4(),
        acquired_at=start,
        last_renewed_at=start,
        lease_expires_at=start + timedelta(seconds=30),
    )
    owner = {
        "attempt_id": lease.attempt_id,
        "worker_session_id": lease.worker_session_id,
        "lease_token": lease.lease_token,
    }
    renewed = lease.renew(
        **owner, observed_at=start + timedelta(seconds=10), lease_seconds=30
    )
    print(f"Renewal preserves token: {renewed.lease_token == lease.lease_token}")
    elapsed = (renewed.lease_expires_at - start).total_seconds()
    print(f"Deadline after renewal: {elapsed:.0f}s")
    try:
        renewed.require_valid_owner(**owner, observed_at=renewed.lease_expires_at)
    except LeaseExpiredError:
        print("Exact deadline rejects the old owner.")
    try:
        renewed.require_valid_owner(
            **{**owner, "worker_session_id": uuid4()},
            observed_at=start + timedelta(seconds=11),
        )
    except LeaseOwnershipError:
        print("A restarted Worker cannot inherit the lease.")
    print("In-memory lease checks only; no task claim, persistence or execution.")


if __name__ == "__main__":
    main()
