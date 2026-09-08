"""Demonstrate completion and lost-response replay without database or network."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from workflow_engine.domain.completion import (
    AttemptCompletion,
    CompletionConflictError,
    CompletionOutcome,
    CompletionResult,
    accept_completion,
)
from workflow_engine.domain.lease import AttemptLease, LeaseExpiredError
from workflow_engine.domain.runtime import TaskAttempt


def main() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    attempt = TaskAttempt(id=uuid4(), task_id=uuid4(), attempt_number=1)
    lease = AttemptLease(
        attempt_id=attempt.id,
        worker_session_id=uuid4(),
        lease_token=uuid4(),
        acquired_at=start,
        last_renewed_at=start,
        lease_expires_at=start + timedelta(seconds=30),
    )
    report = AttemptCompletion(
        attempt_id=attempt.id,
        worker_session_id=lease.worker_session_id,
        lease_token=lease.lease_token,
        result=CompletionResult(outcome=CompletionOutcome.SUCCEEDED),
    )
    receipt = accept_completion(attempt, lease, report, observed_at=start)
    print(f"Accepted outcome: {receipt.attempt.status}")
    try:
        accept_completion(attempt, lease, report, observed_at=lease.lease_expires_at)
    except LeaseExpiredError:
        print("Expired lease rejects a new completion.")
    print(f"Original receipt replayed: {receipt.replay(report) == receipt}")
    conflicting = AttemptCompletion(
        attempt_id=attempt.id,
        worker_session_id=lease.worker_session_id,
        lease_token=lease.lease_token,
        result=CompletionResult(
            outcome=CompletionOutcome.FAILED, error_code="handler_failed"
        ),
    )
    try:
        receipt.replay(conflicting)
    except CompletionConflictError:
        print("Different result rejected as a conflict.")
    print("In-memory contract only; no commit, execution or capacity release.")


if __name__ == "__main__":
    main()
