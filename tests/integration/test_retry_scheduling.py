"""Due-time boundaries, latest-Attempt selection and concurrent promotion."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import Connection, Engine, select

from tests.integration.test_completions import completion_schema as completion_schema
from tests.integration.test_completions import report, transaction
from tests.integration.test_retry_completion import make_claim
from workflow_engine.repositories.claim_requests import ClaimRequestRepository
from workflow_engine.repositories.completions import CompletionRepository
from workflow_engine.repositories.scheduling import SchedulingRepository
from workflow_engine.schema import task_retry_schedules, task_runs

pytestmark = pytest.mark.integration


class ClockedScheduling(SchedulingRepository):
    def __init__(self, connection: Connection, observed: datetime) -> None:
        super().__init__(connection)
        self.observed = observed

    def _database_now(self) -> datetime:
        return self.observed


def test_retry_budget_latest_attempt_and_rollback(
    engine: Engine, completion_schema: str
) -> None:
    claim = make_claim(engine, completion_schema)
    original_task = claim.task.id
    first_due = None
    for number in (1, 2, 3):
        assert claim.attempt.attempt_number == number and claim.task.id == original_task
        with transaction(engine, completion_schema) as connection:
            receipt = CompletionRepository(connection).complete(
                report(claim, failed=True)
            )
        with transaction(engine, completion_schema) as connection:
            if number == 3:
                assert connection.scalar(select(task_runs.c.status)) == "FAILED"
                assert (
                    SchedulingRepository(connection).reconcile(claim.task.run_id) == ()
                )
                break
            due = connection.scalar(
                select(task_retry_schedules.c.available_at).where(
                    task_retry_schedules.c.attempt_id == claim.attempt.id
                )
            )
            assert isinstance(due, datetime)
            if first_due is None:
                first_due = due
            assert (
                ClockedScheduling(
                    connection, due - timedelta(microseconds=1)
                ).reconcile(claim.task.run_id)
                == ()
            )
            if number == 2:
                # The old eligibility has passed, but cannot authorize Attempt 3.
                assert (
                    ClockedScheduling(connection, first_due).reconcile(
                        claim.task.run_id
                    )
                    == ()
                )
        with pytest.raises(RuntimeError):
            with transaction(engine, completion_schema) as connection:
                assert (
                    len(ClockedScheduling(connection, due).reconcile(claim.task.run_id))
                    == 1
                )
                raise RuntimeError("rollback promotion")
        with transaction(engine, completion_schema) as connection:
            assert connection.scalar(select(task_runs.c.status)) == "RETRY_WAIT"
            assert (
                len(ClockedScheduling(connection, due).reconcile(claim.task.run_id))
                == 1
            )
            assert ClockedScheduling(connection, due).reconcile(claim.task.run_id) == ()
            assert (
                CompletionRepository(connection).complete(report(claim, failed=True))
                == receipt
            )
        with transaction(engine, completion_schema) as connection:
            replacement = ClaimRequestRepository(connection).claim_next(
                claim.task.run_id, claim.lease.worker_session_id, request_id=uuid4()
            )
            assert (
                replacement is not None and replacement.attempt.id != claim.attempt.id
            )
            claim = replacement


def test_two_schedulers_promote_once(engine: Engine, completion_schema: str) -> None:
    claim = make_claim(engine, completion_schema)
    with transaction(engine, completion_schema) as connection:
        CompletionRepository(connection).complete(report(claim, failed=True))
        due = connection.scalar(select(task_retry_schedules.c.available_at))
    assert isinstance(due, datetime)
    barrier = Barrier(2, timeout=5)

    def promote() -> int:
        barrier.wait()
        with transaction(engine, completion_schema) as connection:
            return len(ClockedScheduling(connection, due).reconcile(claim.task.run_id))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(promote) for _ in range(2)]
        assert sorted(future.result(timeout=10) for future in futures) == [0, 1]
