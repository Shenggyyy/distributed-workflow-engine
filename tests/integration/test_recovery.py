"""Expiry settlement, repeated recovery, stale results and post-lock clocks."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Event

import pytest
from sqlalchemy import Connection, Engine, Table, func, select

from tests.integration.test_completion_races import LOCK_TABLES, watch_lock
from tests.integration.test_completions import ClockedCompletions, report, transaction
from tests.integration.test_completions import completion_schema as completion_schema
from tests.integration.test_lease_renewal import ClockedLeases
from tests.integration.test_retry_completion import make_claim
from workflow_engine.repositories.completions import CompletionInactiveError
from workflow_engine.repositories.recovery import RecoveryRepository
from workflow_engine.schema import (
    attempt_completions,
    task_attempts,
    task_retry_schedules,
    task_runs,
    workflow_runs,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("commit", [False, True])
def test_recovery_waits_for_predecessor_renewal(
    engine: Engine, completion_schema: str, commit: bool
) -> None:
    claim = make_claim(engine, completion_schema)
    entered = Event()

    def recover() -> object:
        with transaction(engine, completion_schema) as connection:
            watch_lock(connection, workflow_runs, entered)
            return ClockedRecovery(connection, claim.lease.lease_expires_at).recover(
                claim.attempt.id
            )

    with ThreadPoolExecutor(max_workers=1) as pool:
        with engine.connect() as owner:
            tx = owner.begin()
            from tests.integration.migration_helpers import migration_config

            migration_config(owner, completion_schema)
            ClockedLeases(
                owner, claim.lease.lease_expires_at - timedelta(seconds=1)
            ).renew(
                claim.attempt.id,
                worker_session_id=claim.lease.worker_session_id,
                lease_token=claim.lease.lease_token,
            )
            future = pool.submit(recover)
            assert entered.wait(5) and not future.done()
            if commit:
                tx.commit()
            else:
                tx.rollback()
        result = future.result(timeout=5)
        assert (result is None) is commit


class ClockedRecovery(RecoveryRepository):
    def __init__(self, connection: Connection, observed: datetime) -> None:
        super().__init__(connection)
        self.observed = observed

    def _database_now(self) -> datetime:
        return self.observed


@pytest.mark.parametrize(
    "timeout,expected", [(1, "TIMED_OUT"), (30, "TIMED_OUT"), (300, "LOST")]
)
@pytest.mark.parametrize("budget", [1, 3])
def test_expiry_classification_retry_and_stale_result(
    engine: Engine, completion_schema: str, timeout: int, expected: str, budget: int
) -> None:
    claim = make_claim(engine, completion_schema, timeout=timeout, budget=budget)
    deadline = min(
        claim.lease.lease_expires_at,
        claim.lease.acquired_at + timedelta(seconds=timeout),
    )
    with transaction(engine, completion_schema) as connection:
        assert (
            ClockedRecovery(connection, deadline - timedelta(microseconds=1)).recover(
                claim.attempt.id
            )
            is None
        )
    with pytest.raises(RuntimeError):
        with transaction(engine, completion_schema) as connection:
            assert (
                ClockedRecovery(connection, deadline).recover(claim.attempt.id)
                is not None
            )
            raise RuntimeError("rollback")
    with transaction(engine, completion_schema) as connection:
        assert connection.scalar(select(task_attempts.c.status)) == "RUNNING"
        # A delayed scan does not change which deadline expired first.
        result = ClockedRecovery(connection, deadline + timedelta(hours=1)).recover(
            claim.attempt.id
        )
        assert result is not None and result.status.value == expected
    with transaction(engine, completion_schema) as connection:
        assert (
            ClockedRecovery(connection, deadline + timedelta(days=2)).recover(
                claim.attempt.id
            )
            is None
        )
        with pytest.raises(CompletionInactiveError):
            ClockedCompletions(connection, deadline).complete(report(claim))
        assert connection.scalar(select(task_runs.c.status)) == (
            "FAILED" if budget == 1 else "RETRY_WAIT"
        )
        assert connection.scalar(
            select(func.count()).select_from(task_retry_schedules)
        ) == (0 if budget == 1 else 1)
        assert (
            connection.scalar(select(func.count()).select_from(attempt_completions))
            == 0
        )


@pytest.mark.parametrize("table", LOCK_TABLES)
def test_recovery_samples_clock_after_all_locks(
    engine: Engine, completion_schema: str, table: Table
) -> None:
    claim = make_claim(engine, completion_schema, timeout=1)
    entered, sampled = Event(), Event()
    deadline = claim.lease.acquired_at + timedelta(seconds=1)

    class AfterLocks(RecoveryRepository):
        def _database_now(self) -> datetime:
            sampled.set()
            return deadline

    def recover() -> object:
        with transaction(engine, completion_schema) as connection:
            watch_lock(connection, table, entered)
            return AfterLocks(connection).recover(claim.attempt.id)

    with ThreadPoolExecutor(max_workers=1) as pool:
        with transaction(engine, completion_schema) as owner:
            owner.execute(select(table).with_for_update()).all()
            future = pool.submit(recover)
            assert entered.wait(5) and not sampled.wait(0.05)
        assert future.result(timeout=5) is not None
    assert sampled.is_set()


def test_committed_completion_wins_over_recovery(
    engine: Engine, completion_schema: str
) -> None:
    claim = make_claim(engine, completion_schema, timeout=1)
    with transaction(engine, completion_schema) as connection:
        receipt = ClockedCompletions(connection, claim.lease.acquired_at).complete(
            report(claim)
        )
    with transaction(engine, completion_schema) as connection:
        assert (
            ClockedRecovery(connection, claim.lease.lease_expires_at).recover(
                claim.attempt.id
            )
            is None
        )
        assert (
            ClockedCompletions(connection, claim.lease.lease_expires_at).complete(
                report(claim)
            )
            == receipt
        )
