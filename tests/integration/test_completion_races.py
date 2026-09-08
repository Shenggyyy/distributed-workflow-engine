"""Controlled completion interleavings; recovery SQL is a test-only predecessor."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Event

import pytest
from sqlalchemy import Connection, Engine, Table, event, func, select
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from tests.integration.test_completions import (
    ClockedCompletions,
    report,
    transaction,
)
from tests.integration.test_completions import (
    claimed as claimed,
)
from tests.integration.test_completions import (
    completion_schema as completion_schema,
)
from workflow_engine.domain.completion import CompletionConflictError
from workflow_engine.domain.lease import LeaseExpiredError
from workflow_engine.repositories._ownership import lock_ownership
from workflow_engine.repositories.claims import TaskClaim
from workflow_engine.repositories.completions import (
    CompletionInactiveError,
    CompletionRepository,
)
from workflow_engine.repositories.leases import LeaseInactiveError, LeaseRepository
from workflow_engine.schema import (
    attempt_completions,
    attempt_leases,
    task_attempts,
    task_runs,
    worker_sessions,
    workflow_runs,
)

pytestmark = pytest.mark.integration
LOCK_TABLES = (workflow_runs, worker_sessions, task_runs, task_attempts, attempt_leases)


def watch_lock(connection: Connection, table: Table, entered: Event) -> None:
    def before_execute(
        conn: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if table.name in statement and "FOR UPDATE" in statement:
            entered.set()

    event.listen(connection, "before_cursor_execute", before_execute)


@pytest.mark.parametrize("table", LOCK_TABLES)
def test_clock_is_observed_after_every_lock(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
    table: Table,
) -> None:
    entered, sampled = Event(), Event()

    class ExpiredAfterWait(CompletionRepository):
        def _database_now(self) -> datetime:
            sampled.set()
            return claimed.lease.lease_expires_at

    def complete() -> None:
        with transaction(engine, completion_schema) as connection:
            watch_lock(connection, table, entered)
            ExpiredAfterWait(connection).complete(report(claimed))

    with ThreadPoolExecutor(max_workers=1) as executor:
        with transaction(engine, completion_schema) as owner:
            owner.execute(select(table).with_for_update()).all()
            pending = executor.submit(complete)
            assert (
                entered.wait(timeout=5) and not sampled.is_set() and not pending.done()
            )
        with pytest.raises(LeaseExpiredError):
            pending.result(timeout=10)
    assert sampled.is_set()
    with transaction(engine, completion_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(select(task_attempts.c.status)).scalar_one() == "RUNNING"
        )


@pytest.mark.parametrize("table", LOCK_TABLES)
def test_timeout_rolls_back_and_same_report_can_retry(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
    table: Table,
) -> None:
    with transaction(engine, completion_schema) as owner:
        owner.execute(select(table).with_for_update()).all()
        with pytest.raises(DBAPIError) as error:
            with transaction(engine, completion_schema) as contender:
                contender.exec_driver_sql("SET LOCAL lock_timeout = '100ms'")
                CompletionRepository(contender).complete(report(claimed))
        assert getattr(error.value.orig, "sqlstate", None) == "55P03"
    with transaction(engine, completion_schema) as connection:
        assert (
            connection.execute(select(task_attempts.c.status)).scalar_one() == "RUNNING"
        )
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
            ).scalar_one()
            == 0
        )
        receipt = CompletionRepository(connection).complete(report(claimed))
    with transaction(engine, completion_schema) as connection:
        assert CompletionRepository(connection).complete(report(claimed)) == receipt


@pytest.mark.parametrize("commit_owner", [False, True])
def test_completion_waits_for_renewal_and_uses_committed_deadline(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
    commit_owner: bool,
) -> None:
    entered = Event()

    class Renewal(LeaseRepository):
        def _database_now(self) -> datetime:
            return claimed.lease.acquired_at + timedelta(seconds=10)

    def complete() -> None:
        with transaction(engine, completion_schema) as connection:
            watch_lock(connection, workflow_runs, entered)
            ClockedCompletions(connection, claimed.lease.lease_expires_at).complete(
                report(claimed)
            )

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as owner:
            tx = owner.begin()
            try:
                migration_config(owner, completion_schema)
                renewed = Renewal(owner, lease_seconds=600).renew(
                    claimed.attempt.id,
                    worker_session_id=claimed.lease.worker_session_id,
                    lease_token=claimed.lease.lease_token,
                )
                assert renewed.lease_expires_at > claimed.lease.lease_expires_at
                pending = executor.submit(complete)
                assert entered.wait(timeout=5) and not pending.done()
                if commit_owner:
                    tx.commit()
                else:
                    tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
        if commit_owner:
            pending.result(timeout=10)
        else:
            with pytest.raises(LeaseExpiredError):
                pending.result(timeout=10)
    with transaction(engine, completion_schema) as connection:
        assert connection.execute(select(task_attempts.c.status)).scalar_one() == (
            "SUCCEEDED" if commit_owner else "RUNNING"
        )


@pytest.mark.parametrize("commit_owner", [False, True])
@pytest.mark.parametrize("following", ["renewal", "completion"])
def test_completion_predecessor_commit_or_rollback(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
    commit_owner: bool,
    following: str,
) -> None:
    entered = Event()

    def follow() -> None:
        with transaction(engine, completion_schema) as connection:
            watch_lock(connection, workflow_runs, entered)
            if following == "completion":
                CompletionRepository(connection).complete(report(claimed, failed=True))
            else:
                LeaseRepository(connection).renew(
                    claimed.attempt.id,
                    worker_session_id=claimed.lease.worker_session_id,
                    lease_token=claimed.lease.lease_token,
                )

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as owner:
            tx = owner.begin()
            try:
                migration_config(owner, completion_schema)
                CompletionRepository(owner).complete(report(claimed))
                pending = executor.submit(follow)
                assert entered.wait(timeout=5) and not pending.done()
                if commit_owner:
                    tx.commit()
                else:
                    tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
        if commit_owner:
            expected = (
                CompletionConflictError
                if following == "completion"
                else LeaseInactiveError
            )
            with pytest.raises(expected):
                pending.result(timeout=10)
        else:
            pending.result(timeout=10)
    with transaction(engine, completion_schema) as connection:
        expected_status = (
            "SUCCEEDED"
            if commit_owner
            else "FAILED"
            if following == "completion"
            else "RUNNING"
        )
        assert (
            connection.execute(select(task_attempts.c.status)).scalar_one()
            == expected_status
        )
        assert connection.execute(
            select(func.count()).select_from(attempt_completions)
        ).scalar_one() == (0 if expected_status == "RUNNING" else 1)


@pytest.mark.parametrize("commit_owner", [False, True])
def test_recovery_predecessor_fences_completion_at_expiry(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
    commit_owner: bool,
) -> None:
    entered = Event()

    def complete() -> None:
        with transaction(engine, completion_schema) as connection:
            watch_lock(connection, workflow_runs, entered)
            ClockedCompletions(connection, claimed.lease.lease_expires_at).complete(
                report(claimed)
            )

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as owner:
            tx = owner.begin()
            try:
                migration_config(owner, completion_schema)
                owned = lock_ownership(owner, claimed.attempt.id)
                owned.require_running()
                # Simulate a future recovery decision at the exclusive deadline.
                # This is not a production recovery scanner or retry scheduler.
                owner.execute(
                    task_attempts.update()
                    .where(task_attempts.c.id == claimed.attempt.id)
                    .values(status="LOST")
                )
                owner.execute(
                    task_runs.update()
                    .where(task_runs.c.id == claimed.task.id)
                    .values(status="FAILED")
                )
                pending = executor.submit(complete)
                assert entered.wait(timeout=5) and not pending.done()
                if commit_owner:
                    tx.commit()
                else:
                    tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
        with pytest.raises(
            CompletionInactiveError if commit_owner else LeaseExpiredError
        ):
            pending.result(timeout=10)
    with transaction(engine, completion_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
            ).scalar_one()
            == 0
        )
        assert connection.execute(select(task_attempts.c.status)).scalar_one() == (
            "LOST" if commit_owner else "RUNNING"
        )


def test_recovery_observes_committed_completion_instead_of_overwriting(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
) -> None:
    entered = Event()

    def recover() -> None:
        with transaction(engine, completion_schema) as connection:
            watch_lock(connection, workflow_runs, entered)
            owned = lock_ownership(connection, claimed.attempt.id)
            owned.require_running()  # Must reject before any recovery update.

    with ThreadPoolExecutor(max_workers=1) as executor:
        with transaction(engine, completion_schema) as owner:
            receipt = CompletionRepository(owner).complete(report(claimed))
            pending = executor.submit(recover)
            assert entered.wait(timeout=5) and not pending.done()
        with pytest.raises(LeaseInactiveError):
            pending.result(timeout=10)
    with transaction(engine, completion_schema) as connection:
        assert CompletionRepository(connection).complete(report(claimed)) == receipt
