"""Failure and retry eligibility commit together; receipt replay never reschedules."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select, text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_completions import completion_schema as completion_schema
from tests.integration.test_completions import report, transaction
from workflow_engine.domain.retry import ExecutionPolicy
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.claim_requests import ClaimRequestRepository
from workflow_engine.repositories.claims import TaskClaim
from workflow_engine.repositories.completions import CompletionRepository
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    attempt_completions,
    task_attempts,
    task_retry_schedules,
    task_runs,
)

pytestmark = pytest.mark.integration


def make_claim(engine: Engine, schema: str, *, budget: int = 3) -> TaskClaim:
    with transaction(engine, schema) as connection:
        version = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                schema_version=2,
                name="retry_" + uuid4().hex,
                tasks=(
                    TaskDefinition(
                        task_id="A",
                        task_type="demo.fail",
                        execution=ExecutionPolicy(max_attempts=budget),
                    ),
                ),
            )
        )
        run = RunRepository(connection).create(version.id)
        worker = uuid4()
        WorkerRepository(connection, heartbeat_timeout_seconds=300).register(
            worker, worker_name="retry_worker", max_concurrency=1
        )
        claimed = ClaimRequestRepository(connection).claim_next(
            run.run.id, worker, request_id=uuid4()
        )
        assert claimed is not None
        return claimed


@pytest.mark.parametrize("budget,failed", [(1, True), (3, True), (3, False)])
def test_policy_controls_retry_and_replay_keeps_original_time(
    engine: Engine, completion_schema: str, budget: int, failed: bool
) -> None:
    claimed = make_claim(engine, completion_schema, budget=budget)
    submitted = report(claimed, failed=failed)
    with transaction(engine, completion_schema) as connection:
        receipt = CompletionRepository(connection).complete(submitted)
    with transaction(engine, completion_schema) as connection:
        assert CompletionRepository(connection).complete(submitted) == receipt
        status = connection.scalar(select(task_runs.c.status))
        rows = connection.execute(select(task_retry_schedules)).mappings().all()
        if failed and budget > 1:
            assert status == "RETRY_WAIT" and len(rows) == 1
            assert rows[0]["scheduled_at"] == receipt.accepted_at
            delay = rows[0]["available_at"] - receipt.accepted_at
            assert timedelta(milliseconds=500) <= delay <= timedelta(seconds=1)
        else:
            assert status == ("FAILED" if failed else "SUCCEEDED") and not rows
        assert connection.scalar(select(func.count()).select_from(task_attempts)) == 1


@pytest.mark.parametrize("failure", ["caller", "insert", "deferred"])
def test_retry_schedule_failures_roll_back_all_effects(
    engine: Engine, completion_schema: str, failure: str
) -> None:
    claimed = make_claim(engine, completion_schema)
    if failure != "caller":
        with transaction(engine, completion_schema) as connection:
            connection.execute(
                text("""
                CREATE FUNCTION reject_retry() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'injected retry failure'; END; $$
            """)
            )
            kind = "CONSTRAINT TRIGGER" if failure == "deferred" else "TRIGGER"
            timing = "AFTER" if failure == "deferred" else "BEFORE"
            deferred = "DEFERRABLE INITIALLY DEFERRED" if failure == "deferred" else ""
            connection.execute(
                text(
                    f"CREATE {kind} reject_retry {timing} INSERT "
                    "ON task_retry_schedules "
                    f"{deferred} FOR EACH ROW EXECUTE FUNCTION reject_retry()"
                )
            )
    with pytest.raises((RuntimeError, DBAPIError)):
        with transaction(engine, completion_schema) as connection:
            CompletionRepository(connection).complete(report(claimed, failed=True))
            if failure == "caller":
                raise RuntimeError("caller rollback")
    with transaction(engine, completion_schema) as connection:
        assert connection.scalar(select(task_runs.c.status)) == "RUNNING"
        assert connection.scalar(select(task_attempts.c.status)) == "RUNNING"
        for table in (task_retry_schedules, attempt_completions):
            assert connection.scalar(select(func.count()).select_from(table)) == 0


def test_concurrent_failure_reports_choose_one_retry_schedule(
    engine: Engine, completion_schema: str
) -> None:
    claimed = make_claim(engine, completion_schema)
    barrier = Barrier(2, timeout=5)

    def complete() -> object:
        barrier.wait()
        with transaction(engine, completion_schema) as connection:
            return CompletionRepository(connection).complete(
                report(claimed, failed=True)
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(complete) for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]
    assert results[0] == results[1]
    with transaction(engine, completion_schema) as connection:
        assert (
            connection.scalar(select(func.count()).select_from(task_retry_schedules))
            == 1
        )
