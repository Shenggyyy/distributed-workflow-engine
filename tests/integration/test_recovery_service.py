"""Scheduler restart and shared recovery require no process-local timers."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest
from sqlalchemy import Engine, func, select

from tests.integration.test_completions import completion_schema as completion_schema
from tests.integration.test_completions import transaction
from tests.integration.test_retry_completion import make_claim
from workflow_engine.repositories.recovery import RecoveryRepository
from workflow_engine.repositories.recovery_discovery import RecoveryDiscoveryRepository
from workflow_engine.repositories.scheduling import SchedulingRepository
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.scheduler.service import SchedulerService
from workflow_engine.schema import (
    task_attempts,
    task_retry_schedules,
    task_runs,
    worker_sessions,
    workflow_runs,
)

pytestmark = pytest.mark.integration


def test_recovery_skips_busy_run_and_continues_independent_run(
    engine: Engine, completion_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = make_claim(engine, completion_schema)
    second = make_claim(engine, completion_schema)
    observed = second.lease.lease_expires_at + timedelta(seconds=1)
    for repository in (RecoveryRepository, RecoveryDiscoveryRepository):
        monkeypatch.setattr(repository, "_database_now", lambda self: observed)
    mapped = engine.execution_options(schema_translate_map={None: completion_schema})
    with transaction(engine, completion_schema) as owner:
        owner.execute(
            select(workflow_runs)
            .where(workflow_runs.c.id == first.task.run_id)
            .with_for_update()
        ).all()
        SchedulerService(mapped).run(None, Event(), once=True)
    with transaction(engine, completion_schema) as connection:
        statuses = {
            row.id: row.status
            for row in connection.execute(
                select(task_attempts.c.id, task_attempts.c.status)
            )
        }
        assert statuses[first.attempt.id] == "RUNNING"
        assert statuses[second.attempt.id] == "LOST"


def test_two_schedulers_recover_once_and_restart_promotes_retry(
    engine: Engine, completion_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    claim = make_claim(engine, completion_schema)
    observed = claim.lease.acquired_at + timedelta(seconds=400)
    for repository in (
        RecoveryRepository,
        RecoveryDiscoveryRepository,
        WorkerRepository,
    ):
        monkeypatch.setattr(repository, "_database_now", lambda self: observed)
    mapped = engine.execution_options(schema_translate_map={None: completion_schema})
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(SchedulerService(mapped).run, None, Event(), once=True)
            for _ in range(2)
        ]
        for future in futures:
            future.result(timeout=10)
    with transaction(engine, completion_schema) as connection:
        assert connection.scalar(select(task_attempts.c.status)) == "LOST"
        assert connection.scalar(select(worker_sessions.c.status)) == "LOST"
        assert connection.scalar(select(task_runs.c.status)) == "RETRY_WAIT"
        assert (
            connection.scalar(select(func.count()).select_from(task_retry_schedules))
            == 1
        )
        due = connection.scalar(select(task_retry_schedules.c.available_at))
    monkeypatch.setattr(SchedulingRepository, "_database_now", lambda self: due)
    assert SchedulerService(mapped).run(None, Event(), once=True) == 1
    with transaction(engine, completion_schema) as connection:
        assert connection.scalar(select(task_runs.c.status)) == "READY"


def test_worker_expiry_does_not_recover_live_attempt(
    engine: Engine, completion_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    claim = make_claim(engine, completion_schema)
    # Mark Worker due through its own clock; Attempt discovery still sees live time.
    monkeypatch.setattr(
        WorkerRepository,
        "_database_now",
        lambda self: claim.lease.acquired_at + timedelta(seconds=400),
    )
    monkeypatch.setattr(
        RecoveryDiscoveryRepository,
        "workers",
        lambda self: (claim.lease.worker_session_id,),
    )
    mapped = engine.execution_options(schema_translate_map={None: completion_schema})
    assert SchedulerService(mapped).run(None, Event(), once=True) == 0
    with transaction(engine, completion_schema) as connection:
        assert connection.scalar(select(worker_sessions.c.status)) == "LOST"
        assert connection.scalar(select(task_attempts.c.status)) == "RUNNING"
