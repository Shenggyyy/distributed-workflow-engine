"""Scheduler service publishes readiness only after its transaction commits."""

import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from uuid import UUID

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.exc import DBAPIError

from tests.integration.test_completions import claimed as claimed
from tests.integration.test_completions import completion_schema as completion_schema
from tests.integration.test_completions import report, transaction
from workflow_engine.repositories.claims import ClaimRepository, TaskClaim
from workflow_engine.repositories.completions import CompletionRepository
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.scheduler.service import SchedulerService
from workflow_engine.schema import task_runs, workflow_runs

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("fail_commit", [False, True])
def test_committed_progress_only(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
    fail_commit: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="workflow_engine.scheduler.service")
    with transaction(engine, completion_schema) as connection:
        CompletionRepository(connection).complete(report(claimed))
        if fail_commit:
            connection.exec_driver_sql(
                "CREATE FUNCTION reject_service_ready() RETURNS trigger "
                "LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'injected'; END; $$"
            )
            connection.exec_driver_sql(
                "CREATE CONSTRAINT TRIGGER reject_service_ready "
                "AFTER UPDATE ON task_runs DEFERRABLE INITIALLY DEFERRED "
                "FOR EACH ROW EXECUTE FUNCTION reject_service_ready()"
            )
    service = SchedulerService(
        engine.execution_options(schema_translate_map={None: completion_schema})
    )
    if fail_commit:
        with pytest.raises(DBAPIError):
            service.reconcile(claimed.task.run_id)
    else:
        assert service.reconcile(claimed.task.run_id) == 1
        assert service.reconcile(claimed.task.run_id) == 0
    with transaction(engine, completion_schema) as connection:
        assert connection.scalar(
            select(task_runs.c.status).where(task_runs.c.task_key == "C")
        ) == ("PENDING" if fail_commit else "READY")
    progress = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "tasks_ready"
    ]
    assert len(progress) == (0 if fail_commit else 1)


def test_global_scan_skips_busy_run_then_revisits(
    engine: Engine, completion_schema: str, claimed: TaskClaim
) -> None:
    with transaction(engine, completion_schema) as connection:
        CompletionRepository(connection).complete(report(claimed))
        version_id = connection.scalar(
            select(workflow_runs.c.workflow_version_id).where(
                workflow_runs.c.id == claimed.task.run_id
            )
        )
        assert isinstance(version_id, UUID)
        other = RunRepository(connection).create(version_id)
        second = ClaimRepository(connection).claim_next(
            other.run.id, claimed.lease.worker_session_id
        )
        assert second is not None
        CompletionRepository(connection).complete(report(second))
    service = SchedulerService(
        engine.execution_options(schema_translate_map={None: completion_schema})
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        with transaction(engine, completion_schema) as owner:
            owner.execute(
                select(workflow_runs)
                .where(workflow_runs.c.id == claimed.task.run_id)
                .with_for_update()
            ).all()
            result = executor.submit(service.run, None, Event(), once=True)
            assert result.result(timeout=3) == 1
    assert service.run(None, Event(), once=True) == 1
    assert service.run(None, Event(), once=True) == 0


def test_concurrent_global_scans_publish_readiness_once(
    engine: Engine, completion_schema: str, claimed: TaskClaim
) -> None:
    with transaction(engine, completion_schema) as connection:
        CompletionRepository(connection).complete(report(claimed))
    barrier = Barrier(3)

    def scan() -> int:
        service = SchedulerService(
            engine.execution_options(schema_translate_map={None: completion_schema})
        )
        barrier.wait(timeout=5)
        return service.run(None, Event(), once=True)

    with ThreadPoolExecutor(max_workers=3) as executor:
        results = [executor.submit(scan) for _ in range(3)]
        assert sum(result.result(timeout=10) for result in results) == 1


def test_scan_has_one_transaction_per_run_and_bounded_pages(
    engine: Engine,
    completion_schema: str,
    claimed: TaskClaim,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with transaction(engine, completion_schema) as connection:
        version_id = connection.scalar(select(workflow_runs.c.workflow_version_id))
        assert isinstance(version_id, UUID)
        for _ in range(4):
            RunRepository(connection).create(version_id)
        expected = tuple(
            connection.scalars(select(workflow_runs.c.id).order_by(workflow_runs.c.id))
        )
    service = SchedulerService(
        engine.execution_options(schema_translate_map={None: completion_schema}),
        page_size=2,
    )
    seen: list[UUID] = []
    original = service.reconcile

    def visit(run_id: UUID, *, skip_locked: bool = False) -> int:
        seen.append(run_id)
        return original(run_id, skip_locked=skip_locked)

    monkeypatch.setattr(service, "reconcile", visit)
    cursor = None
    for count in (2, 4, 5):
        _, cursor = service.scan_page(cursor, Event())
        assert len(seen) == count
    assert cursor is None and tuple(seen) == expected
    stopped = Event()
    stopped.set()
    service.scan_page(None, stopped)
    assert tuple(seen) == expected
