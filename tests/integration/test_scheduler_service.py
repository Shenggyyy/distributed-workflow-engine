"""Scheduler service publishes readiness only after its transaction commits."""

import logging

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.exc import DBAPIError

from tests.integration.test_completions import claimed as claimed
from tests.integration.test_completions import completion_schema as completion_schema
from tests.integration.test_completions import report, transaction
from workflow_engine.repositories.claims import TaskClaim
from workflow_engine.repositories.completions import CompletionRepository
from workflow_engine.scheduler.service import SchedulerService
from workflow_engine.schema import task_runs

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
