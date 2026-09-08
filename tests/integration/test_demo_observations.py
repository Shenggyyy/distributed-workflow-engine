"""Real database scope, sequence and evidence/ownership separation."""

from dataclasses import replace
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, select

from tests.integration.test_completion_schema import seed
from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.config import Settings
from workflow_engine.demo.handlers import ObservedHandler
from workflow_engine.demo.observations import ObservationError, append, start
from workflow_engine.schema import (
    demo_runs,
    demo_samples,
    demo_workers,
    task_attempts,
    task_runs,
    workflow_runs,
)
from workflow_engine.worker.handlers import HandlerContext

pytestmark = pytest.mark.integration


def context_for(engine: Engine, *, registered: bool = True) -> HandlerContext:
    with engine.begin() as connection:
        data = seed(connection)
        row = (
            connection.execute(
                select(task_runs, workflow_runs.c.workflow_version_id).join(
                    workflow_runs, workflow_runs.c.id == task_runs.c.run_id
                )
            )
            .mappings()
            .one()
        )
        if registered:
            connection.execute(
                demo_runs.insert().values(run_id=row["run_id"], scenario="parallel")
            )
            connection.execute(
                demo_workers.insert().values(
                    worker_session_id=data["worker_session_id"], run_id=row["run_id"]
                )
            )
        return HandlerContext(
            run_id=row["run_id"],
            workflow_version_id=row["workflow_version_id"],
            task_id=row["id"],
            task_key=row["task_key"],
            attempt_id=UUID(str(data["attempt_id"])),
            attempt_number=1,
        )


def test_samples_do_not_complete_attempt(http_engine: Engine) -> None:
    context = context_for(http_engine)
    invocation = start(http_engine, context, "kernel:namespace", 10)
    append(http_engine, invocation, "PULSE", 20)
    append(http_engine, invocation, "FINISH", 30)
    with http_engine.connect() as connection:
        assert connection.scalar(select(task_attempts.c.status)) == "RUNNING"
        assert list(
            connection.scalars(
                select(demo_samples.c.monotonic_ns).order_by(demo_samples.c.sequence)
            )
        ) == [10, 20, 30]
    with pytest.raises(ObservationError):
        append(http_engine, invocation, "PULSE", 40)


def test_non_demo_is_rejected(http_engine: Engine) -> None:
    with pytest.raises(ObservationError):
        start(http_engine, context_for(http_engine, registered=False), "kernel", 1)


@pytest.mark.parametrize(
    "field", ["run_id", "task_id", "attempt_id", "workflow_version_id"]
)
def test_context_mismatch_is_rejected(http_engine: Engine, field: str) -> None:
    context = context_for(http_engine).model_copy(update={field: uuid4()})
    with pytest.raises(ObservationError):
        start(http_engine, context, "kernel", 1)


def test_regressing_clock_rejected(http_engine: Engine) -> None:
    invocation = start(http_engine, context_for(http_engine), "kernel", 10)
    with pytest.raises(ObservationError):
        append(http_engine, invocation, "PULSE", 9)


def test_interruption_has_no_fabricated_finish(http_engine: Engine) -> None:
    from contextlib import nullcontext

    context = context_for(http_engine)
    handler = ObservedHandler(Settings(), 60)
    with (
        patch("workflow_engine.demo.handlers.clock_domain", return_value="kernel"),
        patch(
            "workflow_engine.demo.handlers.database_engine",
            return_value=nullcontext(http_engine),
        ),
        patch(
            "workflow_engine.demo.handlers.time.sleep", side_effect=KeyboardInterrupt
        ),
        pytest.raises(KeyboardInterrupt),
    ):
        handler(context)
    with http_engine.connect() as connection:
        assert list(connection.scalars(select(demo_samples.c.phase))) == ["START"]
    with pytest.raises(ValueError):
        replace(handler, seconds=61)
