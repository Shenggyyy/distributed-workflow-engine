"""Cohort readiness uses scoped committed membership and the database clock."""

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, func, select

from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.demo.startup import DemoStartupError, cohort_ready
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import demo_runs, demo_workers, worker_sessions

pytestmark = pytest.mark.integration


def run(engine: Engine) -> UUID:
    with engine.begin() as connection:
        version = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name="cohort_" + uuid4().hex,
                tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),),
            )
        )
        result = RunRepository(connection).create(version.id)
        connection.execute(
            demo_runs.insert().values(run_id=result.run.id, scenario="distribution")
        )
        return result.run.id


def member(
    engine: Engine,
    run_id: UUID,
    name: str,
    *,
    status: str = "ACTIVE",
    heartbeat_offset: int = 0,
    expiry_offset: int = 300,
) -> UUID:
    session = uuid4()
    with engine.begin() as connection:
        stamp = connection.execute(select(func.clock_timestamp())).scalar_one()
        connection.execute(
            worker_sessions.insert().values(
                id=session,
                worker_name=name,
                max_concurrency=1,
                status=status,
                created_at=stamp + timedelta(seconds=min(heartbeat_offset, 0) - 10),
                last_heartbeat_at=stamp + timedelta(seconds=heartbeat_offset),
                heartbeat_expires_at=stamp + timedelta(seconds=expiry_offset),
            )
        )
        connection.execute(
            demo_workers.insert().values(worker_session_id=session, run_id=run_id)
        )
    return session


def test_two_fresh_names_in_same_run_and_exact_self_required(
    http_engine: Engine,
) -> None:
    current = run(http_engine)
    own = member(http_engine, current, "demo-a")
    assert cohort_ready(http_engine, current, own, cohort_size=1)
    assert not cohort_ready(http_engine, current, own, cohort_size=2)
    member(http_engine, current, "demo-b")
    assert cohort_ready(http_engine, current, own, cohort_size=2)
    with pytest.raises(DemoStartupError, match="own fresh registered"):
        cohort_ready(http_engine, current, uuid4(), cohort_size=2)


@pytest.mark.parametrize(
    "case", ["duplicate", "other-run", "expired", "future", "LOST", "STOPPED"]
)
def test_unqualified_sessions_do_not_release_cohort(
    http_engine: Engine, case: str
) -> None:
    current = run(http_engine)
    own = member(http_engine, current, "demo-a")
    target = run(http_engine) if case == "other-run" else current
    member(
        http_engine,
        target,
        "demo-a" if case == "duplicate" else "demo-b",
        status=case if case in ("LOST", "STOPPED") else "ACTIVE",
        heartbeat_offset=-20 if case == "expired" else 30 if case == "future" else 0,
        expiry_offset=-1 if case == "expired" else 300,
    )
    assert not cohort_ready(http_engine, current, own, cohort_size=2)


@pytest.mark.parametrize("case", ["expired", "LOST", "STOPPED", "other-run"])
def test_invalid_own_session_cannot_use_other_fresh_peers(
    http_engine: Engine, case: str
) -> None:
    current = run(http_engine)
    target = run(http_engine) if case == "other-run" else current
    own = member(
        http_engine,
        target,
        "demo-a",
        status=case if case in ("LOST", "STOPPED") else "ACTIVE",
        heartbeat_offset=-20 if case == "expired" else 0,
        expiry_offset=-1 if case == "expired" else 300,
    )
    member(http_engine, current, "demo-b")
    member(http_engine, current, "demo-c")
    with pytest.raises(DemoStartupError, match="own fresh registered"):
        cohort_ready(http_engine, current, own, cohort_size=2)
