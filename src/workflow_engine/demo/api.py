"""Opt-in fixed scenarios and coherent, scoped demonstration snapshots."""

from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, FastAPI
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine, String, cast, func, select, text

from workflow_engine.api.app import create_app
from workflow_engine.api.dependencies import get_engine
from workflow_engine.api.errors import APIError
from workflow_engine.config import Settings
from workflow_engine.domain.retry import ExecutionPolicy
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    attempt_completions,
    attempt_leases,
    demo_invocations,
    demo_runs,
    demo_samples,
    demo_workers,
    task_attempts,
    task_retry_schedules,
    task_runs,
    worker_sessions,
    workflow_runs,
    workflow_versions,
)

Scenario = Literal["parallel", "distribution", "recovery"]
Database = Annotated[Engine, Depends(get_engine)]
router = APIRouter(prefix="/demo", tags=["local demonstration"])


class ScenarioRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    scenario: Scenario


def definition(scenario: Scenario) -> WorkflowDefinition:
    roots = ("A",) if scenario == "recovery" else ("A", "B", "C", "D")
    policy = ExecutionPolicy(
        max_attempts=2,
        timeout_seconds=90,
        initial_backoff_ms=10000,
        max_backoff_ms=10000,
    )
    return WorkflowDefinition(
        name=f"demo_{scenario}_{uuid4().hex}",
        schema_version=2,
        tasks=tuple(
            TaskDefinition(
                task_id=key,
                task_type="demo.recover" if scenario == "recovery" else "demo.observe",
                execution=policy,
            )
            for key in roots
        )
        + (
            TaskDefinition(
                task_id="Join",
                task_type="demo.join",
                depends_on=roots,
                execution=policy,
            ),
        ),
    )


@router.post("/runs", status_code=201)
def create_run(body: ScenarioRequest, engine: Database) -> dict[str, object]:
    """Every explicit request creates a fresh run; do not automatically retry POST."""
    with engine.begin() as connection:
        version = WorkflowRepository(connection).publish(definition(body.scenario))
        result = RunRepository(connection).create(version.id)
        connection.execute(
            demo_runs.insert().values(run_id=result.run.id, scenario=body.scenario)
        )
    return {"run_id": result.run.id, "scenario": body.scenario}


@router.get("/runs")
def list_runs(engine: Database) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                select(demo_runs, workflow_runs.c.status)
                .join(workflow_runs, workflow_runs.c.id == demo_runs.c.run_id)
                .order_by(demo_runs.c.created_at.desc(), demo_runs.c.run_id)
                .limit(50)
            ).mappings()
        ]


@router.get("/runs/{run_id}")
def snapshot(run_id: UUID, engine: Database) -> dict[str, Any]:
    """One MVCC snapshot; expose selected evidence fields, never ownership tokens."""
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            stamp = conn.scalar(select(func.clock_timestamp()))
            run = (
                conn.execute(
                    select(
                        workflow_runs,
                        demo_runs.c.scenario,
                        workflow_versions.c.definition,
                    )
                    .join(demo_runs, demo_runs.c.run_id == workflow_runs.c.id)
                    .join(
                        workflow_versions,
                        workflow_versions.c.id == workflow_runs.c.workflow_version_id,
                    )
                    .where(workflow_runs.c.id == run_id)
                )
                .mappings()
                .one_or_none()
            )
            if run is None:
                raise APIError(404, "demo_run_not_found", "Demo Run not found.")
            tasks = list(
                conn.execute(
                    select(task_runs)
                    .where(task_runs.c.run_id == run_id)
                    .order_by(task_runs.c.task_key)
                ).mappings()
            )
            attempts = list(
                conn.execute(
                    select(
                        task_attempts.c.id,
                        task_attempts.c.task_id,
                        task_attempts.c.attempt_number,
                        task_attempts.c.status,
                        attempt_leases.c.worker_session_id,
                        attempt_leases.c.acquired_at,
                        attempt_leases.c.lease_expires_at,
                        attempt_completions.c.accepted_at,
                        task_retry_schedules.c.scheduled_at,
                        task_retry_schedules.c.available_at,
                    )
                    .join(task_runs, task_runs.c.id == task_attempts.c.task_id)
                    .outerjoin(
                        attempt_leases,
                        attempt_leases.c.attempt_id == task_attempts.c.id,
                    )
                    .outerjoin(
                        attempt_completions,
                        attempt_completions.c.attempt_id == task_attempts.c.id,
                    )
                    .outerjoin(
                        task_retry_schedules,
                        task_retry_schedules.c.attempt_id == task_attempts.c.id,
                    )
                    .where(task_runs.c.run_id == run_id)
                    .order_by(task_runs.c.task_key, task_attempts.c.attempt_number)
                ).mappings()
            )
            workers = list(
                conn.execute(
                    select(worker_sessions)
                    .join(
                        demo_workers,
                        demo_workers.c.worker_session_id == worker_sessions.c.id,
                    )
                    .where(demo_workers.c.run_id == run_id)
                    .order_by(worker_sessions.c.created_at)
                ).mappings()
            )
            samples = list(
                conn.execute(
                    select(
                        demo_invocations.c.id.label("invocation_id"),
                        demo_invocations.c.attempt_id,
                        demo_invocations.c.clock_domain,
                        demo_samples.c.sequence,
                        demo_samples.c.phase,
                        cast(demo_samples.c.monotonic_ns, String).label("monotonic_ns"),
                        demo_samples.c.recorded_at,
                    )
                    .join(
                        demo_samples,
                        demo_samples.c.invocation_id == demo_invocations.c.id,
                    )
                    .join(
                        task_attempts,
                        task_attempts.c.id == demo_invocations.c.attempt_id,
                    )
                    .join(task_runs, task_runs.c.id == task_attempts.c.task_id)
                    .where(task_runs.c.run_id == run_id)
                    .order_by(demo_invocations.c.id, demo_samples.c.sequence)
                ).mappings()
            )
    return {
        "snapshot_at": stamp,
        "run": dict(run),
        "tasks": [dict(row) for row in tasks],
        "attempts": [dict(row) for row in attempts],
        "workers": [dict(row) for row in workers],
        "samples": [dict(row) for row in samples],
    }


def create_demo_app(settings: Settings, *, engine: Engine | None = None) -> FastAPI:
    app = create_app(settings, engine=engine)
    app.include_router(router)
    app.mount(
        "/demo",
        StaticFiles(directory=Path(__file__).with_name("static"), html=True),
        name="demo-page",
    )
    return app
