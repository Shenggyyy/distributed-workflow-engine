"""Bounded custom demo membership; never task ownership or Worker routing."""

from typing import Any
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import Connection, Engine, RowMapping, and_, func, select
from sqlalchemy.sql import ColumnElement

from workflow_engine.demo.custom_definitions import (
    CustomDefinitionError,
    normalize_custom,
)
from workflow_engine.demo.startup import DemoStartupError
from workflow_engine.domain.worker import WorkerSession, WorkerStatus
from workflow_engine.domain.workflow import WorkflowDefinition
from workflow_engine.schema import (
    demo_runs,
    demo_workers,
    worker_sessions,
    workflow_runs,
    workflow_versions,
)


def custom_worker_names(run_id: UUID) -> tuple[str, str]:
    return (f"dwe-demo-{run_id.hex}-a", f"dwe-demo-{run_id.hex}-b")


def _scenario(connection: Connection, run_id: UUID, *, lock: bool) -> str:
    statement = select(demo_runs.c.scenario).where(demo_runs.c.run_id == run_id)
    if lock:
        statement = statement.with_for_update()
    scenario: str | None = connection.scalar(statement)
    if scenario is None:
        raise DemoStartupError("Only registered demo Runs may be executed.")
    return scenario


def _session_fields() -> tuple[ColumnElement[Any], ...]:
    return (
        worker_sessions.c.worker_name,
        worker_sessions.c.max_concurrency,
        worker_sessions.c.status,
        and_(
            worker_sessions.c.last_heartbeat_at <= func.statement_timestamp(),
            worker_sessions.c.heartbeat_expires_at > func.statement_timestamp(),
        ).label("fresh"),
    )


def _valid_member(row: RowMapping, names: tuple[str, str]) -> bool:
    return (
        row["worker_name"] in names
        and row["max_concurrency"] == 1
        and row["status"] == "ACTIVE"
        and row["fresh"] is True
    )


def _check_custom(
    connection: Connection,
    run_id: UUID,
    session: WorkerSession,
    *,
    cohort_size: int,
    registered: bool,
) -> None:
    try:
        session = WorkerSession.model_validate(session)
    except ValidationError:
        raise DemoStartupError("Custom demo Worker identity is invalid.") from None
    names = custom_worker_names(run_id)
    if (
        session.worker_name not in names
        or session.max_concurrency != 1
        or session.status is not WorkerStatus.ACTIVE
        or type(cohort_size) is not int
        or cohort_size != 2
    ):
        raise DemoStartupError("Custom demos require two scoped one-slot Workers.")
    run = (
        connection.execute(
            select(workflow_runs.c.status, workflow_versions.c.definition)
            .join(
                workflow_versions,
                workflow_versions.c.id == workflow_runs.c.workflow_version_id,
            )
            .where(workflow_runs.c.id == run_id)
        )
        .mappings()
        .one_or_none()
    )
    if run is None or run["status"] != "RUNNING":
        raise DemoStartupError("Custom demo Run must still be RUNNING.")
    try:
        canonical = normalize_custom(
            WorkflowDefinition.model_validate(run["definition"])
        ).model_dump(mode="json")
    except (ValidationError, CustomDefinitionError, TypeError):
        raise DemoStartupError("Stored custom demo definition is invalid.") from None
    if canonical != run["definition"]:
        raise DemoStartupError("Stored custom demo definition is not canonical.")

    # After the demo row lock, this separate READ COMMITTED statement observes
    # any preceding attachment's committed membership. No core row is locked.
    members = (
        connection.execute(
            select(demo_workers.c.worker_session_id, *_session_fields())
            .select_from(
                demo_workers.outerjoin(
                    worker_sessions,
                    worker_sessions.c.id == demo_workers.c.worker_session_id,
                )
            )
            .where(demo_workers.c.run_id == run_id)
        )
        .mappings()
        .all()
    )
    if (
        len(members) >= 2
        or any(not _valid_member(row, names) for row in members)
        or any(row["worker_name"] == session.worker_name for row in members)
    ):
        raise DemoStartupError(
            "Custom demo Worker membership is already used or invalid."
        )
    if registered:
        own = (
            connection.execute(
                select(*_session_fields(), demo_workers.c.run_id)
                .select_from(
                    worker_sessions.outerjoin(
                        demo_workers,
                        demo_workers.c.worker_session_id == worker_sessions.c.id,
                    )
                )
                .where(worker_sessions.c.id == session.id)
            )
            .mappings()
            .one_or_none()
        )
        if (
            own is None
            or not _valid_member(own, names)
            or own["worker_name"] != session.worker_name
            or own["run_id"] is not None
        ):
            raise DemoStartupError("Custom demo registration is not fresh or unused.")


def check_demo_startup(
    engine: Engine, run_id: UUID, session: WorkerSession, *, cohort_size: int
) -> None:
    """Read-only preflight, closed before HTTP registration; attachment rechecks."""
    with engine.connect() as connection:
        if _scenario(connection, run_id, lock=False) == "custom":
            _check_custom(
                connection, run_id, session, cohort_size=cohort_size, registered=False
            )


def add_demo_worker(
    engine: Engine, run_id: UUID, session: WorkerSession, *, cohort_size: int
) -> None:
    """Commit membership before cohort waits; no HTTP or core locks in this scope."""
    with engine.connect().execution_options(isolation_level="READ COMMITTED") as conn:
        with conn.begin():
            if _scenario(conn, run_id, lock=True) == "custom":
                _check_custom(
                    conn, run_id, session, cohort_size=cohort_size, registered=True
                )
            conn.execute(
                demo_workers.insert().values(
                    worker_session_id=session.id, run_id=run_id
                )
            )
