"""Append-only observations, deliberately outside ownership transactions."""

from uuid import UUID, uuid4

from sqlalchemy import Engine, select

from workflow_engine.schema import (
    attempt_leases,
    demo_invocations,
    demo_runs,
    demo_samples,
    demo_workers,
    task_attempts,
    task_runs,
    workflow_runs,
)
from workflow_engine.worker.handlers import HandlerContext


class ObservationError(ValueError):
    """Unknown demo identity or invalid observation sequence."""


def start(engine: Engine, context: HandlerContext, domain: str, stamp: int) -> UUID:
    """Bind one invocation to a demo Run and its registered owning session."""
    if not domain or len(domain) > 160 or stamp < 0:
        raise ObservationError("Invalid clock sample.")
    invocation = uuid4()
    with engine.begin() as connection:
        scoped = connection.scalar(
            select(task_attempts.c.id)
            .join(task_runs, task_runs.c.id == task_attempts.c.task_id)
            .join(workflow_runs, workflow_runs.c.id == task_runs.c.run_id)
            .join(demo_runs, demo_runs.c.run_id == workflow_runs.c.id)
            .join(attempt_leases, attempt_leases.c.attempt_id == task_attempts.c.id)
            .join(
                demo_workers,
                (demo_workers.c.worker_session_id == attempt_leases.c.worker_session_id)
                & (demo_workers.c.run_id == demo_runs.c.run_id),
            )
            .where(
                task_attempts.c.id == context.attempt_id,
                task_attempts.c.attempt_number == context.attempt_number,
                task_runs.c.id == context.task_id,
                task_runs.c.task_key == context.task_key,
                workflow_runs.c.id == context.run_id,
                workflow_runs.c.workflow_version_id == context.workflow_version_id,
            )
        )
        if scoped is None:
            raise ObservationError("Invocation is not a registered demo execution.")
        connection.execute(
            demo_invocations.insert().values(
                id=invocation, attempt_id=context.attempt_id, clock_domain=domain
            )
        )
        connection.execute(
            demo_samples.insert().values(
                invocation_id=invocation, sequence=0, phase="START", monotonic_ns=stamp
            )
        )
    return invocation


def append(engine: Engine, invocation: UUID, phase: str, stamp: int) -> None:
    """Serialize only this evidence stream; never lock or mutate a core row."""
    if phase not in ("PULSE", "FINISH"):
        raise ObservationError("Invalid observation phase.")
    with engine.begin() as connection:
        identity = connection.scalar(
            select(demo_invocations.c.id)
            .where(demo_invocations.c.id == invocation)
            .with_for_update()
        )
        if identity is None:
            raise ObservationError("Unknown invocation.")
        previous = (
            connection.execute(
                select(demo_samples)
                .where(demo_samples.c.invocation_id == invocation)
                .order_by(demo_samples.c.sequence.desc())
                .limit(1)
            )
            .mappings()
            .one()
        )
        if (
            previous["phase"] == "FINISH"
            or previous["sequence"] >= 240
            or stamp < previous["monotonic_ns"]
        ):
            raise ObservationError("Invalid observation sequence.")
        connection.execute(
            demo_samples.insert().values(
                invocation_id=invocation,
                sequence=previous["sequence"] + 1,
                phase=phase,
                monotonic_ns=stamp,
            )
        )
