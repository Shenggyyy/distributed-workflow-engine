"""Bounded demo-only startup coordination, never task ownership authority."""

import math
import time
from threading import Event
from uuid import UUID

from sqlalchemy import Engine, func, select

from workflow_engine.domain.worker import WorkerStatus
from workflow_engine.schema import demo_workers, worker_sessions
from workflow_engine.worker.transport import WorkerTransport


class DemoStartupError(RuntimeError):
    """The requested demo cohort could not be observed safely before execution."""


def cohort_ready(
    engine: Engine, run_id: UUID, session_id: UUID, *, cohort_size: int
) -> bool:
    """Observe scoped, fresh names in one statement; do not lock or assign work."""
    with engine.connect() as connection:
        rows = connection.execute(
            select(worker_sessions.c.id, worker_sessions.c.worker_name)
            .join(
                demo_workers,
                demo_workers.c.worker_session_id == worker_sessions.c.id,
            )
            .where(
                demo_workers.c.run_id == run_id,
                worker_sessions.c.status == "ACTIVE",
                # A stable DB wall-clock value for every row in this statement.
                worker_sessions.c.last_heartbeat_at <= func.statement_timestamp(),
                worker_sessions.c.heartbeat_expires_at > func.statement_timestamp(),
            )
        ).all()
    if session_id not in {row.id for row in rows}:
        raise DemoStartupError("Demo cohort lost its own fresh registered session.")
    return len({row.worker_name for row in rows}) >= cohort_size


def wait_for_cohort(
    engine: Engine,
    transport: WorkerTransport,
    run_id: UUID,
    stop: Event,
    *,
    cohort_size: int = 1,
    timeout_seconds: float = 60,
) -> None:
    """Heartbeat while waiting for names; no polling transaction spans a wait.

    Readiness is a past observation, not a guarantee peers remain alive or claim.
    The monotonic wait budget is checked between bounded HTTP/database operations;
    in-flight I/O can delay stop/timeout reporting. It never authorizes execution
    after the budget expires, and never revives an expired session identity.
    """
    if type(cohort_size) is not int or cohort_size not in (1, 2):
        raise ValueError("Demo cohort size must be 1 or 2.")
    if (
        isinstance(timeout_seconds, bool)
        or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= 60
    ):
        raise ValueError("Demo cohort timeout must be in (0, 60] seconds.")
    deadline = time.monotonic() + timeout_seconds

    def remaining() -> float:
        if stop.is_set():
            raise DemoStartupError("Demo cohort startup was stopped.")
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise DemoStartupError("Demo cohort startup timed out.")
        return seconds

    def heartbeat() -> None:
        remaining()
        if transport.heartbeat().session.status is not WorkerStatus.ACTIVE:
            raise DemoStartupError("Demo cohort heartbeat is not ACTIVE.")
        remaining()

    while True:
        try:
            heartbeat()
            ready = cohort_ready(
                engine, run_id, transport.session.id, cohort_size=cohort_size
            )
            remaining()
            if ready:
                # The query can have consumed session lifetime. Registration in
                # WorkerLoop only replays identity, so confirm heartbeat again.
                heartbeat()
                return
        except DemoStartupError:
            raise
        except Exception:
            raise DemoStartupError("Demo cohort heartbeat or query failed.") from None
        stop.wait(min(0.5, remaining()))
