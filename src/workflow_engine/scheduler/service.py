"""Repeat short readiness transactions; never retain uncommitted progress."""

import logging
import math
from threading import Event
from uuid import UUID

from sqlalchemy import Engine
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError, TimeoutError

from workflow_engine.repositories.scheduling import SchedulingRepository

logger = logging.getLogger(__name__)


def transient_database_error(error: DBAPIError) -> bool:
    state = getattr(error.orig, "sqlstate", None)
    return (
        error.connection_invalidated
        or (state is None and isinstance(error, (OperationalError, InterfaceError)))
        or str(state).startswith("08")
        or state
        in {
            "40001",
            "40P01",
            "55P03",
            "57014",
            "57P01",
            "57P02",
            "57P03",
        }
    )


class SchedulerService:
    def __init__(self, engine: Engine, *, poll_seconds: float = 0.5) -> None:
        if (
            isinstance(poll_seconds, bool)
            or not math.isfinite(poll_seconds)
            or poll_seconds <= 0
        ):
            raise ValueError("Scheduler interval must be positive and finite.")
        self._engine, self._poll_seconds = engine, poll_seconds

    def reconcile(self, run_id: UUID) -> int:
        with self._engine.begin() as connection:
            ready = SchedulingRepository(connection).reconcile(run_id)
        # Publication only after successful commit, including deferred constraints.
        if ready:
            logger.info(
                "Dependent tasks became ready.",
                extra={"event": "tasks_ready", "run_id": str(run_id)},
            )
        return len(ready)

    def run(self, run_id: UUID, stop: Event, *, once: bool = False) -> int:
        ready_count = 0
        while not stop.is_set():
            try:
                ready_count += self.reconcile(run_id)
            except (DBAPIError, TimeoutError) as error:
                if once or (
                    isinstance(error, DBAPIError)
                    and not transient_database_error(error)
                ):
                    raise
                logger.warning(
                    "Scheduler database operation will retry.",
                    extra={"event": "scheduler_retry", "run_id": str(run_id)},
                )
            if once:
                break
            stop.wait(self._poll_seconds)
        return ready_count
