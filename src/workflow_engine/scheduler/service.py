"""Repeat short readiness transactions; never retain uncommitted progress."""

import logging
import math
from threading import Event
from uuid import UUID

from sqlalchemy import Engine
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError, TimeoutError

from workflow_engine.repositories.discovery import RunDiscoveryRepository
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
    def __init__(
        self, engine: Engine, *, poll_seconds: float = 0.5, page_size: int = 50
    ) -> None:
        if (
            isinstance(poll_seconds, bool)
            or not math.isfinite(poll_seconds)
            or poll_seconds <= 0
        ):
            raise ValueError("Scheduler interval must be positive and finite.")
        self._engine, self._poll_seconds = engine, poll_seconds
        if type(page_size) is not int or not 1 <= page_size <= 100:
            raise ValueError("Scheduler page size must be between 1 and 100.")
        self._page_size = page_size

    def reconcile(self, run_id: UUID, *, skip_locked: bool = False) -> int:
        with self._engine.begin() as connection:
            ready = SchedulingRepository(connection).reconcile(
                run_id, skip_locked=skip_locked
            )
        # Publication only after successful commit, including deferred constraints.
        if ready:
            logger.info(
                "Dependent tasks became ready.",
                extra={"event": "tasks_ready", "run_id": str(run_id)},
            )
        return len(ready)

    def scan_page(self, after: UUID | None, stop: Event) -> tuple[int, UUID | None]:
        # Release the discovery transaction before taking any ownership locks.
        with self._engine.begin() as connection:
            page = RunDiscoveryRepository(connection).active(
                after=after, limit=self._page_size
            )
        count = 0
        for run_id in page.run_ids:
            if stop.is_set():
                break
            count += self.reconcile(run_id, skip_locked=True)
        return count, page.next_after

    def run(self, run_id: UUID | None, stop: Event, *, once: bool = False) -> int:
        ready_count = 0
        cursor: UUID | None = None
        while not stop.is_set():
            try:
                if run_id is None:
                    count, cursor = self.scan_page(cursor, stop)
                    ready_count += count
                else:
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
