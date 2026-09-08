"""Repeat short readiness transactions; never retain uncommitted progress."""

import logging
import math
from threading import Event
from uuid import UUID

from sqlalchemy import Engine
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError, TimeoutError

from workflow_engine.repositories.discovery import RunDiscoveryRepository
from workflow_engine.repositories.recovery import RecoveryRepository
from workflow_engine.repositories.recovery_discovery import RecoveryDiscoveryRepository
from workflow_engine.repositories.scheduling import SchedulingRepository
from workflow_engine.repositories.workers import WorkerRepository

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

    def recover_run(self, run_id: UUID, stop: Event) -> int:
        with self._engine.begin() as connection:
            attempts = RecoveryDiscoveryRepository(connection).attempts(run_id)
        count = 0
        for attempt_id in attempts:
            if stop.is_set():
                break
            with self._engine.begin() as connection:
                settled = RecoveryRepository(connection).recover(
                    attempt_id, skip_locked=True
                )
            if settled is not None:
                count += 1
                logger.info(
                    "Expired Attempt recovered.",
                    extra={
                        "event": "attempt_recovered",
                        "attempt_id": str(attempt_id),
                        "run_id": str(run_id),
                        "outcome": settled.status.value,
                    },
                )
        return count

    def expire_workers(self, stop: Event) -> None:
        with self._engine.begin() as connection:
            workers = RecoveryDiscoveryRepository(connection).workers()
        for worker_id in workers:
            if stop.is_set():
                break
            # Separate transaction: never take Run locks after a Worker lock.
            with self._engine.begin() as connection:
                WorkerRepository(connection).expire(worker_id)

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
        self.expire_workers(stop)
        # Release the discovery transaction before taking any ownership locks.
        with self._engine.begin() as connection:
            page = RunDiscoveryRepository(connection).active(
                after=after, limit=self._page_size
            )
        count = 0
        for run_id in page.run_ids:
            if stop.is_set():
                break
            self.recover_run(run_id, stop)
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
                    self.recover_run(run_id, stop)
                    if stop.is_set():
                        break
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
