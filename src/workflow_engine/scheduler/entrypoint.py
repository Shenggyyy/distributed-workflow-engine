"""Scheduler CLI process with explicit database configuration and signal cleanup."""

import logging
import signal
from threading import Event
from uuid import UUID

from workflow_engine.config import Settings
from workflow_engine.database import database_engine
from workflow_engine.logging import configure_logging
from workflow_engine.scheduler.service import SchedulerService


def run_scheduler(
    settings: Settings, run_id: UUID | None, *, once: bool = False
) -> int:
    configure_logging(settings, component="scheduler")
    logger = logging.getLogger(__name__)
    stop = Event()

    def request_stop(signum: int, frame: object) -> None:
        stop.set()

    previous = {
        signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        for signum in previous:
            signal.signal(signum, request_stop)
        with database_engine(settings) as engine:
            count = SchedulerService(
                engine,
                poll_seconds=settings.scheduler_poll_seconds,
                page_size=settings.scheduler_page_size,
            ).run(run_id, stop, once=once)
    except Exception:
        logger.error(
            "Scheduler stopped after a storage or configuration failure.",
            extra={"event": "scheduler_failed", "run_id": str(run_id)},
        )
        return 1
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    print(f"Scheduler stopped after {count} committed readiness transitions.")
    return 0
