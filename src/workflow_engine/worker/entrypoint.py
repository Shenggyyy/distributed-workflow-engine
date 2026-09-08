"""Worker process startup, signals and sanitized shutdown reporting."""

import logging
import signal
from threading import Event
from uuid import UUID, uuid4

from workflow_engine.config import Settings
from workflow_engine.domain.worker import WorkerSession
from workflow_engine.logging import configure_logging
from workflow_engine.worker.handlers import builtin_registry
from workflow_engine.worker.loop import WorkerLoop
from workflow_engine.worker.transport import HTTPSender, WorkerTransport


def run_worker(
    settings: Settings, run_id: UUID, *, max_tasks: int | None = None
) -> int:
    configure_logging(settings, component="worker")
    logger = logging.getLogger(__name__)
    stop = Event()
    session = WorkerSession(
        id=uuid4(), worker_name=settings.worker_name, max_concurrency=1
    )

    def request_stop(signum: int, frame: object) -> None:
        stop.set()

    previous = {
        signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        for signum in previous:
            signal.signal(signum, request_stop)
        logger.info(
            "Worker starting.",
            extra={
                "event": "worker_starting",
                "worker_session_id": str(session.id),
                "run_id": str(run_id),
            },
        )
        transport = WorkerTransport(
            session,
            HTTPSender(
                settings.worker_api_url, timeout=settings.worker_http_timeout_seconds
            ),
        )
        completed = WorkerLoop(
            transport,
            builtin_registry(),
            run_id,
            poll_seconds=settings.worker_poll_seconds,
            retry_seconds=settings.worker_retry_seconds,
        ).run(stop, max_tasks=max_tasks)
    except Exception:
        logger.error(
            "Worker stopped after a control or execution failure.",
            extra={"event": "worker_failed", "worker_session_id": str(session.id)},
        )
        return 1
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    logger.info(
        "Worker stopped.",
        extra={"event": "worker_stopped", "worker_session_id": str(session.id)},
    )
    print(f"Worker stopped after {completed} confirmed completions.")
    return 0
