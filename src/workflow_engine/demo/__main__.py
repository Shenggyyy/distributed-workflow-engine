"""Explicit opt-in process roles for the isolated local Compose demonstration."""

import argparse
import logging
import signal
from threading import Event
from uuid import UUID, uuid4

import uvicorn
from sqlalchemy import select

from workflow_engine.config import Settings, load_settings
from workflow_engine.database import database_engine
from workflow_engine.demo.api import create_demo_app
from workflow_engine.demo.handlers import registry
from workflow_engine.demo.startup import DemoStartupError, wait_for_cohort
from workflow_engine.domain.worker import WorkerSession
from workflow_engine.logging import configure_logging
from workflow_engine.schema import demo_runs, demo_workers
from workflow_engine.worker.loop import WorkerLoop
from workflow_engine.worker.transport import HTTPSender, WorkerTransport


def run_demo_worker(settings: Settings, run_id: UUID, *, cohort_size: int = 1) -> None:
    if type(cohort_size) is not int or cohort_size not in (1, 2):
        raise ValueError("Demo cohort size must be 1 or 2.")
    configure_logging(settings, component="demo-worker")
    stop = Event()

    def request_stop(signum: int, frame: object) -> None:
        stop.set()

    previous = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        for signum in previous:
            signal.signal(signum, request_stop)
        session = WorkerSession(
            id=uuid4(),
            worker_name=settings.worker_name,
            max_concurrency=settings.worker_concurrency,
        )
        transport = WorkerTransport(
            session,
            HTTPSender(
                settings.worker_api_url, timeout=settings.worker_http_timeout_seconds
            ),
        )
        with database_engine(settings) as engine:
            with engine.connect() as connection:
                if (
                    connection.scalar(
                        select(demo_runs.c.run_id).where(demo_runs.c.run_id == run_id)
                    )
                    is None
                ):
                    raise ValueError("Only registered demo Runs may be executed.")
            transport.register()
            with engine.begin() as connection:
                connection.execute(
                    demo_workers.insert().values(
                        worker_session_id=session.id, run_id=run_id
                    )
                )
            if cohort_size > 1:
                wait_for_cohort(
                    engine, transport, run_id, stop, cohort_size=cohort_size
                )
        logging.getLogger(__name__).info(
            "Demo Worker registered.",
            extra={
                "event": "demo_worker_registered",
                "worker_session_id": str(session.id),
                "run_id": str(run_id),
            },
        )
        # Re-registration by the unchanged loop replays the same session identity.
        WorkerLoop(
            transport,
            registry(settings),
            run_id,
            poll_seconds=settings.worker_poll_seconds,
            retry_seconds=settings.worker_retry_seconds,
        ).run(stop)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="Isolated demonstration runtime")
    commands = parser.add_subparsers(dest="role", required=True)
    commands.add_parser("api")
    worker = commands.add_parser("worker")
    worker.add_argument("--run-id", type=UUID, required=True)
    worker.add_argument("--cohort-size", type=int, choices=(1, 2), default=1)
    args = parser.parse_args()
    try:
        settings = load_settings()
        if args.role == "worker":
            run_demo_worker(settings, args.run_id, cohort_size=args.cohort_size)
        else:
            uvicorn.run(
                create_demo_app(settings),
                host=str(settings.api_host),
                port=settings.api_port,
                access_log=False,
                proxy_headers=False,
            )
    except DemoStartupError as error:
        logging.getLogger(__name__).error(
            str(error), extra={"event": "demo_cohort_startup_failed"}
        )
        raise SystemExit(1) from None
    except Exception:
        # Never print settings, database errors, request bodies or secrets.
        logging.getLogger(__name__).error(
            "Demo runtime failed.", extra={"event": "demo_runtime_failed"}
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
