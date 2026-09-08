"""A real Worker process dies after claim; a new Scheduler and Worker recover."""

import multiprocessing
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Event, Thread, Timer
from uuid import UUID, uuid4

import pytest
import uvicorn
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select

from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.domain.completion import (
    AttemptCompletion,
    CompletionOutcome,
    CompletionResult,
)
from workflow_engine.domain.worker import WorkerSession
from workflow_engine.scheduler.service import SchedulerService
from workflow_engine.schema import (
    attempt_completions,
    task_attempts,
    task_retry_schedules,
    task_runs,
)
from workflow_engine.worker.handlers import builtin_registry
from workflow_engine.worker.loop import WorkerLoop
from workflow_engine.worker.transport import (
    ClaimedTask,
    ClaimPoll,
    HTTPSender,
    WorkerAPIError,
    WorkerTransport,
)

pytestmark = pytest.mark.integration


def crash_after_claim(base: str, run_id: UUID, result: Connection) -> None:
    transport = WorkerTransport(
        WorkerSession(id=uuid4(), worker_name="crashing", max_concurrency=1),
        HTTPSender(base),
    )
    transport.register()
    claimed = transport.claim(ClaimPoll(run_id=run_id, request_id=uuid4())).claim
    assert claimed is not None
    result.send(claimed.model_dump_json())
    os._exit(17)


@pytest.mark.parametrize("fault", ["lease", "timeout"])
def test_process_crash_recovers_and_rejects_stale_completion(
    http_engine: Engine, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2]))
    settings = Settings(
        environment="test", attempt_lease_seconds=2 if fault == "lease" else 5
    )
    app = create_app(settings, engine=http_engine)
    with TestClient(app) as client:
        version = client.post(
            "/workflows",
            json={
                "name": "crash",
                "schema_version": 2,
                "tasks": [
                    {
                        "task_id": "A",
                        "task_type": "demo.echo",
                        "execution": {
                            "max_attempts": 2,
                            "timeout_seconds": 10 if fault == "lease" else 2,
                            "initial_backoff_ms": 20,
                            "max_backoff_ms": 100,
                        },
                    }
                ],
            },
        )
        assert version.status_code == 201
        run = client.post(
            "/runs",
            json={"workflow_version_id": version.json()["id"]},
            headers={"Idempotency-Key": uuid4().hex},
        )
        assert run.status_code == 201
        run_id = UUID(run.json()["run_id"])
    spawn = multiprocessing.get_context("spawn")
    receiver, sender = spawn.Pipe(duplex=False)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        base = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(app, log_config=None, access_log=False))
        thread = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        crashed = spawn.Process(target=crash_after_claim, args=(base, run_id, sender))
        try:
            deadline = time.monotonic() + 5
            while (
                not server.started and thread.is_alive() and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            assert server.started
            crashed.start()
            sender.close()
            assert receiver.poll(10)
            abandoned = ClaimedTask.model_validate_json(receiver.recv())
            crashed.join(timeout=5)
            assert crashed.exitcode == 17
            replacement = WorkerTransport(
                WorkerSession(id=uuid4(), worker_name="replacement", max_concurrency=1),
                HTTPSender(base),
            )
            stop = Event()
            timer = Timer(15, stop.set)
            timer.start()
            with ThreadPoolExecutor(max_workers=1) as pool:
                scheduler = pool.submit(
                    SchedulerService(http_engine, poll_seconds=0.02).run, run_id, stop
                )
                try:
                    assert (
                        WorkerLoop(
                            replacement,
                            builtin_registry(),
                            run_id,
                            poll_seconds=0.02,
                            tick_seconds=0.01,
                        ).run(stop, max_tasks=1)
                        == 1
                    )
                    assert not stop.is_set()
                finally:
                    stop.set()
                    timer.cancel()
                    timer.join(timeout=1)
                    scheduler.result(timeout=5)
            old = WorkerTransport(
                WorkerSession(
                    id=abandoned.lease.worker_session_id,
                    worker_name="crashing",
                    max_concurrency=1,
                ),
                HTTPSender(base),
            )
            with pytest.raises(WorkerAPIError) as error:
                old.complete(
                    AttemptCompletion(
                        attempt_id=abandoned.attempt.id,
                        worker_session_id=abandoned.lease.worker_session_id,
                        lease_token=abandoned.lease.lease_token,
                        result=CompletionResult(outcome=CompletionOutcome.SUCCEEDED),
                    )
                )
            assert error.value.code == "completion_inactive"
        finally:
            if crashed.pid is not None:
                if crashed.is_alive():
                    crashed.terminate()
                crashed.join(timeout=5)
                crashed.close()
            sender.close()
            receiver.close()
            server.should_exit = True
            thread.join(timeout=5)
            assert not thread.is_alive()
    with http_engine.connect() as connection:
        assert connection.scalar(select(task_runs.c.status)) == "SUCCEEDED"
        statuses = connection.scalars(
            select(task_attempts.c.status).order_by(task_attempts.c.attempt_number)
        ).all()
        assert statuses == ["LOST" if fault == "lease" else "TIMED_OUT", "SUCCEEDED"]
        for table in (task_retry_schedules, attempt_completions):
            assert connection.scalar(select(func.count()).select_from(table)) == 1
