"""Two Worker processes, four simultaneous handlers and two Scheduler processes."""

import multiprocessing
import socket
import time
from multiprocessing.connection import Connection
from multiprocessing.synchronize import Event as ProcessEvent
from pathlib import Path
from threading import Event, Thread
from typing import cast
from uuid import uuid4

import pytest
import uvicorn
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select

from tests.integration.test_lease_http import client as client
from tests.integration.test_lease_http import http_engine as http_engine
from tests.integration.test_worker_loop import BarrierHandler
from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.database import database_engine
from workflow_engine.domain.worker import WorkerSession
from workflow_engine.scheduler.service import SchedulerService
from workflow_engine.schema import (
    attempt_completions,
    attempt_leases,
    task_attempts,
    task_runs,
)
from workflow_engine.worker.handlers import HandlerRegistration, HandlerRegistry, echo
from workflow_engine.worker.loop import WorkerLoop
from workflow_engine.worker.transport import HTTPSender, WorkerTransport

pytestmark = pytest.mark.integration


def worker_process(
    base: str,
    entries: tuple[HandlerRegistration, ...],
    stop: ProcessEvent,
    result: Connection,
) -> None:
    try:
        transport = WorkerTransport(
            WorkerSession(id=uuid4(), worker_name="distributed", max_concurrency=2),
            HTTPSender(base),
        )
        count = WorkerLoop(
            transport, HandlerRegistry(entries), tick_seconds=0.01, poll_seconds=0.01
        ).run(cast(Event, stop), max_tasks=3)
        result.send(count)
    finally:
        result.close()


def scheduler_process(settings: Settings, schema: str, stop: ProcessEvent) -> None:
    with database_engine(settings) as engine:
        SchedulerService(
            engine.execution_options(schema_translate_map={None: schema}),
            poll_seconds=0.01,
            page_size=1,
        ).run(None, cast(Event, stop))


def test_multi_process_execution_has_distinct_owners_and_exact_attempts(
    client: TestClient,
    http_engine: Engine,
    database_settings: Settings,
    migration_schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2]))
    version = client.post(
        "/workflows",
        json={
            "name": "distributed",
            "tasks": [
                *(
                    {"task_id": key, "task_type": "test.barrier"}
                    for key in ("A", "B", "C", "D")
                ),
                {
                    "task_id": "E",
                    "task_type": "demo.echo",
                    "depends_on": ["A", "B", "C", "D"],
                },
                {"task_id": "F", "task_type": "demo.echo", "depends_on": ["E"]},
            ],
        },
    )
    assert version.status_code == 201
    assert (
        client.post(
            "/runs",
            json={"workflow_version_id": version.json()["id"]},
            headers={"Idempotency-Key": uuid4().hex},
        ).status_code
        == 201
    )
    spawn = multiprocessing.get_context("spawn")
    stop = spawn.Event()
    registry = HandlerRegistry(
        (
            HandlerRegistration("test.barrier", BarrierHandler(spawn.Barrier(4))),
            HandlerRegistration("demo.echo", echo),
        )
    )
    processes = []
    receivers = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        base = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(Settings(environment="test"), engine=http_engine),
                log_config=None,
                access_log=False,
            )
        )
        thread = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 5
            while (
                not server.started and thread.is_alive() and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            assert server.started
            for _ in range(2):
                scheduler = spawn.Process(
                    target=scheduler_process,
                    args=(database_settings, migration_schema, stop),
                )
                scheduler.start()
                processes.append(scheduler)
                receiver, sender = spawn.Pipe(duplex=False)
                receivers.append(receiver)
                worker = spawn.Process(
                    target=worker_process,
                    args=(base, registry.registrations, stop, sender),
                )
                try:
                    worker.start()
                finally:
                    sender.close()
                processes.append(worker)
            for receiver in receivers:
                assert receiver.poll(25)
                assert receiver.recv() == 3
            for worker in processes[1::2]:
                worker.join(timeout=5)
                assert worker.exitcode == 0
            assert all(process.is_alive() for process in processes[::2])
        finally:
            stop.set()
            for process in processes:
                process.join(timeout=5)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                assert not process.is_alive()
                process.close()
            for receiver in receivers:
                receiver.close()
            server.should_exit = True
            thread.join(timeout=5)
            assert not thread.is_alive()
    with http_engine.connect() as connection:
        assert list(connection.scalars(select(task_runs.c.status))) == ["SUCCEEDED"] * 6
        for table in (task_attempts, attempt_completions, attempt_leases):
            assert connection.scalar(select(func.count()).select_from(table)) == 6
        owners = connection.execute(
            select(attempt_leases.c.worker_session_id, func.count()).group_by(
                attempt_leases.c.worker_session_id
            )
        ).all()
        assert len(owners) == 2 and sorted(row[1] for row in owners) == [3, 3]
