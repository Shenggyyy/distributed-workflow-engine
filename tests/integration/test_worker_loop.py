"""Spawned handler + HTTP protocol + PostgreSQL with lost committed responses."""

import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from multiprocessing.synchronize import Barrier
from pathlib import Path
from threading import Event, Timer
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select

from tests.integration.test_lease_http import client as client
from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.domain.completion import CompletionOutcome, CompletionResult
from workflow_engine.domain.worker import WorkerSession
from workflow_engine.scheduler.service import SchedulerService
from workflow_engine.schema import (
    attempt_completions,
    task_attempts,
    task_retry_schedules,
    task_runs,
)
from workflow_engine.worker.handlers import (
    HandlerContext,
    HandlerRegistration,
    HandlerRegistry,
    builtin_registry,
)
from workflow_engine.worker.loop import WorkerLoop
from workflow_engine.worker.transport import TransportUnavailable, WorkerTransport

pytestmark = pytest.mark.integration


def test_worker_retries_with_scheduler_and_lost_completion(
    client: TestClient,
    http_engine: Engine,
) -> None:
    version = client.post(
        "/workflows",
        json={
            "schema_version": 2,
            "name": "retry_loop",
            "tasks": [
                {
                    "task_id": "A",
                    "task_type": "demo.fail",
                    "execution": {
                        "max_attempts": 3,
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
    lost = False

    def send(method: str, path: str, body: bytes) -> tuple[int, bytes]:
        nonlocal lost
        response = client.request(
            method, path, content=body, headers={"Content-Type": "application/json"}
        )
        if path.endswith("/complete") and not lost:
            lost = True
            assert response.status_code == 200
            raise TransportUnavailable("Completion response lost.")
        return response.status_code, response.content

    worker = WorkerLoop(
        WorkerTransport(
            WorkerSession(id=uuid4(), worker_name="retry", max_concurrency=1), send
        ),
        builtin_registry(),
        UUID(run.json()["run_id"]),
        retry_seconds=0.01,
        tick_seconds=0.01,
    )
    stop = Event()
    timer = Timer(20, stop.set)
    timer.start()
    with ThreadPoolExecutor(max_workers=1) as pool:
        scheduler = pool.submit(
            SchedulerService(http_engine, poll_seconds=0.01).run,
            UUID(run.json()["run_id"]),
            stop,
        )
        try:
            assert worker.run(stop, max_tasks=3) == 3
            assert not stop.is_set()
        finally:
            stop.set()
            timer.cancel()
            timer.join(timeout=1)
            scheduler.result(timeout=5)
    with http_engine.connect() as connection:
        assert connection.scalar(select(task_runs.c.status)) == "FAILED"
        numbers = connection.scalars(
            select(task_attempts.c.attempt_number).order_by(
                task_attempts.c.attempt_number
            )
        ).all()
        assert numbers == [1, 2, 3]
        assert (
            connection.scalar(select(func.count()).select_from(attempt_completions))
            == 3
        )
        assert (
            connection.scalar(select(func.count()).select_from(task_retry_schedules))
            == 2
        )


@dataclass(frozen=True)
class BarrierHandler:
    barrier: Barrier

    def __call__(self, context: HandlerContext) -> CompletionResult:
        self.barrier.wait(timeout=8)
        return CompletionResult(outcome=CompletionOutcome.SUCCEEDED)


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("automatic", [False, True])
def test_real_handler_with_lost_claim_and_completion(
    client: TestClient, http_engine: Engine, failed: bool, automatic: bool
) -> None:
    version = client.post(
        "/workflows",
        json={
            "name": "loop_" + uuid4().hex,
            "tasks": [
                {"task_id": "A", "task_type": "demo.fail" if failed else "demo.echo"}
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
    lost: set[str] = set()

    def send(method: str, path: str, body: bytes) -> tuple[int, bytes]:
        response = client.request(
            method, path, content=body, headers={"Content-Type": "application/json"}
        )
        operation = path.rsplit("/", 1)[1]
        if operation in ("claims", "complete") and operation not in lost:
            lost.add(operation)
            assert response.status_code == 200
            raise TransportUnavailable("Committed response lost.")
        return response.status_code, response.content

    transport = WorkerTransport(
        WorkerSession(id=uuid4(), worker_name="loop", max_concurrency=1), send
    )
    worker = WorkerLoop(
        transport,
        builtin_registry(),
        None if automatic else UUID(run.json()["run_id"]),
        retry_seconds=0.01,
        tick_seconds=0.01,
    )
    stop = Event()
    timer = Timer(15, stop.set)
    timer.start()
    try:
        assert worker.run(stop, max_tasks=1) == 1
        assert not stop.is_set()
    finally:
        timer.cancel()
        timer.join(timeout=1)
    assert lost == {"claims", "complete"}
    with http_engine.connect() as connection:
        for table in (task_attempts, attempt_completions):
            assert connection.scalar(select(func.count()).select_from(table)) == 1
        for table in (task_runs, task_attempts):
            assert connection.scalar(select(table.c.status)) == (
                "FAILED" if failed else "SUCCEEDED"
            )


def test_automatic_worker_advances_across_runs(
    client: TestClient, http_engine: Engine
) -> None:
    version = client.post(
        "/workflows",
        json={
            "name": "many_runs",
            "tasks": [{"task_id": "A", "task_type": "demo.echo"}],
        },
    )
    assert version.status_code == 201
    for _ in range(3):
        assert (
            client.post(
                "/runs",
                json={"workflow_version_id": version.json()["id"]},
                headers={"Idempotency-Key": uuid4().hex},
            ).status_code
            == 201
        )

    def send(method: str, path: str, body: bytes) -> tuple[int, bytes]:
        response = client.request(
            method, path, content=body, headers={"Content-Type": "application/json"}
        )
        return response.status_code, response.content

    transport = WorkerTransport(
        WorkerSession(id=uuid4(), worker_name="discovery", max_concurrency=1), send
    )
    stop = Event()
    timer = Timer(15, stop.set)
    timer.start()
    try:
        assert (
            WorkerLoop(transport, builtin_registry(), tick_seconds=0.005).run(
                stop, max_tasks=3
            )
            == 3
        )
        assert not stop.is_set()
    finally:
        timer.cancel()
        timer.join(timeout=1)
    with http_engine.connect() as connection:
        assert list(connection.scalars(select(task_runs.c.status))) == ["SUCCEEDED"] * 3
        assert connection.scalar(select(func.count()).select_from(task_attempts)) == 3


def test_two_real_handler_processes_overlap_in_one_worker(
    client: TestClient, http_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Spawn must import this test-defined trusted handler outside pytest's loader.
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2]))
    version = client.post(
        "/workflows",
        json={
            "name": "parallel",
            "tasks": [
                {"task_id": key, "task_type": "test.barrier"} for key in ("A", "B")
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

    def send(method: str, path: str, body: bytes) -> tuple[int, bytes]:
        response = client.request(
            method, path, content=body, headers={"Content-Type": "application/json"}
        )
        return response.status_code, response.content

    transport = WorkerTransport(
        WorkerSession(id=uuid4(), worker_name="parallel", max_concurrency=2), send
    )
    barrier = multiprocessing.get_context("spawn").Barrier(2)
    registry = HandlerRegistry(
        (HandlerRegistration("test.barrier", BarrierHandler(barrier)),)
    )
    stop = Event()
    timer = Timer(15, stop.set)
    timer.start()
    try:
        assert (
            WorkerLoop(
                transport, registry, UUID(run.json()["run_id"]), tick_seconds=0.005
            ).run(stop, max_tasks=2)
            == 2
        )
        assert not stop.is_set()
    finally:
        timer.cancel()
        timer.join(timeout=1)
    with http_engine.connect() as connection:
        assert list(connection.scalars(select(task_runs.c.status))) == ["SUCCEEDED"] * 2
        assert connection.scalar(select(func.count()).select_from(task_attempts)) == 2
