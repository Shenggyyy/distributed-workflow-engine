"""Spawned handler + HTTP protocol + PostgreSQL with lost committed responses."""

from threading import Event, Timer
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select

from tests.integration.test_lease_http import client as client
from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.domain.worker import WorkerSession
from workflow_engine.schema import attempt_completions, task_attempts, task_runs
from workflow_engine.worker.handlers import builtin_registry
from workflow_engine.worker.loop import WorkerLoop
from workflow_engine.worker.transport import TransportUnavailable, WorkerTransport

pytestmark = pytest.mark.integration


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
