"""Transactional cascading failure, independent branches and retained replay."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, event, func, select

from tests.integration.test_lease_http import client as client
from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.repositories.discovery import RunDiscoveryRepository
from workflow_engine.repositories.scheduling import SchedulingRepository
from workflow_engine.schema import task_attempts, task_runs, workflow_runs

pytestmark = pytest.mark.integration


def seed(client: TestClient) -> tuple[UUID, str, str, dict[str, object]]:
    version = client.post(
        "/workflows",
        json={
            "name": "settlement",
            "tasks": [
                {"task_id": "A", "task_type": "demo.fail"},
                {"task_id": "B", "task_type": "demo.echo"},
                {"task_id": "C", "task_type": "demo.echo", "depends_on": ["A"]},
                {"task_id": "D", "task_type": "demo.echo", "depends_on": ["C"]},
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
    session = str(uuid4())
    assert (
        client.put(
            f"/worker-sessions/{session}",
            json={"worker_name": "settlement", "max_concurrency": 1},
        ).status_code
        == 200
    )
    claim = client.post(
        f"/worker-sessions/{session}/claims",
        json={"run_id": str(run_id), "request_id": str(uuid4())},
    )
    assert claim.status_code == 200
    lease = claim.json()["claim"]["lease"]
    path = f"/worker-sessions/{session}/attempts/{lease['attempt_id']}/complete"
    body: dict[str, object] = {
        "lease_token": lease["lease_token"],
        "result": {"outcome": "FAILED", "error_code": "handler_failed"},
    }
    assert client.post(path, json=body).status_code == 200
    return run_id, session, path, body


def finish_independent(client: TestClient, run_id: UUID, session: str) -> None:
    claim = client.post(
        f"/worker-sessions/{session}/claims",
        json={"run_id": str(run_id), "request_id": str(uuid4())},
    )
    assert claim.status_code == 200
    assert claim.json()["claim"]["task"]["task_key"] == "B"
    lease = claim.json()["claim"]["lease"]
    assert (
        client.post(
            f"/worker-sessions/{session}/attempts/{lease['attempt_id']}/complete",
            json={
                "lease_token": lease["lease_token"],
                "result": {"outcome": "SUCCEEDED"},
            },
        ).status_code
        == 200
    )


def test_cascade_waits_for_independent_branch_and_retains_receipt(
    client: TestClient, http_engine: Engine
) -> None:
    run_id, session, path, body = seed(client)
    with http_engine.begin() as connection:
        assert SchedulingRepository(connection).reconcile(run_id) == ()
    assert client.get(f"/runs/{run_id}").json()["status"] == "RUNNING"
    assert {
        t["task_key"]: t["status"]
        for t in client.get(f"/runs/{run_id}/tasks").json()["tasks"]
    } == {"A": "FAILED", "B": "READY", "C": "SKIPPED", "D": "SKIPPED"}
    finish_independent(client, run_id, session)
    with http_engine.begin() as connection:
        assert SchedulingRepository(connection).reconcile(run_id) == ()
    assert client.get(f"/runs/{run_id}").json()["status"] == "FAILED"
    assert client.post(path, json=body).status_code == 200
    with http_engine.begin() as connection:
        assert RunDiscoveryRepository(connection).active().run_ids == ()
        assert connection.scalar(select(func.count()).select_from(task_attempts)) == 2


@pytest.mark.parametrize("fault", ["rollback", "commit"])
def test_failed_settlement_transaction_preserves_pending(
    client: TestClient, http_engine: Engine, fault: str
) -> None:
    run_id, session, _, _ = seed(client)
    finish_independent(client, run_id, session)

    def fail_commit(connection: object) -> None:
        raise RuntimeError("injected commit failure")

    if fault == "commit":
        with pytest.raises(RuntimeError, match="injected"):
            with http_engine.begin() as connection:
                event.listen(connection, "commit", fail_commit)
                SchedulingRepository(connection).reconcile(run_id)
    else:
        with http_engine.begin() as connection:
            SchedulingRepository(connection).reconcile(run_id)
            connection.rollback()
    with http_engine.begin() as connection:
        assert connection.scalar(select(workflow_runs.c.status)) == "RUNNING"
        assert (
            connection.scalar(
                select(func.count())
                .select_from(task_runs)
                .where(task_runs.c.status == "PENDING")
            )
            == 2
        )
        SchedulingRepository(connection).reconcile(run_id)
    assert client.get(f"/runs/{run_id}").json()["status"] == "FAILED"


def test_concurrent_settlement_is_repeatable(
    client: TestClient, http_engine: Engine
) -> None:
    run_id, session, _, _ = seed(client)
    finish_independent(client, run_id, session)
    barrier = Barrier(2, timeout=5)

    def reconcile() -> None:
        with http_engine.begin() as connection:
            barrier.wait()
            assert SchedulingRepository(connection).reconcile(run_id) == ()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(reconcile) for _ in range(2)]
        for future in futures:
            future.result(timeout=10)
    assert client.get(f"/runs/{run_id}").json()["status"] == "FAILED"
    with http_engine.begin() as connection:
        assert (
            connection.scalar(
                select(func.count())
                .select_from(task_runs)
                .where(task_runs.c.status == "SKIPPED")
            )
            == 2
        )
