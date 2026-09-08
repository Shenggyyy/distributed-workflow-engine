"""HTTP demo scope and snapshot contracts against actual core identities."""

from datetime import datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, event, select

from tests.integration.test_demo_observations import context_for
from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.demo.api import create_demo_app
from workflow_engine.demo.observations import append, start
from workflow_engine.schema import attempt_leases

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("scenario", ["parallel", "distribution", "recovery"])
def test_fresh_real_dag_and_scope(http_engine: Engine, scenario: str) -> None:
    with TestClient(create_demo_app(Settings(), engine=http_engine)) as client:
        first = client.post("/demo/runs", json={"scenario": scenario})
        assert first.status_code == 201
        second = client.post("/demo/runs", json={"scenario": scenario})
        assert first.json()["run_id"] != second.json()["run_id"]
        snapshot = client.get("/demo/runs/" + first.json()["run_id"]).json()
        assert snapshot["run"]["scenario"] == scenario
        tasks = snapshot["tasks"]
        assert len(tasks) == (2 if scenario == "recovery" else 5)
        assert next(t for t in tasks if t["task_key"] == "Join")["status"] == "PENDING"
        assert snapshot["attempts"] == snapshot["samples"] == []
        assert len(client.get("/demo/runs").json()) == 2
        assert client.get(f"/demo/runs/{uuid4()}").status_code == 404
        assert client.post("/demo/runs", json={"scenario": "shell"}).status_code == 422


def test_private_tokens_excluded_and_real_samples_preserved(
    http_engine: Engine,
) -> None:
    context = context_for(http_engine)
    invocation = start(http_engine, context, "kernel", 123456789012345678)
    append(http_engine, invocation, "PULSE", 123456789012345679)
    with TestClient(create_demo_app(Settings(), engine=http_engine)) as client:
        response = client.get(f"/demo/runs/{context.run_id}")
        assert response.status_code == 200
        assert "lease_token" not in response.text
        snapshot = response.json()
        assert snapshot["samples"][0]["monotonic_ns"] == "123456789012345678"
        assert UUID(snapshot["attempts"][0]["id"]) == context.attempt_id
        assert snapshot["attempts"][0]["accepted_at"] is None
        with http_engine.connect() as connection:
            renewed = connection.scalar(select(attempt_leases.c.last_renewed_at))
        assert renewed is not None
        assert (
            datetime.fromisoformat(snapshot["attempts"][0]["last_renewed_at"])
            == renewed
        )
        assert snapshot["workers"][0]["last_heartbeat_at"]


def test_stock_api_and_non_demo_run_not_exposed(http_engine: Engine) -> None:
    context = context_for(http_engine, registered=False)
    with TestClient(create_demo_app(Settings(), engine=http_engine)) as client:
        assert client.get(f"/demo/runs/{context.run_id}").status_code == 404
        assert client.get("/demo/runs").json() == []
        assert client.get("/demo/").status_code == 200
        for asset in ("app.js", "evidence.js", "flow.js", "style.css"):
            assert client.get("/demo/" + asset).status_code == 200
    with TestClient(create_app(Settings(), engine=http_engine)) as client:
        assert client.get("/demo/runs").status_code == 404


def test_snapshot_does_not_mix_later_observations(http_engine: Engine) -> None:
    context = context_for(http_engine)
    invocation = start(http_engine, context, "kernel", 1)
    inserted = False

    def inject(*args: object) -> None:
        nonlocal inserted
        statement = str(args[2])
        if (
            not inserted
            and "REPEATABLE READ" not in statement
            and "clock_timestamp()" in statement
        ):
            inserted = True
            append(http_engine, invocation, "PULSE", 2)

    # A real concurrent committed insert after the read snapshot was established.
    event.listen(http_engine, "after_cursor_execute", inject)
    try:
        with TestClient(create_demo_app(Settings(), engine=http_engine)) as client:
            snapshot = client.get(f"/demo/runs/{context.run_id}").json()
            assert inserted
            assert len(snapshot["samples"]) == 1
            assert (
                len(client.get(f"/demo/runs/{context.run_id}").json()["samples"]) == 2
            )
    finally:
        event.remove(http_engine, "after_cursor_execute", inject)
