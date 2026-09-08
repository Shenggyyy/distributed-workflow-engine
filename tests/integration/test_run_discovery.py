"""Bounded Run discovery, advisory pagination and additive index migration."""

from concurrent.futures import ThreadPoolExecutor
from uuid import UUID, uuid4

import pytest
from alembic import command
from fastapi.testclient import TestClient
from sqlalchemy import Engine, inspect, select

from tests.integration.migration_helpers import migration_config
from tests.integration.test_completions import report
from tests.integration.test_lease_http import client as client
from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.repositories.claims import ClaimRepository
from workflow_engine.repositories.completions import CompletionRepository
from workflow_engine.repositories.discovery import RunDiscoveryRepository
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.schema import workflow_runs

pytestmark = pytest.mark.integration


@pytest.fixture
def run_ids(client: TestClient) -> tuple[UUID, ...]:
    version = client.post(
        "/workflows",
        json={
            "name": "discovery",
            "tasks": [{"task_id": "A", "task_type": "demo.echo"}],
        },
    )
    assert version.status_code == 201
    values = []
    for _ in range(7):
        run = client.post(
            "/runs",
            json={"workflow_version_id": version.json()["id"]},
            headers={"Idempotency-Key": uuid4().hex},
        )
        assert run.status_code == 201
        values.append(UUID(run.json()["run_id"]))
    return tuple(sorted(values))


def test_pages_are_bounded_sorted_and_exclude_terminal(
    client: TestClient, http_engine: Engine, run_ids: tuple[UUID, ...]
) -> None:
    with http_engine.begin() as connection:
        connection.execute(
            workflow_runs.update()
            .where(workflow_runs.c.id == run_ids[3])
            .values(status="FAILED")
        )
    found: list[UUID] = []
    cursor: str | None = None
    for _ in range(10):
        response = client.get(
            "/runs", params={"limit": 2, **({"after": cursor} if cursor else {})}
        )
        assert (
            response.status_code == 200
            and response.headers["Cache-Control"] == "no-store"
        )
        assert set(response.json()) == {"run_ids", "next_after"}
        page = response.json()
        assert len(page["run_ids"]) <= 2
        found.extend(UUID(value) for value in page["run_ids"])
        cursor = page["next_after"]
        if cursor is None:
            break
    assert found == [value for value in run_ids if value != run_ids[3]]


def test_ready_filter_is_advisory_and_has_no_side_effects(
    http_engine: Engine, client: TestClient, run_ids: tuple[UUID, ...]
) -> None:
    with http_engine.begin() as connection:
        session = WorkerRepository(connection).register(
            uuid4(), worker_name="filter", max_concurrency=1
        )
        claim = ClaimRepository(connection).claim_next(run_ids[0], session.session.id)
        assert claim is not None
    response = client.get("/runs", params={"ready_only": True})
    assert response.status_code == 200
    assert tuple(UUID(value) for value in response.json()["run_ids"]) == run_ids[1:]
    with http_engine.begin() as connection:
        CompletionRepository(connection).complete(report(claim, failed=True))
        assert RunDiscoveryRepository(connection).active().run_ids == run_ids
        assert (
            RunDiscoveryRepository(connection).active(ready_only=True).run_ids
            == run_ids[1:]
        )


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 0},
        {"limit": 101},
        {"limit": "x"},
        {"after": "bad"},
        {"ready_only": "bad"},
    ],
)
def test_invalid_queries(client: TestClient, params: dict[str, object]) -> None:
    response = client.get("/runs", params=params)  # type: ignore[arg-type]
    assert response.status_code == 422


def test_empty_database(client: TestClient) -> None:
    assert client.get("/runs").json() == {"run_ids": [], "next_after": None}


def test_cursor_is_order_boundary_not_existing_row(
    http_engine: Engine, run_ids: tuple[UUID, ...]
) -> None:
    with http_engine.begin() as connection:
        repository = RunDiscoveryRepository(connection)
        assert repository.active(after=UUID(int=0), limit=100).run_ids == run_ids
        assert repository.active(after=UUID(int=2**128 - 1)).run_ids == ()


def test_discovery_does_not_wait_for_execution_row_locks(
    http_engine: Engine, run_ids: tuple[UUID, ...]
) -> None:
    def discover() -> tuple[UUID, ...]:
        with http_engine.begin() as connection:
            return RunDiscoveryRepository(connection).active().run_ids

    with ThreadPoolExecutor(max_workers=1) as executor:
        with http_engine.begin() as owner:
            owner.execute(select(workflow_runs).with_for_update()).all()
            assert executor.submit(discover).result(timeout=3) == run_ids


def test_index_migration_preserves_populated_runs(
    engine: Engine, migration_schema: str
) -> None:
    from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
    from workflow_engine.repositories.workflows import WorkflowRepository

    with engine.begin() as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "0008")
        version = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name="indexed",
                tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),),
            )
        )
        run = RunRepository(connection).create(version.id)
    for revision in ("head", "0008", "head"):
        with engine.begin() as connection:
            config = migration_config(connection, migration_schema)
            if revision == "0008":
                command.downgrade(config, revision)
            else:
                command.upgrade(config, revision)
                command.check(config)
            assert RunDiscoveryRepository(connection).active().run_ids == (run.run.id,)
            indexes = {
                index["name"]
                for index in inspect(connection).get_indexes(
                    "workflow_runs", schema=migration_schema
                )
            }
            assert ("ix_workflow_runs_active_id" in indexes) == (revision == "head")
