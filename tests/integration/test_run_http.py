"""Run HTTP behavior on PostgreSQL, including races and COMMIT failures."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from uuid import UUID, uuid4

import pytest
from alembic import command
from fastapi.testclient import TestClient
from sqlalchemy import Engine, event, func, select

from tests.integration.migration_helpers import migration_config
from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.database import database_engine
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    run_creation_requests,
    task_attempts,
    task_runs,
    workflow_runs,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def http_engine(engine: Engine, migration_schema: str) -> Engine:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return engine.execution_options(schema_translate_map={None: migration_schema})


@pytest.fixture
def client(http_engine: Engine) -> Iterator[TestClient]:
    with TestClient(
        create_app(Settings(environment="test"), engine=http_engine)
    ) as value:
        yield value


@pytest.fixture
def versions(http_engine: Engine) -> tuple[str, str]:
    definition = WorkflowDefinition(
        name="http_run",
        tasks=(
            TaskDefinition(task_id="A", task_type="demo.echo"),
            TaskDefinition(task_id="B", task_type="demo.echo", depends_on=("A",)),
        ),
    )
    with http_engine.begin() as connection:
        repo = WorkflowRepository(connection)
        return str(repo.publish(definition).id), str(repo.publish(definition).id)


def assert_counts(engine: Engine, count: int) -> None:
    with engine.begin() as connection:
        for table, expected in (
            (workflow_runs, count),
            (task_runs, count * 2),
            (run_creation_requests, count),
            (task_attempts, 0),
        ):
            assert (
                connection.execute(select(func.count()).select_from(table)).scalar_one()
                == expected
            )


def test_committed_creation_replay_and_queries(
    client: TestClient, http_engine: Engine, versions: tuple[str, str]
) -> None:
    body, headers = {"workflow_version_id": versions[0]}, {"Idempotency-Key": "request"}
    first = client.post("/runs", json=body, headers=headers)
    assert first.status_code == 201
    receipt = first.json()
    assert set(receipt) == {"run_id", "workflow_version_id"}
    assert receipt["workflow_version_id"] == versions[0]
    assert first.headers["Location"] == f"/runs/{receipt['run_id']}"
    assert_counts(http_engine, 1)
    with http_engine.begin() as connection:
        stored = RunRepository(connection).get_run_with_tasks(UUID(receipt["run_id"]))
        assert stored is not None and len(stored.tasks) == 2
    replay = client.post("/runs", json=body, headers=headers)
    assert replay.status_code == 201 and replay.json() == receipt
    assert replay.headers["Location"] == first.headers["Location"]
    summary = client.get(first.headers["Location"])
    detail = client.get(first.headers["Location"] + "/tasks")
    assert summary.status_code == detail.status_code == 200
    assert detail.json()["run"] == summary.json()
    assert summary.json()["status"] == "RUNNING"
    assert "created_at" in summary.json()
    tasks = detail.json()["tasks"]
    assert [(task["task_key"], task["status"]) for task in tasks] == [
        ("A", "READY"),
        ("B", "PENDING"),
    ]
    assert all(task["run_id"] == receipt["run_id"] for task in tasks)
    assert_counts(http_engine, 1)


@pytest.mark.parametrize("missing", [False, True])
def test_conflict_is_409_and_preserves_original(
    client: TestClient, http_engine: Engine, versions: tuple[str, str], missing: bool
) -> None:
    headers = {"Idempotency-Key": "private-key"}
    first = client.post(
        "/runs", json={"workflow_version_id": versions[0]}, headers=headers
    )
    assert first.status_code == 201
    conflict = client.post(
        "/runs",
        json={"workflow_version_id": str(uuid4()) if missing else versions[1]},
        headers=headers,
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_conflict"
    assert "Location" not in conflict.headers
    assert "private-key" not in conflict.text
    assert_counts(http_engine, 1)


def test_missing_resources_and_failed_creation_release_key(
    client: TestClient, http_engine: Engine, versions: tuple[str, str]
) -> None:
    for suffix in ("", "/tasks"):
        result = client.get(f"/runs/{uuid4()}{suffix}")
        assert result.status_code == 404
        assert result.json()["error"]["code"] == "run_not_found"
    missing = client.post(
        "/runs",
        json={"workflow_version_id": str(uuid4())},
        headers={"Idempotency-Key": "retry"},
    )
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "version_not_found"
    assert "Location" not in missing.headers
    assert_counts(http_engine, 0)
    assert (
        client.post(
            "/runs",
            json={"workflow_version_id": versions[0]},
            headers={"Idempotency-Key": "retry"},
        ).status_code
        == 201
    )


def test_replay_does_not_reset_observed_execution_state(
    client: TestClient, http_engine: Engine, versions: tuple[str, str]
) -> None:
    body, headers = (
        {"workflow_version_id": versions[0]},
        {"Idempotency-Key": "advanced"},
    )
    first = client.post("/runs", json=body, headers=headers)
    assert first.status_code == 201
    with http_engine.begin() as connection:
        # Simulate persisted progress; handlers and aggregation are not running.
        connection.execute(
            task_runs.update()
            .where(task_runs.c.status == "PENDING")
            .values(status="READY")
        )
        connection.execute(task_runs.update().values(status="RUNNING"))
        connection.execute(task_runs.update().values(status="FAILED"))
        connection.execute(workflow_runs.update().values(status="FAILED"))
    assert client.post("/runs", json=body, headers=headers).json() == first.json()
    detail = client.get(first.headers["Location"] + "/tasks").json()
    assert detail["run"]["status"] == "FAILED"
    assert {task["status"] for task in detail["tasks"]} == {"FAILED"}


@pytest.mark.parametrize("different", [False, True])
def test_concurrent_http_requests_create_one_run(
    client: TestClient, http_engine: Engine, versions: tuple[str, str], different: bool
) -> None:
    barrier = Barrier(4, timeout=10)

    def submit(index: int) -> tuple[int, dict[str, object]]:
        barrier.wait()
        response = client.post(
            "/runs",
            json={
                "workflow_version_id": versions[index % 2] if different else versions[0]
            },
            headers={"Idempotency-Key": "shared"},
        )
        return response.status_code, dict(response.json())

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(submit, index) for index in range(4)]
        results = [future.result(timeout=20) for future in futures]
    successful = [value for status, value in results if status == 201]
    assert len(successful) == (2 if different else 4)
    assert all(value == successful[0] for value in successful)
    assert all(status in (201, 409) for status, _ in results)
    assert_counts(http_engine, 1)


@pytest.mark.parametrize("phase", ["initialization", "commit"])
def test_database_failure_never_returns_created_and_can_retry(
    client: TestClient,
    http_engine: Engine,
    versions: tuple[str, str],
    migration_schema: str,
    phase: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    table = "task_runs" if phase == "initialization" else "run_creation_requests"
    with http_engine.begin() as connection:
        connection.exec_driver_sql(f"""
            CREATE FUNCTION "{migration_schema}".reject_run_http() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION USING ERRCODE='23514',
                    MESSAGE='sentinel-private-database-detail';
            END; $$""")
        if phase == "commit":
            connection.exec_driver_sql(f"""
                CREATE CONSTRAINT TRIGGER reject_run_http AFTER INSERT
                ON "{migration_schema}".run_creation_requests
                DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
                EXECUTE FUNCTION "{migration_schema}".reject_run_http()""")
        else:
            connection.exec_driver_sql(f"""
                CREATE TRIGGER reject_run_http BEFORE UPDATE
                ON "{migration_schema}".task_runs FOR EACH STATEMENT
                EXECUTE FUNCTION "{migration_schema}".reject_run_http()""")
    body, headers = {"workflow_version_id": versions[0]}, {"Idempotency-Key": "retry"}
    response = client.post("/runs", json=body, headers=headers)
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "storage_error"
    assert "Location" not in response.headers
    assert (
        "sentinel-private-database-detail"
        not in response.text + capsys.readouterr().err
    )
    assert_counts(http_engine, 0)
    with http_engine.begin() as connection:
        connection.exec_driver_sql(
            f'DROP TRIGGER reject_run_http ON "{migration_schema}".{table}'
        )
    assert client.post("/runs", json=body, headers=headers).status_code == 201
    assert_counts(http_engine, 1)


@pytest.mark.parametrize("kind", ["run", "task"])
def test_invalid_runtime_maps_to_sanitized_storage_error(
    client: TestClient,
    http_engine: Engine,
    versions: tuple[str, str],
    migration_schema: str,
    kind: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with http_engine.begin() as connection:
        run = RunRepository(connection).create(UUID(versions[0])).run
        if kind == "run":
            connection.exec_driver_sql(
                f'ALTER TABLE "{migration_schema}".workflow_runs '
                "DROP CONSTRAINT ck_workflow_runs_status_values"
            )
            run_id = uuid4()
            connection.execute(
                workflow_runs.insert().values(
                    id=run_id,
                    workflow_version_id=UUID(versions[0]),
                    status="PRIVATE_BAD",
                )
            )
        else:
            run_id = run.id
            connection.exec_driver_sql(
                f'ALTER TABLE "{migration_schema}".task_runs '
                "DROP CONSTRAINT ck_task_runs_status_values"
            )
            connection.execute(
                task_runs.insert().values(
                    id=uuid4(), run_id=run_id, task_key="bad", status="PRIVATE_BAD"
                )
            )
    paths = [f"/runs/{run_id}/tasks"]
    if kind == "run":
        paths.append(f"/runs/{run_id}")
    for path in paths:
        response = client.get(path)
        assert response.status_code == 500
        assert response.json()["error"]["code"] == "storage_error"
        assert "PRIVATE_BAD" not in response.text + capsys.readouterr().err


def test_liveness_while_creation_waits_for_key(
    client: TestClient, http_engine: Engine, versions: tuple[str, str]
) -> None:
    entered = Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if statement.startswith("INSERT INTO") and "run_creation_requests" in statement:
            entered.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        with http_engine.begin() as owner:
            receipt = RunRepository(owner).create_idempotent(
                UUID(versions[0]), idempotency_key="held"
            )
            event.listen(http_engine, "before_cursor_execute", before_execute)
            try:
                pending = executor.submit(
                    client.post,
                    "/runs",
                    json={"workflow_version_id": versions[0]},
                    headers={"Idempotency-Key": "held"},
                )
                assert entered.wait(timeout=5)
                health = executor.submit(client.get, "/health/live")
                assert health.result(timeout=2).status_code == 200
                assert not pending.done()
            finally:
                event.remove(http_engine, "before_cursor_execute", before_execute)
        result = pending.result(timeout=5)
        assert result.status_code == 201
        assert result.json()["run_id"] == str(receipt.run_id)


def test_pool_exhaustion_maps_to_503(
    http_engine: Engine,
    database_settings: Settings,
    migration_schema: str,
    versions: tuple[str, str],
) -> None:
    settings = database_settings.model_copy(
        update={"database_pool_size": 1, "database_pool_timeout_seconds": 1}
    )
    with database_engine(settings) as bounded:
        scoped = bounded.execution_options(
            schema_translate_map={None: migration_schema}
        )
        with TestClient(create_app(settings, engine=scoped)) as client:
            with scoped.connect():
                response = client.post(
                    "/runs",
                    json={"workflow_version_id": versions[0]},
                    headers={"Idempotency-Key": "retry"},
                )
                assert response.status_code == 503
                assert response.json()["error"]["code"] == "database_unavailable"
                assert "Location" not in response.headers
                assert client.get("/health/live").status_code == 200
            assert (
                client.post(
                    "/runs",
                    json={"workflow_version_id": versions[0]},
                    headers={"Idempotency-Key": "retry"},
                ).status_code
                == 201
            )
