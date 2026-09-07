"""HTTP integration against migrated PostgreSQL, including deferred COMMIT failure."""

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
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import workflow_versions, workflows

pytestmark = pytest.mark.integration


@pytest.fixture
def http_engine(engine: Engine, migration_schema: str) -> Engine:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    # Core tables are qualified per test, with no global search_path or schema mutation.
    return engine.execution_options(schema_translate_map={None: migration_schema})


@pytest.fixture
def http_client(http_engine: Engine) -> Iterator[TestClient]:
    with TestClient(
        create_app(Settings(environment="test"), engine=http_engine)
    ) as client:
        yield client


def definition(name: str = "demo") -> WorkflowDefinition:
    return WorkflowDefinition(
        name=name, tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),)
    )


def test_publish_is_committed_before_response_and_all_lookup_routes(
    http_client: TestClient, http_engine: Engine
) -> None:
    body = definition().model_dump(mode="json")
    response = http_client.post("/workflows", json=body)
    assert response.status_code == 201
    first = response.json()
    assert first["definition"] == body
    assert first["version_number"] == 1
    assert response.headers["Location"] == f"/workflow-versions/{first['id']}"
    # Independent checkout proves HTTP success did not precede transaction commit.
    with http_engine.begin() as connection:
        stored = WorkflowRepository(connection).get_version(UUID(first["id"]))
        assert stored is not None
        assert stored.definition == definition()
        assert stored.created_at.tzinfo is not None
    second_response = http_client.post("/workflows", json=body)
    assert second_response.status_code == 201
    second = second_response.json()
    assert second["version_number"] == 2
    assert second["workflow_id"] == first["workflow_id"]
    assert second["id"] != first["id"]
    workflow = http_client.get("/workflows/demo")
    assert workflow.status_code == 200
    assert workflow.json()["id"] == first["workflow_id"]
    paths = [
        (response.headers["Location"], first),
        (f"/workflows/{first['workflow_id']}/versions/1", first),
        (f"/workflows/{first['workflow_id']}/versions/latest", second),
    ]
    for path, expected in paths:
        result = http_client.get(path)
        assert result.status_code == 200
        assert result.json() == expected


@pytest.mark.parametrize(
    "path",
    [
        "/workflows/missing",
        f"/workflow-versions/{uuid4()}",
        f"/workflows/{uuid4()}/versions/1",
        f"/workflows/{uuid4()}/versions/latest",
    ],
)
def test_missing_resources_are_404(http_client: TestClient, path: str) -> None:
    response = http_client.get(path)
    assert response.status_code == 404
    assert response.json()["error"]["code"] in {
        "workflow_not_found",
        "version_not_found",
    }


def test_invalid_requests_and_unsupported_idempotency_do_not_write(
    http_client: TestClient, http_engine: Engine
) -> None:
    body = definition().model_dump(mode="json")
    assert (
        http_client.post(
            "/workflows", json=body, headers={"Idempotency-Key": "not-supported"}
        ).status_code
        == 400
    )
    body["tasks"][0]["depends_on"] = ["missing"]
    assert http_client.post("/workflows", json=body).status_code == 422
    with http_engine.begin() as connection:
        assert (
            connection.execute(select(func.count()).select_from(workflows)).scalar()
            == 0
        )
        assert (
            connection.execute(
                select(func.count()).select_from(workflow_versions)
            ).scalar()
            == 0
        )


def test_corrupt_snapshot_returns_storage_error(
    http_client: TestClient, http_engine: Engine, capsys: pytest.CaptureFixture[str]
) -> None:
    workflow_id, version_id = uuid4(), uuid4()
    with http_engine.begin() as connection:
        connection.execute(workflows.insert().values(id=workflow_id, name="corrupt"))
        connection.execute(
            workflow_versions.insert().values(
                id=version_id,
                workflow_id=workflow_id,
                version_number=1,
                definition={"private_payload": "sentinel-do-not-expose"},
            )
        )
    response = http_client.get(f"/workflow-versions/{version_id}")
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "storage_error"
    assert "sentinel-do-not-expose" not in response.text + capsys.readouterr().err


def test_deferred_commit_failure_cannot_return_created(
    http_client: TestClient, http_engine: Engine, migration_schema: str
) -> None:
    # The version INSERT succeeds; PostgreSQL rejects only COMMIT. This catches
    # transactions mistakenly committed in a dependency after response delivery.
    with http_engine.begin() as connection:
        connection.exec_driver_sql(f"""
            CREATE FUNCTION "{migration_schema}".reject_test_commit() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.definition->>'name' = 'reject' THEN
                    RAISE EXCEPTION USING ERRCODE='23514',
                        MESSAGE='sentinel-private-database-detail';
                END IF;
                RETURN NEW;
            END;
            $$
        """)
        connection.exec_driver_sql(f"""
            CREATE CONSTRAINT TRIGGER reject_test_commit
            AFTER INSERT ON "{migration_schema}".workflow_versions
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            EXECUTE FUNCTION "{migration_schema}".reject_test_commit()
        """)
    response = http_client.post(
        "/workflows", json=definition("reject").model_dump(mode="json")
    )
    assert response.status_code == 500
    assert "Location" not in response.headers
    assert response.json()["error"]["code"] == "storage_error"
    assert "sentinel-private-database-detail" not in response.text
    with http_engine.begin() as connection:
        assert WorkflowRepository(connection).get_workflow("reject") is None
        assert (
            connection.execute(
                select(func.count()).select_from(workflow_versions)
            ).scalar()
            == 0
        )
    # Pool and application remain usable after the failed commit.
    assert (
        http_client.post(
            "/workflows", json=definition("accepted").model_dump(mode="json")
        ).status_code
        == 201
    )


def test_concurrent_http_publications(http_client: TestClient) -> None:
    barrier = Barrier(4, timeout=10)

    def publish() -> dict[str, object]:
        barrier.wait()
        response = http_client.post(
            "/workflows", json=definition().model_dump(mode="json")
        )
        assert response.status_code == 201
        return dict(response.json())

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(publish) for _ in range(4)]
        records = [future.result(timeout=20) for future in futures]
    assert {record["version_number"] for record in records} == {1, 2, 3, 4}
    assert len({record["workflow_id"] for record in records}) == 1


def test_liveness_runs_while_publication_waits_on_row_lock(
    http_client: TestClient, http_engine: Engine
) -> None:
    body = definition().model_dump(mode="json")
    assert http_client.post("/workflows", json=body).status_code == 201
    entered = Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if "FOR UPDATE" in statement:
            entered.set()

    # Acquire before registering the listener so only the HTTP contender signals.
    with ThreadPoolExecutor(max_workers=2) as executor:
        with http_engine.begin() as connection:
            WorkflowRepository(connection).publish(definition())
            event.listen(http_engine, "before_cursor_execute", before_execute)
            try:
                pending = executor.submit(http_client.post, "/workflows", json=body)
                assert entered.wait(timeout=5)
                health = executor.submit(http_client.get, "/health/live")
                assert health.result(timeout=2).json() == {"status": "ok"}
                assert not pending.done()
            finally:
                event.remove(http_engine, "before_cursor_execute", before_execute)
        assert pending.result(timeout=5).status_code == 201


def test_pool_exhaustion_returns_503(
    http_engine: Engine, database_settings: Settings, migration_schema: str
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
                response = client.get("/workflows/demo")
                assert response.status_code == 503
                assert response.json()["error"]["code"] == "database_unavailable"
                assert client.get("/health/live").status_code == 200
            assert client.get("/workflows/demo").status_code == 404
