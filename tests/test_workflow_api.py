"""HTTP validation, safe failures and pool lifecycle without PostgreSQL."""

import json
import socket
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import Engine, event

from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.database import DatabaseConfigurationError, create_database_engine


@pytest.fixture
def client() -> Iterator[TestClient]:
    # Lazy engine: invalid requests and OpenAPI must not check out a connection.
    with TestClient(
        create_app(Settings(database_password=SecretStr("test-only")))
    ) as value:
        yield value


def payload() -> dict[str, object]:
    return {
        "name": "demo",
        "tasks": [{"task_id": "A", "task_type": "demo.echo", "depends_on": []}],
    }


@pytest.mark.parametrize(
    ("body", "error_type"),
    [
        ({}, "missing"),
        ({"name": "demo", "tasks": [], "private": "secret-value"}, "too_short"),
        (
            {
                "name": "demo",
                "tasks": [
                    {"task_id": "A", "task_type": "demo.echo", "depends_on": ["A"]}
                ],
            },
            "self_dependency",
        ),
        (
            {
                "name": "demo",
                "tasks": [
                    {"task_id": "A", "task_type": "demo.echo", "depends_on": ["B"]},
                    {"task_id": "B", "task_type": "demo.echo", "depends_on": ["A"]},
                ],
            },
            "cycle_detected",
        ),
    ],
)
def test_invalid_definitions_return_sanitized_422(
    client: TestClient, body: dict[str, object], error_type: str
) -> None:
    response = client.post("/workflows", json=body)
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "invalid_request"
    assert error_type in {item["type"] for item in error["details"]}
    assert all(set(item) == {"location", "type"} for item in error["details"])
    assert "secret-value" not in response.text
    assert "Location" not in response.headers


def test_malformed_json_does_not_echo_body(client: TestClient) -> None:
    response = client.post(
        "/workflows",
        content='{"name": secret-value}',
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert response.json()["error"]["details"][0]["type"] == "json_invalid"
    assert "secret-value" not in response.text


@pytest.mark.parametrize(
    "path",
    [
        "/workflows/1invalid",
        "/workflow-versions/not-a-uuid",
        "/workflows/not-a-uuid/versions/latest",
        f"/workflows/{uuid4()}/versions/0",
        f"/workflows/{uuid4()}/versions/2147483648",
        f"/workflows/{uuid4()}/versions/abc",
    ],
)
def test_invalid_path_parameters(client: TestClient, path: str) -> None:
    response = client.get(path)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


def test_unsupported_idempotency_key_is_explicit(client: TestClient) -> None:
    response = client.post(
        "/workflows", json=payload(), headers={"Idempotency-Key": "secret-value"}
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "idempotency_not_supported"
    assert "secret-value" not in response.text


def test_database_unconfigured_preserves_liveness() -> None:
    with TestClient(create_app(Settings())) as value:
        assert value.get("/health/live").json() == {"status": "ok"}
        response = value.get("/workflows/demo")
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "database_not_configured"


def test_unreachable_database_returns_safe_503_and_keeps_api_alive(
    capsys: pytest.CaptureFixture[str],
) -> None:
    sentinel = "sentinel-private-password"
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        settings = Settings(
            database_password=SecretStr(sentinel),
            database_port=reserved.getsockname()[1],
            database_connect_timeout_seconds=2,
        )
        with TestClient(create_app(settings)) as value:
            assert value.get("/health/live").status_code == 200
            response = value.post("/workflows", json=payload())
            assert response.status_code == 503
            assert response.json()["error"]["code"] == "database_unavailable"
            assert "Location" not in response.headers
            assert value.get("/health/live").status_code == 200
    logs = capsys.readouterr().err
    assert sentinel not in response.text + logs
    assert "postgresql" not in response.text
    records = [json.loads(line) for line in logs.splitlines()]
    assert any(record["event"] == "api_storage_failed" for record in records)


def test_openapi_documents_workflow_contract(client: TestClient) -> None:
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/workflows"]["post"]
    assert "Idempotency-Key" in operation["description"]
    assert "Location" in operation["responses"]["201"]["headers"]
    for code in ["400", "422", "500", "503"]:
        assert operation["responses"][code]["content"]["application/json"][
            "schema"
        ] == {"$ref": "#/components/schemas/ErrorResponse"}
    assert "/workflows/{workflow_id}/versions/latest" in schema["paths"]
    assert "/workflows/{workflow_id}/versions/{version_number}" in schema["paths"]
    assert "/workflow-versions/{version_id}" in schema["paths"]


@pytest.mark.parametrize("injected", [False, True])
def test_engine_creation_is_lazy_and_disposal_respects_ownership(
    monkeypatch: pytest.MonkeyPatch, injected: bool
) -> None:
    settings = Settings(database_password=SecretStr("test-only"))
    engine = create_database_engine(settings)
    disposed: list[Engine] = []
    created: list[Settings] = []

    def factory(value: Settings) -> Engine:
        created.append(value)
        return engine

    def forbid_checkout(*args: object) -> None:
        raise AssertionError("Liveness/startup must not connect to PostgreSQL.")

    event.listen(engine, "engine_disposed", disposed.append)
    event.listen(engine, "connect", forbid_checkout)
    monkeypatch.setattr("workflow_engine.api.app.create_database_engine", factory)
    app = create_app(settings, engine=engine if injected else None)
    assert created == []
    try:
        with TestClient(app) as value:
            assert value.get("/health/live").status_code == 200
            assert app.state.database_engine is engine
        assert created == ([] if injected else [settings])
        assert disposed == ([] if injected else [engine])
        assert app.state.database_engine is None
    finally:
        engine.dispose()


def test_bad_password_file_fails_startup_with_fixed_error(tmp_path: Path) -> None:
    app = create_app(Settings(database_password_file=tmp_path / "missing-secret"))
    with pytest.raises(DatabaseConfigurationError) as error:
        with TestClient(app):
            pass
    assert str(error.value) == "Database password file could not be read."
