"""HTTP and lifecycle tests for the application assembled by the factory."""

import json
from importlib.metadata import version

import pytest
from fastapi.testclient import TestClient

from workflow_engine.api.app import create_app
from workflow_engine.config import Settings


def test_liveness_returns_only_the_public_status() -> None:
    with TestClient(create_app(Settings())) as client:
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert response.json() == {"status": "ok"}


def test_openapi_documents_the_liveness_response() -> None:
    with TestClient(create_app(Settings())) as client:
        response = client.get("/openapi.json")
        docs = client.get("/docs")

    assert response.status_code == 200
    schema = response.json()
    assert schema["info"]["version"] == version("distributed-workflow-engine")
    operation = schema["paths"]["/health/live"]["get"]
    assert "readiness" in operation["description"]
    response_schema = operation["responses"]["200"]["content"]["application/json"][
        "schema"
    ]
    assert response_schema["$ref"] == "#/components/schemas/HealthResponse"
    assert docs.status_code == 200
    assert "text/html" in docs.headers["content-type"]


def test_factory_uses_injected_settings_without_reloading_the_environment(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = Settings(environment="test")
    monkeypatch.setenv("DWE_ENVIRONMENT", "invalid")

    with TestClient(create_app(settings)) as client:
        assert client.get("/health/live").status_code == 200

    records = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert [record["event"] for record in records] == [
        "api_startup_complete",
        "api_shutdown_complete",
    ]
    assert all(record["component"] == "api" for record in records)
    assert all(record["environment"] == "test" for record in records)


def test_constructing_app_does_not_start_lifecycle(
    capsys: pytest.CaptureFixture[str],
) -> None:
    create_app(Settings())

    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""
