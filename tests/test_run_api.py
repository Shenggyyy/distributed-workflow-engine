"""Run HTTP validation and OpenAPI contracts without PostgreSQL."""

from collections.abc import Iterator
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import event

from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.database import create_database_engine


@pytest.fixture
def client() -> Iterator[TestClient]:
    engine = create_database_engine(Settings(database_password=SecretStr("test-only")))

    def forbid_connect(*args: object) -> None:
        raise AssertionError("Invalid requests and OpenAPI must not connect.")

    event.listen(engine, "do_connect", forbid_connect)
    try:
        with TestClient(create_app(Settings(), engine=engine)) as value:
            yield value
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"workflow_version_id": "private-value"},
        {"workflow_version_id": None},
        {"workflow_version_id": 123},
        {"workflow_version_id": str(uuid4()), "input": "private-value"},
        {"workflow_version_id": str(uuid4()), "status": "private-value"},
    ],
)
def test_invalid_body_is_sanitized(client: TestClient, body: dict[str, object]) -> None:
    response = client.post("/runs", json=body, headers={"Idempotency-Key": "valid"})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert "private-value" not in response.text
    assert "Location" not in response.headers


@pytest.mark.parametrize("key", [None, "", " ", "private/key", "a" * 129, "one,two"])
def test_required_header_and_key_format(client: TestClient, key: str | None) -> None:
    headers = {} if key is None else {"Idempotency-Key": key}
    response = client.post(
        "/runs", json={"workflow_version_id": str(uuid4())}, headers=headers
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert "private/key" not in response.text
    assert "Location" not in response.headers


@pytest.mark.parametrize("second", ["first", "second"])
def test_duplicate_header_is_rejected(client: TestClient, second: str) -> None:
    response = client.post(
        "/runs",
        json={"workflow_version_id": str(uuid4())},
        headers=[("Idempotency-Key", "first"), ("idempotency-key", second)],
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


def test_malformed_json_is_sanitized(client: TestClient) -> None:
    response = client.post(
        "/runs",
        content='{"workflow_version_id": private-value}',
        headers={"Content-Type": "application/json", "Idempotency-Key": "valid"},
    )
    assert response.status_code == 422
    assert "private-value" not in response.text


@pytest.mark.parametrize("path", ["/runs/not-a-uuid", "/runs/not-a-uuid/tasks"])
def test_invalid_path(client: TestClient, path: str) -> None:
    response = client.get(path)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


@pytest.mark.parametrize("method", ["post", "summary", "tasks"])
def test_unconfigured_database_returns_503(method: str) -> None:
    with TestClient(create_app(Settings())) as value:
        if method == "post":
            response = value.post(
                "/runs",
                json={"workflow_version_id": str(uuid4())},
                headers={"Idempotency-Key": "valid"},
            )
        else:
            suffix = "/tasks" if method == "tasks" else ""
            response = value.get(f"/runs/{uuid4()}{suffix}")
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "database_not_configured"
        assert "Location" not in response.headers
        assert value.get("/health/live").status_code == 200


def test_openapi_run_contract(client: TestClient) -> None:
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/runs"]["post"]
    header = next(p for p in operation["parameters"] if p["name"] == "Idempotency-Key")
    assert header["required"] and header["in"] == "header"
    assert header["schema"]["type"] == "string"
    assert header["schema"]["minLength"] == 1
    assert header["schema"]["maxLength"] == 128
    assert "pattern" in header["schema"]
    assert "Location" in operation["responses"]["201"]["headers"]
    for code in ("404", "409", "422", "500", "503"):
        assert operation["responses"][code]["content"]["application/json"][
            "schema"
        ] == {"$ref": "#/components/schemas/ErrorResponse"}
    assert (
        schema["components"]["schemas"]["CreateRunRequest"]["additionalProperties"]
        is False
    )
    assert set(
        schema["components"]["schemas"]["RunCreationResponse"]["properties"]
    ) == {"run_id", "workflow_version_id"}
    assert "/runs/{run_id}" in schema["paths"]
    assert "/runs/{run_id}/tasks" in schema["paths"]
    assert (
        schema["components"]["schemas"]["RunTasksResponse"]["properties"]["tasks"][
            "maxItems"
        ]
        == 1000
    )
