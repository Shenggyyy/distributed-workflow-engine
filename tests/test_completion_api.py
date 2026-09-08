"""Public completion shape and validation without opening a database connection."""

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from tests.test_lease_api import client as client
from workflow_engine.api.app import create_app
from workflow_engine.config import Settings


def path() -> str:
    return f"/worker-sessions/{uuid4()}/attempts/{uuid4()}/complete"


@pytest.mark.parametrize(
    "result",
    [
        None,
        {},
        {"outcome": "LOST"},
        {"outcome": "TIMED_OUT"},
        {"outcome": True},
        {"outcome": 1},
        {"outcome": "FAILED"},
        {"outcome": "FAILED", "error_code": ""},
        {"outcome": "FAILED", "error_code": "x" * 65},
        {"outcome": "FAILED", "error_code": "private code"},
        {"outcome": "SUCCEEDED", "error_code": "private-code"},
        {"outcome": "SUCCEEDED", "output": {}},
    ],
)
def test_invalid_result(client: TestClient, result: object) -> None:
    token = str(uuid4())
    response = client.post(path(), json={"lease_token": token, "result": result})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert token not in response.text and "private" not in response.text


@pytest.mark.parametrize(
    "field,value",
    [
        ("lease_token", None),
        ("lease_token", "private-value"),
        ("request_id", "private-value"),
        ("accepted_at", "private-value"),
        ("lease_seconds", 30),
        ("attempt_id", str(uuid4())),
    ],
)
def test_invalid_request_fields(client: TestClient, field: str, value: object) -> None:
    body = {
        "lease_token": str(uuid4()),
        "result": {"outcome": "SUCCEEDED"},
        field: value,
    }
    response = client.post(path(), json=body)
    assert response.status_code == 422 and "private-value" not in response.text


@pytest.mark.parametrize("content", [None, "null", "[]", "{}", '{"result":'])
def test_requires_complete_json(client: TestClient, content: str | None) -> None:
    assert (
        client.post(
            path(), content=content, headers={"Content-Type": "application/json"}
        ).status_code
        == 422
    )


@pytest.mark.parametrize("bad_session", [False, True])
def test_invalid_path(client: TestClient, bad_session: bool) -> None:
    session, attempt = (
        ("private-value", str(uuid4()))
        if bad_session
        else (str(uuid4()), "private-value")
    )
    response = client.post(
        f"/worker-sessions/{session}/attempts/{attempt}/complete",
        json={"lease_token": str(uuid4()), "result": {"outcome": "SUCCEEDED"}},
    )
    assert response.status_code == 422 and "private-value" not in response.text


@pytest.mark.parametrize(
    "headers",
    [
        [("Idempotency-Key", "")],
        [("Idempotency-Key", "one")],
        [("Idempotency-Key", "one"), ("idempotency-key", "two")],
    ],
)
def test_header_rejected(client: TestClient, headers: list[tuple[str, str]]) -> None:
    response = client.post(
        path(),
        json={"lease_token": str(uuid4()), "result": {"outcome": "SUCCEEDED"}},
        headers=headers,
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "idempotency_not_supported"


def test_unconfigured_database() -> None:
    with TestClient(create_app(Settings())) as client:
        response = client.post(
            path(),
            json={"lease_token": str(uuid4()), "result": {"outcome": "SUCCEEDED"}},
        )
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "database_not_configured"


def test_openapi_response_excludes_token(client: TestClient) -> None:
    schema = client.get("/openapi.json").json()
    operation = schema["paths"][
        "/worker-sessions/{session_id}/attempts/{attempt_id}/complete"
    ]["post"]
    assert set(operation["responses"]) == {
        "200",
        "400",
        "404",
        "409",
        "422",
        "500",
        "503",
    }
    assert operation["requestBody"]["required"]
    response = schema["components"]["schemas"]["CompleteAttemptResponse"]
    assert set(response["required"]) == {
        "attempt",
        "worker_session_id",
        "result",
        "accepted_at",
    }
    assert "lease_token" not in response["properties"]
    assert (
        schema["components"]["schemas"]["CompleteAttemptRequest"][
            "additionalProperties"
        ]
        is False
    )
