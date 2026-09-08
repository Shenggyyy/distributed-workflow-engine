"""Claim HTTP input validation and OpenAPI without database connections."""

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
        raise AssertionError("Invalid claims must not connect.")

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
        {"run_id": str(uuid4())},
        {"request_id": str(uuid4())},
        {"run_id": "private-value", "request_id": str(uuid4())},
        {"run_id": str(uuid4()), "request_id": "private-value"},
        {"run_id": 123, "request_id": str(uuid4())},
        {"run_id": str(uuid4()), "request_id": None},
        {"run_id": str(uuid4()), "request_id": True},
        {"run_id": str(uuid4()), "request_id": str(uuid4()), "lease_seconds": 900},
        {
            "run_id": str(uuid4()),
            "request_id": str(uuid4()),
            "worker_session_id": str(uuid4()),
        },
        {
            "run_id": str(uuid4()),
            "request_id": str(uuid4()),
            "lease_token": "private-value",
        },
    ],
)
def test_invalid_claim_body(client: TestClient, body: dict[str, object]) -> None:
    result = client.post(f"/worker-sessions/{uuid4()}/claims", json=body)
    assert result.status_code == 422
    assert result.json()["error"]["code"] == "invalid_request"
    assert "private-value" not in result.text


@pytest.mark.parametrize(
    "content", [None, "null", "[]", '"private-value"', '{"run_id":']
)
def test_requires_json_object(client: TestClient, content: str | None) -> None:
    result = client.post(
        f"/worker-sessions/{uuid4()}/claims",
        content=content,
        headers={"Content-Type": "application/json"},
    )
    assert result.status_code == 422
    assert "private-value" not in result.text


def test_invalid_session_path(client: TestClient) -> None:
    result = client.post(
        "/worker-sessions/private-value/claims",
        json={"run_id": str(uuid4()), "request_id": str(uuid4())},
    )
    assert result.status_code == 422 and "private-value" not in result.text


@pytest.mark.parametrize(
    "headers",
    [
        [("Idempotency-Key", "")],
        [("Idempotency-Key", "private-value")],
        [("Idempotency-Key", "one"), ("idempotency-key", "two")],
    ],
)
def test_second_identity_channel_is_rejected(
    client: TestClient, headers: list[tuple[str, str]]
) -> None:
    result = client.post(
        f"/worker-sessions/{uuid4()}/claims",
        json={"run_id": str(uuid4()), "request_id": str(uuid4())},
        headers=headers,
    )
    assert result.status_code == 400
    assert result.json()["error"]["code"] == "idempotency_not_supported"
    assert "private-value" not in result.text


def test_unconfigured_database() -> None:
    with TestClient(create_app(Settings())) as client:
        result = client.post(
            f"/worker-sessions/{uuid4()}/claims",
            json={"run_id": str(uuid4()), "request_id": str(uuid4())},
        )
        assert result.status_code == 503
        assert result.json()["error"]["code"] == "database_not_configured"
        assert client.get("/health/live").status_code == 200


def test_openapi_claim_contract(client: TestClient) -> None:
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/worker-sessions/{session_id}/claims"]["post"]
    assert operation["requestBody"]["required"]
    assert "201" not in operation["responses"] and "204" not in operation["responses"]
    assert (
        operation["responses"]["200"]["headers"]["Cache-Control"]["description"]
        == "no-store"
    )
    for status in ("400", "404", "409", "422", "500", "503"):
        assert operation["responses"][status]["content"]["application/json"][
            "schema"
        ] == {"$ref": "#/components/schemas/ErrorResponse"}
    request = schema["components"]["schemas"]["ClaimTaskRequest"]
    assert request["additionalProperties"] is False
    assert set(request["required"]) == {"run_id", "request_id"}
    response = schema["components"]["schemas"]["ClaimTaskResponse"]
    assert "claim" in response["required"]
    assert {"type": "null"} in response["properties"]["claim"]["anyOf"]
    assert schema["components"]["schemas"]["TaskClaimResponse"]["properties"]["lease"]
    # Renewal is a separate route and cannot be requested through the claim body.
    assert "lease_seconds" not in request["properties"]
