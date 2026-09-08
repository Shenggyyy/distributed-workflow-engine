"""Renewal HTTP validation and token-safe public contracts without database I/O."""

from collections.abc import Iterator
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import event

from workflow_engine.api.app import create_app
from workflow_engine.api.leases import RenewLeaseRequest
from workflow_engine.config import Settings
from workflow_engine.database import create_database_engine


@pytest.fixture
def client() -> Iterator[TestClient]:
    engine = create_database_engine(Settings(database_password=SecretStr("test-only")))

    def forbid_connect(*args: object) -> None:
        raise AssertionError("Invalid renewal must not connect.")

    event.listen(engine, "do_connect", forbid_connect)
    try:
        with TestClient(create_app(Settings(), engine=engine)) as value:
            yield value
    finally:
        engine.dispose()


def path() -> str:
    return f"/worker-sessions/{uuid4()}/attempts/{uuid4()}/renew"


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"lease_token": "private-value"},
        {"lease_token": 123},
        {"lease_token": None},
        {"lease_token": True},
        {"lease_token": str(uuid4()), "lease_seconds": 900},
        {"lease_token": str(uuid4()), "request_id": str(uuid4())},
        {"lease_token": str(uuid4()), "worker_session_id": str(uuid4())},
        {"lease_token": str(uuid4()), "attempt_id": str(uuid4())},
        {"lease_token": str(uuid4()), "observed_at": "private-value"},
    ],
)
def test_invalid_body(client: TestClient, body: dict[str, object]) -> None:
    response = client.post(path(), json=body)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert "private-value" not in response.text
    token = body.get("lease_token")
    if isinstance(token, str):
        assert token not in response.text


@pytest.mark.parametrize(
    "content", [None, "null", "[]", '"private-value"', '{"lease_token":']
)
def test_requires_json_object(client: TestClient, content: str | None) -> None:
    response = client.post(
        path(), content=content, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422 and "private-value" not in response.text


@pytest.mark.parametrize("field", ["session", "attempt"])
def test_invalid_path_ids(client: TestClient, field: str) -> None:
    session, attempt = str(uuid4()), str(uuid4())
    if field == "session":
        session = "private-value"
    else:
        attempt = "private-value"
    response = client.post(
        f"/worker-sessions/{session}/attempts/{attempt}/renew",
        json={"lease_token": str(uuid4())},
    )
    assert response.status_code == 422 and "private-value" not in response.text


@pytest.mark.parametrize(
    "headers",
    [
        [("Idempotency-Key", "")],
        [("Idempotency-Key", "private-value")],
        [("Idempotency-Key", "one"), ("idempotency-key", "two")],
    ],
)
def test_receipt_header_is_rejected(
    client: TestClient, headers: list[tuple[str, str]]
) -> None:
    response = client.post(path(), json={"lease_token": str(uuid4())}, headers=headers)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "idempotency_not_supported"
    assert "private-value" not in response.text


def test_unconfigured_database() -> None:
    with TestClient(create_app(Settings())) as client:
        response = client.post(path(), json={"lease_token": str(uuid4())})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "database_not_configured"
        assert client.get("/health/live").status_code == 200


def test_openapi_and_request_repr(client: TestClient) -> None:
    schema = client.get("/openapi.json").json()
    operation = schema["paths"][
        "/worker-sessions/{session_id}/attempts/{attempt_id}/renew"
    ]["post"]
    assert operation["requestBody"]["required"]
    assert set(operation["responses"]) == {
        "200",
        "400",
        "404",
        "409",
        "422",
        "500",
        "503",
    }
    assert (
        operation["responses"]["200"]["headers"]["Cache-Control"]["description"]
        == "no-store"
    )
    for code in ("400", "404", "409", "422", "500", "503"):
        assert operation["responses"][code]["content"]["application/json"][
            "schema"
        ] == {"$ref": "#/components/schemas/ErrorResponse"}
    request = schema["components"]["schemas"]["RenewLeaseRequest"]
    assert request["additionalProperties"] is False
    assert request["required"] == ["lease_token"]
    assert schema["components"]["schemas"]["RenewLeaseResponse"]["required"] == [
        "lease"
    ]
    token = uuid4()
    assert str(token) not in repr(RenewLeaseRequest(lease_token=token))
