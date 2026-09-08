"""Worker HTTP validation and schema contracts without database access."""

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
        raise AssertionError("Invalid Worker requests must not connect.")

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
        {"worker_name": "private/value", "max_concurrency": 2},
        {"worker_name": "worker"},
        {"worker_name": "worker", "max_concurrency": True},
        {"worker_name": "worker", "max_concurrency": "2"},
        {"worker_name": "worker", "max_concurrency": 2.0},
        {"worker_name": "worker", "max_concurrency": 0},
        {"worker_name": "worker", "max_concurrency": 2147483648},
        {"worker_name": "worker", "max_concurrency": 2, "status": "ACTIVE"},
        {"worker_name": "worker", "max_concurrency": 2, "timeout_seconds": 60},
    ],
)
def test_registration_rejects_invalid_body(
    client: TestClient, body: dict[str, object]
) -> None:
    result = client.put(f"/worker-sessions/{uuid4()}", json=body)
    assert result.status_code == 422
    assert result.json()["error"]["code"] == "invalid_request"
    assert "private/value" not in result.text


@pytest.mark.parametrize(
    "content", [None, "null", "[]", '{"now":"private-value"}', '{"timeout_seconds":60}']
)
def test_heartbeat_requires_empty_object(
    client: TestClient, content: str | None
) -> None:
    result = client.post(
        f"/worker-sessions/{uuid4()}/heartbeat",
        content=content,
        headers={"Content-Type": "application/json"},
    )
    assert result.status_code == 422
    assert result.json()["error"]["code"] == "invalid_request"
    assert "private-value" not in result.text


@pytest.mark.parametrize("heartbeat", [False, True])
@pytest.mark.parametrize("key", ["", "private-value"])
def test_unsupported_header_is_rejected(
    client: TestClient, heartbeat: bool, key: str
) -> None:
    path = f"/worker-sessions/{uuid4()}"
    result = client.request(
        "POST" if heartbeat else "PUT",
        path + ("/heartbeat" if heartbeat else ""),
        json={} if heartbeat else {"worker_name": "worker", "max_concurrency": 2},
        headers={"Idempotency-Key": key},
    )
    assert result.status_code == 400
    assert result.json()["error"]["code"] == "idempotency_not_supported"
    assert "private-value" not in result.text


@pytest.mark.parametrize("heartbeat", [False, True])
def test_invalid_session_id(client: TestClient, heartbeat: bool) -> None:
    result = client.request(
        "POST" if heartbeat else "PUT",
        "/worker-sessions/private-value" + ("/heartbeat" if heartbeat else ""),
        json={} if heartbeat else {"worker_name": "worker", "max_concurrency": 2},
    )
    assert result.status_code == 422
    assert "private-value" not in result.text


@pytest.mark.parametrize("heartbeat", [False, True])
def test_unconfigured_database(heartbeat: bool) -> None:
    with TestClient(create_app(Settings())) as client:
        result = client.request(
            "POST" if heartbeat else "PUT",
            f"/worker-sessions/{uuid4()}" + ("/heartbeat" if heartbeat else ""),
            json={} if heartbeat else {"worker_name": "worker", "max_concurrency": 2},
        )
        assert result.status_code == 503
        assert result.json()["error"]["code"] == "database_not_configured"
        assert client.get("/health/live").status_code == 200


def test_openapi_worker_contract(client: TestClient) -> None:
    schema = client.get("/openapi.json").json()
    for path, method in (
        ("/worker-sessions/{session_id}", "put"),
        ("/worker-sessions/{session_id}/heartbeat", "post"),
    ):
        operation = schema["paths"][path][method]
        assert operation["requestBody"]["required"]
        assert "200" in operation["responses"]
        assert "201" not in operation["responses"]
        assert "headers" not in operation["responses"]["200"]
        for code in ("400", "409", "422", "500", "503"):
            assert operation["responses"][code]["content"]["application/json"][
                "schema"
            ] == {"$ref": "#/components/schemas/ErrorResponse"}
    assert (
        "404"
        in schema["paths"]["/worker-sessions/{session_id}/heartbeat"]["post"][
            "responses"
        ]
    )
    for name in ("RegisterWorkerRequest", "WorkerHeartbeatRequest"):
        assert schema["components"]["schemas"][name]["additionalProperties"] is False
    assert schema["components"]["schemas"]["WorkerHeartbeatRequest"]["properties"] == {}
    assert "/worker-sessions/{session_id}/expire" not in schema["paths"]
