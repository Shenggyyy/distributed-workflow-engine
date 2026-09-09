"""Custom previews validate bounded input without publishing or connecting to SQL."""

import asyncio
import json
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette.types import Message, Scope

from workflow_engine.api.app import create_app
from workflow_engine.api.dependencies import get_engine
from workflow_engine.config import Settings
from workflow_engine.demo.api import create_demo_app
from workflow_engine.demo.handlers import ObservedHandler, registry
from workflow_engine.demo.scenarios import Scenario, diamond_tasks

VALIDATE = "/demo/custom/validate"
CATALOG = "/demo/custom/catalog"
BODY_LIMIT = 16384
POLICY = {
    "max_attempts": 2,
    "timeout_seconds": 90,
    "initial_backoff_ms": 10000,
    "max_backoff_ms": 10000,
}


@pytest.fixture
def client() -> Iterator[TestClient]:
    app = create_demo_app(Settings())

    def forbid_database_dependency() -> None:
        raise AssertionError("Preview/catalog must not request a database engine.")

    app.dependency_overrides[get_engine] = forbid_database_dependency
    with TestClient(app) as value:
        assert app.state.database_engine is None
        yield value


def task(key: str, *parents: str, kind: str = "demo.observe") -> dict[str, Any]:
    return {"task_id": key, "task_type": kind, "depends_on": list(parents)}


def definition(*tasks: dict[str, Any]) -> dict[str, Any]:
    return {"name": "Custom_preview", "tasks": list(tasks or (task("Start"),))}


def test_catalog_matches_registered_handlers_and_real_templates(
    client: TestClient,
) -> None:
    response = client.get(CATALOG)
    assert response.status_code == 200
    catalog = response.json()
    entries = registry(Settings()).registrations
    expected: dict[str, float] = {}
    for entry in entries:
        assert isinstance(entry.handler, ObservedHandler)
        expected[entry.task_type] = entry.handler.seconds
    assert {item["task_type"]: item["seconds"] for item in catalog["handlers"]} == (
        expected
    )
    assert len(catalog["handlers"]) == len(expected) == 8
    assert catalog["limits"] == {
        "max_tasks": 12,
        "max_body_bytes": BODY_LIMIT,
        "max_attempts": 2,
        "timeout_seconds": 90,
        "worker_count": 2,
        "slots_per_worker": 1,
    }
    templates = {item["scenario"]: item["definition"] for item in catalog["templates"]}
    assert len(catalog["templates"]) == len(templates) == 3
    scenarios: tuple[Scenario, ...] = ("parallel", "distribution", "recovery")
    for scenario in scenarios:
        saved = templates[scenario]
        assert saved["schema_version"] == 2
        assert saved["tasks"] == [
            item.model_dump(mode="json") for item in diamond_tasks(scenario)
        ]
        preview = client.post(VALIDATE, json=saved)
        assert preview.status_code == 200
        assert preview.json()["definition"] == saved
    assert "run_id" not in response.text


def test_preview_normalizes_without_changing_arbitrary_graph_or_array_order(
    client: TestClient,
) -> None:
    body = definition(
        task("Publish_report", "Validate_rows", "Summarize", kind="demo.join"),
        task("Summarize", "Load_west"),
        task("Load_east", kind="demo.diamond.a"),
        task("Validate_rows", "Load_west", "Load_east"),
        task("Load_west"),
    )
    del body["tasks"][2]["depends_on"]
    response = client.post(VALIDATE, json=body)
    assert response.status_code == 200
    result = response.json()
    canonical = result["definition"]
    assert canonical["name"] == body["name"]
    assert canonical["schema_version"] == 2
    assert canonical["tasks"] == [
        {**item, "depends_on": item.get("depends_on", []), "execution": POLICY}
        for item in body["tasks"]
    ]
    assert [set(layer) for layer in result["layers"]] == [
        {"Load_east", "Load_west"},
        {"Summarize", "Validate_rows"},
        {"Publish_report"},
    ]
    assert result["limits"] == client.get(CATALOG).json()["limits"]
    replay = client.post(VALIDATE, json=canonical)
    assert replay.status_code == 200 and replay.json() == result
    assert not {"run_id", "workflow_version_id", "status", "attempts"} & result.keys()
    assert "Location" not in response.headers


@pytest.mark.parametrize(
    ("body", "issue"),
    [
        (definition(task("Same"), task("Same")), "duplicate_task_id"),
        (definition(task("Start", "Start")), "self_dependency"),
        (definition(task("Start", "Missing")), "unknown_dependency"),
        (
            definition(task("Start"), task("End", "Start", "Start")),
            "duplicate_dependency",
        ),
        (
            definition(task("First", "Second"), task("Second", "First")),
            "cycle_detected",
        ),
        (definition(task("A" * 65)), "string_too_long"),
        ({"name": "Custom", "tasks": []}, "too_short"),
        (
            definition(task("Start", kind="private_handler_sentinel")),
            "demo_handler_not_allowed",
        ),
        (
            definition(*(task(f"Task_{index}") for index in range(13))),
            "demo_task_limit",
        ),
    ],
)
def test_core_and_demo_validation_errors_keep_codes_without_values(
    client: TestClient, body: dict[str, Any], issue: str
) -> None:
    response = client.post(VALIDATE, json=body)
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "invalid_request"
    assert issue in {item["type"] for item in error["details"]}
    assert all(
        set(item) == {"location", "type"} and item["location"][0] == "body"
        for item in error["details"]
    )
    assert "private_handler_sentinel" not in response.text
    assert "Location" not in response.headers


@pytest.mark.parametrize(
    ("field", "value"),
    [("max_attempts", 3), ("timeout_seconds", 30), ("max_backoff_ms", 11000)],
)
def test_explicit_policy_differences_are_rejected_not_overwritten(
    client: TestClient, field: str, value: int
) -> None:
    body = definition(task("Start"))
    body["schema_version"] = 2
    body["tasks"][0]["execution"] = {**POLICY, field: value}
    response = client.post(VALIDATE, json=body)
    assert response.status_code == 422
    assert "demo_execution_policy" in {
        item["type"] for item in response.json()["error"]["details"]
    }


def test_schema_one_explicit_policy_still_obeys_the_core_contract(
    client: TestClient,
) -> None:
    body = definition(task("Start"))
    body["schema_version"] = 1
    body["tasks"][0]["execution"] = POLICY
    response = client.post(VALIDATE, json=body)
    assert response.status_code == 422
    assert "execution_requires_schema_v2" in {
        item["type"] for item in response.json()["error"]["details"]
    }


@pytest.mark.parametrize(
    "body",
    [
        b'{"private_sentinel":',
        b"\xff",
        b'{"name":"private_sentinel","name":"second","tasks":[]}',
        b'{"tasks":[{"task_id":"A","task_id":"B"}]}',
        b'{"private_sentinel":NaN}',
        b'{"private_sentinel":Infinity}',
        b'{"private_sentinel":-Infinity}',
        b'{"private_sentinel":1e999}',
        b"[" * 2000 + b"0" + b"]" * 2000,
    ],
    ids=[
        "malformed",
        "invalid-utf8",
        "duplicate-root-key",
        "duplicate-nested-key",
        "nan",
        "infinity",
        "negative-infinity",
        "overflowed-float",
        "deep-nesting",
    ],
)
def test_invalid_json_encoding_constants_keys_and_depth_are_safe(
    client: TestClient, body: bytes
) -> None:
    response = client.post(
        VALIDATE, content=body, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert response.json()["error"]["details"] == [
        {"location": ["body"], "type": "json_invalid"}
    ]
    assert "private_sentinel" not in response.text


@pytest.mark.parametrize(("depth", "issue"), [(16, "model_type"), (17, "json_invalid")])
def test_json_container_depth_limit_has_a_deterministic_boundary(
    client: TestClient, depth: int, issue: str
) -> None:
    # At the depth boundary decoding succeeds; the core still rejects a root list.
    response = client.post(
        VALIDATE,
        content=b"[" * depth + b"0" + b"]" * depth,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert response.json()["error"]["details"] == [
        {"location": ["body"], "type": issue}
    ]


@pytest.mark.parametrize(
    "content_type", [None, "text/plain", "application/octet-stream"]
)
def test_json_media_type_is_required(
    client: TestClient, content_type: str | None
) -> None:
    response = client.post(
        VALIDATE,
        content=json.dumps(definition()).encode(),
        headers={} if content_type is None else {"Content-Type": content_type},
    )
    assert response.status_code == 415
    assert response.json()["error"]["code"] == "demo_json_required"


def test_actual_body_byte_limit_accepts_exact_boundary_and_counts_utf8(
    client: TestClient,
) -> None:
    body = json.dumps(definition()).encode()
    exact = body + b" " * (BODY_LIMIT - len(body))
    assert (
        client.post(
            VALIDATE,
            content=exact,
            headers={"Content-Type": "application/json; charset=utf-8"},
        ).status_code
        == 200
    )
    # Fewer than 16KiB characters still exceed the byte limit on the wire.
    oversized = json.dumps({"private": "汉" * 6000}, ensure_ascii=False).encode()
    assert len(oversized.decode()) < BODY_LIMIT < len(oversized)
    response = client.post(
        VALIDATE, content=oversized, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "demo_body_too_large"
    assert "汉" not in response.text


@pytest.mark.parametrize("declared_length", [None, b"1"])
def test_streaming_limit_cannot_be_bypassed_by_content_length(
    declared_length: bytes | None,
) -> None:
    app = create_demo_app(Settings())
    headers = [(b"content-type", b"application/json")]
    if declared_length is not None:
        headers.append((b"content-length", declared_length))
    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": VALIDATE,
        "raw_path": VALIDATE.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "server": ("testserver", 80),
        "client": ("testclient", 50000),
    }
    chunks = iter([b" " * 8192, b" " * 8192, b" "])
    responses: list[Message] = []

    async def receive() -> Message:
        chunk = next(chunks)
        return {"type": "http.request", "body": chunk, "more_body": len(chunk) > 1}

    async def send(message: Message) -> None:
        responses.append(message)

    asyncio.run(app(scope, receive, send))
    assert (
        next(item for item in responses if item["type"] == "http.response.start")[
            "status"
        ]
        == 413
    )
    body = b"".join(
        item.get("body", b"")
        for item in responses
        if item["type"] == "http.response.body"
    )
    assert json.loads(body)["error"]["code"] == "demo_body_too_large"


def test_routes_remain_demo_only_and_openapi_request_schema_resolves(
    client: TestClient,
) -> None:
    with TestClient(create_app(Settings())) as core:
        assert core.get(CATALOG).status_code == 404
        assert core.post(VALIDATE, json=definition()).status_code == 404
        assert not any(
            path.startswith("/demo")
            for path in core.get("/openapi.json").json()["paths"]
        )
    schema = client.get("/openapi.json").json()
    operation = schema["paths"][VALIDATE]["post"]
    request = operation["requestBody"]
    assert request["required"] is True
    reference = request["content"]["application/json"]["schema"]["$ref"]
    target = schema
    for part in reference.removeprefix("#/").split("/"):
        target = target[part]
    assert {"name", "tasks"} <= target["properties"].keys()
    assert "post" not in schema["paths"].get("/demo/custom/runs", {})
