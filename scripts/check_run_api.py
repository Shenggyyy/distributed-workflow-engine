"""Smoke-test Run HTTP contracts; creates two versions and one run in the DB."""

import argparse
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4


def request(
    base_url: str,
    path: str,
    *,
    body: dict[str, object] | None = None,
    key: str | None = None,
    expected: int = 200,
) -> tuple[dict[str, object], str | None]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {} if data is None else {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    outgoing = Request(base_url + path, data=data, headers=headers)
    try:
        response = urlopen(outgoing, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        if response.status != expected:
            raise RuntimeError(f"Expected HTTP {expected}, received {response.status}.")
        result = json.load(response)
        if not isinstance(result, dict):
            raise RuntimeError("Expected a JSON object.")
        return result, response.headers.get("Location")


def assert_error(result: tuple[dict[str, object], str | None], code: str) -> None:
    body, location = result
    assert location is None
    assert isinstance(body["error"], dict)
    assert body["error"]["code"] == code


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base_url = str(args.base_url).rstrip("/")
    definition = json.loads(
        (Path(__file__).resolve().parents[1] / "examples" / "diamond.json").read_text(
            encoding="utf-8"
        )
    )
    definition["name"] = "run_http_smoke_" + uuid4().hex
    version, _ = request(base_url, "/workflows", body=definition, expected=201)
    body = {"workflow_version_id": version["id"]}
    key = uuid4().hex
    receipt, location = request(base_url, "/runs", body=body, key=key, expected=201)
    assert set(receipt) == {"run_id", "workflow_version_id"}
    assert receipt["workflow_version_id"] == version["id"]
    predecessor = UUID(int=UUID(str(receipt["run_id"])).int - 1)
    page, _ = request(base_url, f"/runs?after={predecessor}&limit=1&ready_only=true")
    assert page["run_ids"] == [receipt["run_id"]]
    assert location == f"/runs/{receipt['run_id']}"
    replay = request(base_url, "/runs", body=body, key=key, expected=201)
    assert replay == (receipt, location)
    run, _ = request(base_url, location)
    assert run["id"] == receipt["run_id"]
    assert run["workflow_version_id"] == version["id"]
    assert run["status"] == "RUNNING"
    snapshot, _ = request(base_url, location + "/tasks")
    # No scheduler or other writer changes this uniquely named run in this check.
    assert snapshot["run"] == run
    tasks = snapshot["tasks"]
    assert isinstance(tasks, list)
    assert [task["task_key"] for task in tasks] == ["A", "B", "C", "D"]
    assert [task["status"] for task in tasks] == [
        "READY",
        "PENDING",
        "PENDING",
        "PENDING",
    ]
    assert all(task["run_id"] == run["id"] for task in tasks)
    second, _ = request(base_url, "/workflows", body=definition, expected=201)
    assert_error(
        request(
            base_url,
            "/runs",
            body={"workflow_version_id": second["id"]},
            key=key,
            expected=409,
        ),
        "idempotency_conflict",
    )
    for suffix in ("", "/tasks"):
        assert_error(
            request(base_url, f"/runs/{uuid4()}{suffix}", expected=404),
            "run_not_found",
        )
    assert_error(
        request(
            base_url,
            "/runs",
            body={"workflow_version_id": str(uuid4())},
            key=uuid4().hex,
            expected=404,
        ),
        "version_not_found",
    )
    assert_error(request(base_url, "/runs", body=body, expected=422), "invalid_request")
    assert request(base_url, "/runs", body=body, key=key, expected=201) == replay
    print("HTTP run checks passed: creation, replay, discovery, queries and errors.")


if __name__ == "__main__":
    main()
