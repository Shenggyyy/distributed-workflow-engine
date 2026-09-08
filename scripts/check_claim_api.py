"""Check claims over real HTTP; creates retained ownership but executes no handler."""

import argparse
import json
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4


def request(
    base: str,
    path: str,
    body: dict[str, object],
    *,
    method: str = "POST",
    expected: int = 200,
    key: str | None = None,
) -> dict[str, object]:
    headers = {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    outgoing = Request(
        base + path,
        method=method,
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
    )
    try:
        response = urlopen(outgoing, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        if response.status != expected:
            # Never dump a response that could include a lease token.
            raise RuntimeError(f"Expected HTTP {expected}, received {response.status}.")
        if path.endswith("/claims"):
            assert response.headers.get("Location") is None
            if expected == 200:
                assert response.headers.get("Cache-Control") == "no-store"
        result = json.load(response)
        if not isinstance(result, dict):
            raise RuntimeError("Expected a JSON object.")
        return result


def assert_error(body: dict[str, object], code: str) -> None:
    assert isinstance(body["error"], dict) and body["error"]["code"] == code
    assert "claim" not in body


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base = str(args.base_url).rstrip("/")
    definition = json.loads(
        (Path(__file__).resolve().parents[1] / "examples" / "diamond.json").read_text(
            encoding="utf-8"
        )
    )
    definition["name"] = "claim_http_" + uuid4().hex
    version = request(base, "/workflows", definition, expected=201)
    run = request(
        base,
        "/runs",
        {"workflow_version_id": version["id"]},
        key=uuid4().hex,
        expected=201,
    )
    session = str(uuid4())
    request(
        base,
        f"/worker-sessions/{session}",
        {"worker_name": "claim_http_worker", "max_concurrency": 2},
        method="PUT",
    )
    path = f"/worker-sessions/{session}/claims"
    body = {"run_id": run["run_id"], "request_id": str(uuid4())}
    first = request(base, path, body)
    assert first["worker_session_id"] == session
    assert (
        first["run_id"] == body["run_id"] and first["request_id"] == body["request_id"]
    )
    claim = first["claim"]
    assert isinstance(claim, dict)
    assert claim["task"]["task_key"] == "A" and claim["attempt"]["attempt_number"] == 1
    assert claim["task"]["status"] == claim["attempt"]["status"] == "RUNNING"
    assert claim["workflow_version_id"] == version["id"]
    lease = claim["lease"]
    assert (
        lease["worker_session_id"] == session
        and lease["attempt_id"] == claim["attempt"]["id"]
    )
    UUID(lease["lease_token"])
    assert datetime.fromisoformat(lease["lease_expires_at"]) > datetime.fromisoformat(
        lease["acquired_at"]
    )
    assert request(base, path, body) == first
    empty_body = {**body, "request_id": str(uuid4())}
    empty = request(base, path, empty_body)
    assert empty["claim"] is None and request(base, path, empty_body) == empty
    assert_error(
        request(base, path, {**body, "run_id": str(uuid4())}, expected=409),
        "claim_request_conflict",
    )
    assert_error(
        request(base, f"/worker-sessions/{uuid4()}/claims", body, expected=404),
        "worker_not_found",
    )
    assert_error(
        request(
            base,
            path,
            {**body, "request_id": str(uuid4()), "run_id": str(uuid4())},
            expected=404,
        ),
        "run_not_found",
    )
    assert_error(
        request(base, path, {**body, "lease_seconds": 100}, expected=422),
        "invalid_request",
    )
    assert_error(
        request(base, path, body, key="unsupported", expected=400),
        "idempotency_not_supported",
    )
    print(
        "HTTP claim checks passed: committed grant, replay, no-work, "
        "409, 404, 422 and 400."
    )
    print(
        "No handler executed; the disposable Run and Worker retain one occupied slot."
    )


if __name__ == "__main__":
    main()
