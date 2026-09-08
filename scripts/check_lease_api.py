"""Exercise claim, lease renewal and current replay over HTTP without logging tokens."""

import argparse
import json
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


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
            raise RuntimeError(f"Expected HTTP {expected}, received {response.status}.")
        if path.endswith(("/claims", "/renew")):
            assert response.headers.get("Location") is None
            if expected == 200:
                assert response.headers.get("Cache-Control") == "no-store"
        value = json.load(response)
        if not isinstance(value, dict):
            raise RuntimeError("Expected a JSON object.")
        return value


def error(value: dict[str, object], code: str) -> None:
    assert isinstance(value["error"], dict) and value["error"]["code"] == code
    assert "lease" not in value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base = str(args.base_url).rstrip("/")
    version = request(
        base,
        "/workflows",
        {
            "name": "lease_http_" + uuid4().hex,
            "tasks": [{"task_id": "A", "task_type": "demo.echo"}],
        },
        expected=201,
    )
    run = request(
        base,
        "/runs",
        {"workflow_version_id": version["id"]},
        expected=201,
        key=uuid4().hex,
    )
    session = str(uuid4())
    request(
        base,
        f"/worker-sessions/{session}",
        {"worker_name": "lease_http_worker", "max_concurrency": 1},
        method="PUT",
    )
    claim_path = f"/worker-sessions/{session}/claims"
    claim_body = {"run_id": run["run_id"], "request_id": str(uuid4())}
    claim = request(base, claim_path, claim_body)["claim"]
    assert isinstance(claim, dict)
    lease = claim["lease"]
    uri = f"/worker-sessions/{session}/attempts/{lease['attempt_id']}/renew"
    payload = {"lease_token": lease["lease_token"]}
    original = dict(lease)
    for _ in range(2):
        renewed = request(base, uri, payload)["lease"]
        assert isinstance(renewed, dict)
        for name in ("attempt_id", "worker_session_id", "lease_token", "acquired_at"):
            assert renewed[name] == original[name]
        for name in ("last_renewed_at", "lease_expires_at"):
            assert isinstance(renewed[name], str)
            assert datetime.fromisoformat(renewed[name]) >= datetime.fromisoformat(
                lease[name]
            )
        lease = renewed
    replay = request(base, claim_path, claim_body)["claim"]
    assert isinstance(replay, dict) and replay["lease"] == lease
    error(
        request(base, uri, {"lease_token": str(uuid4())}, expected=409),
        "lease_ownership_mismatch",
    )
    error(
        request(
            base,
            f"/worker-sessions/{uuid4()}/attempts/{original['attempt_id']}/renew",
            payload,
            expected=409,
        ),
        "lease_ownership_mismatch",
    )
    error(
        request(
            base,
            f"/worker-sessions/{session}/attempts/{uuid4()}/renew",
            payload,
            expected=404,
        ),
        "lease_not_found",
    )
    error(
        request(base, uri, {**payload, "lease_seconds": 100}, expected=422),
        "invalid_request",
    )
    error(
        request(base, uri, payload, key="unsupported", expected=400),
        "idempotency_not_supported",
    )
    print(
        "HTTP lease checks passed: renewal, retry, claim replay, 409, 404, 422 and 400."
    )
    print("Tokens stayed in memory; no handler executed or capacity released.")


if __name__ == "__main__":
    main()
