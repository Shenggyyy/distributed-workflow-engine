"""Smoke-test Worker HTTP contracts; creates one retained session in the DB."""

import argparse
import json
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def request(
    base_url: str,
    path: str,
    *,
    method: str,
    body: dict[str, object],
    expected: int = 200,
    key: str | None = None,
) -> dict[str, object]:
    headers = {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    outgoing = Request(
        base_url + path,
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
        assert response.headers.get("Location") is None
        result = json.load(response)
        if not isinstance(result, dict):
            raise RuntimeError("Expected a JSON object.")
        return result


def assert_error(body: dict[str, object], code: str) -> None:
    assert isinstance(body["error"], dict)
    assert body["error"]["code"] == code


def stamp(body: dict[str, object], field: str) -> datetime:
    value = body[field]
    assert isinstance(value, str)
    result = datetime.fromisoformat(value)
    assert result.tzinfo is not None
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base_url = str(args.base_url).rstrip("/")
    session_id = str(uuid4())
    path = f"/worker-sessions/{session_id}"
    body: dict[str, object] = {
        "worker_name": "http_smoke_" + uuid4().hex,
        "max_concurrency": 2,
    }
    original = request(base_url, path, method="PUT", body=body)
    assert original["session"] == {"id": session_id, **body, "status": "ACTIVE"}
    assert stamp(original, "created_at") == stamp(original, "last_heartbeat_at")
    assert stamp(original, "heartbeat_expires_at") > stamp(
        original, "last_heartbeat_at"
    )
    assert request(base_url, path, method="PUT", body=body) == original
    current = request(base_url, path + "/heartbeat", method="POST", body={})
    assert current["session"] == original["session"]
    assert current["created_at"] == original["created_at"]
    assert stamp(current, "last_heartbeat_at") >= stamp(original, "last_heartbeat_at")
    assert stamp(current, "heartbeat_expires_at") >= stamp(
        original, "heartbeat_expires_at"
    )
    assert request(base_url, path, method="PUT", body=body) == current
    assert_error(
        request(
            base_url,
            path,
            method="PUT",
            body={**body, "max_concurrency": 3},
            expected=409,
        ),
        "worker_registration_conflict",
    )
    assert_error(
        request(
            base_url,
            f"/worker-sessions/{uuid4()}/heartbeat",
            method="POST",
            body={},
            expected=404,
        ),
        "worker_not_found",
    )
    assert_error(
        request(
            base_url,
            path + "/heartbeat",
            method="POST",
            body={"timeout_seconds": 60},
            expected=422,
        ),
        "invalid_request",
    )
    for suffix, method, payload in (("", "PUT", body), ("/heartbeat", "POST", {})):
        assert_error(
            request(
                base_url,
                path + suffix,
                method=method,
                body=payload,
                key="unsupported",
                expected=400,
            ),
            "idempotency_not_supported",
        )
    print(
        "HTTP worker checks passed: registration, replay, heartbeat, "
        "409, 404, 422 and 400."
    )


if __name__ == "__main__":
    main()
