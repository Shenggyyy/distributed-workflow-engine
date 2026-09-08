"""Exercise committed HTTP completion and replay without exposing ownership tokens."""

import argparse
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def request(
    base: str,
    path: str,
    body: dict[str, object],
    *,
    expected: int = 200,
    method: str = "POST",
    key: str | None = None,
) -> dict[str, object]:
    headers = {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    outgoing = Request(
        base + path, method=method, data=json.dumps(body).encode(), headers=headers
    )
    try:
        response = urlopen(outgoing, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        if response.status != expected:
            raise RuntimeError(f"Expected HTTP {expected}, received {response.status}.")
        if path.endswith("/complete") and expected == 200:
            assert response.headers.get("Cache-Control") == "no-store"
            assert response.headers.get("Location") is None
        value = json.load(response)
        if not isinstance(value, dict):
            raise RuntimeError("Expected a JSON object.")
        return value


def error_code(value: dict[str, object]) -> object:
    error = value["error"]
    assert isinstance(error, dict)
    return error["code"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    base = str(parser.parse_args().base_url).rstrip("/")
    for failed in (False, True):
        version = request(
            base,
            "/workflows",
            {
                "name": "complete_http_" + uuid4().hex,
                "tasks": [
                    {"task_id": "A", "task_type": "demo.echo"},
                    {"task_id": "B", "task_type": "demo.echo"},
                ],
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
            {"worker_name": "completion_http", "max_concurrency": 1},
            method="PUT",
        )
        claim_path = f"/worker-sessions/{session}/claims"
        claim_body = {"run_id": run["run_id"], "request_id": str(uuid4())}
        claim = request(base, claim_path, claim_body)["claim"]
        assert isinstance(claim, dict)
        lease = claim["lease"]
        completion_path = (
            f"/worker-sessions/{session}/attempts/{lease['attempt_id']}/complete"
        )
        result = {
            "outcome": "FAILED" if failed else "SUCCEEDED",
            "error_code": "handler_failed" if failed else None,
        }
        body = {"lease_token": lease["lease_token"], "result": result}
        receipt = request(base, completion_path, body)
        assert receipt["result"] == result
        assert "lease_token" not in json.dumps(receipt)
        assert lease["lease_token"] not in json.dumps(receipt)
        assert request(base, completion_path, body) == receipt
        conflict = {
            **body,
            "result": {
                "outcome": "SUCCEEDED" if failed else "FAILED",
                "error_code": None if failed else "conflict",
            },
        }
        assert (
            error_code(request(base, completion_path, conflict, expected=409))
            == "completion_conflict"
        )
        assert (
            error_code(request(base, claim_path, claim_body, expected=409))
            == "claim_replay_unavailable"
        )
        next_claim = request(
            base, claim_path, {**claim_body, "request_id": str(uuid4())}
        )["claim"]
        assert (
            isinstance(next_claim, dict)
            and next_claim["attempt"]["id"] != lease["attempt_id"]
        )
        assert request(base, completion_path, body) == receipt
        # Settle the second simulated task as well; do not leave occupied capacity.
        next_lease = next_claim["lease"]
        request(
            base,
            f"/worker-sessions/{session}/attempts/{next_lease['attempt_id']}/complete",
            {
                "lease_token": next_lease["lease_token"],
                "result": {"outcome": "SUCCEEDED"},
            },
        )
    print(
        "HTTP completion checks passed: success, failure, replay, conflict "
        "and capacity reuse."
    )
    print(
        "No token returned or printed; outcomes were simulated, not handler execution."
    )


if __name__ == "__main__":
    main()
