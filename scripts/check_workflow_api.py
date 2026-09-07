"""Smoke-test the real HTTP API; creates two versions in the target database."""

import argparse
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def request(
    base_url: str,
    path: str,
    *,
    body: dict[str, object] | None = None,
    expected: int = 200,
) -> tuple[dict[str, object], str | None]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {} if data is None else {"Content-Type": "application/json"}
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base_url = str(args.base_url).rstrip("/")
    body = json.loads(
        (Path(__file__).resolve().parents[1] / "examples" / "diamond.json").read_text(
            encoding="utf-8"
        )
    )
    body["name"] = "http_smoke_" + uuid4().hex
    first, location = request(base_url, "/workflows", body=body, expected=201)
    assert location == f"/workflow-versions/{first['id']}"
    assert first["version_number"] == 1
    # The response includes defaults omitted by the example root task.
    expected_definition = {
        **body,
        "tasks": [{"depends_on": [], **task} for task in body["tasks"]],
    }
    assert first["definition"] == expected_definition
    stored, _ = request(base_url, location)
    assert stored == first
    second, _ = request(base_url, "/workflows", body=body, expected=201)
    assert second["version_number"] == 2
    assert second["workflow_id"] == first["workflow_id"]
    identity, _ = request(base_url, f"/workflows/{body['name']}")
    assert identity["id"] == first["workflow_id"]
    latest, _ = request(base_url, f"/workflows/{first['workflow_id']}/versions/latest")
    assert latest == second
    historical, _ = request(base_url, f"/workflows/{first['workflow_id']}/versions/1")
    assert historical == first
    missing, _ = request(base_url, f"/workflow-versions/{uuid4()}", expected=404)
    assert missing["error"] == {
        "code": "version_not_found",
        "message": "Workflow version was not found.",
        "details": [],
    }
    invalid, _ = request(
        base_url, "/workflows", body={"name": "invalid", "tasks": []}, expected=422
    )
    assert isinstance(invalid["error"], dict)
    assert invalid["error"]["code"] == "invalid_request"
    print("HTTP workflow checks passed: publication, history, latest, 404 and 422.")


if __name__ == "__main__":
    main()
