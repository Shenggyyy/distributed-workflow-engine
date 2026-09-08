"""Run the installed Worker CLI against the API, optionally inside Compose."""

import argparse
import json
import os
import subprocess
import sys
from urllib.request import Request, urlopen
from uuid import uuid4


def request(
    base: str, path: str, body: object = None, *, key: str | None = None
) -> dict[str, object]:
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    data = json.dumps(body).encode() if body is not None else None
    with urlopen(
        Request(base + path, data=data, headers=headers), timeout=10
    ) as response:
        value = json.load(response)
        assert isinstance(value, dict)
        return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument(
        "--automatic",
        action="store_true",
        help="Requires an otherwise idle disposable queue.",
    )
    parser.add_argument(
        "--container",
        action="store_true",
        help="Run the Worker using docker compose run.",
    )
    args = parser.parse_args()
    base = str(args.base_url).rstrip("/")
    if args.automatic and request(base, "/runs?limit=1&ready_only=true")["run_ids"]:
        raise RuntimeError(
            "Automatic smoke requires an empty READY queue in a disposable database."
        )
    version = request(
        base,
        "/workflows",
        {
            "name": "worker_smoke_" + uuid4().hex,
            "tasks": [
                {"task_id": "A", "task_type": "demo.echo"},
                {"task_id": "B", "task_type": "demo.fail"},
            ],
        },
    )
    run = request(
        base, "/runs", {"workflow_version_id": version["id"]}, key=uuid4().hex
    )
    selection = [] if args.automatic else ["--run-id", str(run["run_id"])]
    arguments = ["worker", *selection, "--max-tasks", "2"]
    if args.container:
        command = [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "worker",
            *arguments,
        ]
    else:
        command = [sys.executable, "-m", "workflow_engine", *arguments]
    environment = {**os.environ, "DWE_WORKER_API_URL": base}
    result = subprocess.run(
        command,
        env=environment,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Worker CLI execution failed; inspect service health and configuration."
        )
    assert "2 confirmed completions" in result.stdout
    snapshot = request(base, f"/runs/{run['run_id']}/tasks")
    tasks = snapshot["tasks"]
    assert isinstance(tasks, list)
    assert {task["task_key"]: task["status"] for task in tasks} == {
        "A": "SUCCEEDED",
        "B": "FAILED",
    }
    print(
        "Worker execution checks passed: real handlers, success/failure "
        "and persisted Task outcomes."
    )
    print("This check uses independent roots; Run aggregation remains pending.")


if __name__ == "__main__":
    main()
