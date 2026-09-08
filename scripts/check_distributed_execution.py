"""Execute a fresh six-task DAG using two Workers and two Schedulers."""

import argparse
import json
import os
import subprocess
import sys
import time
from urllib.request import Request, urlopen
from uuid import uuid4


def request(base: str, path: str, body: object = None) -> dict[str, object]:
    headers = {"Content-Type": "application/json"}
    if path == "/runs" and body is not None:
        headers["Idempotency-Key"] = uuid4().hex
    with urlopen(
        Request(
            base + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
        ),
        timeout=10,
    ) as response:
        value = json.load(response)
        assert isinstance(value, dict)
        return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--container", action="store_true")
    parser.add_argument("--database-env-file")
    args = parser.parse_args()
    if not args.container and not args.database_env_file:
        parser.error("Host Schedulers require --database-env-file.")
    base = str(args.base_url).rstrip("/")
    version = request(
        base,
        "/workflows",
        {
            "name": "distributed_" + uuid4().hex,
            "tasks": [
                *(
                    {"task_id": key, "task_type": "demo.echo"}
                    for key in ("A", "B", "C", "D")
                ),
                {
                    "task_id": "E",
                    "task_type": "demo.echo",
                    "depends_on": ["A", "B", "C", "D"],
                },
                {"task_id": "F", "task_type": "demo.echo", "depends_on": ["E"]},
            ],
        },
    )
    run = request(base, "/runs", {"workflow_version_id": version["id"]})
    processes: list[subprocess.Popen[bytes]] = []
    containers: list[str] = []
    environment = {
        **os.environ,
        "DWE_WORKER_API_URL": base,
        "DWE_WORKER_CONCURRENCY": "2",
    }
    creation_flags = 0
    if sys.platform == "win32":
        creation_flags = subprocess.CREATE_NO_WINDOW
    try:
        for role in ("scheduler", "scheduler", "worker", "worker"):
            arguments = [role]
            if role == "worker":
                arguments += ["--run-id", str(run["run_id"]), "--max-tasks", "3"]
            elif not args.container:
                arguments += ["--env-file", str(args.database_env_file)]
            if args.container:
                name = "dwe-distributed-" + uuid4().hex
                containers.append(name)
                command = [
                    "docker",
                    "compose",
                    "run",
                    "--rm",
                    "--no-deps",
                    "-T",
                    "--name",
                    name,
                    role,
                    *arguments,
                ]
            else:
                command = [sys.executable, "-m", "workflow_engine", *arguments]
            processes.append(
                subprocess.Popen(
                    command,
                    env=environment,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=creation_flags,
                )
            )
        for worker in processes[2:]:
            if worker.wait(timeout=60) != 0:
                raise RuntimeError("Distributed Worker failed.")
        assert all(scheduler.poll() is None for scheduler in processes[:2])
        tasks = request(base, f"/runs/{run['run_id']}/tasks")["tasks"]
        assert isinstance(tasks, list)
        assert {task["task_key"]: task["status"] for task in tasks} == {
            key: "SUCCEEDED" for key in "ABCDEF"
        }
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if request(base, f"/runs/{run['run_id']}")["status"] == "SUCCEEDED":
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("Distributed Schedulers did not settle the Run.")
        print(
            "Distributed execution passed: two Workers, two Schedulers, "
            "six successful Tasks and a SUCCEEDED Run."
        )
    finally:
        try:
            if containers:
                subprocess.run(
                    ["docker", "stop", "--time", "10", *containers],
                    capture_output=True,
                    timeout=60,
                    check=False,
                )
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    main()
