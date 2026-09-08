"""Run independent Scheduler and Worker processes through a complete diamond DAG."""

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
    parser.add_argument("--database-env-file", help="Required for a host Scheduler.")
    parser.add_argument("--scheduler-container", action="store_true")
    parser.add_argument("--automatic-scheduler", action="store_true")
    args = parser.parse_args()
    if not args.scheduler_container and not args.database_env_file:
        parser.error("Provide --database-env-file or --scheduler-container.")
    base = str(args.base_url).rstrip("/")
    version = request(
        base,
        "/workflows",
        {
            "name": "dag_smoke_" + uuid4().hex,
            "tasks": [
                {"task_id": "A", "task_type": "demo.echo"},
                {"task_id": "B", "task_type": "demo.echo", "depends_on": ["A"]},
                {"task_id": "C", "task_type": "demo.echo", "depends_on": ["A"]},
                {"task_id": "D", "task_type": "demo.echo", "depends_on": ["B", "C"]},
            ],
        },
    )
    run = request(
        base, "/runs", {"workflow_version_id": version["id"]}, key=uuid4().hex
    )
    run_id = str(run["run_id"])
    run_arguments = [] if args.automatic_scheduler else ["--run-id", run_id]
    container_name = "dwe-scheduler-check-" + uuid4().hex
    scheduler: subprocess.Popen[bytes] | None = None
    container_started = False
    try:
        if args.scheduler_container:
            container_started = True
            started = subprocess.run(
                [
                    "docker",
                    "compose",
                    "run",
                    "-d",
                    "--rm",
                    "--no-deps",
                    "--name",
                    container_name,
                    "scheduler",
                    "scheduler",
                    *run_arguments,
                ],
                capture_output=True,
                timeout=30,
                check=False,
            )
            if started.returncode != 0:
                raise RuntimeError("Scheduler container could not start.")
        else:
            creation_flags = 0
            if sys.platform == "win32":
                creation_flags = subprocess.CREATE_NO_WINDOW
            scheduler = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "workflow_engine",
                    "scheduler",
                    *run_arguments,
                    "--env-file",
                    str(args.database_env_file),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creation_flags,
            )
        worker = subprocess.run(
            [
                sys.executable,
                "-m",
                "workflow_engine",
                "worker",
                "--run-id",
                run_id,
                "--max-tasks",
                "4",
            ],
            env={**os.environ, "DWE_WORKER_API_URL": base},
            capture_output=True,
            timeout=90,
            check=False,
        )
        if worker.returncode != 0:
            raise RuntimeError("Worker failed to execute the diamond DAG.")
        snapshot = request(base, f"/runs/{run_id}/tasks")
        tasks = snapshot["tasks"]
        assert isinstance(tasks, list)
        assert {task["task_key"]: task["status"] for task in tasks} == {
            key: "SUCCEEDED" for key in ("A", "B", "C", "D")
        }
        if scheduler is not None and scheduler.poll() is not None:
            raise RuntimeError("Scheduler exited unexpectedly.")
        print(
            "DAG execution passed: independent Scheduler/Worker processes "
            "completed A -> B/C -> D."
        )
        print("Run aggregation remains M5; this check verifies all Task outcomes.")
    finally:
        if scheduler is not None:
            scheduler.terminate()
            try:
                scheduler.wait(timeout=15)
            except subprocess.TimeoutExpired:
                scheduler.kill()
                scheduler.wait(timeout=5)
        if container_started:
            subprocess.run(
                ["docker", "stop", "--time", "10", container_name],
                capture_output=True,
                timeout=20,
                check=False,
            )


if __name__ == "__main__":
    main()
