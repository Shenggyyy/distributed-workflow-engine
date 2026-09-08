"""Run independent Scheduler and Worker processes through a complete diamond DAG."""

import argparse
import json
import os
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def request(
    base: str,
    path: str,
    body: object = None,
    *,
    key: str | None = None,
    method: str | None = None,
) -> dict[str, object]:
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    with urlopen(
        Request(
            base + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
            method=method,
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
    parser.add_argument(
        "--retry-failure",
        action="store_true",
        help="Execute one failing Task until its three-Attempt budget ends.",
    )
    parser.add_argument(
        "--abandon-claim",
        action="store_true",
        help="Abandon one allocation and verify automatic recovery.",
    )
    parser.add_argument(
        "--failed-branch",
        action="store_true",
        help="Fail one branch while independent work succeeds.",
    )
    args = parser.parse_args()
    if sum((args.retry_failure, args.abandon_claim, args.failed_branch)) > 1:
        parser.error("Choose only one failure scenario.")
    if not args.scheduler_container and not args.database_env_file:
        parser.error("Provide --database-env-file or --scheduler-container.")
    base = str(args.base_url).rstrip("/")
    retry_tasks = [
        {
            "task_id": "A",
            "task_type": "demo.fail",
            "execution": {
                "max_attempts": 3,
                "initial_backoff_ms": 200,
                "max_backoff_ms": 1000,
            },
        }
    ]
    if args.abandon_claim:
        retry_tasks = [
            {
                "task_id": "A",
                "task_type": "demo.echo",
                "execution": {
                    "max_attempts": 2,
                    "timeout_seconds": 2,
                    "initial_backoff_ms": 200,
                    "max_backoff_ms": 1000,
                },
            }
        ]
    if args.failed_branch:
        retry_tasks = [
            {"task_id": "A", "task_type": "demo.fail"},
            {"task_id": "B", "task_type": "demo.echo"},
            {"task_id": "C", "task_type": "demo.echo", "depends_on": ["A"]},
            {"task_id": "D", "task_type": "demo.echo", "depends_on": ["C"]},
        ]
    version = request(
        base,
        "/workflows",
        {
            "name": "dag_smoke_" + uuid4().hex,
            "schema_version": 2 if args.retry_failure or args.abandon_claim else 1,
            "tasks": retry_tasks
            if args.retry_failure or args.abandon_claim or args.failed_branch
            else [
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
    abandoned: dict[str, object] | None = None
    ghost = str(uuid4())
    if args.abandon_claim:
        request(
            base,
            f"/worker-sessions/{ghost}",
            {"worker_name": "abandoned", "max_concurrency": 1},
            method="PUT",
        )
        value = request(
            base,
            f"/worker-sessions/{ghost}/claims",
            {"run_id": run_id, "request_id": str(uuid4())},
        )["claim"]
        assert isinstance(value, dict)
        abandoned = value
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
                "1"
                if args.abandon_claim
                else "2"
                if args.failed_branch
                else "3"
                if args.retry_failure
                else "4",
            ],
            env={**os.environ, "DWE_WORKER_API_URL": base},
            capture_output=True,
            timeout=90,
            check=False,
        )
        if worker.returncode != 0:
            raise RuntimeError("Worker failed to execute the diamond DAG.")
        final_status = (
            "FAILED" if args.failed_branch or args.retry_failure else "SUCCEEDED"
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if request(base, f"/runs/{run_id}")["status"] == final_status:
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("Scheduler did not settle the Run.")
        snapshot = request(base, f"/runs/{run_id}/tasks")
        tasks = snapshot["tasks"]
        assert isinstance(tasks, list)
        expected = (
            {"A": "FAILED", "B": "SUCCEEDED", "C": "SKIPPED", "D": "SKIPPED"}
            if args.failed_branch
            else {"A": "FAILED"}
            if args.retry_failure
            else {"A": "SUCCEEDED"}
            if args.abandon_claim
            else {key: "SUCCEEDED" for key in ("A", "B", "C", "D")}
        )
        assert {task["task_key"]: task["status"] for task in tasks} == expected
        if abandoned is not None:
            lease = abandoned["lease"]
            assert isinstance(lease, dict)
            try:
                request(
                    base,
                    f"/worker-sessions/{ghost}/attempts/{lease['attempt_id']}/complete",
                    {
                        "lease_token": lease["lease_token"],
                        "result": {"outcome": "SUCCEEDED"},
                    },
                )
            except HTTPError as error:
                assert error.code == 409
                assert json.load(error)["error"]["code"] == "completion_inactive"
            else:
                raise RuntimeError("Stale completion was accepted.")
        if scheduler is not None and scheduler.poll() is not None:
            raise RuntimeError("Scheduler exited unexpectedly.")
        print(
            "Failure propagation passed: A failed; B succeeded; C/D skipped."
            if args.failed_branch
            else "Recovery passed: abandoned Attempt replaced; stale result rejected."
            if args.abandon_claim
            else "Retry execution passed: three failed Attempts exhausted the budget."
            if args.retry_failure
            else "DAG execution passed: independent Scheduler/Worker processes "
            "completed A -> B/C -> D."
        )
        print(f"Run {run_id}: {final_status}")
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
