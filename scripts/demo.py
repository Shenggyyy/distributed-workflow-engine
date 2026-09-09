"""Local-only, fixed demo operations. No remote command execution endpoint."""

import argparse
import json
import os
import secrets
import shutil
import subprocess
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "dwe-demo"
MARKER = "io.dwe.demo"


def worker_name(run_id: UUID, slot: str) -> str:
    if slot not in ("a", "b"):
        raise ValueError("Unknown demo Worker slot.")
    return f"dwe-demo-{run_id.hex}-{slot}"


def verify_worker(info: dict[str, Any], run_id: UUID, slot: str) -> str:
    labels = info.get("Config", {}).get("Labels", {})
    expected = {
        "com.docker.compose.project": PROJECT,
        "com.docker.compose.service": "worker",
        MARKER: "1",
        "io.dwe.run": str(run_id),
    }
    if info.get("Name") != "/" + worker_name(run_id, slot) or any(
        labels.get(k) != v for k, v in expected.items()
    ):
        raise ValueError("Refusing to operate on an unrecognized demo Worker.")
    identity = info.get("Id", "")
    if len(identity) != 64 or any(c not in "0123456789abcdef" for c in identity):
        raise ValueError("Invalid container identity.")
    return str(identity)


class Demo:
    def __init__(self, port: int) -> None:
        if not 1 <= port <= 65535:
            raise ValueError("Invalid demo port.")
        self.origin = f"http://127.0.0.1:{port}"
        self.env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("COMPOSE_", "DWE_"))
        }
        self.env["DWE_DEMO_PORT"] = str(port)
        # Finished one-off demo containers are deliberately retained for inspection.
        self.env["COMPOSE_IGNORE_ORPHANS"] = "true"
        docker = shutil.which("docker")
        if docker is None and os.name == "nt":
            candidate = Path(os.environ.get("LOCALAPPDATA", "")) / (
                "Programs/DockerDesktop/resources/bin/docker.exe"
            )
            if candidate.is_file():
                docker = str(candidate)
        if docker is None:
            raise ValueError("Docker CLI is unavailable. Start Docker Desktop.")
        self.docker = docker
        self.env["PATH"] = (
            str(Path(docker).parent) + os.pathsep + self.env.get("PATH", "")
        )
        context = json.loads(self.command("context", "inspect"))[0]
        host = self.env.get("DOCKER_HOST") or context["Endpoints"]["docker"]["Host"]
        if not host.startswith(("unix://", "npipe://")):
            raise ValueError("This demo requires a local Docker socket/context.")

    def command(self, *args: str, capture: bool = True) -> str:
        result = subprocess.run(
            [self.docker, *args],
            cwd=ROOT,
            env=self.env,
            check=True,
            text=True,
            encoding="utf-8",
            stdout=subprocess.PIPE if capture else None,
        )
        return result.stdout or ""

    def compose(self, *args: str) -> None:
        self.command(
            "compose",
            "--project-name",
            PROJECT,
            "--file",
            str(ROOT / "compose.demo.yaml"),
            *args,
            capture=False,
        )

    def request(self, path: str, body: dict[str, str] | None = None) -> Any:
        request = urllib.request.Request(
            self.origin + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=5) as response:
            return json.load(response)

    def up(self) -> None:
        path = ROOT / "secrets/demo_password.txt"
        path.parent.mkdir(exist_ok=True)
        if not path.exists():
            with path.open("x", encoding="utf-8") as secret:
                secret.write(secrets.token_urlsafe(32))
        self.compose("build", "api")
        self.compose("up", "-d", "--wait", "postgres")
        self.compose(
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "alembic",
            "api",
            "upgrade",
            "head",
        )
        self.compose("up", "-d", "--wait", "api", "scheduler")
        print(f"Open {self.origin}/demo/", flush=True)

    def worker(
        self, run_id: UUID, slot: str, concurrency: int = 1, *, cohort_size: int = 1
    ) -> None:
        if cohort_size not in (1, 2):
            raise ValueError("Demo cohort must contain one or two Workers.")
        name = worker_name(run_id, slot)
        self.compose(
            "run",
            "-d",
            "--no-deps",
            "--name",
            name,
            "--label",
            f"{MARKER}=1",
            "--label",
            f"io.dwe.run={run_id}",
            "-e",
            f"DWE_WORKER_NAME={name}",
            "-e",
            f"DWE_WORKER_CONCURRENCY={concurrency}",
            "worker",
            "worker",
            "--run-id",
            str(run_id),
            "--cohort-size",
            str(cohort_size),
        )

    def wait_ready(self, run_id: UUID, slots: tuple[str, ...]) -> None:
        """Observe scoped fresh registrations; never authorize or assign work."""
        names = {worker_name(run_id, slot) for slot in slots}
        deadline = time.monotonic() + 60
        while True:
            snapshot = self.request(f"/demo/runs/{run_id}")
            if UUID(snapshot["run"]["id"]) != run_id:
                raise ValueError("Readiness snapshot belongs to a different Run.")
            stamp = datetime.fromisoformat(snapshot["snapshot_at"])
            fresh = {
                worker["worker_name"]
                for worker in snapshot["workers"]
                if worker["status"] == "ACTIVE"
                and datetime.fromisoformat(worker["heartbeat_expires_at"]) > stamp
            }
            if time.monotonic() >= deadline:
                raise ValueError("Demo Workers did not register within 60 seconds.")
            if names <= fresh:
                print("Demo cohort registered with fresh heartbeats.", flush=True)
                return
            time.sleep(0.2)

    def run(self, scenario: str) -> UUID:
        result = self.request("/demo/runs", {"scenario": scenario})
        run_id = UUID(result["run_id"])
        print(f"Run ID: {run_id}\nOpen {self.origin}/demo/?run={run_id}", flush=True)
        cohort_size = 2 if scenario == "distribution" else 1
        self.worker(
            run_id, "a", 2 if scenario == "parallel" else 1, cohort_size=cohort_size
        )
        if scenario == "distribution":
            self.worker(run_id, "b", cohort_size=cohort_size)
            self.wait_ready(run_id, ("a", "b"))
        if scenario == "recovery":
            print(
                f"Inject failure: uv run python scripts/demo.py fail --run-id {run_id}"
            )
        return run_id

    def fail(self, run_id: UUID) -> None:
        info = json.loads(self.command("inspect", worker_name(run_id, "a")))[0]
        identity = verify_worker(info, run_id, "a")
        if not info["State"]["Running"]:
            raise ValueError("Demo Worker A is not running.")
        deadline = time.monotonic() + 30
        while True:
            snapshot = self.request(f"/demo/runs/{run_id}")
            if snapshot["run"]["scenario"] != "recovery":
                raise ValueError("Fault injection requires a recovery scenario.")
            owner = next(
                (
                    w["id"]
                    for w in snapshot["workers"]
                    if w["worker_name"] == worker_name(run_id, "a")
                ),
                None,
            )
            task_id = next(t["id"] for t in snapshot["tasks"] if t["task_key"] == "A")
            attempts = {
                a["id"]
                for a in snapshot["attempts"]
                if a["worker_session_id"] == owner
                and a["status"] == "RUNNING"
                and a["task_id"] == task_id
                and a["attempt_number"] == 1
            }
            samples = [s for s in snapshot["samples"] if s["attempt_id"] in attempts]
            if any(s["phase"] == "START" for s in samples) and not any(
                s["phase"] == "FINISH" for s in samples
            ):
                break
            if (
                snapshot["run"]["status"] in ("SUCCEEDED", "FAILED")
                or time.monotonic() >= deadline
            ):
                raise ValueError(
                    "No executing recovery Handler; create a fresh recovery Run."
                )
            time.sleep(0.2)
        # Container ID avoids a name-replacement race. SIGKILL also kills children.
        self.command("kill", "--signal", "KILL", identity, capture=False)
        print("Worker A killed after a real START. Starting replacement B.", flush=True)
        self.worker(run_id, "b")

    def down(self) -> None:
        ids = self.command(
            "ps",
            "-q",
            "--filter",
            f"label={MARKER}=1",
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
        )
        for identity in ids.split():
            info = json.loads(self.command("inspect", identity))[0]
            run_id = UUID(info["Config"]["Labels"]["io.dwe.run"])
            slot = info["Name"].rsplit("-", 1)[-1]
            verified = verify_worker(info, run_id, slot)
            self.command("stop", "--time", "10", verified, capture=False)
        self.compose("stop", "scheduler", "api", "postgres")
        print("Demo stopped. Containers, database volume and Run history retained.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--port", type=int, default=int(os.getenv("DWE_DEMO_PORT", "18080"))
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("up")
    commands.add_parser("down")
    run = commands.add_parser("run")
    run.add_argument("scenario", choices=["parallel", "distribution", "recovery"])
    fail = commands.add_parser("fail")
    fail.add_argument("--run-id", type=UUID, required=True)
    args = parser.parse_args()
    try:
        demo = Demo(args.port)
        if args.command == "run":
            demo.run(args.scenario)
        elif args.command == "fail":
            demo.fail(args.run_id)
        elif args.command == "up":
            demo.up()
        else:
            demo.down()
    except (ValueError, OSError, subprocess.CalledProcessError):
        parser.exit(
            1,
            "Demo command failed. Check Docker/local port and command scope. "
            "No automatic POST retry; inspect /demo/runs for any created Run.\n",
        )


if __name__ == "__main__":
    main()
