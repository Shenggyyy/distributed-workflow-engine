"""Local-only, fixed demo operations. No remote command execution endpoint."""

import argparse
import json
import os
import secrets
import shutil
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

# Preserve the documented `python scripts/demo.py` entry point after extraction.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.demo_containers import MARKER as MARKER
from scripts.demo_containers import PROJECT as PROJECT
from scripts.demo_containers import verify_worker as verify_worker
from scripts.demo_containers import worker_name as worker_name
from scripts.demo_fault import FaultRefused, inject_branch_fault

ROOT = Path(__file__).resolve().parents[1]


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

    def command(
        self, *args: str, capture: bool = True, timeout: float | None = None
    ) -> str:
        result = subprocess.run(
            [self.docker, *args],
            cwd=ROOT,
            env=self.env,
            check=True,
            text=True,
            encoding="utf-8",
            stdout=subprocess.PIPE if capture else None,
            timeout=timeout,
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
        cohort_size = 2 if scenario in ("distribution", "recovery") else 1
        self.worker(
            run_id, "a", 2 if scenario == "parallel" else 1, cohort_size=cohort_size
        )
        if cohort_size == 2:
            self.worker(run_id, "b", cohort_size=cohort_size)
            self.wait_ready(run_id, ("a", "b"))
        if scenario == "recovery":
            print(
                "Inject failure: uv run python scripts/demo.py "
                f"--port {self.origin.rsplit(':', 1)[1]} fail --run-id {run_id}",
                flush=True,
            )
        return run_id

    def fail(self, run_id: UUID, *, output: Path | None = None) -> None:
        target = inject_branch_fault(
            self,
            run_id,
            output if output is not None else ROOT / ".uv-cache/demo-acceptance",
        )
        print(
            f"Stopped C Attempt #1 {target.attempt_id} on {target.worker_name} "
            "after real overlapping branch execution. "
            "The existing surviving Worker can claim a new Attempt after recovery; "
            "no replacement container was started.",
            flush=True,
        )

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
    except FaultRefused as error:
        parser.exit(1, f"Fault refused: {error} No automatic retry.\n")
    except (ValueError, OSError, subprocess.CalledProcessError):
        parser.exit(
            1,
            "Demo command failed. Check Docker/local port and command scope. "
            "No automatic POST retry; inspect /demo/runs for any created Run.\n",
        )


if __name__ == "__main__":
    main()
