"""Capture one existing custom demo Run without creating, retrying or deleting it."""

import argparse
import json
import math
import time
from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.custom_demo_evidence import Expectation, validate_custom
from scripts.demo import ROOT, Demo, check_custom_snapshot
from scripts.demo_containers import MARKER, verify_worker, worker_name

_MESSAGES = {
    "invalid_options": "Custom capture options are invalid.",
    "initial_refused": (
        "Capture needs an untouched custom Run without Workers, Attempts or samples."
    ),
    "output_unavailable": (
        "Evidence directory already exists or cannot be created; "
        "nothing was overwritten."
    ),
    "snapshot_changed": (
        "An observation changed the pinned Run, definition or Task identities."
    ),
    "capture_timeout": "Custom Run did not finish within the capture wait budget.",
    "startup_failed": (
        "Worker startup did not complete; existing containers were retained."
    ),
    "observation_failed": "The Run observation or evidence write failed.",
    "containers_failed": "Dedicated Worker container evidence could not be verified.",
    "container_timeout": (
        "Dedicated Workers did not exit within the container wait budget."
    ),
    "evidence_rejected": (
        "Retained evidence does not establish the requested custom execution."
    ),
    "interrupted": (
        "Capture was interrupted; existing evidence and resources were retained."
    ),
}


class CustomCaptureError(ValueError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(_MESSAGES[reason])


def _write(output: Path, name: str, value: object) -> None:
    with (output / name).open("x", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write("\n")


def _identity(snapshot: dict[str, Any]) -> tuple[object, ...]:
    run = snapshot["run"]
    return (
        *(
            run[field]
            for field in ("id", "workflow_version_id", "scenario", "definition")
        ),
        sorted(
            (task["id"], task["run_id"], task["task_key"]) for task in snapshot["tasks"]
        ),
    )


def _signature(snapshot: dict[str, Any]) -> tuple[object, ...]:
    return (
        snapshot["run"]["status"],
        sorted((task["id"], task["status"]) for task in snapshot["tasks"]),
        sorted(
            (
                attempt["id"],
                attempt["task_id"],
                attempt["attempt_number"],
                attempt["worker_session_id"],
                attempt["status"],
            )
            for attempt in snapshot["attempts"]
        ),
        sorted(
            (sample["invocation_id"], sample["attempt_id"], sample["phase"])
            for sample in snapshot["samples"]
            if sample["phase"] in ("START", "FINISH")
        ),
    )


def _container_records(demo: Demo, run_id: UUID) -> list[dict[str, Any]]:
    deadline = time.monotonic() + 15
    identities: dict[str, str] = {}
    while True:
        records = []
        for slot in ("a", "b"):
            found = json.loads(
                demo.command(
                    "inspect",
                    identities.get(slot, worker_name(run_id, slot)),
                    timeout=5,
                )
            )
            if not isinstance(found, list) or len(found) != 1:
                raise CustomCaptureError("containers_failed")
            info = found[0]
            identity = verify_worker(info, run_id, slot)
            if slot in identities and identity != identities[slot]:
                raise CustomCaptureError("containers_failed")
            identities[slot] = identity
            state = info["State"]
            records.append(
                {
                    "Id": identity,
                    "Name": info["Name"],
                    "Config": {
                        "Labels": {
                            label: info["Config"]["Labels"][label]
                            for label in (
                                "com.docker.compose.project",
                                "com.docker.compose.service",
                                MARKER,
                                "io.dwe.run",
                            )
                        }
                    },
                    "State": {
                        key: state[key]
                        for key in (
                            "Status",
                            "Running",
                            "ExitCode",
                            "Paused",
                            "Restarting",
                            "OOMKilled",
                        )
                        if key in state
                    },
                }
            )
        if time.monotonic() >= deadline:
            raise CustomCaptureError("container_timeout")
        if all(record["State"]["Status"] == "exited" for record in records):
            return records
        time.sleep(0.2)


def capture(
    demo: Demo,
    run_id: UUID,
    output: Path,
    *,
    expectation: Expectation = "any",
    start_workers: bool = False,
    timeout_seconds: float = 600,
) -> dict[str, object]:
    """Capture budgets are checked between I/O; in-flight operations may delay exit."""
    if (
        expectation not in ("parallel", "serial", "any")
        or type(start_workers) is not bool
        or type(timeout_seconds) not in (int, float)
        or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= 600
    ):
        raise CustomCaptureError("invalid_options")
    deadline = time.monotonic() + timeout_seconds
    if output.exists():
        raise CustomCaptureError("output_unavailable")
    try:
        last = demo.request(f"/demo/runs/{run_id}")
        check_custom_snapshot(last, run_id)
        if last["samples"] != []:
            raise CustomCaptureError("initial_refused")
        waiting = deepcopy(last)
        identity, signature = _identity(waiting), _signature(waiting)
    except Exception:
        raise CustomCaptureError("initial_refused") from None
    try:
        output.mkdir(parents=True, exist_ok=False)
    except OSError:
        raise CustomCaptureError("output_unavailable") from None
    reason = "observation_failed"
    try:
        _write(output, "waiting.json", waiting)
        if time.monotonic() >= deadline:
            raise CustomCaptureError("capture_timeout")
        if start_workers:
            reason = "startup_failed"
            demo.workers(run_id)
        else:
            port = demo.origin.rsplit(":", 1)[1]
            print(
                "Waiting for Workers. In another terminal run:\n"
                f"uv run python scripts/demo.py --port {port} "
                f"workers --run-id {run_id}",
                flush=True,
            )
        reason = "observation_failed"
        checkpoint = 0
        while True:
            if time.monotonic() >= deadline:
                raise CustomCaptureError("capture_timeout")
            candidate = demo.request(f"/demo/runs/{run_id}")
            if _identity(candidate) != identity:
                raise CustomCaptureError("snapshot_changed")
            observed = _signature(candidate)
            last = candidate
            if observed != signature:
                checkpoint += 1
                _write(output, f"checkpoint-{checkpoint:04d}.json", last)
                signature = observed
            if last["run"]["status"] in ("SUCCEEDED", "FAILED"):
                _write(output, "final.json", last)
                break
            time.sleep(0.5)
        if time.monotonic() >= deadline:
            raise CustomCaptureError("capture_timeout")
        reason = "containers_failed"
        containers = _container_records(demo, run_id)
        _write(output, "containers.json", containers)
        reason = "evidence_rejected"
        report = validate_custom(
            last, waiting=waiting, expectation=expectation, containers=containers
        )
        _write(output, "report.json", report)
        print(json.dumps(report, ensure_ascii=False), flush=True)
        return report
    except (Exception, KeyboardInterrupt) as error:
        failure = (
            error
            if isinstance(error, CustomCaptureError)
            else CustomCaptureError(
                "interrupted" if isinstance(error, KeyboardInterrupt) else reason
            )
        )
        # Diagnostics must not overwrite external files or replace the safe error
        # if the disk itself is unavailable. All prior snapshots remain intact.
        for name, value in (
            ("last.json", last),
            ("error.json", {"run_id": str(run_id), "reason": failure.reason}),
        ):
            try:
                _write(output, name, value)
            except (OSError, TypeError, ValueError):
                pass
        raise failure from None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Retain actual evidence for an existing custom Run"
    )
    parser.add_argument("--run-id", required=True, type=UUID)
    parser.add_argument(
        "--expect", choices=("any", "parallel", "serial"), default="any"
    )
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--start-workers", action="store_true")
    args = parser.parse_args()
    try:
        capture(
            Demo(args.port),
            args.run_id,
            ROOT / ".uv-cache" / "custom-acceptance" / str(args.run_id),
            expectation=args.expect,
            start_workers=args.start_workers,
        )
    except CustomCaptureError as error:
        parser.exit(
            1, f"Custom acceptance stopped: {error}\nNo automatic retry or cleanup.\n"
        )
    except Exception:
        parser.exit(
            1, "Custom acceptance could not start. Check the local demo connection.\n"
        )


if __name__ == "__main__":
    main()
