"""Synthetic unit-test snapshots only; never served by the demo or used as evidence."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from scripts.demo import worker_name

from workflow_engine.demo.scenarios import diamond_definition


def stamp(seconds: float) -> str:
    return (datetime(2026, 9, 9, tzinfo=UTC) + timedelta(seconds=seconds)).isoformat()


def branch_snapshot(*, swap_owners: bool = False) -> dict[str, Any]:
    run_id = uuid4()
    tasks = {
        key: {
            "id": str(uuid4()),
            "run_id": str(run_id),
            "task_key": key,
            "status": "SUCCEEDED"
            if key == "A"
            else "PENDING"
            if key == "D"
            else "RUNNING",
            "created_at": stamp(0),
        }
        for key in "ABCD"
    }
    workers = [
        {
            "id": str(uuid4()),
            "worker_name": worker_name(run_id, slot),
            "max_concurrency": 1,
            "status": "ACTIVE",
            "created_at": stamp(0),
            "last_heartbeat_at": stamp(9),
            "heartbeat_expires_at": stamp(15),
        }
        for slot in ("a", "b")
    ]
    attempts, samples = [], []
    for key in "ABC":
        owner = 0 if key == "A" else int((key == "C") != swap_owners)
        acquired = 0.1 if key == "A" else 6.4 if key == "B" else 6.5
        attempt_id, invocation_id = str(uuid4()), str(uuid4())
        attempts.append(
            {
                "id": attempt_id,
                "task_id": tasks[key]["id"],
                "attempt_number": 1,
                "status": tasks[key]["status"],
                "worker_session_id": workers[owner]["id"],
                "acquired_at": stamp(acquired),
                "last_renewed_at": stamp(5 if key == "A" else 9),
                "lease_expires_at": stamp(11 if key == "A" else 15),
                "accepted_at": stamp(6.3) if key == "A" else None,
                "scheduled_at": None,
                "available_at": None,
            }
        )
        points = (
            [("START", 0.2), ("PULSE", 5.7), ("FINISH", 6.2)]
            if key == "A"
            else [("START", 6.6 if key == "B" else 6.7), ("PULSE", 7.5), ("PULSE", 9.5)]
        )
        for sequence, (phase, seconds) in enumerate(points):
            samples.append(
                {
                    "invocation_id": invocation_id,
                    "attempt_id": attempt_id,
                    "clock_domain": (
                        "linux:12345678-1234-1234-1234-123456789012:"
                        "monotonic-offset:0:0"
                    ),
                    "sequence": sequence,
                    "phase": phase,
                    "monotonic_ns": str(int(seconds * 1_000_000_000)),
                    "recorded_at": stamp(seconds + 0.05),
                }
            )
    return {
        "snapshot_at": stamp(10),
        "run": {
            "id": str(run_id),
            "workflow_version_id": str(uuid4()),
            "scenario": "recovery",
            "status": "RUNNING",
            "created_at": stamp(0),
            "definition": diamond_definition("recovery").model_dump(mode="json"),
        },
        "tasks": list(tasks.values()),
        "attempts": attempts,
        "workers": workers,
        "samples": samples,
    }
