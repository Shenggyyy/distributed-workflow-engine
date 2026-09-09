"""Synthetic unit-test snapshots only; never served by the demo or used as evidence."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from scripts.demo import worker_name

from workflow_engine.demo.scenarios import Scenario, diamond_definition


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


def evidence_case(
    scenario: Scenario, *, swap_owners: bool = False
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any] | None]:
    branches = branch_snapshot(swap_owners=swap_owners)
    branches["run"]["scenario"] = scenario
    branches["run"]["definition"] = diamond_definition(scenario).model_dump(mode="json")
    if scenario == "parallel":
        branches["workers"] = [branches["workers"][0]]
        branches["workers"][0]["max_concurrency"] = 2
        for attempt in branches["attempts"]:
            attempt["worker_session_id"] = branches["workers"][0]["id"]
    root = deepcopy(branches)
    root["snapshot_at"] = stamp(1)
    for task in root["tasks"]:
        task["status"] = "RUNNING" if task["task_key"] == "A" else "PENDING"
    root["attempts"] = [
        {
            **root["attempts"][0],
            "status": "RUNNING",
            "accepted_at": None,
            "last_renewed_at": stamp(0.5),
            "lease_expires_at": stamp(6.5),
        }
    ]
    root["samples"] = [root["samples"][0]]
    for worker in root["workers"]:
        worker.update(last_heartbeat_at=stamp(0.5), heartbeat_expires_at=stamp(6.5))

    def append_sample(
        data: dict[str, Any], attempt: dict[str, Any], phase: str, seconds: float
    ) -> None:
        prior = [s for s in data["samples"] if s["attempt_id"] == attempt["id"]]
        data["samples"].append(
            {
                "invocation_id": prior[0]["invocation_id"] if prior else str(uuid4()),
                "attempt_id": attempt["id"],
                "clock_domain": data["samples"][0]["clock_domain"],
                "sequence": len(prior),
                "phase": phase,
                "monotonic_ns": str(int(seconds * 1_000_000_000)),
                "recorded_at": stamp(seconds + 0.05),
            }
        )

    waiting = deepcopy(branches)
    waiting["snapshot_at"] = stamp(16.1)
    waiting["tasks"][1]["status"] = "SUCCEEDED"
    healthy = waiting["attempts"][1]
    healthy.update(
        status="SUCCEEDED",
        accepted_at=stamp(14.7),
        last_renewed_at=stamp(14),
        lease_expires_at=stamp(20),
    )
    append_sample(waiting, healthy, "FINISH", 14.6)
    for worker in waiting["workers"]:
        worker.update(last_heartbeat_at=stamp(16), heartbeat_expires_at=stamp(22))
    old = waiting["attempts"][2]
    if scenario == "recovery":
        waiting["tasks"][2]["status"] = "RETRY_WAIT"
        old.update(status="LOST", scheduled_at=stamp(15.1), available_at=stamp(22.1))
        for worker in waiting["workers"]:
            if worker["id"] == old["worker_session_id"]:
                worker.update(
                    status="LOST",
                    last_heartbeat_at=stamp(9),
                    heartbeat_expires_at=stamp(15),
                )
    else:
        append_sample(waiting, old, "PULSE", 16)
        old.update(last_renewed_at=stamp(16), lease_expires_at=stamp(22))
    checkpoints = {"root": root, "branches": branches, "join_wait": deepcopy(waiting)}
    final = deepcopy(waiting)
    final["run"]["status"] = "SUCCEEDED"
    for task in final["tasks"]:
        task["status"] = "SUCCEEDED"
    receipt = None
    if scenario == "recovery":
        checkpoints.update(retry=deepcopy(waiting), fault_before=deepcopy(branches))
        new = {
            **final["attempts"][2],
            "id": str(uuid4()),
            "attempt_number": 2,
            "worker_session_id": healthy["worker_session_id"],
            "status": "SUCCEEDED",
            "acquired_at": stamp(22.2),
            "last_renewed_at": stamp(42),
            "lease_expires_at": stamp(48),
            "accepted_at": stamp(42.5),
            "scheduled_at": None,
            "available_at": None,
        }
        final["attempts"].append(new)
        append_sample(final, new, "START", 22.4)
        append_sample(final, new, "PULSE", 30)
        append_sample(final, new, "FINISH", 42.4)
        killed = next(
            w for w in branches["workers"] if w["id"] == old["worker_session_id"]
        )
        receipt = {
            "action": "SIGKILL",
            "docker_acknowledged": True,
            "run_id": branches["run"]["id"],
            "task_key": "C",
            "task_id": old["task_id"],
            "attempt_id": old["id"],
            "worker_session_id": killed["id"],
            "worker_name": killed["worker_name"],
            "slot": killed["worker_name"][-1],
            "container_id": "c" * 64,
            "snapshot_at": branches["snapshot_at"],
            "invocation_id": next(
                s["invocation_id"]
                for s in branches["samples"]
                if s["attempt_id"] == old["id"]
            ),
        }
        join_claim, join_start = 42.6, 42.8
    else:
        new = final["attempts"][2]
        new.update(
            status="SUCCEEDED",
            accepted_at=stamp(20.8),
            last_renewed_at=stamp(20),
            lease_expires_at=stamp(26),
        )
        append_sample(final, new, "FINISH", 20.7)
        join_claim, join_start = 21.0, 21.2
    join = {
        **new,
        "id": str(uuid4()),
        "task_id": final["tasks"][3]["id"],
        "attempt_number": 1,
        "worker_session_id": healthy["worker_session_id"],
        "acquired_at": stamp(join_claim),
        "last_renewed_at": stamp(join_start + 2),
        "lease_expires_at": stamp(join_start + 8),
        "accepted_at": stamp(join_start + 3.1),
    }
    final["attempts"].append(join)
    append_sample(final, join, "START", join_start)
    append_sample(final, join, "PULSE", join_start + 2)
    append_sample(final, join, "FINISH", join_start + 3)
    final["snapshot_at"] = stamp(join_start + 4)
    for worker in final["workers"]:
        if worker["status"] == "ACTIVE":
            worker.update(
                last_heartbeat_at=stamp(join_start + 3),
                heartbeat_expires_at=stamp(join_start + 9),
            )
    return final, checkpoints, receipt
