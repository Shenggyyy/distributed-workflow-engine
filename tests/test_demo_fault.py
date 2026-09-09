"""Branch fault injection must resolve real ownership and fail closed before kill."""

import json
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

import pytest
from scripts.demo import MARKER, worker_name
from scripts.demo_fault import FaultRefused, inject_branch_fault, select_fault_target

from tests.demo_fixtures import branch_snapshot, stamp


@pytest.mark.parametrize("swapped", [False, True])
def test_fault_target_follows_actual_branch_owner(swapped: bool) -> None:
    snapshot = branch_snapshot(swap_owners=swapped)
    run_id = UUID(snapshot["run"]["id"])
    target = select_fault_target(snapshot, run_id)
    assert target is not None
    assert target.task_key == "C"
    assert target.slot == ("a" if swapped else "b")
    assert target.worker_name == worker_name(run_id, target.slot)
    branch = next(t for t in snapshot["tasks"] if t["task_key"] == "C")
    attempt = next(a for a in snapshot["attempts"] if a["task_id"] == branch["id"])
    assert target.attempt_id == UUID(attempt["id"])
    assert target.worker_session_id == UUID(attempt["worker_session_id"])


def test_pre_branch_execution_is_waiting_not_a_root_fault() -> None:
    snapshot = branch_snapshot()
    for task in snapshot["tasks"]:
        task["status"] = "RUNNING" if task["task_key"] == "A" else "PENDING"
    snapshot["attempts"] = [
        {**snapshot["attempts"][0], "status": "RUNNING", "accepted_at": None}
    ]
    snapshot["samples"] = []
    assert select_fault_target(snapshot, UUID(snapshot["run"]["id"])) is None


@pytest.mark.parametrize(
    "case",
    [
        "scenario",
        "run",
        "task-run",
        "definition",
        "root",
        "join",
        "completed",
        "retry",
        "owner",
        "same-owner",
        "worker-name",
        "capacity",
        "heartbeat",
        "lease",
        "finish",
        "invocation",
        "stale-sample",
        "sibling-completed",
        "different-clock",
        "future-sample",
        "late-window",
    ],
)
def test_invalid_or_finished_targets_are_refused(case: str) -> None:
    snapshot = branch_snapshot()
    run_id = UUID(snapshot["run"]["id"])
    task = next(t for t in snapshot["tasks"] if t["task_key"] == "C")
    attempt = next(a for a in snapshot["attempts"] if a["task_id"] == task["id"])
    worker = next(
        w for w in snapshot["workers"] if w["id"] == attempt["worker_session_id"]
    )
    samples = [s for s in snapshot["samples"] if s["attempt_id"] == attempt["id"]]
    if case == "scenario":
        snapshot["run"]["scenario"] = "distribution"
    elif case == "run":
        run_id = uuid4()
    elif case == "task-run":
        task["run_id"] = str(uuid4())
    elif case == "definition":
        snapshot["run"]["definition"]["tasks"][3]["depends_on"] = ["A"]
    elif case == "root":
        snapshot["tasks"][0]["status"] = "RUNNING"
    elif case == "join":
        snapshot["tasks"][3]["status"] = "READY"
    elif case == "completed":
        task["status"] = attempt["status"] = "SUCCEEDED"
        attempt["accepted_at"] = stamp(9.9)
    elif case == "retry":
        attempt["attempt_number"] = 2
    elif case == "owner":
        attempt["worker_session_id"] = str(uuid4())
    elif case == "same-owner":
        attempt["worker_session_id"] = snapshot["attempts"][1]["worker_session_id"]
    elif case == "worker-name":
        worker["worker_name"] = "ordinary-worker"
    elif case == "capacity":
        worker["max_concurrency"] = 2
    elif case == "heartbeat":
        worker["heartbeat_expires_at"] = snapshot["snapshot_at"]
    elif case == "lease":
        attempt["lease_expires_at"] = snapshot["snapshot_at"]
    elif case == "finish":
        samples[-1]["phase"] = "FINISH"
    elif case == "invocation":
        samples[-1]["invocation_id"] = str(uuid4())
    elif case == "stale-sample":
        for sample in samples:
            sample["recorded_at"] = stamp(7.9)
    elif case == "sibling-completed":
        snapshot["tasks"][1]["status"] = snapshot["attempts"][1]["status"] = "SUCCEEDED"
    elif case == "different-clock":
        for sample in samples:
            sample["clock_domain"] = "other-kernel"
    elif case == "future-sample":
        samples[-1]["recorded_at"] = stamp(10.1)
    elif case == "late-window":
        samples[-1]["monotonic_ns"] = "22000000000"
    with pytest.raises(FaultRefused):
        select_fault_target(snapshot, run_id)


@pytest.mark.parametrize("last", [6.64, 7.5])
def test_waits_for_one_second_of_actual_common_branch_overlap(last: float) -> None:
    snapshot = branch_snapshot()
    attempt = snapshot["attempts"][1]
    samples = [s for s in snapshot["samples"] if s["attempt_id"] == attempt["id"]]
    samples[1]["monotonic_ns"] = "6620000000"
    samples[2]["monotonic_ns"] = str(int(last * 1_000_000_000))
    assert select_fault_target(snapshot, UUID(snapshot["run"]["id"])) is None


def control_for(snapshot: dict[str, Any]) -> tuple[Mock, dict[str, Any]]:
    run_id = UUID(snapshot["run"]["id"])
    target = select_fault_target(snapshot, run_id)
    assert target is not None
    info = {
        "Name": "/" + target.worker_name,
        "Id": "b" * 64,
        "State": {"Running": True, "Paused": False},
        "Config": {
            "Labels": {
                "com.docker.compose.project": "dwe-demo",
                "com.docker.compose.service": "worker",
                MARKER: "1",
                "io.dwe.run": str(run_id),
            }
        },
    }
    control = Mock()
    control.request.return_value = snapshot
    control.command.side_effect = [json.dumps([info]), json.dumps([info]), ""]
    return control, info


def test_kill_is_by_verified_immutable_identity_and_evidence_is_retained(
    tmp_path: Path,
) -> None:
    snapshot = branch_snapshot()
    run_id = UUID(snapshot["run"]["id"])
    control, info = control_for(snapshot)
    target = inject_branch_fault(control, run_id, tmp_path)
    assert target.task_key == "C"
    kills = [c for c in control.command.call_args_list if c.args[0] == "kill"]
    assert len(kills) == 1 and kills[0].args == ("kill", "--signal", "KILL", info["Id"])
    assert (
        json.loads((tmp_path / f"{run_id}-fault-before.json").read_text()) == snapshot
    )
    receipt = json.loads((tmp_path / f"{run_id}-fault.json").read_text())
    assert receipt["container_id"] == info["Id"]
    assert receipt["attempt_id"] == str(target.attempt_id)
    control.worker.assert_not_called()


@pytest.mark.parametrize(
    "case",
    [
        "completed-after-inspect",
        "changed-container",
        "paused",
        "stopped",
        "existing-record",
        "existing-ack",
    ],
)
def test_revalidation_refuses_without_killing_or_overwriting(
    tmp_path: Path, case: str
) -> None:
    snapshot = branch_snapshot()
    run_id = UUID(snapshot["run"]["id"])
    control, info = control_for(snapshot)
    if case == "completed-after-inspect":
        changed = deepcopy(snapshot)
        changed["tasks"][2]["status"] = changed["attempts"][2]["status"] = "SUCCEEDED"
        control.request.side_effect = [snapshot, changed]
    elif case == "changed-container":
        control.command.side_effect = [
            json.dumps([info]),
            json.dumps([{**info, "Id": "c" * 64}]),
        ]
    elif case in ("paused", "stopped"):
        info["State"] = {"Running": case != "stopped", "Paused": case == "paused"}
        control.command.side_effect = [json.dumps([info])]
    elif case == "existing-record":
        (tmp_path / f"{run_id}-fault-before.json").write_text("retained evidence")
    else:
        (tmp_path / f"{run_id}-fault.json").write_text("retained acknowledgment")
    with pytest.raises(FaultRefused):
        inject_branch_fault(control, run_id, tmp_path)
    assert all(c.args[0] != "kill" for c in control.command.call_args_list)
    if case == "existing-record":
        assert (
            tmp_path / f"{run_id}-fault-before.json"
        ).read_text() == "retained evidence"
    if case == "existing-ack":
        assert (
            tmp_path / f"{run_id}-fault.json"
        ).read_text() == "retained acknowledgment"


def test_failed_kill_is_not_retried_or_reported_as_acknowledged(tmp_path: Path) -> None:
    snapshot = branch_snapshot()
    run_id = UUID(snapshot["run"]["id"])
    control, info = control_for(snapshot)
    control.command.side_effect = [
        json.dumps([info]),
        json.dumps([info]),
        subprocess.CalledProcessError(1, "docker kill"),
    ]
    with pytest.raises(FaultRefused, match="uncertain"):
        inject_branch_fault(control, run_id, tmp_path)
    assert (tmp_path / f"{run_id}-fault-before.json").is_file()
    assert not (tmp_path / f"{run_id}-fault.json").exists()
    assert sum(c.args[0] == "kill" for c in control.command.call_args_list) == 1


@pytest.mark.parametrize("delay_at", ["inspect", "write"])
def test_stale_final_inspect_or_evidence_write_prevents_kill(
    tmp_path: Path, delay_at: str
) -> None:
    snapshot = branch_snapshot()
    run_id = UUID(snapshot["run"]["id"])
    control, info = control_for(snapshot)
    clock = [0.0]
    inspections = [0]
    original_dump = json.dump

    def command(*args: str, **kwargs: Any) -> str:
        inspections[0] += 1
        if inspections[0] == 2 and delay_at == "inspect":
            clock[0] = 3.0
        return json.dumps([info])

    def dump(*args: Any, **kwargs: Any) -> None:
        original_dump(*args, **kwargs)
        if delay_at == "write":
            clock[0] = 3.0

    control.command.side_effect = command
    with (
        patch("scripts.demo_fault.time.monotonic", side_effect=lambda: clock[0]),
        patch("scripts.demo_fault.json.dump", side_effect=dump),
        pytest.raises(FaultRefused),
    ):
        inject_branch_fault(control, run_id, tmp_path)
    assert all(c.args[0] != "kill" for c in control.command.call_args_list)
    assert not (tmp_path / f"{run_id}-fault.json").exists()
