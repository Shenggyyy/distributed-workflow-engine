"""Fault injection is restricted to named, labelled demo Worker containers."""

from pathlib import Path
from unittest.mock import Mock, call, patch
from uuid import UUID, uuid4

import pytest
from scripts.demo import MARKER, Demo, verify_worker, worker_name
from scripts.demo_fault import select_fault_target

from tests.demo_fixtures import branch_snapshot


@pytest.mark.parametrize(
    "changed", ["name", "project", "service", "run", "marker", "id"]
)
def test_fault_target_requires_all_identity_checks(changed: str) -> None:
    run = uuid4()
    labels = {
        "com.docker.compose.project": "dwe-demo",
        "com.docker.compose.service": "worker",
        MARKER: "1",
        "io.dwe.run": str(run),
    }
    info = {
        "Name": "/" + worker_name(run, "a"),
        "Id": "a" * 64,
        "Config": {"Labels": labels},
    }
    assert verify_worker(info, run, "a") == "a" * 64
    if changed == "name":
        info["Name"] = "/user-worker"
    elif changed == "id":
        info["Id"] = "bad"
    else:
        field = {
            "project": "com.docker.compose.project",
            "service": "com.docker.compose.service",
            "run": "io.dwe.run",
            "marker": MARKER,
        }[changed]
        labels[field] = "other"
    with pytest.raises(ValueError):
        verify_worker(info, run, "a")


@pytest.mark.parametrize("scenario", ["distribution", "recovery"])
def test_two_worker_scenario_launches_both_before_readiness_check(
    scenario: str,
) -> None:
    run_id = uuid4()
    demo = object.__new__(Demo)
    demo.origin = "http://127.0.0.1:18080"
    with (
        patch.object(Demo, "request", return_value={"run_id": str(run_id)}),
        patch.object(Demo, "worker") as launch,
        patch.object(Demo, "wait_ready") as ready,
    ):
        sequence = Mock()
        sequence.attach_mock(launch, "launch")
        sequence.attach_mock(ready, "ready")
        assert demo.run(scenario) == run_id
        assert sequence.mock_calls == [
            call.launch(run_id, "a", 1, cohort_size=2),
            call.launch(run_id, "b", cohort_size=2),
            call.ready(run_id, ("a", "b")),
        ]


def test_readiness_requires_both_expected_names_and_fresh_heartbeats() -> None:
    run_id = uuid4()
    demo = object.__new__(Demo)
    stamp = "2026-09-09T00:00:00Z"
    workers = [
        {
            "worker_name": worker_name(run_id, "a"),
            "status": "ACTIVE",
            "heartbeat_expires_at": "2026-09-09T00:00:06Z",
        },
        {
            "worker_name": worker_name(run_id, "b"),
            "status": "ACTIVE",
            "heartbeat_expires_at": stamp,
        },
    ]
    before = {"run": {"id": str(run_id)}, "snapshot_at": stamp, "workers": workers}
    after = {
        **before,
        "workers": [
            workers[0],
            {**workers[1], "heartbeat_expires_at": workers[0]["heartbeat_expires_at"]},
        ],
    }
    with (
        patch.object(Demo, "request", side_effect=[before, after]) as request,
        patch("scripts.demo.time.sleep") as sleep,
    ):
        demo.wait_ready(run_id, ("a", "b"))
    assert request.call_count == 2
    sleep.assert_called_once_with(0.2)


def test_readiness_timeout_does_not_launch_or_stop_any_worker() -> None:
    run_id = uuid4()
    demo = object.__new__(Demo)
    snapshot = {
        "run": {"id": str(run_id)},
        "snapshot_at": "2026-09-09T00:00:00Z",
        "workers": [],
    }
    with (
        patch.object(Demo, "request", return_value=snapshot),
        patch.object(Demo, "command") as command,
        patch("scripts.demo.time.monotonic", side_effect=[0, 61]),
        pytest.raises(ValueError, match="did not register"),
    ):
        demo.wait_ready(run_id, ("a", "b"))
    command.assert_not_called()


def test_readiness_refuses_a_different_run_snapshot() -> None:
    demo = object.__new__(Demo)
    with (
        patch.object(Demo, "request", return_value={"run": {"id": str(uuid4())}}),
        pytest.raises(ValueError, match="different Run"),
    ):
        demo.wait_ready(uuid4(), ("a", "b"))


def test_parallel_starts_only_one_two_slot_worker() -> None:
    run_id = uuid4()
    demo = object.__new__(Demo)
    demo.origin = "http://127.0.0.1:18080"
    with (
        patch.object(Demo, "request", return_value={"run_id": str(run_id)}),
        patch.object(Demo, "worker") as launch,
        patch.object(Demo, "wait_ready") as ready,
    ):
        assert demo.run("parallel") == run_id
    launch.assert_called_once_with(run_id, "a", 2, cohort_size=1)
    ready.assert_not_called()


def test_fail_uses_guarded_actual_branch_and_never_starts_a_replacement(
    tmp_path: Path,
) -> None:
    snapshot = branch_snapshot(swap_owners=True)
    run_id = UUID(snapshot["run"]["id"])
    target = select_fault_target(snapshot, run_id)
    assert target is not None
    demo = object.__new__(Demo)
    with (
        patch("scripts.demo.inject_branch_fault", return_value=target) as inject,
        patch.object(Demo, "worker") as launch,
    ):
        demo.fail(run_id, output=tmp_path)
    inject.assert_called_once_with(demo, run_id, tmp_path)
    launch.assert_not_called()
