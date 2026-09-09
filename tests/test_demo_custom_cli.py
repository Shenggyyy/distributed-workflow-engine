"""Existing custom Run startup stays bounded and never creates or kills work."""

import copy
import subprocess
from typing import Any
from unittest.mock import Mock, call, patch
from uuid import UUID, uuid4

import pytest
from scripts.demo import CustomWorkersRefused, Demo, main, worker_name
from scripts.demo_fault import FaultRefused, select_fault_target

from tests.demo_fixtures import branch_snapshot
from workflow_engine.demo.custom_definitions import normalize_custom
from workflow_engine.domain.workflow import WorkflowDefinition


def snapshot(run_id: UUID) -> dict[str, Any]:
    definition = normalize_custom(
        WorkflowDefinition.model_validate(
            {
                "name": "custom_cli",
                "tasks": [
                    {"task_id": "One", "task_type": "demo.join"},
                    {"task_id": "Independent", "task_type": "demo.observe"},
                    {
                        "task_id": "End",
                        "task_type": "demo.join",
                        "depends_on": ["One", "Independent"],
                    },
                ],
            }
        )
    )
    return {
        "run": {
            "id": str(run_id),
            "scenario": "custom",
            "status": "RUNNING",
            "definition": definition.model_dump(mode="json"),
        },
        "workers": [],
        "attempts": [],
    }


def test_custom_workers_rechecks_then_starts_two_one_slot_containers() -> None:
    run_id = uuid4()
    demo = object.__new__(Demo)
    demo.origin = "http://127.0.0.1:18081"
    actions = Mock()
    with (
        patch.object(Demo, "request", return_value=snapshot(run_id)) as request,
        patch.object(Demo, "command", return_value="") as command,
        patch.object(Demo, "worker") as worker,
        patch.object(Demo, "wait_ready") as ready,
    ):
        for name, method in (
            ("get", request),
            ("inspect", command),
            ("start", worker),
            ("ready", ready),
        ):
            actions.attach_mock(method, name)
        demo.workers(run_id)
    assert actions.mock_calls == [
        call.get(f"/demo/runs/{run_id}"),
        call.inspect("ps", "-aq", "--filter", f"name=^/{worker_name(run_id, 'a')}$"),
        call.inspect("ps", "-aq", "--filter", f"name=^/{worker_name(run_id, 'b')}$"),
        call.get(f"/demo/runs/{run_id}"),
        call.start(run_id, "a", 1, cohort_size=2),
        call.start(run_id, "b", 1, cohort_size=2),
        call.ready(run_id, ("a", "b")),
    ]


@pytest.mark.parametrize(
    "changed",
    [
        "id",
        "scenario",
        "terminal",
        "worker",
        "attempt",
        "handler",
        "policy",
        "noncanonical",
        "cycle",
        "missing",
    ],
)
def test_invalid_or_used_run_refused_before_any_docker_operation(changed: str) -> None:
    run_id = uuid4()
    data = snapshot(run_id)
    if changed == "id":
        data["run"]["id"] = str(uuid4())
    elif changed == "scenario":
        data["run"]["scenario"] = "distribution"
    elif changed == "terminal":
        data["run"]["status"] = "SUCCEEDED"
    elif changed == "worker":
        data["workers"] = [{"status": "LOST"}]
    elif changed == "attempt":
        data["attempts"] = [{"status": "FAILED"}]
    elif changed == "handler":
        data["run"]["definition"]["tasks"][0]["task_type"] = "shell"
    elif changed == "policy":
        data["run"]["definition"]["tasks"][0]["execution"]["max_attempts"] = 10
    elif changed == "noncanonical":
        del data["run"]["definition"]["tasks"][0]["execution"]
    elif changed == "cycle":
        data["run"]["definition"]["tasks"][0]["depends_on"] = ["End"]
    else:
        del data["workers"]
    demo = object.__new__(Demo)
    with (
        patch.object(Demo, "request", return_value=data) as request,
        patch.object(Demo, "command") as command,
        patch.object(Demo, "worker") as worker,
        pytest.raises(CustomWorkersRefused),
    ):
        demo.workers(run_id)
    request.assert_called_once_with(f"/demo/runs/{run_id}")
    command.assert_not_called()
    worker.assert_not_called()


@pytest.mark.parametrize("existing", [0, 1])
def test_any_existing_expected_container_refuses_without_reuse(existing: int) -> None:
    run_id = uuid4()
    demo = object.__new__(Demo)
    with (
        patch.object(Demo, "request", return_value=snapshot(run_id)),
        patch.object(Demo, "command", side_effect=[""] * existing + ["abc\n"]),
        patch.object(Demo, "worker") as worker,
        pytest.raises(CustomWorkersRefused, match="container already exists"),
    ):
        demo.workers(run_id)
    worker.assert_not_called()


def test_membership_changed_during_docker_inspection_refuses_startup() -> None:
    run_id = uuid4()
    before = snapshot(run_id)
    after = copy.deepcopy(before)
    after["workers"] = [{"worker_name": worker_name(run_id, "a")}]
    demo = object.__new__(Demo)
    with (
        patch.object(Demo, "request", side_effect=[before, after]),
        patch.object(Demo, "command", return_value=""),
        patch.object(Demo, "worker") as worker,
        pytest.raises(CustomWorkersRefused, match="already has"),
    ):
        demo.workers(run_id)
    worker.assert_not_called()


def test_partial_startup_is_retained_without_automatic_retry() -> None:
    run_id = uuid4()
    demo = object.__new__(Demo)
    demo.origin = "http://127.0.0.1:18080"
    with (
        patch.object(Demo, "request", return_value=snapshot(run_id)),
        patch.object(Demo, "command", return_value="") as command,
        patch.object(
            Demo, "worker", side_effect=[None, subprocess.CalledProcessError(1, [])]
        ) as worker,
        patch.object(Demo, "wait_ready") as ready,
        pytest.raises(CustomWorkersRefused, match="Any started container is retained"),
    ):
        demo.workers(run_id)
    assert worker.call_count == 2
    assert command.call_count == 2  # Only the two preflight reads; no stop/remove.
    ready.assert_not_called()


def test_custom_diamond_does_not_expand_fault_command_scope() -> None:
    data = branch_snapshot()
    data["run"]["scenario"] = "custom"
    with pytest.raises(FaultRefused, match="saved recovery diamond"):
        select_fault_target(data, UUID(data["run"]["id"]))


def test_cli_workers_uses_existing_uuid_and_explicit_port() -> None:
    run_id = uuid4()
    with (
        patch(
            "sys.argv",
            ["demo.py", "--port", "18081", "workers", "--run-id", str(run_id)],
        ),
        patch("scripts.demo.Demo") as demo,
    ):
        main()
    demo.assert_called_once_with(18081)
    demo.return_value.workers.assert_called_once_with(run_id)
    demo.return_value.run.assert_not_called()
    demo.return_value.fail.assert_not_called()
