"""Acceptance rejects misleading final states and incomplete diamond evidence."""

from copy import deepcopy
from uuid import uuid4

import pytest
from scripts.demo_evidence import EvidenceError, checkpoint_names, validate

from tests.demo_fixtures import evidence_case, stamp
from workflow_engine.demo.scenarios import Scenario


@pytest.mark.parametrize("scenario", ["parallel", "distribution", "recovery"])
@pytest.mark.parametrize("swapped", [False, True])
def test_complete_diamond_evidence_passes_without_preassigned_owners(
    scenario: Scenario, swapped: bool
) -> None:
    final, checkpoints, receipt = evidence_case(scenario, swap_owners=swapped)
    report = validate(final, checkpoints=checkpoints, fault_receipt=receipt)
    assert report["run_id"] == final["run"]["id"]
    overlap = report["branch_overlap_ns"]
    assert isinstance(overlap, int) and overlap > 1_000_000_000
    assert report["owners"] == (1 if scenario == "parallel" else 2)
    assert "root" in checkpoint_names(checkpoints["root"])
    assert "branches" in checkpoint_names(checkpoints["branches"])
    assert "join_wait" in checkpoint_names(checkpoints["join_wait"])
    if scenario == "recovery":
        assert "retry" in checkpoint_names(checkpoints["retry"])


@pytest.mark.parametrize(
    "case",
    [
        "early-branch",
        "early-join",
        "missing-finish",
        "different-clock",
        "no-branch-overlap",
        "wrong-owners",
        "missing-root",
        "rewritten-sample",
        "wrong-run",
        "wrong-graph",
        "future-renewal",
        "zero-duration",
    ],
)
def test_final_success_does_not_hide_invalid_parallel_or_dependency_evidence(
    case: str,
) -> None:
    final, checkpoints, _ = evidence_case("distribution")
    branch = final["attempts"][1]
    if case == "early-branch":
        branch["acquired_at"] = stamp(6.299999)
    elif case == "early-join":
        final["attempts"][-1]["acquired_at"] = stamp(20.799999)
    elif case == "missing-finish":
        next(
            s
            for s in final["samples"]
            if s["attempt_id"] == branch["id"] and s["phase"] == "FINISH"
        )["phase"] = "PULSE"
    elif case in ("different-clock", "no-branch-overlap"):
        for sample in final["samples"]:
            if sample["attempt_id"] == final["attempts"][2]["id"]:
                if case == "different-clock":
                    sample["clock_domain"] = "other-kernel"
                else:
                    sample["monotonic_ns"] = str(
                        int(sample["monotonic_ns"]) + 20_000_000_000
                    )
    elif case == "wrong-owners":
        final["attempts"][2]["worker_session_id"] = branch["worker_session_id"]
    elif case == "missing-root":
        del checkpoints["root"]
    elif case == "rewritten-sample":
        final["samples"][0]["monotonic_ns"] = "1"
    elif case == "wrong-run":
        checkpoints["join_wait"]["run"]["id"] = str(uuid4())
    elif case == "wrong-graph":
        final["run"]["definition"]["tasks"][3]["depends_on"] = ["A"]
    elif case == "future-renewal":
        checkpoints["root"]["attempts"][0]["last_renewed_at"] = stamp(5)
    elif case == "zero-duration":
        for sample in final["samples"]:
            if sample["attempt_id"] == final["attempts"][-1]["id"]:
                sample["monotonic_ns"] = "21200000000"
    with pytest.raises(EvidenceError):
        validate(final, checkpoints=checkpoints)


@pytest.mark.parametrize(
    "case",
    [
        "sibling-rerun",
        "sibling-rewritten",
        "early-join-checkpoint",
        "missing-retry",
        "fault-root",
        "wrong-receipt-owner",
        "unacknowledged",
        "lost-finish",
        "same-owner-retry",
        "early-retry",
        "missing-before",
        "same-slot-overlap",
        "short-retry",
    ],
)
def test_recovery_requires_the_complete_branch_recovery_sequence(case: str) -> None:
    final, checkpoints, receipt = evidence_case("recovery")
    assert receipt is not None
    if case == "sibling-rerun":
        final["attempts"].append(
            {**deepcopy(final["attempts"][1]), "id": str(uuid4()), "attempt_number": 2}
        )
    elif case == "sibling-rewritten":
        final["attempts"][1]["accepted_at"] = stamp(14.8)
    elif case == "early-join-checkpoint":
        checkpoints["retry"]["tasks"][3]["status"] = "READY"
    elif case == "missing-retry":
        del checkpoints["retry"]
    elif case == "fault-root":
        receipt["task_key"] = "A"
    elif case == "wrong-receipt-owner":
        receipt["worker_session_id"] = str(uuid4())
    elif case == "unacknowledged":
        receipt["docker_acknowledged"] = False
    elif case == "lost-finish":
        sample = next(
            s
            for s in reversed(final["samples"])
            if s["attempt_id"] == final["attempts"][2]["id"]
        )
        sample["phase"] = "FINISH"
    elif case == "same-owner-retry":
        final["attempts"][3]["worker_session_id"] = final["attempts"][2][
            "worker_session_id"
        ]
    elif case == "early-retry":
        final["attempts"][3]["acquired_at"] = stamp(22.099999)
    elif case == "missing-before":
        del checkpoints["fault_before"]
    elif case == "same-slot-overlap":
        next(
            s
            for s in final["samples"]
            if s["attempt_id"] == final["attempts"][3]["id"] and s["phase"] == "START"
        )["monotonic_ns"] = "8000000000"
    elif case == "short-retry":
        next(
            s
            for s in final["samples"]
            if s["attempt_id"] == final["attempts"][3]["id"] and s["phase"] == "FINISH"
        )["monotonic_ns"] = "31000000000"
    with pytest.raises(EvidenceError):
        validate(final, checkpoints=checkpoints, fault_receipt=receipt)
