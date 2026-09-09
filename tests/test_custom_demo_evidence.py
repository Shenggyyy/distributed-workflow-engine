"""Synthetic unit fixtures only: never presented as actual execution evidence."""

from copy import deepcopy
from typing import Any
from uuid import UUID, uuid4

import pytest
from scripts.custom_demo_evidence import EvidenceError, validate_custom
from scripts.demo_containers import MARKER, PROJECT, worker_name

from tests.demo_fixtures import stamp
from workflow_engine.demo.custom_definitions import CUSTOM_POLICY


def evidence_case(
    *, serial: bool = False, swap_owners: bool = False
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    """An unsorted named DAG includes a direct edge that skips an intermediate row."""
    run_id = uuid4()
    definitions = [
        ("Publish_report", "demo.join", ["Read_rows", "Inspect_rows"]),
        ("Read_rows", "demo.join", []),
        ("Inspect_rows", "demo.observe", ["Read_rows"]),
    ]
    timings = {"Read_rows": (1.0, 3.0, 0), "Inspect_rows": (4.0, 12.0, 0)}
    if not serial:
        definitions[0][2].append("Aggregate_rows")
        definitions.insert(1, ("Aggregate_rows", "demo.join", ["Read_rows"]))
        timings["Aggregate_rows"] = (4.3, 6.3, 1)
    timings["Publish_report"] = (13.0, 15.0, 0 if serial else 1)
    definition = {
        "name": "Synthetic_custom_evidence",
        "schema_version": 2,
        "tasks": [
            {
                "task_id": key,
                "task_type": kind,
                "depends_on": parents,
                "execution": CUSTOM_POLICY.model_dump(mode="json"),
            }
            for key, kind, parents in definitions
        ],
    }
    tasks = [
        {
            "id": str(uuid4()),
            "run_id": str(run_id),
            "task_key": key,
            "status": "SUCCEEDED",
            "created_at": stamp(0),
        }
        for key, _, _ in definitions
    ]
    workers: list[dict[str, Any]] = [
        {
            "id": str(uuid4()),
            "worker_name": worker_name(run_id, slot),
            "max_concurrency": 1,
            "status": "LOST",
            "created_at": stamp(0.6),
            "last_heartbeat_at": stamp(16),
            "heartbeat_expires_at": stamp(22),
        }
        for slot in ("a", "b")
    ]
    attempts, samples = [], []
    for task in tasks:
        start, finish, owner = timings[task["task_key"]]
        if swap_owners:
            owner = 1 - owner
        identity, invocation = str(uuid4()), str(uuid4())
        attempts.append(
            {
                "id": identity,
                "task_id": task["id"],
                "attempt_number": 1,
                "status": "SUCCEEDED",
                "worker_session_id": workers[owner]["id"],
                "acquired_at": stamp(start - 0.2),
                "last_renewed_at": stamp(finish - 0.2),
                "lease_expires_at": stamp(finish + 5.8),
                "accepted_at": stamp(finish + 0.2),
                "scheduled_at": None,
                "available_at": None,
            }
        )
        for sequence, (phase, seconds) in enumerate(
            [("START", start), ("PULSE", (start + finish) / 2), ("FINISH", finish)]
        ):
            samples.append(
                {
                    "invocation_id": invocation,
                    "attempt_id": identity,
                    "sequence": sequence,
                    "phase": phase,
                    "monotonic_ns": str(round(seconds * 1_000_000_000)),
                    "recorded_at": stamp(seconds + 0.05),
                    "clock_domain": (
                        "linux:12345678-1234-1234-1234-123456789012:"
                        "monotonic-offset:0:0"
                    ),
                }
            )
    final: dict[str, Any] = {
        "snapshot_at": stamp(30),
        "run": {
            "id": str(run_id),
            "workflow_version_id": str(uuid4()),
            "scenario": "custom",
            "status": "SUCCEEDED",
            "created_at": stamp(0),
            "definition": definition,
        },
        "tasks": tasks,
        "attempts": attempts,
        "workers": workers,
        "samples": samples,
    }
    waiting = deepcopy(final)
    waiting["snapshot_at"] = stamp(0.5)
    waiting["run"]["status"] = "RUNNING"
    roots = {key for key, _, parents in definitions if not parents}
    for task in waiting["tasks"]:
        task["status"] = "READY" if task["task_key"] in roots else "PENDING"
    for field in ("workers", "attempts", "samples"):
        waiting[field] = []
    containers = [
        {
            "Id": str(index + 1) * 64,
            "Name": "/" + worker["worker_name"],
            "Config": {
                "Labels": {
                    "com.docker.compose.project": PROJECT,
                    "com.docker.compose.service": "worker",
                    MARKER: "1",
                    "io.dwe.run": str(run_id),
                }
            },
            "State": {
                "Status": "exited",
                "Running": False,
                "Paused": False,
                "Restarting": False,
                "OOMKilled": False,
                "ExitCode": 0,
            },
        }
        for index, worker in enumerate(workers)
    ]
    return final, waiting, containers


def attempt(snapshot: dict[str, Any], key: str) -> dict[str, Any]:
    task_id = next(t["id"] for t in snapshot["tasks"] if t["task_key"] == key)
    return next(a for a in snapshot["attempts"] if a["task_id"] == task_id)


def stream(snapshot: dict[str, Any], key: str) -> list[dict[str, Any]]:
    identity = attempt(snapshot, key)["id"]
    return [
        sample for sample in snapshot["samples"] if sample["attempt_id"] == identity
    ]


@pytest.mark.parametrize("swapped", [False, True])
def test_named_unsorted_graph_and_skip_edges_accept_real_overlap_without_fixed_owners(
    swapped: bool,
) -> None:
    final, waiting, containers = evidence_case(swap_owners=swapped)
    original = deepcopy((final, waiting, containers))
    report = validate_custom(
        final, waiting=waiting, expectation="parallel", containers=containers
    )
    assert report["run_id"] == final["run"]["id"]
    assert report["scenario"] == "custom"
    assert report["task_count"] == 4
    assert report["dependency_count"] == 5
    assert report["registered_workers"] == report["owners"] == 2
    assert report["peak_measured_overlap"] == 2
    assert report["max_distinct_owner_overlap_ns"] == 2_000_000_000
    assert report["containers_verified"] is True
    container_ids = report["container_ids"]
    assert isinstance(container_ids, list)
    assert set(container_ids) == {container["Id"] for container in containers}
    assert (final, waiting, containers) == original


def test_serial_total_order_accepts_one_used_owner_with_two_expired_members() -> None:
    final, waiting, _ = evidence_case(serial=True)
    report = validate_custom(final, waiting=waiting, expectation="serial")
    assert report["task_count"] == 3
    assert report["dependency_count"] == 3
    assert report["owners"] == 1
    assert report["registered_workers"] == 2
    assert report["peak_measured_overlap"] == 1
    assert report["max_distinct_owner_overlap_ns"] == 0
    assert report["containers_verified"] is False
    # Normal exit and later heartbeat expiry do not invalidate completed execution.
    assert {worker["status"] for worker in final["workers"]} == {"LOST"}
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting, expectation="parallel")


def test_minimal_archived_container_state_accepts_optional_inspect_fields() -> None:
    final, waiting, containers = evidence_case()
    for container in containers:
        container["State"] = {"Status": "exited", "Running": False, "ExitCode": 0}
    report = validate_custom(final, waiting=waiting, containers=containers)
    assert report["containers_verified"] is True


def test_dependency_acceptance_precedes_claim_independently_of_samples() -> None:
    final, waiting, _ = evidence_case()
    root = attempt(final, "Read_rows")
    child = attempt(final, "Aggregate_rows")
    assert root["worker_session_id"] != child["worker_session_id"]
    assert int(stream(final, "Read_rows")[-1]["monotonic_ns"]) < int(
        stream(final, "Aggregate_rows")[0]["monotonic_ns"]
    )
    child["acquired_at"] = stamp(3.199999)
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting)


def test_dependency_sample_order_is_checked_separately_from_database_receipts() -> None:
    final, waiting, _ = evidence_case()
    stream(final, "Aggregate_rows")[0]["monotonic_ns"] = "2999999999"
    assert (
        attempt(final, "Read_rows")["accepted_at"]
        < attempt(final, "Aggregate_rows")["acquired_at"]
    )
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting)


def test_parallel_requires_positive_sample_overlap_even_with_two_actual_owners() -> (
    None
):
    final, waiting, _ = evidence_case(serial=True)
    attempt(final, "Publish_report")["worker_session_id"] = final["workers"][1]["id"]
    report = validate_custom(final, waiting=waiting)
    assert report["owners"] == 2
    assert report["peak_measured_overlap"] == 1
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting, expectation="parallel")


def test_serial_requires_a_total_order_not_just_accidentally_serial_execution() -> None:
    final, waiting, _ = evidence_case(serial=True)
    for snapshot in (final, waiting):
        snapshot["run"]["definition"]["tasks"][0]["depends_on"] = ["Read_rows"]
    assert validate_custom(final, waiting=waiting)["peak_measured_overlap"] == 1
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting, expectation="serial")


def test_one_slot_claim_intervals_cannot_overlap_even_when_handler_samples_do_not() -> (
    None
):
    final, waiting, _ = evidence_case(serial=True)
    for snapshot in (final, waiting):
        snapshot["run"]["definition"]["tasks"][0]["depends_on"] = ["Read_rows"]
    assert validate_custom(final, waiting=waiting)["peak_measured_overlap"] == 1
    attempt(final, "Publish_report")["acquired_at"] = stamp(12.199999)
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting)


@pytest.mark.parametrize(
    "case",
    [
        "missing-start",
        "missing-finish",
        "missing-stream",
        "repeated-sequence",
        "duplicate-invocation",
        "wrong-clock",
        "short-handler",
        "future-receipt",
        "receipt-before-claim",
        "accepted-before-finish",
        "accepted-after-lease",
        "renewed-after-acceptance",
        "future-renewal",
    ],
)
def test_final_success_cannot_replace_complete_consistent_execution_samples(
    case: str,
) -> None:
    final, waiting, _ = evidence_case()
    points = stream(final, "Inspect_rows")
    owner = attempt(final, "Inspect_rows")
    if case == "missing-start":
        points[0]["phase"] = "PULSE"
    elif case == "missing-finish":
        points[-1]["phase"] = "PULSE"
    elif case == "missing-stream":
        final["samples"] = [s for s in final["samples"] if s not in points]
    elif case == "repeated-sequence":
        points[1]["sequence"] = 0
    elif case == "duplicate-invocation":
        invocation = str(uuid4())
        final["samples"].extend(
            {**deepcopy(point), "invocation_id": invocation} for point in points
        )
    elif case == "wrong-clock":
        for point in points:
            point["clock_domain"] = "different-host-clock"
    elif case == "short-handler":
        points[-1]["monotonic_ns"] = "11999999999"
    elif case == "future-receipt":
        points[-1]["recorded_at"] = stamp(31)
    elif case == "receipt-before-claim":
        points[0]["recorded_at"] = stamp(3.7)
    elif case == "accepted-before-finish":
        owner["accepted_at"] = stamp(12.049999)
    elif case == "accepted-after-lease":
        owner["lease_expires_at"] = owner["accepted_at"]
    elif case == "renewed-after-acceptance":
        owner["last_renewed_at"] = stamp(13)
    elif case == "future-renewal":
        owner["last_renewed_at"] = stamp(31)
        owner["lease_expires_at"] = stamp(37)
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting)


@pytest.mark.parametrize("status", ["READY", "RUNNING", "SUCCEEDED"])
def test_task_states_without_samples_never_prove_execution_overlap(status: str) -> None:
    final, waiting, _ = evidence_case()
    final["samples"] = []
    for task in final["tasks"]:
        task["status"] = status
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting, expectation="parallel")


@pytest.mark.parametrize(
    "case",
    [
        "duplicate-task",
        "duplicate-attempt",
        "second-attempt",
        "unknown-owner",
        "duplicate-worker",
        "wrong-name",
        "two-slots",
        "slot-overlap",
        "retry-date",
    ],
)
def test_identity_ownership_and_one_slot_contracts_are_required(case: str) -> None:
    final, waiting, _ = evidence_case()
    if case == "duplicate-task":
        final["tasks"].append(deepcopy(final["tasks"][0]))
    elif case == "duplicate-attempt":
        final["attempts"].append(deepcopy(final["attempts"][0]))
    elif case == "second-attempt":
        extra = deepcopy(final["attempts"][0])
        extra.update(id=str(uuid4()), attempt_number=2)
        final["attempts"].append(extra)
    elif case == "unknown-owner":
        final["attempts"][0]["worker_session_id"] = str(uuid4())
    elif case == "duplicate-worker":
        final["workers"].append(deepcopy(final["workers"][0]))
    elif case == "wrong-name":
        final["workers"][0]["worker_name"] = worker_name(uuid4(), "a")
    elif case == "two-slots":
        final["workers"][0]["max_concurrency"] = 2
    elif case == "slot-overlap":
        attempt(final, "Aggregate_rows")["worker_session_id"] = attempt(
            final, "Inspect_rows"
        )["worker_session_id"]
    elif case == "retry-date":
        final["attempts"][0]["scheduled_at"] = stamp(20)
        final["attempts"][0]["available_at"] = stamp(25)
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting, expectation="any")


@pytest.mark.parametrize(
    "case",
    [
        "other-run",
        "other-version",
        "other-task",
        "changed-definition",
        "root-pending",
        "dependent-ready",
        "already-registered",
        "already-claimed",
        "already-sampled",
        "late-snapshot",
    ],
)
def test_waiting_snapshot_must_identify_the_same_untouched_custom_run(
    case: str,
) -> None:
    final, waiting, _ = evidence_case()
    if case == "other-run":
        waiting["run"]["id"] = str(uuid4())
    elif case == "other-version":
        waiting["run"]["workflow_version_id"] = str(uuid4())
    elif case == "other-task":
        waiting["tasks"][0]["id"] = str(uuid4())
    elif case == "changed-definition":
        waiting["run"]["definition"]["name"] = "Different_definition"
    elif case == "root-pending":
        next(t for t in waiting["tasks"] if t["task_key"] == "Read_rows")["status"] = (
            "PENDING"
        )
    elif case == "dependent-ready":
        waiting["tasks"][0]["status"] = "READY"
    elif case == "already-registered":
        waiting["workers"] = deepcopy(final["workers"])
    elif case == "already-claimed":
        waiting["attempts"] = deepcopy(final["attempts"])
    elif case == "already-sampled":
        waiting["samples"] = deepcopy(final["samples"])
    elif case == "late-snapshot":
        waiting["snapshot_at"] = stamp(1)
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting)


@pytest.mark.parametrize(
    "case",
    [
        "wrong-label",
        "wrong-name",
        "bad-id",
        "duplicate-id",
        "missing-worker",
        "running",
        "nonzero-exit",
        "oom",
    ],
)
def test_optional_container_evidence_requires_exact_scope_and_clean_exit(
    case: str,
) -> None:
    final, waiting, containers = evidence_case()
    if case == "wrong-label":
        containers[0]["Config"]["Labels"]["io.dwe.run"] = str(uuid4())
    elif case == "wrong-name":
        containers[0]["Name"] = "/" + worker_name(UUID(final["run"]["id"]), "b")
    elif case == "bad-id":
        containers[0]["Id"] = "not-a-container-id"
    elif case == "duplicate-id":
        containers[0]["Id"] = containers[1]["Id"]
    elif case == "missing-worker":
        containers.pop()
    elif case == "running":
        containers[0]["State"].update(Status="running", Running=True)
    elif case == "nonzero-exit":
        containers[0]["State"]["ExitCode"] = 1
    elif case == "oom":
        containers[0]["State"]["OOMKilled"] = True
    with pytest.raises(EvidenceError):
        validate_custom(final, waiting=waiting, containers=containers)
