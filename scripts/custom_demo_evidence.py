"""Fault-free custom Run acceptance from real snapshots, never engine policy.

Each Task must have exactly one successful Attempt. Retry/fault acceptance remains
the separate strict predefined diamond scenario, not a generic custom fault mode.
"""

from collections.abc import Iterable
from itertools import combinations
from typing import Any, Literal
from uuid import UUID

from scripts.demo_containers import verify_worker
from scripts.demo_evidence import EvidenceError as EvidenceError
from scripts.demo_evidence import _id, _require, _Stream, _streams, _time
from workflow_engine.demo.custom_definitions import CUSTOM_HANDLERS, normalize_custom
from workflow_engine.demo.custom_workers import custom_worker_names
from workflow_engine.domain.workflow import WorkflowDefinition

Expectation = Literal["parallel", "serial", "any"]


def _tasks(snapshot: dict[str, Any], keys: set[str]) -> dict[str, dict[str, Any]]:
    tasks = {task["task_key"]: task for task in snapshot["tasks"]}
    _require(
        len(tasks) == len(snapshot["tasks"]) == len(keys) and set(tasks) == keys,
        "Custom Task keys are missing, duplicated or unrelated.",
    )
    _require(
        len({_id(task["id"]) for task in tasks.values()}) == len(keys),
        "Custom Task identities are duplicated.",
    )
    _require(
        all(
            _id(task["run_id"]) == _id(snapshot["run"]["id"]) for task in tasks.values()
        ),
        "Custom Task belongs to another Run.",
    )
    return tasks


def _peak(streams: Iterable[_Stream]) -> int:
    events = sorted(
        (point, change)
        for stream in streams
        for point, change in ((stream.first, 1), (stream.last, -1))
    )
    active = peak = 0
    for _, change in events:  # FINISH endpoints sort before equal START endpoints.
        active += change
        peak = max(peak, active)
    return peak


def _containers(containers: list[dict[str, Any]], run_id: UUID) -> list[str]:
    _require(len(containers) == 2, "Two dedicated container records are required.")
    expected = custom_worker_names(run_id)
    by_name = {item["Name"]: item for item in containers}
    _require(
        set(by_name) == {"/" + name for name in expected},
        "Container names do not match the scoped custom Workers.",
    )
    identities = []
    for slot, name in zip(("a", "b"), expected, strict=True):
        info = by_name["/" + name]
        identities.append(verify_worker(info, run_id, slot))
        state = info["State"]
        _require(
            state["Status"] == "exited"
            and state["Running"] is False
            and type(state["ExitCode"]) is int
            and state["ExitCode"] == 0
            and all(
                state.get(field, False) is False
                for field in ("Paused", "Restarting", "OOMKilled")
            ),
            "Custom Worker container did not exit normally.",
        )
    _require(len(set(identities)) == 2, "Container identities are duplicated.")
    return sorted(identities)


def _validate(
    snapshot: dict[str, Any],
    waiting: dict[str, Any],
    expectation: Expectation,
    containers: list[dict[str, Any]] | None,
) -> dict[str, object]:
    _require(
        expectation in ("parallel", "serial", "any"), "Unknown custom expectation."
    )
    run = snapshot["run"]
    _require(
        run["scenario"] == "custom" and run["status"] == "SUCCEEDED",
        "A successful explicitly custom Run is required.",
    )
    run_id = UUID(_id(run["id"]))
    _id(run["workflow_version_id"])
    definition = normalize_custom(WorkflowDefinition.model_validate(run["definition"]))
    _require(
        definition.model_dump(mode="json") == run["definition"],
        "Saved custom definition is not canonical.",
    )
    keys = {task.task_id for task in definition.tasks}
    tasks, initial = _tasks(snapshot, keys), _tasks(waiting, keys)
    _require(
        all(
            waiting["run"][field] == run[field]
            for field in ("id", "workflow_version_id", "scenario", "definition")
        ),
        "Waiting checkpoint changed the pinned custom Run or definition.",
    )
    _require(
        waiting["run"]["status"] == "RUNNING"
        and all(waiting[field] == [] for field in ("workers", "attempts", "samples")),
        "Waiting checkpoint must precede all Worker and execution evidence.",
    )
    waiting_at, final_at = _time(waiting["snapshot_at"]), _time(snapshot["snapshot_at"])
    _require(waiting_at <= final_at, "Waiting checkpoint is newer than final evidence.")
    for task in definition.tasks:
        _require(
            initial[task.task_id]["id"] == tasks[task.task_id]["id"]
            and initial[task.task_id]["status"]
            == ("PENDING" if task.depends_on else "READY")
            and tasks[task.task_id]["status"] == "SUCCEEDED",
            "Task identity, initial dependency state or final success is invalid.",
        )

    workers = {_id(worker["id"]): worker for worker in snapshot["workers"]}
    _require(
        len(workers) == len(snapshot["workers"]) == 2
        and {worker["worker_name"] for worker in workers.values()}
        == set(custom_worker_names(run_id)),
        "Custom evidence needs exactly two scoped Worker identities.",
    )
    for worker in workers.values():
        _require(
            type(worker["max_concurrency"]) is int
            and worker["max_concurrency"] == 1
            and worker["status"] in ("ACTIVE", "LOST", "STOPPED"),
            "Custom Worker identity or one-slot capacity is invalid.",
        )
        _require(
            _time(worker["last_heartbeat_at"]) <= final_at
            and _time(worker["last_heartbeat_at"])
            < _time(worker["heartbeat_expires_at"]),
            "Worker heartbeat timestamps are invalid.",
        )
        # Expired heartbeat after normal exit is not proof of task failure.
    by_task = {_id(task["id"]): key for key, task in tasks.items()}
    attempts: dict[str, dict[str, Any]] = {}
    _require(
        len(snapshot["attempts"]) == len(keys),
        "Custom acceptance requires one Attempt per Task.",
    )
    for attempt in snapshot["attempts"]:
        identity, task_id = _id(attempt["id"]), _id(attempt["task_id"])
        _require(
            identity not in attempts and task_id in by_task,
            "Unknown or duplicate custom Attempt.",
        )
        _require(
            type(attempt["attempt_number"]) is int
            and attempt["attempt_number"] == 1
            and attempt["status"] == "SUCCEEDED"
            and _id(attempt["worker_session_id"]) in workers
            and attempt["scheduled_at"] is None
            and attempt["available_at"] is None,
            "Custom acceptance requires first successful Attempts "
            "without retry history.",
        )
        _require(
            waiting_at
            <= _time(attempt["acquired_at"])
            <= _time(attempt["last_renewed_at"])
            <= final_at
            and _time(attempt["last_renewed_at"]) < _time(attempt["lease_expires_at"]),
            "Custom claim or Lease timestamps are invalid.",
        )
        attempts[identity] = attempt
    _require(
        {_id(attempt["task_id"]) for attempt in attempts.values()} == set(by_task),
        "A custom Task is missing its unique Attempt.",
    )
    streams = _streams(snapshot)
    _require(
        set(streams) == set(attempts),
        "Every custom Attempt needs one complete Handler invocation.",
    )
    domains = {stream.domain for stream in streams.values()}
    _require(len(domains) == 1, "Custom execution uses incomparable clock domains.")
    types = {task.task_id: task.task_type for task in definition.tasks}
    task_attempts = {}
    for identity, attempt in attempts.items():
        stream = streams[identity]
        key = by_task[_id(attempt["task_id"])]
        task_attempts[key] = identity
        _require(
            stream.finished
            and stream.last - stream.first
            >= CUSTOM_HANDLERS[types[key]] * 1_000_000_000,
            "Custom Handler lacks FINISH or its trusted duration.",
        )
        _require(
            _time(attempt["acquired_at"])
            <= _time(stream.rows[0]["recorded_at"])
            <= _time(stream.rows[-1]["recorded_at"])
            <= _time(attempt["accepted_at"])
            <= final_at
            and _time(attempt["last_renewed_at"]) <= _time(attempt["accepted_at"])
            and _time(attempt["accepted_at"]) < _time(attempt["lease_expires_at"]),
            "Claim, Handler receipts and accepted completion are inconsistent.",
        )
    for task in definition.tasks:
        child = task_attempts[task.task_id]
        for parent in task.depends_on:
            predecessor = task_attempts[parent]
            _require(
                _time(attempts[predecessor]["accepted_at"])
                <= _time(attempts[child]["acquired_at"])
                and streams[predecessor].last <= streams[child].first,
                "A dependent Task was claimed or executed before its parent succeeded.",
            )
    owners = {_id(attempt["worker_session_id"]) for attempt in attempts.values()}
    for owner in owners:
        owned = sorted(
            (
                identity
                for identity, attempt in attempts.items()
                if _id(attempt["worker_session_id"]) == owner
            ),
            key=lambda identity: _time(attempts[identity]["acquired_at"]),
        )
        _require(
            _peak(streams[identity] for identity in owned) == 1,
            "A one-slot Worker has overlapping actual Handler execution.",
        )
        for previous, current in zip(owned, owned[1:], strict=False):
            _require(
                _time(attempts[previous]["accepted_at"])
                <= _time(attempts[current]["acquired_at"]),
                "A one-slot Worker has overlapping claim ownership.",
            )
    peak = _peak(streams.values())
    overlap = max(
        (
            max(0, min(left.last, right.last) - max(left.first, right.first))
            for left, right in combinations(streams.values(), 2)
            if attempts[left.attempt]["worker_session_id"]
            != attempts[right.attempt]["worker_session_id"]
        ),
        default=0,
    )
    if expectation == "parallel":
        _require(
            peak == 2 and overlap > 0,
            "Custom parallel execution lacks measured two-Worker overlap.",
        )
    if expectation == "serial":
        graph = definition.dag()
        order = graph.topological_order
        _require(
            all(
                previous in graph.dependencies[current]
                for previous, current in zip(order, order[1:], strict=False)
            )
            and peak == 1,
            "Serial acceptance requires a total-order DAG and measured peak one.",
        )
    container_ids = _containers(containers, run_id) if containers is not None else []
    return {
        "run_id": str(run_id),
        "scenario": "custom",
        "expectation": expectation,
        "task_count": len(keys),
        "dependency_count": sum(len(task.depends_on) for task in definition.tasks),
        "peak_measured_overlap": peak,
        "owners": len(owners),
        "registered_workers": len(workers),
        "samples": len(snapshot["samples"]),
        "clock_domains": sorted(domains),
        "max_distinct_owner_overlap_ns": overlap,
        "containers_verified": containers is not None,
        "container_ids": container_ids,
    }


def validate_custom(
    snapshot: dict[str, Any],
    *,
    waiting: dict[str, Any],
    expectation: Expectation = "any",
    containers: list[dict[str, Any]] | None = None,
) -> dict[str, object]:
    try:
        return _validate(snapshot, waiting, expectation, containers)
    except EvidenceError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
        raise EvidenceError(
            "Malformed or incompatible custom demonstration evidence."
        ) from None
