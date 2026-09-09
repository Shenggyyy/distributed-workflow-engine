"""Guarded local SIGKILL of the observed C-branch owner, without replacement."""

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from scripts.demo_containers import verify_worker, worker_name
from workflow_engine.demo.scenarios import diamond_tasks


class FaultRefused(ValueError):
    """The available evidence does not safely identify the intended demo fault."""


@dataclass(frozen=True, slots=True)
class FaultTarget:
    run_id: UUID
    task_id: UUID
    attempt_id: UUID
    worker_session_id: UUID
    worker_name: str
    slot: str
    invocation_id: UUID
    task_key: str = "C"


class FaultControl(Protocol):
    def request(self, path: str, body: dict[str, str] | None = None) -> Any: ...

    def command(
        self, *args: str, capture: bool = True, timeout: float | None = None
    ) -> str: ...


def _uuid(value: object) -> UUID:
    if not isinstance(value, str):
        raise FaultRefused("Invalid fault evidence identity.")
    return UUID(value)


def _stamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise FaultRefused("Invalid fault evidence timestamp.")
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise FaultRefused("Fault evidence requires timezone-aware timestamps.")
    return stamp


@dataclass(frozen=True, slots=True)
class _Stream:
    invocation_id: UUID
    domain: str
    first: int
    last: int


def _stream(samples: list[dict[str, Any]], now: datetime) -> _Stream | None:
    if not samples:
        return None
    streams = {_uuid(sample["invocation_id"]) for sample in samples}
    domains = {sample["clock_domain"] for sample in samples}
    if len(streams) != 1 or len(domains) != 1:
        raise FaultRefused("Branch execution evidence is not one clocked invocation.")
    domain = next(iter(domains))
    if not isinstance(domain, str) or not domain:
        raise FaultRefused("Branch execution clock is missing.")
    if any(type(sample["sequence"]) is not int for sample in samples):
        raise FaultRefused("Branch sample sequence is invalid.")
    ordered = sorted(samples, key=lambda sample: sample["sequence"])
    if len(ordered) > 241 or [sample["sequence"] for sample in ordered] != list(
        range(len(ordered))
    ):
        raise FaultRefused("Branch sample sequence is incomplete or duplicated.")
    readings: list[int] = []
    receipts: list[datetime] = []
    for index, sample in enumerate(ordered):
        if sample["phase"] != ("START" if index == 0 else "PULSE"):
            raise FaultRefused("Branch already finished or has invalid sample phases.")
        raw = sample["monotonic_ns"]
        if not isinstance(raw, str) or not raw.isascii() or not raw.isdigit():
            raise FaultRefused("Branch monotonic sample is invalid.")
        reading = int(raw)
        if reading > 2**63 - 1 or (readings and reading < readings[-1]):
            raise FaultRefused("Branch monotonic samples regress or overflow.")
        readings.append(reading)
        receipt = _stamp(sample["recorded_at"])
        if receipt > now or (receipts and receipt < receipts[-1]):
            raise FaultRefused("Branch receipt timestamps are inconsistent.")
        receipts.append(receipt)
    if now - receipts[-1] > timedelta(seconds=2):
        raise FaultRefused("Branch execution observations are stale.")
    return _Stream(next(iter(streams)), domain, readings[0], readings[-1])


def _select(snapshot: dict[str, Any], run_id: UUID) -> FaultTarget | None:
    now = _stamp(snapshot["snapshot_at"])
    run = snapshot["run"]
    expected = [task.model_dump(mode="json") for task in diamond_tasks("recovery")]
    definition = run["definition"]
    if (
        _uuid(run["id"]) != run_id
        or run["scenario"] != "recovery"
        or run["status"] != "RUNNING"
        or definition["schema_version"] != 2
        or definition["tasks"] != expected
    ):
        raise FaultRefused("Fault injection requires the saved recovery diamond.")
    tasks = {task["task_key"]: task for task in snapshot["tasks"]}
    if len(snapshot["tasks"]) != 4 or set(tasks) != {"A", "B", "C", "D"}:
        raise FaultRefused("Recovery task identities do not match the diamond.")
    task_ids = {_uuid(task["id"]) for task in tasks.values()}
    if len(task_ids) != 4 or any(
        _uuid(task["run_id"]) != run_id for task in tasks.values()
    ):
        raise FaultRefused("Recovery tasks belong to a different Run.")
    workers: dict[UUID, dict[str, Any]] = {}
    names: set[str] = set()
    allowed_names = {worker_name(run_id, slot) for slot in ("a", "b")}
    for worker in snapshot["workers"]:
        identity = _uuid(worker["id"])
        name = worker["worker_name"]
        if (
            identity in workers
            or name in names
            or name not in allowed_names
            or type(worker["max_concurrency"]) is not int
            or worker["max_concurrency"] != 1
            or worker["status"] != "ACTIVE"
            or not _stamp(worker["last_heartbeat_at"])
            <= now
            < _stamp(worker["heartbeat_expires_at"])
        ):
            raise FaultRefused("Fault injection requires distinct fresh demo Workers.")
        workers[identity] = worker
        names.add(name)
    attempts: dict[UUID, dict[str, Any]] = {}
    by_task: dict[UUID, dict[str, Any]] = {}
    for attempt in snapshot["attempts"]:
        identity, task_id = _uuid(attempt["id"]), _uuid(attempt["task_id"])
        if (
            identity in attempts
            or task_id in by_task
            or task_id not in task_ids
            or type(attempt["attempt_number"]) is not int
            or attempt["attempt_number"] != 1
            or attempt["scheduled_at"] is not None
            or attempt["available_at"] is not None
        ):
            raise FaultRefused(
                "Unexpected retry or Attempt in recovery fault evidence."
            )
        if _uuid(attempt["worker_session_id"]) not in workers:
            raise FaultRefused("Attempt owner is not a fresh scoped demo Worker.")
        attempts[identity], by_task[task_id] = attempt, attempt
    if tasks["D"]["status"] != "PENDING" or _uuid(tasks["D"]["id"]) in by_task:
        raise FaultRefused("Final task already became runnable or was claimed.")
    root = by_task.get(_uuid(tasks["A"]["id"]))
    branches: dict[str, dict[str, Any]] = {}
    waiting = tasks["A"]["status"] in ("READY", "RUNNING")
    if waiting:
        if tasks["A"]["status"] == "READY" and root is not None:
            raise FaultRefused("Unexpected root Attempt before execution.")
        if tasks["A"]["status"] == "RUNNING" and (
            root is None
            or root["status"] != "RUNNING"
            or root["accepted_at"] is not None
        ):
            raise FaultRefused("Root execution evidence is inconsistent.")
    elif (
        tasks["A"]["status"] != "SUCCEEDED"
        or root is None
        or root["status"] != "SUCCEEDED"
        or not _stamp(root["acquired_at"]) <= _stamp(root["accepted_at"]) <= now
    ):
        raise FaultRefused("Root did not succeed on its first accepted Attempt.")
    for key in ("B", "C"):
        task = tasks[key]
        attempt = by_task.get(_uuid(task["id"]))
        if task["status"] in ("PENDING", "READY") and attempt is None:
            waiting = True
            continue
        if waiting and tasks["A"]["status"] != "SUCCEEDED":
            raise FaultRefused("Branch execution precedes root success.")
        if (
            task["status"] != "RUNNING"
            or attempt is None
            or attempt["status"] != "RUNNING"
            or attempt["accepted_at"] is not None
            or not _stamp(attempt["acquired_at"])
            <= _stamp(attempt["last_renewed_at"])
            <= now
            < _stamp(attempt["lease_expires_at"])
            or root is None
            or _stamp(attempt["acquired_at"]) < _stamp(root["accepted_at"])
        ):
            raise FaultRefused("Branch is no longer a first live executing Attempt.")
        branches[key] = attempt
    samples: dict[UUID, list[dict[str, Any]]] = {}
    for sample in snapshot["samples"]:
        attempt_id = _uuid(sample["attempt_id"])
        if attempt_id not in attempts:
            raise FaultRefused("Observation references an unexpected Attempt.")
        samples.setdefault(attempt_id, []).append(sample)
    streams = {
        key: _stream(samples.get(_uuid(attempt["id"]), []), now)
        for key, attempt in branches.items()
    }
    target_stream = streams.get("C")
    if (
        target_stream is not None
        and target_stream.last - target_stream.first > 15_000_000_000
    ):
        raise FaultRefused(
            "Observed recovery execution has insufficient timing margin."
        )
    if len(branches) == 2 and _uuid(branches["B"]["worker_session_id"]) == _uuid(
        branches["C"]["worker_session_id"]
    ):
        raise FaultRefused("Branches do not have different actual Worker owners.")
    if waiting or streams.get("B") is None or target_stream is None:
        return None
    sibling_stream = streams["B"]
    assert sibling_stream is not None
    if sibling_stream.invocation_id == target_stream.invocation_id:
        raise FaultRefused("Branches cannot share one invocation identity.")
    if sibling_stream.domain != target_stream.domain:
        raise FaultRefused("Branch clocks cannot establish execution overlap.")
    overlap = min(sibling_stream.last, target_stream.last) - max(
        sibling_stream.first, target_stream.first
    )
    if overlap < 1_000_000_000:
        return None
    target = branches["C"]
    owner_id = _uuid(target["worker_session_id"])
    owner_name = str(workers[owner_id]["worker_name"])
    slot = "a" if owner_name == worker_name(run_id, "a") else "b"
    return FaultTarget(
        run_id,
        _uuid(tasks["C"]["id"]),
        _uuid(target["id"]),
        owner_id,
        owner_name,
        slot,
        target_stream.invocation_id,
    )


def select_fault_target(snapshot: dict[str, Any], run_id: UUID) -> FaultTarget | None:
    """None means pre-start waiting only; malformed or late evidence is refused."""
    try:
        return _select(snapshot, run_id)
    except FaultRefused:
        raise
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
        raise FaultRefused("Malformed recovery fault evidence.") from None


def _inspect(control: FaultControl, reference: str, target: FaultTarget) -> str:
    try:
        entries = json.loads(control.command("inspect", reference, timeout=5))
        if not isinstance(entries, list) or len(entries) != 1:
            raise ValueError()
        info = entries[0]
        identity = verify_worker(info, target.run_id, target.slot)
        state = info["State"]
        if (
            state.get("Running") is not True
            or state.get("Paused") is not False
            or state.get("Restarting") is True
        ):
            raise ValueError()
        return identity
    except Exception:
        raise FaultRefused(
            "Fault container identity or running state was not confirmed."
        ) from None


def inject_branch_fault(
    control: FaultControl, run_id: UUID, output: Path
) -> FaultTarget:
    """Recheck evidence/identity, persist intent, then issue exactly one local kill.

    Database reads and Docker are not atomic; acknowledgement is a local command
    result, not an engine event. A retained before-file prevents a blind retry.
    """
    before_path = output / f"{run_id}-fault-before.json"
    receipt_path = output / f"{run_id}-fault.json"
    if before_path.exists() or receipt_path.exists():
        raise FaultRefused("Fault evidence already exists; refusing a repeated action.")
    deadline = time.monotonic() + 60
    while True:
        snapshot = control.request(f"/demo/runs/{run_id}")
        target = select_fault_target(snapshot, run_id)
        if time.monotonic() >= deadline:
            raise FaultRefused(
                "Recovery branch did not become eligible for fault injection."
            )
        if target is not None:
            break
        time.sleep(0.2)
    identity = _inspect(control, target.worker_name, target)
    final_read_started = time.monotonic()
    fresh = control.request(f"/demo/runs/{run_id}")
    if select_fault_target(fresh, run_id) != target or _stamp(
        fresh["snapshot_at"]
    ) < _stamp(snapshot["snapshot_at"]):
        raise FaultRefused("Recovery fault target changed before injection.")
    if _inspect(control, identity, target) != identity:
        raise FaultRefused("Fault container identity changed before injection.")

    def check_age() -> None:
        now = time.monotonic()
        if now >= deadline or now - final_read_started > 2:
            raise FaultRefused("Final fault evidence became stale before injection.")

    check_age()
    output.mkdir(parents=True, exist_ok=True)
    try:
        with before_path.open("x", encoding="utf-8") as evidence:
            json.dump(fresh, evidence, indent=2)
    except OSError:
        raise FaultRefused("Could not exclusively save pre-fault evidence.") from None
    check_age()
    try:
        control.command("kill", "--signal", "KILL", identity, capture=False, timeout=5)
    except Exception:
        raise FaultRefused(
            "Fault command outcome is uncertain; do not automatically retry."
        ) from None
    receipt = {
        "action": "SIGKILL",
        "run_id": str(target.run_id),
        "task_key": target.task_key,
        "task_id": str(target.task_id),
        "attempt_id": str(target.attempt_id),
        "worker_session_id": str(target.worker_session_id),
        "worker_name": target.worker_name,
        "slot": target.slot,
        "invocation_id": str(target.invocation_id),
        "container_id": identity,
        "snapshot_at": fresh["snapshot_at"],
        "docker_acknowledged": True,
    }
    with receipt_path.open("x", encoding="utf-8") as evidence:
        json.dump(receipt, evidence, indent=2)
    return target
