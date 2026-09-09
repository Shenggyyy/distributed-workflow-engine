"""Strict evidence checks for the fixed diamond demonstration, not engine policy."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from scripts.demo_fault import select_fault_target
from workflow_engine.demo.scenarios import DIAMOND_DURATIONS, Scenario, diamond_tasks


class EvidenceError(ValueError):
    """The retained snapshots do not establish the claimed diamond execution."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceError(message)


def _time(value: Any) -> datetime:
    _require(isinstance(value, str), "A database timestamp is missing.")
    result = datetime.fromisoformat(value)
    _require(result.utcoffset() is not None, "Database timestamps need timezones.")
    return result


def _id(value: Any) -> str:
    _require(isinstance(value, str), "An evidence identity is missing.")
    return str(UUID(value))


@dataclass(frozen=True)
class _Stream:
    invocation: str
    attempt: str
    domain: str
    first: int
    last: int
    finished: bool
    rows: list[dict[str, Any]]


def _streams(snapshot: dict[str, Any]) -> dict[str, _Stream]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for sample in snapshot["samples"]:
        grouped.setdefault(_id(sample["invocation_id"]), []).append(sample)
    result: dict[str, _Stream] = {}
    for invocation, rows in grouped.items():
        _require(
            all(type(row["sequence"]) is int for row in rows),
            "Invalid sample sequence.",
        )
        rows = sorted(rows, key=lambda row: row["sequence"])
        _require(
            len(rows) <= 241
            and [row["sequence"] for row in rows] == list(range(len(rows))),
            "Sample sequences are incomplete or duplicated.",
        )
        owners = {_id(row["attempt_id"]) for row in rows}
        domains = {row["clock_domain"] for row in rows}
        _require(
            len(owners) == len(domains) == 1, "Invocation identities or clocks changed."
        )
        attempt, domain = next(iter(owners)), next(iter(domains))
        _require(
            isinstance(domain, str) and bool(domain),
            "Execution clock domain is missing.",
        )
        _require(attempt not in result, "An Attempt has multiple Handler invocations.")
        phases = [row["phase"] for row in rows]
        finished = phases[-1] == "FINISH"
        expected = ["START"] + ["PULSE"] * (len(rows) - 1)
        if finished and len(rows) > 1:
            expected[-1] = "FINISH"
        _require(phases == expected, "Invalid START/PULSE/FINISH evidence.")
        readings: list[int] = []
        receipts: list[datetime] = []
        for row in rows:
            raw = row["monotonic_ns"]
            _require(
                isinstance(raw, str) and raw.isascii() and raw.isdigit(),
                "Invalid monotonic sample value.",
            )
            reading = int(raw)
            _require(reading <= 2**63 - 1, "Monotonic sample exceeds database range.")
            readings.append(reading)
            receipts.append(_time(row["recorded_at"]))
        _require(readings == sorted(readings), "Handler monotonic samples regressed.")
        _require(receipts == sorted(receipts), "Database sample receipts regressed.")
        _require(
            receipts[-1] <= _time(snapshot["snapshot_at"]),
            "Sample receipt is in the future.",
        )
        result[attempt] = _Stream(
            invocation, attempt, domain, readings[0], readings[-1], finished, rows
        )
    return result


@dataclass
class _View:
    raw: dict[str, Any]
    scenario: Scenario
    tasks: dict[str, dict[str, Any]]
    attempts: dict[str, list[dict[str, Any]]]
    by_id: dict[str, dict[str, Any]]
    workers: dict[str, dict[str, Any]]
    streams: dict[str, _Stream]

    def attempt(self, key: str, number: int = 1) -> dict[str, Any]:
        values = self.attempts[key]
        _require(len(values) >= number, "A required Attempt is missing.")
        return values[number - 1]

    def stream(self, key: str, number: int = 1) -> _Stream:
        identity = _id(self.attempt(key, number)["id"])
        _require(identity in self.streams, "A required Handler START is missing.")
        return self.streams[identity]


def _view(snapshot: dict[str, Any]) -> _View:
    run = snapshot["run"]
    scenario: Scenario = run["scenario"]
    _require(
        scenario in ("parallel", "distribution", "recovery"),
        "Unknown diamond scenario.",
    )
    definition = run["definition"]
    _require(
        type(definition["schema_version"]) is int
        and definition["schema_version"] == 2
        and definition["tasks"]
        == [task.model_dump(mode="json") for task in diamond_tasks(scenario)],
        "Saved definition is not the expected diamond.",
    )
    _id(run["workflow_version_id"])
    tasks = {task["task_key"]: task for task in snapshot["tasks"]}
    _require(
        len(snapshot["tasks"]) == 4 and set(tasks) == set("ABCD"),
        "Diamond Tasks are missing or duplicated.",
    )
    keys = {_id(task["id"]): key for key, task in tasks.items()}
    _require(len(keys) == 4, "Task identities are duplicated.")
    _require(
        all(_id(task["run_id"]) == _id(run["id"]) for task in tasks.values()),
        "Task belongs to another Run.",
    )
    workers = {_id(worker["id"]): worker for worker in snapshot["workers"]}
    _require(
        len(workers) == len(snapshot["workers"]), "Worker identities are duplicated."
    )
    attempts: dict[str, list[dict[str, Any]]] = {key: [] for key in tasks}
    by_id: dict[str, dict[str, Any]] = {}
    for attempt in snapshot["attempts"]:
        identity, task_id = _id(attempt["id"]), _id(attempt["task_id"])
        _require(
            identity not in by_id and task_id in keys,
            "Unknown or duplicate Attempt identity.",
        )
        _require(type(attempt["attempt_number"]) is int, "Invalid Attempt number.")
        _require(
            _id(attempt["worker_session_id"]) in workers,
            "Attempt owner is absent from this Run.",
        )
        _require(
            _time(attempt["acquired_at"])
            <= _time(attempt["last_renewed_at"])
            < _time(attempt["lease_expires_at"]),
            "Invalid Lease timestamp ordering.",
        )
        _require(
            _time(attempt["last_renewed_at"]) <= _time(snapshot["snapshot_at"]),
            "Claim or Lease renewal is in the future.",
        )
        attempts[keys[task_id]].append(attempt)
        by_id[identity] = attempt
    for key, values in attempts.items():
        values.sort(key=lambda attempt: attempt["attempt_number"])
        _require(
            [a["attempt_number"] for a in values] == list(range(1, len(values) + 1)),
            "Attempt numbering is not contiguous.",
        )
        state = tasks[key]["status"]
        latest = values[-1]["status"] if values else None
        _require(
            (state == "PENDING" and not values)
            or (state == "READY" and latest in (None, "LOST"))
            or (state == "RUNNING" and latest == "RUNNING")
            or (state == "SUCCEEDED" and latest == "SUCCEEDED")
            or (state == "RETRY_WAIT" and latest == "LOST"),
            "Task state disagrees with its Attempt history.",
        )
    streams = _streams(snapshot)
    _require(
        set(streams) <= set(by_id), "Execution evidence references an absent Attempt."
    )
    for identity, stream in streams.items():
        attempt = by_id[identity]
        _require(
            _time(attempt["acquired_at"]) <= _time(stream.rows[0]["recorded_at"]),
            "START receipt precedes its confirmed claim.",
        )
        if attempt["status"] == "SUCCEEDED":
            _require(stream.finished, "Successful Attempt has no observed FINISH.")
            accepted = _time(attempt["accepted_at"])
            _require(
                _time(stream.rows[-1]["recorded_at"])
                <= accepted
                <= _time(snapshot["snapshot_at"]),
                "Completion admission precedes FINISH receipt or is in the future.",
            )
            _require(
                accepted < _time(attempt["lease_expires_at"]),
                "Completion admission exceeds its Lease.",
            )
        else:
            _require(
                attempt["accepted_at"] is None,
                "Unsuccessful Attempt has accepted completion evidence.",
            )
            if attempt["status"] == "LOST":
                _require(not stream.finished, "Lost Attempt has a fabricated FINISH.")
    return _View(snapshot, scenario, tasks, attempts, by_id, workers, streams)


def _overlap(left: _Stream, right: _Stream) -> int:
    _require(left.domain == right.domain, "Cannot compare different execution clocks.")
    return max(0, min(left.last, right.last) - max(left.first, right.first))


def _prefix(checkpoint: _View, final: _View) -> None:
    for field in ("id", "workflow_version_id", "definition", "scenario"):
        _require(
            checkpoint.raw["run"][field] == final.raw["run"][field],
            "Checkpoint changed the pinned Run or definition.",
        )
    _require(
        all(checkpoint.tasks[key]["id"] == final.tasks[key]["id"] for key in "ABCD"),
        "Checkpoint changed Task identities.",
    )
    _require(
        _time(checkpoint.raw["snapshot_at"]) <= _time(final.raw["snapshot_at"]),
        "Checkpoint is newer than the final snapshot.",
    )
    for identity, attempt in checkpoint.by_id.items():
        _require(identity in final.by_id, "A checkpoint Attempt disappeared.")
        later = final.by_id[identity]
        for field in (
            "id",
            "task_id",
            "attempt_number",
            "worker_session_id",
            "acquired_at",
        ):
            _require(
                attempt[field] == later[field],
                "Immutable Attempt identity or claim changed.",
            )
        _require(
            attempt["status"] in ("RUNNING", later["status"]),
            "Attempt outcome was rewritten.",
        )
        _require(
            _time(attempt["last_renewed_at"]) <= _time(later["last_renewed_at"])
            and _time(attempt["lease_expires_at"]) <= _time(later["lease_expires_at"]),
            "Lease history regressed.",
        )
        for field in ("accepted_at", "scheduled_at", "available_at"):
            _require(
                attempt[field] is None or attempt[field] == later[field],
                "Saved completion or retry evidence changed.",
            )
        if attempt["status"] in ("SUCCEEDED", "LOST"):
            _require(attempt == later, "Terminal Attempt evidence was rewritten.")
    for identity, stream in checkpoint.streams.items():
        _require(
            identity in final.streams, "A recorded Handler invocation disappeared."
        )
        later_stream = final.streams[identity]
        _require(
            stream.invocation == later_stream.invocation
            and stream.rows == later_stream.rows[: len(stream.rows)],
            "Recorded Handler samples are not an immutable prefix.",
        )
    for identity, worker in checkpoint.workers.items():
        _require(identity in final.workers, "A checkpoint Worker disappeared.")
        _require(
            all(
                worker[field] == final.workers[identity][field]
                for field in ("id", "worker_name", "max_concurrency")
            ),
            "Worker identity or capacity changed.",
        )


def _no_final_claim(view: _View) -> None:
    _require(
        view.tasks["D"]["status"] == "PENDING" and not view.attempts["D"],
        "D was claimed before the observed join condition.",
    )


def _checkpoint_conditions(views: dict[str, _View], final: _View) -> None:
    root, branches, join = (views[name] for name in ("root", "branches", "join_wait"))
    _require(
        root.raw["run"]["status"] == "RUNNING"
        and root.tasks["A"]["status"] == "RUNNING"
        and len(root.attempts["A"]) == 1,
        "Root execution was not observed.",
    )
    root.stream("A")
    _require(
        all(
            not root.attempts[key] and root.tasks[key]["status"] == "PENDING"
            for key in "BCD"
        ),
        "Branches were claimed before root success.",
    )
    _require(
        branches.tasks["A"]["status"] == "SUCCEEDED",
        "Parallel branches precede A success.",
    )
    _require(
        all(
            branches.tasks[key]["status"] == "RUNNING"
            and len(branches.attempts[key]) == 1
            for key in "BC"
        ),
        "Both first branch Attempts were not observed RUNNING.",
    )
    _no_final_claim(branches)
    _require(
        _overlap(branches.stream("B"), branches.stream("C")) > 0,
        "Branch checkpoint has no measured overlap.",
    )
    _no_final_claim(join)
    _require(
        join.tasks["A"]["status"] == join.tasks["B"]["status"] == "SUCCEEDED"
        and join.tasks["C"]["status"] != "SUCCEEDED",
        "The incomplete join was not observed.",
    )
    _require(
        join.attempt("B") == final.attempt("B"),
        "Successful B was retried or rewritten.",
    )
    _require(
        _time(root.raw["snapshot_at"])
        <= _time(branches.raw["snapshot_at"])
        <= _time(join.raw["snapshot_at"]),
        "Checkpoint chronology is inconsistent.",
    )


def _recovery(
    final: _View, views: dict[str, _View], receipt: dict[str, Any] | None
) -> None:
    old, new = final.attempts["C"]
    survivor = final.attempt("B")["worker_session_id"]
    _require(
        new["worker_session_id"] == survivor != old["worker_session_id"]
        and final.attempt("D")["worker_session_id"] == survivor,
        "Recovery was not completed by the surviving sibling Worker.",
    )
    _require(
        final.workers[_id(old["worker_session_id"])]["status"] == "LOST",
        "Old Worker loss was not confirmed.",
    )
    _require(
        _time(final.attempt("B")["accepted_at"]) <= _time(new["acquired_at"])
        and final.stream("B").last <= final.stream("C", 2).first
        and final.stream("C").last <= final.stream("C", 2).first,
        "The retry overlaps its one-slot survivor or the old observed invocation.",
    )
    _require(
        _time(old["lease_expires_at"])
        <= _time(old["scheduled_at"])
        < _time(old["available_at"])
        <= _time(new["acquired_at"]),
        "Recovery lease/backoff/new-claim ordering is invalid.",
    )
    retry, before = views["retry"], views["fault_before"]
    _no_final_claim(retry)
    _require(
        retry.tasks["C"]["status"] == "RETRY_WAIT"
        and len(retry.attempts["C"]) == 1
        and retry.attempt("C")["status"] == "LOST",
        "A real old-Attempt RETRY_WAIT checkpoint is missing.",
    )
    _require(
        retry.tasks["B"]["status"] in ("RUNNING", "SUCCEEDED"),
        "The sibling did not survive recovery.",
    )
    _require(
        _time(old["scheduled_at"])
        <= _time(retry.raw["snapshot_at"])
        < _time(old["available_at"]),
        "Retry checkpoint was not captured during backoff.",
    )
    target = select_fault_target(before.raw, UUID(final.raw["run"]["id"]))
    _require(
        target is not None, "Fault checkpoint did not identify an eligible branch."
    )
    assert target is not None
    _require(
        str(target.attempt_id) == _id(old["id"])
        and str(target.worker_session_id) == _id(old["worker_session_id"])
        and str(target.invocation_id) == final.stream("C").invocation,
        "Fault checkpoint does not identify the lost C Attempt.",
    )
    _require(receipt is not None, "Acknowledged local fault receipt is missing.")
    assert receipt is not None
    expected = {
        "run_id": str(target.run_id),
        "task_id": str(target.task_id),
        "attempt_id": str(target.attempt_id),
        "worker_session_id": str(target.worker_session_id),
        "worker_name": target.worker_name,
        "slot": target.slot,
        "invocation_id": str(target.invocation_id),
        "task_key": "C",
        "action": "SIGKILL",
        "snapshot_at": before.raw["snapshot_at"],
    }
    _require(
        all(receipt.get(key) == value for key, value in expected.items())
        and receipt.get("docker_acknowledged") is True,
        "Fault acknowledgement does not match the saved target.",
    )
    container = receipt.get("container_id")
    _require(
        isinstance(container, str)
        and len(container) == 64
        and all(c in "0123456789abcdef" for c in container),
        "Fault container identity is invalid.",
    )
    _require(
        _time(views["branches"].raw["snapshot_at"])
        <= _time(before.raw["snapshot_at"])
        <= _time(retry.raw["snapshot_at"]),
        "Fault and retry checkpoints are out of order.",
    )


def _validate(
    snapshot: dict[str, Any],
    checkpoints: dict[str, dict[str, Any]],
    receipt: dict[str, Any] | None,
) -> dict[str, object]:
    final = _view(snapshot)
    _require(
        snapshot["run"]["status"] == "SUCCEEDED"
        and all(task["status"] == "SUCCEEDED" for task in final.tasks.values()),
        "Diamond Run did not succeed.",
    )
    recovery = final.scenario == "recovery"
    required = {"root", "branches", "join_wait"} | (
        {"retry", "fault_before"} if recovery else set()
    )
    _require(required <= set(checkpoints), "Required live checkpoints are missing.")
    for key, attempts in final.attempts.items():
        expected = ["LOST", "SUCCEEDED"] if recovery and key == "C" else ["SUCCEEDED"]
        _require(
            [attempt["status"] for attempt in attempts] == expected,
            "Unexpected retry, missing Attempt or final outcome.",
        )
    _require(
        set(final.by_id) == set(final.streams),
        "Every final Attempt needs one actual invocation.",
    )
    domains = {stream.domain for stream in final.streams.values()}
    _require(len(domains) == 1, "Execution spans incomparable clock domains.")
    owner_ids = {_id(attempt["worker_session_id"]) for attempt in final.by_id.values()}
    owners = 1 if final.scenario == "parallel" else 2
    _require(
        len(owner_ids) == len(final.workers) == owners,
        "Unexpected number of actual Worker owners.",
    )
    _require(
        all(
            type(w["max_concurrency"]) is int
            and w["max_concurrency"] == (2 if owners == 1 else 1)
            for w in final.workers.values()
        ),
        "Worker slot configuration does not match the scenario.",
    )
    if owners == 2:
        _require(
            _id(final.attempt("B")["worker_session_id"])
            != _id(final.attempt("C")["worker_session_id"]),
            "B/C did not execute on different Workers.",
        )
    branch_overlap = _overlap(final.stream("B"), final.stream("C"))
    _require(
        branch_overlap >= 1_000_000_000,
        "B/C execution overlap is less than one second.",
    )
    for key in "BC":
        for number, attempt in enumerate(final.attempts[key], 1):
            _require(
                final.stream("A").last <= final.stream(key, number).first
                and _time(final.attempt("A")["accepted_at"])
                <= _time(attempt["acquired_at"]),
                "Branch execution or claim precedes successful A.",
            )
        successful = final.attempts[key][-1]
        stream = final.stream(key, len(final.attempts[key]))
        _require(
            stream.last <= final.stream("D").first
            and _time(successful["accepted_at"])
            <= _time(final.attempt("D")["acquired_at"]),
            "D ran or was claimed before both branches completed.",
        )
    views = {name: _view(value) for name, value in checkpoints.items()}
    for view in views.values():
        _prefix(view, final)
    _checkpoint_conditions(views, final)
    if recovery:
        _recovery(final, views, receipt)
    else:
        _require(receipt is None, "A normal scenario has an unexpected fault receipt.")
    task_types = {
        task.task_id: task.task_type for task in diamond_tasks(final.scenario)
    }
    for key, attempts in final.attempts.items():
        for number, attempt in enumerate(attempts, 1):
            if attempt["status"] == "SUCCEEDED":
                _require(
                    attempt["scheduled_at"] is None and attempt["available_at"] is None,
                    "Successful Attempt has unexpected retry scheduling.",
                )
                stream = final.stream(key, number)
                _require(
                    stream.last - stream.first
                    >= DIAMOND_DURATIONS[task_types[key]] * 1_000_000_000,
                    "Successful Handler evidence is shorter than its trusted duration.",
                )
    events = sorted(
        (point, change)
        for stream in final.streams.values()
        if stream.last > stream.first
        for point, change in ((stream.first, 1), (stream.last, -1))
    )
    active = peak = 0
    for _, change in events:
        active += change
        peak = max(peak, active)
    return {
        "run_id": snapshot["run"]["id"],
        "scenario": final.scenario,
        "peak_measured_overlap": peak,
        "branch_overlap_ns": branch_overlap,
        "owners": len(owner_ids),
        "clock_domains": sorted(domains),
        "samples": len(snapshot["samples"]),
        "checkpoint_names": sorted(checkpoints),
    }


def validate(
    snapshot: dict[str, Any],
    *,
    checkpoints: dict[str, dict[str, Any]],
    fault_receipt: dict[str, Any] | None = None,
) -> dict[str, object]:
    try:
        return _validate(snapshot, checkpoints, fault_receipt)
    except EvidenceError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
        raise EvidenceError("Malformed or incompatible diamond evidence.") from None


def checkpoint_names(snapshot: dict[str, Any]) -> set[str]:
    """Select candidate live snapshots; final validation establishes all contracts."""
    try:
        tasks = {task["task_key"]: task for task in snapshot["tasks"]}
        attempts = {
            key: [a for a in snapshot["attempts"] if a["task_id"] == task["id"]]
            for key, task in tasks.items()
        }
        names: set[str] = set()
        no_join = tasks["D"]["status"] == "PENDING" and not attempts["D"]
        streams = _streams(snapshot)
        if (
            tasks["A"]["status"] == "RUNNING"
            and len(attempts["A"]) == 1
            and all(not attempts[key] for key in "BCD")
            and _id(attempts["A"][0]["id"]) in streams
        ):
            names.add("root")
        if no_join and all(
            tasks[key]["status"] == "RUNNING"
            and len(attempts[key]) == 1
            and attempts[key][0]["attempt_number"] == 1
            for key in "BC"
        ):
            left, right = (streams.get(_id(attempts[key][0]["id"])) for key in "BC")
            if left is not None and right is not None and _overlap(left, right) > 0:
                names.add("branches")
        if (
            no_join
            and tasks["B"]["status"] == "SUCCEEDED"
            and tasks["C"]["status"] != "SUCCEEDED"
        ):
            names.add("join_wait")
        if (
            no_join
            and tasks["C"]["status"] == "RETRY_WAIT"
            and len(attempts["C"]) == 1
            and _time(snapshot["snapshot_at"]) < _time(attempts["C"][0]["available_at"])
        ):
            names.add("retry")
        return names
    except EvidenceError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
        raise EvidenceError("Malformed diamond checkpoint candidate.") from None
