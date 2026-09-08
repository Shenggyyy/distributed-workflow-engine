"""Run real demo scenarios and retain JSON evidence; keep the browser open."""

import argparse
import json
import time
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.demo import ROOT, Demo


def validate(snapshot: dict[str, Any]) -> dict[str, object]:
    assert snapshot["run"]["status"] == "SUCCEEDED"
    streams: dict[str, list[dict[str, Any]]] = {}
    for sample in snapshot["samples"]:
        streams.setdefault(sample["invocation_id"], []).append(sample)
    events: list[tuple[int, int]] = []
    domains = {sample["clock_domain"] for sample in snapshot["samples"]}
    assert len(domains) == 1, "Cannot compare different clock domains"
    for samples in streams.values():
        samples.sort(key=lambda s: s["sequence"])
        assert samples[0]["phase"] == "START"
        first, last = int(samples[0]["monotonic_ns"]), int(samples[-1]["monotonic_ns"])
        if last > first:
            events.extend(((first, 1), (last, -1)))
    active = peak = 0
    for _, delta in sorted(events):
        active += delta
        peak = max(peak, active)
    attempts = snapshot["attempts"]
    owners = {a["worker_session_id"] for a in attempts}
    scenario = snapshot["run"]["scenario"]
    if scenario in ("parallel", "distribution"):
        assert peak >= 2, "No measured execution overlap"
        assert len(owners) == (2 if scenario == "distribution" else 1)
        assert all(a["status"] == "SUCCEEDED" for a in attempts)
        assert all(samples[-1]["phase"] == "FINISH" for samples in streams.values())
    else:
        task = next(t for t in snapshot["tasks"] if t["task_key"] == "A")
        root = sorted(
            (a for a in attempts if a["task_id"] == task["id"]),
            key=lambda a: a["attempt_number"],
        )
        assert [a["status"] for a in root] == ["LOST", "SUCCEEDED"]
        assert [a["attempt_number"] for a in root] == [1, 2]
        assert root[0]["worker_session_id"] != root[1]["worker_session_id"]
        assert root[0]["accepted_at"] is None
        assert (
            root[0]["scheduled_at"] < root[0]["available_at"] <= root[1]["acquired_at"]
        )
        old = [s for s in snapshot["samples"] if s["attempt_id"] == root[0]["id"]]
        assert old and not any(s["phase"] == "FINISH" for s in old)
        assert any(
            w["id"] == root[0]["worker_session_id"] and w["status"] == "LOST"
            for w in snapshot["workers"]
        )
    return {
        "run_id": snapshot["run"]["id"],
        "scenario": scenario,
        "peak_measured_overlap": peak,
        "owners": len(owners),
        "clock_domains": sorted(domains),
        "samples": len(snapshot["samples"]),
    }


def run(demo: Demo, scenario: str, output: Path) -> dict[str, object]:
    run_id: UUID = demo.run(scenario)
    output.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 150
    injected = False
    saw_retry = False
    while time.monotonic() < deadline:
        snapshot = demo.request(f"/demo/runs/{run_id}")
        if scenario == "recovery" and not injected:
            pulses = [s for s in snapshot["samples"] if s["phase"] == "PULSE"]
            if len(pulses) >= 3:
                demo.fail(run_id)
                injected = True
        if any(t["status"] == "RETRY_WAIT" for t in snapshot["tasks"]):
            if not saw_retry:
                print(f"Checkpoint RETRY_WAIT: {run_id}", flush=True)
                (output / f"{run_id}-retry.json").write_text(
                    json.dumps(snapshot, indent=2), encoding="utf-8"
                )
            saw_retry = True
        if snapshot["run"]["status"] in ("SUCCEEDED", "FAILED"):
            report = validate(snapshot)
            if scenario == "recovery":
                assert injected and saw_retry, "Recovery sequence was not observed"
            (output / f"{run_id}.json").write_text(
                json.dumps(snapshot, indent=2), encoding="utf-8"
            )
            print(json.dumps(report), flush=True)
            return report
        time.sleep(0.2)
    raise RuntimeError("Demo did not reach a terminal state within 150 seconds.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=["all", "parallel", "distribution", "recovery"],
        default="all",
    )
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    demo = Demo(args.port)
    scenarios = (
        ["parallel", "distribution", "recovery"]
        if args.scenario == "all"
        else [args.scenario]
    )
    output = ROOT / ".uv-cache/demo-acceptance"
    for scenario in scenarios:
        run(demo, scenario, output)


if __name__ == "__main__":
    main()
