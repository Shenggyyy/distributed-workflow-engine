"""Run real demo scenarios and retain JSON evidence; keep the browser open."""

import argparse
import json
import time
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.demo import ROOT, Demo
from scripts.demo_evidence import EvidenceError, checkpoint_names, validate
from scripts.demo_fault import select_fault_target


def retain(path: Path, value: dict[str, Any]) -> None:
    """Keep new evidence exclusively; never replace earlier Run records."""
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2)


def run(demo: Demo, scenario: str, output: Path) -> dict[str, object]:
    run_id: UUID = demo.run(scenario)
    output.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 150
    injected = False
    checkpoints: dict[str, dict[str, Any]] = {}
    while time.monotonic() < deadline:
        snapshot = demo.request(f"/demo/runs/{run_id}")
        if UUID(snapshot["run"]["id"]) != run_id:
            raise EvidenceError("Acceptance snapshot belongs to another Run.")
        for name in sorted(checkpoint_names(snapshot) - checkpoints.keys()):
            retain(output / f"{run_id}-{name}.json", snapshot)
            checkpoints[name] = snapshot
            print(f"Checkpoint {name}: {run_id}", flush=True)
        terminal = snapshot["run"]["status"] in ("SUCCEEDED", "FAILED")
        if scenario == "recovery" and not injected and not terminal:
            # Selection observes actual B/C execution, never counts A's pulses.
            if select_fault_target(snapshot, run_id) is not None:
                demo.fail(run_id, output=output)
                injected = True
        if terminal:
            retain(output / f"{run_id}.json", snapshot)
            receipt = None
            if scenario == "recovery":
                if not injected:
                    raise EvidenceError("The recovery fault was not injected.")
                checkpoints["fault_before"] = json.loads(
                    (output / f"{run_id}-fault-before.json").read_text(encoding="utf-8")
                )
                receipt = json.loads(
                    (output / f"{run_id}-fault.json").read_text(encoding="utf-8")
                )
            report = validate(snapshot, checkpoints=checkpoints, fault_receipt=receipt)
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
