"""The runner retains real-response checkpoints and never retries missing evidence.

Synthetic unit inputs exercise control flow only; browser acceptance uses real Runs.
"""

import json
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

import pytest
from scripts.demo import Demo
from scripts.demo_acceptance import run
from scripts.demo_evidence import EvidenceError

from tests.demo_fixtures import evidence_case
from workflow_engine.demo.scenarios import Scenario


@pytest.mark.parametrize("scenario", ["parallel", "distribution", "recovery"])
def test_runner_keeps_required_live_checkpoints_and_validates_them(
    tmp_path: Path, scenario: Scenario
) -> None:
    final, checkpoints, receipt = evidence_case(scenario)
    identity = UUID(final["run"]["id"])
    demo = Mock(spec=Demo)
    demo.run.return_value = identity
    snapshots = [checkpoints[name] for name in ("root", "branches", "join_wait")]
    demo.request.side_effect = [*snapshots, final]

    def fault(run_id: UUID, *, output: Path) -> None:
        assert run_id == identity and output == tmp_path
        (output / f"{identity}-fault-before.json").write_text(
            json.dumps(checkpoints["fault_before"]), encoding="utf-8"
        )
        (output / f"{identity}-fault.json").write_text(
            json.dumps(receipt), encoding="utf-8"
        )

    demo.fail.side_effect = fault
    with patch("scripts.demo_acceptance.time.sleep"):
        report = run(demo, scenario, tmp_path)
    assert report["run_id"] == str(identity)
    assert json.loads((tmp_path / f"{identity}.json").read_text()) == final
    for name in ("root", "branches", "join_wait"):
        assert (
            json.loads((tmp_path / f"{identity}-{name}.json").read_text())
            == checkpoints[name]
        )
    if scenario == "recovery":
        demo.fail.assert_called_once_with(identity, output=tmp_path)
        assert (
            json.loads((tmp_path / f"{identity}-retry.json").read_text())
            == checkpoints["retry"]
        )
    else:
        demo.fail.assert_not_called()
    demo.run.assert_called_once_with(scenario)


def test_runner_preserves_terminal_snapshot_but_rejects_missing_live_proof(
    tmp_path: Path,
) -> None:
    final, _, _ = evidence_case("parallel")
    demo = Mock(spec=Demo)
    demo.run.return_value = UUID(final["run"]["id"])
    demo.request.return_value = final
    with pytest.raises(EvidenceError, match="checkpoints are missing"):
        run(demo, "parallel", tmp_path)
    assert json.loads(next(tmp_path.glob("*.json")).read_text()) == final
    demo.run.assert_called_once()
    demo.fail.assert_not_called()


def test_runner_does_not_overwrite_an_existing_checkpoint(tmp_path: Path) -> None:
    _, checkpoints, _ = evidence_case("parallel")
    identity = UUID(checkpoints["root"]["run"]["id"])
    retained = tmp_path / f"{identity}-root.json"
    retained.write_text("retained earlier evidence", encoding="utf-8")
    demo = Mock(spec=Demo)
    demo.run.return_value = identity
    demo.request.return_value = checkpoints["root"]
    with pytest.raises(FileExistsError):
        run(demo, "parallel", tmp_path)
    assert retained.read_text() == "retained earlier evidence"
    demo.fail.assert_not_called()


def test_runner_refuses_a_response_from_another_run(tmp_path: Path) -> None:
    _, checkpoints, _ = evidence_case("parallel")
    demo = Mock(spec=Demo)
    demo.run.return_value = uuid4()
    demo.request.return_value = checkpoints["root"]
    with pytest.raises(EvidenceError, match="another Run"):
        run(demo, "parallel", tmp_path)
    assert list(tmp_path.iterdir()) == []
    demo.fail.assert_not_called()
