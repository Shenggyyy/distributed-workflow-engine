"""Worker enforces hard timeout despite fresh heartbeats and renewed leases."""

import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Timer

import pytest

from tests.test_worker_loop import Gateway
from workflow_engine.domain.completion import CompletionResult
from workflow_engine.domain.retry import ExecutionPolicy
from workflow_engine.worker.handlers import (
    HandlerContext,
    HandlerRegistration,
    HandlerRegistry,
)
from workflow_engine.worker.loop import WorkerControlError, WorkerLoop


@dataclass(frozen=True)
class SlowHandler:
    marker: str

    def __call__(self, context: HandlerContext) -> CompletionResult:
        Path(self.marker).write_text("started", encoding="utf-8")
        time.sleep(10)
        raise AssertionError("Handler should have been terminated.")


@pytest.mark.parametrize("blocked_renewal", [False, True])
def test_running_child_stops_at_fixed_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocked_renewal: bool
) -> None:
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    gateway = Gateway()
    gateway.lease_seconds = 1.5
    gateway.block_renewal = blocked_renewal
    gateway.grant = gateway.grant.model_copy(
        update={
            "definition": gateway.grant.definition.model_copy(
                update={"execution": ExecutionPolicy(timeout_seconds=2)}
            )
        }
    )
    marker = tmp_path / "started.txt"
    registry = HandlerRegistry(
        [HandlerRegistration("demo.echo", SlowHandler(str(marker)))]
    )
    loop = WorkerLoop(gateway, registry, gateway.run_id, tick_seconds=0.01)
    stop = Event()
    timer = Timer(6, stop.set)
    timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(WorkerControlError, match="deadline|lease confirmation"):
            loop.run(stop, max_tasks=1)
        assert marker.read_text() == "started"
        assert time.monotonic() - started < 5
        assert gateway.renewals >= 2 and not gateway.reports
    finally:
        gateway.release.set()
        timer.cancel()
        timer.join(timeout=1)
