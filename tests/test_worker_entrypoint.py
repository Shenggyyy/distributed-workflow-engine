"""Worker configuration, signal cleanup and safe process exit reporting."""

import signal
from threading import Event
from uuid import uuid4

import pytest
from pydantic import ValidationError

from workflow_engine.config import Settings
from workflow_engine.worker.entrypoint import run_worker
from workflow_engine.worker.loop import WorkerLoop


@pytest.mark.parametrize(
    "url",
    [
        "http://user:secret@localhost",
        "http://localhost/x",
        "http://localhost?secret=x",
        "ftp://localhost",
        "http://localhost:0",
        "http://localhost:99999",
        "http://local host",
    ],
)
def test_worker_origin_rejected(url: str) -> None:
    with pytest.raises(ValidationError):
        Settings(worker_api_url=url)


@pytest.mark.parametrize(
    "field",
    ["worker_http_timeout_seconds", "worker_poll_seconds", "worker_retry_seconds"],
)
@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan"), 61])
def test_invalid_worker_intervals(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        Settings(**{field: value})  # type: ignore[arg-type]


@pytest.mark.parametrize("failure", [False, True])
def test_signal_restoration_and_redacted_failures(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], failure: bool
) -> None:
    previous = {
        signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
    }

    def run(self: WorkerLoop, stop: Event, *, max_tasks: int | None = None) -> int:
        assert max_tasks == 2
        signal.raise_signal(signal.SIGINT)
        assert stop.is_set()
        if failure:
            raise RuntimeError("private-sentinel")
        return 2

    monkeypatch.setattr(WorkerLoop, "run", run)
    assert run_worker(Settings(), uuid4(), max_tasks=2) == (1 if failure else 0)
    assert {signum: signal.getsignal(signum) for signum in previous} == previous
    output = capsys.readouterr()
    assert "private-sentinel" not in output.out + output.err
    assert (
        "worker_failed" in output.err
        if failure
        else "2 confirmed completions" in output.out
    )
