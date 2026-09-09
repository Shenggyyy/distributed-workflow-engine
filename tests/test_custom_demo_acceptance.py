"""Capture orchestration with synthetic snapshots; no Docker or engine is contacted."""

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from scripts import custom_demo_acceptance as capture_module
from scripts.custom_demo_acceptance import CustomCaptureError, capture
from scripts.demo import Demo

from tests.test_custom_demo_evidence import evidence_case


class Clock:
    def __init__(self) -> None:
        self.current = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.current

    def sleep(self, seconds: float) -> None:
        assert seconds > 0
        self.sleeps.append(seconds)
        self.current += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    value = Clock()
    monkeypatch.setattr(
        capture_module,
        "time",
        SimpleNamespace(monotonic=value.monotonic, sleep=value.sleep),
    )
    return value


class FakeDemo(Demo):
    def __init__(
        self,
        snapshots: list[dict[str, Any] | Exception],
        containers: list[dict[str, Any]],
        output: Path,
    ) -> None:
        self.origin = "http://127.0.0.1:18081"
        self.snapshots = deepcopy(snapshots)
        self.containers = deepcopy(containers)
        self.output = output
        self.requests: list[str] = []
        self.starts: list[UUID] = []
        self.commands: list[tuple[str, ...]] = []
        self.start_error: Exception | None = None

    def request(self, path: str, body: dict[str, str] | None = None) -> Any:
        assert body is None, "Capture must never create or mutate a Run over HTTP"
        self.requests.append(path)
        value = self.snapshots.pop(0) if len(self.snapshots) > 1 else self.snapshots[0]
        if isinstance(value, Exception):
            raise value
        return deepcopy(value)

    def workers(self, run_id: UUID) -> None:
        waiting = json.loads((self.output / "waiting.json").read_text(encoding="utf-8"))
        assert waiting["run"]["id"] == str(run_id)
        assert waiting["workers"] == waiting["attempts"] == waiting["samples"] == []
        self.starts.append(run_id)
        if self.start_error:
            raise self.start_error

    def command(
        self, *args: str, capture: bool = True, timeout: float | None = None
    ) -> str:
        assert args[0] == "inspect", (
            "Capture must not start, stop or clean Docker itself"
        )
        assert timeout == 5, "Inspection needs a bounded subprocess timeout"
        self.commands.append(args)
        selected = []
        for target in args[1:]:
            matches = [
                item
                for item in self.containers
                if target in (item["Id"], item["Name"], item["Name"].removeprefix("/"))
            ]
            assert len(matches) == 1, (
                "Inspect only exact Run-scoped names or verified IDs"
            )
            selected.append(matches[0])
        return json.dumps(selected)


def setup_case(
    tmp_path: Path, *, serial: bool = False
) -> tuple[FakeDemo, UUID, dict[str, Any], dict[str, Any]]:
    final, waiting, containers = evidence_case(serial=serial)
    run_id = UUID(final["run"]["id"])
    output = tmp_path / str(run_id)
    return FakeDemo([waiting, final], containers, output), run_id, final, waiting


def read(output: Path, name: str) -> Any:
    return json.loads((output / name).read_text(encoding="utf-8"))


def assert_failed(output: Path) -> None:
    assert (output / "last.json").is_file()
    assert (output / "error.json").is_file()
    assert not (output / "report.json").exists()


@pytest.mark.usefixtures("clock")
def test_default_watch_retains_verified_evidence_without_starting_or_mutating(
    tmp_path: Path,
) -> None:
    demo, run_id, final, waiting = setup_case(tmp_path)
    for container in demo.containers:
        container["Config"]["Env"] = ["PASSWORD=private-env-sentinel"]
        container["Config"]["Labels"]["unrelated.secret"] = "private-label-sentinel"
        container["HostConfig"] = {"Binds": ["private-host-path-sentinel:/secrets"]}
    summary = capture(demo, run_id, demo.output, expectation="parallel")
    assert summary["peak_measured_overlap"] == 2
    assert summary["containers_verified"] is True
    assert read(demo.output, "waiting.json") == waiting
    assert read(demo.output, "final.json") == final
    assert read(demo.output, "report.json") == summary
    assert demo.starts == []
    assert demo.requests and set(demo.requests) == {f"/demo/runs/{run_id}"}
    assert demo.commands and all(command[0] == "inspect" for command in demo.commands)
    written = "\n".join(
        path.read_text(encoding="utf-8") for path in demo.output.iterdir()
    )
    assert "private-" not in written
    assert not (demo.output / "error.json").exists()


@pytest.mark.usefixtures("clock")
def test_explicit_start_happens_once_after_waiting_is_retained(tmp_path: Path) -> None:
    demo, run_id, final, waiting = setup_case(tmp_path)
    demo.snapshots = [waiting, deepcopy(waiting), final]
    summary = capture(demo, run_id, demo.output, start_workers=True)
    assert summary["run_id"] == str(run_id)
    assert demo.starts == [run_id]
    assert read(demo.output, "waiting.json") == waiting


@pytest.mark.usefixtures("clock")
def test_existing_output_is_refused_before_inspection_or_worker_start(
    tmp_path: Path,
) -> None:
    demo, run_id, _, _ = setup_case(tmp_path)
    demo.output.mkdir()
    sentinel = demo.output / "retained.json"
    sentinel.write_text('{"existing": true}', encoding="utf-8")
    with pytest.raises(CustomCaptureError, match="Evidence directory"):
        capture(demo, run_id, demo.output, start_workers=True)
    assert not demo.starts and not demo.commands and not demo.requests
    assert list(demo.output.iterdir()) == [sentinel]
    assert sentinel.read_text(encoding="utf-8") == '{"existing": true}'


@pytest.mark.usefixtures("clock")
@pytest.mark.parametrize("history", ["workers", "attempts", "samples"])
def test_start_guard_refuses_existing_execution_history(
    history: str, tmp_path: Path
) -> None:
    demo, run_id, final, waiting = setup_case(tmp_path)
    waiting[history] = deepcopy(final[history])
    demo.snapshots = [waiting]
    with pytest.raises(CustomCaptureError) as caught:
        capture(demo, run_id, demo.output, start_workers=True)
    assert caught.value.reason == "initial_refused"
    assert demo.starts == demo.commands == []
    assert not demo.output.exists()


@pytest.mark.usefixtures("clock")
@pytest.mark.parametrize("field", ["run", "version", "task", "definition"])
def test_later_snapshot_identity_drift_retains_diagnostics_and_refuses_success(
    field: str, tmp_path: Path
) -> None:
    demo, run_id, final, waiting = setup_case(tmp_path)
    if field == "run":
        final["run"]["id"] = str(uuid4())
    elif field == "version":
        final["run"]["workflow_version_id"] = str(uuid4())
    elif field == "task":
        final["tasks"][0]["id"] = str(uuid4())
    elif field == "definition":
        final["run"]["definition"]["name"] = "Different_saved_definition"
    demo.snapshots[-1] = final
    with pytest.raises(CustomCaptureError) as caught:
        capture(demo, run_id, demo.output)
    assert caught.value.reason == "snapshot_changed"
    assert demo.starts == []
    assert_failed(demo.output)
    assert read(demo.output, "last.json") == waiting


@pytest.mark.usefixtures("clock")
def test_timeout_keeps_last_waiting_snapshot_without_launch_or_cleanup(
    tmp_path: Path,
) -> None:
    demo, run_id, _, waiting = setup_case(tmp_path)
    demo.snapshots = [waiting]
    with pytest.raises(CustomCaptureError) as caught:
        capture(demo, run_id, demo.output, timeout_seconds=0.5)
    assert caught.value.reason == "capture_timeout"
    assert_failed(demo.output)
    assert read(demo.output, "last.json") == waiting
    assert read(demo.output, "waiting.json") == waiting
    assert demo.starts == demo.commands == []


@pytest.mark.usefixtures("clock")
def test_terminal_failure_retains_final_snapshot_without_success_report(
    tmp_path: Path,
) -> None:
    demo, run_id, final, _ = setup_case(tmp_path)
    final["run"]["status"] = "FAILED"
    final["tasks"][0]["status"] = "FAILED"
    demo.snapshots[-1] = final
    with pytest.raises(CustomCaptureError):
        capture(demo, run_id, demo.output)
    assert_failed(demo.output)
    assert read(demo.output, "final.json") == final
    assert demo.starts == []


@pytest.mark.usefixtures("clock")
@pytest.mark.parametrize("source", ["request", "workers"])
def test_transport_and_start_failure_keep_safe_diagnostics_without_retry(
    source: str, tmp_path: Path
) -> None:
    demo, run_id, _, _ = setup_case(tmp_path)
    if source == "request":
        demo.snapshots[-1] = RuntimeError("private-transport-sentinel")
    else:
        demo.start_error = RuntimeError("private-worker-command-sentinel")
    with pytest.raises(CustomCaptureError) as caught:
        capture(demo, run_id, demo.output, start_workers=source == "workers")
    assert_failed(demo.output)
    assert "private-" not in str(caught.value)
    assert caught.value.reason == (
        "startup_failed" if source == "workers" else "observation_failed"
    )
    assert "private-" not in (demo.output / "error.json").read_text(encoding="utf-8")
    assert demo.starts == ([run_id] if source == "workers" else [])
    assert demo.commands == []


@pytest.mark.usefixtures("clock")
def test_capture_does_not_relax_final_handler_evidence_validation(
    tmp_path: Path,
) -> None:
    demo, run_id, final, _ = setup_case(tmp_path)
    final["samples"][-1]["phase"] = "PULSE"
    demo.snapshots[-1] = final
    with pytest.raises(CustomCaptureError) as caught:
        capture(demo, run_id, demo.output)
    assert caught.value.reason == "evidence_rejected"
    assert_failed(demo.output)
    assert read(demo.output, "final.json") == final


@pytest.mark.usefixtures("clock")
@pytest.mark.parametrize("failure", ["labels", "id", "exit"])
def test_final_container_identity_or_exit_failure_never_yields_a_report(
    failure: str, tmp_path: Path
) -> None:
    demo, run_id, _, _ = setup_case(tmp_path)
    if failure == "labels":
        demo.containers[0]["Config"]["Labels"]["io.dwe.run"] = str(uuid4())
    elif failure == "id":
        demo.containers[0]["Id"] = "invalid-id"
    else:
        demo.containers[0]["State"]["ExitCode"] = 1
    with pytest.raises(CustomCaptureError):
        capture(demo, run_id, demo.output)
    assert_failed(demo.output)
    assert demo.starts == []


def test_container_exit_wait_uses_verified_immutable_ids(
    tmp_path: Path, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    demo, run_id, _, _ = setup_case(tmp_path)
    for container in demo.containers:
        container["State"].update(Status="running", Running=True)
    inspect = demo.command

    def transition(
        *args: str, capture: bool = True, timeout: float | None = None
    ) -> str:
        result = inspect(*args, capture=capture, timeout=timeout)
        if len(demo.commands) == 2:
            for container in demo.containers:
                container["State"].update(Status="exited", Running=False)
        return result

    monkeypatch.setattr(demo, "command", transition)
    assert capture(demo, run_id, demo.output)["containers_verified"] is True
    assert len(demo.commands) == 4
    assert [command[1] for command in demo.commands[:2]] == [
        container["Name"].removeprefix("/") for container in demo.containers
    ]
    assert [command[1] for command in demo.commands[2:]] == [
        container["Id"] for container in demo.containers
    ]
    assert clock.sleeps
    assert demo.starts == []


def test_container_wait_timeout_retains_final_evidence_and_never_stops_workers(
    tmp_path: Path, clock: Clock
) -> None:
    demo, run_id, final, _ = setup_case(tmp_path)
    for container in demo.containers:
        container["State"].update(Status="running", Running=True)
    with pytest.raises(CustomCaptureError) as caught:
        capture(demo, run_id, demo.output)
    assert caught.value.reason == "container_timeout"
    assert 15 <= clock.current < 16
    assert_failed(demo.output)
    assert read(demo.output, "final.json") == final
    assert demo.starts == []
    assert all(command[0] == "inspect" for command in demo.commands)


@pytest.mark.usefixtures("clock")
def test_container_id_change_between_inspections_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    demo, run_id, _, _ = setup_case(tmp_path)
    for container in demo.containers:
        container["State"].update(Status="running", Running=True)
    inspect = demo.command

    def replaced(*args: str, capture: bool = True, timeout: float | None = None) -> str:
        records = json.loads(inspect(*args, capture=capture, timeout=timeout))
        if len(args[1]) == 64:
            records[0]["Id"] = "f" * 64
        return json.dumps(records)

    monkeypatch.setattr(demo, "command", replaced)
    with pytest.raises(CustomCaptureError) as caught:
        capture(demo, run_id, demo.output)
    assert caught.value.reason == "containers_failed"
    assert_failed(demo.output)
    assert demo.starts == []


@pytest.mark.usefixtures("clock")
def test_failed_waiting_write_prevents_worker_start_and_keeps_safe_diagnostics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    demo, run_id, _, _ = setup_case(tmp_path)
    write = capture_module._write

    def denied(output: Path, name: str, value: object) -> None:
        if name == "waiting.json":
            raise OSError("private-path-sentinel")
        write(output, name, value)

    monkeypatch.setattr(capture_module, "_write", denied)
    with pytest.raises(CustomCaptureError) as caught:
        capture(demo, run_id, demo.output, start_workers=True)
    assert caught.value.reason == "observation_failed"
    assert_failed(demo.output)
    assert demo.starts == demo.commands == []
    assert "private-path-sentinel" not in (demo.output / "error.json").read_text(
        encoding="utf-8"
    )


@pytest.mark.parametrize("timeout", [0.0, -1.0, 600.1, float("inf"), float("nan")])
def test_invalid_capture_budget_fails_before_any_side_effect(
    timeout: float, tmp_path: Path
) -> None:
    demo, run_id, _, _ = setup_case(tmp_path)
    with pytest.raises(CustomCaptureError):
        capture(demo, run_id, demo.output, timeout_seconds=timeout)
    assert not demo.starts and not demo.commands and not demo.requests
    assert not demo.output.exists()


@pytest.mark.parametrize(
    ("extra", "expectation", "start", "port"),
    [
        ([], "any", False, 18080),
        (
            ["--start-workers", "--expect", "serial", "--port", "18081"],
            "serial",
            True,
            18081,
        ),
    ],
)
def test_cli_watch_or_explicit_start_keeps_fixed_exclusive_run_directory(
    extra: list[str],
    expectation: str,
    start: bool,
    port: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = uuid4()
    demo = Mock(spec=Demo)
    factory = Mock(return_value=demo)
    operation = Mock(return_value={"run_id": str(run_id)})
    monkeypatch.setattr(capture_module, "ROOT", tmp_path)
    monkeypatch.setattr(capture_module, "Demo", factory)
    monkeypatch.setattr(capture_module, "capture", operation)
    monkeypatch.setattr(
        "sys.argv", ["custom_demo_acceptance", "--run-id", str(run_id), *extra]
    )
    capture_module.main()
    args, options = operation.call_args
    assert args == (
        demo,
        run_id,
        tmp_path / ".uv-cache" / "custom-acceptance" / str(run_id),
    )
    assert options.get("start_workers", False) is start
    assert options.get("expectation", "any") == expectation
    assert options.get("timeout_seconds", 600) == 600
    factory.assert_called_once_with(port)


@pytest.mark.parametrize(
    "arguments",
    [["--output", "elsewhere"], ["--expect", "recovery"], ["--run-id", "not-a-uuid"]],
)
def test_invalid_cli_arguments_never_construct_docker_client(
    arguments: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    factory = Mock()
    monkeypatch.setattr(capture_module, "Demo", factory)
    monkeypatch.setattr(
        "sys.argv", ["custom_demo_acceptance", "--run-id", str(uuid4()), *arguments]
    )
    with pytest.raises(SystemExit) as caught:
        capture_module.main()
    assert caught.value.code == 2
    factory.assert_not_called()
