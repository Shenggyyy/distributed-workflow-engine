"""Smoke tests for the installed package and its public entry points."""

import json
import os
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path

import pytest


def console_script() -> str:
    """Locate the console script installed beside the test interpreter."""
    filename = "engine.exe" if os.name == "nt" else "engine"
    return str(Path(sys.executable).parent / filename)


def test_installed_console_reports_distribution_version(tmp_path: Path) -> None:
    result = subprocess.run(
        [console_script(), "--version"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )

    assert result.stdout.strip() == (f"engine {version('distributed-workflow-engine')}")
    assert result.stderr == ""


def test_module_entry_point_works_outside_repository(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "workflow_engine", "--version"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )

    assert result.stdout.strip() == (f"engine {version('distributed-workflow-engine')}")


def test_help_describes_current_capabilities(tmp_path: Path) -> None:
    result = subprocess.run(
        [console_script(), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )

    assert "--version" in result.stdout
    assert "run bounded parallel workers" in " ".join(result.stdout.split())


def test_worker_run_id_is_optional(tmp_path: Path) -> None:
    result = subprocess.run(
        [console_script(), "worker", "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 0
    assert "[--run-id RUN_ID]" in result.stdout


def test_check_config_accepts_an_explicit_file(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("DWE_API_PORT=9000\n", encoding="utf-8")

    result = subprocess.run(
        [console_script(), "check-config", "--env-file", str(env_file)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )

    assert result.stdout.strip() == "Configuration is valid."
    records = [json.loads(line) for line in result.stderr.splitlines()]
    assert len(records) == 1
    assert records[0]["event"] == "configuration_validated"
    assert records[0]["component"] == "cli"
    assert records[0]["level"] == "INFO"
    assert "api_port" not in records[0]


@pytest.mark.parametrize("command", ["check-config", "api", "check-db"])
def test_invalid_settings_prevent_command_startup_without_echoing_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    sensitive_value = "example-sensitive-value"
    monkeypatch.setenv("DWE_API_PORT", sensitive_value)

    result = subprocess.run(
        [console_script(), command],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 2
    assert "api_port (int_parsing)" in result.stderr
    assert sensitive_value not in result.stdout + result.stderr
    assert "Traceback" not in result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize("command", ["check-config", "api", "check-db"])
def test_missing_file_prevents_command_startup(tmp_path: Path, command: str) -> None:
    result = subprocess.run(
        [console_script(), command, "--env-file", "missing.env"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 2
    assert "Environment file could not be read as UTF-8." in result.stderr
    assert "Traceback" not in result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize(
    "arguments", [["--help"], ["api", "--help"], ["check-db", "--help"]]
)
def test_help_does_not_load_invalid_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    monkeypatch.setenv("DWE_API_PORT", "invalid")

    result = subprocess.run(
        [console_script(), *arguments],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )

    assert "usage:" in result.stdout


def test_check_config_log_level_does_not_hide_stdout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DWE_LOG_LEVEL", "WARNING")

    result = subprocess.run(
        [console_script(), "check-config"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )

    assert result.stdout.strip() == "Configuration is valid."
    assert result.stderr == ""


@pytest.mark.parametrize("source", ["missing", "conflict", "unreadable"])
def test_check_db_rejects_missing_or_conflicting_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    password = "example-sensitive-password"
    if source != "missing":
        monkeypatch.setenv("DWE_DATABASE_PASSWORD_FILE", str(tmp_path / "missing"))
    if source == "conflict":
        monkeypatch.setenv("DWE_DATABASE_PASSWORD", password)
    result = subprocess.run(
        [console_script(), "check-db"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 2
    assert result.stdout == ""
    assert password not in result.stderr
    assert "Traceback" not in result.stderr


def test_check_db_connection_failure_has_safe_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import socket

    password = "example-sensitive-password"
    # Reserve a local port without listening, producing a connection refusal.
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        monkeypatch.setenv("DWE_DATABASE_HOST", "127.0.0.1")
        monkeypatch.setenv("DWE_DATABASE_PORT", str(reserved.getsockname()[1]))
        monkeypatch.setenv("DWE_DATABASE_PASSWORD", password)
        monkeypatch.setenv("DWE_DATABASE_CONNECT_TIMEOUT_SECONDS", "2")
        result = subprocess.run(
            [console_script(), "check-db"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    assert result.returncode == 1
    assert result.stdout == ""
    assert password not in result.stderr
    assert "Traceback" not in result.stderr
    record = json.loads(result.stderr)
    assert record["event"] == "database_check_failed"
