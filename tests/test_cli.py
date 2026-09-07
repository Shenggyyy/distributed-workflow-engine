"""Smoke tests for the installed package and its public entry points."""

import os
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path


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
    assert "workflow execution is not implemented yet" in " ".join(
        result.stdout.split()
    )


def test_unimplemented_command_fails_explicitly(tmp_path: Path) -> None:
    result = subprocess.run(
        [console_script(), "worker"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 2
    assert "unrecognized arguments: worker" in result.stderr
