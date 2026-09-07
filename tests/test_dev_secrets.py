"""Exercise the development helper as a standalone command."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "init_dev_secrets.py"


def run_helper(directory: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--directory", str(directory)],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )


def test_create_and_reuse_secret(tmp_path: Path) -> None:
    directory = tmp_path / "secrets"
    first = run_helper(directory)
    assert first.returncode == 0
    secret_file = directory / "postgres_password.txt"
    value = secret_file.read_text().strip()
    assert len(value) >= 32
    assert value not in first.stdout + first.stderr
    if os.name == "posix":
        assert secret_file.stat().st_mode & 0o777 == 0o600

    second = run_helper(directory)
    assert second.returncode == 0
    assert secret_file.read_text().strip() == value
    assert "retained" in second.stdout
    assert value not in second.stdout + second.stderr


@pytest.mark.parametrize("existing", ["", " \n", "directory"])
def test_reject_invalid_existing_secret(tmp_path: Path, existing: str) -> None:
    destination = tmp_path / "postgres_password.txt"
    if existing == "directory":
        destination.mkdir()
    else:
        destination.write_text(existing)
    result = run_helper(tmp_path)
    assert result.returncode == 1
    assert "Unable to initialize secret" in result.stderr
    if existing != "directory":
        assert destination.read_text() == existing


def test_reject_directory_that_is_a_file(tmp_path: Path) -> None:
    destination = tmp_path / "blocked"
    destination.write_text("leave this unchanged")
    result = run_helper(destination)
    assert result.returncode == 1
    assert destination.read_text() == "leave this unchanged"
