"""Migration discovery, offline SQL, and safe configuration failures."""

import subprocess
import sys
from importlib.resources import files
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

CONFIG = Path(__file__).resolve().parents[1] / "alembic.ini"


def run_alembic(tmp_path: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(CONFIG), *arguments],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )


def test_revision_history_has_one_baseline_head() -> None:
    scripts = ScriptDirectory.from_config(Config(str(CONFIG)))
    assert scripts.get_heads() == ["0001"]
    assert scripts.get_bases() == ["0001"]
    assert files("workflow_engine.migrations").joinpath("script.py.mako").is_file()


def test_heads_work_outside_repository_without_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DWE_API_PORT", "invalid")
    result = run_alembic(tmp_path, "heads")
    assert result.returncode == 0
    assert "0001 (head)" in result.stdout


def test_offline_upgrade_does_not_load_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DWE_DATABASE_PASSWORD", "sensitive-example-password")
    monkeypatch.setenv("DWE_DATABASE_PASSWORD_FILE", str(tmp_path / "missing"))
    result = run_alembic(tmp_path, "upgrade", "head", "--sql")
    assert result.returncode == 0
    assert "CREATE TABLE alembic_version" in result.stdout
    assert "0001" in result.stdout
    assert "sensitive-example-password" not in result.stdout + result.stderr


@pytest.mark.parametrize("mode", ["missing", "conflict", "file"])
def test_online_configuration_failure_is_safe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    if mode == "conflict":
        monkeypatch.setenv("DWE_DATABASE_PASSWORD", "sensitive-example-password")
        monkeypatch.setenv("DWE_DATABASE_PASSWORD_FILE", str(tmp_path / "missing"))
    arguments = ["current"]
    if mode == "file":
        arguments = ["-x", "env_file=missing.env", "current"]
    result = run_alembic(tmp_path, *arguments)
    assert result.returncode != 0
    assert (
        "Migration configuration or credentials are invalid"
        in result.stdout + result.stderr
    )
    assert "Traceback" not in result.stderr
    assert "sensitive-example-password" not in result.stdout + result.stderr


def test_packaged_template_can_generate_a_revision(tmp_path: Path) -> None:
    from shutil import copytree

    from alembic import command
    from alembic.script import Script

    scripts = tmp_path / "migrations"
    copytree(str(files("workflow_engine.migrations")), scripts)
    config = Config()
    config.set_main_option("script_location", str(scripts))
    revision = command.revision(config, message="template smoke", rev_id="0002")
    assert isinstance(revision, Script)
    assert revision.down_revision == "0001"
    assert ScriptDirectory.from_config(config).get_heads() == ["0002"]
