"""Configuration precedence, validation, and explicit file-loading contracts."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from workflow_engine.config import load_settings


def test_defaults_do_not_require_an_environment_file() -> None:
    settings = load_settings()

    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert str(settings.api_host) == "127.0.0.1"
    assert settings.api_port == 8000
    assert settings.worker_heartbeat_timeout_seconds == 30
    assert settings.attempt_lease_seconds == 30


def test_dotenv_is_not_implicitly_loaded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text("DWE_API_PORT=invalid\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert load_settings().api_port == 8000


def test_environment_overrides_dotenv_and_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / "settings.env"
    env_file.write_text(
        "DWE_ENVIRONMENT=test\nDWE_API_PORT=9000\nDWE_API_HOST=::1\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DWE_API_PORT", "9001")
    monkeypatch.setenv("UNRELATED_SETTING", "ignored")

    settings = load_settings(env_file=env_file)

    assert settings.api_port == 9001
    assert settings.environment == "test"
    assert str(settings.api_host) == "::1"
    assert settings.log_level == "INFO"


@pytest.mark.parametrize(
    ("name", "value", "field"),
    [
        ("DWE_API_PORT", "0", "api_port"),
        ("DWE_API_PORT", "65536", "api_port"),
        ("DWE_API_PORT", "not-a-number", "api_port"),
        ("DWE_API_PORT", "", "api_port"),
        ("DWE_ATTEMPT_LEASE_SECONDS", "0", "attempt_lease_seconds"),
        ("DWE_ATTEMPT_LEASE_SECONDS", "86401", "attempt_lease_seconds"),
        ("DWE_ATTEMPT_LEASE_SECONDS", "1.5", "attempt_lease_seconds"),
        ("DWE_ENVIRONMENT", "staging", "environment"),
        ("DWE_LOG_LEVEL", "debug", "log_level"),
        ("DWE_API_HOST", "https://localhost", "api_host"),
        (
            "DWE_WORKER_HEARTBEAT_TIMEOUT_SECONDS",
            "0",
            "worker_heartbeat_timeout_seconds",
        ),
        (
            "DWE_WORKER_HEARTBEAT_TIMEOUT_SECONDS",
            "86401",
            "worker_heartbeat_timeout_seconds",
        ),
        (
            "DWE_WORKER_HEARTBEAT_TIMEOUT_SECONDS",
            "1.5",
            "worker_heartbeat_timeout_seconds",
        ),
    ],
)
def test_invalid_environment_values_fail(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str, field: str
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError) as error:
        load_settings()

    assert all(item["loc"][0] == field for item in error.value.errors())


@pytest.mark.parametrize("port", [1, 65535])
def test_valid_port_boundaries(monkeypatch: pytest.MonkeyPatch, port: int) -> None:
    monkeypatch.setenv("DWE_API_PORT", str(port))

    assert load_settings().api_port == port


def test_unknown_dotenv_keys_are_rejected(tmp_path: Path) -> None:
    env_file = tmp_path / "settings.env"
    env_file.write_text("DWE_API_PORRT=9000\n", encoding="utf-8")

    with pytest.raises(ValidationError) as error:
        load_settings(env_file=env_file)

    assert error.value.errors()[0]["type"] == "extra_forbidden"


def test_explicit_missing_file_does_not_silently_use_defaults(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_settings(env_file=tmp_path / "missing.env")


def test_directory_is_not_an_environment_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_settings(env_file=tmp_path)


def test_settings_are_immutable_and_loads_are_not_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings()

    with pytest.raises(ValidationError) as error:
        # Deliberately bypass static rejection to test runtime immutability.
        settings.api_port = 9000  # type: ignore[misc]
    assert error.value.errors()[0]["type"] == "frozen_instance"

    monkeypatch.setenv("DWE_API_PORT", "9001")
    assert load_settings().api_port == 9001
    assert settings.api_port == 8000


def test_repository_example_is_valid() -> None:
    example = Path(__file__).resolve().parents[1] / ".env.example"

    assert load_settings(env_file=example).environment == "development"


@pytest.mark.parametrize("seconds", [1, 86400])
def test_heartbeat_policy_precedence_and_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, seconds: int
) -> None:
    env_file = tmp_path / "worker.env"
    env_file.write_text("DWE_WORKER_HEARTBEAT_TIMEOUT_SECONDS=7\n", encoding="utf-8")
    assert load_settings(env_file=env_file).worker_heartbeat_timeout_seconds == 7
    monkeypatch.setenv("DWE_WORKER_HEARTBEAT_TIMEOUT_SECONDS", str(seconds))
    assert load_settings(env_file=env_file).worker_heartbeat_timeout_seconds == seconds


@pytest.mark.parametrize("seconds", [1, 86400])
def test_lease_policy_precedence_and_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, seconds: int
) -> None:
    env_file = tmp_path / "lease.env"
    env_file.write_text("DWE_ATTEMPT_LEASE_SECONDS=7\n", encoding="utf-8")
    assert load_settings(env_file=env_file).attempt_lease_seconds == 7
    monkeypatch.setenv("DWE_ATTEMPT_LEASE_SECONDS", str(seconds))
    assert load_settings(env_file=env_file).attempt_lease_seconds == seconds
