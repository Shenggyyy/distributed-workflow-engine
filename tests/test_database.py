"""Credential loading and lazy database setup without a running PostgreSQL."""

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from workflow_engine.config import Settings, load_settings
from workflow_engine.database import DatabaseConfigurationError, database_engine


def test_engine_is_lazy_and_password_is_not_url_encoded() -> None:
    password = "p@ss:/?#% with spaces"
    settings = Settings(database_password=SecretStr(password))
    with database_engine(settings) as engine:
        assert engine.url.password == password
        assert password not in repr(settings)
        assert password not in str(engine.url)
        assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]


def test_password_file_strips_line_endings_only(tmp_path: Path) -> None:
    password_file = tmp_path / "password"
    password_file.write_bytes(b" spaced password \r\n")
    with database_engine(Settings(database_password_file=password_file)) as engine:
        assert engine.url.password == " spaced password "


@pytest.mark.parametrize("content", [b"", b"\n", b"bad\x00password", b"\xff"])
def test_invalid_password_file_is_rejected(tmp_path: Path, content: bytes) -> None:
    password_file = tmp_path / "password"
    password_file.write_bytes(content)
    with pytest.raises(DatabaseConfigurationError):
        with database_engine(Settings(database_password_file=password_file)):
            pytest.fail("Invalid credential must prevent engine creation.")


def test_missing_password_file_is_only_read_when_engine_is_created(
    tmp_path: Path,
) -> None:
    settings = Settings(database_password_file=tmp_path / "missing")
    with pytest.raises(DatabaseConfigurationError, match="could not be read"):
        with database_engine(settings):
            pytest.fail("Missing credential must prevent engine creation.")


def test_database_requires_explicit_credentials() -> None:
    with pytest.raises(DatabaseConfigurationError, match="source is required"):
        with database_engine(Settings()):
            pytest.fail("Missing credential must prevent engine creation.")


def test_password_sources_cannot_be_combined(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        Settings(
            database_password=SecretStr("example"),
            database_password_file=tmp_path / "password",
        )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("DWE_DATABASE_PORT", "0"),
        ("DWE_DATABASE_PORT", "65536"),
        ("DWE_DATABASE_POOL_SIZE", "0"),
        ("DWE_DATABASE_POOL_TIMEOUT_SECONDS", "0"),
        ("DWE_DATABASE_CONNECT_TIMEOUT_SECONDS", "0"),
        ("DWE_DATABASE_STATEMENT_TIMEOUT_MS", "0"),
        ("DWE_DATABASE_HOST", ""),
        ("DWE_DATABASE_NAME", ""),
        ("DWE_DATABASE_USER", ""),
        ("DWE_DATABASE_PASSWORD", ""),
    ],
)
def test_invalid_database_settings(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        load_settings()


def test_database_environment_overrides_explicit_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_file = tmp_path / "database.env"
    config_file.write_text("DWE_DATABASE_PORT=5432\nDWE_DATABASE_PASSWORD=example\n")
    monkeypatch.setenv("DWE_DATABASE_PORT", "15432")
    settings = load_settings(env_file=config_file)
    assert settings.database_port == 15432
    assert settings.database_password is not None
    assert settings.database_password.get_secret_value() == "example"
