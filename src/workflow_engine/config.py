"""Validated application settings with explicit, optional dotenv loading."""

from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from workflow_engine.domain.workflow import Identifier


class Settings(BaseSettings):
    """An immutable configuration snapshot for one process startup."""

    model_config = SettingsConfigDict(
        env_prefix="DWE_",
        case_sensitive=False,
        env_file=None,
        env_file_encoding="utf-8",
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
    )

    environment: Literal["development", "test", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    api_host: IPv4Address | IPv6Address = IPv4Address("127.0.0.1")
    api_port: int = Field(default=8000, ge=1, le=65535)
    worker_heartbeat_timeout_seconds: int = Field(default=30, ge=1, le=86400)
    attempt_lease_seconds: int = Field(default=30, ge=1, le=86400)
    worker_api_url: str = "http://127.0.0.1:8000"
    worker_name: Identifier = "worker"
    worker_http_timeout_seconds: float = Field(
        default=5, ge=0.05, le=60, allow_inf_nan=False
    )
    worker_poll_seconds: float = Field(default=0.5, ge=0.05, le=60, allow_inf_nan=False)
    worker_retry_seconds: float = Field(
        default=0.5, ge=0.05, le=60, allow_inf_nan=False
    )
    scheduler_poll_seconds: float = Field(
        default=0.5, ge=0.05, le=60, allow_inf_nan=False
    )

    database_host: str = Field(default="127.0.0.1", min_length=1)
    database_port: int = Field(default=5432, ge=1, le=65535)
    database_name: str = Field(default="workflow", min_length=1)
    database_user: str = Field(default="workflow_admin", min_length=1)
    database_password: SecretStr | None = Field(default=None, min_length=1)
    database_password_file: Path | None = None
    database_pool_size: int = Field(default=5, ge=1, le=20)
    database_pool_timeout_seconds: int = Field(default=5, ge=1, le=60)
    database_connect_timeout_seconds: int = Field(default=5, ge=2, le=60)
    database_statement_timeout_ms: int = Field(default=10000, ge=1, le=600000)

    @field_validator("worker_api_url")
    @classmethod
    def validate_worker_origin(cls, value: str) -> str:
        origin = urlsplit(value)
        if (
            origin.scheme not in ("http", "https")
            or not origin.hostname
            or origin.username is not None
            or origin.password is not None
            or origin.path not in ("", "/")
            or origin.query
            or origin.fragment
            or any(character.isspace() for character in value)
        ):
            raise ValueError(
                "Worker API URL must be an HTTP(S) origin without credentials."
            )
        if origin.port is not None and origin.port < 1:
            raise ValueError("Worker API port must be positive.")
        return value.rstrip("/")

    @model_validator(mode="after")
    def validate_password_sources(self) -> Self:
        if self.database_password is not None:
            if self.database_password_file is not None:
                raise ValueError("Specify only one database password source.")
            if "\x00" in self.database_password.get_secret_value():
                raise ValueError("Database password must not contain NUL.")
        return self


def load_settings(*, env_file: Path | None = None) -> Settings:
    """Load environment > explicit dotenv file > defaults without global caching."""
    if env_file is not None and not env_file.is_file():
        raise FileNotFoundError("Environment file is missing or is not a regular file.")
    return Settings(_env_file=env_file)
