"""Validated application settings with explicit, optional dotenv loading."""

from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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


def load_settings(*, env_file: Path | None = None) -> Settings:
    """Load environment > explicit dotenv file > defaults without global caching."""
    if env_file is not None and not env_file.is_file():
        raise FileNotFoundError("Environment file is missing or is not a regular file.")
    return Settings(_env_file=env_file)
