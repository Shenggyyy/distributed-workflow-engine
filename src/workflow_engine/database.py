"""Explicit PostgreSQL engine lifetime and a minimal connectivity probe."""

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import URL, Engine, create_engine, text

from workflow_engine.config import Settings


class DatabaseConfigurationError(ValueError):
    """Database credentials are missing or could not be loaded safely."""


def _password(settings: Settings) -> str:
    if settings.database_password is not None:
        return settings.database_password.get_secret_value()
    if settings.database_password_file is None:
        raise DatabaseConfigurationError("A database password source is required.")
    try:
        # Remove file line endings, preserving spaces that may belong to the password.
        value = settings.database_password_file.read_text(encoding="utf-8").rstrip(
            "\r\n"
        )
    except (OSError, UnicodeError):
        raise DatabaseConfigurationError(
            "Database password file could not be read."
        ) from None
    if not value or "\x00" in value:
        raise DatabaseConfigurationError(
            "Database password file contains an invalid value."
        )
    return value


def create_database_engine(settings: Settings) -> Engine:
    """Create a lazy, process-local pool; callers own disposal and transactions."""
    url = URL.create(
        "postgresql+psycopg",
        username=settings.database_user,
        password=_password(settings),
        host=settings.database_host,
        port=settings.database_port,
        database=settings.database_name,
    )
    return create_engine(
        url,
        pool_size=settings.database_pool_size,
        max_overflow=0,
        pool_timeout=settings.database_pool_timeout_seconds,
        pool_pre_ping=True,
        isolation_level="READ COMMITTED",
        hide_parameters=True,
        echo=False,
        connect_args={
            "connect_timeout": settings.database_connect_timeout_seconds,
            "options": f"-c statement_timeout={settings.database_statement_timeout_ms}",
            "application_name": "distributed-workflow-engine",
        },
    )


@contextmanager
def database_engine(settings: Settings) -> Iterator[Engine]:
    """Dispose idle pooled connections on normal exit and on exceptions."""
    engine = create_database_engine(settings)
    try:
        yield engine
    finally:
        engine.dispose()


def check_database(engine: Engine) -> None:
    """Check an authenticated round trip, without claiming schema readiness."""
    with engine.connect() as connection:
        if connection.execute(text("SELECT 1")).scalar_one() != 1:
            raise RuntimeError("Unexpected database probe result.")
