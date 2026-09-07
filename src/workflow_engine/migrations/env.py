"""Run migrations with application settings and an explicit transaction."""

from pathlib import Path

from alembic import context
from alembic.util import CommandError
from pydantic import ValidationError
from sqlalchemy import Connection, text
from sqlalchemy.exc import SQLAlchemyError

from workflow_engine.config import load_settings
from workflow_engine.database import DatabaseConfigurationError, database_engine
from workflow_engine.migrations import MIGRATION_LOCK_KEY
from workflow_engine.schema import metadata


def run_on_connection(connection: Connection) -> None:
    if not connection.in_transaction():
        raise CommandError(
            "Migration connection must have an active outer transaction."
        )
    acquired = connection.execute(
        text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": MIGRATION_LOCK_KEY}
    ).scalar_one()
    if not acquired:
        raise CommandError(
            "Another migration command holds the database migration lock."
        )
    context.configure(
        connection=connection,
        target_metadata=metadata,
        compare_type=True,
        transactional_ddl=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_online() -> None:
    supplied_connection = context.config.attributes.get("connection")
    if supplied_connection is not None:
        if not isinstance(supplied_connection, Connection):
            raise CommandError("Migration connection must be a SQLAlchemy Connection.")
        run_on_connection(supplied_connection)
        return

    arguments = context.get_x_argument(as_dictionary=True)
    if set(arguments) - {"env_file"}:
        raise CommandError("Only -x env_file=<path> is supported.")
    env_file = Path(arguments["env_file"]) if "env_file" in arguments else None
    settings = load_settings(env_file=env_file)
    with database_engine(settings) as engine:
        with engine.begin() as connection:
            run_on_connection(connection)


def run_offline() -> None:
    # SQL generation requires neither credentials nor a live database.
    context.configure(
        dialect_name="postgresql",
        target_metadata=metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        transactional_ddl=True,
    )
    with context.begin_transaction():
        context.run_migrations()


try:
    if context.is_offline_mode():
        run_offline()
    else:
        run_online()
except (ValidationError, DatabaseConfigurationError, OSError, UnicodeError):
    raise CommandError("Migration configuration or credentials are invalid.") from None
except SQLAlchemyError as exc:
    # Do not expose driver messages, bound values, or credentials.
    raise CommandError(
        f"Database migration command failed ({type(exc).__name__}); no automatic retry."
    ) from None
