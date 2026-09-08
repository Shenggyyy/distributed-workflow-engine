"""Exercise revision history and transactional DDL against real PostgreSQL."""

from importlib.resources import files
from pathlib import Path
from shutil import copytree

import pytest
from alembic import command
from alembic.runtime.migration import MigrationContext
from alembic.util import CommandError
from sqlalchemy import Engine, inspect, text

from tests.integration.migration_helpers import migration_config
from workflow_engine.migrations import MIGRATION_LOCK_KEY

pytestmark = pytest.mark.integration


def test_upgrade_repeat_downgrade_and_reupgrade(
    engine: Engine, migration_schema: str
) -> None:
    for operation in ("upgrade", "upgrade", "downgrade", "upgrade"):
        with engine.begin() as connection:
            config = migration_config(connection, migration_schema)
            if operation == "downgrade":
                command.downgrade(config, "base")
                assert MigrationContext.configure(connection).get_current_heads() == ()
            else:
                command.upgrade(config, "head")
                assert MigrationContext.configure(connection).get_current_heads() == (
                    "0011",
                )
                command.check(config)
    with engine.connect() as connection:
        assert set(inspect(connection).get_table_names(schema=migration_schema)) == {
            "alembic_version",
            "workflow_versions",
            "workflows",
            "workflow_runs",
            "task_runs",
            "task_attempts",
            "run_creation_requests",
            "worker_sessions",
            "attempt_leases",
            "claim_requests",
            "attempt_completions",
            "task_retry_schedules",
            "demo_runs",
            "demo_workers",
            "demo_invocations",
            "demo_samples",
        }


def test_failed_revision_rolls_back_ddl_and_version(
    engine: Engine, migration_schema: str, tmp_path: Path
) -> None:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")

    # Copy the migration environment into test-owned storage, never edit history.
    scripts = tmp_path / "migrations"
    copytree(str(files("workflow_engine.migrations")), scripts)
    (scripts / "versions" / "test_failure.py").write_text(
        "from alembic import op\n"
        'revision = "test_failure"\n'
        'down_revision = "0011"\n'
        "def upgrade():\n"
        '    op.execute("CREATE TABLE rollback_probe (id integer)")\n'
        '    op.execute("SELECT 1 / 0")\n'
        "def downgrade():\n"
        "    pass\n",
        encoding="utf-8",
    )
    with pytest.raises(CommandError, match="Database migration command failed"):
        with engine.begin() as connection:
            config = migration_config(connection, migration_schema)
            config.set_main_option("script_location", str(scripts))
            command.upgrade(config, "head")
    with engine.begin() as connection:
        migration_config(connection, migration_schema)
        assert MigrationContext.configure(connection).get_current_heads() == ("0011",)
        assert not inspect(connection).has_table(
            "rollback_probe", schema=migration_schema
        )


def test_concurrent_migration_is_rejected_and_lock_released(
    engine: Engine, migration_schema: str
) -> None:
    with engine.begin() as owner:
        owner.execute(
            text("SELECT pg_advisory_xact_lock(:key)"), {"key": MIGRATION_LOCK_KEY}
        )
        with pytest.raises(CommandError, match="holds the database migration lock"):
            with engine.begin() as contender:
                command.upgrade(migration_config(contender, migration_schema), "head")
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")


def test_unknown_database_revision_is_not_stamped_over(
    engine: Engine, migration_schema: str
) -> None:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
        connection.execute(
            text("UPDATE alembic_version SET version_num = 'unknown_revision'")
        )
    with pytest.raises(CommandError):
        with engine.begin() as connection:
            command.upgrade(migration_config(connection, migration_schema), "head")
    with engine.begin() as connection:
        migration_config(connection, migration_schema)
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == "unknown_revision"
        )
