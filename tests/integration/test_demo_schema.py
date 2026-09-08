"""Additive demo migration preserves existing data and rejects rewritten evidence."""

from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import Engine, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.migration_helpers import migration_config
from tests.integration.test_completion_schema import seed, transaction
from workflow_engine.schema import demo_invocations, demo_samples, task_attempts

pytestmark = pytest.mark.integration


def test_upgrade_preserves_attempts_and_metadata(
    engine: Engine, migration_schema: str
) -> None:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "0010")
        original = seed(connection)
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "head")
        command.check(config)
        assert connection.scalar(select(task_attempts.c.id)) == original["attempt_id"]
        invocation = uuid4()
        connection.execute(
            demo_invocations.insert().values(
                id=invocation,
                attempt_id=original["attempt_id"],
                clock_domain="kernel:namespace",
            )
        )
        connection.execute(
            demo_samples.insert().values(
                invocation_id=invocation, sequence=0, phase="START", monotonic_ns=10
            )
        )
    with transaction(engine, migration_schema) as connection:
        assert connection.scalar(select(demo_samples.c.recorded_at)) is not None
        command.downgrade(migration_config(connection, migration_schema), "0010")
        assert connection.scalar(select(task_attempts.c.id)) == original["attempt_id"]
        command.upgrade(migration_config(connection, migration_schema), "head")


@pytest.mark.parametrize(
    "table", ["demo_runs", "demo_workers", "demo_invocations", "demo_samples"]
)
@pytest.mark.parametrize(
    "operation",
    [
        "UPDATE {table} SET {column} = {column}",
        "DELETE FROM {table}",
        "TRUNCATE {table}",
    ],
)
def test_evidence_is_append_only(
    engine: Engine, migration_schema: str, table: str, operation: str
) -> None:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    column = {
        "demo_runs": "scenario",
        "demo_workers": "run_id",
        "demo_invocations": "clock_domain",
        "demo_samples": "phase",
    }[table]
    with pytest.raises(DBAPIError):
        with transaction(engine, migration_schema) as connection:
            connection.execute(text(operation.format(table=table, column=column)))


@pytest.mark.parametrize(
    "phase,sequence,monotonic",
    [("PULSE", 0, 1), ("START", 1, 1), ("FINISH", 241, 1), ("FINISH", 1, -1)],
)
def test_invalid_sample_rejected(
    engine: Engine, migration_schema: str, phase: str, sequence: int, monotonic: int
) -> None:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
        original = seed(connection)
        invocation = uuid4()
        connection.execute(
            demo_invocations.insert().values(
                id=invocation,
                attempt_id=original["attempt_id"],
                clock_domain="kernel:namespace",
            )
        )
    with pytest.raises(IntegrityError):
        with transaction(engine, migration_schema) as connection:
            connection.execute(
                demo_samples.insert().values(
                    invocation_id=invocation,
                    sequence=sequence,
                    phase=phase,
                    monotonic_ns=monotonic,
                )
            )
