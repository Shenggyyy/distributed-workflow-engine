"""Retry eligibility constraints, append-only history and populated upgrades."""

from datetime import timedelta

import pytest
from alembic import command
from sqlalchemy import Engine, func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.migration_helpers import migration_config
from tests.integration.test_completion_schema import seed, transaction
from workflow_engine.schema import task_attempts, task_retry_schedules

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("outcome", ["FAILED", "TIMED_OUT", "LOST"])
def test_upgrade_retains_history_and_retry_roundtrip(
    engine: Engine, migration_schema: str, outcome: str
) -> None:
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "0009")
        original = seed(connection)
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
        stamp = connection.execute(select(func.clock_timestamp())).scalar_one()
        data = dict(
            attempt_id=original["attempt_id"],
            outcome=outcome,
            scheduled_at=stamp,
            available_at=stamp + timedelta(seconds=1),
        )
        # Deferred FK permits schedule and terminal transition in either order.
        connection.execute(task_retry_schedules.insert().values(**data))
        connection.execute(task_attempts.update().values(status=outcome))
    with transaction(engine, migration_schema) as connection:
        assert (
            dict(connection.execute(select(task_retry_schedules)).mappings().one())
            == data
        )
        command.check(migration_config(connection, migration_schema))
        command.downgrade(migration_config(connection, migration_schema), "0009")
        assert (
            connection.execute(select(task_attempts.c.id)).scalar_one()
            == original["attempt_id"]
        )
        command.upgrade(migration_config(connection, migration_schema), "head")
        assert (
            connection.execute(
                select(func.count()).select_from(task_retry_schedules)
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize(
    "change", ["running", "success", "mismatch", "equal", "past", "infinite", "missing"]
)
def test_invalid_retry_record_rolls_back(
    engine: Engine, migration_schema: str, change: str
) -> None:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
        original = seed(connection)
    with pytest.raises(IntegrityError):
        with transaction(engine, migration_schema) as connection:
            stamp = connection.execute(select(func.clock_timestamp())).scalar_one()
            outcome = "FAILED"
            if change != "running":
                connection.execute(task_attempts.update().values(status="FAILED"))
            if change == "success":
                outcome = "SUCCEEDED"
            if change == "mismatch":
                outcome = "LOST"
            available = stamp + timedelta(seconds=1)
            if change == "equal":
                available = stamp
            if change == "past":
                available = stamp - timedelta(seconds=1)
            connection.execute(
                task_retry_schedules.insert().values(
                    attempt_id=original["attempt_id"],
                    outcome=outcome,
                    scheduled_at=stamp,
                    available_at=text("'infinity'::timestamptz")
                    if change == "infinite"
                    else None
                    if change == "missing"
                    else available,
                )
            )
    with transaction(engine, migration_schema) as connection:
        assert (
            connection.execute(select(task_attempts.c.status)).scalar_one() == "RUNNING"
        )
        assert (
            connection.execute(
                select(func.count()).select_from(task_retry_schedules)
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE task_retry_schedules SET available_at = available_at",
        "DELETE FROM task_retry_schedules",
        "TRUNCATE task_retry_schedules",
    ],
)
def test_retry_history_rejects_mutation(
    engine: Engine, migration_schema: str, statement: str
) -> None:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(text(statement))
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
