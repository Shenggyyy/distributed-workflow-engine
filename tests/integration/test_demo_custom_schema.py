"""Custom submission migration preserves history and enforces durable receipts."""

from typing import Any
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.runtime.migration import MigrationContext
from alembic.util import CommandError
from sqlalchemy import (
    JSON,
    Connection,
    Engine,
    String,
    Table,
    func,
    inspect,
    null,
    select,
    text,
)
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from tests.integration.test_completion_schema import seed, transaction
from workflow_engine.demo.custom_definitions import normalize_custom
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    attempt_leases,
    claim_requests,
    demo_custom_submissions,
    demo_invocations,
    demo_runs,
    demo_samples,
    demo_workers,
    task_attempts,
    task_runs,
    worker_sessions,
    workflow_runs,
    workflow_versions,
    workflows,
)

pytestmark = pytest.mark.integration
HISTORY = (
    workflows,
    workflow_versions,
    workflow_runs,
    task_runs,
    task_attempts,
    worker_sessions,
    attempt_leases,
    claim_requests,
    demo_runs,
    demo_workers,
    demo_invocations,
    demo_samples,
)


@pytest.fixture
def custom_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


def rows(connection: Connection, tables: tuple[Table, ...]) -> dict[str, Any]:
    return {
        table.name: [
            dict(row)
            for row in connection.execute(
                select(table).order_by(*table.primary_key.columns)
            ).mappings()
        ]
        for table in tables
    }


def fresh_run(
    connection: Connection, scenario: str | None = "custom"
) -> tuple[UUID, dict[str, Any]]:
    definition = normalize_custom(
        WorkflowDefinition(
            name="custom_schema_" + uuid4().hex,
            tasks=(TaskDefinition(task_id="Start", task_type="demo.observe"),),
        )
    )
    version = WorkflowRepository(connection).publish(definition)
    run_id = RunRepository(connection).create(version.id).run.id
    if scenario is not None:
        connection.execute(demo_runs.insert().values(run_id=run_id, scenario=scenario))
    return run_id, definition.model_dump(mode="json")


def legacy_evidence(connection: Connection) -> None:
    # Schema fixtures only; these rows are not claimed as a real Handler execution.
    original = seed(connection)
    run_id = connection.scalar(select(workflow_runs.c.id))
    connection.execute(demo_runs.insert().values(run_id=run_id, scenario="parallel"))
    connection.execute(
        demo_workers.insert().values(
            worker_session_id=original["worker_session_id"], run_id=run_id
        )
    )
    invocation = uuid4()
    connection.execute(
        demo_invocations.insert().values(
            id=invocation,
            attempt_id=original["attempt_id"],
            clock_domain="schema-fixture",
        )
    )
    connection.execute(
        demo_samples.insert().values(
            invocation_id=invocation, sequence=0, phase="START", monotonic_ns=10
        )
    )


def test_upgrade_preserves_populated_0011_history_and_matches_metadata(
    engine: Engine, migration_schema: str
) -> None:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "0011")
        legacy_evidence(connection)
        before = rows(connection, HISTORY)
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "head")
        command.check(config)
        assert MigrationContext.configure(connection).get_current_heads() == ("0012",)
        assert rows(connection, HISTORY) == before
        assert (
            connection.scalar(select(func.count()).select_from(demo_custom_submissions))
            == 0
        )


def test_new_and_legacy_membership_names_and_receipt_metadata(
    engine: Engine, custom_schema: str
) -> None:
    with transaction(engine, custom_schema) as connection:
        for scenario in ("parallel", "distribution", "recovery", "custom"):
            fresh_run(connection, scenario)
        with pytest.raises(DBAPIError) as error, connection.begin_nested():
            fresh_run(connection, "arbitrary")
        assert getattr(error.value.orig, "sqlstate", None) == "23514"
        reflected = inspect(connection)
        columns = reflected.get_columns("demo_custom_submissions", schema=custom_schema)
        assert {column["name"] for column in columns} == {
            "idempotency_key",
            "definition",
            "run_id",
            "created_at",
        }
        assert all(not column["nullable"] for column in columns)
        default = next(
            column["default"] for column in columns if column["name"] == "created_at"
        )
        assert default is not None and "clock_timestamp()" in default
        key = next(column for column in columns if column["name"] == "idempotency_key")
        assert isinstance(key["type"], String) and key["type"].length == 128
        assert (
            connection.scalar(
                text(
                    "SELECT collname FROM pg_attribute a JOIN pg_collation c "
                    "ON c.oid = a.attcollation "
                    "WHERE a.attrelid = CAST(:table_name AS regclass) "
                    "AND a.attname = 'idempotency_key'"
                ),
                {"table_name": f'"{custom_schema}".demo_custom_submissions'},
            )
            == "C"
        )
        assert reflected.get_pk_constraint(
            "demo_custom_submissions", schema=custom_schema
        )["constrained_columns"] == ["idempotency_key"]
        foreign_keys = reflected.get_foreign_keys(
            "demo_custom_submissions", schema=custom_schema
        )
        assert len(foreign_keys) == 1
        assert foreign_keys[0]["referred_table"] == "demo_runs"
        assert foreign_keys[0]["constrained_columns"] == ["run_id"]
        assert foreign_keys[0]["options"]["ondelete"] == "RESTRICT"


def test_receipts_have_db_timestamps_case_sensitive_keys_and_unique_runs(
    engine: Engine, custom_schema: str
) -> None:
    with transaction(engine, custom_schema) as connection:
        before = connection.scalar(select(func.clock_timestamp()))
        original, definition = fresh_run(connection)
        connection.execute(
            demo_custom_submissions.insert().values(
                idempotency_key="Request.A", definition=definition, run_id=original
            )
        )
        for key in ("request.a", "A" * 128):
            run_id, body = fresh_run(connection)
            connection.execute(
                demo_custom_submissions.insert().values(
                    idempotency_key=key, definition=body, run_id=run_id
                )
            )
        after = connection.scalar(select(func.clock_timestamp()))
        stamps = connection.scalars(select(demo_custom_submissions.c.created_at)).all()
        assert before is not None and after is not None
        assert len(stamps) == 3 and all(before <= stamp <= after for stamp in stamps)
        other, body = fresh_run(connection)
        for key, run_id in (("Request.A", other), ("different-key", original)):
            with pytest.raises(DBAPIError) as error, connection.begin_nested():
                connection.execute(
                    demo_custom_submissions.insert().values(
                        idempotency_key=key, definition=body, run_id=run_id
                    )
                )
            assert getattr(error.value.orig, "sqlstate", None) == "23505"
        assert (
            connection.scalar(select(func.count()).select_from(demo_custom_submissions))
            == 3
        )


@pytest.mark.parametrize("key", ["", " leading", "A\n", "é", "A" * 129])
def test_invalid_receipt_keys_are_rejected(
    engine: Engine, custom_schema: str, key: str
) -> None:
    with transaction(engine, custom_schema) as connection:
        run_id, definition = fresh_run(connection)
        with pytest.raises(DBAPIError) as error, connection.begin_nested():
            connection.execute(
                demo_custom_submissions.insert().values(
                    idempotency_key=key, definition=definition, run_id=run_id
                )
            )
        assert getattr(error.value.orig, "sqlstate", None) in {"23514", "22001"}


@pytest.mark.parametrize(
    ("field", "value", "state"),
    [
        ("idempotency_key", None, "23502"),
        ("run_id", None, "23502"),
        ("definition", null(), "23502"),
        ("definition", JSON.NULL, "23514"),
        ("definition", [], "23514"),
        ("created_at", None, "23502"),
        ("created_at", "infinity", "23514"),
        ("created_at", "-infinity", "23514"),
    ],
    ids=[
        "null-key",
        "null-run",
        "sql-null-body",
        "json-null-body",
        "array-body",
        "null-time",
        "infinite-time",
        "negative-infinite-time",
    ],
)
def test_receipt_required_object_and_finite_timestamp_constraints(
    engine: Engine, custom_schema: str, field: str, value: Any, state: str
) -> None:
    with transaction(engine, custom_schema) as connection:
        run_id, definition = fresh_run(connection)
        values = {
            "idempotency_key": "valid-key",
            "run_id": run_id,
            "definition": definition,
        }
        values[field] = value
        with pytest.raises(DBAPIError) as error, connection.begin_nested():
            connection.execute(demo_custom_submissions.insert().values(**values))
        assert getattr(error.value.orig, "sqlstate", None) == state


def test_receipt_foreign_key_requires_demo_membership(
    engine: Engine, custom_schema: str
) -> None:
    with transaction(engine, custom_schema) as connection:
        ordinary, definition = fresh_run(connection, None)
        for run_id in (ordinary, uuid4()):
            with pytest.raises(DBAPIError) as error, connection.begin_nested():
                connection.execute(
                    demo_custom_submissions.insert().values(
                        idempotency_key=uuid4().hex,
                        definition=definition,
                        run_id=run_id,
                    )
                )
            assert getattr(error.value.orig, "sqlstate", None) == "23503"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE demo_custom_submissions SET definition = definition",
        "DELETE FROM demo_custom_submissions",
        "TRUNCATE demo_custom_submissions",
    ],
)
def test_receipts_are_append_only(
    engine: Engine, custom_schema: str, statement: str
) -> None:
    with transaction(engine, custom_schema) as connection:
        run_id, definition = fresh_run(connection)
        connection.execute(
            demo_custom_submissions.insert().values(
                idempotency_key="retained", definition=definition, run_id=run_id
            )
        )
        before = rows(connection, (demo_custom_submissions,))
        with pytest.raises(DBAPIError) as error, connection.begin_nested():
            connection.execute(text(statement))
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        assert rows(connection, (demo_custom_submissions,)) == before


@pytest.mark.parametrize(
    ("scenario", "receipt"), [("custom", False), ("custom", True), ("parallel", True)]
)
def test_downgrade_refuses_any_custom_history_and_rolls_back(
    engine: Engine, custom_schema: str, scenario: str, receipt: bool
) -> None:
    preserved = (*HISTORY, demo_custom_submissions)
    with transaction(engine, custom_schema) as connection:
        run_id, definition = fresh_run(connection, scenario)
        if receipt:
            connection.execute(
                demo_custom_submissions.insert().values(
                    idempotency_key="retained", definition=definition, run_id=run_id
                )
            )
        before = rows(connection, preserved)
    with pytest.raises(CommandError, match="Database migration command failed"):
        with transaction(engine, custom_schema) as connection:
            command.downgrade(migration_config(connection, custom_schema), "0011")
    with transaction(engine, custom_schema) as connection:
        assert MigrationContext.configure(connection).get_current_heads() == ("0012",)
        assert rows(connection, preserved) == before
        command.check(migration_config(connection, custom_schema))


def test_downgrade_without_custom_history_preserves_legacy_and_reupgrades(
    engine: Engine, custom_schema: str
) -> None:
    with transaction(engine, custom_schema) as connection:
        legacy_evidence(connection)
        before = rows(connection, HISTORY)
    with transaction(engine, custom_schema) as connection:
        command.downgrade(migration_config(connection, custom_schema), "0011")
        assert MigrationContext.configure(connection).get_current_heads() == ("0011",)
        assert not inspect(connection).has_table(
            "demo_custom_submissions", schema=custom_schema
        )
        assert rows(connection, HISTORY) == before
        with pytest.raises(DBAPIError) as error, connection.begin_nested():
            fresh_run(connection, "custom")
        assert getattr(error.value.orig, "sqlstate", None) == "23514"
    with transaction(engine, custom_schema) as connection:
        config = migration_config(connection, custom_schema)
        command.upgrade(config, "head")
        command.check(config)
        assert rows(connection, HISTORY) == before
        assert inspect(connection).has_table(
            "demo_custom_submissions", schema=custom_schema
        )
