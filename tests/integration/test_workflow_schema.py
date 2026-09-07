"""Verify the migrated workflow schema against PostgreSQL, not metadata.create_all."""

from collections.abc import Iterator
from datetime import datetime
from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.types import JSON

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.schema import workflow_versions, workflows

pytestmark = pytest.mark.integration


@pytest.fixture
def workflow_connection(engine: Engine, migration_schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    with engine.begin() as connection:
        migration_config(connection, migration_schema)
        yield connection


@pytest.fixture
def workflow_id(workflow_connection: Connection) -> UUID:
    value = uuid4()
    workflow_connection.execute(workflows.insert().values(id=value, name="demo"))
    return value


def definition() -> dict[str, object]:
    return WorkflowDefinition(
        name="demo", tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),)
    ).model_dump(mode="json")


def insert_version(connection: Connection, workflow_id: UUID, number: int = 1) -> UUID:
    version_id = uuid4()
    connection.execute(
        workflow_versions.insert().values(
            id=version_id,
            workflow_id=workflow_id,
            version_number=number,
            definition=definition(),
        )
    )
    return version_id


def test_definition_round_trip_and_timestamp_defaults(
    workflow_connection: Connection, workflow_id: UUID
) -> None:
    version_id = insert_version(workflow_connection, workflow_id)
    row = workflow_connection.execute(
        select(workflow_versions).where(workflow_versions.c.id == version_id)
    ).one()
    assert row.workflow_id == workflow_id
    assert row.version_number == 1
    assert WorkflowDefinition.model_validate(row.definition).name == "demo"
    assert isinstance(row.created_at, datetime)
    assert row.created_at.tzinfo is not None
    created_at = workflow_connection.execute(
        select(workflows.c.created_at)
    ).scalar_one()
    assert isinstance(created_at, datetime)
    assert created_at.tzinfo is not None


def test_workflow_names_are_unique_and_case_sensitive(
    workflow_connection: Connection,
) -> None:
    workflow_connection.execute(workflows.insert().values(id=uuid4(), name="demo"))
    workflow_connection.execute(workflows.insert().values(id=uuid4(), name="Demo"))
    with pytest.raises(IntegrityError) as error:
        with workflow_connection.begin_nested():
            workflow_connection.execute(
                workflows.insert().values(id=uuid4(), name="demo")
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23505"


@pytest.mark.parametrize(
    "name", ["", "with space", "1first", "slash/name", "A\n", "中"]
)
def test_invalid_workflow_names_rejected_by_database(
    workflow_connection: Connection, name: str
) -> None:
    with pytest.raises(IntegrityError) as error:
        with workflow_connection.begin_nested():
            workflow_connection.execute(
                workflows.insert().values(id=uuid4(), name=name)
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23514"


def test_version_number_unique_within_workflow(
    workflow_connection: Connection, workflow_id: UUID
) -> None:
    insert_version(workflow_connection, workflow_id)
    with pytest.raises(IntegrityError) as error:
        with workflow_connection.begin_nested():
            insert_version(workflow_connection, workflow_id)
    assert getattr(error.value.orig, "sqlstate", None) == "23505"
    insert_version(workflow_connection, workflow_id, 2)
    another = uuid4()
    workflow_connection.execute(workflows.insert().values(id=another, name="another"))
    insert_version(workflow_connection, another)


@pytest.mark.parametrize("number", [0, -1])
def test_version_number_must_be_positive(
    workflow_connection: Connection, workflow_id: UUID, number: int
) -> None:
    with pytest.raises(IntegrityError) as error:
        with workflow_connection.begin_nested():
            insert_version(workflow_connection, workflow_id, number)
    assert getattr(error.value.orig, "sqlstate", None) == "23514"


def test_version_requires_existing_workflow(workflow_connection: Connection) -> None:
    with pytest.raises(IntegrityError) as error:
        with workflow_connection.begin_nested():
            insert_version(workflow_connection, uuid4())
    assert getattr(error.value.orig, "sqlstate", None) == "23503"


@pytest.mark.parametrize("payload", [[], "text", 42, JSON.NULL])
def test_definition_must_be_a_json_object(
    workflow_connection: Connection, workflow_id: UUID, payload: object
) -> None:
    with pytest.raises(IntegrityError) as error:
        with workflow_connection.begin_nested():
            workflow_connection.execute(
                workflow_versions.insert().values(
                    id=uuid4(),
                    workflow_id=workflow_id,
                    version_number=1,
                    definition=payload,
                )
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23514"


def test_definition_cannot_be_sql_null(
    workflow_connection: Connection, workflow_id: UUID
) -> None:
    with pytest.raises(IntegrityError) as error:
        with workflow_connection.begin_nested():
            workflow_connection.execute(
                workflow_versions.insert().values(
                    id=uuid4(),
                    workflow_id=workflow_id,
                    version_number=1,
                    definition=None,
                )
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23502"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE workflow_versions SET version_number = 2",
        "DELETE FROM workflow_versions",
        "TRUNCATE workflow_versions",
    ],
)
def test_versions_are_append_only(
    workflow_connection: Connection, workflow_id: UUID, statement: str
) -> None:
    version_id = insert_version(workflow_connection, workflow_id)
    with pytest.raises(DBAPIError) as error:
        with workflow_connection.begin_nested():
            workflow_connection.execute(text(statement))
    assert getattr(error.value.orig, "sqlstate", None) == "55000"
    row = workflow_connection.execute(select(workflow_versions)).one()
    assert row.id == version_id
    assert row.version_number == 1
    assert row.definition == definition()


def test_workflow_with_versions_cannot_be_deleted(
    workflow_connection: Connection, workflow_id: UUID
) -> None:
    insert_version(workflow_connection, workflow_id)
    with pytest.raises(IntegrityError) as error:
        with workflow_connection.begin_nested():
            workflow_connection.execute(
                workflows.delete().where(workflows.c.id == workflow_id)
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23001"


def test_upgrade_from_baseline_and_destructive_downgrade(
    engine: Engine, migration_schema: str
) -> None:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "0001")
        assert inspect(connection).get_table_names(schema=migration_schema) == [
            "alembic_version"
        ]
    with engine.begin() as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "head")
        workflow_id = uuid4()
        connection.execute(workflows.insert().values(id=workflow_id, name="demo"))
        insert_version(connection, workflow_id)
    with engine.begin() as connection:
        config = migration_config(connection, migration_schema)
        command.downgrade(config, "0001")
        assert inspect(connection).get_table_names(schema=migration_schema) == [
            "alembic_version"
        ]
        assert (
            connection.execute(
                text("SELECT to_regprocedure('dwe_reject_workflow_version_mutation()')")
            ).scalar_one()
            is None
        )
        command.upgrade(config, "head")
        command.check(config)
