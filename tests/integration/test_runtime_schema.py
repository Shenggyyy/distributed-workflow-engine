"""Verify runtime schema invariants and lifecycle guards using migrated PostgreSQL."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from enum import Enum
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from alembic import command
from pydantic import BaseModel
from sqlalchemy import Connection, Engine, Table, func, inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.runtime import (
    AttemptEvent,
    AttemptStatus,
    InvalidStateTransition,
    RunEvent,
    RunStatus,
    TaskAttempt,
    TaskEvent,
    TaskRun,
    TaskStatus,
    WorkflowRun,
)
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import task_attempts, task_runs, workflow_runs

pytestmark = pytest.mark.integration

TABLES: dict[str, Table] = {
    "workflow_runs": workflow_runs,
    "task_runs": task_runs,
    "task_attempts": task_attempts,
}
MODELS: dict[str, type[BaseModel]] = {
    "workflow_runs": WorkflowRun,
    "task_runs": TaskRun,
    "task_attempts": TaskAttempt,
}
STATUSES: dict[str, type[Enum]] = {
    "workflow_runs": RunStatus,
    "task_runs": TaskStatus,
    "task_attempts": AttemptStatus,
}
EVENTS: dict[str, type[Enum]] = {
    "workflow_runs": RunEvent,
    "task_runs": TaskEvent,
    "task_attempts": AttemptEvent,
}


@pytest.fixture
def runtime_connection(engine: Engine, migration_schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    with engine.begin() as connection:
        migration_config(connection, migration_schema)
        yield connection


def create_parents(connection: Connection) -> dict[str, UUID]:
    version = WorkflowRepository(connection).publish(
        WorkflowDefinition(
            name="demo", tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),)
        )
    )
    run_id, task_id = uuid4(), uuid4()
    connection.execute(
        workflow_runs.insert().values(id=run_id, workflow_version_id=version.id)
    )
    connection.execute(
        task_runs.insert().values(id=task_id, run_id=run_id, task_key="A")
    )
    return {"version_id": version.id, "run_id": run_id, "task_id": task_id}


@pytest.fixture
def parents(runtime_connection: Connection) -> dict[str, UUID]:
    return create_parents(runtime_connection)


def values_for(kind: str, parents: dict[str, UUID]) -> dict[str, object]:
    if kind == "workflow_runs":
        return {"id": uuid4(), "workflow_version_id": parents["version_id"]}
    if kind == "task_runs":
        return {"id": uuid4(), "run_id": parents["run_id"], "task_key": "B"}
    return {"id": uuid4(), "task_id": parents["task_id"], "attempt_number": 1}


def test_defaults_timestamps_and_domain_rehydration(
    runtime_connection: Connection, parents: dict[str, UUID]
) -> None:
    for kind, table in TABLES.items():
        values = values_for(kind, parents)
        row = (
            runtime_connection.execute(table.insert().values(**values).returning(table))
            .mappings()
            .one()
        )
        assert row["status"] == ("RUNNING" if kind == "task_attempts" else "PENDING")
        assert row["created_at"].tzinfo is not None
        snapshot = MODELS[kind].model_validate(
            {**values, "status": STATUSES[kind](row["status"])}
        )
        assert snapshot.model_dump(mode="json")["id"] == str(row["id"])


@pytest.mark.parametrize("kind", TABLES)
@pytest.mark.parametrize("status", ["unknown", "pending", "", None])
def test_invalid_or_null_status_is_rejected(
    runtime_connection: Connection,
    parents: dict[str, UUID],
    kind: str,
    status: str | None,
) -> None:
    with pytest.raises(IntegrityError) as error:
        with runtime_connection.begin_nested():
            runtime_connection.execute(
                TABLES[kind].insert().values(**values_for(kind, parents), status=status)
            )
    assert getattr(error.value.orig, "sqlstate", None) == (
        "23502" if status is None else "23514"
    )


@pytest.mark.parametrize("kind", TABLES)
def test_status_update_matrix_matches_domain_events(
    runtime_connection: Connection, parents: dict[str, UUID], kind: str
) -> None:
    table = TABLES[kind]
    for origin in STATUSES[kind]:
        base = values_for(kind, parents)
        snapshot = MODELS[kind].model_validate({**base, "status": origin})
        # Parameterized over the three public domain models, not their private maps.
        transition = snapshot.__getattribute__("transition")
        allowed = {origin.value}  # SQL no-op updates are allowed.
        for event in EVENTS[kind]:
            try:
                allowed.add(transition(event).status.value)
            except InvalidStateTransition:
                pass
        for target in STATUSES[kind]:
            with runtime_connection.begin_nested() as case:
                runtime_connection.execute(
                    table.insert().values(**base, status=origin.value)
                )
                update = (
                    table.update()
                    .where(table.c.id == base["id"])
                    .values(status=target.value)
                )
                if target.value in allowed:
                    runtime_connection.execute(update)
                    expected = target.value
                else:
                    with pytest.raises(DBAPIError) as error:
                        with runtime_connection.begin_nested():
                            runtime_connection.execute(update)
                    assert getattr(error.value.orig, "sqlstate", None) == "55000"
                    expected = origin.value
                assert (
                    runtime_connection.execute(
                        select(table.c.status).where(table.c.id == base["id"])
                    ).scalar_one()
                    == expected
                )
                case.rollback()


@pytest.mark.parametrize(
    ("kind", "field"),
    [
        ("workflow_runs", "workflow_version_id"),
        ("task_runs", "run_id"),
        ("task_attempts", "task_id"),
    ],
)
def test_runtime_foreign_keys_require_existing_parent(
    runtime_connection: Connection, parents: dict[str, UUID], kind: str, field: str
) -> None:
    values = values_for(kind, parents)
    values[field] = uuid4()
    with pytest.raises(IntegrityError) as error:
        with runtime_connection.begin_nested():
            runtime_connection.execute(TABLES[kind].insert().values(**values))
    assert getattr(error.value.orig, "sqlstate", None) == "23503"


def test_task_key_unique_per_run_but_reusable_in_other_runs(
    runtime_connection: Connection, parents: dict[str, UUID]
) -> None:
    with pytest.raises(IntegrityError) as error:
        with runtime_connection.begin_nested():
            runtime_connection.execute(
                task_runs.insert().values(
                    id=uuid4(), run_id=parents["run_id"], task_key="A"
                )
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23505"
    another_run = uuid4()
    runtime_connection.execute(
        workflow_runs.insert().values(
            id=another_run, workflow_version_id=parents["version_id"]
        )
    )
    runtime_connection.execute(
        task_runs.insert().values(id=uuid4(), run_id=another_run, task_key="A")
    )
    runtime_connection.execute(
        task_runs.insert().values(id=uuid4(), run_id=parents["run_id"], task_key="a")
    )


@pytest.mark.parametrize("key", ["", "1bad", "with space", "A" * 65])
def test_task_key_format(
    runtime_connection: Connection, parents: dict[str, UUID], key: str
) -> None:
    with pytest.raises(DBAPIError):
        with runtime_connection.begin_nested():
            runtime_connection.execute(
                task_runs.insert().values(
                    id=uuid4(), run_id=parents["run_id"], task_key=key
                )
            )


@pytest.mark.parametrize("number", [0, -1, None])
def test_attempt_number_positive_and_not_null(
    runtime_connection: Connection, parents: dict[str, UUID], number: int | None
) -> None:
    with pytest.raises(IntegrityError) as error:
        with runtime_connection.begin_nested():
            runtime_connection.execute(
                task_attempts.insert().values(
                    id=uuid4(), task_id=parents["task_id"], attempt_number=number
                )
            )
    assert getattr(error.value.orig, "sqlstate", None) == (
        "23502" if number is None else "23514"
    )


def test_attempt_number_unique_per_task(
    runtime_connection: Connection, parents: dict[str, UUID]
) -> None:
    for number in (1, 2):
        runtime_connection.execute(
            task_attempts.insert().values(
                id=uuid4(),
                task_id=parents["task_id"],
                attempt_number=number,
                status="FAILED",
            )
        )
    with pytest.raises(IntegrityError) as error:
        with runtime_connection.begin_nested():
            runtime_connection.execute(
                task_attempts.insert().values(
                    id=uuid4(),
                    task_id=parents["task_id"],
                    attempt_number=1,
                    status="SUCCEEDED",
                )
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23505"
    other_task = uuid4()
    runtime_connection.execute(
        task_runs.insert().values(id=other_task, run_id=parents["run_id"], task_key="B")
    )
    runtime_connection.execute(
        task_attempts.insert().values(id=uuid4(), task_id=other_task, attempt_number=1)
    )


def test_one_running_attempt_and_release_after_terminal_outcome(
    runtime_connection: Connection, parents: dict[str, UUID]
) -> None:
    first = uuid4()
    runtime_connection.execute(
        task_attempts.insert().values(
            id=first, task_id=parents["task_id"], attempt_number=1
        )
    )
    with pytest.raises(IntegrityError) as error:
        with runtime_connection.begin_nested():
            runtime_connection.execute(
                task_attempts.insert().values(
                    id=uuid4(), task_id=parents["task_id"], attempt_number=2
                )
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23505"
    runtime_connection.execute(
        task_attempts.update().where(task_attempts.c.id == first).values(status="LOST")
    )
    runtime_connection.execute(
        task_attempts.insert().values(
            id=uuid4(), task_id=parents["task_id"], attempt_number=2
        )
    )


@pytest.mark.parametrize("kind", TABLES)
def test_identity_fields_cannot_be_changed(
    runtime_connection: Connection, parents: dict[str, UUID], kind: str
) -> None:
    table = TABLES[kind]
    base = values_for(kind, parents)
    row = (
        runtime_connection.execute(table.insert().values(**base).returning(table))
        .mappings()
        .one()
    )
    changes: dict[str, object] = {
        "id": uuid4(),
        "created_at": row["created_at"] + timedelta(seconds=1),
    }
    if kind == "workflow_runs":
        changes["workflow_version_id"] = uuid4()
    elif kind == "task_runs":
        changes.update(run_id=uuid4(), task_key="changed")
    else:
        changes.update(task_id=uuid4(), attempt_number=2)
    for field, value in changes.items():
        with pytest.raises(DBAPIError) as error:
            with runtime_connection.begin_nested():
                runtime_connection.execute(
                    table.update()
                    .where(table.c.id == base["id"])
                    .values(**{field: value})
                )
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
    assert dict(
        runtime_connection.execute(select(table).where(table.c.id == base["id"]))
        .mappings()
        .one()
    ) == dict(row)


@pytest.mark.parametrize("kind", TABLES)
@pytest.mark.parametrize("operation", ["DELETE FROM", "TRUNCATE"])
def test_history_cannot_be_deleted_or_truncated(
    runtime_connection: Connection, parents: dict[str, UUID], kind: str, operation: str
) -> None:
    runtime_connection.execute(
        task_attempts.insert().values(**values_for("task_attempts", parents))
    )
    suffix = " CASCADE" if operation == "TRUNCATE" else ""
    with pytest.raises(DBAPIError) as error:
        with runtime_connection.begin_nested():
            runtime_connection.execute(text(f"{operation} {kind}{suffix}"))
    assert getattr(error.value.orig, "sqlstate", None) == "55000"
    assert (
        runtime_connection.execute(
            select(func.count()).select_from(TABLES[kind])
        ).scalar_one()
        == 1
    )


def test_concurrent_attempt_inserts_have_one_winner(
    engine: Engine, migration_schema: str
) -> None:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
        parents = create_parents(connection)
    barrier = Barrier(2, timeout=10)

    def insert_attempt(number: int) -> str:
        try:
            with engine.begin() as connection:
                migration_config(connection, migration_schema)
                barrier.wait()
                connection.execute(
                    task_attempts.insert().values(
                        id=uuid4(), task_id=parents["task_id"], attempt_number=number
                    )
                )
            return "accepted"
        except IntegrityError as error:
            assert getattr(error.orig, "sqlstate", None) == "23505"
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [executor.submit(insert_attempt, number) for number in (1, 2)]
        assert sorted(future.result(timeout=20) for future in results) == [
            "accepted",
            "conflict",
        ]
    with engine.begin() as connection:
        migration_config(connection, migration_schema)
        assert (
            connection.execute(
                select(func.count()).select_from(task_attempts)
            ).scalar_one()
            == 1
        )


def test_upgrade_and_downgrade_preserve_published_definitions(
    engine: Engine, migration_schema: str
) -> None:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "0002")
        published = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name="keep", tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),)
            )
        )
    with engine.begin() as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "head")
        assert WorkflowRepository(connection).get_version(published.id) == published
        create_parents(connection)
        command.check(config)
    with engine.begin() as connection:
        config = migration_config(connection, migration_schema)
        command.downgrade(config, "0002")
        assert WorkflowRepository(connection).get_version(published.id) == published
        assert set(inspect(connection).get_table_names(schema=migration_schema)) == {
            "alembic_version",
            "workflows",
            "workflow_versions",
        }
        for function in ("dwe_guard_runtime_update()", "dwe_reject_runtime_deletion()"):
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:function)"), {"function": function}
                ).scalar_one()
                is None
            )
        command.upgrade(config, "head")
        command.check(config)
