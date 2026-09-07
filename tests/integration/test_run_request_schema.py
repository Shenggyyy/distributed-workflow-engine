"""Verify durable request-binding invariants against migrated PostgreSQL."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, func, inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    run_creation_requests,
    task_runs,
    workflow_runs,
)

pytestmark = pytest.mark.integration


@contextmanager
def transaction(engine: Engine, schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        migration_config(connection, schema)
        yield connection


def publish(connection: Connection) -> UUID:
    return (
        WorkflowRepository(connection)
        .publish(
            WorkflowDefinition(
                name="demo", tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),)
            )
        )
        .id
    )


@pytest.fixture
def request_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


@pytest.fixture
def version_id(engine: Engine, request_schema: str) -> UUID:
    with transaction(engine, request_schema) as connection:
        return publish(connection)


def bind(connection: Connection, key: str, version_id: UUID, run_id: UUID) -> None:
    connection.execute(
        run_creation_requests.insert().values(
            idempotency_key=key, workflow_version_id=version_id, run_id=run_id
        )
    )


@pytest.mark.parametrize("key", ["a", "0", "ABC_123.:-", "a" * 128])
def test_valid_keys_and_timestamp(
    engine: Engine, request_schema: str, version_id: UUID, key: str
) -> None:
    with transaction(engine, request_schema) as connection:
        run = RunRepository(connection).create(version_id).run
        bind(connection, key, version_id, run.id)
    with transaction(engine, request_schema) as connection:
        row = connection.execute(select(run_creation_requests)).one()
        assert row.idempotency_key == key
        assert row.run_id == run.id
        assert row.workflow_version_id == version_id
        assert row.created_at.tzinfo is not None


@pytest.mark.parametrize(
    "key", ["", " ", "a b", "a\n", "a/b", "-a", "_a", "中文", "a" * 129, None]
)
def test_invalid_keys_are_rejected(
    engine: Engine, request_schema: str, version_id: UUID, key: str | None
) -> None:
    with pytest.raises(DBAPIError):
        with transaction(engine, request_schema) as connection:
            connection.execute(
                run_creation_requests.insert().values(
                    idempotency_key=key, workflow_version_id=version_id, run_id=uuid4()
                )
            )
    with transaction(engine, request_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(run_creation_requests)
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize("field", ["run_id", "workflow_version_id"])
def test_references_cannot_be_null(
    engine: Engine, request_schema: str, version_id: UUID, field: str
) -> None:
    values: dict[str, object] = {
        "idempotency_key": "key",
        "workflow_version_id": version_id,
        "run_id": uuid4(),
    }
    values[field] = None
    with pytest.raises(IntegrityError) as error:
        with transaction(engine, request_schema) as connection:
            connection.execute(run_creation_requests.insert().values(**values))
    assert getattr(error.value.orig, "sqlstate", None) == "23502"


def test_can_reserve_key_before_run_exists_in_same_transaction(
    engine: Engine, request_schema: str, version_id: UUID
) -> None:
    run_id = uuid4()
    with transaction(engine, request_schema) as connection:
        bind(connection, "reserved", version_id, run_id)
        # The reference is pending, not checked at INSERT or savepoint release.
        with connection.begin_nested():
            assert (
                connection.execute(select(run_creation_requests.c.run_id)).scalar_one()
                == run_id
            )
        with transaction(engine, request_schema) as observer:
            assert (
                observer.execute(
                    select(func.count()).select_from(run_creation_requests)
                ).scalar_one()
                == 0
            )
        # This is a schema test. The FK proves identity, not full DAG initialization.
        connection.execute(
            workflow_runs.insert().values(id=run_id, workflow_version_id=version_id)
        )
    with transaction(engine, request_schema) as connection:
        assert (
            connection.execute(select(run_creation_requests.c.run_id)).scalar_one()
            == run_id
        )


@pytest.mark.parametrize("wrong_version", [False, True])
def test_commit_rejects_missing_or_mismatched_run_and_releases_key(
    engine: Engine, request_schema: str, version_id: UUID, wrong_version: bool
) -> None:
    with transaction(engine, request_schema) as connection:
        other_version = publish(connection)
        existing = RunRepository(connection).create(version_id).run
    insert_returned = False
    with pytest.raises(IntegrityError) as error:
        with transaction(engine, request_schema) as connection:
            bind(
                connection,
                "retryable",
                other_version if wrong_version else version_id,
                existing.id if wrong_version else uuid4(),
            )
            insert_returned = True
            # Unrelated runtime writes must also roll back when COMMIT fails.
            RunRepository(connection).create(version_id)
    assert insert_returned
    assert getattr(error.value.orig, "sqlstate", None) == "23503"
    with transaction(engine, request_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(workflow_runs)
            ).scalar_one()
            == 1
        )
        assert (
            connection.execute(select(func.count()).select_from(task_runs)).scalar_one()
            == 1
        )
        assert (
            connection.execute(
                select(func.count()).select_from(run_creation_requests)
            ).scalar_one()
            == 0
        )
        bind(connection, "retryable", version_id, existing.id)


@pytest.mark.parametrize("collision", ["key", "run"])
def test_key_and_run_are_each_unique(
    engine: Engine, request_schema: str, version_id: UUID, collision: str
) -> None:
    with transaction(engine, request_schema) as connection:
        first = RunRepository(connection).create(version_id).run
        second = RunRepository(connection).create(version_id).run
        bind(connection, "key", version_id, first.id)
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                bind(
                    connection,
                    "key" if collision == "key" else "other",
                    version_id,
                    second.id if collision == "key" else first.id,
                )
        assert getattr(error.value.orig, "sqlstate", None) == "23505"
        # Distinct case-sensitive keys and distinct runs are permitted.
        bind(connection, "Key", version_id, second.id)


@pytest.mark.parametrize(
    "field", ["idempotency_key", "run_id", "workflow_version_id", "created_at"]
)
def test_every_binding_field_is_immutable(
    engine: Engine, request_schema: str, version_id: UUID, field: str
) -> None:
    with transaction(engine, request_schema) as connection:
        run = RunRepository(connection).create(version_id).run
        bind(connection, "key", version_id, run.id)
    with transaction(engine, request_schema) as connection:
        row = connection.execute(select(run_creation_requests)).mappings().one()
        values = {
            "idempotency_key": "changed",
            "run_id": uuid4(),
            "workflow_version_id": uuid4(),
            "created_at": row["created_at"] + timedelta(seconds=1),
        }
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(
                    run_creation_requests.update().values(**{field: values[field]})
                )
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        assert dict(
            connection.execute(select(run_creation_requests)).mappings().one()
        ) == dict(row)


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE run_creation_requests SET idempotency_key = idempotency_key",
        (
            "UPDATE run_creation_requests SET idempotency_key = idempotency_key "
            "WHERE false"
        ),
        "DELETE FROM run_creation_requests",
        "DELETE FROM run_creation_requests WHERE false",
        "TRUNCATE run_creation_requests",
    ],
)
def test_mutation_guard_rejects_noops_and_history_removal(
    engine: Engine, request_schema: str, version_id: UUID, statement: str
) -> None:
    with transaction(engine, request_schema) as connection:
        run = RunRepository(connection).create(version_id).run
        bind(connection, "key", version_id, run.id)
    with transaction(engine, request_schema) as connection:
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(text(statement))
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        assert (
            connection.execute(select(run_creation_requests.c.run_id)).scalar_one()
            == run.id
        )


def test_explicit_rollback_leaves_key_reusable(
    engine: Engine, request_schema: str, version_id: UUID
) -> None:
    with pytest.raises(RuntimeError, match="abort"):
        with transaction(engine, request_schema) as connection:
            run = RunRepository(connection).create(version_id).run
            bind(connection, "reusable", version_id, run.id)
            raise RuntimeError("abort")
    with transaction(engine, request_schema) as connection:
        replacement = RunRepository(connection).create(version_id).run
        bind(connection, "reusable", version_id, replacement.id)
    with transaction(engine, request_schema) as connection:
        assert (
            connection.execute(select(workflow_runs.c.id)).scalar_one()
            == replacement.id
        )


def test_concurrent_bindings_have_one_committed_winner(
    engine: Engine, request_schema: str, version_id: UUID
) -> None:
    barrier = Barrier(4, timeout=10)

    def attempt() -> str:
        try:
            with transaction(engine, request_schema) as connection:
                run = RunRepository(connection).create(version_id).run
                barrier.wait()
                bind(connection, "shared", version_id, run.id)
            return "accepted"
        except IntegrityError as error:
            assert getattr(error.orig, "sqlstate", None) == "23505"
            return "conflict"

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(attempt) for _ in range(4)]
        results = [future.result(timeout=20) for future in futures]
    assert results.count("accepted") == 1
    assert results.count("conflict") == 3
    with transaction(engine, request_schema) as connection:
        binding = connection.execute(select(run_creation_requests)).one()
        assert (
            connection.execute(select(workflow_runs.c.id)).scalar_one()
            == binding.run_id
        )
        assert (
            connection.execute(select(task_runs.c.run_id)).scalar_one()
            == binding.run_id
        )


def test_migration_preserves_existing_runs_and_downgrade_only_drops_bindings(
    engine: Engine, migration_schema: str
) -> None:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "0003")
        version_id = publish(connection)
        created = RunRepository(connection).create(version_id)
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "head")
        command.check(config)
        assert (
            connection.execute(select(workflow_runs.c.id)).scalar_one()
            == created.run.id
        )
        assert (
            connection.execute(select(task_runs.c.id)).scalar_one()
            == created.tasks[0].id
        )
        bind(connection, "retained", version_id, created.run.id)
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.downgrade(config, "0003")
        assert not inspect(connection).has_table(
            "run_creation_requests", schema=migration_schema
        )
        assert (
            connection.execute(
                text("SELECT to_regprocedure('dwe_reject_run_request_mutation()')")
            ).scalar_one()
            is None
        )
        assert not any(
            item["name"] == "uq_workflow_runs_id"
            for item in inspect(connection).get_unique_constraints(
                "workflow_runs", schema=migration_schema
            )
        )
        assert (
            connection.execute(select(workflow_runs.c.id)).scalar_one()
            == created.run.id
        )
        assert (
            connection.execute(select(task_runs.c.id)).scalar_one()
            == created.tasks[0].id
        )
        assert WorkflowRepository(connection).get_version(version_id) is not None
        command.upgrade(config, "head")
        command.check(config)
        assert (
            connection.execute(
                select(func.count()).select_from(run_creation_requests)
            ).scalar_one()
            == 0
        )
