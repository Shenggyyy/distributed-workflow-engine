"""Verify run initialization atomicity and isolation on real PostgreSQL."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, create_engine, func, select, text
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.runtime import RunStatus, TaskStatus
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import (
    CreatedRun,
    RunRepository,
    WorkflowVersionNotFoundError,
)
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    StoredDefinitionError,
    WorkflowRepository,
)
from workflow_engine.schema import (
    task_attempts,
    task_runs,
    workflow_runs,
    workflow_versions,
    workflows,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def run_schema(engine: Engine, migration_schema: str) -> str:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


@contextmanager
def transaction(engine: Engine, schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        migration_config(connection, schema)
        yield connection


def definition(shape: str = "diamond") -> WorkflowDefinition:
    dependencies: dict[str, tuple[str, ...]]
    if shape == "single":
        dependencies = {"A": ()}
    elif shape == "multiple_roots":
        dependencies = {"D": ("A", "B"), "B": (), "C": (), "A": ()}
    elif shape == "limit":
        dependencies = {f"T{i:04}": () for i in range(1000)}
    else:
        # Intentionally not topologically ordered in the submitted definition.
        dependencies = {"D": ("B", "C"), "C": ("B",), "B": ("A",), "A": ()}
    return WorkflowDefinition(
        name="demo",
        tasks=tuple(
            TaskDefinition(task_id=key, task_type="demo.echo", depends_on=parents)
            for key, parents in dependencies.items()
        ),
    )


def publish(engine: Engine, schema: str, shape: str = "diamond") -> UUID:
    with transaction(engine, schema) as connection:
        return WorkflowRepository(connection).publish(definition(shape)).id


def assert_no_runtime(connection: Connection) -> None:
    for table in (workflow_runs, task_runs, task_attempts):
        assert (
            connection.execute(select(func.count()).select_from(table)).scalar_one()
            == 0
        )


def assert_committed(connection: Connection, created: CreatedRun) -> None:
    row = connection.execute(
        select(workflow_runs).where(workflow_runs.c.id == created.run.id)
    ).one()
    assert row.workflow_version_id == created.run.workflow_version_id
    assert row.status == created.run.status.value == "RUNNING"
    assert row.created_at.tzinfo is not None
    tasks = connection.execute(
        select(task_runs).where(task_runs.c.run_id == row.id)
    ).all()
    assert {task.id for task in tasks} == {task.id for task in created.tasks}
    assert {task.task_key: task.status for task in tasks} == {
        task.task_key: task.status.value for task in created.tasks
    }
    assert all(task.created_at.tzinfo is not None for task in tasks)
    assert (
        connection.execute(select(func.count()).select_from(task_attempts)).scalar_one()
        == 0
    )


@pytest.mark.parametrize("shape", ["single", "diamond", "multiple_roots", "limit"])
def test_complete_initialization_is_visible_only_after_commit(
    engine: Engine, run_schema: str, shape: str
) -> None:
    version_id = publish(engine, run_schema, shape)
    graph = definition(shape).dag()
    with transaction(engine, run_schema) as connection:
        created = RunRepository(connection).create(version_id)
        assert created.run.status is RunStatus.RUNNING
        assert tuple(task.task_key for task in created.tasks) == graph.topological_order
        assert {
            task.task_key for task in created.tasks if task.status is TaskStatus.READY
        } == set(graph.roots)
        assert all(
            task.status is TaskStatus.PENDING
            for task in created.tasks
            if task.task_key not in graph.roots
        )
        assert len({task.id for task in created.tasks}) == len(graph.topological_order)
        assert all(task.run_id == created.run.id for task in created.tasks)
        with transaction(engine, run_schema) as observer:
            assert_no_runtime(observer)
    with transaction(engine, run_schema) as connection:
        assert_committed(connection, created)


def test_pins_requested_version_and_repeated_calls_create_fresh_runs(
    engine: Engine, run_schema: str
) -> None:
    old_id = publish(engine, run_schema, "diamond")
    new_id = publish(engine, run_schema, "single")
    with transaction(engine, run_schema) as connection:
        repository = RunRepository(connection)
        old = repository.create(old_id)
        repeated = repository.create(old_id)
        new = repository.create(new_id)
    assert len({old.run.id, repeated.run.id, new.run.id}) == 3
    assert len(old.tasks) == len(repeated.tasks) == 4
    assert len(new.tasks) == 1
    assert {task.id for task in old.tasks}.isdisjoint(
        task.id for task in repeated.tasks
    )
    with transaction(engine, run_schema) as connection:
        for created in (old, repeated, new):
            assert_committed(connection, created)


def test_caller_failure_rolls_back_the_entire_initialization(
    engine: Engine, run_schema: str
) -> None:
    version_id = publish(engine, run_schema)
    with pytest.raises(RuntimeError, match="caller failed"):
        with transaction(engine, run_schema) as connection:
            RunRepository(connection).create(version_id)
            raise RuntimeError("caller failed")
    with transaction(engine, run_schema) as connection:
        assert_no_runtime(connection)
        assert WorkflowRepository(connection).get_version(version_id) is not None


@pytest.mark.parametrize(
    ("table", "event"),
    [("task_runs", "INSERT"), ("task_runs", "UPDATE"), ("workflow_runs", "UPDATE")],
)
def test_database_failure_cannot_leave_partial_initialization(
    engine: Engine, run_schema: str, table: str, event: str
) -> None:
    version_id = publish(engine, run_schema)
    with transaction(engine, run_schema) as connection:
        connection.execute(
            text("""
            CREATE FUNCTION reject_initialization() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='injected failure';
            END;
            $$
        """)
        )
        # Identifiers are fixed test parameters in this fixture-owned schema.
        connection.execute(
            text(f"""
            CREATE TRIGGER reject_initialization
            BEFORE {event} ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION reject_initialization()
        """)
        )
    with pytest.raises(DBAPIError) as error:
        with transaction(engine, run_schema) as connection:
            RunRepository(connection).create(version_id)
    assert getattr(error.value.orig, "sqlstate", None) == "55000"
    with transaction(engine, run_schema) as connection:
        assert_no_runtime(connection)
        connection.execute(text(f"DROP TRIGGER reject_initialization ON {table}"))
        created = RunRepository(connection).create(version_id)
    with transaction(engine, run_schema) as connection:
        assert_committed(connection, created)


def test_missing_version_does_not_write_runtime_records(
    engine: Engine, run_schema: str
) -> None:
    with transaction(engine, run_schema) as connection:
        with pytest.raises(WorkflowVersionNotFoundError, match="was not found"):
            RunRepository(connection).create(uuid4())
    with transaction(engine, run_schema) as connection:
        assert_no_runtime(connection)


@pytest.mark.parametrize("corruption", ["fields", "cycle", "future_schema"])
def test_invalid_stored_dag_is_rejected_before_runtime_writes(
    engine: Engine, run_schema: str, corruption: str
) -> None:
    payload = definition().model_dump(mode="json")
    if corruption == "fields":
        payload = {}
    elif corruption == "cycle":
        payload["tasks"][0]["depends_on"] = ["D"]
    else:
        payload["schema_version"] = 2
    version_id, workflow_id = uuid4(), uuid4()
    with transaction(engine, run_schema) as connection:
        connection.execute(workflows.insert().values(id=workflow_id, name="corrupt"))
        connection.execute(
            workflow_versions.insert().values(
                id=version_id,
                workflow_id=workflow_id,
                version_number=1,
                definition=payload,
            )
        )
    with transaction(engine, run_schema) as connection:
        with pytest.raises(StoredDefinitionError):
            RunRepository(connection).create(version_id)
    with transaction(engine, run_schema) as connection:
        assert_no_runtime(connection)


def test_concurrent_creators_produce_complete_independent_runs(
    engine: Engine, run_schema: str
) -> None:
    version_id = publish(engine, run_schema)
    barrier = Barrier(4, timeout=10)

    def create() -> CreatedRun:
        with transaction(engine, run_schema) as connection:
            repository = RunRepository(connection)
            barrier.wait()
            created = repository.create(version_id)
        return created

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(create) for _ in range(4)]
        runs = [future.result(timeout=20) for future in futures]
    assert len({created.run.id for created in runs}) == 4
    assert len({task.id for created in runs for task in created.tasks}) == 16
    with transaction(engine, run_schema) as connection:
        for created in runs:
            assert_committed(connection, created)
        assert (
            connection.execute(
                select(func.count()).select_from(workflow_runs)
            ).scalar_one()
            == 4
        )


def test_publishing_next_version_does_not_block_pinned_run_creation(
    engine: Engine, run_schema: str
) -> None:
    version_id = publish(engine, run_schema)
    with transaction(engine, run_schema) as publisher:
        WorkflowRepository(publisher).publish(definition("single"))
        with transaction(engine, run_schema) as creator:
            creator.execute(text("SET LOCAL lock_timeout = '250ms'"))
            created = RunRepository(creator).create(version_id)
    with transaction(engine, run_schema) as connection:
        assert_committed(connection, created)
        assert len(created.tasks) == 4


@pytest.mark.parametrize("end", ["commit", "rollback"])
def test_repository_rejects_ended_or_replaced_transaction(
    engine: Engine, end: str
) -> None:
    with engine.connect() as connection:
        with pytest.raises(RepositoryTransactionError):
            RunRepository(connection)
        tx = connection.begin()
        repository = RunRepository(connection)
        if end == "commit":
            tx.commit()
        else:
            tx.rollback()
        with pytest.raises(RepositoryTransactionError):
            repository.create(uuid4())
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                repository.create(uuid4())


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE", "AUTOCOMMIT"])
def test_repository_rejects_unsupported_isolation(
    engine: Engine, isolation: str
) -> None:
    with engine.connect().execution_options(isolation_level=isolation) as connection:
        with connection.begin():
            with pytest.raises(RepositoryTransactionError, match="READ COMMITTED"):
                RunRepository(connection)


def test_repository_rejects_engine_level_autocommit(engine: Engine) -> None:
    unsafe = create_engine(engine.url, isolation_level="AUTOCOMMIT")
    try:
        with unsafe.begin() as connection:
            with pytest.raises(RepositoryTransactionError, match="READ COMMITTED"):
                RunRepository(connection)
    finally:
        unsafe.dispose()
