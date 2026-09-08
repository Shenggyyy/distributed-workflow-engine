"""Read-model correctness and statement-snapshot consistency on PostgreSQL."""

from collections.abc import Iterator
from contextlib import contextmanager
from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, event, text

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.dag import MAX_TASKS
from workflow_engine.domain.runtime import RunStatus, TaskStatus
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import (
    RunRepository,
    StoredRuntimeError,
)
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    WorkflowRepository,
)
from workflow_engine.schema import task_runs, workflow_runs

pytestmark = pytest.mark.integration


@contextmanager
def transaction(engine: Engine, schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        migration_config(connection, schema)
        yield connection


@pytest.fixture
def query_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


@pytest.fixture
def version_id(engine: Engine, query_schema: str) -> UUID:
    with transaction(engine, query_schema) as connection:
        return (
            WorkflowRepository(connection)
            .publish(
                WorkflowDefinition(
                    name="query",
                    tasks=(
                        TaskDefinition(task_id="Z", task_type="demo.echo"),
                        TaskDefinition(
                            task_id="A", task_type="demo.echo", depends_on=("Z",)
                        ),
                        TaskDefinition(task_id="a", task_type="demo.echo"),
                    ),
                )
            )
            .id
        )


@pytest.fixture
def run_id(engine: Engine, query_schema: str, version_id: UUID) -> UUID:
    with transaction(engine, query_schema) as connection:
        return (
            RunRepository(connection)
            .create_idempotent(version_id, idempotency_key="query")
            .run_id
        )


def advance_to_failure(connection: Connection, run_id: UUID) -> None:
    connection.execute(
        task_runs.update()
        .where(task_runs.c.run_id == run_id, task_runs.c.status == "PENDING")
        .values(status="READY")
    )
    connection.execute(
        task_runs.update().where(task_runs.c.run_id == run_id).values(status="RUNNING")
    )
    connection.execute(
        task_runs.update().where(task_runs.c.run_id == run_id).values(status="FAILED")
    )
    connection.execute(
        workflow_runs.update()
        .where(workflow_runs.c.id == run_id)
        .values(status="FAILED")
    )


def test_reads_pinned_metadata_and_sorted_tasks_without_cross_run_leakage(
    engine: Engine, query_schema: str, version_id: UUID, run_id: UUID
) -> None:
    with transaction(engine, query_schema) as connection:
        other = RunRepository(connection).create(version_id)
        assert other.run.id != run_id
    with transaction(engine, query_schema) as connection:
        repository = RunRepository(connection)
        summary = repository.get_run(run_id)
        snapshot = repository.get_run_with_tasks(run_id)
        assert summary is not None and snapshot is not None
        assert snapshot.run == summary
        assert summary.id == run_id and summary.workflow_version_id == version_id
        assert summary.status is RunStatus.RUNNING
        assert summary.created_at.tzinfo is not None
        # ASCII C ordering is intentionally not the DAG's topological ordering.
        assert tuple(task.task_key for task in snapshot.tasks) == ("A", "Z", "a")
        assert tuple(task.status for task in snapshot.tasks) == (
            TaskStatus.PENDING,
            TaskStatus.READY,
            TaskStatus.READY,
        )
        assert all(task.run_id == run_id for task in snapshot.tasks)
        assert all(task.created_at.tzinfo is not None for task in snapshot.tasks)
        assert {task.id for task in snapshot.tasks}.isdisjoint(
            task.id for task in other.tasks
        )


def test_missing_run_and_empty_existing_run_are_distinct(
    engine: Engine, query_schema: str, version_id: UUID
) -> None:
    with transaction(engine, query_schema) as connection:
        repository = RunRepository(connection)
        assert repository.get_run(uuid4()) is None
        assert repository.get_run_with_tasks(uuid4()) is None
        empty_id = uuid4()
        # Intentionally incomplete direct-SQL state for diagnostic reading.
        connection.execute(
            workflow_runs.insert().values(id=empty_id, workflow_version_id=version_id)
        )
        empty = repository.get_run_with_tasks(empty_id)
        assert empty is not None
        assert empty.run.status is RunStatus.PENDING
        assert empty.tasks == ()


def test_uncommitted_creation_is_visible_only_to_its_transaction(
    engine: Engine, query_schema: str, version_id: UUID
) -> None:
    with transaction(engine, query_schema) as connection:
        repository = RunRepository(connection)
        created = repository.create(version_id)
        assert repository.get_run(created.run.id) is not None
        assert repository.get_run_with_tasks(created.run.id) is not None
        with transaction(engine, query_schema) as observer:
            assert RunRepository(observer).get_run(created.run.id) is None
            assert RunRepository(observer).get_run_with_tasks(created.run.id) is None
    with transaction(engine, query_schema) as connection:
        assert RunRepository(connection).get_run_with_tasks(created.run.id) is not None


def test_both_reads_work_in_read_only_transaction(
    engine: Engine, query_schema: str, run_id: UUID
) -> None:
    with engine.begin() as connection:
        connection.execute(text("SET TRANSACTION READ ONLY"))
        migration_config(connection, query_schema)
        repository = RunRepository(connection)
        assert repository.get_run(run_id) is not None
        assert repository.get_run_with_tasks(run_id) is not None


def test_reader_does_not_wait_for_uncommitted_runtime_writer(
    engine: Engine, query_schema: str, run_id: UUID
) -> None:
    with transaction(engine, query_schema) as writer:
        advance_to_failure(writer, run_id)
        with transaction(engine, query_schema) as reader:
            reader.execute(text("SET LOCAL statement_timeout = '250ms'"))
            repository = RunRepository(reader)
            summary = repository.get_run(run_id)
            snapshot = repository.get_run_with_tasks(run_id)
            assert summary is not None and summary.status is RunStatus.RUNNING
            assert snapshot is not None and snapshot.run.status is RunStatus.RUNNING
            assert {task.status for task in snapshot.tasks} == {
                TaskStatus.PENDING,
                TaskStatus.READY,
            }
    with transaction(engine, query_schema) as reader:
        snapshot = RunRepository(reader).get_run_with_tasks(run_id)
        assert snapshot is not None and snapshot.run.status is RunStatus.FAILED
        assert {task.status for task in snapshot.tasks} == {TaskStatus.FAILED}


def test_one_statement_snapshot_survives_commit_between_execute_and_decode(
    engine: Engine, query_schema: str, run_id: UUID
) -> None:
    with transaction(engine, query_schema) as reader:
        repository = RunRepository(reader)
        statements: list[str] = []

        def commit_writer(
            connection: Connection,
            cursor: object,
            statement: str,
            parameters: object,
            context: object,
            executemany: bool,
        ) -> None:
            statements.append(statement)
            if len(statements) == 1:
                with transaction(engine, query_schema) as writer:
                    writer.execute(text("SET LOCAL statement_timeout = '250ms'"))
                    advance_to_failure(writer, run_id)

        event.listen(reader, "after_cursor_execute", commit_writer)
        try:
            old = repository.get_run_with_tasks(run_id)
            assert len(statements) == 1
            assert old is not None and old.run.status is RunStatus.RUNNING
            assert {task.status for task in old.tasks} == {
                TaskStatus.PENDING,
                TaskStatus.READY,
            }
            new = repository.get_run_with_tasks(run_id)
            assert len(statements) == 2
            assert new is not None and new.run.status is RunStatus.FAILED
            assert {task.status for task in new.tasks} == {TaskStatus.FAILED}
            # Already returned objects remain historical snapshots.
            assert old.run.status is RunStatus.RUNNING
        finally:
            event.remove(reader, "after_cursor_execute", commit_writer)


def test_reads_stored_run_status_without_aggregation(
    engine: Engine, query_schema: str, run_id: UUID
) -> None:
    with transaction(engine, query_schema) as connection:
        connection.execute(
            task_runs.update()
            .where(task_runs.c.run_id == run_id, task_runs.c.status == "PENDING")
            .values(status="READY")
        )
        connection.execute(
            task_runs.update()
            .where(task_runs.c.run_id == run_id)
            .values(status="RUNNING")
        )
        connection.execute(
            task_runs.update()
            .where(task_runs.c.run_id == run_id)
            .values(status="SUCCEEDED")
        )
    with transaction(engine, query_schema) as connection:
        snapshot = RunRepository(connection).get_run_with_tasks(run_id)
        assert snapshot is not None
        assert snapshot.run.status is RunStatus.RUNNING
        assert {task.status for task in snapshot.tasks} == {TaskStatus.SUCCEEDED}


@pytest.mark.parametrize("extra", [False, True])
def test_task_limit_is_inclusive_and_overflow_is_not_silently_truncated(
    engine: Engine, query_schema: str, extra: bool
) -> None:
    with transaction(engine, query_schema) as connection:
        version = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name="large",
                tasks=tuple(
                    TaskDefinition(task_id=f"T{i:04}", task_type="demo.echo")
                    for i in range(MAX_TASKS)
                ),
            )
        )
        created = RunRepository(connection).create(version.id)
        if extra:
            connection.execute(
                task_runs.insert().values(
                    id=uuid4(), run_id=created.run.id, task_key="overflow"
                )
            )
    with transaction(engine, query_schema) as connection:
        repository = RunRepository(connection)
        assert repository.get_run(created.run.id) is not None
        if extra:
            with pytest.raises(StoredRuntimeError, match="task limit"):
                repository.get_run_with_tasks(created.run.id)
        else:
            snapshot = repository.get_run_with_tasks(created.run.id)
            assert snapshot is not None and len(snapshot.tasks) == MAX_TASKS


@pytest.mark.parametrize("kind", ["run_status", "task_status", "task_key"])
def test_invalid_stored_runtime_is_rejected_with_fixed_error(
    engine: Engine, query_schema: str, run_id: UUID, version_id: UUID, kind: str
) -> None:
    with transaction(engine, query_schema) as connection:
        # Bypass CHECK only in the fixture-owned schema to model corrupted or
        # unsupported stored data. Published migrations themselves stay unchanged.
        if kind == "run_status":
            connection.execute(
                text(
                    "ALTER TABLE workflow_runs DROP CONSTRAINT "
                    "ck_workflow_runs_status_values"
                )
            )
            run_id = uuid4()
            connection.execute(
                workflow_runs.insert().values(
                    id=run_id, workflow_version_id=version_id, status="PRIVATE_BAD"
                )
            )
        else:
            constraint = (
                "ck_task_runs_status_values"
                if kind == "task_status"
                else "ck_task_runs_task_key_format"
            )
            connection.execute(
                text(f"ALTER TABLE task_runs DROP CONSTRAINT {constraint}")
            )
            connection.execute(
                task_runs.insert().values(
                    id=uuid4(),
                    run_id=run_id,
                    task_key="private bad" if kind == "task_key" else "bad",
                    status="PRIVATE_BAD" if kind == "task_status" else "PENDING",
                )
            )
    with transaction(engine, query_schema) as connection:
        repository = RunRepository(connection)
        with pytest.raises(StoredRuntimeError) as error:
            repository.get_run_with_tasks(run_id)
        assert str(error.value) == "Stored runtime data is invalid."
        if kind == "run_status":
            with pytest.raises(
                StoredRuntimeError, match="Stored runtime data is invalid"
            ):
                repository.get_run(run_id)


@pytest.mark.parametrize("detail", [False, True])
def test_invalid_query_id_is_rejected(engine: Engine, detail: bool) -> None:
    with engine.begin() as connection:
        repository = RunRepository(connection)
        with pytest.raises(TypeError, match="run_id must be a UUID"):
            if detail:
                repository.get_run_with_tasks(str(uuid4()))  # type: ignore[arg-type]
            else:
                repository.get_run(str(uuid4()))  # type: ignore[arg-type]


@pytest.mark.parametrize("finish", ["commit", "rollback"])
@pytest.mark.parametrize("detail", [False, True])
def test_query_rejects_ended_or_replaced_transaction(
    engine: Engine, finish: str, detail: bool
) -> None:
    with engine.connect() as connection:
        tx = connection.begin()
        repository = RunRepository(connection)
        if finish == "commit":
            tx.commit()
        else:
            tx.rollback()
        query = repository.get_run_with_tasks if detail else repository.get_run
        with pytest.raises(RepositoryTransactionError):
            query(uuid4())
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                query(uuid4())
