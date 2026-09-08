"""Exercise repository atomicity and coordination with independent PG connections."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier
from uuid import uuid4

import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import Connection, Engine, create_engine, func, select, text
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    StoredDefinitionError,
    StoredWorkflowVersion,
    WorkflowRepository,
)
from workflow_engine.schema import workflow_versions, workflows

pytestmark = pytest.mark.integration


@pytest.fixture
def repository_schema(engine: Engine, migration_schema: str) -> str:
    with engine.begin() as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


@contextmanager
def transaction(engine: Engine, schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        migration_config(connection, schema)
        yield connection


def definition(name: str = "demo", task_type: str = "demo.echo") -> WorkflowDefinition:
    return WorkflowDefinition(
        name=name, tasks=(TaskDefinition(task_id="A", task_type=task_type),)
    )


def test_publish_and_read_committed_versions(
    engine: Engine, repository_schema: str
) -> None:
    with transaction(engine, repository_schema) as connection:
        repository = WorkflowRepository(connection)
        first = repository.publish(definition())
        second = repository.publish(definition(task_type="demo.changed"))
        # No caller commit yet; other transactions must not see partial publication.
        with transaction(engine, repository_schema) as observer:
            assert WorkflowRepository(observer).get_workflow("demo") is None

    with transaction(engine, repository_schema) as connection:
        repository = WorkflowRepository(connection)
        workflow = repository.get_workflow("demo")
        assert workflow is not None
        assert workflow.id == first.workflow_id == second.workflow_id
        assert workflow.created_at.tzinfo is not None
        assert first.created_at.tzinfo is not None
        assert (first.version_number, second.version_number) == (1, 2)
        assert first.id != second.id
        assert repository.get_version(first.id) == first
        assert repository.get_numbered_version(workflow.id, 2) == second
        assert repository.get_latest_version(workflow.id) == second
        assert first.definition == definition()
        assert second.definition == definition(task_type="demo.changed")


def test_identical_publications_are_not_deduplicated(
    engine: Engine, repository_schema: str
) -> None:
    with transaction(engine, repository_schema) as connection:
        repository = WorkflowRepository(connection)
        first = repository.publish(definition())
        second = repository.publish(definition())
        assert first.id != second.id
        assert (first.version_number, second.version_number) == (1, 2)
        assert first.definition == second.definition


def test_missing_records_and_case_sensitive_identity(
    engine: Engine, repository_schema: str
) -> None:
    with transaction(engine, repository_schema) as connection:
        repository = WorkflowRepository(connection)
        missing = uuid4()
        assert repository.get_workflow("missing") is None
        assert repository.get_version(missing) is None
        assert repository.get_numbered_version(missing, 1) is None
        assert repository.get_latest_version(missing) is None
        first = repository.publish(definition("demo"))
        second = repository.publish(definition("Demo"))
        assert first.workflow_id != second.workflow_id
        assert second.version_number == 1
        assert repository.get_numbered_version(first.workflow_id, 2) is None
        assert repository.get_latest_version(first.workflow_id) == first


def test_caller_rollback_removes_first_publication(
    engine: Engine, repository_schema: str
) -> None:
    with pytest.raises(RuntimeError, match="caller failed"):
        with transaction(engine, repository_schema) as connection:
            provisional = WorkflowRepository(connection).publish(definition())
            raise RuntimeError("caller failed")
    with transaction(engine, repository_schema) as connection:
        repository = WorkflowRepository(connection)
        assert repository.get_workflow("demo") is None
        assert repository.get_version(provisional.id) is None
        committed = repository.publish(definition())
        assert committed.version_number == 1


def test_database_failure_rolls_back_publication_and_releases_lock(
    engine: Engine, repository_schema: str
) -> None:
    with transaction(engine, repository_schema) as connection:
        first = WorkflowRepository(connection).publish(definition())
    with pytest.raises(DBAPIError):
        with transaction(engine, repository_schema) as connection:
            provisional = WorkflowRepository(connection).publish(definition())
            connection.execute(text("SELECT 1 / 0"))
    with transaction(engine, repository_schema) as connection:
        repository = WorkflowRepository(connection)
        assert repository.get_latest_version(first.workflow_id) == first
        assert repository.get_version(provisional.id) is None
        assert repository.publish(definition()).version_number == 2


@pytest.mark.parametrize("invalid_part", ["workflow", "task"])
def test_revalidate_bypassed_models_before_writing(
    engine: Engine, repository_schema: str, invalid_part: str
) -> None:
    valid = definition()
    invalid = valid.model_copy(update={"tasks": ()})
    if invalid_part == "task":
        task = valid.tasks[0].model_copy(update={"depends_on": ("missing",)})
        invalid = valid.model_copy(update={"tasks": (task,)})
    with transaction(engine, repository_schema) as connection:
        with pytest.raises(ValidationError):
            WorkflowRepository(connection).publish(invalid)
        assert (
            connection.execute(select(func.count()).select_from(workflows)).scalar()
            == 0
        )
        assert (
            connection.execute(
                select(func.count()).select_from(workflow_versions)
            ).scalar()
            == 0
        )


@pytest.mark.parametrize("read_method", ["id", "number", "latest"])
@pytest.mark.parametrize("corruption", ["fields", "cycle", "future_schema"])
def test_invalid_stored_definition_rejected_on_every_read(
    engine: Engine, repository_schema: str, read_method: str, corruption: str
) -> None:
    payload = definition().model_dump(mode="json")
    if corruption == "fields":
        payload = {}
    elif corruption == "cycle":
        payload["tasks"] = [
            {"task_id": "A", "task_type": "private.handler", "depends_on": ["A"]}
        ]
    else:
        payload["schema_version"] = 3
    workflow_id, version_id = uuid4(), uuid4()
    with transaction(engine, repository_schema) as connection:
        connection.execute(workflows.insert().values(id=workflow_id, name="demo"))
        connection.execute(
            workflow_versions.insert().values(
                id=version_id,
                workflow_id=workflow_id,
                version_number=1,
                definition=payload,
            )
        )
    with transaction(engine, repository_schema) as connection:
        repository = WorkflowRepository(connection)
        with pytest.raises(StoredDefinitionError) as error:
            if read_method == "id":
                repository.get_version(version_id)
            elif read_method == "number":
                repository.get_numbered_version(workflow_id, 1)
            else:
                repository.get_latest_version(workflow_id)
        assert str(error.value) == "Stored workflow definition is invalid."


def test_requires_original_active_transaction(engine: Engine) -> None:
    with engine.connect() as connection:
        with pytest.raises(RepositoryTransactionError):
            WorkflowRepository(connection)
        with connection.begin():
            repository = WorkflowRepository(connection)
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                repository.get_workflow("demo")


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE", "AUTOCOMMIT"])
def test_rejects_unsupported_isolation(engine: Engine, isolation: str) -> None:
    with engine.connect().execution_options(isolation_level=isolation) as connection:
        with connection.begin():
            with pytest.raises(RepositoryTransactionError, match="READ COMMITTED"):
                WorkflowRepository(connection)


def test_rejects_engine_level_autocommit(engine: Engine) -> None:
    unsafe_engine = create_engine(engine.url, isolation_level="AUTOCOMMIT")
    try:
        with unsafe_engine.begin() as connection:
            # SQLAlchemy exposes a transaction object even though the driver
            # auto-commits. Its isolation query alone also looks acceptable.
            assert connection.in_transaction()
            assert connection.get_isolation_level() == "READ COMMITTED"
            with pytest.raises(RepositoryTransactionError, match="READ COMMITTED"):
                WorkflowRepository(connection)
    finally:
        unsafe_engine.dispose()


@pytest.mark.parametrize("existing", [False, True])
def test_concurrent_publications_allocate_distinct_ordered_versions(
    engine: Engine, repository_schema: str, existing: bool
) -> None:
    if existing:
        with transaction(engine, repository_schema) as connection:
            WorkflowRepository(connection).publish(definition())

    barrier = Barrier(4, timeout=10)

    def publish(index: int) -> StoredWorkflowVersion:
        with transaction(engine, repository_schema) as connection:
            repository = WorkflowRepository(connection)
            barrier.wait()
            version = repository.publish(definition(task_type=f"demo.worker{index}"))
        return version

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(publish, index) for index in range(4)]
        versions = [future.result(timeout=20) for future in futures]
    start = 2 if existing else 1
    assert sorted(version.version_number for version in versions) == list(
        range(start, start + 4)
    )
    assert len({version.id for version in versions}) == 4
    assert len({version.workflow_id for version in versions}) == 1
    assert {version.definition.tasks[0].task_type for version in versions} == {
        f"demo.worker{index}" for index in range(4)
    }
    with transaction(engine, repository_schema) as connection:
        repository = WorkflowRepository(connection)
        assert (
            connection.execute(select(func.count()).select_from(workflows)).scalar()
            == 1
        )
        for version in versions:
            assert repository.get_version(version.id) == version


def test_same_workflow_lock_timeout_and_other_workflow_progress(
    engine: Engine, repository_schema: str
) -> None:
    with transaction(engine, repository_schema) as connection:
        held = WorkflowRepository(connection).publish(definition("held"))
        WorkflowRepository(connection).publish(definition("other"))
    with transaction(engine, repository_schema) as owner:
        WorkflowRepository(owner).publish(definition("held"))
        # The owner's transaction stays open throughout both independent calls.
        with transaction(engine, repository_schema) as independent:
            independent.execute(text("SET LOCAL lock_timeout = '250ms'"))
            assert (
                WorkflowRepository(independent)
                .publish(definition("other"))
                .version_number
                == 2
            )
        with pytest.raises(DBAPIError) as error:
            with transaction(engine, repository_schema) as contender:
                contender.execute(text("SET LOCAL lock_timeout = '250ms'"))
                WorkflowRepository(contender).publish(definition("held"))
        assert getattr(error.value.orig, "sqlstate", None) == "55P03"
    with transaction(engine, repository_schema) as connection:
        repository = WorkflowRepository(connection)
        assert repository.get_numbered_version(held.workflow_id, 3) is None
        assert repository.publish(definition("held")).version_number == 3
