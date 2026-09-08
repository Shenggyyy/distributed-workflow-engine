"""Exercise keyed creation/replay and transaction races on real PostgreSQL."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from queue import Queue
from threading import Barrier
from time import monotonic, sleep
from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, func, select, text
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.idempotency import InvalidIdempotencyKeyError
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import (
    IdempotencyConflictError,
    RunCreationReceipt,
    RunRepository,
    WorkflowVersionNotFoundError,
)
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    StoredDefinitionError,
    WorkflowRepository,
)
from workflow_engine.schema import (
    run_creation_requests,
    task_attempts,
    task_runs,
    workflow_runs,
    workflow_versions,
    workflows,
)

pytestmark = pytest.mark.integration


@contextmanager
def transaction(engine: Engine, schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        migration_config(connection, schema)
        yield connection


@pytest.fixture
def keyed_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


@pytest.fixture
def versions(engine: Engine, keyed_schema: str) -> tuple[UUID, UUID]:
    with transaction(engine, keyed_schema) as connection:
        repository = WorkflowRepository(connection)
        definition = WorkflowDefinition(
            name="demo",
            tasks=(
                TaskDefinition(task_id="A", task_type="demo.echo"),
                TaskDefinition(task_id="B", task_type="demo.echo", depends_on=("A",)),
            ),
        )
        return repository.publish(definition).id, repository.publish(definition).id


def assert_counts(connection: Connection, count: int) -> None:
    for table, expected in (
        (run_creation_requests, count),
        (workflow_runs, count),
        (task_runs, count * 2),
        (task_attempts, 0),
    ):
        assert (
            connection.execute(select(func.count()).select_from(table)).scalar_one()
            == expected
        )


def assert_initialization(connection: Connection, receipt: RunCreationReceipt) -> None:
    run = connection.execute(
        select(workflow_runs).where(workflow_runs.c.id == receipt.run_id)
    ).one()
    assert run.workflow_version_id == receipt.workflow_version_id
    assert run.status == "RUNNING"
    tasks = connection.execute(
        select(task_runs).where(task_runs.c.run_id == receipt.run_id)
    ).all()
    assert {task.task_key: task.status for task in tasks} == {
        "A": "READY",
        "B": "PENDING",
    }


def test_replays_in_same_transaction_and_after_commit(
    engine: Engine, keyed_schema: str, versions: tuple[UUID, UUID]
) -> None:
    with transaction(engine, keyed_schema) as connection:
        repository = RunRepository(connection)
        first = repository.create_idempotent(versions[0], idempotency_key="request")
        assert (
            repository.create_idempotent(versions[0], idempotency_key="request")
            == first
        )
        with transaction(engine, keyed_schema) as observer:
            assert_counts(observer, 0)
    # Also represents a caller losing a response after a successful commit.
    with transaction(engine, keyed_schema) as connection:
        assert (
            RunRepository(connection).create_idempotent(
                versions[0], idempotency_key="request"
            )
            == first
        )
        assert_counts(connection, 1)
        assert_initialization(connection, first)


def test_replay_preserves_advanced_runtime_state(
    engine: Engine, keyed_schema: str, versions: tuple[UUID, UUID]
) -> None:
    with transaction(engine, keyed_schema) as connection:
        receipt = RunRepository(connection).create_idempotent(
            versions[0], idempotency_key="done"
        )
    with transaction(engine, keyed_schema) as connection:
        task_ids = set(connection.execute(select(task_runs.c.id)).scalars())
        # Simulate later lifecycle progress with legal edges; no handler is run.
        connection.execute(
            task_runs.update().where(task_runs.c.task_key == "B").values(status="READY")
        )
        connection.execute(task_runs.update().values(status="RUNNING"))
        connection.execute(task_runs.update().values(status="SUCCEEDED"))
        connection.execute(workflow_runs.update().values(status="SUCCEEDED"))
    with transaction(engine, keyed_schema) as connection:
        assert (
            RunRepository(connection).create_idempotent(
                versions[0], idempotency_key="done"
            )
            == receipt
        )
        assert (
            connection.execute(select(workflow_runs.c.status)).scalar_one()
            == "SUCCEEDED"
        )
        assert set(connection.execute(select(task_runs.c.status)).scalars()) == {
            "SUCCEEDED"
        }
        assert set(connection.execute(select(task_runs.c.id)).scalars()) == task_ids
        assert_counts(connection, 1)


@pytest.mark.parametrize("missing", [False, True])
def test_same_key_different_version_conflicts_without_new_run(
    engine: Engine, keyed_schema: str, versions: tuple[UUID, UUID], missing: bool
) -> None:
    with transaction(engine, keyed_schema) as connection:
        first = RunRepository(connection).create_idempotent(
            versions[0], idempotency_key="private-key"
        )
    with transaction(engine, keyed_schema) as connection:
        with pytest.raises(IdempotencyConflictError) as error:
            RunRepository(connection).create_idempotent(
                uuid4() if missing else versions[1], idempotency_key="private-key"
            )
        assert "private-key" not in str(error.value)
        assert_counts(connection, 1)
        assert_initialization(connection, first)


def test_distinct_case_sensitive_keys_do_not_wait_for_each_other(
    engine: Engine, keyed_schema: str, versions: tuple[UUID, UUID]
) -> None:
    with transaction(engine, keyed_schema) as owner:
        first = RunRepository(owner).create_idempotent(
            versions[0], idempotency_key="Key"
        )
        with transaction(engine, keyed_schema) as independent:
            independent.execute(text("SET LOCAL lock_timeout = '250ms'"))
            second = RunRepository(independent).create_idempotent(
                versions[0], idempotency_key="key"
            )
            assert first.run_id != second.run_id
    with transaction(engine, keyed_schema) as connection:
        assert_counts(connection, 2)


@pytest.mark.parametrize("invalid", ["key", "version_type"])
def test_invalid_input_fails_before_reservation(
    engine: Engine, keyed_schema: str, versions: tuple[UUID, UUID], invalid: str
) -> None:
    with transaction(engine, keyed_schema) as connection:
        repository = RunRepository(connection)
        if invalid == "key":
            with pytest.raises(InvalidIdempotencyKeyError):
                repository.create_idempotent(versions[0], idempotency_key="private/key")
        else:
            with pytest.raises(TypeError, match="must be a UUID"):
                repository.create_idempotent(str(versions[0]), idempotency_key="key")  # type: ignore[arg-type]
        assert_counts(connection, 0)


@pytest.mark.parametrize("corrupt", [False, True])
def test_missing_or_corrupt_version_rolls_back_reservation(
    engine: Engine, keyed_schema: str, versions: tuple[UUID, UUID], corrupt: bool
) -> None:
    version_id = uuid4()
    if corrupt:
        with transaction(engine, keyed_schema) as connection:
            workflow_id = uuid4()
            connection.execute(
                workflows.insert().values(id=workflow_id, name="invalid")
            )
            connection.execute(
                workflow_versions.insert().values(
                    id=version_id,
                    workflow_id=workflow_id,
                    version_number=1,
                    definition={},
                )
            )
    with pytest.raises(
        StoredDefinitionError if corrupt else WorkflowVersionNotFoundError
    ):
        with transaction(engine, keyed_schema) as connection:
            RunRepository(connection).create_idempotent(
                version_id, idempotency_key="retry"
            )
    with transaction(engine, keyed_schema) as connection:
        assert_counts(connection, 0)
        RunRepository(connection).create_idempotent(
            versions[0], idempotency_key="retry"
        )


def test_caller_failure_rolls_back_binding_run_and_tasks(
    engine: Engine, keyed_schema: str, versions: tuple[UUID, UUID]
) -> None:
    with pytest.raises(RuntimeError, match="caller failed"):
        with transaction(engine, keyed_schema) as connection:
            RunRepository(connection).create_idempotent(
                versions[0], idempotency_key="retry"
            )
            raise RuntimeError("caller failed")
    with transaction(engine, keyed_schema) as connection:
        assert_counts(connection, 0)
        RunRepository(connection).create_idempotent(
            versions[0], idempotency_key="retry"
        )


@pytest.mark.parametrize("failure", ["initialization", "commit"])
def test_database_failure_rolls_back_and_allows_retry(
    engine: Engine, keyed_schema: str, versions: tuple[UUID, UUID], failure: str
) -> None:
    table = "task_runs" if failure == "initialization" else "run_creation_requests"
    with transaction(engine, keyed_schema) as connection:
        connection.execute(
            text("""
            CREATE FUNCTION reject_keyed_creation() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='injected failure';
            END;
            $$
        """)
        )
        if failure == "initialization":
            connection.execute(
                text("""
                CREATE TRIGGER reject_keyed_creation BEFORE UPDATE ON task_runs
                FOR EACH STATEMENT EXECUTE FUNCTION reject_keyed_creation()
            """)
            )
        else:
            connection.execute(
                text("""
                CREATE CONSTRAINT TRIGGER reject_keyed_creation
                AFTER INSERT ON run_creation_requests DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW EXECUTE FUNCTION reject_keyed_creation()
            """)
            )
    returned: RunCreationReceipt | None = None
    with pytest.raises(DBAPIError) as error:
        with transaction(engine, keyed_schema) as connection:
            returned = RunRepository(connection).create_idempotent(
                versions[0], idempotency_key="retry"
            )
    assert getattr(error.value.orig, "sqlstate", None) == "55000"
    assert (returned is not None) == (failure == "commit")
    with transaction(engine, keyed_schema) as connection:
        assert_counts(connection, 0)
        connection.execute(text(f"DROP TRIGGER reject_keyed_creation ON {table}"))
        successful = RunRepository(connection).create_idempotent(
            versions[0], idempotency_key="retry"
        )
    with transaction(engine, keyed_schema) as connection:
        assert_counts(connection, 1)
        assert_initialization(connection, successful)


@pytest.mark.parametrize("different_versions", [False, True])
def test_four_concurrent_requests_commit_one_run(
    engine: Engine,
    keyed_schema: str,
    versions: tuple[UUID, UUID],
    different_versions: bool,
) -> None:
    barrier = Barrier(4, timeout=10)

    def create(index: int) -> RunCreationReceipt | None:
        try:
            with transaction(engine, keyed_schema) as connection:
                repository = RunRepository(connection)
                barrier.wait()
                return repository.create_idempotent(
                    versions[index % 2] if different_versions else versions[0],
                    idempotency_key="shared",
                )
        except IdempotencyConflictError:
            return None

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(create, index) for index in range(4)]
        results = [future.result(timeout=20) for future in futures]
    receipts = [result for result in results if result is not None]
    assert len(receipts) == (2 if different_versions else 4)
    assert len(set(receipts)) == 1
    with transaction(engine, keyed_schema) as connection:
        assert_counts(connection, 1)
        assert_initialization(connection, receipts[0])


def wait_for_block(engine: Engine, waiter: int, owner: int) -> None:
    deadline = monotonic() + 5
    with engine.connect() as observer:
        while monotonic() < deadline:
            if observer.execute(
                text("SELECT :owner = ANY(pg_blocking_pids(:waiter))"),
                {"owner": owner, "waiter": waiter},
            ).scalar_one():
                return
            sleep(0.01)
    raise AssertionError("Contender never blocked on the owning transaction")


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_blocked_contender_replays_or_takes_over_after_owner_finishes(
    engine: Engine, keyed_schema: str, versions: tuple[UUID, UUID], finish: str
) -> None:
    waiter_pid: Queue[int] = Queue()

    def contend() -> RunCreationReceipt:
        with transaction(engine, keyed_schema) as connection:
            waiter_pid.put(
                connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
            )
            return RunRepository(connection).create_idempotent(
                versions[0], idempotency_key="held"
            )

    with engine.connect() as owner:
        tx = owner.begin()
        migration_config(owner, keyed_schema)
        owner_pid = owner.execute(text("SELECT pg_backend_pid()")).scalar_one()
        provisional = RunRepository(owner).create_idempotent(
            versions[0], idempotency_key="held"
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(contend)
            try:
                wait_for_block(engine, waiter_pid.get(timeout=5), owner_pid)
                if finish == "commit":
                    tx.commit()
                else:
                    tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
            result = future.result(timeout=10)
    assert (result == provisional) == (finish == "commit")
    with transaction(engine, keyed_schema) as connection:
        assert_counts(connection, 1)
        assert_initialization(connection, result)


def test_contention_timeout_propagates_and_later_replay_succeeds(
    engine: Engine, keyed_schema: str, versions: tuple[UUID, UUID]
) -> None:
    with transaction(engine, keyed_schema) as owner:
        receipt = RunRepository(owner).create_idempotent(
            versions[0], idempotency_key="held"
        )
        with pytest.raises(DBAPIError) as error:
            with transaction(engine, keyed_schema) as contender:
                contender.execute(text("SET LOCAL lock_timeout = '100ms'"))
                RunRepository(contender).create_idempotent(
                    versions[0], idempotency_key="held"
                )
        assert getattr(error.value.orig, "sqlstate", None) == "55P03"
    with transaction(engine, keyed_schema) as connection:
        assert (
            RunRepository(connection).create_idempotent(
                versions[0], idempotency_key="held"
            )
            == receipt
        )
        assert_counts(connection, 1)


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_keyed_path_rejects_ended_and_replaced_transaction(
    engine: Engine, finish: str
) -> None:
    with engine.connect() as connection:
        tx = connection.begin()
        repository = RunRepository(connection)
        if finish == "commit":
            tx.commit()
        else:
            tx.rollback()
        with pytest.raises(RepositoryTransactionError):
            repository.create_idempotent(uuid4(), idempotency_key="key")
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                repository.create_idempotent(uuid4(), idempotency_key="key")
