"""Worker storage invariants, serialization and populated migration round trips."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import Barrier, Event
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, event, func, inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.runtime import InvalidStateTransition
from workflow_engine.domain.worker import WorkerEvent, WorkerSession, WorkerStatus
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    run_creation_requests,
    task_runs,
    worker_sessions,
    workflow_runs,
)

pytestmark = pytest.mark.integration
STAMP = datetime(2026, 9, 8, tzinfo=UTC)


@contextmanager
def transaction(engine: Engine, schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        migration_config(connection, schema)
        yield connection


@pytest.fixture
def worker_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


def values(**updates: object) -> dict[str, object]:
    return {
        "id": uuid4(),
        "worker_name": "worker_local",
        "max_concurrency": 2,
        "created_at": STAMP,
        "last_heartbeat_at": STAMP,
        "heartbeat_expires_at": STAMP + timedelta(seconds=30),
        **updates,
    }


def test_defaults_rehydration_same_names_and_explicit_times(
    engine: Engine, worker_schema: str
) -> None:
    with transaction(engine, worker_schema) as connection:
        for capacity in (1, 2_147_483_647):
            connection.execute(
                worker_sessions.insert().values(**values(max_concurrency=capacity))
            )
    with transaction(engine, worker_schema) as connection:
        rows = connection.execute(select(worker_sessions)).mappings().all()
        assert len(rows) == 2 and rows[0]["id"] != rows[1]["id"]
        for row in rows:
            model = WorkerSession(
                id=row["id"],
                worker_name=row["worker_name"],
                max_concurrency=row["max_concurrency"],
                status=WorkerStatus(row["status"]),
            )
            assert model.status is WorkerStatus.ACTIVE
            assert row["created_at"] == row["last_heartbeat_at"] == STAMP
            assert row["heartbeat_expires_at"] == STAMP + timedelta(seconds=30)
            assert row["created_at"].tzinfo is not None
        # Reflection confirms no independently evaluated timestamp defaults.
        columns = inspect(connection).get_columns(
            "worker_sessions", schema=worker_schema
        )
        assert all(
            c["default"] is None
            for c in columns
            if c["name"] in ("created_at", "last_heartbeat_at", "heartbeat_expires_at")
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"worker_name": ""},
        {"worker_name": "bad name"},
        {"worker_name": "9worker"},
        {"worker_name": "w" * 65},
        {"worker_name": "节点"},
        {"worker_name": "a\n"},
        {"max_concurrency": 0},
        {"max_concurrency": -1},
        {"max_concurrency": 2_147_483_648},
        {"status": "UNKNOWN"},
        {"last_heartbeat_at": STAMP - timedelta(seconds=1)},
        {"heartbeat_expires_at": STAMP},
        {"heartbeat_expires_at": STAMP - timedelta(seconds=1)},
    ],
)
def test_invalid_values_and_time_order_are_rejected(
    engine: Engine, worker_schema: str, updates: dict[str, object]
) -> None:
    with pytest.raises(DBAPIError):
        with transaction(engine, worker_schema) as connection:
            connection.execute(worker_sessions.insert().values(**values(**updates)))


@pytest.mark.parametrize(
    "field",
    [
        "id",
        "worker_name",
        "max_concurrency",
        "status",
        "created_at",
        "last_heartbeat_at",
        "heartbeat_expires_at",
    ],
)
def test_required_columns_reject_null(
    engine: Engine, worker_schema: str, field: str
) -> None:
    with pytest.raises(IntegrityError) as error:
        with transaction(engine, worker_schema) as connection:
            connection.execute(
                worker_sessions.insert().values(**values(**{field: None}))
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23502"


@pytest.mark.parametrize(
    "field", ["created_at", "last_heartbeat_at", "heartbeat_expires_at"]
)
@pytest.mark.parametrize("literal", ["infinity", "-infinity"])
def test_nonfinite_time_is_rejected(
    engine: Engine, worker_schema: str, field: str, literal: str
) -> None:
    with pytest.raises(IntegrityError) as error:
        with transaction(engine, worker_schema) as connection:
            connection.execute(
                worker_sessions.insert().values(
                    **values(**{field: text(f"'{literal}'::timestamptz")})
                )
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23514"


@pytest.mark.parametrize("before", list(WorkerStatus))
@pytest.mark.parametrize("after", list(WorkerStatus))
def test_every_status_edge_matches_domain_or_sql_noop(
    engine: Engine, worker_schema: str, before: WorkerStatus, after: WorkerStatus
) -> None:
    # INSERT may rehydrate any valid status, like the other runtime tables.
    original = WorkerSession(
        id=uuid4(), worker_name="worker", max_concurrency=1, status=before
    )
    destinations = set()
    for action in WorkerEvent:
        try:
            destinations.add(original.transition(action).status)
        except InvalidStateTransition:
            pass
    with transaction(engine, worker_schema) as connection:
        connection.execute(
            worker_sessions.insert().values(
                **values(id=original.id, status=before.value)
            )
        )
        statement = worker_sessions.update().values(status=after.value)
        if after is before or after in destinations:
            connection.execute(statement)
            assert (
                connection.execute(select(worker_sessions.c.status)).scalar_one()
                == after
            )
        else:
            with pytest.raises(DBAPIError) as error:
                with connection.begin_nested():
                    connection.execute(statement)
            assert getattr(error.value.orig, "sqlstate", None) == "55000"
            assert (
                connection.execute(select(worker_sessions.c.status)).scalar_one()
                == before
            )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("id", uuid4()),
        ("worker_name", "different"),
        ("max_concurrency", 3),
        ("created_at", STAMP - timedelta(seconds=1)),
    ],
)
def test_registration_fields_are_immutable(
    engine: Engine, worker_schema: str, field: str, replacement: object
) -> None:
    with transaction(engine, worker_schema) as connection:
        original = values()
        connection.execute(worker_sessions.insert().values(**original))
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(
                    worker_sessions.update().values(**{field: replacement})
                )
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        row = connection.execute(select(worker_sessions)).mappings().one()
        assert row[field] == original[field]


def test_active_heartbeat_can_advance_but_neither_time_can_regress(
    engine: Engine, worker_schema: str
) -> None:
    with transaction(engine, worker_schema) as connection:
        connection.execute(worker_sessions.insert().values(**values()))
        advanced = {
            "last_heartbeat_at": STAMP + timedelta(seconds=10),
            "heartbeat_expires_at": STAMP + timedelta(seconds=40),
        }
        connection.execute(worker_sessions.update().values(**advanced))
        connection.execute(worker_sessions.update().values(**advanced))  # SQL no-op.
        for field in advanced:
            with pytest.raises(DBAPIError) as error:
                with connection.begin_nested():
                    connection.execute(
                        worker_sessions.update().values(
                            **{field: advanced[field] - timedelta(seconds=1)}
                        )
                    )
            assert getattr(error.value.orig, "sqlstate", None) == "55000"
        row = connection.execute(select(worker_sessions)).mappings().one()
        assert all(row[field] == value for field, value in advanced.items())


@pytest.mark.parametrize("terminal", [WorkerStatus.LOST, WorkerStatus.STOPPED])
@pytest.mark.parametrize("same_update", [False, True])
def test_heartbeat_cannot_change_with_or_after_terminal_transition(
    engine: Engine, worker_schema: str, terminal: WorkerStatus, same_update: bool
) -> None:
    with transaction(engine, worker_schema) as connection:
        connection.execute(worker_sessions.insert().values(**values()))
        if not same_update:
            connection.execute(worker_sessions.update().values(status=terminal.value))
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(
                    worker_sessions.update().values(
                        status=terminal.value,
                        last_heartbeat_at=STAMP + timedelta(seconds=1),
                        heartbeat_expires_at=STAMP + timedelta(seconds=31),
                    )
                )
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        expected = WorkerStatus.ACTIVE if same_update else terminal
        assert (
            connection.execute(select(worker_sessions.c.status)).scalar_one()
            == expected
        )


@pytest.mark.parametrize(
    "statement",
    [
        "DELETE FROM worker_sessions",
        "DELETE FROM worker_sessions WHERE false",
        "TRUNCATE worker_sessions",
    ],
)
def test_history_deletion_is_rejected(
    engine: Engine, worker_schema: str, statement: str
) -> None:
    with transaction(engine, worker_schema) as connection:
        connection.execute(worker_sessions.insert().values(**values()))
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(text(statement))
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        assert (
            connection.execute(
                select(func.count()).select_from(worker_sessions)
            ).scalar_one()
            == 1
        )


def test_rollback_and_concurrent_duplicate_session_ids(
    engine: Engine, worker_schema: str
) -> None:
    session_id = uuid4()
    with pytest.raises(RuntimeError, match="abort"):
        with transaction(engine, worker_schema) as connection:
            connection.execute(worker_sessions.insert().values(**values(id=session_id)))
            raise RuntimeError("abort")
    barrier = Barrier(4, timeout=10)

    def insert() -> str:
        try:
            with transaction(engine, worker_schema) as connection:
                barrier.wait()
                connection.execute(
                    worker_sessions.insert().values(**values(id=session_id))
                )
            return "committed"
        except IntegrityError as error:
            assert getattr(error.orig, "sqlstate", None) == "23505"
            return "duplicate"

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(insert) for _ in range(4)]
        outcomes = [future.result(timeout=20) for future in futures]
    assert outcomes.count("committed") == 1 and outcomes.count("duplicate") == 3
    with transaction(engine, worker_schema) as connection:
        assert (
            connection.execute(select(worker_sessions.c.id)).scalar_one() == session_id
        )


def test_waiting_heartbeat_cannot_overwrite_committed_terminal_state(
    engine: Engine, worker_schema: str
) -> None:
    with transaction(engine, worker_schema) as connection:
        connection.execute(worker_sessions.insert().values(**values()))
    entered = Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if statement.startswith("UPDATE worker_sessions"):
            entered.set()

    def heartbeat() -> str | None:
        try:
            with transaction(engine, worker_schema) as connection:
                connection.execute(
                    worker_sessions.update().values(
                        last_heartbeat_at=STAMP + timedelta(seconds=1),
                        heartbeat_expires_at=STAMP + timedelta(seconds=31),
                    )
                )
        except DBAPIError as error:
            return getattr(error.orig, "sqlstate", None)
        return None

    with ThreadPoolExecutor(max_workers=1) as executor:
        with transaction(engine, worker_schema) as owner:
            owner.execute(worker_sessions.update().values(status="LOST"))
            event.listen(engine, "before_cursor_execute", before_execute)
            try:
                pending = executor.submit(heartbeat)
                assert entered.wait(timeout=5)
                assert not pending.done()
            finally:
                event.remove(engine, "before_cursor_execute", before_execute)
        assert pending.result(timeout=5) == "55000"
    with transaction(engine, worker_schema) as connection:
        row = connection.execute(select(worker_sessions)).one()
        assert row.status == "LOST" and row.last_heartbeat_at == STAMP


def test_partial_deadline_index_and_no_clock_enforcement(
    engine: Engine, worker_schema: str
) -> None:
    with transaction(engine, worker_schema) as connection:
        command.check(migration_config(connection, worker_schema))
        indexes = inspect(connection).get_indexes(
            "worker_sessions", schema=worker_schema
        )
        index = next(
            i for i in indexes if i["name"] == "ix_worker_sessions_active_deadline"
        )
        assert index["column_names"] == ["heartbeat_expires_at", "id"]
        assert not index["unique"]
        predicate = index["dialect_options"]["postgresql_where"]
        assert "status" in predicate and "'ACTIVE'" in predicate
        # A historical deadline is structurally valid; clock policy belongs in
        # the later repository and does not run automatically inside this schema.
        past = datetime(2000, 1, 1, tzinfo=UTC)
        connection.execute(
            worker_sessions.insert().values(
                **values(
                    created_at=past,
                    last_heartbeat_at=past,
                    heartbeat_expires_at=past + timedelta(seconds=30),
                )
            )
        )
        assert (
            connection.execute(select(worker_sessions.c.status)).scalar_one()
            == "ACTIVE"
        )


def test_populated_upgrade_downgrade_preserves_existing_run_and_key(
    engine: Engine, migration_schema: str
) -> None:
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "0004")
        version = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name="retained",
                tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),),
            )
        )
        receipt = RunRepository(connection).create_idempotent(
            version.id, idempotency_key="key"
        )
        task_id = connection.execute(select(task_runs.c.id)).scalar_one()
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "head")
        command.check(config)
        connection.execute(worker_sessions.insert().values(**values()))
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.downgrade(config, "0004")
        assert not inspect(connection).has_table(
            "worker_sessions", schema=migration_schema
        )
        for name in (
            "dwe_guard_worker_session_update",
            "dwe_reject_worker_session_deletion",
        ):
            assert (
                connection.execute(
                    text(f"SELECT to_regprocedure('{name}()')")
                ).scalar_one()
                is None
            )
        assert (
            connection.execute(select(workflow_runs.c.id)).scalar_one()
            == receipt.run_id
        )
        assert (
            connection.execute(select(run_creation_requests.c.run_id)).scalar_one()
            == receipt.run_id
        )
        assert connection.execute(select(task_runs.c.id)).scalar_one() == task_id
        assert WorkflowRepository(connection).get_version(version.id) is not None
        assert (
            RunRepository(connection).create_idempotent(
                version.id, idempotency_key="key"
            )
            == receipt
        )
        command.upgrade(config, "head")
        command.check(config)
        assert (
            connection.execute(
                select(func.count()).select_from(worker_sessions)
            ).scalar_one()
            == 0
        )
