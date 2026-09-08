"""Lease storage guards, concurrent writes and populated migration compatibility."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import Barrier, Event
from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, event, func, inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.lease import AttemptLease
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    attempt_leases,
    run_creation_requests,
    task_attempts,
    task_runs,
    worker_sessions,
    workflow_runs,
    workflow_versions,
    workflows,
)

pytestmark = pytest.mark.integration
STAMP = datetime(2000, 1, 1, tzinfo=UTC)


@contextmanager
def transaction(engine: Engine, schema: str) -> Iterator[Connection]:
    with engine.begin() as connection:
        migration_config(connection, schema)
        yield connection


@pytest.fixture
def lease_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


def parents(connection: Connection) -> tuple[UUID, UUID]:
    version = WorkflowRepository(connection).publish(
        WorkflowDefinition(
            name="lease_" + uuid4().hex,
            tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),),
        )
    )
    receipt = RunRepository(connection).create_idempotent(
        version.id, idempotency_key=uuid4().hex
    )
    task_id = connection.execute(
        select(task_runs.c.id).where(task_runs.c.run_id == receipt.run_id)
    ).scalar_one()
    connection.execute(
        task_runs.update().where(task_runs.c.id == task_id).values(status="RUNNING")
    )
    attempt_id, worker_id = uuid4(), uuid4()
    connection.execute(
        task_attempts.insert().values(id=attempt_id, task_id=task_id, attempt_number=1)
    )
    WorkerRepository(connection).register(
        worker_id, worker_name="worker", max_concurrency=2
    )
    return attempt_id, worker_id


def values(ids: tuple[UUID, UUID], **updates: object) -> dict[str, object]:
    return {
        "attempt_id": ids[0],
        "worker_session_id": ids[1],
        "lease_token": uuid4(),
        "acquired_at": STAMP,
        "last_renewed_at": STAMP,
        "lease_expires_at": STAMP + timedelta(seconds=30),
        **updates,
    }


def test_rehydration_explicit_fields_indexes_and_metadata(
    engine: Engine, lease_schema: str
) -> None:
    with transaction(engine, lease_schema) as connection:
        data = values(parents(connection))
        connection.execute(attempt_leases.insert().values(**data))
    with transaction(engine, lease_schema) as connection:
        row = connection.execute(select(attempt_leases)).mappings().one()
        assert AttemptLease.model_validate(dict(row)).model_dump() == data
        columns = inspect(connection).get_columns("attempt_leases", schema=lease_schema)
        assert all(not c["nullable"] and c["default"] is None for c in columns)
        indexes = {
            item["name"]: item
            for item in inspect(connection).get_indexes(
                "attempt_leases", schema=lease_schema
            )
        }
        assert indexes["ix_attempt_leases_worker_attempt"]["column_names"] == [
            "worker_session_id",
            "attempt_id",
        ]
        assert indexes["ix_attempt_leases_deadline"]["column_names"] == [
            "lease_expires_at",
            "attempt_id",
        ]
        keys = inspect(connection).get_foreign_keys(
            "attempt_leases", schema=lease_schema
        )
        assert {key["referred_table"] for key in keys} == {
            "task_attempts",
            "worker_sessions",
        }
        assert all(key["options"]["ondelete"] == "RESTRICT" for key in keys)
        command.check(migration_config(connection, lease_schema))


@pytest.mark.parametrize("field", list(AttemptLease.model_fields))
def test_required_fields_have_no_defaults(
    engine: Engine, lease_schema: str, field: str
) -> None:
    with transaction(engine, lease_schema) as connection:
        data = values(parents(connection))
        for explicit_null in (False, True):
            invalid = dict(data)
            if explicit_null:
                invalid[field] = None
            else:
                invalid.pop(field)
            with pytest.raises(IntegrityError) as error:
                with connection.begin_nested():
                    # Raw INSERT avoids SQLAlchemy's intentional missing-PK warning.
                    columns = ", ".join(invalid)
                    parameters = ", ".join(":" + key for key in invalid)
                    connection.execute(
                        text(
                            f"INSERT INTO attempt_leases ({columns}) "
                            f"VALUES ({parameters})"
                        ),
                        invalid,
                    )
            assert getattr(error.value.orig, "sqlstate", None) == "23502"


@pytest.mark.parametrize("field", ["attempt_id", "worker_session_id"])
def test_missing_parent_is_rejected(
    engine: Engine, lease_schema: str, field: str
) -> None:
    with transaction(engine, lease_schema) as connection:
        data = values(parents(connection), **{field: uuid4()})
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(attempt_leases.insert().values(**data))
        assert getattr(error.value.orig, "sqlstate", None) == "23503"


@pytest.mark.parametrize(
    "updates",
    [
        {"acquired_at": STAMP + timedelta(seconds=1)},
        {"last_renewed_at": STAMP - timedelta(seconds=1)},
        {"lease_expires_at": STAMP},
        {"lease_expires_at": STAMP - timedelta(seconds=1)},
    ],
)
def test_invalid_time_order(
    engine: Engine, lease_schema: str, updates: dict[str, object]
) -> None:
    with transaction(engine, lease_schema) as connection:
        data = values(parents(connection), **updates)
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(attempt_leases.insert().values(**data))
        assert getattr(error.value.orig, "sqlstate", None) == "23514"


@pytest.mark.parametrize(
    "field", ["acquired_at", "last_renewed_at", "lease_expires_at"]
)
@pytest.mark.parametrize("literal", ["infinity", "-infinity"])
def test_nonfinite_times(
    engine: Engine, lease_schema: str, field: str, literal: str
) -> None:
    with transaction(engine, lease_schema) as connection:
        data = values(parents(connection), **{field: text(f"'{literal}'::timestamptz")})
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(attempt_leases.insert().values(**data))
        assert getattr(error.value.orig, "sqlstate", None) == "23514"


@pytest.mark.parametrize(
    "field", ["attempt_id", "worker_session_id", "lease_token", "acquired_at"]
)
def test_identity_is_immutable(engine: Engine, lease_schema: str, field: str) -> None:
    with transaction(engine, lease_schema) as connection:
        ids = parents(connection)
        alternatives = parents(connection)
        data = values(ids)
        replacement = {
            "attempt_id": alternatives[0],
            "worker_session_id": alternatives[1],
            "lease_token": uuid4(),
            "acquired_at": STAMP - timedelta(seconds=1),
        }[field]
        connection.execute(attempt_leases.insert().values(**data))
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(
                    attempt_leases.update().values(**{field: replacement})
                )
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        assert dict(connection.execute(select(attempt_leases)).mappings().one()) == data


def test_times_advance_and_noop_but_never_regress(
    engine: Engine, lease_schema: str
) -> None:
    with transaction(engine, lease_schema) as connection:
        connection.execute(
            attempt_leases.insert().values(**values(parents(connection)))
        )
        advanced = {
            "last_renewed_at": STAMP + timedelta(seconds=10),
            "lease_expires_at": STAMP + timedelta(seconds=40),
        }
        connection.execute(attempt_leases.update().values(**advanced))
        connection.execute(attempt_leases.update().values(**advanced))
        for field, value in advanced.items():
            with pytest.raises(DBAPIError) as error:
                with connection.begin_nested():
                    connection.execute(
                        attempt_leases.update().values(
                            **{field: value - timedelta(seconds=1)}
                        )
                    )
            assert getattr(error.value.orig, "sqlstate", None) == "55000"
        row = connection.execute(select(attempt_leases)).mappings().one()
        assert all(row[key] == value for key, value in advanced.items())


@pytest.mark.parametrize(
    "statement",
    [
        "DELETE FROM attempt_leases",
        "DELETE FROM attempt_leases WHERE false",
        "TRUNCATE attempt_leases",
    ],
)
@pytest.mark.parametrize("populated", [False, True])
def test_history_is_retained(
    engine: Engine, lease_schema: str, statement: str, populated: bool
) -> None:
    with transaction(engine, lease_schema) as connection:
        if populated:
            connection.execute(
                attempt_leases.insert().values(**values(parents(connection)))
            )
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(text(statement))
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        assert connection.execute(
            select(func.count()).select_from(attempt_leases)
        ).scalar_one() == int(populated)


@pytest.mark.parametrize("same_attempt", [False, True])
def test_concurrent_identity_uniqueness(
    engine: Engine, lease_schema: str, same_attempt: bool
) -> None:
    with transaction(engine, lease_schema) as connection:
        first, second = parents(connection), parents(connection)
    token = uuid4()
    barrier = Barrier(2, timeout=10)

    def insert(index: int) -> str:
        data = values(
            first if same_attempt or index == 0 else second,
            lease_token=uuid4() if same_attempt else token,
        )
        try:
            with transaction(engine, lease_schema) as connection:
                barrier.wait()
                connection.execute(attempt_leases.insert().values(**data))
            return "committed"
        except IntegrityError as error:
            assert getattr(error.orig, "sqlstate", None) == "23505"
            return "duplicate"

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(insert, i) for i in range(2)]
        assert sorted(f.result(timeout=20) for f in futures) == [
            "committed",
            "duplicate",
        ]
    with transaction(engine, lease_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_leases)
            ).scalar_one()
            == 1
        )


@pytest.mark.parametrize("commit_owner", [False, True])
def test_waiting_update_checks_latest_committed_times(
    engine: Engine, lease_schema: str, commit_owner: bool
) -> None:
    with transaction(engine, lease_schema) as connection:
        connection.execute(
            attempt_leases.insert().values(**values(parents(connection)))
        )
    entered = Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if statement.startswith("UPDATE attempt_leases"):
            entered.set()

    def update() -> str:
        try:
            with transaction(engine, lease_schema) as connection:
                connection.execute(
                    attempt_leases.update().values(
                        last_renewed_at=STAMP + timedelta(seconds=5),
                        lease_expires_at=STAMP + timedelta(seconds=35),
                    )
                )
            return "committed"
        except DBAPIError as error:
            assert getattr(error.orig, "sqlstate", None) == "55000"
            return "rejected"

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as owner:
            tx = owner.begin()
            try:
                migration_config(owner, lease_schema)
                owner.execute(
                    attempt_leases.update().values(
                        last_renewed_at=STAMP + timedelta(seconds=10),
                        lease_expires_at=STAMP + timedelta(seconds=40),
                    )
                )
                event.listen(engine, "before_cursor_execute", before_execute)
                try:
                    pending = executor.submit(update)
                    assert entered.wait(timeout=5)
                    assert not pending.done()
                finally:
                    event.remove(engine, "before_cursor_execute", before_execute)
                if commit_owner:
                    tx.commit()
                else:
                    tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
        assert pending.result(timeout=5) == (
            "rejected" if commit_owner else "committed"
        )
    with transaction(engine, lease_schema) as connection:
        row = connection.execute(select(attempt_leases)).one()
        assert row.last_renewed_at == STAMP + timedelta(
            seconds=10 if commit_owner else 5
        )


def test_insert_rollback_does_not_fabricate_ownership(
    engine: Engine, lease_schema: str
) -> None:
    with transaction(engine, lease_schema) as connection:
        ids = parents(connection)
    with pytest.raises(RuntimeError, match="abort"):
        with transaction(engine, lease_schema) as connection:
            connection.execute(attempt_leases.insert().values(**values(ids)))
            raise RuntimeError("abort")
    with transaction(engine, lease_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_leases)
            ).scalar_one()
            == 0
        )
        assert connection.execute(select(task_attempts.c.id)).scalar_one() == ids[0]


@pytest.mark.parametrize("parent_table", ["worker_sessions", "task_attempts"])
def test_parent_truncate_cascade_preserves_lease_history(
    engine: Engine, lease_schema: str, parent_table: str
) -> None:
    with transaction(engine, lease_schema) as connection:
        data = values(parents(connection))
        connection.execute(attempt_leases.insert().values(**data))
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(text(f"TRUNCATE {parent_table} CASCADE"))
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        assert dict(connection.execute(select(attempt_leases)).mappings().one()) == data
        assert (
            connection.execute(select(task_attempts.c.id)).scalar_one()
            == data["attempt_id"]
        )
        assert (
            connection.execute(select(worker_sessions.c.id)).scalar_one()
            == data["worker_session_id"]
        )


def test_schema_does_not_authorize_renewal_from_status_or_clock(
    engine: Engine, lease_schema: str
) -> None:
    with transaction(engine, lease_schema) as connection:
        ids = parents(connection)
        connection.execute(attempt_leases.insert().values(**values(ids)))
        connection.execute(task_attempts.update().values(status="LOST"))
        connection.execute(worker_sessions.update().values(status="LOST"))
        # Structural storage allows historical snapshots. This is NOT an authorized
        # renewal; future repositories must reject terminal/expired ownership.
        connection.execute(
            attempt_leases.update().values(
                last_renewed_at=STAMP + timedelta(seconds=10),
                lease_expires_at=STAMP + timedelta(seconds=40),
            )
        )
        assert connection.execute(select(task_attempts.c.status)).scalar_one() == "LOST"


def test_populated_round_trip_preserves_legacy_attempts_and_all_older_rows(
    engine: Engine, migration_schema: str
) -> None:
    tables = (
        workflows,
        workflow_versions,
        workflow_runs,
        task_runs,
        task_attempts,
        run_creation_requests,
        worker_sessions,
    )
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "0005")
        ids = parents(connection)
        before = {
            table.name: connection.execute(
                select(table).order_by(*table.primary_key.columns)
            ).all()
            for table in tables
        }
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "head")
        command.check(config)
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_leases)
            ).scalar_one()
            == 0
        )
        for table in tables:
            assert (
                connection.execute(
                    select(table).order_by(*table.primary_key.columns)
                ).all()
                == before[table.name]
            )
        connection.execute(attempt_leases.insert().values(**values(ids)))
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.downgrade(config, "0005")
        assert not inspect(connection).has_table(
            "attempt_leases", schema=migration_schema
        )
        for name in (
            "dwe_guard_attempt_lease_update",
            "dwe_reject_attempt_lease_deletion",
        ):
            assert (
                connection.execute(
                    text(f"SELECT to_regprocedure('{name}()')")
                ).scalar_one()
                is None
            )
        for table in tables:
            assert (
                connection.execute(
                    select(table).order_by(*table.primary_key.columns)
                ).all()
                == before[table.name]
            )
        command.upgrade(config, "head")
        command.check(config)
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_leases)
            ).scalar_one()
            == 0
        )
