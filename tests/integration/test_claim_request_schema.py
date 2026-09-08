"""Durable claim decisions: identity, retention, races and populated upgrades."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from threading import Event
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, event, func, inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.claims import ClaimRepository
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    attempt_leases,
    claim_requests,
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
def request_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


def seed(connection: Connection) -> dict[str, object]:
    version = WorkflowRepository(connection).publish(
        WorkflowDefinition(
            name="request_" + uuid4().hex,
            tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),),
        )
    )
    run = RunRepository(connection).create_idempotent(
        version.id, idempotency_key=uuid4().hex
    )
    worker_id = uuid4()
    WorkerRepository(connection, heartbeat_timeout_seconds=300).register(
        worker_id, worker_name="worker", max_concurrency=2
    )
    claim = ClaimRepository(connection).claim_next(run.run_id, worker_id)
    assert claim is not None
    return {
        "worker_session_id": worker_id,
        "request_id": uuid4(),
        "run_id": run.run_id,
        "attempt_id": claim.attempt.id,
        "created_at": STAMP,
    }


def test_committed_grant_no_work_and_metadata(
    engine: Engine, request_schema: str
) -> None:
    with transaction(engine, request_schema) as connection:
        data = seed(connection)
        connection.execute(claim_requests.insert().values(**data))
        # Several completed no-work polls are legal; NULL is not a placeholder.
        for _ in range(2):
            connection.execute(
                claim_requests.insert().values(
                    **{**data, "request_id": uuid4(), "attempt_id": None}
                )
            )
    with transaction(engine, request_schema) as connection:
        rows = connection.execute(select(claim_requests)).mappings().all()
        assert len(rows) == 3
        assert sum(row["attempt_id"] is None for row in rows) == 2
        assert dict(next(row for row in rows if row["attempt_id"] is not None)) == data
        columns = inspect(connection).get_columns(
            "claim_requests", schema=request_schema
        )
        assert all(column["default"] is None for column in columns)
        assert {c["name"] for c in columns if c["nullable"]} == {"attempt_id"}
        pk = inspect(connection).get_pk_constraint(
            "claim_requests", schema=request_schema
        )
        assert pk["constrained_columns"] == ["worker_session_id", "request_id"]
        keys = inspect(connection).get_foreign_keys(
            "claim_requests", schema=request_schema
        )
        assert len(keys) == 3
        assert all(key["options"]["ondelete"] == "RESTRICT" for key in keys)
        assert any(
            key["referred_table"] == "attempt_leases"
            and key["constrained_columns"] == ["attempt_id", "worker_session_id"]
            for key in keys
        )
        command.check(migration_config(connection, request_schema))


@pytest.mark.parametrize(
    "field", ["worker_session_id", "request_id", "run_id", "created_at"]
)
@pytest.mark.parametrize("explicit_null", [False, True])
def test_required_fields(
    engine: Engine, request_schema: str, field: str, explicit_null: bool
) -> None:
    with transaction(engine, request_schema) as connection:
        data = seed(connection)
        if explicit_null:
            data[field] = None
        else:
            del data[field]
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(
                    text(
                        f"INSERT INTO claim_requests ({', '.join(data)}) "
                        f"VALUES ({', '.join(':' + key for key in data)})"
                    ),
                    data,
                )
        assert getattr(error.value.orig, "sqlstate", None) == "23502"


@pytest.mark.parametrize("stamp", ["infinity", "-infinity"])
def test_nonfinite_time_is_rejected(
    engine: Engine, request_schema: str, stamp: str
) -> None:
    with transaction(engine, request_schema) as connection:
        data = seed(connection)
        data["created_at"] = stamp
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(claim_requests.insert().values(**data))
        assert getattr(error.value.orig, "sqlstate", None) == "23514"


@pytest.mark.parametrize(
    "field", ["worker_session_id", "run_id", "attempt_id", "wrong_owner"]
)
def test_foreign_keys_include_exact_lease_owner(
    engine: Engine, request_schema: str, field: str
) -> None:
    with transaction(engine, request_schema) as connection:
        data = seed(connection)
        if field == "wrong_owner":
            other = seed(connection)
            data["worker_session_id"] = other["worker_session_id"]
        else:
            data[field] = uuid4()
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(claim_requests.insert().values(**data))
        assert getattr(error.value.orig, "sqlstate", None) == "23503"


def test_no_work_still_requires_existing_worker(
    engine: Engine, request_schema: str
) -> None:
    with transaction(engine, request_schema) as connection:
        data = seed(connection)
        data.update(worker_session_id=uuid4(), attempt_id=None)
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(claim_requests.insert().values(**data))
        assert getattr(error.value.orig, "sqlstate", None) == "23503"


def test_unleased_attempt_cannot_be_bound(engine: Engine, request_schema: str) -> None:
    with transaction(engine, request_schema) as connection:
        data = seed(connection)
        task_id = connection.execute(select(task_attempts.c.task_id)).scalar_one()
        connection.execute(task_attempts.update().values(status="LOST"))
        unowned = uuid4()
        connection.execute(
            task_attempts.insert().values(id=unowned, task_id=task_id, attempt_number=2)
        )
        data["attempt_id"] = unowned
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(claim_requests.insert().values(**data))
        assert getattr(error.value.orig, "sqlstate", None) == "23503"


@pytest.mark.parametrize("duplicate_attempt", [False, True])
def test_duplicate_request_or_attempt_is_rejected(
    engine: Engine, request_schema: str, duplicate_attempt: bool
) -> None:
    with transaction(engine, request_schema) as connection:
        data = seed(connection)
        connection.execute(claim_requests.insert().values(**data))
        duplicate = (
            {**data, "request_id": uuid4()}
            if duplicate_attempt
            else {**data, "attempt_id": None}
        )
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(claim_requests.insert().values(**duplicate))
        assert getattr(error.value.orig, "sqlstate", None) == "23505"


def test_request_id_is_scoped_to_worker_not_run(
    engine: Engine, request_schema: str
) -> None:
    with transaction(engine, request_schema) as connection:
        first, second = seed(connection), seed(connection)
        first["attempt_id"] = second["attempt_id"] = None
        second["request_id"] = first["request_id"]
        connection.execute(claim_requests.insert().values(**first))
        connection.execute(claim_requests.insert().values(**second))
        # A different Run cannot reuse a request within the original Worker scope.
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(
                    claim_requests.insert().values(
                        **{**first, "run_id": second["run_id"]}
                    )
                )
        assert getattr(error.value.orig, "sqlstate", None) == "23505"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE claim_requests SET attempt_id = NULL",
        "UPDATE claim_requests SET request_id = request_id WHERE false",
        "DELETE FROM claim_requests",
        "DELETE FROM claim_requests WHERE false",
        "TRUNCATE claim_requests",
        "TRUNCATE worker_sessions CASCADE",
    ],
)
@pytest.mark.parametrize("populated", [False, True])
def test_history_cannot_be_mutated(
    engine: Engine, request_schema: str, statement: str, populated: bool
) -> None:
    with transaction(engine, request_schema) as connection:
        if populated:
            connection.execute(claim_requests.insert().values(**seed(connection)))
        before = connection.execute(select(claim_requests)).all()
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(text(statement))
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        assert connection.execute(select(claim_requests)).all() == before


def test_no_work_cannot_be_upgraded_to_a_grant(
    engine: Engine, request_schema: str
) -> None:
    with transaction(engine, request_schema) as connection:
        data = seed(connection)
        connection.execute(
            claim_requests.insert().values(**{**data, "attempt_id": None})
        )
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(
                    claim_requests.update().values(attempt_id=data["attempt_id"])
                )
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        assert (
            connection.execute(select(claim_requests.c.attempt_id)).scalar_one() is None
        )


@pytest.mark.parametrize("commit_owner", [False, True])
@pytest.mark.parametrize("same_request", [False, True])
def test_waiting_duplicate_after_commit_or_rollback(
    engine: Engine, request_schema: str, commit_owner: bool, same_request: bool
) -> None:
    with transaction(engine, request_schema) as connection:
        data = seed(connection)
    entered = Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if statement.startswith("INSERT INTO claim_requests"):
            entered.set()

    def insert() -> str:
        try:
            with transaction(engine, request_schema) as connection:
                event.listen(connection, "before_cursor_execute", before_execute)
                contender = dict(data)
                if same_request:
                    # Isolate the request PK conflict from Attempt uniqueness.
                    contender["attempt_id"] = None
                else:
                    contender["request_id"] = uuid4()
                connection.execute(claim_requests.insert().values(**contender))
            return "committed"
        except IntegrityError as error:
            assert getattr(error.orig, "sqlstate", None) == "23505"
            return "duplicate"

    # Release the owner before joining the executor, even if an assertion fails.
    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as owner:
            tx = owner.begin()
            try:
                migration_config(owner, request_schema)
                owner.execute(claim_requests.insert().values(**data))
                with transaction(engine, request_schema) as observer:
                    assert (
                        observer.execute(
                            select(func.count()).select_from(claim_requests)
                        ).scalar_one()
                        == 0
                    )
                pending = executor.submit(insert)
                assert entered.wait(timeout=5)
                assert not pending.done()
                if commit_owner:
                    tx.commit()
                else:
                    tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
        assert pending.result(timeout=10) == (
            "duplicate" if commit_owner else "committed"
        )
    with transaction(engine, request_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(claim_requests)
            ).scalar_one()
            == 1
        )


def test_storage_is_not_claim_authorization(
    engine: Engine, request_schema: str
) -> None:
    with transaction(engine, request_schema) as connection:
        data, other = seed(connection), seed(connection)
        connection.execute(task_attempts.update().values(status="LOST"))
        connection.execute(worker_sessions.update().values(status="LOST"))
        # Existence and owner FKs do not verify Run membership, liveness or clock.
        # The future repository must reject this as executable ownership.
        connection.execute(
            claim_requests.insert().values(**{**data, "run_id": other["run_id"]})
        )
        assert (
            connection.execute(select(claim_requests.c.run_id)).scalar_one()
            == other["run_id"]
        )


def test_populated_upgrade_downgrade_preserves_all_prior_data(
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
        attempt_leases,
    )
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "0006")
        data = seed(connection)
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
                select(func.count()).select_from(claim_requests)
            ).scalar_one()
            == 0
        )
        connection.execute(claim_requests.insert().values(**data))
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.downgrade(config, "0006")
        assert not inspect(connection).has_table(
            "claim_requests", schema=migration_schema
        )
        assert (
            connection.execute(
                text("SELECT to_regprocedure('dwe_reject_claim_request_mutation()')")
            ).scalar_one()
            is None
        )
        assert "uq_attempt_leases_attempt_id" not in {
            key["name"]
            for key in inspect(connection).get_unique_constraints(
                "attempt_leases", schema=migration_schema
            )
        }
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
                select(func.count()).select_from(claim_requests)
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
