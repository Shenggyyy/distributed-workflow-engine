"""Completion ownership, deferred outcome consistency and migration retention."""

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
from workflow_engine.repositories.claim_requests import ClaimRequestRepository
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    attempt_completions,
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
def completion_schema(engine: Engine, migration_schema: str) -> str:
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "head")
    return migration_schema


def seed(connection: Connection) -> dict[str, object]:
    version = WorkflowRepository(connection).publish(
        WorkflowDefinition(
            name="completion_" + uuid4().hex,
            tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),),
        )
    )
    run = RunRepository(connection).create_idempotent(
        version.id, idempotency_key=uuid4().hex
    )
    session_id = uuid4()
    WorkerRepository(connection, heartbeat_timeout_seconds=300).register(
        session_id, worker_name="worker", max_concurrency=1
    )
    decision = ClaimRequestRepository(connection).claim_next(
        run.run_id, session_id, request_id=uuid4()
    )
    assert decision is not None
    lease = decision.lease
    return {
        "attempt_id": lease.attempt_id,
        "worker_session_id": lease.worker_session_id,
        "lease_token": lease.lease_token,
        "outcome": "SUCCEEDED",
        "error_code": None,
        "accepted_at": lease.last_renewed_at,
    }


def finish(connection: Connection, data: dict[str, object]) -> None:
    connection.execute(
        task_attempts.update()
        .where(task_attempts.c.id == data["attempt_id"])
        .values(status=data["outcome"])
    )


@pytest.mark.parametrize("outcome", ["SUCCEEDED", "FAILED"])
@pytest.mark.parametrize("receipt_first", [False, True])
def test_commit_in_either_order(
    engine: Engine, completion_schema: str, outcome: str, receipt_first: bool
) -> None:
    with transaction(engine, completion_schema) as connection:
        data = seed(connection)
        data.update(
            outcome=outcome,
            error_code="handler_failed" if outcome == "FAILED" else None,
        )
        if not receipt_first:
            finish(connection, data)
        connection.execute(attempt_completions.insert().values(**data))
        if receipt_first:
            finish(connection, data)
    with transaction(engine, completion_schema) as connection:
        assert (
            dict(connection.execute(select(attempt_completions)).mappings().one())
            == data
        )


def test_schema_matches_metadata(engine: Engine, completion_schema: str) -> None:
    with transaction(engine, completion_schema) as connection:
        inspector = inspect(connection)
        columns = inspector.get_columns("attempt_completions", schema=completion_schema)
        assert all(c["default"] is None for c in columns)
        assert {c["name"] for c in columns if c["nullable"]} == {"error_code"}
        assert inspector.get_pk_constraint(
            "attempt_completions", schema=completion_schema
        )["constrained_columns"] == ["attempt_id"]
        keys = {
            fk["name"]: fk
            for fk in inspector.get_foreign_keys(
                "attempt_completions", schema=completion_schema
            )
        }
        assert len(keys) == 2
        owner = keys["fk_attempt_completions_owner_lease"]
        assert owner["constrained_columns"] == [
            "attempt_id",
            "worker_session_id",
            "lease_token",
        ]
        assert owner["options"]["ondelete"] == "RESTRICT"
        outcome = keys["fk_attempt_completions_attempt_outcome"]
        assert outcome["referred_columns"] == ["id", "status"]
        assert outcome["options"]["deferrable"] is True
        assert outcome["options"]["initially"] == "DEFERRED"
        command.check(migration_config(connection, completion_schema))


@pytest.mark.parametrize(
    "field",
    ["attempt_id", "worker_session_id", "lease_token", "outcome", "accepted_at"],
)
@pytest.mark.parametrize("explicit_null", [False, True])
def test_required_fields(
    engine: Engine, completion_schema: str, field: str, explicit_null: bool
) -> None:
    with transaction(engine, completion_schema) as connection:
        data = seed(connection)
        if explicit_null:
            data[field] = None
        else:
            del data[field]
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(
                    text(
                        f"INSERT INTO attempt_completions ({', '.join(data)}) "
                        f"VALUES ({', '.join(':' + key for key in data)})"
                    ),
                    data,
                )
        assert getattr(error.value.orig, "sqlstate", None) == "23502"


@pytest.mark.parametrize(
    "outcome,code,state",
    [
        ("RUNNING", None, "23514"),
        ("LOST", None, "23514"),
        ("TIMED_OUT", None, "23514"),
        ("succeeded", None, "23514"),
        ("FAILED", None, "23514"),
        ("FAILED", "", "23514"),
        ("FAILED", "bad code", "23514"),
        ("FAILED", "1invalid", "23514"),
        ("FAILED", "bad\n", "23514"),
        ("FAILED", "错误", "23514"),
        ("FAILED", "x" * 65, "22001"),
        ("SUCCEEDED", "error", "23514"),
    ],
)
def test_result_constraints(
    engine: Engine, completion_schema: str, outcome: str, code: str | None, state: str
) -> None:
    with transaction(engine, completion_schema) as connection:
        data = seed(connection)
        data.update(outcome=outcome, error_code=code)
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(attempt_completions.insert().values(**data))
        assert getattr(error.value.orig, "sqlstate", None) == state


def test_error_boundary_and_omitted_success_code(
    engine: Engine, completion_schema: str
) -> None:
    with transaction(engine, completion_schema) as connection:
        success, failure = seed(connection), seed(connection)
        del success["error_code"]
        failure.update(outcome="FAILED", error_code="A" + "_-0" * 21)
        for data in (success, failure):
            connection.execute(attempt_completions.insert().values(**data))
            finish(connection, data)


@pytest.mark.parametrize("stamp", ["infinity", "-infinity"])
def test_nonfinite_accepted_at(
    engine: Engine, completion_schema: str, stamp: str
) -> None:
    with transaction(engine, completion_schema) as connection:
        data = seed(connection)
        data["accepted_at"] = stamp
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(attempt_completions.insert().values(**data))
        assert getattr(error.value.orig, "sqlstate", None) == "23514"


@pytest.mark.parametrize(
    "field",
    ["attempt_id", "worker_session_id", "lease_token", "other_session", "other_token"],
)
def test_exact_ownership_fk(engine: Engine, completion_schema: str, field: str) -> None:
    with transaction(engine, completion_schema) as connection:
        data, other = seed(connection), seed(connection)
        if field == "other_session":
            data["worker_session_id"] = other["worker_session_id"]
        elif field == "other_token":
            data["lease_token"] = other["lease_token"]
        else:
            data[field] = uuid4()
        with pytest.raises(IntegrityError) as error:
            with connection.begin_nested():
                connection.execute(attempt_completions.insert().values(**data))
        assert getattr(error.value.orig, "sqlstate", None) == "23503"


@pytest.mark.parametrize("status", ["RUNNING", "FAILED", "TIMED_OUT", "LOST"])
def test_commit_rejects_mismatched_outcome_and_rolls_back(
    engine: Engine, completion_schema: str, status: str
) -> None:
    with transaction(engine, completion_schema) as connection:
        data = seed(connection)
    with pytest.raises(IntegrityError) as error:
        with transaction(engine, completion_schema) as connection:
            connection.execute(attempt_completions.insert().values(**data))
            finish(connection, {**data, "outcome": status})
            # The row can exist provisionally; validation happens at actual COMMIT.
            assert (
                connection.execute(
                    select(func.count()).select_from(attempt_completions)
                ).scalar_one()
                == 1
            )
    assert getattr(error.value.orig, "sqlstate", None) == "23503"
    with transaction(engine, completion_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(select(task_attempts.c.status)).scalar_one() == "RUNNING"
        )


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE attempt_completions SET outcome = outcome",
        "UPDATE attempt_completions SET error_code = NULL WHERE false",
        "DELETE FROM attempt_completions",
        "DELETE FROM attempt_completions WHERE false",
        "TRUNCATE attempt_completions",
    ],
)
@pytest.mark.parametrize("populated", [False, True])
def test_append_only(
    engine: Engine, completion_schema: str, statement: str, populated: bool
) -> None:
    with transaction(engine, completion_schema) as connection:
        if populated:
            data = seed(connection)
            finish(connection, data)
            connection.execute(attempt_completions.insert().values(**data))
    # Commit deferred FK events before testing the statement-level history guard.
    with transaction(engine, completion_schema) as connection:
        before = connection.execute(select(attempt_completions)).all()
        with pytest.raises(DBAPIError) as error:
            with connection.begin_nested():
                connection.execute(text(statement))
        assert getattr(error.value.orig, "sqlstate", None) == "55000"
        assert connection.execute(select(attempt_completions)).all() == before


@pytest.mark.parametrize("commit_owner", [True, False])
def test_concurrent_duplicate_waits_for_owner(
    engine: Engine, completion_schema: str, commit_owner: bool
) -> None:
    with transaction(engine, completion_schema) as connection:
        data = seed(connection)
        finish(connection, data)
    entered = Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if statement.startswith("INSERT INTO attempt_completions"):
            entered.set()

    def insert() -> str:
        try:
            with transaction(engine, completion_schema) as connection:
                event.listen(connection, "before_cursor_execute", before_execute)
                connection.execute(attempt_completions.insert().values(**data))
            return "committed"
        except IntegrityError as error:
            assert getattr(error.orig, "sqlstate", None) == "23505"
            return "duplicate"

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as owner:
            tx = owner.begin()
            try:
                migration_config(owner, completion_schema)
                owner.execute(attempt_completions.insert().values(**data))
                with transaction(engine, completion_schema) as observer:
                    assert (
                        observer.execute(
                            select(func.count()).select_from(attempt_completions)
                        ).scalar_one()
                        == 0
                    )
                pending = executor.submit(insert)
                assert entered.wait(timeout=5) and not pending.done()
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
    with transaction(engine, completion_schema) as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
            ).scalar_one()
            == 1
        )


def test_storage_does_not_authorize_time_or_task_state(
    engine: Engine, completion_schema: str
) -> None:
    with transaction(engine, completion_schema) as connection:
        data = seed(connection)
        data["accepted_at"] = (
            STAMP  # Before acquisition; only the repository can authorize.
        )
        connection.execute(worker_sessions.update().values(status="STOPPED"))
        finish(connection, data)
        connection.execute(attempt_completions.insert().values(**data))
    with transaction(engine, completion_schema) as connection:
        assert connection.execute(select(task_runs.c.status)).scalar_one() == "RUNNING"
        assert (
            connection.execute(select(attempt_completions.c.accepted_at)).scalar_one()
            == STAMP
        )


def test_populated_upgrade_downgrade_preserves_prior_history(
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
        claim_requests,
    )
    with transaction(engine, migration_schema) as connection:
        command.upgrade(migration_config(connection, migration_schema), "0007")
        seed(connection)  # A RUNNING Attempt and request binding remain unchanged.
        failed = seed(connection)
        failed.update(outcome="FAILED", error_code="handler_failed")
        finish(connection, failed)
        task_id = connection.execute(
            select(task_attempts.c.task_id).where(
                task_attempts.c.id == failed["attempt_id"]
            )
        ).scalar_one()
        connection.execute(
            task_attempts.insert().values(
                id=uuid4(), task_id=task_id, attempt_number=2, status="LOST"
            )
        )
        before = {
            t.name: connection.execute(select(t).order_by(*t.primary_key.columns)).all()
            for t in tables
        }
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.upgrade(config, "head")
        command.check(config)
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
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
        connection.execute(attempt_completions.insert().values(**failed))
    with transaction(engine, migration_schema) as connection:
        config = migration_config(connection, migration_schema)
        command.downgrade(config, "0007")
        assert not inspect(connection).has_table(
            "attempt_completions", schema=migration_schema
        )
        assert (
            connection.execute(
                text("SELECT to_regprocedure('dwe_reject_completion_mutation()')")
            ).scalar_one()
            is None
        )
        for table_name, name in (
            ("task_attempts", "uq_task_attempts_id_status"),
            ("attempt_leases", "uq_attempt_leases_owner_token"),
        ):
            assert name not in {
                key["name"]
                for key in inspect(connection).get_unique_constraints(
                    table_name, schema=migration_schema
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
                select(func.count()).select_from(attempt_completions)
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
