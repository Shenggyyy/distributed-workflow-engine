"""Current ownership renewal, terminal fencing, lock clocks and rollback."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import Barrier, Event
from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, event, select
from sqlalchemy.exc import DBAPIError

from tests.integration.migration_helpers import migration_config
from workflow_engine.domain.lease import (
    AttemptLease,
    LeaseClockRegressionError,
    LeaseExpiredError,
    LeaseOwnershipError,
)
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.claims import ClaimRepository, TaskClaim
from workflow_engine.repositories.leases import (
    LeaseInactiveError,
    LeaseNotFoundError,
    LeaseRepository,
    StoredLeaseError,
)
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    WorkflowRepository,
)
from workflow_engine.schema import (
    attempt_leases,
    task_attempts,
    task_runs,
    worker_sessions,
    workflow_runs,
)

pytestmark = pytest.mark.integration


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


@pytest.fixture
def leased(engine: Engine, lease_schema: str) -> TaskClaim:
    with transaction(engine, lease_schema) as connection:
        version = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name="renewal",
                tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),),
            )
        )
        run = RunRepository(connection).create(version.id).run
        worker = (
            WorkerRepository(connection)
            .register(uuid4(), worker_name="worker", max_concurrency=1)
            .session
        )
    with transaction(engine, lease_schema) as connection:
        result = ClaimRepository(connection, lease_seconds=300).claim_next(
            run.id, worker.id
        )
    assert result is not None
    return result


def renew(repo: LeaseRepository, value: TaskClaim) -> AttemptLease:
    return repo.renew(
        value.attempt.id,
        worker_session_id=value.lease.worker_session_id,
        lease_token=value.lease.lease_token,
    )


class ClockedLeases(LeaseRepository):
    def __init__(
        self,
        connection: Connection,
        observed: datetime,
        *,
        seconds: int = 30,
        sampled: Event | None = None,
    ) -> None:
        super().__init__(connection, lease_seconds=seconds)
        self.observed = observed
        self.sampled = sampled

    def _database_now(self) -> datetime:
        if self.sampled is not None:
            self.sampled.set()
        return self.observed


def test_committed_renewal_changes_only_lease_times(
    engine: Engine, lease_schema: str, leased: TaskClaim
) -> None:
    tables = (workflow_runs, task_runs, task_attempts, worker_sessions)
    with transaction(engine, lease_schema) as connection:
        before = {
            table.name: connection.execute(select(table)).all() for table in tables
        }
    observed = leased.lease.acquired_at + timedelta(seconds=10)
    with transaction(engine, lease_schema) as connection:
        result = renew(ClockedLeases(connection, observed, seconds=600), leased)
        with transaction(engine, lease_schema) as reader:
            assert (
                reader.execute(select(attempt_leases.c.last_renewed_at)).scalar_one()
                == leased.lease.last_renewed_at
            )
    assert result.lease_expires_at == observed + timedelta(seconds=600)
    assert result.last_renewed_at == observed
    assert result.model_dump(
        exclude={"last_renewed_at", "lease_expires_at"}
    ) == leased.lease.model_dump(exclude={"last_renewed_at", "lease_expires_at"})
    with transaction(engine, lease_schema) as connection:
        assert (
            AttemptLease.model_validate(
                dict(connection.execute(select(attempt_leases)).mappings().one())
            )
            == result
        )
        for table in tables:
            assert connection.execute(select(table)).all() == before[table.name]
        # Same observation with shorter policy cannot shrink accepted ownership.
        assert renew(ClockedLeases(connection, observed, seconds=1), leased) == result


@pytest.mark.parametrize("offset", [0, 299.999999, 300, 300.000001, -0.000001])
def test_exclusive_deadline_and_backward_clock(
    engine: Engine, lease_schema: str, leased: TaskClaim, offset: float
) -> None:
    observed = leased.lease.acquired_at + timedelta(seconds=offset)
    if 0 <= offset < 300:
        with transaction(engine, lease_schema) as connection:
            result = renew(ClockedLeases(connection, observed), leased)
        assert result.last_renewed_at == observed
    else:
        with pytest.raises(
            LeaseClockRegressionError if offset < 0 else LeaseExpiredError
        ):
            with transaction(engine, lease_schema) as connection:
                renew(ClockedLeases(connection, observed), leased)
        with transaction(engine, lease_schema) as connection:
            assert (
                connection.execute(
                    select(attempt_leases.c.last_renewed_at)
                ).scalar_one()
                == leased.lease.last_renewed_at
            )


@pytest.mark.parametrize("field", ["worker_session_id", "lease_token"])
def test_wrong_owner_cannot_renew(
    engine: Engine, lease_schema: str, leased: TaskClaim, field: str
) -> None:
    values = {
        "worker_session_id": leased.lease.worker_session_id,
        "lease_token": leased.lease.lease_token,
    }
    values[field] = uuid4()
    with pytest.raises(LeaseOwnershipError) as error:
        with transaction(engine, lease_schema) as connection:
            LeaseRepository(connection).renew(leased.attempt.id, **values)
    assert all(str(value) not in str(error.value) for value in values.values())


@pytest.mark.parametrize("kind", ["missing", "legacy"])
def test_missing_lease_and_unowned_historical_attempt(
    engine: Engine, lease_schema: str, leased: TaskClaim, kind: str
) -> None:
    attempt_id = uuid4()
    if kind == "legacy":
        with transaction(engine, lease_schema) as connection:
            connection.execute(
                task_attempts.insert().values(
                    id=attempt_id,
                    task_id=leased.task.id,
                    attempt_number=2,
                    status="FAILED",
                )
            )
    with pytest.raises(LeaseNotFoundError):
        with transaction(engine, lease_schema) as connection:
            LeaseRepository(connection).renew(
                attempt_id,
                worker_session_id=leased.lease.worker_session_id,
                lease_token=leased.lease.lease_token,
            )


@pytest.mark.parametrize(
    "table,status",
    [
        ("workflow_runs", "FAILED"),
        ("workflow_runs", "SUCCEEDED"),
        ("task_runs", "SUCCEEDED"),
        ("task_runs", "FAILED"),
        ("task_runs", "RETRY_WAIT"),
        ("task_attempts", "SUCCEEDED"),
        ("task_attempts", "FAILED"),
        ("task_attempts", "LOST"),
        ("task_attempts", "TIMED_OUT"),
        ("worker_sessions", "STOPPED"),
    ],
)
def test_inactive_execution_is_rejected(
    engine: Engine, lease_schema: str, leased: TaskClaim, table: str, status: str
) -> None:
    with transaction(engine, lease_schema) as connection:
        connection.exec_driver_sql(f"UPDATE {table} SET status = '{status}'")
    with pytest.raises(LeaseInactiveError):
        with transaction(engine, lease_schema) as connection:
            renew(LeaseRepository(connection), leased)
    with transaction(engine, lease_schema) as connection:
        assert (
            connection.execute(select(attempt_leases.c.last_renewed_at)).scalar_one()
            == leased.lease.last_renewed_at
        )


@pytest.mark.parametrize("lost", [False, True])
def test_expired_worker_heartbeat_does_not_revoke_live_attempt(
    engine: Engine, lease_schema: str, leased: TaskClaim, lost: bool
) -> None:
    with transaction(engine, lease_schema) as connection:
        deadline = connection.execute(
            select(worker_sessions.c.heartbeat_expires_at)
        ).scalar_one()
        if lost:
            connection.execute(worker_sessions.update().values(status="LOST"))
    with transaction(engine, lease_schema) as connection:
        result = renew(
            ClockedLeases(connection, deadline + timedelta(seconds=1)), leased
        )
    assert result.last_renewed_at > deadline


def test_old_attempt_cannot_renew_replacement_ownership(
    engine: Engine, lease_schema: str, leased: TaskClaim
) -> None:
    with transaction(engine, lease_schema) as connection:
        connection.execute(task_attempts.update().values(status="LOST"))
        connection.execute(task_runs.update().values(status="RETRY_WAIT"))
        connection.execute(task_runs.update().values(status="READY"))
    with transaction(engine, lease_schema) as connection:
        replacement = ClaimRepository(connection).claim_next(
            leased.task.run_id, leased.lease.worker_session_id
        )
    assert replacement is not None
    with pytest.raises(LeaseInactiveError):
        with transaction(engine, lease_schema) as connection:
            renew(LeaseRepository(connection), leased)
    with pytest.raises(LeaseOwnershipError):
        with transaction(engine, lease_schema) as connection:
            LeaseRepository(connection).renew(
                replacement.attempt.id,
                worker_session_id=leased.lease.worker_session_id,
                lease_token=leased.lease.lease_token,
            )
    with transaction(engine, lease_schema) as connection:
        assert (
            renew(LeaseRepository(connection), replacement).attempt_id
            == replacement.attempt.id
        )


def test_concurrent_renewals_keep_one_identity_and_monotonic_time(
    engine: Engine, lease_schema: str, leased: TaskClaim
) -> None:
    barrier = Barrier(4, timeout=10)

    def submit() -> AttemptLease:
        barrier.wait()
        with transaction(engine, lease_schema) as connection:
            result = renew(LeaseRepository(connection, lease_seconds=600), leased)
        return result

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(submit) for _ in range(4)]
        results = [f.result(timeout=20) for f in futures]
    assert {r.lease_token for r in results} == {leased.lease.lease_token}
    with transaction(engine, lease_schema) as connection:
        row = connection.execute(select(attempt_leases)).one()
        assert row.last_renewed_at == max(r.last_renewed_at for r in results)
        assert row.lease_expires_at == max(r.lease_expires_at for r in results)


@pytest.mark.parametrize(
    "target",
    [
        "workflow_runs",
        "worker_sessions",
        "task_runs",
        "task_attempts",
        "attempt_leases",
    ],
)
def test_no_clock_sample_before_all_locks_and_expiry_rechecked(
    engine: Engine, lease_schema: str, leased: TaskClaim, target: str
) -> None:
    entered, sampled = Event(), Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if f"FROM {target}" in statement and "FOR UPDATE" in statement:
            entered.set()

    def submit() -> None:
        with pytest.raises(LeaseExpiredError):
            with transaction(engine, lease_schema) as connection:
                renew(
                    ClockedLeases(
                        connection, leased.lease.lease_expires_at, sampled=sampled
                    ),
                    leased,
                )

    with ThreadPoolExecutor(max_workers=1) as executor:
        with transaction(engine, lease_schema) as blocker:
            blocker.exec_driver_sql(f"SELECT * FROM {target} FOR UPDATE").all()
            event.listen(engine, "before_cursor_execute", before_execute)
            try:
                pending = executor.submit(submit)
                assert entered.wait(timeout=5)
                assert not sampled.is_set() and not pending.done()
            finally:
                event.remove(engine, "before_cursor_execute", before_execute)
        pending.result(timeout=5)
    assert sampled.is_set()


@pytest.mark.parametrize("commit", [False, True])
@pytest.mark.parametrize("terminal", [False, True])
def test_waiter_rechecks_owner_commit_or_rollback(
    engine: Engine, lease_schema: str, leased: TaskClaim, commit: bool, terminal: bool
) -> None:
    entered = Event()

    def before_execute(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: object,
    ) -> None:
        if "FROM workflow_runs" in statement and "FOR UPDATE" in statement:
            entered.set()

    def submit() -> AttemptLease:
        with transaction(engine, lease_schema) as connection:
            return renew(
                ClockedLeases(
                    connection, leased.lease.acquired_at + timedelta(seconds=100)
                ),
                leased,
            )

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as owner:
            tx = owner.begin()
            try:
                migration_config(owner, lease_schema)
                renew(
                    ClockedLeases(
                        owner, leased.lease.acquired_at + timedelta(seconds=200)
                    ),
                    leased,
                )
                if terminal:
                    owner.execute(task_attempts.update().values(status="LOST"))
                event.listen(engine, "before_cursor_execute", before_execute)
                try:
                    pending = executor.submit(submit)
                    assert entered.wait(timeout=5) and not pending.done()
                finally:
                    event.remove(engine, "before_cursor_execute", before_execute)
                if commit:
                    tx.commit()
                else:
                    tx.rollback()
            finally:
                if tx.is_active:
                    tx.rollback()
        if commit:
            with pytest.raises(
                LeaseInactiveError if terminal else LeaseClockRegressionError
            ):
                pending.result(timeout=5)
        else:
            assert pending.result(
                timeout=5
            ).last_renewed_at == leased.lease.acquired_at + timedelta(seconds=100)


@pytest.mark.parametrize("phase", ["write", "commit", "abort"])
def test_failed_renewal_rolls_back_metadata(
    engine: Engine, lease_schema: str, leased: TaskClaim, phase: str
) -> None:
    if phase != "abort":
        with transaction(engine, lease_schema) as connection:
            connection.exec_driver_sql("""
                CREATE FUNCTION reject_renewal() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='23514',
                MESSAGE='injected renewal failure'; END; $$""")
            kind = (
                "CONSTRAINT TRIGGER reject_renewal AFTER"
                if phase == "commit"
                else "TRIGGER reject_renewal BEFORE"
            )
            deferred = "DEFERRABLE INITIALLY DEFERRED" if phase == "commit" else ""
            connection.exec_driver_sql(
                f"CREATE {kind} UPDATE ON attempt_leases {deferred} "
                "FOR EACH ROW EXECUTE FUNCTION reject_renewal()"
            )
    with pytest.raises(RuntimeError if phase == "abort" else DBAPIError):
        with transaction(engine, lease_schema) as connection:
            renew(
                ClockedLeases(
                    connection, leased.lease.acquired_at + timedelta(seconds=10)
                ),
                leased,
            )
            if phase == "abort":
                raise RuntimeError("abort")
    with transaction(engine, lease_schema) as connection:
        assert (
            connection.execute(select(attempt_leases.c.last_renewed_at)).scalar_one()
            == leased.lease.last_renewed_at
        )


def test_corrupt_stored_lease_is_rejected(
    engine: Engine, lease_schema: str, leased: TaskClaim
) -> None:
    with transaction(engine, lease_schema) as connection:
        connection.exec_driver_sql(
            "ALTER TABLE attempt_leases DROP CONSTRAINT ck_attempt_leases_lease_order"
        )
        connection.execute(
            attempt_leases.update().values(
                last_renewed_at=leased.lease.lease_expires_at
            )
        )
    with pytest.raises(StoredLeaseError):
        with transaction(engine, lease_schema) as connection:
            renew(LeaseRepository(connection), leased)


def test_transaction_lifetime_and_lock_timeout(
    engine: Engine, lease_schema: str, leased: TaskClaim
) -> None:
    with engine.connect() as connection:
        with pytest.raises(RepositoryTransactionError):
            LeaseRepository(connection)
        with connection.begin():
            repo = LeaseRepository(connection)
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                renew(repo, leased)
    with transaction(engine, lease_schema) as owner:
        owner.execute(select(workflow_runs).with_for_update()).all()
        with pytest.raises(DBAPIError) as error:
            with transaction(engine, lease_schema) as contender:
                contender.exec_driver_sql("SET LOCAL lock_timeout = '100ms'")
                renew(LeaseRepository(contender), leased)
        assert getattr(error.value.orig, "sqlstate", None) == "55P03"


@pytest.mark.parametrize("mode", ["AUTOCOMMIT", "REPEATABLE READ", "SERIALIZABLE"])
def test_transaction_modes(engine: Engine, mode: str) -> None:
    with engine.connect().execution_options(isolation_level=mode) as connection:
        with connection.begin():
            with pytest.raises(RepositoryTransactionError):
                LeaseRepository(connection)


@pytest.mark.parametrize("duration", [True, "30", 30.0, 0, 86401])
def test_policy_rejects_invalid_values_before_sql(
    engine: Engine, duration: object
) -> None:
    with engine.connect() as connection:
        with pytest.raises(ValueError):
            LeaseRepository(connection, lease_seconds=duration)  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["attempt_id", "worker_session_id", "lease_token"])
def test_invalid_ids_before_sql(engine: Engine, field: str) -> None:
    with engine.begin() as connection:
        values: dict[str, UUID] = {
            "attempt_id": uuid4(),
            "worker_session_id": uuid4(),
            "lease_token": uuid4(),
        }
        values[field] = "bad"  # type: ignore[assignment]
        with pytest.raises(TypeError):
            LeaseRepository(connection).renew(**values)
