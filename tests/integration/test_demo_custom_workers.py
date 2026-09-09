"""Custom demo startup admits only the intended, bounded worker cohort."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, func, select

from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.demo.custom_submissions import submit_custom
from workflow_engine.demo.custom_workers import (
    add_demo_worker,
    check_demo_startup,
    custom_worker_names,
)
from workflow_engine.demo.startup import DemoStartupError, cohort_ready
from workflow_engine.domain.worker import WorkerSession
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    demo_runs,
    demo_workers,
    task_runs,
    worker_sessions,
    workflow_runs,
)

pytestmark = pytest.mark.integration


def custom_run(engine: Engine) -> UUID:
    return submit_custom(
        engine,
        WorkflowDefinition(
            name="custom_workers_" + uuid4().hex,
            tasks=(
                TaskDefinition(task_id="Fetch", task_type="demo.observe"),
                TaskDefinition(
                    task_id="Finish", task_type="demo.join", depends_on=("Fetch",)
                ),
            ),
        ),
        uuid4().hex,
    ).run_id


def session(run_id: UUID, slot: int = 0, **fields: Any) -> WorkerSession:
    return WorkerSession.model_validate(
        {
            "id": uuid4(),
            "worker_name": custom_worker_names(run_id)[slot],
            "max_concurrency": 1,
            **fields,
        }
    )


def register(engine: Engine, worker: WorkerSession) -> None:
    with engine.begin() as connection:
        actual = WorkerRepository(connection, heartbeat_timeout_seconds=300).register(
            worker.id,
            worker_name=worker.worker_name,
            max_concurrency=worker.max_concurrency,
        )
        assert actual.session == worker


def memberships(engine: Engine) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                select(demo_workers).order_by(demo_workers.c.worker_session_id)
            ).mappings()
        ]


def manual_run(
    engine: Engine, *, scenario: str | None, kind: str = "demo.observe"
) -> UUID:
    # Intentional old/noncanonical storage fixtures; they bypass custom submission.
    with engine.begin() as connection:
        version = WorkflowRepository(connection).publish(
            WorkflowDefinition(
                name="manual_workers_" + uuid4().hex,
                tasks=(TaskDefinition(task_id="Legacy", task_type=kind),),
            )
        )
        run_id = RunRepository(connection).create(version.id).run.id
        if scenario is not None:
            connection.execute(
                demo_runs.insert().values(run_id=run_id, scenario=scenario)
            )
        return run_id


def test_two_custom_members_use_exact_names_and_release_the_real_cohort(
    http_engine: Engine,
) -> None:
    run_id = custom_run(http_engine)
    first, second = session(run_id), session(run_id, 1)
    assert custom_worker_names(run_id) == (
        f"dwe-demo-{run_id.hex}-a",
        f"dwe-demo-{run_id.hex}-b",
    )
    for worker in (first, second):
        check_demo_startup(http_engine, run_id, worker, cohort_size=2)
    with http_engine.connect() as connection:
        assert connection.execute(select(worker_sessions)).first() is None
        assert connection.execute(select(demo_workers)).first() is None
    register(http_engine, first)
    add_demo_worker(http_engine, run_id, first, cohort_size=2)
    assert not cohort_ready(http_engine, run_id, first.id, cohort_size=2)
    register(http_engine, second)
    add_demo_worker(http_engine, run_id, second, cohort_size=2)
    assert cohort_ready(http_engine, run_id, first.id, cohort_size=2)
    assert cohort_ready(http_engine, run_id, second.id, cohort_size=2)
    assert {row["worker_session_id"] for row in memberships(http_engine)} == {
        first.id,
        second.id,
    }
    with http_engine.connect() as connection:
        assert {
            row.task_key: row.status for row in connection.execute(select(task_runs))
        } == {"Fetch": "READY", "Finish": "PENDING"}


@pytest.mark.parametrize("invalid", ["name", "slots", "cohort"])
def test_wrong_custom_startup_parameters_refuse_without_membership(
    http_engine: Engine, invalid: str
) -> None:
    run_id = custom_run(http_engine)
    fields = (
        {"worker_name": "unrelated_worker"}
        if invalid == "name"
        else {"max_concurrency": 2}
        if invalid == "slots"
        else {}
    )
    worker = session(run_id, **fields)
    cohort = 1 if invalid == "cohort" else 2
    with pytest.raises(DemoStartupError):
        check_demo_startup(http_engine, run_id, worker, cohort_size=cohort)
    register(http_engine, worker)
    with pytest.raises(DemoStartupError):
        add_demo_worker(http_engine, run_id, worker, cohort_size=cohort)
    assert memberships(http_engine) == []


@pytest.mark.parametrize(
    "case", ["unsupported-handler", "noncanonical", "terminal", "ordinary", "missing"]
)
def test_scope_saved_definition_and_run_liveness_are_rechecked_before_attachment(
    http_engine: Engine, case: str
) -> None:
    if case == "missing":
        run_id = uuid4()
    elif case == "terminal":
        run_id = custom_run(http_engine)
    else:
        run_id = manual_run(
            http_engine,
            scenario=None if case == "ordinary" else "custom",
            kind="demo.echo" if case == "unsupported-handler" else "demo.observe",
        )
    worker = session(run_id)
    if case == "terminal":
        # Preflight can succeed, then the Run can finish before HTTP registration.
        check_demo_startup(http_engine, run_id, worker, cohort_size=2)
        with http_engine.begin() as connection:
            connection.execute(
                workflow_runs.update()
                .where(workflow_runs.c.id == run_id)
                .values(status="FAILED")
            )
    with pytest.raises(DemoStartupError):
        check_demo_startup(http_engine, run_id, worker, cohort_size=2)
    register(http_engine, worker)
    with pytest.raises(DemoStartupError):
        add_demo_worker(http_engine, run_id, worker, cohort_size=2)
    assert memberships(http_engine) == []


@pytest.mark.parametrize(
    "actual", ["missing", "different-name", "different-slots", "lost"]
)
def test_attachment_requires_the_real_registered_identity(
    http_engine: Engine, actual: str
) -> None:
    run_id = custom_run(http_engine)
    requested = session(run_id)
    check_demo_startup(http_engine, run_id, requested, cohort_size=2)
    if actual != "missing":
        registered = requested.model_copy(
            update={
                "worker_name": "other_worker"
                if actual == "different-name"
                else requested.worker_name,
                "max_concurrency": 2 if actual == "different-slots" else 1,
            }
        )
        register(http_engine, registered)
        if actual == "lost":
            with http_engine.begin() as connection:
                connection.execute(
                    worker_sessions.update()
                    .where(worker_sessions.c.id == requested.id)
                    .values(status="LOST")
                )
    with pytest.raises(DemoStartupError):
        add_demo_worker(http_engine, run_id, requested, cohort_size=2)
    assert memberships(http_engine) == []


def test_expired_registered_session_cannot_attach_or_be_revived(
    http_engine: Engine,
) -> None:
    run_id = custom_run(http_engine)
    requested = session(run_id)
    check_demo_startup(http_engine, run_id, requested, cohort_size=2)
    with http_engine.begin() as connection:
        # Clock fixture for a previously registered session; no sleeps or clock changes.
        stamp = connection.scalar(select(func.clock_timestamp()))
        assert stamp is not None
        connection.execute(
            worker_sessions.insert().values(
                id=requested.id,
                worker_name=requested.worker_name,
                max_concurrency=1,
                status="ACTIVE",
                created_at=stamp - timedelta(seconds=10),
                last_heartbeat_at=stamp - timedelta(seconds=5),
                heartbeat_expires_at=stamp - timedelta(seconds=1),
            )
        )
        before = dict(connection.execute(select(worker_sessions)).mappings().one())
    with pytest.raises(DemoStartupError):
        add_demo_worker(http_engine, run_id, requested, cohort_size=2)
    assert memberships(http_engine) == []
    with http_engine.connect() as connection:
        assert (
            dict(connection.execute(select(worker_sessions)).mappings().one()) == before
        )


def test_concurrent_same_name_startups_can_attach_only_one_session(
    http_engine: Engine,
) -> None:
    run_id = custom_run(http_engine)
    contenders = (session(run_id), session(run_id))
    for worker in contenders:
        check_demo_startup(http_engine, run_id, worker, cohort_size=2)
        register(http_engine, worker)
    barrier = Barrier(2, timeout=10)

    def attach(worker: WorkerSession) -> bool:
        barrier.wait()
        try:
            add_demo_worker(http_engine, run_id, worker, cohort_size=2)
            return True
        except DemoStartupError:
            return False

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(attach, worker) for worker in contenders]
        results = [future.result(timeout=15) for future in futures]
    assert sum(results) == 1
    saved = memberships(http_engine)
    assert len(saved) == 1 and saved[0]["worker_session_id"] in {
        worker.id for worker in contenders
    }
    other = session(run_id, 1)
    check_demo_startup(http_engine, run_id, other, cohort_size=2)
    register(http_engine, other)
    add_demo_worker(http_engine, run_id, other, cohort_size=2)
    assert len(memberships(http_engine)) == 2
    assert cohort_ready(http_engine, run_id, other.id, cohort_size=2)


def test_full_custom_cohort_rejects_an_extra_session_without_changing_other_runs(
    http_engine: Engine,
) -> None:
    current, unrelated = custom_run(http_engine), custom_run(http_engine)
    for run_id, slot in ((current, 0), (current, 1), (unrelated, 0)):
        worker = session(run_id, slot)
        register(http_engine, worker)
        add_demo_worker(http_engine, run_id, worker, cohort_size=2)
    before = memberships(http_engine)
    extra = session(current)
    with pytest.raises(DemoStartupError):
        check_demo_startup(http_engine, current, extra, cohort_size=2)
    register(http_engine, extra)
    with pytest.raises(DemoStartupError):
        add_demo_worker(http_engine, current, extra, cohort_size=2)
    assert memberships(http_engine) == before
    assert sum(row["run_id"] == unrelated for row in before) == 1


def test_an_unexpected_existing_member_is_not_silently_accepted(
    http_engine: Engine,
) -> None:
    run_id = custom_run(http_engine)
    unexpected = session(run_id, worker_name="unexpected_member")
    register(http_engine, unexpected)
    with http_engine.begin() as connection:
        connection.execute(
            demo_workers.insert().values(worker_session_id=unexpected.id, run_id=run_id)
        )
    before = memberships(http_engine)
    requested = session(run_id)
    with pytest.raises(DemoStartupError):
        check_demo_startup(http_engine, run_id, requested, cohort_size=2)
    register(http_engine, requested)
    with pytest.raises(DemoStartupError):
        add_demo_worker(http_engine, run_id, requested, cohort_size=2)
    assert memberships(http_engine) == before


def test_predefined_legacy_worker_settings_remain_unchanged(
    http_engine: Engine,
) -> None:
    run_id = manual_run(http_engine, scenario="parallel", kind="demo.echo")
    worker = session(run_id, worker_name="legacy_label", max_concurrency=2)
    check_demo_startup(http_engine, run_id, worker, cohort_size=1)
    register(http_engine, worker)
    add_demo_worker(http_engine, run_id, worker, cohort_size=1)
    assert memberships(http_engine) == [
        {"worker_session_id": worker.id, "run_id": run_id}
    ]
