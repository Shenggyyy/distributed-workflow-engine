"""Custom HTTP submission is atomic and replayable against real PostgreSQL."""

import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Connection, Engine, event, func, select
from starlette.types import Message, Receive, Scope, Send

from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.config import Settings
from workflow_engine.demo.api import create_demo_app
from workflow_engine.demo.custom_submissions import submit_custom
from workflow_engine.domain.workflow import WorkflowDefinition
from workflow_engine.schema import (
    demo_custom_submissions,
    demo_invocations,
    demo_runs,
    demo_samples,
    demo_workers,
    run_creation_requests,
    task_attempts,
    task_runs,
    worker_sessions,
    workflow_runs,
    workflow_versions,
    workflows,
)

pytestmark = pytest.mark.integration
PATH = "/demo/custom/runs"
TABLES = (
    workflows,
    workflow_versions,
    workflow_runs,
    task_runs,
    task_attempts,
    worker_sessions,
    demo_runs,
    demo_workers,
    demo_custom_submissions,
    demo_invocations,
    demo_samples,
    run_creation_requests,
)


@pytest.fixture
def client(http_engine: Engine) -> Iterator[TestClient]:
    with TestClient(
        create_demo_app(Settings(environment="test"), engine=http_engine)
    ) as value:
        yield value


def payload() -> dict[str, Any]:
    return {
        "name": "custom_submission",
        "tasks": [
            {
                "task_id": "Assemble",
                "task_type": "demo.join",
                "depends_on": ["Check", "Total"],
            },
            {"task_id": "Load", "task_type": "demo.observe"},
            {"task_id": "Total", "task_type": "demo.observe", "depends_on": ["Load"]},
            {"task_id": "Check", "task_type": "demo.observe", "depends_on": ["Load"]},
        ],
    }


def stored(engine: Engine) -> dict[str, Any]:
    with engine.connect() as connection:
        return {
            table.name: [
                dict(row)
                for row in connection.execute(
                    select(table).order_by(*table.primary_key.columns)
                ).mappings()
            ]
            for table in TABLES
        }


def assert_counts(engine: Engine, runs: int) -> None:
    values = stored(engine)
    expected = {
        "workflows": int(runs > 0),
        "workflow_versions": runs,
        "workflow_runs": runs,
        "task_runs": runs * 4,
        "demo_runs": runs,
        "demo_custom_submissions": runs,
    }
    assert {name: len(rows) for name, rows in values.items()} == {
        name: expected.get(name, 0) for name in values
    }


def test_confirmed_creation_waits_for_workers_and_canonical_replay_is_immutable(
    client: TestClient, http_engine: Engine
) -> None:
    body = payload()
    preview = client.post("/demo/custom/validate", json=body)
    assert preview.status_code == 200
    canonical = preview.json()["definition"]
    headers = {"Idempotency-Key": "request-one"}
    response = client.post(PATH, json=body, headers=headers)
    assert response.status_code == 201
    receipt = response.json()
    assert set(receipt) == {"run_id", "workflow_version_id", "scenario"}
    assert receipt["scenario"] == "custom"
    assert response.headers["Location"] == f"/demo/runs/{receipt['run_id']}"
    snapshot = client.get(response.headers["Location"]).json()
    assert snapshot["run"]["definition"] == canonical
    assert snapshot["run"]["workflow_version_id"] == receipt["workflow_version_id"]
    assert snapshot["run"]["status"] == "RUNNING"
    assert {task["task_key"]: task["status"] for task in snapshot["tasks"]} == {
        "Load": "READY",
        "Total": "PENDING",
        "Check": "PENDING",
        "Assemble": "PENDING",
    }
    assert snapshot["attempts"] == snapshot["workers"] == snapshot["samples"] == []
    before = stored(http_engine)
    assert before["demo_custom_submissions"][0]["definition"] == canonical
    assert before["demo_custom_submissions"][0]["run_id"] == UUID(receipt["run_id"])
    # Object-key order and omitted schema/policy/dependency defaults are equivalent.
    for replay_body in (body, json.loads(json.dumps(canonical, sort_keys=True))):
        replay = client.post(PATH, json=replay_body, headers=headers)
        assert replay.status_code == 201 and replay.json() == receipt
        assert replay.headers["Location"] == response.headers["Location"]
    assert stored(http_engine) == before
    assert_counts(http_engine, 1)
    listing = client.get("/demo/runs").json()
    assert [(row["run_id"], row["scenario"]) for row in listing] == [
        (receipt["run_id"], "custom")
    ]


@pytest.mark.parametrize(
    "headers",
    [
        [],
        [("Idempotency-Key", "private/key-sentinel")],
        [("Idempotency-Key", "private-key"), ("idempotency-key", "private-key")],
    ],
    ids=["missing", "malformed", "duplicate"],
)
def test_invalid_idempotency_headers_leave_no_publication_or_runtime_data(
    client: TestClient, http_engine: Engine, headers: list[tuple[str, str]]
) -> None:
    response = client.post(PATH, json=payload(), headers=headers)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert "Location" not in response.headers
    assert "private/key-sentinel" not in response.text
    assert "private-key" not in response.text
    assert_counts(http_engine, 0)


@pytest.mark.parametrize(
    "change", ["name", "task_order", "dependency_order", "handler"]
)
def test_same_key_different_canonical_body_conflicts_without_publishing(
    client: TestClient, http_engine: Engine, change: str
) -> None:
    body = payload()
    headers = {"Idempotency-Key": "private-request-key"}
    assert client.post(PATH, json=body, headers=headers).status_code == 201
    before = stored(http_engine)
    if change == "name":
        body["name"] = "private_other_name"
    elif change == "task_order":
        body["tasks"].reverse()
    elif change == "dependency_order":
        body["tasks"][0]["depends_on"].reverse()
    else:
        body["tasks"][1]["task_type"] = "demo.join"
    response = client.post(PATH, json=body, headers=headers)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "demo_submission_conflict"
    assert "Location" not in response.headers
    assert "private-request-key" not in response.text
    assert "private_other_name" not in response.text
    assert stored(http_engine) == before


@pytest.mark.parametrize("different", [False, True])
def test_concurrent_same_key_requests_commit_one_version_and_run(
    client: TestClient, http_engine: Engine, different: bool
) -> None:
    barrier = Barrier(4, timeout=10)

    def submit(index: int) -> tuple[int, dict[str, Any]]:
        body = payload()
        if different and index % 2:
            body["name"] = "other_submission"
        barrier.wait()
        response = client.post(PATH, json=body, headers={"Idempotency-Key": "shared"})
        return response.status_code, response.json()

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(submit, index) for index in range(4)]
        results = [future.result(timeout=20) for future in futures]
    receipts = [result for status, result in results if status == 201]
    assert len(receipts) == (2 if different else 4)
    assert all(receipt == receipts[0] for receipt in receipts)
    assert all(status in (201, 409) for status, _ in results)
    assert all(
        result["error"]["code"] == "demo_submission_conflict"
        for status, result in results
        if status == 409
    )
    assert_counts(http_engine, 1)


def test_distinct_case_sensitive_keys_create_distinct_versions_of_same_workflow(
    client: TestClient, http_engine: Engine
) -> None:
    responses = [
        client.post(PATH, json=payload(), headers={"Idempotency-Key": key})
        for key in ("Request", "request")
    ]
    assert all(response.status_code == 201 for response in responses)
    assert len({response.json()["run_id"] for response in responses}) == 2
    assert len({response.json()["workflow_version_id"] for response in responses}) == 2
    assert_counts(http_engine, 2)
    versions = stored(http_engine)["workflow_versions"]
    assert {version["version_number"] for version in versions} == {1, 2}


def test_forced_lock_hash_collision_does_not_merge_distinct_request_keys(
    client: TestClient, http_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "workflow_engine.demo.custom_submissions._submission_lock_key", lambda _: 7
    )
    barrier = Barrier(4, timeout=10)

    def submit(index: int) -> tuple[str, dict[str, Any]]:
        key = f"collision-{index % 2}"
        barrier.wait()
        response = client.post(PATH, json=payload(), headers={"Idempotency-Key": key})
        assert response.status_code == 201
        return key, response.json()

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(submit, index) for index in range(4)]
        results = [future.result(timeout=20) for future in futures]
    assert len({receipt["run_id"] for _, receipt in results}) == 2
    for key in ("collision-0", "collision-1"):
        assert (
            len({receipt["run_id"] for found, receipt in results if found == key}) == 1
        )
    assert_counts(http_engine, 2)


@pytest.mark.parametrize("inherited", ["REPEATABLE READ", "AUTOCOMMIT"])
def test_service_owns_a_real_read_committed_transaction(
    http_engine: Engine, inherited: str
) -> None:
    inherited_engine = http_engine.execution_options(isolation_level=inherited)
    observed: list[tuple[str, object, bool]] = []

    def observe(
        connection: Connection,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        if "pg_advisory_xact_lock" in statement:
            observed.append(
                (
                    connection.get_isolation_level(),
                    connection.get_execution_options().get("isolation_level"),
                    connection.in_transaction(),
                )
            )

    event.listen(inherited_engine, "after_cursor_execute", observe)
    try:
        definition = WorkflowDefinition.model_validate(payload())
        first = submit_custom(inherited_engine, definition, "owned-transaction")
        assert submit_custom(inherited_engine, definition, "owned-transaction") == first
    finally:
        event.remove(inherited_engine, "after_cursor_execute", observe)
    assert observed == [("READ COMMITTED", "READ COMMITTED", True)] * 2
    assert_counts(http_engine, 1)


def test_replay_retains_a_terminal_runs_identity_and_task_progress(
    client: TestClient, http_engine: Engine
) -> None:
    headers = {"Idempotency-Key": "completed-run"}
    first = client.post(PATH, json=payload(), headers=headers)
    assert first.status_code == 201
    with http_engine.begin() as connection:
        # Legal persisted-state fixture only; this is not Handler execution proof.
        connection.execute(
            task_runs.update()
            .where(task_runs.c.status == "PENDING")
            .values(status="READY")
        )
        connection.execute(task_runs.update().values(status="RUNNING"))
        connection.execute(task_runs.update().values(status="SUCCEEDED"))
        connection.execute(workflow_runs.update().values(status="SUCCEEDED"))
    before = stored(http_engine)
    replay = client.post(PATH, json=payload(), headers=headers)
    assert replay.status_code == 201 and replay.json() == first.json()
    assert stored(http_engine) == before


@pytest.mark.parametrize(
    "phase", ["version", "tasks", "membership", "receipt", "commit"]
)
def test_write_and_commit_failures_roll_back_everything_without_success(
    client: TestClient, http_engine: Engine, migration_schema: str, phase: str
) -> None:
    table = {
        "version": "workflow_versions",
        "tasks": "task_runs",
        "membership": "demo_runs",
        "receipt": "demo_custom_submissions",
        "commit": "demo_custom_submissions",
    }[phase]
    with http_engine.begin() as connection:
        connection.exec_driver_sql(f'''
            CREATE FUNCTION "{migration_schema}".reject_custom_submission()
            RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN
                RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='private-db-sentinel';
            END; $$
        ''')
        if phase == "commit":
            connection.exec_driver_sql(f'''
                CREATE CONSTRAINT TRIGGER reject_custom_submission
                AFTER INSERT ON "{migration_schema}".{table}
                DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
                EXECUTE FUNCTION "{migration_schema}".reject_custom_submission()
            ''')
        else:
            connection.exec_driver_sql(f'''
                CREATE TRIGGER reject_custom_submission
                BEFORE INSERT ON "{migration_schema}".{table} FOR EACH STATEMENT
                EXECUTE FUNCTION "{migration_schema}".reject_custom_submission()
            ''')
    headers = {"Idempotency-Key": "retry-after-rollback"}
    response = client.post(PATH, json=payload(), headers=headers)
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "storage_error"
    assert "Location" not in response.headers
    assert "private-db-sentinel" not in response.text
    assert_counts(http_engine, 0)
    with http_engine.begin() as connection:
        connection.exec_driver_sql(
            f'DROP TRIGGER reject_custom_submission ON "{migration_schema}".{table}'
        )
    successful = client.post(PATH, json=payload(), headers=headers)
    assert successful.status_code == 201
    assert_counts(http_engine, 1)
    assert stored(http_engine)["workflow_versions"][0]["version_number"] == 1


def test_lost_http_success_can_replay_the_durable_receipt(
    client: TestClient, http_engine: Engine
) -> None:
    app = create_demo_app(Settings(environment="test"), engine=http_engine)

    async def lossy(scope: Scope, receive: Receive, send: Send) -> None:
        async def lose_created(message: Message) -> None:
            if message["type"] == "http.response.start" and message["status"] == 201:
                raise ConnectionResetError("Test transport lost the creation response")
            await send(message)

        await app(scope, receive, lose_created if scope["type"] == "http" else send)

    headers = {"Idempotency-Key": "lost-response"}
    with TestClient(lossy) as transport:
        with pytest.raises(ConnectionResetError, match="lost the creation response"):
            transport.post(PATH, json=payload(), headers=headers)
    assert_counts(http_engine, 1)
    before = stored(http_engine)
    replay = client.post(PATH, json=payload(), headers=headers)
    assert replay.status_code == 201
    assert replay.json()["run_id"] == str(
        before["demo_custom_submissions"][0]["run_id"]
    )
    assert stored(http_engine) == before


def test_custom_entry_does_not_change_core_publication_or_demo_membership_scope(
    client: TestClient, http_engine: Engine
) -> None:
    body = payload()
    body["name"] = "core_publication"
    publications = [client.post("/workflows", json=body) for _ in range(2)]
    assert all(response.status_code == 201 for response in publications)
    versions = [response.json() for response in publications]
    assert versions[0]["id"] != versions[1]["id"]
    assert [version["version_number"] for version in versions] == [1, 2]
    core = client.post(
        "/runs",
        json={"workflow_version_id": versions[0]["id"]},
        headers={"Idempotency-Key": "shared-across-operations"},
    )
    assert core.status_code == 201
    assert client.get("/demo/runs/" + core.json()["run_id"]).status_code == 404
    custom = client.post(
        PATH, json=payload(), headers={"Idempotency-Key": "shared-across-operations"}
    )
    assert (
        custom.status_code == 201 and custom.json()["run_id"] != core.json()["run_id"]
    )
    assert [run["run_id"] for run in client.get("/demo/runs").json()] == [
        custom.json()["run_id"]
    ]
    assert client.get("/workflow-versions/" + versions[0]["id"]).json() == versions[0]
    assert client.get("/workflow-versions/" + versions[1]["id"]).json() == versions[1]
    with http_engine.connect() as connection:
        assert (
            connection.scalar(select(func.count()).select_from(run_creation_requests))
            == 1
        )
        assert (
            connection.scalar(select(func.count()).select_from(demo_custom_submissions))
            == 1
        )
