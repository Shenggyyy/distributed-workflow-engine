"""Committed completion responses, historical replay and sanitized failures."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select

from tests.integration.test_lease_http import client as client
from tests.integration.test_lease_http import http_engine as http_engine
from tests.integration.test_lease_http import lease as lease
from workflow_engine.domain.completion import AttemptCompletion, CompletionReceipt
from workflow_engine.domain.lease import AttemptLease
from workflow_engine.repositories.completions import CompletionRepository
from workflow_engine.schema import (
    attempt_completions,
    claim_requests,
    task_attempts,
    task_runs,
    worker_sessions,
    workflow_runs,
)

pytestmark = pytest.mark.integration


def path(lease: AttemptLease) -> str:
    return (
        f"/worker-sessions/{lease.worker_session_id}"
        f"/attempts/{lease.attempt_id}/complete"
    )


def payload(lease: AttemptLease, failed: bool = False) -> dict[str, object]:
    return {
        "lease_token": str(lease.lease_token),
        "result": {
            "outcome": "FAILED" if failed else "SUCCEEDED",
            "error_code": "handler_failed" if failed else None,
        },
    }


@pytest.mark.parametrize("failed", [False, True])
def test_commit_and_replay_without_token(
    client: TestClient,
    http_engine: Engine,
    lease: AttemptLease,
    failed: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    response = client.post(path(lease), json=payload(lease, failed))
    assert response.status_code == 200
    assert (
        response.headers["Cache-Control"] == "no-store"
        and "Location" not in response.headers
    )
    assert set(response.json()) == {
        "attempt",
        "worker_session_id",
        "result",
        "accepted_at",
    }
    assert (
        "lease_token" not in response.text
        and str(lease.lease_token) not in response.text
    )
    with http_engine.begin() as connection:
        assert connection.execute(select(task_attempts.c.status)).scalar_one() == (
            "FAILED" if failed else "SUCCEEDED"
        )
        assert connection.execute(select(task_runs.c.status)).scalar_one() == (
            "FAILED" if failed else "SUCCEEDED"
        )
        connection.execute(worker_sessions.update().values(status="STOPPED"))
        connection.execute(workflow_runs.update().values(status="FAILED"))
        binding = connection.execute(select(claim_requests)).one()
    replay = client.post(path(lease), json=payload(lease, failed))
    assert replay.status_code == 200 and replay.json() == response.json()
    assert str(lease.lease_token) not in capsys.readouterr().err
    claim = client.post(
        f"/worker-sessions/{lease.worker_session_id}/claims",
        json={"run_id": str(binding.run_id), "request_id": str(binding.request_id)},
    )
    assert claim.status_code == 409


@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize("wrong_session", [False, True])
def test_wrong_owner(
    client: TestClient, lease: AttemptLease, completed: bool, wrong_session: bool
) -> None:
    if completed:
        assert client.post(path(lease), json=payload(lease)).status_code == 200
    uri = (
        f"/worker-sessions/{uuid4()}/attempts/{lease.attempt_id}/complete"
        if wrong_session
        else path(lease)
    )
    body = (
        payload(lease)
        if wrong_session
        else {**payload(lease), "lease_token": str(uuid4())}
    )
    response = client.post(uri, json=body)
    assert (
        response.status_code == 409
        and response.json()["error"]["code"] == "lease_ownership_mismatch"
    )
    assert str(lease.lease_token) not in response.text


def test_conflicting_completion(client: TestClient, lease: AttemptLease) -> None:
    original = client.post(path(lease), json=payload(lease))
    response = client.post(path(lease), json=payload(lease, True))
    assert (
        response.status_code == 409
        and response.json()["error"]["code"] == "completion_conflict"
    )
    assert client.post(path(lease), json=payload(lease)).json() == original.json()


def test_missing_attempt(client: TestClient, lease: AttemptLease) -> None:
    response = client.post(
        f"/worker-sessions/{lease.worker_session_id}/attempts/{uuid4()}/complete",
        json=payload(lease),
    )
    assert (
        response.status_code == 404
        and response.json()["error"]["code"] == "completion_not_found"
    )


def test_terminal_without_receipt(
    client: TestClient, http_engine: Engine, lease: AttemptLease
) -> None:
    with http_engine.begin() as connection:
        connection.execute(task_attempts.update().values(status="LOST"))
    response = client.post(path(lease), json=payload(lease))
    assert (
        response.status_code == 409
        and response.json()["error"]["code"] == "completion_inactive"
    )


@pytest.mark.parametrize(
    "seconds,code,status",
    [(-1, "lease_clock_regression", 503), (600, "lease_expired", 409)],
)
def test_clock_errors(
    client: TestClient,
    lease: AttemptLease,
    monkeypatch: pytest.MonkeyPatch,
    seconds: int,
    code: str,
    status: int,
) -> None:
    monkeypatch.setattr(
        CompletionRepository,
        "_database_now",
        lambda self: lease.acquired_at + timedelta(seconds=seconds),
    )
    response = client.post(path(lease), json=payload(lease))
    assert response.status_code == status and response.json()["error"]["code"] == code


@pytest.mark.parametrize("deferred", [False, True])
def test_write_and_commit_failures_are_sanitized(
    client: TestClient,
    http_engine: Engine,
    migration_schema: str,
    lease: AttemptLease,
    deferred: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with http_engine.begin() as connection:
        connection.exec_driver_sql(
            f'CREATE FUNCTION "{migration_schema}".reject_completion_http() '
            "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION "
            "USING ERRCODE='23514', MESSAGE='private-sentinel'; END; $$"
        )
        kind = (
            "CONSTRAINT TRIGGER reject_completion_http AFTER"
            if deferred
            else "TRIGGER reject_completion_http BEFORE"
        )
        timing = "DEFERRABLE INITIALLY DEFERRED" if deferred else ""
        connection.exec_driver_sql(
            f'CREATE {kind} INSERT ON "{migration_schema}".attempt_completions '
            f"{timing} FOR EACH ROW EXECUTE FUNCTION "
            f'"{migration_schema}".reject_completion_http()'
        )
    response = client.post(path(lease), json=payload(lease))
    assert (
        response.status_code == 500
        and response.json()["error"]["code"] == "storage_error"
    )
    output = response.text + capsys.readouterr().err
    assert "private-sentinel" not in output and str(lease.lease_token) not in output
    with http_engine.begin() as connection:
        assert (
            connection.execute(select(task_attempts.c.status)).scalar_one() == "RUNNING"
        )
        assert connection.execute(select(task_runs.c.status)).scalar_one() == "RUNNING"
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
            ).scalar_one()
            == 0
        )


def test_invalid_internal_receipt_rolls_back(
    client: TestClient,
    http_engine: Engine,
    lease: AttemptLease,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = CompletionRepository.complete

    def invalid(
        self: CompletionRepository, report: AttemptCompletion
    ) -> CompletionReceipt:
        receipt = original(self, report)
        return receipt.model_copy(update={"accepted_at": None})

    monkeypatch.setattr(CompletionRepository, "complete", invalid)
    response = client.post(path(lease), json=payload(lease))
    assert (
        response.status_code == 500
        and response.json()["error"]["code"] == "storage_error"
    )
    with http_engine.begin() as connection:
        assert (
            connection.execute(select(task_attempts.c.status)).scalar_one() == "RUNNING"
        )
        assert (
            connection.execute(
                select(func.count()).select_from(attempt_completions)
            ).scalar_one()
            == 0
        )


def test_concurrent_reports_return_same_receipt(
    client: TestClient, lease: AttemptLease
) -> None:
    barrier = Barrier(3, timeout=10)

    def submit() -> dict[str, object]:
        barrier.wait()
        response = client.post(path(lease), json=payload(lease))
        assert response.status_code == 200
        return dict(response.json())

    with ThreadPoolExecutor(max_workers=3) as executor:
        pending = [executor.submit(submit) for _ in range(3)]
        results = [f.result(timeout=20) for f in pending]
    assert results[0] == results[1] == results[2]
