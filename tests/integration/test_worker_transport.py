"""Worker wire contracts against the real API and migrated PostgreSQL storage."""

from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select

from tests.integration.test_lease_http import client as client
from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.domain.completion import (
    AttemptCompletion,
    CompletionOutcome,
    CompletionResult,
)
from workflow_engine.domain.worker import WorkerSession
from workflow_engine.schema import attempt_completions, task_attempts
from workflow_engine.worker.transport import (
    ClaimPoll,
    TransportUnavailable,
    WorkerTransport,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def run_id(client: TestClient) -> UUID:
    version = client.post(
        "/workflows",
        json={
            "name": "transport_" + uuid4().hex,
            "tasks": [{"task_id": "A", "task_type": "demo.echo"}],
        },
    )
    assert version.status_code == 201
    run = client.post(
        "/runs",
        json={"workflow_version_id": version.json()["id"]},
        headers={"Idempotency-Key": uuid4().hex},
    )
    assert run.status_code == 201
    return UUID(run.json()["run_id"])


@pytest.mark.parametrize("lost_operation", ["register", "claim", "renew", "complete"])
def test_retry_after_committed_response_loss(
    client: TestClient, http_engine: Engine, run_id: UUID, lost_operation: str
) -> None:
    calls: list[tuple[str, str, bytes]] = []
    fail_once = True

    def send(method: str, path: str, body: bytes) -> tuple[int, bytes]:
        nonlocal fail_once
        response = client.request(
            method, path, content=body, headers={"Content-Type": "application/json"}
        )
        calls.append((method, path, body))
        operation = (
            "register" if method == "PUT" else path.rsplit("/", 1)[1].rstrip("s")
        )
        if operation == lost_operation and fail_once:
            fail_once = False
            assert response.status_code == 200
            raise TransportUnavailable("Committed response lost.")
        return response.status_code, response.content

    transport = WorkerTransport(
        WorkerSession(id=uuid4(), worker_name="transport", max_concurrency=1), send
    )
    if lost_operation == "register":
        with pytest.raises(TransportUnavailable):
            transport.register()
    assert transport.register().session.id == transport.session.id
    transport.heartbeat()
    poll = ClaimPoll(run_id=run_id, request_id=uuid4())
    if lost_operation == "claim":
        with pytest.raises(TransportUnavailable):
            transport.claim(poll)
    claim = transport.claim(poll).claim
    assert claim is not None
    if lost_operation == "renew":
        with pytest.raises(TransportUnavailable):
            transport.renew(claim.lease)
    renewed = transport.renew(claim.lease)
    assert renewed.lease_token == claim.lease.lease_token
    report = AttemptCompletion(
        attempt_id=claim.attempt.id,
        worker_session_id=transport.session.id,
        lease_token=claim.lease.lease_token,
        result=CompletionResult(outcome=CompletionOutcome.SUCCEEDED),
    )
    if lost_operation == "complete":
        with pytest.raises(TransportUnavailable):
            transport.complete(report)
    receipt = transport.complete(report)
    assert transport.complete(report) == receipt
    assert transport.claim(ClaimPoll(run_id=run_id, request_id=uuid4())).claim is None
    matching = [
        call
        for call in calls
        if (
            (lost_operation == "register" and call[0] == "PUT")
            or call[1].endswith(
                "/" + ("claims" if lost_operation == "claim" else lost_operation)
            )
        )
    ]
    assert len(matching) >= 2 and matching[0] == matching[1]
    with http_engine.connect() as connection:
        for table in (task_attempts, attempt_completions):
            assert connection.scalar(select(func.count()).select_from(table)) == 1
