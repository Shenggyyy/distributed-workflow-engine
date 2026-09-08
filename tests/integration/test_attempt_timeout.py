"""A renewable lease cannot extend fixed Attempt execution time."""

from datetime import timedelta
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, select

from tests.integration.test_completions import ClockedCompletions, report, transaction
from tests.integration.test_completions import completion_schema as completion_schema
from tests.integration.test_retry_completion import make_claim
from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.domain.timeout import AttemptTimeoutError
from workflow_engine.repositories.claim_requests import (
    ClaimReplayUnavailableError,
    ClaimRequestRepository,
)
from workflow_engine.repositories.completions import CompletionRepository
from workflow_engine.repositories.leases import LeaseRepository
from workflow_engine.schema import attempt_leases, claim_requests, task_attempts

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("operation", ["completion", "renewal", "replay"])
def test_fixed_timeout_boundary_and_no_mutation(
    engine: Engine,
    completion_schema: str,
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim = make_claim(engine, completion_schema, timeout=1)
    deadline = claim.lease.acquired_at + timedelta(seconds=1)
    before = deadline - timedelta(microseconds=1)
    monkeypatch.setattr(LeaseRepository, "_database_now", lambda self: before)
    with transaction(engine, completion_schema) as connection:
        renewed = LeaseRepository(connection).renew(
            claim.attempt.id,
            worker_session_id=claim.lease.worker_session_id,
            lease_token=claim.lease.lease_token,
        )
        assert renewed.lease_expires_at > deadline
    monkeypatch.setattr(LeaseRepository, "_database_now", lambda self: deadline)
    monkeypatch.setattr(ClaimRequestRepository, "_database_now", lambda self: deadline)
    with transaction(engine, completion_schema) as connection:
        with pytest.raises((AttemptTimeoutError, ClaimReplayUnavailableError)):
            if operation == "completion":
                ClockedCompletions(connection, deadline).complete(report(claim))
            elif operation == "renewal":
                LeaseRepository(connection).renew(
                    claim.attempt.id,
                    worker_session_id=claim.lease.worker_session_id,
                    lease_token=claim.lease.lease_token,
                )
            else:
                request_id = connection.scalar(select(claim_requests.c.request_id))
                assert isinstance(request_id, UUID)
                ClaimRequestRepository(connection).claim_next(
                    claim.task.run_id,
                    claim.lease.worker_session_id,
                    request_id=request_id,
                )
        assert connection.scalar(select(task_attempts.c.status)) == "RUNNING"
        assert connection.scalar(select(attempt_leases.c.last_renewed_at)) == before


def test_predeadline_completion_replays_after_timeout(
    engine: Engine, completion_schema: str
) -> None:
    claim = make_claim(engine, completion_schema, timeout=1)
    before = claim.lease.acquired_at + timedelta(seconds=1, microseconds=-1)
    with transaction(engine, completion_schema) as connection:
        receipt = ClockedCompletions(connection, before).complete(report(claim))
    with transaction(engine, completion_schema) as connection:
        assert (
            ClockedCompletions(connection, before + timedelta(days=2)).complete(
                report(claim)
            )
            == receipt
        )


@pytest.mark.parametrize("operation", ["complete", "renew"])
def test_timeout_http_conflict(
    engine: Engine,
    completion_schema: str,
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim = make_claim(engine, completion_schema, timeout=1)
    deadline = claim.lease.acquired_at + timedelta(seconds=1)
    monkeypatch.setattr(CompletionRepository, "_database_now", lambda self: deadline)
    monkeypatch.setattr(LeaseRepository, "_database_now", lambda self: deadline)
    mapped = engine.execution_options(schema_translate_map={None: completion_schema})
    with TestClient(create_app(Settings(environment="test"), engine=mapped)) as client:
        body: dict[str, object] = {"lease_token": str(claim.lease.lease_token)}
        if operation == "complete":
            body["result"] = {"outcome": "SUCCEEDED"}
        path = (
            f"/worker-sessions/{claim.lease.worker_session_id}"
            f"/attempts/{claim.attempt.id}/{operation}"
        )
        response = client.post(path, json=body)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "attempt_timed_out"
