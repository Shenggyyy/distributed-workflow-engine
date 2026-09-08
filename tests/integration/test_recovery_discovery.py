"""Expiry discovery boundaries and advisory reads independent of held row locks."""

from datetime import timedelta

import pytest
from sqlalchemy import Engine, select

from tests.integration.test_completions import completion_schema as completion_schema
from tests.integration.test_completions import transaction
from tests.integration.test_retry_completion import make_claim
from workflow_engine.repositories.recovery_discovery import RecoveryDiscoveryRepository
from workflow_engine.schema import worker_sessions, workflow_runs

pytestmark = pytest.mark.integration


def test_candidates_are_bounded_nonlocking_and_clock_based(
    engine: Engine, completion_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = make_claim(engine, completion_schema, timeout=1)
    second = make_claim(engine, completion_schema, timeout=300)
    deadline = first.lease.acquired_at + timedelta(seconds=1)
    monkeypatch.setattr(
        RecoveryDiscoveryRepository, "_database_now", lambda self: deadline
    )
    with transaction(engine, completion_schema) as owner:
        owner.execute(select(workflow_runs).with_for_update()).all()
        owner.execute(select(worker_sessions).with_for_update()).all()
        with transaction(engine, completion_schema) as connection:
            discovery = RecoveryDiscoveryRepository(connection)
            assert discovery.attempts(first.task.run_id) == (first.attempt.id,)
            assert discovery.attempts(second.task.run_id) == ()
            assert discovery.workers() == ()
    monkeypatch.setattr(
        RecoveryDiscoveryRepository,
        "_database_now",
        lambda self: deadline + timedelta(seconds=400),
    )
    with transaction(engine, completion_schema) as connection:
        discovery = RecoveryDiscoveryRepository(connection)
        assert len(discovery.workers(limit=1)) == 1
        assert set(discovery.workers()) == {
            first.lease.worker_session_id,
            second.lease.worker_session_id,
        }
        assert discovery.attempts(second.task.run_id) == (second.attempt.id,)
        for limit in (0, 101, True):
            with pytest.raises(ValueError):
                discovery.workers(limit=limit)
