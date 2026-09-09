"""Cohort waiting fails closed and renews only the original session."""

from datetime import UTC, datetime, timedelta
from threading import Event
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from workflow_engine.demo.startup import DemoStartupError, wait_for_cohort
from workflow_engine.domain.worker import WorkerSession, WorkerStatus
from workflow_engine.worker.transport import WorkerAPIError, WorkerObservation


def transport() -> Mock:
    session = WorkerSession(id=uuid4(), worker_name="demo-a", max_concurrency=1)
    stamp = datetime(2026, 1, 1, tzinfo=UTC)
    value = Mock(session=session)
    value.heartbeat.return_value = WorkerObservation(
        session=session,
        created_at=stamp,
        last_heartbeat_at=stamp,
        heartbeat_expires_at=stamp + timedelta(seconds=6),
    )
    return value


def test_heartbeats_between_probes_and_before_handoff() -> None:
    worker = transport()
    stop = Mock(spec=Event)
    stop.is_set.return_value = False
    order: list[str] = []
    observation: WorkerObservation = worker.heartbeat.return_value

    def heartbeat() -> WorkerObservation:
        order.append("heartbeat")
        return observation

    worker.heartbeat.side_effect = heartbeat
    stop.wait.side_effect = lambda seconds: order.append("wait")
    results = iter((False, False, True))

    def ready(*args: object, **kwargs: object) -> bool:
        order.append("query")
        return next(results)

    with patch("workflow_engine.demo.startup.cohort_ready", side_effect=ready):
        wait_for_cohort(Mock(), worker, uuid4(), stop, cohort_size=2)
    assert order == [
        "heartbeat",
        "query",
        "wait",
        "heartbeat",
        "query",
        "wait",
        "heartbeat",
        "query",
        "heartbeat",
    ]
    worker.register.assert_not_called()
    worker.claim.assert_not_called()


def test_deadline_after_slow_query_never_hands_off() -> None:
    worker = transport()
    clock = [0.0]

    def slow(*args: object, **kwargs: object) -> bool:
        clock[0] = 60.0
        return True

    with (
        patch(
            "workflow_engine.demo.startup.time.monotonic", side_effect=lambda: clock[0]
        ),
        patch("workflow_engine.demo.startup.cohort_ready", side_effect=slow),
        pytest.raises(DemoStartupError, match="timed out"),
    ):
        wait_for_cohort(Mock(), worker, uuid4(), Event(), cohort_size=2)
    assert worker.heartbeat.call_count == 1


def test_peer_can_arrive_after_original_six_second_heartbeat_window() -> None:
    worker = transport()
    clock, expiry = [0.0], [6.0]
    stop = Mock(spec=Event)
    stop.is_set.return_value = False
    observation: WorkerObservation = worker.heartbeat.return_value

    def heartbeat() -> WorkerObservation:
        assert clock[0] < expiry[0], "Original session expired while waiting"
        expiry[0] = clock[0] + 6
        return observation

    def wait(seconds: float) -> None:
        clock[0] += seconds

    worker.heartbeat.side_effect = heartbeat
    stop.wait.side_effect = wait
    with (
        patch(
            "workflow_engine.demo.startup.time.monotonic", side_effect=lambda: clock[0]
        ),
        patch(
            "workflow_engine.demo.startup.cohort_ready",
            side_effect=lambda *a, **kw: clock[0] >= 7,
        ),
    ):
        wait_for_cohort(Mock(), worker, uuid4(), stop, cohort_size=2)
    assert clock[0] == 7 and expiry[0] > clock[0]
    worker.register.assert_not_called()


def test_missing_peer_exhausts_wait_budget_without_new_identity() -> None:
    worker = transport()
    clock = [0.0]
    stop = Mock(spec=Event)
    stop.is_set.return_value = False

    def wait(seconds: float) -> None:
        clock[0] += seconds

    stop.wait.side_effect = wait
    with (
        patch(
            "workflow_engine.demo.startup.time.monotonic", side_effect=lambda: clock[0]
        ),
        patch("workflow_engine.demo.startup.cohort_ready", return_value=False),
        pytest.raises(DemoStartupError, match="timed out"),
    ):
        wait_for_cohort(Mock(), worker, uuid4(), stop, cohort_size=2, timeout_seconds=1)
    assert clock[0] == 1
    assert worker.heartbeat.call_count == 2
    worker.register.assert_not_called()


@pytest.mark.parametrize("during_query", [False, True])
def test_stop_never_hands_off(during_query: bool) -> None:
    worker = transport()
    stop = Event()
    if not during_query:
        stop.set()

    def ready(*args: object, **kwargs: object) -> bool:
        stop.set()
        return True

    with (
        patch("workflow_engine.demo.startup.cohort_ready", side_effect=ready),
        pytest.raises(DemoStartupError, match="stopped"),
    ):
        wait_for_cohort(Mock(), worker, uuid4(), stop, cohort_size=2)
    assert worker.heartbeat.call_count == int(during_query)


@pytest.mark.parametrize("failure", ["heartbeat", "query", "handoff"])
def test_io_rejection_never_re_registers(failure: str) -> None:
    worker = transport()
    expired = WorkerAPIError(409, "worker_expired")
    if failure == "heartbeat":
        worker.heartbeat.side_effect = expired
    elif failure == "handoff":
        worker.heartbeat.side_effect = [worker.heartbeat.return_value, expired]
    with (
        patch(
            "workflow_engine.demo.startup.cohort_ready",
            side_effect=RuntimeError("private database error")
            if failure == "query"
            else None,
            return_value=True,
        ),
        pytest.raises(DemoStartupError, match="heartbeat or query failed") as error,
    ):
        wait_for_cohort(Mock(), worker, uuid4(), Event(), cohort_size=2)
    assert "private" not in str(error.value)
    worker.register.assert_not_called()


def test_terminal_heartbeat_is_not_readiness() -> None:
    worker = transport()
    worker.heartbeat.return_value = worker.heartbeat.return_value.model_copy(
        update={
            "session": worker.session.model_copy(update={"status": WorkerStatus.LOST})
        }
    )
    with (
        patch("workflow_engine.demo.startup.cohort_ready") as query,
        pytest.raises(DemoStartupError, match="not ACTIVE"),
    ):
        wait_for_cohort(Mock(), worker, uuid4(), Event(), cohort_size=2)
    query.assert_not_called()


@pytest.mark.parametrize("size", [0, 3, True, 1.5])
def test_invalid_cohort_size_fails_before_io(size: int) -> None:
    worker = transport()
    with pytest.raises(ValueError, match="size"):
        wait_for_cohort(Mock(), worker, uuid4(), Event(), cohort_size=size)
    worker.heartbeat.assert_not_called()


@pytest.mark.parametrize("timeout", [0, -1, 61, float("nan"), float("inf"), True])
def test_invalid_wait_budget_fails_before_io(timeout: float) -> None:
    worker = transport()
    with pytest.raises(ValueError, match="timeout"):
        wait_for_cohort(Mock(), worker, uuid4(), Event(), timeout_seconds=timeout)
    worker.heartbeat.assert_not_called()
