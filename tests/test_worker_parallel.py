"""Bounded local reservations, independent reports and shared heartbeat loss."""

from datetime import UTC, datetime
from threading import Event, Lock, Timer
from uuid import UUID, uuid4

import pytest

from tests.test_worker_loop import FakeExecution, Gateway
from tests.test_worker_loop import children as children
from workflow_engine.domain.completion import AttemptCompletion, CompletionResult
from workflow_engine.domain.runtime import (
    AttemptEvent,
    TaskAttempt,
    TaskRun,
    TaskStatus,
)
from workflow_engine.worker.handlers import builtin_registry, echo
from workflow_engine.worker.loop import WorkerControlError, WorkerLoop
from workflow_engine.worker.transport import (
    ClaimedTask,
    ClaimObservation,
    ClaimPoll,
    CompletionObservation,
    TransportUnavailable,
)


class ParallelGateway(Gateway):
    def __init__(self) -> None:
        super().__init__(2)
        self.grants: dict[UUID, ClaimedTask] = {}
        self.lock = Lock()

    def claim(self, poll: ClaimPoll) -> ClaimObservation:
        with self.lock:
            self.polls.append(poll)
            if poll.request_id not in self.grants:
                task = TaskRun(
                    id=uuid4(),
                    run_id=self.run_id,
                    task_key="A",
                    status=TaskStatus.RUNNING,
                )
                attempt = TaskAttempt(id=uuid4(), task_id=task.id, attempt_number=1)
                self.grants[poll.request_id] = self.grant.model_copy(
                    update={
                        "task": task,
                        "attempt": attempt,
                        "lease": self.grant.lease.model_copy(
                            update={"attempt_id": attempt.id}
                        ),
                    }
                )
                if self.lose_claim:
                    raise TransportUnavailable("Lost response.")
            return ClaimObservation(
                worker_session_id=self.session.id,
                run_id=self.run_id,
                request_id=poll.request_id,
                claim=self.grants[poll.request_id],
            )

    def complete(self, report: AttemptCompletion) -> CompletionObservation:
        with self.lock:
            self.reports.append(report)
            grant = next(
                value
                for value in self.grants.values()
                if value.attempt.id == report.attempt_id
            )
            if (
                self.lose_completion
                and sum(value.attempt_id == report.attempt_id for value in self.reports)
                == 1
            ):
                raise TransportUnavailable("Lost completion.")
            return CompletionObservation(
                attempt=grant.attempt.transition(AttemptEvent.SUCCEED),
                worker_session_id=self.session.id,
                result=report.result,
                accepted_at=datetime.now(UTC),
            )


@pytest.mark.parametrize("limit", [1, 2, 3, 5])
def test_parallel_slots_never_exceed_completion_reservations(
    children: list[FakeExecution], monkeypatch: pytest.MonkeyPatch, limit: int
) -> None:
    gateway = ParallelGateway()
    gateway.lose_claim = gateway.lose_completion = True
    peak = 0

    def poll(self: FakeExecution) -> CompletionResult | None:
        nonlocal peak
        peak = max(peak, sum(not child.closed for child in children))
        return echo(self.context) if len(children) >= min(2, limit) else None

    monkeypatch.setattr(FakeExecution, "poll", poll)
    stop = Event()
    timer = Timer(5, stop.set)
    timer.start()
    try:
        assert (
            WorkerLoop(
                gateway,
                builtin_registry(),
                gateway.run_id,
                retry_seconds=0.005,
                tick_seconds=0.005,
            ).run(stop, max_tasks=limit)
            == limit
        )
        assert not stop.is_set()
    finally:
        timer.cancel()
        timer.join(timeout=1)
    assert peak == min(2, limit)
    assert len(children) == len(gateway.grants) == limit
    assert all(child.closed for child in children)
    for grant in gateway.grants.values():
        reports = [
            value for value in gateway.reports if value.attempt_id == grant.attempt.id
        ]
        assert len(reports) == 2 and reports[0] == reports[1]


def test_shared_heartbeat_failure_stops_all_slots(
    children: list[FakeExecution], monkeypatch: pytest.MonkeyPatch
) -> None:
    gateway = ParallelGateway()
    gateway.block_heartbeat = True
    gateway.heartbeat_seconds = 0.4
    monkeypatch.setattr(FakeExecution, "poll", lambda self: None)
    try:
        with pytest.raises(WorkerControlError, match="heartbeat"):
            WorkerLoop(
                gateway, builtin_registry(), gateway.run_id, tick_seconds=0.005
            ).run(Event())
    finally:
        gateway.release.set()
    assert len(children) == 2 and all(child.closed for child in children)
    assert gateway.beats == 2 and gateway.reports == []


def test_slow_child_cleanup_does_not_block_another_slot(
    children: list[FakeExecution], monkeypatch: pytest.MonkeyPatch
) -> None:
    gateway = ParallelGateway()

    def request_stop(self: FakeExecution) -> None:
        pass

    def poll_closed(self: FakeExecution) -> bool:
        self.closed = self is not children[0] or bool(gateway.reports)
        return self.closed

    monkeypatch.setattr(FakeExecution, "request_stop", request_stop)
    monkeypatch.setattr(FakeExecution, "poll_closed", poll_closed)
    stop = Event()
    timer = Timer(5, stop.set)
    timer.start()
    try:
        assert (
            WorkerLoop(
                gateway, builtin_registry(), gateway.run_id, tick_seconds=0.005
            ).run(stop, max_tasks=2)
            == 2
        )
        assert not stop.is_set()
    finally:
        timer.cancel()
        timer.join(timeout=1)
    assert gateway.reports[0].attempt_id == children[1].context.attempt_id


def test_uncertain_inflight_claim_keeps_its_capacity_reservation(
    children: list[FakeExecution], monkeypatch: pytest.MonkeyPatch
) -> None:
    gateway = ParallelGateway()
    original_claim = gateway.claim
    release = Event()

    def claim(poll: ClaimPoll) -> ClaimObservation:
        result = original_claim(poll)
        if poll.request_id == next(iter(gateway.grants)):
            assert release.wait(5)
        return result

    original_complete = gateway.complete

    def complete(report: AttemptCompletion) -> CompletionObservation:
        result = original_complete(report)
        if len(gateway.reports) == 2:
            assert len(gateway.grants) == 3
            release.set()
        return result

    monkeypatch.setattr(gateway, "claim", claim)
    monkeypatch.setattr(gateway, "complete", complete)
    stop = Event()
    timer = Timer(5, stop.set)
    timer.start()
    try:
        assert (
            WorkerLoop(
                gateway, builtin_registry(), gateway.run_id, tick_seconds=0.005
            ).run(stop, max_tasks=3)
            == 3
        )
        assert not stop.is_set()
    finally:
        release.set()
        timer.cancel()
        timer.join(timeout=1)
    assert len(children) == len(gateway.grants) == 3
