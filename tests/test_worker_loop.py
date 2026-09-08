"""Single-slot admission, stable retries and supervision during stalled HTTP."""

import time
from datetime import UTC, datetime, timedelta
from threading import Event, Timer
from uuid import UUID, uuid4

import pytest

from workflow_engine.domain.completion import AttemptCompletion, CompletionResult
from workflow_engine.domain.lease import AttemptLease
from workflow_engine.domain.runtime import (
    AttemptEvent,
    TaskAttempt,
    TaskRun,
    TaskStatus,
)
from workflow_engine.domain.worker import WorkerSession
from workflow_engine.domain.workflow import TaskDefinition
from workflow_engine.worker.execution import ProcessExecution
from workflow_engine.worker.handlers import (
    HandlerContext,
    HandlerRegistry,
    builtin_registry,
    echo,
)
from workflow_engine.worker.loop import WorkerControlError, WorkerLoop
from workflow_engine.worker.transport import (
    ClaimedTask,
    ClaimObservation,
    ClaimPoll,
    CompletionObservation,
    DiscoveryPage,
    TransportUnavailable,
    WorkerAPIError,
    WorkerObservation,
    WorkerTransport,
)


class Gateway(WorkerTransport):
    def __init__(self, concurrency: int = 1) -> None:
        super().__init__(
            WorkerSession(id=uuid4(), worker_name="test", max_concurrency=concurrency),
            lambda *args: (500, b""),
        )
        self.run_id = uuid4()
        task = TaskRun(
            id=uuid4(), run_id=self.run_id, task_key="A", status=TaskStatus.RUNNING
        )
        attempt = TaskAttempt(id=uuid4(), task_id=task.id, attempt_number=1)
        now = datetime.now(UTC)
        lease = AttemptLease(
            attempt_id=attempt.id,
            worker_session_id=self.session.id,
            lease_token=uuid4(),
            acquired_at=now,
            last_renewed_at=now,
            lease_expires_at=now + timedelta(seconds=5),
        )
        self.grant = ClaimedTask(
            workflow_version_id=uuid4(),
            task=task,
            attempt=attempt,
            lease=lease,
            definition=TaskDefinition(task_id="A", task_type="demo.echo"),
        )
        self.polls: list[ClaimPoll] = []
        self.reports: list[AttemptCompletion] = []
        self.renewals = self.beats = 0
        self.lose_claim = self.lose_completion = False
        self.no_work = False
        self.inactive = False
        self.lease_seconds = self.heartbeat_seconds = 5.0
        self.block_renewal = self.block_heartbeat = False
        self.reject_renewal = False
        self.first_renewal_delay = 0.0
        self.release = Event()

    def register(self) -> WorkerObservation:
        now = datetime.now(UTC)
        return WorkerObservation(
            session=self.session,
            created_at=now,
            last_heartbeat_at=now,
            heartbeat_expires_at=now + timedelta(seconds=self.heartbeat_seconds),
        )

    def heartbeat(self) -> WorkerObservation:
        self.beats += 1
        if self.block_heartbeat and self.beats > 1:
            assert self.release.wait(5)
        return self.register()

    def claim(self, poll: ClaimPoll) -> ClaimObservation:
        self.polls.append(poll)
        if self.inactive:
            raise WorkerAPIError(409, "run_inactive")
        if self.lose_claim and len(self.polls) == 1:
            raise TransportUnavailable("Claim response lost.")
        return ClaimObservation(
            worker_session_id=self.session.id,
            run_id=poll.run_id,
            request_id=poll.request_id,
            claim=None if self.no_work and len(self.polls) == 1 else self.grant,
        )

    def renew(self, lease: AttemptLease) -> AttemptLease:
        self.renewals += 1
        if self.reject_renewal:
            raise WorkerAPIError(409, "lease_expired")
        if self.renewals == 1 and self.first_renewal_delay:
            time.sleep(self.first_renewal_delay)
        if self.block_renewal and self.renewals > 1:
            assert self.release.wait(5)
        now = datetime.now(UTC)
        return lease.model_copy(
            update={
                "last_renewed_at": now,
                "lease_expires_at": now + timedelta(seconds=self.lease_seconds),
            }
        )

    def complete(self, report: AttemptCompletion) -> CompletionObservation:
        self.reports.append(report)
        if self.lose_completion and len(self.reports) == 1:
            raise TransportUnavailable("Completion response lost.")
        return CompletionObservation(
            attempt=self.grant.attempt.transition(AttemptEvent.SUCCEED),
            worker_session_id=self.session.id,
            result=report.result,
            accepted_at=datetime.now(UTC),
        )


class FakeExecution(ProcessExecution):
    def __init__(
        self, registry: HandlerRegistry, task_type: str, context: HandlerContext
    ) -> None:
        self.context = context
        self.closed = False
        self.finish = True

    def poll(self) -> CompletionResult | None:
        return echo(self.context) if self.finish else None

    def close(self) -> None:
        self.closed = True

    def request_stop(self) -> None:
        self.closed = True

    def poll_closed(self) -> bool:
        return self.closed


@pytest.fixture
def children(monkeypatch: pytest.MonkeyPatch) -> list[FakeExecution]:
    values: list[FakeExecution] = []

    def start(
        registry: HandlerRegistry, task_type: str, context: HandlerContext
    ) -> FakeExecution:
        value = FakeExecution(registry, task_type, context)
        values.append(value)
        return value

    monkeypatch.setattr("workflow_engine.worker.loop.ProcessExecution", start)
    return values


def loop(gateway: Gateway) -> WorkerLoop:
    return WorkerLoop(
        gateway,
        builtin_registry(),
        gateway.run_id,
        poll_seconds=0.01,
        retry_seconds=0.01,
        tick_seconds=0.005,
    )


@pytest.mark.parametrize("lost", ["claim", "completion", "no_work"])
def test_stable_delivery_and_single_execution(
    children: list[FakeExecution], lost: str
) -> None:
    gateway = Gateway()
    gateway.lose_claim = lost == "claim"
    gateway.lose_completion = lost == "completion"
    gateway.no_work = lost == "no_work"
    assert loop(gateway).run(Event(), max_tasks=1) == 1
    assert len(children) == 1 and children[0].closed
    assert gateway.renewals == 1 and gateway.beats >= 1
    if lost == "claim":
        assert gateway.polls[0] == gateway.polls[1]
    elif lost == "completion":
        assert gateway.reports[0] == gateway.reports[1]
    else:
        assert gateway.polls[0].request_id != gateway.polls[1].request_id


@pytest.mark.parametrize("blocked", ["renewal", "heartbeat"])
def test_stalled_network_cannot_block_child_stop(
    children: list[FakeExecution], monkeypatch: pytest.MonkeyPatch, blocked: str
) -> None:
    monkeypatch.setattr(FakeExecution, "poll", lambda self: None)
    gateway = Gateway()
    gateway.block_renewal = blocked == "renewal"
    gateway.block_heartbeat = blocked == "heartbeat"
    gateway.lease_seconds = 0.3 if blocked == "renewal" else 5
    gateway.heartbeat_seconds = 0.4 if blocked == "heartbeat" else 5
    try:
        with pytest.raises(WorkerControlError, match="expired locally"):
            loop(gateway).run(Event())
        assert len(children) == 1 and children[0].closed
        assert gateway.reports == []
    finally:
        gateway.release.set()


@pytest.mark.parametrize("late", [False, True])
def test_no_execution_without_fresh_renewal(
    children: list[FakeExecution], late: bool
) -> None:
    gateway = Gateway()
    gateway.reject_renewal = not late
    if late:
        gateway.lease_seconds = 0.01
        gateway.first_renewal_delay = 0.03
    with pytest.raises(WorkerControlError if late else WorkerAPIError):
        loop(gateway).run(Event())
    assert children == [] and gateway.reports == []


def test_shutdown_cleans_child(
    children: list[FakeExecution], monkeypatch: pytest.MonkeyPatch
) -> None:
    stop = Event()
    monkeypatch.setattr(FakeExecution, "poll", lambda self: stop.set())
    gateway = Gateway()
    assert loop(gateway).run(stop) == 0
    assert len(children) == 1 and children[0].closed and gateway.reports == []


def test_inactive_run_exits_normally(children: list[FakeExecution]) -> None:
    gateway = Gateway()
    gateway.inactive = True
    assert loop(gateway).run(Event()) == 0
    assert children == []


def test_stopped_incarnation_cannot_restart() -> None:
    value = loop(Gateway())
    stop = Event()
    stop.set()
    assert value.run(stop) == 0
    with pytest.raises(RuntimeError, match="restarted"):
        value.run(Event())


def test_stop_during_unresponsive_registration() -> None:
    gateway = Gateway()
    release = Event()

    def block() -> WorkerObservation:
        assert release.wait(5)
        return Gateway.register(gateway)

    gateway.register = block  # type: ignore[method-assign]
    stop = Event()
    timer = Timer(0.05, stop.set)
    timer.start()
    try:
        assert loop(gateway).run(stop) == 0
    finally:
        release.set()
        timer.join(timeout=1)


def test_renews_while_handler_runs(
    children: list[FakeExecution], monkeypatch: pytest.MonkeyPatch
) -> None:
    gateway = Gateway()
    gateway.lease_seconds = 0.3
    monkeypatch.setattr(
        FakeExecution,
        "poll",
        lambda self: echo(self.context) if gateway.renewals >= 3 else None,
    )
    assert loop(gateway).run(Event(), max_tasks=1) == 1
    assert gateway.renewals == 3 and len(gateway.reports) == 1
    assert len(children) == 1 and children[0].closed


def test_execution_loss_never_fabricates_report(
    children: list[FakeExecution], monkeypatch: pytest.MonkeyPatch
) -> None:
    from workflow_engine.worker.execution import ExecutionLost

    def lost(self: FakeExecution) -> CompletionResult | None:
        raise ExecutionLost("Lost child.")

    monkeypatch.setattr(FakeExecution, "poll", lost)
    gateway = Gateway()
    with pytest.raises(ExecutionLost):
        loop(gateway).run(Event())
    assert children[0].closed and gateway.reports == []


@pytest.mark.parametrize("race", ["empty", "inactive", "lost"])
def test_discovery_retries_preserve_claim_and_rotate_after_definite_result(
    children: list[FakeExecution], monkeypatch: pytest.MonkeyPatch, race: str
) -> None:
    gateway = Gateway()
    gateway.no_work = race == "empty"
    gateway.lose_claim = race == "lost"
    cursors: list[UUID | None] = []

    def discover(after: UUID | None = None) -> DiscoveryPage:
        cursors.append(after)
        return DiscoveryPage(run_ids=(gateway.run_id,), next_after=gateway.run_id)

    original = gateway.claim

    def claim(poll: ClaimPoll) -> ClaimObservation:
        if race == "inactive" and not gateway.polls:
            gateway.polls.append(poll)
            raise WorkerAPIError(409, "run_inactive")
        return original(poll)

    monkeypatch.setattr(gateway, "discover", discover)
    monkeypatch.setattr(gateway, "claim", claim)
    worker = WorkerLoop(
        gateway,
        builtin_registry(),
        poll_seconds=0.01,
        retry_seconds=0.01,
        tick_seconds=0.005,
    )
    assert worker.run(Event(), max_tasks=1) == 1
    assert len(children) == 1
    if race == "lost":
        assert cursors == [None]
        assert gateway.polls[0] == gateway.polls[1]
    else:
        assert cursors == [None, gateway.run_id]
        assert gateway.polls[0].request_id != gateway.polls[1].request_id


def test_empty_discovery_wraps_until_new_work(
    children: list[FakeExecution], monkeypatch: pytest.MonkeyPatch
) -> None:
    gateway = Gateway()
    calls = 0

    def discover(after: UUID | None = None) -> DiscoveryPage:
        nonlocal calls
        calls += 1
        assert after is None
        return DiscoveryPage(
            run_ids=() if calls == 1 else (gateway.run_id,), next_after=None
        )

    monkeypatch.setattr(gateway, "discover", discover)
    assert (
        WorkerLoop(
            gateway, builtin_registry(), poll_seconds=0.01, tick_seconds=0.005
        ).run(Event(), max_tasks=1)
        == 1
    )
    assert calls == 2 and len(children) == 1
