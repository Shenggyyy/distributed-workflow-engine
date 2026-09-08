"""Single-slot Worker: network calls never block execution ownership supervision."""

import logging
import math
import time
from collections.abc import Callable
from functools import partial
from threading import Event, Thread
from uuid import UUID, uuid4

from workflow_engine.domain.completion import AttemptCompletion
from workflow_engine.domain.lease import AttemptLease
from workflow_engine.domain.worker import WorkerStatus
from workflow_engine.worker.execution import ProcessExecution
from workflow_engine.worker.handlers import HandlerContext, HandlerRegistry
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

logger = logging.getLogger(__name__)


class WorkerControlError(RuntimeError):
    """Ownership cannot be safely maintained; stop rather than fabricate completion."""


class _Call[T]:
    """One in-flight request. A stalled request cannot block the supervisor thread."""

    def __init__(self, function: Callable[[], T]) -> None:
        self.done = Event()
        self.value: T | None = None
        self.error: BaseException | None = None

        def invoke() -> None:
            try:
                self.value = function()
            except BaseException as error:
                self.error = error
            finally:
                self.done.set()

        Thread(target=invoke, daemon=True, name="worker-http").start()

    def result(self) -> T:
        if not self.done.is_set():
            raise RuntimeError("HTTP operation is still pending.")
        if self.error is not None:
            raise self.error
        assert self.value is not None
        return self.value


def _retryable(error: BaseException) -> bool:
    return isinstance(error, TransportUnavailable) or (
        isinstance(error, WorkerAPIError) and error.retryable
    )


class _Slot:
    """One claim identity, child and report, independent of session heartbeat."""

    def __init__(
        self,
        transport: WorkerTransport,
        registry: HandlerRegistry,
        run_id: UUID | None,
        *,
        poll_seconds: float,
        retry_seconds: float,
    ) -> None:
        self.transport, self.registry, self.run_id = transport, registry, run_id
        self.poll_seconds, self.retry_seconds = poll_seconds, retry_seconds
        self.phase = "idle"
        self.finished = False
        self.work: (
            _Call[
                ClaimObservation | AttemptLease | CompletionObservation | DiscoveryPage
            ]
            | None
        ) = None
        self.work_started = self.next_work = 0.0
        self.lease_deadline = self.renew_at = 0.0
        self.cursor: UUID | None = None
        self.poll: ClaimPoll | None = None
        self.claim: ClaimedTask | None = None
        self.lease: AttemptLease | None = None
        self.execution: ProcessExecution | None = None
        self.report: AttemptCompletion | None = None

    @property
    def busy(self) -> bool:
        return self.phase != "idle" and not self.finished

    def start(self) -> None:
        if self.busy or self.finished:
            raise RuntimeError("Slot is not available.")
        self._select()

    def _select(self) -> None:
        self.phase = "discover" if self.run_id is None else "claim"
        self.poll = (
            ClaimPoll(run_id=self.run_id, request_id=uuid4())
            if self.run_id is not None
            else None
        )

    def step(self, stop: Event, heartbeat_deadline: float) -> bool:
        """Advance nonblocking control; true means one confirmed completion."""
        now = time.monotonic()
        if self.work is not None and self.work.done.is_set():
            try:
                value = self.work.result()
            except BaseException as error:
                if not _retryable(error):
                    if (
                        isinstance(error, WorkerAPIError)
                        and self.phase == "claim"
                        and error.code == "run_inactive"
                    ):
                        if self.run_id is not None:
                            self.finished = True
                            self.work = None
                            return False
                        self._select()
                    else:
                        raise
                self.next_work = now + self.retry_seconds
            else:
                if self.phase == "discover":
                    assert isinstance(value, DiscoveryPage)
                    self.cursor = value.next_after
                    if value.run_ids:
                        self.poll = ClaimPoll(
                            run_id=value.run_ids[0], request_id=uuid4()
                        )
                        self.phase = "claim"
                    else:
                        self.next_work = now + self.poll_seconds
                elif self.phase == "claim":
                    assert isinstance(value, ClaimObservation)
                    if value.claim is None:
                        self._select()
                        self.next_work = now + self.poll_seconds
                    else:
                        self.claim, self.lease = value.claim, value.claim.lease
                        self.phase = "renew"
                elif self.phase == "renew":
                    assert isinstance(value, AttemptLease)
                    self.lease = value
                    duration = (
                        value.lease_expires_at - value.last_renewed_at
                    ).total_seconds()
                    self.lease_deadline = self.work_started + duration * 0.9
                    self.renew_at = self.work_started + duration / 3
                    if now >= self.lease_deadline:
                        raise WorkerControlError(
                            "Lease renewal response arrived too late."
                        )
                    if self.execution is None:
                        assert self.claim is not None
                        if stop.is_set():
                            return False
                        if time.monotonic() >= heartbeat_deadline:
                            raise WorkerControlError(
                                "Heartbeat expired before execution admission."
                            )
                        claim = self.claim
                        self.execution = ProcessExecution(
                            self.registry,
                            claim.definition.task_type,
                            HandlerContext(
                                run_id=claim.task.run_id,
                                workflow_version_id=claim.workflow_version_id,
                                task_id=claim.task.id,
                                task_key=claim.task.task_key,
                                attempt_id=claim.attempt.id,
                                attempt_number=claim.attempt.attempt_number,
                            ),
                        )
                        logger.info(
                            "Handler started.",
                            extra={
                                "event": "handler_started",
                                "attempt_id": str(claim.attempt.id),
                            },
                        )
                    self.phase = "execute"
                elif self.phase == "complete":
                    assert isinstance(value, CompletionObservation)
                    logger.info(
                        "Attempt completion confirmed.",
                        extra={
                            "event": "completion_confirmed",
                            "attempt_id": str(value.attempt.id),
                        },
                    )
                    self.claim = self.lease = self.report = None
                    self.work = None
                    self.phase = "idle"
                    return True
            self.work = None

        now = time.monotonic()
        if self.execution is not None:
            if now >= self.lease_deadline:
                raise WorkerControlError("Attempt lease confirmation expired locally.")
            if self.work is None:
                result = self.execution.poll()
                if result is not None:
                    assert self.lease is not None
                    self.report = AttemptCompletion(
                        attempt_id=self.lease.attempt_id,
                        worker_session_id=self.lease.worker_session_id,
                        lease_token=self.lease.lease_token,
                        result=result,
                    )
                    self.execution.close()
                    self.execution = None
                    self.phase = "complete"
                    self.next_work = now
                elif now >= self.renew_at:
                    self.phase = "renew"
        if stop.is_set():
            return False
        if self.work is None and now >= self.next_work:
            self.work_started = now
            if self.phase == "discover":
                self.work = _Call(partial(self.transport.discover, self.cursor))
            elif self.phase == "claim":
                assert self.poll is not None
                self.work = _Call(partial(self.transport.claim, self.poll))
            elif self.phase == "renew":
                assert self.lease is not None
                self.work = _Call(partial(self.transport.renew, self.lease))
            elif self.phase == "complete":
                assert self.report is not None
                self.work = _Call(partial(self.transport.complete, self.report))
        return False

    def close(self) -> None:
        if self.execution is not None:
            self.execution.close()
            self.execution = None


class WorkerLoop:
    """One session heartbeat supervising a single execution slot.

    Late HTTP requests may commit after shutdown, but cannot restart execution.
    """

    def __init__(
        self,
        transport: WorkerTransport,
        registry: HandlerRegistry,
        run_id: UUID | None = None,
        *,
        poll_seconds: float = 0.5,
        retry_seconds: float = 0.5,
        tick_seconds: float = 0.05,
    ) -> None:
        if transport.session.max_concurrency != 1:
            raise ValueError("Single-slot Worker requires max_concurrency=1.")
        for interval in (poll_seconds, retry_seconds, tick_seconds):
            if (
                isinstance(interval, bool)
                or not math.isfinite(interval)
                or interval <= 0
            ):
                raise ValueError("Worker intervals must be positive and finite.")
        self.transport, self.registry, self.run_id = transport, registry, run_id
        self.poll_seconds, self.retry_seconds, self.tick_seconds = (
            poll_seconds,
            retry_seconds,
            tick_seconds,
        )
        self._started = False

    def run(self, stop: Event, *, max_tasks: int | None = None) -> int:
        if self._started:
            raise RuntimeError("A Worker incarnation cannot be restarted.")
        if max_tasks is not None and (type(max_tasks) is not int or max_tasks < 1):
            raise ValueError("max_tasks must be a positive integer.")
        self._started = True
        slot = _Slot(
            self.transport,
            self.registry,
            self.run_id,
            poll_seconds=self.poll_seconds,
            retry_seconds=self.retry_seconds,
        )
        completed = 0
        phase = "register"
        control: _Call[WorkerObservation] | None = None
        started = next_control = 0.0
        heartbeat_deadline: float | None = None
        try:
            while not stop.is_set():
                now = time.monotonic()
                if control is not None and control.done.is_set():
                    try:
                        observed = control.result()
                    except BaseException as error:
                        if not _retryable(error):
                            raise
                        next_control = now + self.retry_seconds
                    else:
                        if observed.session.status is not WorkerStatus.ACTIVE:
                            raise WorkerControlError(
                                "Worker session is no longer active."
                            )
                        if phase == "register":
                            # Registration replay is not a fresh heartbeat.
                            phase = "heartbeat"
                        else:
                            duration = (
                                observed.heartbeat_expires_at
                                - observed.last_heartbeat_at
                            ).total_seconds()
                            heartbeat_deadline = started + duration * 0.9
                            next_control = started + duration / 3
                    control = None
                now = time.monotonic()
                if heartbeat_deadline is not None and now >= heartbeat_deadline:
                    raise WorkerControlError(
                        "Worker heartbeat confirmation expired locally."
                    )
                if control is None and now >= next_control:
                    started = now
                    control = _Call(
                        self.transport.register
                        if phase == "register"
                        else self.transport.heartbeat
                    )
                if heartbeat_deadline is not None:
                    if not slot.busy:
                        slot.start()
                    if slot.step(stop, heartbeat_deadline):
                        completed += 1
                        if max_tasks is not None and completed >= max_tasks:
                            return completed
                    if slot.finished:
                        return completed
                stop.wait(self.tick_seconds)
            return completed
        finally:
            slot.close()
