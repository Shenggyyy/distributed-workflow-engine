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


class WorkerLoop:
    """Execute one explicit Run with a single local slot and fresh process session.

    At most one heartbeat and one work request are in flight. Delayed requests
    may still commit after local shutdown; no response can restart a stopped loop.
    """

    def __init__(
        self,
        transport: WorkerTransport,
        registry: HandlerRegistry,
        run_id: UUID,
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
        completed = 0
        phase = "register"
        work: (
            _Call[
                WorkerObservation
                | ClaimObservation
                | AttemptLease
                | CompletionObservation
            ]
            | None
        ) = None
        heartbeat: _Call[WorkerObservation] | None = None
        work_started = heartbeat_started = 0.0
        next_work = next_heartbeat = 0.0
        heartbeat_deadline: float | None = None
        lease_deadline = renew_at = 0.0
        poll = ClaimPoll(run_id=self.run_id, request_id=uuid4())
        claim: ClaimedTask | None = None
        lease: AttemptLease | None = None
        execution: ProcessExecution | None = None
        report: AttemptCompletion | None = None
        try:
            while not stop.is_set():
                now = time.monotonic()
                # Process completed calls before deadlines: a confirmed renewal
                # may have arrived since the previous supervisor tick.
                if heartbeat is not None and heartbeat.done.is_set():
                    try:
                        observed = heartbeat.result()
                    except BaseException as error:
                        if not _retryable(error):
                            raise
                        next_heartbeat = now + self.retry_seconds
                    else:
                        if observed.session.status is not WorkerStatus.ACTIVE:
                            raise WorkerControlError(
                                "Worker session is no longer active."
                            )
                        duration = (
                            observed.heartbeat_expires_at - observed.last_heartbeat_at
                        ).total_seconds()
                        heartbeat_deadline = heartbeat_started + duration * 0.9
                        next_heartbeat = heartbeat_started + duration / 3
                        if phase == "heartbeat":
                            phase = "claim"
                    heartbeat = None

                if work is not None and work.done.is_set():
                    try:
                        value = work.result()
                    except BaseException as error:
                        if not _retryable(error):
                            if (
                                isinstance(error, WorkerAPIError)
                                and phase == "claim"
                                and error.code == "run_inactive"
                            ):
                                return completed
                            raise
                        next_work = now + self.retry_seconds
                    else:
                        if phase == "register":
                            assert isinstance(value, WorkerObservation)
                            if value.session.status is not WorkerStatus.ACTIVE:
                                raise WorkerControlError(
                                    "Worker registration is inactive."
                                )
                            # Registration replay is not a fresh heartbeat.
                            phase = "heartbeat"
                        elif phase == "claim":
                            assert isinstance(value, ClaimObservation)
                            if value.claim is None:
                                poll = ClaimPoll(run_id=self.run_id, request_id=uuid4())
                                next_work = now + self.poll_seconds
                            else:
                                claim, lease = value.claim, value.claim.lease
                                phase = "renew"
                        elif phase == "renew":
                            assert isinstance(value, AttemptLease)
                            lease = value
                            duration = (
                                lease.lease_expires_at - lease.last_renewed_at
                            ).total_seconds()
                            lease_deadline = work_started + duration * 0.9
                            renew_at = work_started + duration / 3
                            if now >= lease_deadline:
                                raise WorkerControlError(
                                    "Lease renewal response arrived too late."
                                )
                            if execution is None:
                                assert claim is not None
                                if stop.is_set():
                                    return completed
                                if (
                                    heartbeat_deadline is None
                                    or time.monotonic() >= heartbeat_deadline
                                ):
                                    raise WorkerControlError(
                                        "Heartbeat expired before execution admission."
                                    )
                                execution = ProcessExecution(
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
                            phase = "execute"
                        elif phase == "complete":
                            assert isinstance(value, CompletionObservation)
                            completed += 1
                            logger.info(
                                "Attempt completion confirmed.",
                                extra={
                                    "event": "completion_confirmed",
                                    "attempt_id": str(value.attempt.id),
                                },
                            )
                            if max_tasks is not None and completed >= max_tasks:
                                return completed
                            claim = lease = report = None
                            poll = ClaimPoll(run_id=self.run_id, request_id=uuid4())
                            phase = "claim"
                    work = None

                now = time.monotonic()
                if heartbeat_deadline is not None and now >= heartbeat_deadline:
                    raise WorkerControlError(
                        "Worker heartbeat confirmation expired locally."
                    )
                if execution is not None:
                    if now >= lease_deadline:
                        raise WorkerControlError(
                            "Attempt lease confirmation expired locally."
                        )
                    # Do not overlap completion with an in-flight renewal.
                    if work is None:
                        result = execution.poll()
                        if result is not None:
                            assert lease is not None
                            report = AttemptCompletion(
                                attempt_id=lease.attempt_id,
                                worker_session_id=lease.worker_session_id,
                                lease_token=lease.lease_token,
                                result=result,
                            )
                            execution.close()
                            execution = None
                            phase = "complete"
                            next_work = now
                        elif now >= renew_at:
                            phase = "renew"

                if phase != "register" and heartbeat is None and now >= next_heartbeat:
                    heartbeat_started = now
                    heartbeat = _Call(self.transport.heartbeat)

                if work is None and now >= next_work:
                    work_started = now
                    if phase == "register":
                        work = _Call(self.transport.register)
                    elif phase == "claim":
                        work = _Call(partial(self.transport.claim, poll))
                    elif phase == "renew":
                        assert lease is not None
                        work = _Call(partial(self.transport.renew, lease))
                    elif phase == "complete":
                        assert report is not None
                        work = _Call(partial(self.transport.complete, report))
                stop.wait(self.tick_seconds)
            return completed
        finally:
            if execution is not None:
                execution.close()
