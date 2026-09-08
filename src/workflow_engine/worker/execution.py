"""One cancellable trusted handler process; no database or network control here."""

import contextlib
import multiprocessing
import os
import time
from multiprocessing.connection import Connection

from workflow_engine.domain.completion import CompletionResult
from workflow_engine.worker.handlers import (
    HandlerContext,
    HandlerRegistration,
    HandlerRegistry,
)


class ExecutionLost(RuntimeError):
    """The child exited without a valid result; this is not a business failure."""


def _execute(
    entries: tuple[HandlerRegistration, ...],
    task_type: str,
    context: HandlerContext,
    output: Connection,
) -> None:
    # Child output is untrusted handler text. Control-plane logging happens in
    # the parent using IDs and fixed events, never handler stdout or traceback.
    try:
        with open(os.devnull, "w") as sink:
            with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
                try:
                    result = HandlerRegistry(entries).execute(task_type, context)
                    output.send_bytes(result.model_dump_json().encode())
                except BaseException:
                    # Parent detects exit/EOF. Never turn process loss into a
                    # fabricated FAILED receipt or emit private exception text.
                    return
    finally:
        output.close()


class ProcessExecution:
    """Spawn one child, poll non-blockingly, and always stop/join it when released.

    Registrations must be spawn-pickleable module-level callables. This is process
    isolation for trusted handlers, not a security sandbox or process-tree manager.
    """

    def __init__(
        self, registry: HandlerRegistry, task_type: str, context: HandlerContext
    ) -> None:
        current = HandlerContext.model_validate(context)
        spawn = multiprocessing.get_context("spawn")
        receiver, sender = spawn.Pipe(duplex=False)
        self._receiver = receiver
        self._process = spawn.Process(
            target=_execute,
            args=(registry.registrations, task_type, current, sender),
            daemon=True,
        )
        self._closed = False
        self._result: CompletionResult | None = None
        self._lost = False
        self._stop_started: float | None = None
        self._kill_sent_at: float | None = None
        try:
            self._process.start()
        except Exception:
            receiver.close()
            self._process.close()
            raise ExecutionLost("Handler process could not start.") from None
        finally:
            sender.close()

    @property
    def pid(self) -> int | None:
        return None if self._closed else self._process.pid

    def poll(self) -> CompletionResult | None:
        if self._closed:
            raise RuntimeError("Handler execution is closed.")
        if self._result is not None:
            return self._result
        if self._lost:
            raise ExecutionLost("Handler process exited without a valid result.")
        try:
            if self._receiver.poll():
                self._result = CompletionResult.model_validate_json(
                    self._receiver.recv_bytes(maxlength=4096)
                )
                return self._result
            if not self._process.is_alive():
                # Recheck the pipe after observing process exit: its last write
                # may have happened after the first poll.
                if self._receiver.poll():
                    self._result = CompletionResult.model_validate_json(
                        self._receiver.recv_bytes(maxlength=4096)
                    )
                    return self._result
                self._lost = True
        except (OSError, EOFError, ValueError):
            self._lost = True
        if self._lost:
            raise ExecutionLost(
                "Handler process exited without a valid result."
            ) from None
        return None

    def request_stop(self) -> None:
        """Start bounded cleanup without waiting for this child to exit."""
        if self._closed or self._stop_started is not None:
            return
        self._stop_started = time.monotonic()
        if self._process.is_alive():
            self._process.terminate()

    def poll_closed(self) -> bool:
        if self._closed:
            return True
        if self._stop_started is None:
            raise RuntimeError("Cleanup has not been requested.")
        if self._process.is_alive():
            now = time.monotonic()
            if now - self._stop_started >= 2 and self._kill_sent_at is None:
                self._process.kill()
                self._kill_sent_at = now
            elif self._kill_sent_at is not None and now - self._kill_sent_at >= 2:
                raise RuntimeError("Handler process could not be stopped.")
            return False
        self._process.join(timeout=0)
        self._receiver.close()
        self._process.close()
        self._closed = True
        return True

    def close(self) -> None:
        """Idempotent cleanup; termination cannot undo an external effect."""
        self.request_stop()
        while not self.poll_closed():
            time.sleep(0.005)

    def __enter__(self) -> "ProcessExecution":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
