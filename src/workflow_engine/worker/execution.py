"""One cancellable trusted handler process; no database or network control here."""

import contextlib
import multiprocessing
import os
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

    def close(self) -> None:
        """Idempotent cleanup; termination cannot undo a completed external effect."""
        if self._closed:
            return
        if self._process.is_alive():
            self._process.terminate()
        self._process.join(timeout=2)
        if self._process.is_alive():
            self._process.kill()
            self._process.join(timeout=2)
        if self._process.is_alive():
            raise RuntimeError("Handler process could not be stopped.")
        self._receiver.close()
        self._process.close()
        self._closed = True

    def __enter__(self) -> "ProcessExecution":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
