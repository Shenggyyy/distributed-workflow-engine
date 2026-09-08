"""Trusted synchronous handler contract, independent of delivery and ownership."""

from collections.abc import Iterable
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from workflow_engine.domain.completion import CompletionOutcome, CompletionResult
from workflow_engine.domain.workflow import Identifier, TaskType


class HandlerContext(BaseModel):
    """Business identity only: a handler cannot renew or complete its own lease."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    run_id: UUID
    workflow_version_id: UUID
    task_id: UUID
    task_key: Identifier
    attempt_id: UUID
    attempt_number: int = Field(ge=1)

    @property
    def idempotency_key(self) -> str:
        """Stable across Attempts of this Task, distinct across Run invocations."""
        return str(self.task_id)


class Handler(Protocol):
    def __call__(self, context: HandlerContext, /) -> CompletionResult:
        """Return a finite outcome; raise an Exception for an unexpected failure."""
        ...


@dataclass(frozen=True, slots=True)
class HandlerRegistration:
    task_type: str
    handler: Handler


class HandlerRegistry:
    """Immutable registrations; never import workflow-supplied code."""

    def __init__(self, entries: Iterable[HandlerRegistration]) -> None:
        handlers: dict[str, Handler] = {}
        for entry in entries:
            key = TypeAdapter(TaskType).validate_python(entry.task_type)
            if key in handlers:
                raise ValueError("Duplicate handler registration.")
            if not callable(entry.handler):
                raise TypeError("A registered handler must be callable.")
            handlers[key] = entry.handler
        self._handlers = MappingProxyType(handlers)

    @property
    def task_types(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))

    def execute(self, task_type: str, context: HandlerContext) -> CompletionResult:
        """Execute once per call; the control loop owns admission and deduplication.

        Never include exception text in result codes. BaseException intentionally
        escapes: process exit/interruption is not a completed business failure.
        """
        key = TypeAdapter(TaskType).validate_python(task_type)
        current = HandlerContext.model_validate(context)
        handler = self._handlers.get(key)
        if handler is None:
            return _failure("unknown_task_type")
        try:
            result = handler(current)
        except Exception:
            return _failure("handler_exception")
        try:
            return CompletionResult.model_validate(result)
        except (ValueError, TypeError):
            return _failure("invalid_handler_result")


def _failure(code: str) -> CompletionResult:
    return CompletionResult(outcome=CompletionOutcome.FAILED, error_code=code)


def echo(context: HandlerContext) -> CompletionResult:
    """A successful no-op; schema v1 carries no task input or output payload."""
    return CompletionResult(outcome=CompletionOutcome.SUCCEEDED)


def fail(context: HandlerContext) -> CompletionResult:
    """A deterministic business failure for execution and recovery examples."""
    return _failure("demo_failure")


def builtin_registry() -> HandlerRegistry:
    return HandlerRegistry(
        [HandlerRegistration("demo.echo", echo), HandlerRegistration("demo.fail", fail)]
    )
