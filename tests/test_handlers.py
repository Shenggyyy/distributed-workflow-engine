"""Handler admission, failure isolation and cross-attempt business identity."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from workflow_engine.domain.completion import CompletionOutcome, CompletionResult
from workflow_engine.worker.handlers import (
    HandlerContext,
    HandlerRegistration,
    HandlerRegistry,
    builtin_registry,
    echo,
)


@pytest.fixture
def context() -> HandlerContext:
    return HandlerContext(
        run_id=uuid4(),
        workflow_version_id=uuid4(),
        task_id=uuid4(),
        task_key="A",
        attempt_id=uuid4(),
        attempt_number=1,
    )


def test_business_identity_is_per_task(context: HandlerContext) -> None:
    retry = context.model_copy(update={"attempt_id": uuid4(), "attempt_number": 2})
    other = context.model_copy(update={"task_id": uuid4(), "run_id": uuid4()})
    assert retry.idempotency_key == context.idempotency_key
    assert other.idempotency_key != context.idempotency_key
    assert "lease_token" not in context.model_dump()
    with pytest.raises(ValidationError):
        context.attempt_number = 2  # type: ignore[misc]


def test_explicit_registry_snapshot(context: HandlerContext) -> None:
    entries = [HandlerRegistration("demo.echo", echo)]
    registry = HandlerRegistry(entries)
    entries.clear()
    assert registry.task_types == ("demo.echo",)
    assert registry.execute("demo.echo", context).outcome is CompletionOutcome.SUCCEEDED
    assert (
        registry.execute("arbitrary.module", context).error_code == "unknown_task_type"
    )


def test_duplicate_registration_rejected() -> None:
    with pytest.raises(ValueError, match="Duplicate"):
        HandlerRegistry(
            [HandlerRegistration("a", echo), HandlerRegistration("a", echo)]
        )


@pytest.mark.parametrize("key", ["", "../module", "x:y", "x" * 129])
def test_invalid_registry_key(key: str) -> None:
    with pytest.raises(ValidationError):
        HandlerRegistry([HandlerRegistration(key, echo)])


def test_non_callable_rejected() -> None:
    with pytest.raises(TypeError, match="callable"):
        HandlerRegistry([HandlerRegistration("a", None)])  # type: ignore[arg-type]


def test_exception_text_is_private(context: HandlerContext) -> None:
    def raises(ctx: HandlerContext) -> CompletionResult:
        raise RuntimeError("private-secret")

    result = HandlerRegistry([HandlerRegistration("a", raises)]).execute("a", context)
    assert result.error_code == "handler_exception"
    assert "private-secret" not in result.model_dump_json()


@pytest.mark.parametrize("exit_type", [KeyboardInterrupt, SystemExit])
def test_process_interrupt_not_business_failure(
    context: HandlerContext, exit_type: type[BaseException]
) -> None:
    def raises(ctx: HandlerContext) -> CompletionResult:
        raise exit_type()

    with pytest.raises(exit_type):
        HandlerRegistry([HandlerRegistration("a", raises)]).execute("a", context)


def test_invalid_result_is_failure(context: HandlerContext) -> None:
    def broken(ctx: HandlerContext) -> CompletionResult:
        return CompletionResult(outcome=CompletionOutcome.SUCCEEDED).model_copy(
            update={"error_code": "invalid"}
        )

    assert (
        HandlerRegistry([HandlerRegistration("a", broken)])
        .execute("a", context)
        .error_code
        == "invalid_handler_result"
    )


def test_invalid_context_never_invokes_handler(context: HandlerContext) -> None:
    calls: list[HandlerContext] = []

    def record(ctx: HandlerContext) -> CompletionResult:
        calls.append(ctx)
        return echo(ctx)

    registry = HandlerRegistry([HandlerRegistration("a", record)])
    with pytest.raises(ValidationError):
        registry.execute("a", context.model_copy(update={"attempt_number": True}))
    assert calls == []


def test_execute_does_not_pretend_to_deduplicate(context: HandlerContext) -> None:
    calls: list[str] = []

    def record(ctx: HandlerContext) -> CompletionResult:
        calls.append(ctx.idempotency_key)
        return echo(ctx)

    registry = HandlerRegistry([HandlerRegistration("a", record)])
    registry.execute("a", context)
    registry.execute("a", context)
    assert calls == [context.idempotency_key, context.idempotency_key]


def test_builtin_failure(context: HandlerContext) -> None:
    assert builtin_registry().execute("demo.fail", context).error_code == "demo_failure"
