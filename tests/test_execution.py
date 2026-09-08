"""Real spawned processes: results, abrupt exit and bounded cleanup."""

import multiprocessing
import time

import pytest

from tests.test_handlers import context as context
from workflow_engine.domain.completion import CompletionOutcome, CompletionResult
from workflow_engine.worker.execution import ExecutionLost, ProcessExecution
from workflow_engine.worker.handlers import (
    HandlerContext,
    HandlerRegistration,
    HandlerRegistry,
    builtin_registry,
)


def exits(context: HandlerContext) -> CompletionResult:
    raise SystemExit("private-child-exit")


def hangs(context: HandlerContext) -> CompletionResult:
    while True:
        time.sleep(60)


def wait_result(execution: ProcessExecution) -> CompletionResult:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result = execution.poll()
        if result is not None:
            return result
        time.sleep(0.01)
    raise AssertionError("Child failed to produce a result.")


@pytest.mark.parametrize("task_type", ["demo.echo", "demo.fail", "unknown"])
def test_spawned_outcomes(context: HandlerContext, task_type: str) -> None:
    with ProcessExecution(builtin_registry(), task_type, context) as execution:
        pid = execution.pid
        result = wait_result(execution)
        assert result.outcome is (
            CompletionOutcome.SUCCEEDED
            if task_type == "demo.echo"
            else CompletionOutcome.FAILED
        )
        assert execution.poll() == result
    assert execution.pid is None
    assert pid not in [child.pid for child in multiprocessing.active_children()]
    execution.close()


def test_child_exit_is_not_business_result(context: HandlerContext) -> None:
    with ProcessExecution(
        HandlerRegistry([HandlerRegistration("exit", exits)]), "exit", context
    ) as execution:
        with pytest.raises(ExecutionLost):
            wait_result(execution)
        with pytest.raises(ExecutionLost):
            execution.poll()


def test_hanging_child_can_be_stopped(context: HandlerContext) -> None:
    with ProcessExecution(
        HandlerRegistry([HandlerRegistration("hang", hangs)]), "hang", context
    ) as execution:
        pid = execution.pid
        assert execution.poll() is None
    assert pid not in [child.pid for child in multiprocessing.active_children()]
    with pytest.raises(RuntimeError, match="closed"):
        execution.poll()


def test_unserializable_handler_fails_safely(context: HandlerContext) -> None:
    registry = HandlerRegistry(
        [
            HandlerRegistration(
                "bad", lambda ctx: CompletionResult(outcome=CompletionOutcome.SUCCEEDED)
            )
        ]
    )
    with pytest.raises(ExecutionLost, match="could not start"):
        ProcessExecution(registry, "bad", context)
