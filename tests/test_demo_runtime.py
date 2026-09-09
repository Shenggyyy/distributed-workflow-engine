"""Demo runtime must refuse ordinary Runs before registering or claiming."""

from contextlib import nullcontext
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from workflow_engine.config import Settings
from workflow_engine.demo.__main__ import run_demo_worker
from workflow_engine.demo.startup import DemoStartupError


def test_unknown_run_never_registers_or_executes() -> None:
    engine = Mock()
    engine.connect.return_value = nullcontext(Mock(scalar=Mock(return_value=None)))
    with (
        patch(
            "workflow_engine.demo.__main__.database_engine",
            return_value=nullcontext(engine),
        ),
        patch("workflow_engine.demo.__main__.WorkerTransport") as transport,
        patch("workflow_engine.demo.__main__.WorkerLoop") as loop,
        pytest.raises(ValueError, match="Only registered demo"),
    ):
        run_demo_worker(Settings(), uuid4())
    transport.return_value.register.assert_not_called()
    loop.assert_not_called()


@pytest.mark.parametrize("cohort_size", [1, 2])
def test_cohort_waits_after_membership_commit_before_execution(
    cohort_size: int,
) -> None:
    from collections.abc import Iterator
    from contextlib import contextmanager

    engine = Mock()
    engine.connect.return_value = nullcontext(Mock(scalar=Mock(return_value=uuid4())))
    actions: list[str] = []

    @contextmanager
    def transaction() -> Iterator[Mock]:
        yield Mock()
        actions.append("commit membership")

    engine.begin.side_effect = transaction
    with (
        patch(
            "workflow_engine.demo.__main__.database_engine",
            return_value=nullcontext(engine),
        ),
        patch("workflow_engine.demo.__main__.WorkerTransport") as transport,
        patch("workflow_engine.demo.__main__.wait_for_cohort") as cohort,
        patch("workflow_engine.demo.__main__.WorkerLoop") as loop,
    ):
        transport.return_value.register.side_effect = lambda: actions.append("register")
        cohort.side_effect = lambda *args, **kwargs: actions.append("cohort")
        loop.return_value.run.side_effect = lambda stop: actions.append("execute")
        run_demo_worker(Settings(), uuid4(), cohort_size=cohort_size)
    assert actions == ["register", "commit membership"] + (
        ["cohort"] if cohort_size == 2 else []
    ) + ["execute"]


def test_failed_cohort_never_enters_worker_loop() -> None:
    engine = Mock()
    engine.connect.return_value = nullcontext(Mock(scalar=Mock(return_value=uuid4())))
    engine.begin.return_value = nullcontext(Mock())
    with (
        patch(
            "workflow_engine.demo.__main__.database_engine",
            return_value=nullcontext(engine),
        ),
        patch("workflow_engine.demo.__main__.WorkerTransport"),
        patch(
            "workflow_engine.demo.__main__.wait_for_cohort",
            side_effect=DemoStartupError("stopped"),
        ),
        patch("workflow_engine.demo.__main__.WorkerLoop") as loop,
        pytest.raises(DemoStartupError, match="stopped"),
    ):
        run_demo_worker(Settings(), uuid4(), cohort_size=2)
    loop.assert_not_called()
