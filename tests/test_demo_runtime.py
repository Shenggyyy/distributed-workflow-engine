"""Demo runtime must refuse ordinary Runs before registering or claiming."""

from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
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
        pytest.raises(DemoStartupError, match="Only registered demo"),
    ):
        run_demo_worker(Settings(), uuid4())
    transport.return_value.register.assert_not_called()
    loop.assert_not_called()


@pytest.mark.parametrize("cohort_size", [1, 2])
def test_cohort_waits_after_membership_commit_before_execution(
    cohort_size: int,
) -> None:
    engine = Mock()
    run_id = uuid4()
    actions: list[str] = []
    transaction_open = False

    @contextmanager
    def transaction() -> Iterator[Mock]:
        nonlocal transaction_open
        transaction_open = True
        yield Mock()
        transaction_open = False
        actions.append("commit membership")

    engine.begin.side_effect = transaction

    def attach(*args: object, **kwargs: object) -> None:
        with engine.begin():
            actions.append("attach")

    def register() -> None:
        assert transaction_open is False
        actions.append("register")

    def await_cohort(*args: object, **kwargs: object) -> None:
        assert transaction_open is False
        actions.append("cohort")

    with (
        patch(
            "workflow_engine.demo.__main__.database_engine",
            return_value=nullcontext(engine),
        ),
        patch("workflow_engine.demo.__main__.check_demo_startup") as check,
        patch(
            "workflow_engine.demo.__main__.add_demo_worker", side_effect=attach
        ) as add,
        patch("workflow_engine.demo.__main__.WorkerTransport") as transport,
        patch("workflow_engine.demo.__main__.wait_for_cohort") as cohort,
        patch("workflow_engine.demo.__main__.WorkerLoop") as loop,
    ):
        check.side_effect = lambda *args, **kwargs: actions.append("preflight")
        transport.return_value.register.side_effect = register
        cohort.side_effect = await_cohort
        loop.return_value.run.side_effect = lambda stop: actions.append("execute")
        run_demo_worker(Settings(), run_id, cohort_size=cohort_size)
    session = transport.call_args.args[0]
    check.assert_called_once_with(engine, run_id, session, cohort_size=cohort_size)
    add.assert_called_once_with(engine, run_id, session, cohort_size=cohort_size)
    if cohort_size == 2:
        cohort.assert_called_once()
        assert cohort.call_args.args[:3] == (engine, transport.return_value, run_id)
        assert cohort.call_args.kwargs == {"cohort_size": 2}
    else:
        cohort.assert_not_called()
    assert actions == ["preflight", "register", "attach", "commit membership"] + (
        ["cohort"] if cohort_size == 2 else []
    ) + ["execute"]


@pytest.mark.parametrize("failure", ["preflight", "register", "attach", "cohort"])
def test_failed_startup_never_enters_worker_loop(failure: str) -> None:
    engine = Mock()
    actions: list[str] = []

    def action(name: str) -> None:
        actions.append(name)
        if name == failure:
            raise DemoStartupError("stopped")

    with (
        patch(
            "workflow_engine.demo.__main__.database_engine",
            return_value=nullcontext(engine),
        ),
        patch("workflow_engine.demo.__main__.check_demo_startup") as check,
        patch("workflow_engine.demo.__main__.WorkerTransport") as transport,
        patch("workflow_engine.demo.__main__.add_demo_worker") as add,
        patch("workflow_engine.demo.__main__.wait_for_cohort") as cohort,
        patch("workflow_engine.demo.__main__.WorkerLoop") as loop,
        pytest.raises(DemoStartupError, match="stopped"),
    ):
        check.side_effect = lambda *args, **kwargs: action("preflight")
        transport.return_value.register.side_effect = lambda: action("register")
        add.side_effect = lambda *args, **kwargs: action("attach")
        cohort.side_effect = lambda *args, **kwargs: action("cohort")
        run_demo_worker(Settings(), uuid4(), cohort_size=2)
    loop.assert_not_called()
    expected = ["preflight", "register", "attach", "cohort"]
    assert actions == expected[: expected.index(failure) + 1]
