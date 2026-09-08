"""Demo runtime must refuse ordinary Runs before registering or claiming."""

from contextlib import nullcontext
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from workflow_engine.config import Settings
from workflow_engine.demo.__main__ import run_demo_worker


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
