"""Logging contract tests using actual handlers and parsed JSON output."""

import json
import logging
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from io import StringIO
from uuid import uuid4

import pytest

from workflow_engine.config import Settings
from workflow_engine.logging import configure_logging


@pytest.fixture(autouse=True)
def restore_application_logger() -> Iterator[None]:
    logger = logging.getLogger("workflow_engine")
    handlers = tuple(logger.handlers)
    level, propagate = logger.level, logger.propagate
    try:
        yield
    finally:
        for handler in tuple(logger.handlers):
            if handler not in handlers:
                logger.removeHandler(handler)
                handler.close()
        for handler in handlers:
            if handler not in logger.handlers:
                logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = propagate


def test_json_line_preserves_unicode_newlines_and_supported_fields() -> None:
    output = StringIO()
    configure_logging(Settings(), component="worker", stream=output)
    task_id = uuid4()

    logging.getLogger("workflow_engine.execution").info(
        "Completed %s\nnext line",
        "任务",
        extra={
            "event": "task_completed",
            "request_id": "request-1",
            "run_id": "run-1",
            "task_id": task_id,
            "attempt_id": "attempt-1",
            "worker_session_id": "session-1",
            "duration_ms": 12.5,
            "password": "never-serialize-this",
            "payload": {"arbitrary": object()},
            "component": "cannot-overwrite-worker",
        },
    )

    lines = output.getvalue().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["message"] == "Completed 任务\nnext line"
    assert record["level"] == "INFO"
    assert record["logger"] == "workflow_engine.execution"
    assert record["component"] == "worker"
    assert record["environment"] == "development"
    assert record["event"] == "task_completed"
    assert record["request_id"] == "request-1"
    assert record["run_id"] == "run-1"
    assert record["task_id"] == str(task_id)
    assert record["attempt_id"] == "attempt-1"
    assert record["worker_session_id"] == "session-1"
    assert record["duration_ms"] == 12.5
    assert datetime.fromisoformat(record["timestamp"]).utcoffset() == timedelta(0)
    assert record["timestamp"].endswith("Z")
    assert "password" not in record
    assert "payload" not in record
    assert "never-serialize-this" not in output.getvalue()


@pytest.mark.parametrize("duration", [float("nan"), float("inf"), -1, True, object()])
def test_invalid_optional_values_do_not_break_json(duration: object) -> None:
    output = StringIO()
    configure_logging(Settings(), component="scheduler", stream=output)

    logging.getLogger("workflow_engine.scheduler").info(
        "Tick", extra={"duration_ms": duration, "task_id": object(), "event": object()}
    )

    record = json.loads(output.getvalue())
    assert record["event"] == "log"
    assert "duration_ms" not in record
    assert "task_id" not in record


def test_handler_enforces_level_even_when_child_logger_is_more_verbose() -> None:
    output = StringIO()
    configure_logging(Settings(log_level="WARNING"), component="api", stream=output)
    child = logging.getLogger("workflow_engine.verbose")
    previous_level = child.level
    try:
        child.setLevel(logging.DEBUG)
        child.debug("Not emitted")
        child.info("Not emitted")
        child.warning("Emitted")
    finally:
        child.setLevel(previous_level)

    records = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [record["level"] for record in records] == ["WARNING"]


def test_reconfiguration_replaces_output_and_level_without_duplicate_logs() -> None:
    first, second = StringIO(), StringIO()
    configure_logging(Settings(), component="api", stream=first)
    configure_logging(
        Settings(log_level="DEBUG", environment="test"), component="cli", stream=second
    )

    logging.getLogger("workflow_engine.cli").debug("One event")

    assert first.getvalue() == ""
    records = [json.loads(line) for line in second.getvalue().splitlines()]
    assert len(records) == 1
    assert records[0]["level"] == "DEBUG"
    assert records[0]["environment"] == "test"
    assert records[0]["component"] == "cli"


def test_root_logging_is_unchanged_and_does_not_duplicate_engine_records() -> None:
    root = logging.getLogger()
    original_handlers = tuple(root.handlers)
    original_level = root.level
    root_output, engine_output = StringIO(), StringIO()
    root_handler = logging.StreamHandler(root_output)
    root.addHandler(root_handler)
    try:
        configure_logging(Settings(), component="api", stream=engine_output)
        logging.getLogger("workflow_engine.api").warning("Engine event")

        assert tuple(root.handlers) == (*original_handlers, root_handler)
        assert root.level == original_level
        assert root_output.getvalue() == ""
        assert json.loads(engine_output.getvalue())["message"] == "Engine event"
    finally:
        root.removeHandler(root_handler)
        root_handler.close()


def test_exception_keeps_stack_locations_without_message_or_locals() -> None:
    output = StringIO()
    configure_logging(Settings(), component="worker", stream=output)
    sensitive_value = "example-private-value"
    try:
        raise ValueError(sensitive_value)
    except ValueError:
        logging.getLogger("workflow_engine.worker").exception(
            "Task execution failed", extra={"event": "task_failed"}
        )

    record = json.loads(output.getvalue())
    assert record["exception"]["type"] == "ValueError"
    assert record["exception"]["frames"][-1]["file"] == "test_logging.py"
    assert record["exception"]["frames"][-1]["function"] == (
        "test_exception_keeps_stack_locations_without_message_or_locals"
    )
    assert record["exception"]["frames"][-1]["line"] > 0
    assert sensitive_value not in output.getvalue()
    assert len(output.getvalue().splitlines()) == 1


def test_concurrent_records_keep_their_own_identifiers() -> None:
    output = StringIO()
    configure_logging(Settings(), component="worker", stream=output)
    logger = logging.getLogger("workflow_engine.concurrent")

    def emit(index: int) -> None:
        logger.info(
            "Task %s", index, extra={"task_id": str(index), "event": "task_started"}
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(emit, range(32)))

    records = [json.loads(line) for line in output.getvalue().splitlines()]
    assert len(records) == 32
    assert {record["task_id"] for record in records} == {str(i) for i in range(32)}
    assert all(record["message"] == f"Task {record['task_id']}" for record in records)
