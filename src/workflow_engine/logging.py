"""JSON logging for engine processes, configured explicitly at startup."""

import json
import logging
import math
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO
from uuid import UUID

from workflow_engine.config import Settings

_HANDLER_NAME = "workflow_engine.json"
_ID_FIELDS = ("request_id", "run_id", "task_id", "attempt_id", "worker_session_id")


class JsonFormatter(logging.Formatter):
    """Serialize a log record with an explicit set of supported fields."""

    def __init__(self, *, component: str, environment: str) -> None:
        super().__init__()
        self.component = component
        self.environment = environment

    def format(self, record: logging.LogRecord) -> str:
        """Keep messages on one physical line and omit arbitrary extra values."""
        event = record.__dict__.get("event")
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "component": self.component,
            "environment": self.environment,
            "event": event if isinstance(event, str) and event else "log",
            "message": record.getMessage(),
        }
        for field in _ID_FIELDS:
            value = record.__dict__.get(field)
            if isinstance(value, (str, UUID)):
                payload[field] = str(value)

        duration = record.__dict__.get("duration_ms")
        if (
            isinstance(duration, (int, float))
            and not isinstance(duration, bool)
            and duration >= 0
            and (not isinstance(duration, float) or math.isfinite(duration))
        ):
            payload["duration_ms"] = duration

        if record.exc_info is not None and record.exc_info[0] is not None:
            payload["exception"] = {
                "type": record.exc_info[0].__qualname__,
                "frames": [
                    {
                        "file": Path(frame.f_code.co_filename).name,
                        "line": line,
                        "function": frame.f_code.co_name,
                    }
                    for frame, line in traceback.walk_tb(record.exc_info[2])
                ],
            }
        return json.dumps(payload, ensure_ascii=True, allow_nan=False)


def configure_logging(
    settings: Settings, *, component: str, stream: TextIO | None = None
) -> None:
    """Configure at startup; sequential calls replace the engine-owned handler."""
    logger = logging.getLogger("workflow_engine")
    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setLevel(settings.log_level)
    handler.setFormatter(
        JsonFormatter(component=component, environment=settings.environment)
    )

    for existing in tuple(logger.handlers):
        if existing.name == _HANDLER_NAME:
            logger.removeHandler(existing)
            existing.close()

    handler.set_name(_HANDLER_NAME)
    logger.setLevel(settings.log_level)
    logger.propagate = False
    logger.addHandler(handler)
