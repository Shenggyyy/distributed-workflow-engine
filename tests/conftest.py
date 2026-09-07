"""Keep configuration tests independent of the developer's environment."""

import logging
import os
from collections.abc import Iterator
from pathlib import Path

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--database-env-file",
        type=Path,
        default=None,
        help="Explicit configuration for PostgreSQL integration tests.",
    )


@pytest.fixture(autouse=True)
def isolate_application_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove only application settings; pytest restores them after each test."""
    for name in tuple(os.environ):
        if name.upper().startswith("DWE_"):
            monkeypatch.delenv(name)


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
