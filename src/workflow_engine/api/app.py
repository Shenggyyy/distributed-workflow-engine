"""Application factory with explicit settings and lifecycle management."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version

from fastapi import FastAPI

from workflow_engine.api.health import router as health_router
from workflow_engine.config import Settings
from workflow_engine.logging import configure_logging


def create_app(settings: Settings) -> FastAPI:
    """Build an independent app; settings and logging are not loaded on import."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings, component="api")
        logger = logging.getLogger(__name__)
        logger.info(
            "API application startup completed.",
            extra={"event": "api_startup_complete"},
        )
        try:
            yield
        finally:
            logger.info(
                "API application shutdown completed.",
                extra={"event": "api_shutdown_complete"},
            )

    app = FastAPI(
        title="Distributed Workflow Engine",
        version=version("distributed-workflow-engine"),
        description=(
            "Distributed workflow engine API. Currently exposes liveness only; "
            "workflow execution and dependency readiness are not implemented."
        ),
        lifespan=lifespan,
    )
    app.include_router(health_router)
    return app
