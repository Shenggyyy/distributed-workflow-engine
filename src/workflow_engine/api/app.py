"""Application factory with explicit settings and database pool ownership."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version

from fastapi import FastAPI
from sqlalchemy import Engine
from starlette.concurrency import run_in_threadpool

from workflow_engine.api.claims import router as claim_router
from workflow_engine.api.errors import install_error_handlers
from workflow_engine.api.health import router as health_router
from workflow_engine.api.leases import router as lease_router
from workflow_engine.api.runs import router as run_router
from workflow_engine.api.workers import router as worker_router
from workflow_engine.api.workflows import router as workflow_router
from workflow_engine.config import Settings
from workflow_engine.database import create_database_engine
from workflow_engine.logging import configure_logging


def create_app(settings: Settings, *, engine: Engine | None = None) -> FastAPI:
    """Create one app; an injected engine is borrowed and remains caller-owned."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings, component="api")
        logger = logging.getLogger(__name__)
        database = engine
        owns_engine = False
        if database is None and (
            settings.database_password is not None
            or settings.database_password_file is not None
        ):
            # Credentials are loaded here, but physical connections remain lazy.
            database = await run_in_threadpool(create_database_engine, settings)
            owns_engine = True
        app.state.database_engine = database
        logger.info(
            "API application startup completed.",
            extra={"event": "api_startup_complete"},
        )
        try:
            yield
        finally:
            app.state.database_engine = None
            if owns_engine and database is not None:
                await run_in_threadpool(database.dispose)
            logger.info(
                "API application shutdown completed.",
                extra={"event": "api_shutdown_complete"},
            )

    app = FastAPI(
        title="Distributed Workflow Engine",
        version=version("distributed-workflow-engine"),
        description=(
            "Publish workflows, create runs idempotently and query run/task snapshots. "
            "Register Worker sessions, accept heartbeats, claim tasks idempotently "
            "and renew Attempt leases. "
            "Liveness is independent of database access. Task scheduling and "
            "execution are not implemented."
        ),
        lifespan=lifespan,
    )
    app.state.settings = settings
    install_error_handlers(app)
    app.include_router(health_router)
    app.include_router(workflow_router)
    app.include_router(run_router)
    app.include_router(worker_router)
    app.include_router(claim_router)
    app.include_router(lease_router)
    return app
