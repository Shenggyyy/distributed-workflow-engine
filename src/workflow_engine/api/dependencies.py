"""Request dependencies expose an engine, never a shared connection."""

from fastapi import Request
from sqlalchemy import Engine

from workflow_engine.api.errors import APIError


async def get_engine(request: Request) -> Engine:
    """Read app state without blocking the event loop or opening a transaction."""
    engine = getattr(request.app.state, "database_engine", None)
    if not isinstance(engine, Engine):
        raise APIError(
            503, "database_not_configured", "Database access is not configured."
        )
    return engine
