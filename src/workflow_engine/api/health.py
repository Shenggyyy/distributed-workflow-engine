"""API liveness contract, independent of external dependencies."""

from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    """A minimal response with no configuration or dependency details."""

    status: Literal["ok"] = "ok"


@router.get(
    "/health/live",
    response_model=HealthResponse,
    summary="Check API liveness",
    description=(
        "Reports that the API can serve requests. "
        "Does not check database connectivity, schedulers, workers, or readiness."
    ),
)
async def liveness() -> HealthResponse:
    """Return process liveness without querying external systems."""
    return HealthResponse()
