"""Worker session HTTP operations with server-owned heartbeat policy."""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Engine

from workflow_engine.api.dependencies import get_engine, get_settings
from workflow_engine.api.errors import APIError, ErrorResponse
from workflow_engine.config import Settings
from workflow_engine.domain.worker import MAX_WORKER_CONCURRENCY, WorkerSession
from workflow_engine.domain.workflow import Identifier
from workflow_engine.repositories.workers import (
    WorkerClockRegressionError,
    WorkerRegistrationConflictError,
    WorkerRepository,
    WorkerSessionExpiredError,
    WorkerSessionInactiveError,
    WorkerSessionNotFoundError,
)


async def reject_idempotency_key(request: Request) -> None:
    if "Idempotency-Key" in request.headers:
        raise APIError(
            400,
            "idempotency_not_supported",
            "Worker sessions do not support Idempotency-Key.",
        )


router = APIRouter(
    tags=["worker-sessions"],
    dependencies=[Depends(reject_idempotency_key)],
    responses={
        400: {"model": ErrorResponse, "description": "Idempotency-Key is unsupported."},
        422: {"model": ErrorResponse, "description": "Invalid request."},
        500: {"model": ErrorResponse, "description": "Storage invariant/schema error."},
        503: {
            "model": ErrorResponse,
            "description": "Database unavailable/unconfigured or clock regression.",
        },
    },
)
Database = Annotated[Engine, Depends(get_engine)]
Policy = Annotated[Settings, Depends(get_settings)]


class RegisterWorkerRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    worker_name: Identifier
    max_concurrency: int = Field(strict=True, ge=1, le=MAX_WORKER_CONCURRENCY)


class WorkerHeartbeatRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class WorkerSessionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, frozen=True, extra="forbid")

    session: WorkerSession
    created_at: datetime
    last_heartbeat_at: datetime
    heartbeat_expires_at: datetime


@router.put(
    "/worker-sessions/{session_id}",
    response_model=WorkerSessionResponse,
    responses={409: {"model": ErrorResponse}},
    summary="Register or replay a Worker session",
    description=(
        "Choose a new session UUID for each Worker process start. "
        "First registration and identical registration replays return 200. "
        "Replays return the current snapshot without renewing or reopening it. "
        "Changing the name or capacity for an existing UUID returns 409. "
        "Idempotency-Key is not supported."
    ),
)
def register_worker(
    session_id: UUID,
    body: RegisterWorkerRequest,
    engine: Database,
    settings: Policy,
) -> WorkerSessionResponse:
    try:
        with engine.begin() as connection:
            value = WorkerRepository(
                connection,
                heartbeat_timeout_seconds=settings.worker_heartbeat_timeout_seconds,
            ).register(
                session_id,
                worker_name=body.worker_name,
                max_concurrency=body.max_concurrency,
            )
            result = WorkerSessionResponse.model_validate(value)
    except WorkerRegistrationConflictError as exc:
        raise APIError(
            409,
            "worker_registration_conflict",
            "Worker session registration conflicts with its existing identity.",
        ) from exc
    return result


@router.post(
    "/worker-sessions/{session_id}/heartbeat",
    response_model=WorkerSessionResponse,
    responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
    summary="Observe an active Worker heartbeat",
    description=(
        "Requires an empty JSON object. "
        "Timestamps and timeout policy are server-owned. "
        "Only a still-active, unexpired session can renew its heartbeat. "
        "Retries make new observations; they do not replay an earlier response. "
        "Idempotency-Key is not supported."
    ),
)
def heartbeat_worker(
    session_id: UUID,
    body: WorkerHeartbeatRequest,
    engine: Database,
    settings: Policy,
) -> WorkerSessionResponse:
    try:
        with engine.begin() as connection:
            value = WorkerRepository(
                connection,
                heartbeat_timeout_seconds=settings.worker_heartbeat_timeout_seconds,
            ).heartbeat(session_id)
            result = WorkerSessionResponse.model_validate(value)
    except WorkerSessionNotFoundError as exc:
        raise APIError(
            404, "worker_not_found", "Worker session was not found."
        ) from exc
    except WorkerSessionInactiveError as exc:
        raise APIError(409, "worker_inactive", "Worker session is inactive.") from exc
    except WorkerSessionExpiredError as exc:
        raise APIError(
            409, "worker_expired", "Worker session heartbeat expired."
        ) from exc
    except WorkerClockRegressionError as exc:
        raise APIError(
            503, "worker_clock_regression", "Worker heartbeat clock moved backwards."
        ) from exc
    return result
