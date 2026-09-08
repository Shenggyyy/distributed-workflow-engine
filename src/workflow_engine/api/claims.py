"""Claim and replay committed task ownership for one Worker session and Run."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Engine

from workflow_engine.api.dependencies import get_engine, get_settings
from workflow_engine.api.errors import APIError, ErrorResponse
from workflow_engine.api.workers import reject_idempotency_key
from workflow_engine.config import Settings
from workflow_engine.domain.lease import AttemptLease, LeaseClockRegressionError
from workflow_engine.domain.runtime import TaskAttempt, TaskRun
from workflow_engine.domain.workflow import TaskDefinition
from workflow_engine.repositories.claim_requests import (
    ClaimReplayUnavailableError,
    ClaimRequestConflictError,
    ClaimRequestRepository,
)
from workflow_engine.repositories.claims import (
    AttemptNumberExhaustedError,
    ClaimRunInactiveError,
    ClaimRunNotFoundError,
)
from workflow_engine.repositories.runs import StoredRuntimeError
from workflow_engine.repositories.workers import (
    WorkerClockRegressionError,
    WorkerSessionExpiredError,
    WorkerSessionInactiveError,
    WorkerSessionNotFoundError,
)

router = APIRouter(
    tags=["claims"],
    dependencies=[Depends(reject_idempotency_key)],
    responses={
        400: {"model": ErrorResponse, "description": "Idempotency-Key is unsupported."},
        404: {"model": ErrorResponse, "description": "Run or Worker not found."},
        409: {
            "model": ErrorResponse,
            "description": "Conflict or unavailable execution.",
        },
        422: {"model": ErrorResponse, "description": "Invalid request."},
        500: {"model": ErrorResponse, "description": "Storage invariant/schema error."},
        503: {
            "model": ErrorResponse,
            "description": "Database unavailable or clock regression.",
        },
    },
)
Database = Annotated[Engine, Depends(get_engine)]
Policy = Annotated[Settings, Depends(get_settings)]


class ClaimTaskRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    run_id: UUID
    request_id: UUID


class TaskClaimResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, frozen=True, extra="forbid")

    workflow_version_id: UUID
    task: TaskRun
    attempt: TaskAttempt
    lease: AttemptLease = Field(repr=False)
    definition: TaskDefinition = Field(repr=False)


class ClaimTaskResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    worker_session_id: UUID
    run_id: UUID
    request_id: UUID
    claim: TaskClaimResponse | None


@router.post(
    "/worker-sessions/{session_id}/claims",
    response_model=ClaimTaskResponse,
    responses={
        200: {
            "description": "Committed ownership snapshot or retained no-work result.",
            "headers": {
                "Cache-Control": {
                    "schema": {"type": "string"},
                    "description": "no-store",
                }
            },
        }
    },
    summary="Claim a task or replay a claim request",
    description=(
        "Requires run_id and request_id UUIDs. Retain the request ID across uncertain "
        "retries; use a new ID for a new poll. The identity is scoped to this Worker "
        "session and bound to one Run. New claims, live replays and sticky no-work "
        "results return 200 after commit. No-work is claim: null. Replay may reflect "
        "an independent renewal but never renews or reallocates. Expired/terminal "
        "ownership returns 409. Lease duration is server-owned. Idempotency-Key is "
        "unsupported. This endpoint does not execute a handler."
    ),
)
def claim_task(
    session_id: UUID,
    body: ClaimTaskRequest,
    response: Response,
    engine: Database,
    settings: Policy,
) -> ClaimTaskResponse:
    try:
        with engine.begin() as connection:
            value = ClaimRequestRepository(
                connection, lease_seconds=settings.attempt_lease_seconds
            ).claim_next(body.run_id, session_id, request_id=body.request_id)
            # Validate the complete response before committing any new allocation.
            try:
                result = ClaimTaskResponse(
                    worker_session_id=session_id,
                    run_id=body.run_id,
                    request_id=body.request_id,
                    claim=None
                    if value is None
                    else TaskClaimResponse.model_validate(value),
                )
            except ValidationError:
                raise StoredRuntimeError(
                    "Stored claim cannot be represented."
                ) from None
    except ClaimRunNotFoundError:
        raise APIError(404, "run_not_found", "Run was not found.") from None
    except WorkerSessionNotFoundError:
        raise APIError(
            404, "worker_not_found", "Worker session was not found."
        ) from None
    except ClaimRunInactiveError:
        raise APIError(409, "run_inactive", "Run is inactive.") from None
    except WorkerSessionInactiveError:
        raise APIError(409, "worker_inactive", "Worker session is inactive.") from None
    except WorkerSessionExpiredError:
        raise APIError(
            409, "worker_expired", "Worker session heartbeat expired."
        ) from None
    except ClaimRequestConflictError:
        raise APIError(
            409, "claim_request_conflict", "Claim request targets a different Run."
        ) from None
    except ClaimReplayUnavailableError:
        raise APIError(
            409, "claim_replay_unavailable", "Bound claim is no longer available."
        ) from None
    except AttemptNumberExhaustedError:
        raise APIError(
            409, "attempt_number_exhausted", "Task attempt number limit was reached."
        ) from None
    except WorkerClockRegressionError:
        raise APIError(
            503, "worker_clock_regression", "Worker heartbeat clock moved backwards."
        ) from None
    except LeaseClockRegressionError:
        raise APIError(
            503, "lease_clock_regression", "Attempt lease clock moved backwards."
        ) from None
    response.headers["Cache-Control"] = "no-store"
    return result
