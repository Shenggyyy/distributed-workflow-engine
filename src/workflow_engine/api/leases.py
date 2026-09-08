"""Renew current ownership with a submitted token and server-owned lease policy."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Engine

from workflow_engine.api.dependencies import get_engine, get_settings
from workflow_engine.api.errors import APIError, ErrorResponse
from workflow_engine.api.workers import reject_idempotency_key
from workflow_engine.config import Settings
from workflow_engine.domain.lease import (
    AttemptLease,
    LeaseClockRegressionError,
    LeaseExpiredError,
    LeaseOwnershipError,
)
from workflow_engine.domain.timeout import AttemptTimeoutError
from workflow_engine.repositories.leases import (
    LeaseInactiveError,
    LeaseNotFoundError,
    LeaseRepository,
    StoredLeaseError,
)

router = APIRouter(
    tags=["leases"],
    dependencies=[Depends(reject_idempotency_key)],
    responses={
        400: {"model": ErrorResponse, "description": "Idempotency-Key is unsupported."},
        404: {"model": ErrorResponse, "description": "Attempt lease not found."},
        409: {
            "model": ErrorResponse,
            "description": "Ownership, state or expiry conflict.",
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


class RenewLeaseRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    lease_token: UUID = Field(repr=False)


class RenewLeaseResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    lease: AttemptLease = Field(repr=False)


@router.post(
    "/worker-sessions/{session_id}/attempts/{attempt_id}/renew",
    response_model=RenewLeaseResponse,
    responses={
        200: {
            "description": "Committed current lease; ownership identity is unchanged.",
            "headers": {
                "Cache-Control": {
                    "schema": {"type": "string"},
                    "description": "no-store",
                }
            },
        }
    },
    summary="Renew an Attempt lease",
    description=(
        "Requires a lease_token UUID in the JSON body. Session and Attempt IDs "
        "come from the path. The server checks current RUNNING ownership and "
        "the exclusive lease deadline after ordered locks. Success is returned "
        "only after commit. Repeated calls make new observations, not receipt "
        "replays. The server owns duration; renewal never rotates a token, "
        "shortens a deadline, revives an expired Attempt or extends Worker "
        "heartbeat. Idempotency-Key is unsupported. No handler is executed."
    ),
)
def renew_lease(
    session_id: UUID,
    attempt_id: UUID,
    body: RenewLeaseRequest,
    response: Response,
    engine: Database,
    settings: Policy,
) -> RenewLeaseResponse:
    try:
        with engine.begin() as connection:
            lease = LeaseRepository(
                connection, lease_seconds=settings.attempt_lease_seconds
            ).renew(
                attempt_id,
                worker_session_id=session_id,
                lease_token=body.lease_token,
            )
            try:
                result = RenewLeaseResponse(lease=lease)
            except ValidationError:
                raise StoredLeaseError(
                    "Stored renewal cannot be represented."
                ) from None
    except AttemptTimeoutError:
        raise APIError(409, "attempt_timed_out", "Attempt deadline elapsed.") from None
    except LeaseNotFoundError:
        raise APIError(404, "lease_not_found", "Attempt lease was not found.") from None
    except LeaseInactiveError:
        raise APIError(
            409, "lease_inactive", "Attempt lease execution is inactive."
        ) from None
    except LeaseOwnershipError:
        raise APIError(
            409, "lease_ownership_mismatch", "Attempt lease ownership does not match."
        ) from None
    except LeaseExpiredError:
        raise APIError(409, "lease_expired", "Attempt lease expired.") from None
    except LeaseClockRegressionError:
        raise APIError(
            503, "lease_clock_regression", "Attempt lease clock moved backwards."
        ) from None
    response.headers["Cache-Control"] = "no-store"
    return result
