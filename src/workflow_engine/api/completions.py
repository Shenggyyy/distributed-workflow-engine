"""Complete an Attempt or confirm its retained result without returning tokens."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Response
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Engine

from workflow_engine.api.dependencies import get_engine
from workflow_engine.api.errors import APIError, ErrorResponse
from workflow_engine.api.workers import reject_idempotency_key
from workflow_engine.domain.completion import (
    AttemptCompletion,
    CompletionConflictError,
    CompletionOutcome,
    CompletionReceipt,
    CompletionResult,
)
from workflow_engine.domain.lease import (
    LeaseClockRegressionError,
    LeaseExpiredError,
    LeaseOwnershipError,
)
from workflow_engine.domain.runtime import TaskAttempt
from workflow_engine.repositories.completions import (
    CompletionInactiveError,
    CompletionNotFoundError,
    CompletionRepository,
    StoredCompletionError,
)

router = APIRouter(
    tags=["completions"],
    dependencies=[Depends(reject_idempotency_key)],
    responses={
        code: {"model": ErrorResponse} for code in (400, 404, 409, 422, 500, 503)
    },
)
Database = Annotated[Engine, Depends(get_engine)]


class CompletionResultRequest(CompletionResult):
    # HTTP bodies arrive as decoded Python dictionaries. Keep the pure domain's
    # strict enum while allowing the JSON string spelling at this adapter boundary.
    outcome: CompletionOutcome = Field(strict=False)


class CompleteAttemptRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    lease_token: UUID = Field(repr=False)
    result: CompletionResultRequest = Field(repr=False)


class CompleteAttemptResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    attempt: TaskAttempt
    worker_session_id: UUID
    result: CompletionResult = Field(repr=False)
    accepted_at: AwareDatetime


@router.post(
    "/worker-sessions/{session_id}/attempts/{attempt_id}/complete",
    response_model=CompleteAttemptResponse,
    responses={
        200: {
            "headers": {
                "Cache-Control": {
                    "schema": {"type": "string"},
                    "description": "no-store",
                }
            }
        }
    },
    summary="Complete an Attempt or replay its accepted result",
    description=(
        "Supply the original lease_token and a SUCCEEDED/FAILED result. First "
        "completion requires current valid ownership. Identical historical reports "
        "replay without renewing ownership; conflicting results fail. Success is "
        "returned only after commit. No token is returned. Idempotency-Key is "
        "unsupported: the Attempt ID identifies one completion. No handler executes."
    ),
)
def complete_attempt(
    session_id: UUID,
    attempt_id: UUID,
    body: CompleteAttemptRequest,
    response: Response,
    engine: Database,
) -> CompleteAttemptResponse:
    submitted = AttemptCompletion(
        attempt_id=attempt_id,
        worker_session_id=session_id,
        lease_token=body.lease_token,
        result=CompletionResult(
            outcome=body.result.outcome, error_code=body.result.error_code
        ),
    )
    try:
        with engine.begin() as connection:
            try:
                receipt = CompletionReceipt.model_validate(
                    CompletionRepository(connection).complete(submitted)
                )
                result = CompleteAttemptResponse(
                    attempt=receipt.attempt,
                    worker_session_id=receipt.completion.worker_session_id,
                    result=receipt.completion.result,
                    accepted_at=receipt.accepted_at,
                )
            except ValidationError:
                raise StoredCompletionError(
                    "Stored completion cannot be represented."
                ) from None
    except CompletionNotFoundError:
        raise APIError(
            404, "completion_not_found", "Owned Attempt was not found."
        ) from None
    except CompletionInactiveError:
        raise APIError(
            409, "completion_inactive", "Attempt completion is inactive."
        ) from None
    except CompletionConflictError:
        raise APIError(
            409, "completion_conflict", "Attempt completion result conflicts."
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
