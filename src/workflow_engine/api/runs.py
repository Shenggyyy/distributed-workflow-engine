"""Run creation and snapshot queries with explicit transaction boundaries."""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request, Response
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from sqlalchemy import Engine

from workflow_engine.api.dependencies import get_engine
from workflow_engine.api.errors import APIError, ErrorResponse
from workflow_engine.domain.dag import MAX_TASKS
from workflow_engine.domain.idempotency import validate_idempotency_key
from workflow_engine.domain.runtime import RunStatus, TaskStatus
from workflow_engine.domain.workflow import Identifier
from workflow_engine.repositories.discovery import RunDiscoveryRepository
from workflow_engine.repositories.runs import (
    IdempotencyConflictError,
    RunRepository,
    WorkflowVersionNotFoundError,
)

router = APIRouter(
    tags=["runs"],
    responses={
        404: {"model": ErrorResponse, "description": "Run or version was not found."},
        422: {"model": ErrorResponse, "description": "Invalid body, path or header."},
        500: {"model": ErrorResponse, "description": "Storage invariant/schema error."},
        503: {
            "model": ErrorResponse,
            "description": "Database unavailable/unconfigured.",
        },
    },
)
Database = Annotated[Engine, Depends(get_engine)]


async def get_idempotency_key(
    request: Request,
    key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=128,
            pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
            description="One case-sensitive key; reuse it with the same version.",
        ),
        BeforeValidator(validate_idempotency_key),
    ],
) -> str:
    # Reject ambiguity even when two header fields carry identical values.
    if len(request.headers.getlist("Idempotency-Key")) != 1:
        raise APIError(
            422, "invalid_request", "Exactly one Idempotency-Key is required."
        )
    return key


Key = Annotated[str, Depends(get_idempotency_key)]


class CreateRunRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    workflow_version_id: UUID


class RunCreationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, frozen=True, extra="forbid")
    run_id: UUID
    workflow_version_id: UUID


class RunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, frozen=True, extra="forbid")
    id: UUID
    workflow_version_id: UUID
    status: RunStatus
    created_at: datetime


class TaskRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, frozen=True, extra="forbid")
    id: UUID
    run_id: UUID
    task_key: Identifier
    status: TaskStatus
    created_at: datetime


class RunTasksResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, frozen=True, extra="forbid")
    run: RunResponse
    tasks: tuple[TaskRunResponse, ...] = Field(max_length=MAX_TASKS)


class ActiveRunsResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, frozen=True, extra="forbid")
    run_ids: tuple[UUID, ...] = Field(max_length=100)
    next_after: UUID | None


@router.get(
    "/runs",
    response_model=ActiveRunsResponse,
    summary="Discover active Runs using an advisory cursor",
)
def discover_runs(
    response: Response,
    engine: Database,
    after: UUID | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    ready_only: bool = False,
) -> ActiveRunsResponse:
    with engine.begin() as connection:
        result = ActiveRunsResponse.model_validate(
            RunDiscoveryRepository(connection).active(
                after=after,
                limit=limit,
                ready_only=ready_only,
            )
        )
    response.headers["Cache-Control"] = "no-store"
    return result


@router.post(
    "/runs",
    status_code=201,
    response_model=RunCreationResponse,
    responses={
        409: {
            "model": ErrorResponse,
            "description": "Key is bound to another version.",
        },
        201: {
            "description": "Committed creation receipt, also returned on replay.",
            "headers": {
                "Location": {
                    "description": "Relative URL for the referenced run.",
                    "schema": {"type": "string"},
                }
            },
        },
    },
    summary="Create or replay a workflow run",
    description=(
        "Requires one Idempotency-Key and a concrete workflow version UUID. "
        "First creation and same-key/version replay return the same receipt and 201. "
        "Success is sent only after commit. A different version with the same key "
        "returns 409. The receipt does not contain current execution status. "
        "No task execution or automatic database retry."
    ),
)
def create_run(
    body: CreateRunRequest, response: Response, engine: Database, key: Key
) -> RunCreationResponse:
    try:
        with engine.begin() as connection:
            receipt = RunRepository(connection).create_idempotent(
                body.workflow_version_id, idempotency_key=key
            )
            result = RunCreationResponse.model_validate(receipt)
    except WorkflowVersionNotFoundError:
        raise APIError(
            404, "version_not_found", "Workflow version was not found."
        ) from None
    except IdempotencyConflictError:
        raise APIError(
            409,
            "idempotency_conflict",
            "Idempotency key is already bound to another workflow version.",
        ) from None
    response.headers["Location"] = f"/runs/{result.run_id}"
    return result


@router.get("/runs/{run_id}", response_model=RunResponse, summary="Read run metadata")
def get_run(run_id: UUID, engine: Database) -> RunResponse:
    with engine.begin() as connection:
        stored = RunRepository(connection).get_run(run_id)
        if stored is None:
            raise APIError(404, "run_not_found", "Run was not found.")
        return RunResponse.model_validate(stored)


@router.get(
    "/runs/{run_id}/tasks",
    response_model=RunTasksResponse,
    summary="Read a consistent run and task snapshot",
    description=(
        "Returns run metadata and its tasks from one SQL statement snapshot. "
        "Tasks are sorted by key, not execution order. Reads persisted status; "
        "does not aggregate statuses, execute tasks or retrieve attempt history."
    ),
)
def get_run_tasks(run_id: UUID, engine: Database) -> RunTasksResponse:
    with engine.begin() as connection:
        stored = RunRepository(connection).get_run_with_tasks(run_id)
        if stored is None:
            raise APIError(404, "run_not_found", "Run was not found.")
        return RunTasksResponse.model_validate(stored)
