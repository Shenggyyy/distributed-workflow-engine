"""Synchronous workflow HTTP handlers with commit-before-success semantics."""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Path, Response
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine

from workflow_engine.api.dependencies import get_engine
from workflow_engine.api.errors import APIError, ErrorResponse
from workflow_engine.domain.workflow import Identifier, WorkflowDefinition
from workflow_engine.repositories.workflows import (
    StoredWorkflowVersion,
    WorkflowRepository,
)

router = APIRouter(
    tags=["workflows"],
    responses={
        422: {"model": ErrorResponse, "description": "Invalid request or DAG."},
        500: {"model": ErrorResponse, "description": "Storage invariant/schema error."},
        503: {
            "model": ErrorResponse,
            "description": "Database unavailable/unconfigured.",
        },
    },
)
Database = Annotated[Engine, Depends(get_engine)]
VersionNumber = Annotated[int, Path(ge=1, le=2_147_483_647)]


class WorkflowResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, frozen=True, extra="forbid")

    id: UUID
    name: Identifier
    created_at: datetime


class WorkflowVersionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, frozen=True, extra="forbid")

    id: UUID
    workflow_id: UUID
    version_number: int
    created_at: datetime
    definition: WorkflowDefinition


def version_response(value: StoredWorkflowVersion | None) -> WorkflowVersionResponse:
    if value is None:
        raise APIError(404, "version_not_found", "Workflow version was not found.")
    return WorkflowVersionResponse.model_validate(value)


@router.post(
    "/workflows",
    status_code=201,
    response_model=WorkflowVersionResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Idempotency-Key is unsupported."},
        201: {
            "description": "A new immutable version was committed.",
            "headers": {
                "Location": {
                    "description": "Relative URL of the committed version.",
                    "schema": {"type": "string"},
                }
            },
        },
    },
    summary="Publish a workflow version",
    description=(
        "Creates the workflow by exact name if absent and appends one version. "
        "Every successful request creates a version, including identical definitions. "
        "Idempotency-Key is not supported. No automatic retry or task execution."
    ),
)
def publish_workflow(
    definition: WorkflowDefinition,
    response: Response,
    engine: Database,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> WorkflowVersionResponse:
    if idempotency_key is not None:
        raise APIError(
            400,
            "idempotency_not_supported",
            "Publication does not support Idempotency-Key.",
        )
    # Keep begin, repository calls and COMMIT inside this synchronous handler.
    # A yield dependency could otherwise delay commit until response teardown.
    with engine.begin() as connection:
        result = version_response(WorkflowRepository(connection).publish(definition))
    response.headers["Location"] = f"/workflow-versions/{result.id}"
    return result


@router.get(
    "/workflows/{name}",
    response_model=WorkflowResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Find a workflow by exact name",
)
def get_workflow(name: Identifier, engine: Database) -> WorkflowResponse:
    with engine.begin() as connection:
        value = WorkflowRepository(connection).get_workflow(name)
        if value is None:
            raise APIError(404, "workflow_not_found", "Workflow was not found.")
        return WorkflowResponse.model_validate(value)


# Register the literal "latest" route before the numeric version route.
@router.get(
    "/workflows/{workflow_id}/versions/latest",
    response_model=WorkflowVersionResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Read the latest visible version",
    description=(
        "Latest is resolved for this query's database snapshot. "
        "Use the returned version UUID when a stable reference is needed."
    ),
)
def get_latest_version(workflow_id: UUID, engine: Database) -> WorkflowVersionResponse:
    with engine.begin() as connection:
        return version_response(
            WorkflowRepository(connection).get_latest_version(workflow_id)
        )


@router.get(
    "/workflows/{workflow_id}/versions/{version_number}",
    response_model=WorkflowVersionResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Read a numbered workflow version",
)
def get_numbered_version(
    workflow_id: UUID, version_number: VersionNumber, engine: Database
) -> WorkflowVersionResponse:
    with engine.begin() as connection:
        return version_response(
            WorkflowRepository(connection).get_numbered_version(
                workflow_id, version_number
            )
        )


@router.get(
    "/workflow-versions/{version_id}",
    response_model=WorkflowVersionResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Read a workflow version by UUID",
)
def get_version(version_id: UUID, engine: Database) -> WorkflowVersionResponse:
    with engine.begin() as connection:
        return version_response(WorkflowRepository(connection).get_version(version_id))
