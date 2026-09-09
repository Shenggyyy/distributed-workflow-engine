"""Bounded custom demo input; validation itself has no database dependency."""

import json
import math
from typing import Annotated, Any, NoReturn

from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from workflow_engine.api.errors import APIError, ErrorResponse
from workflow_engine.demo.custom_definitions import (
    CUSTOM_BODY_LIMIT,
    CUSTOM_HANDLERS,
    CUSTOM_MAX_TASKS,
    CUSTOM_POLICY,
    CustomDefinitionError,
    definition_layers,
    normalize_custom,
)
from workflow_engine.demo.scenarios import Scenario, diamond_tasks
from workflow_engine.domain.workflow import WorkflowDefinition

router = APIRouter(prefix="/demo/custom", tags=["custom local demonstration"])
DEFINITION_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/WorkflowDefinition"}
            }
        },
    }
}


def limits() -> dict[str, int]:
    return {
        "max_tasks": CUSTOM_MAX_TASKS,
        "max_body_bytes": CUSTOM_BODY_LIMIT,
        "max_attempts": CUSTOM_POLICY.max_attempts,
        "timeout_seconds": CUSTOM_POLICY.timeout_seconds,
        "worker_count": 2,
        "slots_per_worker": 1,
    }


def _invalid_json(*unused: object) -> NoReturn:
    raise ValueError("Invalid JSON.")


def _unique_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _invalid_json()
        result[key] = value
    return result


def _finite_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        _invalid_json()
    return result


def _check_depth(value: Any) -> None:
    # Valid Workflow JSON is shallow. Do not rely on platform recursion limits.
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if isinstance(item, (dict, list)):
            if depth >= 16:
                _invalid_json()
            children = item.values() if isinstance(item, dict) else item
            pending.extend((child, depth + 1) for child in children)


async def read_definition(request: Request) -> WorkflowDefinition:
    """Bound raw bytes before parsing, including chunked or misleading headers."""
    media_type = (
        request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    )
    if media_type != "application/json":
        raise APIError(415, "demo_json_required", "Use application/json for this demo.")
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > CUSTOM_BODY_LIMIT:
            raise APIError(
                413, "demo_body_too_large", "Custom demo body exceeds 16 KiB."
            )
        body.extend(chunk)
    try:
        raw = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_unique_fields,
            parse_constant=_invalid_json,
            parse_float=_finite_float,
        )
        _check_depth(raw)
    except (ValueError, RecursionError):
        raise RequestValidationError(
            [{"type": "json_invalid", "loc": ("body",)}]
        ) from None
    try:
        return normalize_custom(WorkflowDefinition.model_validate(raw))
    except ValidationError as error:
        raise RequestValidationError(
            [
                {"type": issue["type"], "loc": ("body", *issue["loc"])}
                for issue in error.errors(include_input=False, include_context=False)
            ]
        ) from None
    except CustomDefinitionError as error:
        raise RequestValidationError(
            [{"type": error.code, "loc": ("body", *error.location)}]
        ) from None


CustomDefinition = Annotated[WorkflowDefinition, Depends(read_definition)]


@router.get("/catalog")
def catalog() -> dict[str, Any]:
    """Copy current trusted templates without publishing a version or Run."""
    scenarios: tuple[Scenario, ...] = ("parallel", "distribution", "recovery")
    return {
        "handlers": [
            {"task_type": kind, "seconds": seconds}
            for kind, seconds in CUSTOM_HANDLERS.items()
        ],
        "limits": limits(),
        "templates": [
            {
                "scenario": scenario,
                "definition": normalize_custom(
                    WorkflowDefinition(
                        name=f"custom_{scenario}",
                        schema_version=2,
                        tasks=diamond_tasks(scenario),
                    )
                ).model_dump(mode="json"),
            }
            for scenario in scenarios
        ],
    }


@router.post(
    "/validate",
    openapi_extra=DEFINITION_BODY,
    responses={413: {"model": ErrorResponse}, 422: {"model": ErrorResponse}},
    description=(
        "Validate bounded trusted demo input without writes or task execution. "
        "Returns the exact canonical definition accepted by custom submission."
    ),
)
def preview(definition: CustomDefinition) -> dict[str, Any]:
    return {
        "definition": definition.model_dump(mode="json"),
        "layers": definition_layers(definition),
        "limits": limits(),
    }
