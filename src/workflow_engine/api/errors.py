"""Public error envelopes without request values or database exception messages."""

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.exc import (
    InterfaceError,
    OperationalError,
    SQLAlchemyError,
    TimeoutError,
)

from workflow_engine.repositories.runs import StoredRuntimeError
from workflow_engine.repositories.workers import StoredWorkerError
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    StoredDefinitionError,
)


class ValidationIssue(BaseModel):
    location: list[str | int]
    type: str


class ErrorContent(BaseModel):
    code: str
    message: str
    details: list[ValidationIssue] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    error: ErrorContent


class APIError(Exception):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def error_response(
    status_code: int,
    code: str,
    message: str,
    details: list[ValidationIssue] | None = None,
) -> JSONResponse:
    body = ErrorResponse(
        error=ErrorContent(code=code, message=message, details=details or [])
    )
    return JSONResponse(status_code=status_code, content=body.model_dump(mode="json"))


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(APIError)
    async def api_error(request: Request, exc: APIError) -> JSONResponse:
        return error_response(exc.status_code, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # FastAPI's default errors include input values; DAG error context can
        # also contain user data. Expose only field locations and error codes.
        details = [
            ValidationIssue(location=list(item["loc"]), type=item["type"])
            for item in exc.errors()
        ]
        return error_response(
            422, "invalid_request", "Request validation failed.", details
        )

    @app.exception_handler(RepositoryTransactionError)
    @app.exception_handler(StoredDefinitionError)
    @app.exception_handler(StoredRuntimeError)
    @app.exception_handler(StoredWorkerError)
    @app.exception_handler(SQLAlchemyError)
    async def storage_failure(request: Request, exc: Exception) -> JSONResponse:
        logging.getLogger(__name__).error(
            "Workflow storage operation failed.",
            exc_info=(type(exc), exc, exc.__traceback__),
            extra={"event": "api_storage_failed"},
        )
        if isinstance(exc, (OperationalError, InterfaceError, TimeoutError)):
            return error_response(
                503,
                "database_unavailable",
                "Database operation could not be completed.",
            )
        return error_response(
            500, "storage_error", "Workflow storage operation failed."
        )
