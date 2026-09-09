"""Atomic custom demo submission; core publication and execution stay unchanged."""

from dataclasses import dataclass
from hashlib import blake2s
from typing import Any
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import Connection, Engine, RowMapping, select, text

from workflow_engine.demo.custom_definitions import (
    CustomDefinitionError,
    normalize_custom,
)
from workflow_engine.domain.idempotency import validate_idempotency_key
from workflow_engine.domain.workflow import WorkflowDefinition
from workflow_engine.repositories.runs import RunRepository, StoredRuntimeError
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import (
    demo_custom_submissions,
    demo_runs,
    workflow_runs,
    workflow_versions,
)

# Stable DWED namespace, distinct from Worker/claim two-int gates and the separate
# bigint migration lock space. Hash collisions only serialize independent keys.
_CUSTOM_SUBMISSION_LOCK_NAMESPACE = 0x44574544


def _submission_lock_key(key: str) -> int:
    return int.from_bytes(
        blake2s(key.encode("ascii"), digest_size=4).digest(), signed=True
    )


@dataclass(frozen=True, slots=True)
class CustomSubmission:
    run_id: UUID
    workflow_version_id: UUID


class CustomSubmissionConflictError(ValueError):
    """The exact key already identifies a different normalized definition."""

    def __init__(self) -> None:
        super().__init__(
            "Idempotency key is already bound to a different custom definition."
        )


def _read_receipt(connection: Connection, key: str) -> RowMapping | None:
    return (
        connection.execute(
            select(
                demo_custom_submissions.c.run_id,
                demo_custom_submissions.c.definition.label("request_definition"),
                demo_runs.c.scenario,
                workflow_runs.c.id.label("stored_run_id"),
                workflow_runs.c.workflow_version_id,
                workflow_versions.c.id.label("stored_version_id"),
                workflow_versions.c.definition.label("pinned_definition"),
            )
            .select_from(
                demo_custom_submissions.outerjoin(
                    demo_runs,
                    demo_runs.c.run_id == demo_custom_submissions.c.run_id,
                )
                .outerjoin(workflow_runs, workflow_runs.c.id == demo_runs.c.run_id)
                .outerjoin(
                    workflow_versions,
                    workflow_versions.c.id == workflow_runs.c.workflow_version_id,
                )
            )
            .where(demo_custom_submissions.c.idempotency_key == key)
        )
        .mappings()
        .one_or_none()
    )


def _replay(row: RowMapping, incoming: dict[str, Any]) -> CustomSubmission:
    run_id, version_id = row["run_id"], row["workflow_version_id"]
    if (
        not isinstance(run_id, UUID)
        or not isinstance(version_id, UUID)
        or row["stored_run_id"] != run_id
        or row["stored_version_id"] != version_id
        or row["scenario"] != "custom"
    ):
        raise StoredRuntimeError("Stored custom submission identity is invalid.")
    try:
        normalized = normalize_custom(
            WorkflowDefinition.model_validate(row["pinned_definition"])
        ).model_dump(mode="json")
        recorded = normalize_custom(
            WorkflowDefinition.model_validate(row["request_definition"])
        ).model_dump(mode="json")
    except (ValidationError, CustomDefinitionError, TypeError):
        raise StoredRuntimeError(
            "Stored custom submission definition is invalid."
        ) from None
    if (
        normalized != row["pinned_definition"]
        or recorded != row["request_definition"]
        or normalized != recorded
    ):
        raise StoredRuntimeError("Stored custom submission definition is inconsistent.")
    if normalized != incoming:
        raise CustomSubmissionConflictError()
    return CustomSubmission(run_id, version_id)


def submit_custom(
    engine: Engine, definition: WorkflowDefinition, key: str
) -> CustomSubmission:
    """One fresh transaction, with the request gate before publication/control locks.

    Validate before writes and read in a new statement after the gate. Never
    reserve a committed placeholder, retry writes, or return before COMMIT.
    """
    normalized = normalize_custom(definition)
    key = validate_idempotency_key(key)
    canonical = normalized.model_dump(mode="json")
    with engine.connect().execution_options(isolation_level="READ COMMITTED") as conn:
        with conn.begin():
            # Also enforce PostgreSQL/non-autocommit on replay, without row locks.
            workflows = WorkflowRepository(conn)
            conn.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, :key)"),
                {
                    "namespace": _CUSTOM_SUBMISSION_LOCK_NAMESPACE,
                    "key": _submission_lock_key(key),
                },
            )
            existing = _read_receipt(conn, key)
            if existing is not None:
                result = _replay(existing, canonical)
            else:
                version = workflows.publish(normalized)
                created = RunRepository(conn).create(version.id)
                conn.execute(
                    demo_runs.insert().values(run_id=created.run.id, scenario="custom")
                )
                conn.execute(
                    demo_custom_submissions.insert().values(
                        idempotency_key=key,
                        definition=canonical,
                        run_id=created.run.id,
                    )
                )
                result = CustomSubmission(created.run.id, version.id)
    return result
