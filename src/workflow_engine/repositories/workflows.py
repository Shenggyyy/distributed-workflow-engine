"""Workflow publication and lookup in one caller-owned PostgreSQL transaction."""

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import Connection, RowMapping, select
from sqlalchemy.dialects.postgresql import insert

from workflow_engine.domain.workflow import WorkflowDefinition
from workflow_engine.schema import workflow_versions, workflows


class RepositoryTransactionError(RuntimeError):
    """The repository is outside its required transaction or isolation mode."""


class StoredDefinitionError(ValueError):
    """A stored snapshot does not satisfy the supported workflow contract."""


@dataclass(frozen=True, slots=True)
class StoredWorkflow:
    id: UUID
    name: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class StoredWorkflowVersion:
    id: UUID
    workflow_id: UUID
    version_number: int
    created_at: datetime
    definition: WorkflowDefinition = field(repr=False)


def _version(row: RowMapping | None) -> StoredWorkflowVersion | None:
    if row is None:
        return None
    try:
        definition = WorkflowDefinition.model_validate(row["definition"])
    except ValidationError:
        raise StoredDefinitionError("Stored workflow definition is invalid.") from None
    return StoredWorkflowVersion(
        id=row["id"],
        workflow_id=row["workflow_id"],
        version_number=row["version_number"],
        created_at=row["created_at"],
        definition=definition,
    )


class WorkflowRepository:
    """Use inside engine.begin(); never share this object across threads.

    Publishing returns provisional records. Only the caller's successful commit
    makes them durable. No method commits, retries, or opens a new transaction.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._transaction = connection.get_transaction()
        self._require_transaction()
        # SQLAlchemy get_isolation_level() excludes DBAPI autocommit. Inspect
        # the actual driver flag, including when set on create_engine itself.
        if (
            connection.dialect.name != "postgresql"
            or getattr(connection.connection.driver_connection, "autocommit", True)
            or connection.get_isolation_level() != "READ COMMITTED"
        ):
            raise RepositoryTransactionError(
                "WorkflowRepository requires PostgreSQL READ COMMITTED."
            )

    def _require_transaction(self) -> None:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "WorkflowRepository requires its original active transaction."
            )

    def publish(self, definition: WorkflowDefinition) -> StoredWorkflowVersion:
        """Append one version; identical definitions intentionally create versions."""
        self._require_transaction()
        # model_copy/model_construct can bypass validation even for frozen models.
        validated = WorkflowDefinition.model_validate(definition)
        self._connection.execute(
            insert(workflows)
            .values(id=uuid4(), name=validated.name)
            .on_conflict_do_nothing(index_elements=[workflows.c.name])
        )
        # Separate statements matter: READ COMMITTED gets a new snapshot after a
        # competing first publisher commits. A same-statement CTE can miss its row.
        workflow_id = self._connection.execute(
            select(workflows.c.id)
            .where(workflows.c.name == validated.name)
            .with_for_update()
        ).scalar_one()
        # The workflow row lock is held until the caller commits or rolls back.
        latest_number = self._connection.execute(
            select(workflow_versions.c.version_number)
            .where(workflow_versions.c.workflow_id == workflow_id)
            .order_by(workflow_versions.c.version_number.desc())
            .limit(1)
        ).scalar_one_or_none()
        number = 1 if latest_number is None else latest_number + 1
        row = (
            self._connection.execute(
                workflow_versions.insert()
                .values(
                    id=uuid4(),
                    workflow_id=workflow_id,
                    version_number=number,
                    definition=validated.model_dump(mode="json"),
                )
                .returning(workflow_versions)
            )
            .mappings()
            .one()
        )
        return StoredWorkflowVersion(
            id=row["id"],
            workflow_id=row["workflow_id"],
            version_number=row["version_number"],
            created_at=row["created_at"],
            definition=validated,
        )

    def get_workflow(self, name: str) -> StoredWorkflow | None:
        self._require_transaction()
        row = (
            self._connection.execute(select(workflows).where(workflows.c.name == name))
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return StoredWorkflow(
            id=row["id"], name=row["name"], created_at=row["created_at"]
        )

    def get_version(self, version_id: UUID) -> StoredWorkflowVersion | None:
        self._require_transaction()
        return _version(
            self._connection.execute(
                select(workflow_versions).where(workflow_versions.c.id == version_id)
            )
            .mappings()
            .one_or_none()
        )

    def get_numbered_version(
        self, workflow_id: UUID, version_number: int
    ) -> StoredWorkflowVersion | None:
        self._require_transaction()
        return _version(
            self._connection.execute(
                select(workflow_versions).where(
                    workflow_versions.c.workflow_id == workflow_id,
                    workflow_versions.c.version_number == version_number,
                )
            )
            .mappings()
            .one_or_none()
        )

    def get_latest_version(self, workflow_id: UUID) -> StoredWorkflowVersion | None:
        self._require_transaction()
        return _version(
            self._connection.execute(
                select(workflow_versions)
                .where(workflow_versions.c.workflow_id == workflow_id)
                .order_by(workflow_versions.c.version_number.desc())
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )
