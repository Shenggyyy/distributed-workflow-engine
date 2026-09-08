"""Bounded, advisory keyset discovery; never grants execution ownership."""

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import Connection, literal_column, select

from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    WorkflowRepository,
)
from workflow_engine.schema import task_runs, workflow_runs


@dataclass(frozen=True, slots=True)
class RunPage:
    run_ids: tuple[UUID, ...]
    next_after: UUID | None


class RunDiscoveryRepository:
    def __init__(self, connection: Connection) -> None:
        WorkflowRepository(connection)  # Shared PostgreSQL/transaction admission.
        self._connection = connection
        self._transaction = connection.get_transaction()

    def active(
        self, *, after: UUID | None = None, limit: int = 50, ready_only: bool = False
    ) -> RunPage:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "Run discovery requires its original transaction."
            )
        if after is not None and not isinstance(after, UUID):
            raise TypeError("Run cursor must be a UUID.")
        if (
            type(limit) is not int
            or not 1 <= limit <= 100
            or type(ready_only) is not bool
        ):
            raise ValueError(
                "Discovery requires limit 1..100 and a boolean readiness filter."
            )
        query = select(workflow_runs.c.id).where(
            workflow_runs.c.status == literal_column("'RUNNING'")
        )
        if after is not None:
            query = query.where(workflow_runs.c.id > after)
        if ready_only:
            query = query.where(
                select(task_runs.c.id)
                .where(
                    task_runs.c.run_id == workflow_runs.c.id,
                    task_runs.c.status == literal_column("'READY'"),
                )
                .exists()
            )
        values = tuple(
            self._connection.scalars(
                query.order_by(workflow_runs.c.id).limit(limit + 1)
            )
        )
        page = values[:limit]
        return RunPage(page, page[-1] if len(values) > limit else None)
