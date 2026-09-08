"""Initialize a pinned workflow run in one caller-owned transaction."""

from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy import Connection, select
from sqlalchemy.dialects.postgresql import insert

from workflow_engine.domain.idempotency import validate_idempotency_key
from workflow_engine.domain.runtime import (
    RunEvent,
    TaskEvent,
    TaskRun,
    WorkflowRun,
)
from workflow_engine.repositories.workflows import (
    RepositoryTransactionError,
    StoredWorkflowVersion,
    WorkflowRepository,
)
from workflow_engine.schema import run_creation_requests, task_runs, workflow_runs


class WorkflowVersionNotFoundError(LookupError):
    """The requested immutable version is not visible to this transaction."""


@dataclass(frozen=True, slots=True)
class CreatedRun:
    """Provisional initialization snapshots; durable only after caller commit."""

    run: WorkflowRun
    tasks: tuple[TaskRun, ...]


class IdempotencyConflictError(ValueError):
    """The key already identifies a request for another workflow version."""


@dataclass(frozen=True, slots=True)
class RunCreationReceipt:
    """Stable request result; no mutable execution state or replay-time fields."""

    run_id: UUID
    workflow_version_id: UUID


class RunRepository:
    """Create inside engine.begin(); propagate failures out of the transaction.

    There is no internal commit, retry, dispatch or attempt creation.
    create() always creates fresh identities; create_idempotent() binds a key.
    Do not share instances across threads.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._workflows = WorkflowRepository(connection)
        # The workflow repository checks dialect/isolation/autocommit at entry.
        # Key replays skip version reads, so guard our original transaction too.
        self._transaction = connection.get_transaction()

    def create(self, workflow_version_id: UUID) -> CreatedRun:
        """Pin a version, persist all nodes and activate roots atomically."""
        self._require_transaction()
        return self._initialize(self._get_version(workflow_version_id), uuid4())

    def create_idempotent(
        self, workflow_version_id: UUID, *, idempotency_key: str
    ) -> RunCreationReceipt:
        """Reserve a key and create, or replay its immutable receipt.

        Same key with a different version raises IdempotencyConflictError.
        Results remain provisional until the caller commits.
        """
        self._require_transaction()
        key = validate_idempotency_key(idempotency_key)
        if not isinstance(workflow_version_id, UUID):
            raise TypeError("workflow_version_id must be a UUID.")
        candidate_id = uuid4()
        reserved = self._connection.execute(
            insert(run_creation_requests)
            .values(
                idempotency_key=key,
                workflow_version_id=workflow_version_id,
                run_id=candidate_id,
            )
            .on_conflict_do_nothing(
                index_elements=[run_creation_requests.c.idempotency_key]
            )
            .returning(run_creation_requests.c.run_id)
        ).scalar_one_or_none()
        if reserved is not None:
            # The deferred FK permits reservation first. Initialization failure
            # must leave the outer transaction; binding and run roll back together.
            self._initialize(self._get_version(workflow_version_id), candidate_id)
            return RunCreationReceipt(candidate_id, workflow_version_id)

        # READ COMMITTED needs a new statement snapshot after a competing INSERT.
        # Never use DO UPDATE: bindings are immutable, including no-op updates.
        binding = self._connection.execute(
            select(
                run_creation_requests.c.run_id,
                run_creation_requests.c.workflow_version_id,
            ).where(run_creation_requests.c.idempotency_key == key)
        ).one()
        if binding.workflow_version_id != workflow_version_id:
            raise IdempotencyConflictError(
                "Idempotency key is already bound to another workflow version."
            )
        return RunCreationReceipt(binding.run_id, binding.workflow_version_id)

    def _require_transaction(self) -> None:
        if (
            self._transaction is None
            or not self._transaction.is_active
            or self._connection.get_transaction() is not self._transaction
        ):
            raise RepositoryTransactionError(
                "RunRepository requires its original active transaction."
            )

    def _get_version(self, workflow_version_id: UUID) -> StoredWorkflowVersion:
        version = self._workflows.get_version(workflow_version_id)
        if version is None:
            raise WorkflowVersionNotFoundError("Workflow version was not found.")
        return version

    def _initialize(self, version: StoredWorkflowVersion, run_id: UUID) -> CreatedRun:
        """Initialize a fresh internal UUID using the shared unkeyed/keyed path."""
        graph = version.definition.dag()
        roots = frozenset(graph.roots)
        pending_run = WorkflowRun(id=run_id, workflow_version_id=version.id)
        pending_tasks = tuple(
            TaskRun(id=uuid4(), run_id=pending_run.id, task_key=key)
            for key in graph.topological_order
        )
        # Resolve all domain transitions before writing. These roots have no
        # prerequisites; no task completion or execution is inferred here.
        initialized_tasks = tuple(
            task.transition(TaskEvent.DEPENDENCIES_SUCCEEDED)
            if task.task_key in roots
            else task
            for task in pending_tasks
        )
        active_run = pending_run.transition(RunEvent.START)

        self._connection.execute(
            workflow_runs.insert().values(
                id=pending_run.id,
                workflow_version_id=version.id,
                status=pending_run.status.value,
            )
        )
        self._connection.execute(
            task_runs.insert(),
            [
                {
                    "id": task.id,
                    "run_id": task.run_id,
                    "task_key": task.task_key,
                    "status": task.status.value,
                }
                for task in pending_tasks
            ],
        )
        # Persist actual PENDING -> READY/RUNNING edges, checked again by the
        # database guards. New rows are private to this creating transaction.
        ready_roots = tuple(
            task for task in initialized_tasks if task.task_key in roots
        )
        self._connection.execute(
            task_runs.update()
            .where(task_runs.c.id.in_([task.id for task in ready_roots]))
            .values(status=ready_roots[0].status.value)
        )
        self._connection.execute(
            workflow_runs.update()
            .where(workflow_runs.c.id == active_run.id)
            .values(status=active_run.status.value)
        )
        return CreatedRun(run=active_run, tasks=initialized_tasks)
