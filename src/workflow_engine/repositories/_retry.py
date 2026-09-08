"""Failure settlement helpers used only while holding ordered ownership locks."""

from datetime import datetime
from random import random

from sqlalchemy import Connection, select

from workflow_engine.domain.retry import ExecutionPolicy, retry_delay
from workflow_engine.domain.runtime import (
    AttemptStatus,
    TaskAttempt,
    TaskEvent,
    TaskRun,
    TaskStatus,
)
from workflow_engine.domain.workflow import WorkflowDefinition
from workflow_engine.repositories._ownership import LockedOwnership, StoredLeaseError
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import task_attempts, task_retry_schedules


def execution_policy(connection: Connection, owned: LockedOwnership) -> ExecutionPolicy:
    version = WorkflowRepository(connection).get_version(owned.run.workflow_version_id)
    if version is not None:
        for task in version.definition.tasks:
            if task.task_id == owned.task.task_key:
                return task.execution_policy
    raise StoredLeaseError("Pinned execution policy was not found.")


def failed_task(
    connection: Connection,
    owned: LockedOwnership,
    attempt: TaskAttempt,
    observed_at: datetime,
) -> TaskRun:
    """Persist a retry decision and propose Task state; caller writes both states.

    The caller must roll back any error. The deferred FK validates terminal
    Attempt status at COMMIT; this function never acquires reverse-order locks.
    """
    if (
        attempt.id != owned.attempt.id
        or attempt.task_id != owned.task.id
        or attempt.status
        not in {AttemptStatus.FAILED, AttemptStatus.TIMED_OUT, AttemptStatus.LOST}
    ):
        raise StoredLeaseError("Failure settlement references do not match.")
    policy = execution_policy(connection, owned)
    delay = retry_delay(policy, attempt.attempt_number, jitter=random())
    if delay is None:
        return owned.task.transition(TaskEvent.FAIL_PERMANENTLY)
    task = owned.task.transition(TaskEvent.RETRY_SCHEDULED)
    expected = dict(
        attempt_id=attempt.id,
        outcome=attempt.status.value,
        scheduled_at=observed_at,
        available_at=observed_at + delay,
    )
    stored = (
        connection.execute(
            task_retry_schedules.insert()
            .values(**expected)
            .returning(task_retry_schedules)
        )
        .mappings()
        .one_or_none()
    )
    if stored is None or dict(stored) != expected:
        raise StoredLeaseError("Stored retry schedule differs from its proposal.")
    return task


def due_tasks(
    connection: Connection,
    definition: WorkflowDefinition,
    tasks: tuple[TaskRun, ...],
    observed_at: datetime,
) -> tuple[TaskRun, ...]:
    """Propose RETRY_WAIT -> READY under Run/Task locks, without locking history."""
    waiting = {task.id: task for task in tasks if task.status is TaskStatus.RETRY_WAIT}
    if not waiting:
        return ()
    latest = (
        select(task_attempts)
        .where(task_attempts.c.task_id.in_(waiting))
        .distinct(task_attempts.c.task_id)
        .order_by(task_attempts.c.task_id, task_attempts.c.attempt_number.desc())
        .subquery()
    )
    rows = (
        connection.execute(
            select(
                latest.c.task_id,
                latest.c.attempt_number,
                latest.c.status,
                task_retry_schedules.c.outcome,
                task_retry_schedules.c.scheduled_at,
                task_retry_schedules.c.available_at,
            ).select_from(
                latest.outerjoin(
                    task_retry_schedules,
                    latest.c.id == task_retry_schedules.c.attempt_id,
                )
            )
        )
        .mappings()
        .all()
    )
    if len(rows) != len(waiting):
        raise StoredLeaseError("Waiting Task has no latest Attempt.")
    policies = {task.task_id: task.execution_policy for task in definition.tasks}
    ready = []
    for row in rows:
        task = waiting[row["task_id"]]
        if (
            row["available_at"] is None
            or row["status"] not in {"FAILED", "TIMED_OUT", "LOST"}
            or row["outcome"] != row["status"]
            or row["attempt_number"] >= policies[task.task_key].max_attempts
        ):
            raise StoredLeaseError("Waiting Task retry history is inconsistent.")
        if observed_at >= row["available_at"]:
            ready.append(task.transition(TaskEvent.RETRY_DUE))
    return tuple(ready)
