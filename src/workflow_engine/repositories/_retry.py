"""Failure settlement helpers used only while holding ordered ownership locks."""

from datetime import datetime
from random import random

from sqlalchemy import Connection

from workflow_engine.domain.retry import ExecutionPolicy, retry_delay
from workflow_engine.domain.runtime import (
    AttemptStatus,
    TaskAttempt,
    TaskEvent,
    TaskRun,
)
from workflow_engine.repositories._ownership import LockedOwnership, StoredLeaseError
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import task_retry_schedules


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
