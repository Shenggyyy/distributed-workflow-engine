"""Validated, immutable workflow specifications, without runtime task state."""

from collections import Counter
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator
from pydantic_core import PydanticCustomError

from workflow_engine.domain.dag import DAG, MAX_TASKS, DAGValidationError, analyze_dag

Identifier = Annotated[
    str,
    StringConstraints(
        strict=True, min_length=1, max_length=64, pattern=r"^[A-Za-z][A-Za-z0-9_-]*$"
    ),
]
TaskType = Annotated[
    str,
    StringConstraints(
        strict=True, min_length=1, max_length=128, pattern=r"^[A-Za-z][A-Za-z0-9_.-]*$"
    ),
]


class TaskDefinition(BaseModel):
    """A task's graph identity and opaque handler registry key."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    task_id: Identifier
    task_type: TaskType
    depends_on: tuple[Identifier, ...] = Field(default=(), max_length=MAX_TASKS)


class WorkflowDefinition(BaseModel):
    """A static DAG; construction validates both the fields and graph invariants."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    schema_version: int = Field(default=1, strict=True, ge=1, le=1)
    name: Identifier
    tasks: tuple[TaskDefinition, ...] = Field(min_length=1, max_length=MAX_TASKS)

    def dag(self) -> DAG:
        """Build a fresh analysis snapshot; no cache or infrastructure access."""
        repeated = tuple(
            sorted(
                task_id
                for task_id, count in Counter(
                    task.task_id for task in self.tasks
                ).items()
                if count > 1
            )
        )
        if repeated:
            raise DAGValidationError(
                "duplicate_task_id",
                "Task IDs must be unique within a workflow.",
                repeated,
            )
        return analyze_dag({task.task_id: task.depends_on for task in self.tasks})

    @model_validator(mode="after")
    def validate_graph(self) -> Self:
        try:
            self.dag()
        except DAGValidationError as exc:
            raise PydanticCustomError(
                exc.code, str(exc), {"task_ids": exc.task_ids}
            ) from None
        return self
