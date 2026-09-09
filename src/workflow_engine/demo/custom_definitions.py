"""Pure, bounded definitions for trusted custom demonstrations, not engine policy."""

from collections.abc import Mapping
from types import MappingProxyType

from workflow_engine.demo.scenarios import DIAMOND_DURATIONS
from workflow_engine.domain.retry import ExecutionPolicy
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition

CUSTOM_MAX_TASKS = 12
CUSTOM_BODY_LIMIT = 16_384
CUSTOM_POLICY = ExecutionPolicy(
    max_attempts=2,
    timeout_seconds=90,
    initial_backoff_ms=10_000,
    max_backoff_ms=10_000,
)
CUSTOM_HANDLERS: Mapping[str, int] = MappingProxyType(
    {
        "demo.observe": 8,
        "demo.recover": 20,
        "demo.join": 2,
        **DIAMOND_DURATIONS,
    }
)

_ERROR_MESSAGES = {
    "demo_task_limit": "Custom demonstrations support at most 12 tasks.",
    "demo_handler_not_allowed": "Task type is not a permitted demonstration Handler.",
    "demo_execution_policy": (
        "Custom demonstrations require the fixed execution policy."
    ),
}


class CustomDefinitionError(ValueError):
    """A stable demo restriction error; never includes submitted values."""

    def __init__(self, code: str, location: tuple[str | int, ...]) -> None:
        self.code = code
        self.location = location
        super().__init__(_ERROR_MESSAGES[code])


def normalize_custom(value: WorkflowDefinition) -> WorkflowDefinition:
    """Validate before filling policy defaults; retain exact names and array order."""
    definition = WorkflowDefinition.model_validate(value)
    if len(definition.tasks) > CUSTOM_MAX_TASKS:
        raise CustomDefinitionError("demo_task_limit", ("tasks",))
    tasks: list[TaskDefinition] = []
    for index, task in enumerate(definition.tasks):
        if task.task_type not in CUSTOM_HANDLERS:
            raise CustomDefinitionError(
                "demo_handler_not_allowed", ("tasks", index, "task_type")
            )
        if task.execution is not None and task.execution != CUSTOM_POLICY:
            raise CustomDefinitionError(
                "demo_execution_policy", ("tasks", index, "execution")
            )
        tasks.append(
            TaskDefinition(
                task_id=task.task_id,
                task_type=task.task_type,
                depends_on=task.depends_on,
                execution=CUSTOM_POLICY,
            )
        )
    return WorkflowDefinition(
        name=definition.name, schema_version=2, tasks=tuple(tasks)
    )


def definition_layers(definition: WorkflowDefinition) -> list[list[str]]:
    """Group a validated DAG by longest dependency depth in core topological order."""
    graph = definition.dag()
    depths: dict[str, int] = {}
    layers: list[list[str]] = []
    for task_id in graph.topological_order:
        depth = 1 + max(
            (depths[parent] for parent in graph.dependencies[task_id]), default=-1
        )
        depths[task_id] = depth
        if depth == len(layers):
            layers.append([])
        layers[depth].append(task_id)
    return layers
