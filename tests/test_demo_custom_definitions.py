"""Custom demo restrictions preserve the broader core Workflow contract."""

from typing import cast

import pytest
from pydantic import ValidationError

from workflow_engine.config import Settings
from workflow_engine.demo.custom_definitions import (
    CUSTOM_HANDLERS,
    CUSTOM_POLICY,
    CustomDefinitionError,
    definition_layers,
    normalize_custom,
)
from workflow_engine.demo.handlers import ObservedHandler, registry
from workflow_engine.demo.scenarios import Scenario, diamond_definition
from workflow_engine.domain.retry import ExecutionPolicy
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition


def single_task(**changes: object) -> WorkflowDefinition:
    return WorkflowDefinition.model_validate(
        {
            "name": "Custom_Kept",
            "schema_version": 2,
            "tasks": [{"task_id": "Task_Kept", "task_type": "demo.observe", **changes}],
        }
    )


@pytest.mark.parametrize("scenario", ["parallel", "distribution", "recovery"])
def test_existing_diamond_templates_are_accepted_unchanged(scenario: Scenario) -> None:
    original = diamond_definition(scenario)
    normalized = normalize_custom(original)
    assert normalized == original
    assert definition_layers(normalized) == [["A"], ["B", "C"], ["D"]]


@pytest.mark.parametrize("schema_version", [1, 2])
def test_omitted_policy_normalizes_without_reordering_or_mutating_input(
    schema_version: int,
) -> None:
    original = WorkflowDefinition(
        name="Keep_Name",
        schema_version=schema_version,
        tasks=(
            TaskDefinition(task_id="D", task_type="demo.join", depends_on=("C", "B")),
            TaskDefinition(task_id="C", task_type="demo.observe", depends_on=("A",)),
            TaskDefinition(task_id="A", task_type="demo.observe"),
            TaskDefinition(task_id="B", task_type="demo.observe", depends_on=("A",)),
        ),
    )
    before = original.model_dump(mode="json")
    normalized = normalize_custom(original)
    assert normalized.schema_version == 2
    assert normalized.name == "Keep_Name"
    assert [task.task_id for task in normalized.tasks] == ["D", "C", "A", "B"]
    assert normalized.tasks[0].depends_on == ("C", "B")
    assert all(task.execution == CUSTOM_POLICY for task in normalized.tasks)
    assert normalize_custom(normalized) == normalized
    assert original.model_dump(mode="json") == before
    assert original.tasks[0].execution_policy == ExecutionPolicy()


def test_allowlist_exactly_matches_real_registered_timed_handlers() -> None:
    handlers = registry(Settings()).registrations
    actual: dict[str, float] = {}
    for entry in handlers:
        assert isinstance(entry.handler, ObservedHandler)
        actual[entry.task_type] = entry.handler.seconds
    assert dict(CUSTOM_HANDLERS) == actual
    assert len(actual) == 8
    definition = WorkflowDefinition(
        name="All_Handlers",
        tasks=tuple(
            TaskDefinition(task_id=f"Task{index}", task_type=kind)
            for index, kind in enumerate(actual)
        ),
    )
    assert len(normalize_custom(definition).tasks) == 8
    with pytest.raises(TypeError):
        cast(dict[str, int], CUSTOM_HANDLERS)["arbitrary.handler"] = 1


def test_twelve_tasks_are_accepted_but_thirteen_remain_core_only() -> None:
    tasks = tuple(
        TaskDefinition(task_id=f"Task{index}", task_type="demo.join")
        for index in range(13)
    )
    accepted = WorkflowDefinition(name="At_Limit", tasks=tasks[:12])
    assert len(normalize_custom(accepted).tasks) == 12
    core_only = WorkflowDefinition(name="Core_Valid", tasks=tasks)
    with pytest.raises(CustomDefinitionError) as error:
        normalize_custom(core_only)
    assert error.value.code == "demo_task_limit"
    assert error.value.location == ("tasks",)
    assert len(core_only.dag().roots) == 13


@pytest.mark.parametrize("kind", ["demo.echo", "arbitrary.handler", "demo.Observe"])
def test_core_accepts_opaque_handler_keys_but_demo_rejects_them_safely(
    kind: str,
) -> None:
    definition = single_task(task_type=kind)
    assert definition.tasks[0].task_type == kind
    with pytest.raises(CustomDefinitionError) as error:
        normalize_custom(definition)
    assert error.value.code == "demo_handler_not_allowed"
    assert error.value.location == ("tasks", 0, "task_type")
    assert kind not in str(error.value)
    assert definition.name not in str(error.value)


@pytest.mark.parametrize(
    "changes",
    [
        {"max_attempts": 1},
        {"timeout_seconds": 91},
        {"initial_backoff_ms": 9999},
        {"max_backoff_ms": 10001},
    ],
)
def test_different_explicit_core_policy_is_rejected_not_overwritten(
    changes: dict[str, int],
) -> None:
    policy = ExecutionPolicy.model_validate({**CUSTOM_POLICY.model_dump(), **changes})
    definition = single_task(execution=policy.model_dump())
    with pytest.raises(CustomDefinitionError) as error:
        normalize_custom(definition)
    assert error.value.code == "demo_execution_policy"
    assert error.value.location == ("tasks", 0, "execution")
    assert definition.tasks[0].execution == policy


@pytest.mark.parametrize("level", ["workflow", "task", "policy", "dag"])
def test_copied_or_constructed_instances_cannot_bypass_core_validation(
    level: str,
) -> None:
    original = normalize_custom(single_task())
    if level == "workflow":
        invalid = original.model_copy(update={"name": "not a valid name"})
    elif level == "task":
        task = TaskDefinition.model_construct(
            task_id="invalid name", task_type="demo.join"
        )
        invalid = original.model_copy(update={"tasks": (task,)})
    elif level == "policy":
        policy = CUSTOM_POLICY.model_copy(update={"max_attempts": 0})
        task = original.tasks[0].model_copy(update={"execution": policy})
        invalid = original.model_copy(update={"tasks": (task,)})
    else:
        task = original.tasks[0].model_copy(update={"depends_on": ("Missing",)})
        invalid = WorkflowDefinition.model_construct(
            name=original.name, schema_version=2, tasks=(task,)
        )
    with pytest.raises(ValidationError):
        normalize_custom(invalid)


def test_restriction_location_identifies_the_original_array_index() -> None:
    definition = WorkflowDefinition(
        name="Array_Order",
        tasks=(
            TaskDefinition(task_id="Z", task_type="demo.join"),
            TaskDefinition(task_id="A", task_type="private.handler"),
        ),
    )
    with pytest.raises(CustomDefinitionError) as error:
        normalize_custom(definition)
    assert error.value.location == ("tasks", 1, "task_type")


def test_layers_use_longest_dependency_depth_and_preserve_disconnected_roots() -> None:
    definition = WorkflowDefinition(
        name="Uneven_Graph",
        tasks=(
            TaskDefinition(task_id="D", task_type="demo.join", depends_on=("A", "C")),
            TaskDefinition(task_id="Z", task_type="demo.join"),
            TaskDefinition(task_id="C", task_type="demo.join", depends_on=("B",)),
            TaskDefinition(task_id="B", task_type="demo.join", depends_on=("A",)),
            TaskDefinition(task_id="A", task_type="demo.join"),
        ),
    )
    assert definition_layers(definition) == [["A", "Z"], ["B"], ["C"], ["D"]]
    reordered = WorkflowDefinition(
        name=definition.name, tasks=tuple(reversed(definition.tasks))
    )
    assert definition_layers(reordered) == definition_layers(definition)
