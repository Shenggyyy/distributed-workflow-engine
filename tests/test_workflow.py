"""Workflow input validation, immutability, and JSON round trips."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from workflow_engine.domain.retry import ExecutionPolicy
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "diamond.json"


def test_legacy_serialization_and_fixed_execution_defaults() -> None:
    task = TaskDefinition(task_id="A", task_type="demo.echo")
    assert task.model_dump(mode="json") == {
        "task_id": "A",
        "task_type": "demo.echo",
        "depends_on": [],
    }
    assert task.execution_policy == ExecutionPolicy()
    assert WorkflowDefinition(name="demo", tasks=(task,)).schema_version == 1


def test_policy_requires_explicit_schema_two_and_is_revalidated() -> None:
    policy = ExecutionPolicy(max_attempts=3)
    task = TaskDefinition(task_id="A", task_type="demo.echo", execution=policy)
    with pytest.raises(ValidationError, match="requires schema 2"):
        WorkflowDefinition(name="demo", tasks=(task,))
    workflow = WorkflowDefinition(schema_version=2, name="demo", tasks=(task,))
    assert (
        WorkflowDefinition.model_validate_json(workflow.model_dump_json()) == workflow
    )
    assert workflow.tasks[0].execution_policy.max_attempts == 3
    invalid = task.model_copy(
        update={"execution": policy.model_copy(update={"max_attempts": 0})}
    )
    with pytest.raises(ValidationError):
        WorkflowDefinition(schema_version=2, name="demo", tasks=(invalid,))


def test_example_round_trip_and_dependency_direction() -> None:
    workflow = WorkflowDefinition.model_validate_json(EXAMPLE.read_text())
    assert (
        WorkflowDefinition.model_validate_json(workflow.model_dump_json()) == workflow
    )
    assert workflow.dag().topological_order == ("A", "B", "C", "D")
    assert workflow.dag().roots == ("A",)
    assert workflow.dag().dependencies["D"] == ("B", "C")
    assert workflow.dag().dependents["B"] == ("C", "D")


@pytest.mark.parametrize(
    ("tasks", "code"),
    [
        ([], "too_short"),
        ([{"task_id": "A", "task_type": "echo"}] * 2, "duplicate_task_id"),
        (
            [{"task_id": "A", "task_type": "echo", "depends_on": ["A"]}],
            "self_dependency",
        ),
        (
            [{"task_id": "A", "task_type": "echo", "depends_on": ["X"]}],
            "unknown_dependency",
        ),
        (
            [
                {"task_id": "A", "task_type": "echo"},
                {"task_id": "B", "task_type": "echo", "depends_on": ["A", "A"]},
            ],
            "duplicate_dependency",
        ),
        (
            [
                {"task_id": "A", "task_type": "echo", "depends_on": ["B"]},
                {"task_id": "B", "task_type": "echo", "depends_on": ["A"]},
            ],
            "cycle_detected",
        ),
    ],
)
def test_invalid_workflow_graphs(tasks: list[dict[str, object]], code: str) -> None:
    with pytest.raises(ValidationError) as error:
        WorkflowDefinition.model_validate({"name": "test", "tasks": tasks})
    assert error.value.errors(include_input=False)[0]["type"] == code


@pytest.mark.parametrize(
    "value", ["", "has spaces", "1first", "A/B", "A\n", "中", "A" * 65, 1, b"A"]
)
def test_task_ids_are_strict_identifiers(value: object) -> None:
    with pytest.raises(ValidationError):
        TaskDefinition.model_validate({"task_id": value, "task_type": "echo"})


@pytest.mark.parametrize("value", ["", "type name", "type/echo", "t" * 129, 1])
def test_task_type_is_a_registry_key(value: object) -> None:
    with pytest.raises(ValidationError):
        TaskDefinition.model_validate({"task_id": "A", "task_type": value})


@pytest.mark.parametrize("value", [0, 3, "1", True, 1.0])
def test_unsupported_or_coerced_schema_version_is_rejected(value: object) -> None:
    with pytest.raises(ValidationError):
        WorkflowDefinition.model_validate(
            {
                "schema_version": value,
                "name": "test",
                "tasks": [{"task_id": "A", "task_type": "echo"}],
            }
        )


@pytest.mark.parametrize(
    "data",
    [
        {
            "name": "test",
            "tasks": [{"task_id": "A", "task_type": "echo"}],
            "unexpected": 1,
        },
        {
            "name": "test",
            "tasks": [{"task_id": "A", "task_type": "echo", "depend_on": []}],
        },
        {"name": "test", "tasks": [{"task_id": "A"}]},
        {
            "name": "test",
            "tasks": [{"task_id": "A", "task_type": "echo", "depends_on": "B"}],
        },
    ],
)
def test_unknown_fields_and_invalid_task_shapes_are_rejected(
    data: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        WorkflowDefinition.model_validate(data)


def test_validation_detaches_mutable_input_and_freezes_nested_models() -> None:
    parents = ["A"]
    tasks = [
        {"task_id": "A", "task_type": "echo"},
        {"task_id": "B", "task_type": "echo", "depends_on": parents},
    ]
    workflow = WorkflowDefinition.model_validate({"name": "test", "tasks": tasks})
    parents.append("missing")
    tasks.clear()
    assert len(workflow.tasks) == 2
    assert workflow.tasks[1].depends_on == ("A",)
    with pytest.raises(ValidationError):
        workflow.name = "changed"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        workflow.tasks[1].depends_on = ()  # type: ignore[misc]


def test_existing_instances_are_revalidated() -> None:
    valid = WorkflowDefinition.model_validate_json(EXAMPLE.read_text())
    invalid = valid.model_copy(update={"name": ""})
    with pytest.raises(ValidationError):
        WorkflowDefinition.model_validate(invalid)
    invalid_task = valid.tasks[0].model_copy(update={"task_id": ""})
    with pytest.raises(ValidationError):
        WorkflowDefinition(name="test", tasks=(invalid_task,))


def test_too_many_tasks_are_rejected() -> None:
    with pytest.raises(ValidationError) as error:
        WorkflowDefinition.model_validate(
            {
                "name": "test",
                "tasks": [
                    {"task_id": f"t{i}", "task_type": "echo"} for i in range(1001)
                ],
            }
        )
    assert error.value.errors(include_input=False)[0]["type"] == "too_long"


def test_example_accepts_unknown_handler_without_executing_it() -> None:
    data = json.loads(EXAMPLE.read_text())
    data["tasks"][0]["task_type"] = "unregistered.handler"
    workflow = WorkflowDefinition.model_validate(data)
    assert workflow.tasks[0].task_type == "unregistered.handler"


def test_example_script_runs_from_outside_repository(tmp_path: Path) -> None:
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, str(EXAMPLE.with_name("validate_workflow.py"))],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    assert result.stdout.splitlines() == [
        "Workflow: diamond",
        "Roots: A",
        "Topological order: A, B, C, D",
        "Validation only; no tasks were executed.",
    ]


def test_workflow_exposes_edge_limit_as_a_validation_error() -> None:
    parents = [f"p{i:03}" for i in range(100)]
    tasks: list[dict[str, object]] = [
        {"task_id": task_id, "task_type": "echo"} for task_id in parents
    ]
    tasks.extend(
        {"task_id": f"c{i:03}", "task_type": "echo", "depends_on": parents}
        for i in range(101)
    )
    with pytest.raises(ValidationError) as error:
        WorkflowDefinition.model_validate({"name": "wide", "tasks": tasks})
    assert error.value.errors(include_input=False)[0]["type"] == "too_many_edges"
