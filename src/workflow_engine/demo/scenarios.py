"""Fixed trusted diamond definitions; existing published versions stay immutable."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Literal
from uuid import uuid4

from workflow_engine.domain.retry import ExecutionPolicy
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition

Scenario = Literal["parallel", "distribution", "recovery"]
DIAMOND_DURATIONS: Mapping[str, int] = MappingProxyType(
    {
        "demo.diamond.a": 6,
        "demo.diamond.b": 8,
        "demo.diamond.c": 14,
        "demo.diamond.recover": 20,
        "demo.diamond.d": 3,
    }
)


def diamond_tasks(scenario: Scenario) -> tuple[TaskDefinition, ...]:
    if scenario not in ("parallel", "distribution", "recovery"):
        raise ValueError("Unknown demo scenario.")
    policy = ExecutionPolicy(
        max_attempts=2,
        timeout_seconds=90,
        initial_backoff_ms=10000,
        max_backoff_ms=10000,
    )
    return tuple(
        TaskDefinition(
            task_id=key, task_type=kind, depends_on=dependencies, execution=policy
        )
        for key, kind, dependencies in (
            ("A", "demo.diamond.a", ()),
            ("B", "demo.diamond.b", ("A",)),
            (
                "C",
                "demo.diamond.recover" if scenario == "recovery" else "demo.diamond.c",
                ("A",),
            ),
            ("D", "demo.diamond.d", ("B", "C")),
        )
    )


def diamond_definition(scenario: Scenario) -> WorkflowDefinition:
    return WorkflowDefinition(
        name=f"demo_diamond_{scenario}_{uuid4().hex}",
        schema_version=2,
        tasks=diamond_tasks(scenario),
    )
