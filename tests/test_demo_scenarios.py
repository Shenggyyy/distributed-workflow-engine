"""Shared diamond definitions select explicit, backwards-compatible Handlers."""

from typing import cast
from uuid import UUID

import pytest

from workflow_engine.config import Settings
from workflow_engine.demo.handlers import ObservedHandler, registry
from workflow_engine.demo.scenarios import (
    Scenario,
    diamond_definition,
    diamond_tasks,
)


@pytest.mark.parametrize("scenario", ["parallel", "distribution", "recovery"])
def test_diamond_dependencies_and_execution_policies(scenario: Scenario) -> None:
    definition = diamond_definition(scenario)
    assert definition.schema_version == 2
    assert definition.tasks == diamond_tasks(scenario)
    assert {task.task_id: task.depends_on for task in definition.tasks} == {
        "A": (),
        "B": ("A",),
        "C": ("A",),
        "D": ("B", "C"),
    }
    assert definition.dag().roots == ("A",)
    assert {task.task_id: task.task_type for task in definition.tasks} == {
        "A": "demo.diamond.a",
        "B": "demo.diamond.b",
        "C": "demo.diamond.recover" if scenario == "recovery" else "demo.diamond.c",
        "D": "demo.diamond.d",
    }
    for task in definition.tasks:
        assert task.execution is not None
        assert task.execution.max_attempts == 2
        assert task.execution.timeout_seconds == 90
        assert task.execution.initial_backoff_ms == 10000
        assert task.execution.max_backoff_ms == 10000


def test_explicit_definition_requests_have_distinct_workflow_names() -> None:
    first = diamond_definition("parallel")
    second = diamond_definition("parallel")
    prefix = "demo_diamond_parallel_"
    assert first.name.startswith(prefix) and second.name.startswith(prefix)
    assert UUID(first.name.removeprefix(prefix)) != UUID(
        second.name.removeprefix(prefix)
    )
    assert first.tasks == second.tasks


def test_registered_callables_use_requested_durations_and_keep_legacy_types() -> None:
    settings = Settings()
    handlers = {
        entry.task_type: entry.handler for entry in registry(settings).registrations
    }
    expected = {
        "demo.diamond.a": 6,
        "demo.diamond.b": 8,
        "demo.diamond.c": 14,
        "demo.diamond.recover": 20,
        "demo.diamond.d": 3,
        "demo.observe": 8,
        "demo.recover": 20,
        "demo.join": 2,
    }
    assert handlers.keys() == expected.keys()
    for task_type, seconds in expected.items():
        handler = handlers[task_type]
        assert isinstance(handler, ObservedHandler)
        assert handler.settings == settings
        assert handler.seconds == seconds


def test_unknown_scenario_cannot_silently_choose_an_execution_definition() -> None:
    unknown = cast(Scenario, "arbitrary-command")
    with pytest.raises(ValueError):
        diamond_tasks(unknown)
    with pytest.raises(ValueError):
        diamond_definition(unknown)
