"""Diamond factories use the existing immutable publication and Run contracts."""

import pytest
from sqlalchemy import Engine, select

from tests.integration.test_lease_http import http_engine as http_engine
from workflow_engine.demo.scenarios import Scenario, diamond_definition, diamond_tasks
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workflows import WorkflowRepository
from workflow_engine.schema import task_attempts, task_runs, workflow_runs

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("scenario", ["parallel", "distribution", "recovery"])
def test_committed_diamond_run_only_releases_its_root(
    http_engine: Engine, scenario: Scenario
) -> None:
    definition = diamond_definition(scenario)
    with http_engine.begin() as connection:
        version = WorkflowRepository(connection).publish(definition)
        created = RunRepository(connection).create(version.id)
    with http_engine.begin() as connection:
        stored = WorkflowRepository(connection).get_version(version.id)
        assert stored is not None
        assert stored.definition == definition
        tasks = connection.execute(
            select(task_runs.c.task_key, task_runs.c.status).where(
                task_runs.c.run_id == created.run.id
            )
        ).all()
        assert {task.task_key: task.status for task in tasks} == {
            "A": "READY",
            "B": "PENDING",
            "C": "PENDING",
            "D": "PENDING",
        }
        assert (
            connection.scalar(
                select(workflow_runs.c.workflow_version_id).where(
                    workflow_runs.c.id == created.run.id
                )
            )
            == version.id
        )
        assert connection.execute(select(task_attempts)).first() is None


def test_later_definition_does_not_rewrite_a_published_diamond_or_existing_run(
    http_engine: Engine,
) -> None:
    original = diamond_definition("parallel")
    changed = original.model_copy(update={"tasks": diamond_tasks("recovery")})
    with http_engine.begin() as connection:
        versions = WorkflowRepository(connection)
        first = versions.publish(original)
        old_run = RunRepository(connection).create(first.id)
    with http_engine.begin() as connection:
        versions = WorkflowRepository(connection)
        second = versions.publish(changed)
        new_run = RunRepository(connection).create(second.id)
    with http_engine.begin() as connection:
        versions = WorkflowRepository(connection)
        old = versions.get_version(first.id)
        new = versions.get_version(second.id)
        assert old is not None and new is not None
        assert old.definition == original
        assert new.definition == changed
        assert first.workflow_id == second.workflow_id
        assert first.version_number == 1 and second.version_number == 2
        bindings = {
            row.id: row.workflow_version_id
            for row in connection.execute(
                select(workflow_runs.c.id, workflow_runs.c.workflow_version_id)
            ).all()
        }
        assert bindings == {old_run.run.id: first.id, new_run.run.id: second.id}
        assert {task.id for task in old_run.tasks}.isdisjoint(
            task.id for task in new_run.tasks
        )
