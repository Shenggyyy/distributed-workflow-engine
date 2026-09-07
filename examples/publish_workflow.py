"""Publish and retrieve the example DAG; every invocation appends a version."""

import argparse
from pathlib import Path

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine
from workflow_engine.domain.workflow import WorkflowDefinition
from workflow_engine.repositories.workflows import WorkflowRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    definition = WorkflowDefinition.model_validate_json(
        Path(__file__).with_name("diamond.json").read_text(encoding="utf-8")
    )
    with database_engine(load_settings(env_file=args.env_file)) as engine:
        with engine.begin() as connection:
            version = WorkflowRepository(connection).publish(definition)
        # Report publication only after the transaction has committed.
        with engine.begin() as connection:
            stored = WorkflowRepository(connection).get_version(version.id)
            if stored != version:
                raise RuntimeError("Committed workflow version did not round trip.")
    print(f"Workflow: {version.definition.name}")
    print(f"Version: {version.version_number}")
    print(f"Version ID: {version.id}")
    print("Published and retrieved; no tasks were executed.")


if __name__ == "__main__":
    main()
