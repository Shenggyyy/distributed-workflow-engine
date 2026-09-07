"""Create a run for an existing version; every invocation creates a new run."""

import argparse
from pathlib import Path
from uuid import UUID

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine
from workflow_engine.repositories.runs import RunRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version-id", type=UUID, required=True)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    with database_engine(load_settings(env_file=args.env_file)) as engine:
        with engine.begin() as connection:
            created = RunRepository(connection).create(args.version_id)
    # Emit success only after the caller-owned transaction commits.
    print(f"Run ID: {created.run.id}")
    print(f"Version ID: {created.run.workflow_version_id}")
    print(f"Run status: {created.run.status.value}")
    for task in created.tasks:
        print(f"Task {task.task_key}: {task.status.value}")
    print("Run committed; no attempts created and no tasks executed.")


if __name__ == "__main__":
    main()
