"""Read one run and its tasks from a single database statement snapshot."""

import argparse
from pathlib import Path
from uuid import UUID

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine
from workflow_engine.repositories.runs import RunRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    with database_engine(load_settings(env_file=args.env_file)) as engine:
        with engine.begin() as connection:
            snapshot = RunRepository(connection).get_run_with_tasks(args.run_id)
    if snapshot is None:
        parser.exit(1, "Run was not found.\n")
    print(f"Run ID: {snapshot.run.id}")
    print(f"Version ID: {snapshot.run.workflow_version_id}")
    print(f"Run status: {snapshot.run.status.value}")
    print(f"Created at: {snapshot.run.created_at.isoformat()}")
    for task in snapshot.tasks:
        print(f"Task {task.task_key}: {task.status.value}")
    print("Read-only statement snapshot; no tasks executed or statuses changed.")


if __name__ == "__main__":
    main()
