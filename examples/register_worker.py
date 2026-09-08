"""Register/replay one explicit worker process session without executing tasks."""

import argparse
from pathlib import Path
from uuid import UUID

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine
from workflow_engine.repositories.workers import WorkerRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-id", type=UUID, required=True)
    parser.add_argument("--name", default="worker_local")
    parser.add_argument("--max-concurrency", type=int, default=2)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    with database_engine(load_settings(env_file=args.env_file)) as engine:
        with engine.begin() as connection:
            result = WorkerRepository(connection).register(
                args.session_id,
                worker_name=args.name,
                max_concurrency=args.max_concurrency,
            )
    print(f"Session ID: {result.session.id}")
    print(f"Worker name: {result.session.worker_name}")
    print(f"Stored status: {result.session.status}")
    print(f"Created at: {result.created_at.isoformat()}")
    print(f"Heartbeat expires at: {result.heartbeat_expires_at.isoformat()}")
    print(
        "Registration committed; replay does not renew liveness or prove eligibility."
    )
    print("No heartbeat loop, task claim or execution.")


if __name__ == "__main__":
    main()
