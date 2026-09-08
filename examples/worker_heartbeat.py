"""Apply one Worker heartbeat or expiry decision; no background loop."""

import argparse
from pathlib import Path
from uuid import UUID

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine
from workflow_engine.repositories.workers import WorkerRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("heartbeat", "expire"))
    parser.add_argument("--session-id", type=UUID, required=True)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    with database_engine(load_settings(env_file=args.env_file)) as engine:
        with engine.begin() as connection:
            repo = WorkerRepository(connection)
            result = (
                repo.heartbeat(args.session_id)
                if args.operation == "heartbeat"
                else repo.expire(args.session_id)
            )
    print(f"Session ID: {result.session.id}")
    print(f"Stored status: {result.session.status}")
    print(f"Last heartbeat: {result.last_heartbeat_at.isoformat()}")
    print(f"Heartbeat expires at: {result.heartbeat_expires_at.isoformat()}")
    print("Decision committed; no background scan or task ownership changes.")


if __name__ == "__main__":
    main()
