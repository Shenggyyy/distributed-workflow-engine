"""Claim at most one task for an existing Run/session; never execute a handler."""

import argparse
from pathlib import Path
from uuid import UUID

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine
from workflow_engine.repositories.claims import ClaimRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--session-id", type=UUID, required=True)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    with database_engine(load_settings(env_file=args.env_file)) as engine:
        with engine.begin() as connection:
            result = ClaimRepository(connection).claim_next(
                args.run_id, args.session_id
            )
    if result is None:
        print("No claim: capacity exhausted or no READY task in this Run.")
        return
    print(f"Task key: {result.task.task_key}")
    print(f"Task ID: {result.task.id}")
    print(f"Attempt ID: {result.attempt.id}")
    print(f"Attempt number: {result.attempt.attempt_number}")
    print(f"Handler type: {result.definition.task_type}")
    print(f"Lease expires at: {result.lease.lease_expires_at.isoformat()}")
    print("Claim committed; no handler executed. Token intentionally not printed.")
    print("This demo reserves capacity; automatic lease recovery is not implemented.")


if __name__ == "__main__":
    main()
