"""Claim once, retain its token in memory, and renew in a separate transaction."""

import argparse
from pathlib import Path
from uuid import UUID

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine
from workflow_engine.repositories.claims import ClaimRepository
from workflow_engine.repositories.leases import LeaseRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--session-id", type=UUID, required=True)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    with database_engine(load_settings(env_file=args.env_file)) as engine:
        with engine.begin() as connection:
            claim = ClaimRepository(connection).claim_next(args.run_id, args.session_id)
        if claim is None:
            print("No claim: capacity exhausted or no READY task in this Run.")
            return
        # Only use ownership from our successfully committed claim. Do not query
        # another attempt's token from storage to manufacture authorization.
        with engine.begin() as connection:
            renewed = LeaseRepository(connection, lease_seconds=60).renew(
                claim.attempt.id,
                worker_session_id=claim.lease.worker_session_id,
                lease_token=claim.lease.lease_token,
            )
    print(f"Task key: {claim.task.task_key}")
    print(f"Attempt ID: {claim.attempt.id}")
    print(f"Original deadline: {claim.lease.lease_expires_at.isoformat()}")
    print(f"Renewed deadline: {renewed.lease_expires_at.isoformat()}")
    print(f"Ownership preserved: {renewed.lease_token == claim.lease.lease_token}")
    print("Renewal committed; token stays in memory and is not printed.")
    print(
        "No handler executed; capacity stays reserved until future completion/recovery."
    )


if __name__ == "__main__":
    main()
