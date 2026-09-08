"""Create or replay a run using the same explicit version and idempotency key."""

import argparse
from pathlib import Path
from uuid import UUID

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine
from workflow_engine.domain.idempotency import validate_idempotency_key
from workflow_engine.repositories.runs import RunRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version-id", type=UUID, required=True)
    parser.add_argument(
        "--idempotency-key", type=validate_idempotency_key, required=True
    )
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    with database_engine(load_settings(env_file=args.env_file)) as engine:
        with engine.begin() as connection:
            receipt = RunRepository(connection).create_idempotent(
                args.version_id, idempotency_key=args.idempotency_key
            )
    # Identical receipt on replay, printed only after the transaction commits.
    print(f"Run ID: {receipt.run_id}")
    print(f"Version ID: {receipt.workflow_version_id}")
    print("Creation receipt committed; repeat the same key/version to retrieve it.")
    print("No tasks executed; receipt does not report current execution status.")


if __name__ == "__main__":
    main()
