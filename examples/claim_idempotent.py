"""Claim in one transaction, then retry the same request in a new transaction."""

import argparse
from pathlib import Path
from uuid import UUID

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine
from workflow_engine.repositories.claim_requests import ClaimRequestRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--session-id", type=UUID, required=True)
    parser.add_argument("--request-id", type=UUID, required=True)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    with database_engine(load_settings(env_file=args.env_file)) as engine:
        with engine.begin() as connection:
            original = ClaimRequestRepository(connection).claim_next(
                args.run_id, args.session_id, request_id=args.request_id
            )
        # Simulate losing the response after COMMIT. Retain the request UUID and
        # use a fresh transaction rather than issuing a new allocation request.
        with engine.begin() as connection:
            replay = ClaimRequestRepository(connection).claim_next(
                args.run_id, args.session_id, request_id=args.request_id
            )
    if original is None:
        assert replay is None
        print("No work; the committed empty result was replayed.")
        return
    assert replay is not None and replay.attempt.id == original.attempt.id
    print(f"Task key: {replay.task.task_key}")
    print(f"Attempt ID: {replay.attempt.id}")
    print("Same request replayed the same Attempt: True")
    print("Both transactions committed; no handler executed or lease renewed.")
    print(
        "Tokens are not printed. Capacity remains reserved until completion/recovery."
    )


if __name__ == "__main__":
    main()
