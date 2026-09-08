"""Create disposable work, submit a simulated outcome, then replay its receipt."""

import argparse
from pathlib import Path
from uuid import uuid4

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine
from workflow_engine.domain.completion import (
    AttemptCompletion,
    CompletionOutcome,
    CompletionResult,
)
from workflow_engine.domain.workflow import TaskDefinition, WorkflowDefinition
from workflow_engine.repositories.claim_requests import ClaimRequestRepository
from workflow_engine.repositories.completions import CompletionRepository
from workflow_engine.repositories.runs import RunRepository
from workflow_engine.repositories.workers import WorkerRepository
from workflow_engine.repositories.workflows import WorkflowRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument(
        "--outcome", choices=[v.value for v in CompletionOutcome], default="SUCCEEDED"
    )
    args = parser.parse_args()
    outcome = CompletionOutcome(args.outcome)
    with database_engine(load_settings(env_file=args.env_file)) as engine:
        with engine.begin() as connection:
            version = WorkflowRepository(connection).publish(
                WorkflowDefinition(
                    name="completion_demo_" + uuid4().hex,
                    tasks=(TaskDefinition(task_id="A", task_type="demo.echo"),),
                )
            )
        with engine.begin() as connection:
            run = RunRepository(connection).create_idempotent(
                version.id, idempotency_key=uuid4().hex
            )
        session_id = uuid4()
        with engine.begin() as connection:
            WorkerRepository(connection).register(
                session_id, worker_name="completion_demo", max_concurrency=1
            )
        with engine.begin() as connection:
            claim = ClaimRequestRepository(connection).claim_next(
                run.run_id, session_id, request_id=uuid4()
            )
        if claim is None:
            raise RuntimeError("Expected a claim from the newly created demo Run.")
        report = AttemptCompletion(
            attempt_id=claim.attempt.id,
            worker_session_id=session_id,
            lease_token=claim.lease.lease_token,
            result=CompletionResult(
                outcome=outcome,
                error_code="demo_failure"
                if outcome is CompletionOutcome.FAILED
                else None,
            ),
        )
        with engine.begin() as connection:
            receipt = CompletionRepository(connection).complete(report)
        with engine.begin() as connection:
            replay = CompletionRepository(connection).complete(report)
    print(f"Run ID: {run.run_id}")
    print(f"Attempt ID: {receipt.attempt.id}")
    print(f"Committed simulated outcome: {receipt.attempt.status}")
    print(f"Original receipt replayed: {receipt == replay}")
    print("Attempt and Task settled; capacity released. Token was not printed.")
    print("No handler executed; Run remains RUNNING until aggregation is implemented.")


if __name__ == "__main__":
    main()
