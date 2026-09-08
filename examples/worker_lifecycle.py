"""Show process restart identities and terminal worker states without registering."""

from uuid import uuid4

from workflow_engine.domain.runtime import InvalidStateTransition
from workflow_engine.domain.worker import WorkerEvent, WorkerSession


def main() -> None:
    first = WorkerSession(id=uuid4(), worker_name="worker_local", max_concurrency=2)
    lost = first.transition(WorkerEvent.HEARTBEAT_EXPIRED)
    restarted = WorkerSession(
        id=uuid4(), worker_name=first.worker_name, max_concurrency=first.max_concurrency
    )
    stopped = restarted.transition(WorkerEvent.SHUTDOWN)
    print(f"First session: {first.status} -> {lost.status}")
    print(f"Restart uses a new session ID: {first.id != restarted.id}")
    print(f"Restarted session: {restarted.status} -> {stopped.status}")
    try:
        lost.transition(WorkerEvent.SHUTDOWN)
    except InvalidStateTransition:
        print("Old LOST session rejects further transitions.")
    print("In-memory lifecycle only; no registration, heartbeat or task execution.")


if __name__ == "__main__":
    main()
