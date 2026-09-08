"""Immutable identities for one worker process incarnation, without I/O or clocks."""

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from workflow_engine.domain.runtime import InvalidStateTransition
from workflow_engine.domain.workflow import Identifier

# Match the planned PostgreSQL INTEGER field; this is not a resource guarantee.
MAX_WORKER_CONCURRENCY = 2_147_483_647


class WorkerStatus(StrEnum):
    ACTIVE = "ACTIVE"
    LOST = "LOST"
    STOPPED = "STOPPED"


class WorkerEvent(StrEnum):
    HEARTBEAT_EXPIRED = "HEARTBEAT_EXPIRED"
    SHUTDOWN = "SHUTDOWN"


_TRANSITIONS: Mapping[tuple[WorkerStatus, WorkerEvent], WorkerStatus] = (
    MappingProxyType(
        {
            (WorkerStatus.ACTIVE, WorkerEvent.HEARTBEAT_EXPIRED): WorkerStatus.LOST,
            (WorkerStatus.ACTIVE, WorkerEvent.SHUTDOWN): WorkerStatus.STOPPED,
        }
    )
)


class WorkerSession(BaseModel):
    """One process boot; a name is a label, and ACTIVE is not proof of liveness."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    id: UUID
    worker_name: Identifier
    max_concurrency: int = Field(strict=True, ge=1, le=MAX_WORKER_CONCURRENCY)
    status: WorkerStatus = Field(default=WorkerStatus.ACTIVE, strict=True)

    @property
    def is_terminal(self) -> bool:
        return self.status in (WorkerStatus.LOST, WorkerStatus.STOPPED)

    def transition(self, event: WorkerEvent) -> "WorkerSession":
        current = WorkerSession.model_validate(self)
        # StrEnum members can equal strings or another enum's members.
        if type(event) is not WorkerEvent:
            raise TypeError("Expected WorkerEvent.")
        try:
            status = _TRANSITIONS[current.status, event]
        except KeyError:
            raise InvalidStateTransition(current.status, event) from None
        return WorkerSession.model_validate({**current.model_dump(), "status": status})
