"""Pure attempt lease snapshots; callers supply authoritative state and time."""

from datetime import UTC, datetime, timedelta
from typing import Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    field_validator,
    model_validator,
)

MAX_LEASE_SECONDS = 86400


class LeaseOwnershipError(ValueError):
    """The supplied attempt/session/token does not match this lease."""


class LeaseExpiredError(ValueError):
    """The observation is at or beyond this lease's exclusive deadline."""


class LeaseClockRegressionError(ValueError):
    """The observation precedes this lease's last accepted observation."""


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("Lease time must be an aware datetime.")
    try:
        return value.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise ValueError("Lease time is outside the supported UTC range.") from exc


class AttemptLease(BaseModel):
    """Lease metadata only, not proof of current RUNNING state or persistence."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    attempt_id: UUID
    worker_session_id: UUID
    lease_token: UUID
    acquired_at: AwareDatetime
    last_renewed_at: AwareDatetime
    lease_expires_at: AwareDatetime

    @field_validator("acquired_at", "last_renewed_at", "lease_expires_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_time_order(self) -> Self:
        if not self.acquired_at <= self.last_renewed_at < self.lease_expires_at:
            raise ValueError("Lease times must satisfy acquired <= renewed < expires.")
        return self

    def _checked_observation(
        self,
        *,
        attempt_id: UUID,
        worker_session_id: UUID,
        lease_token: UUID,
        observed_at: datetime,
    ) -> tuple["AttemptLease", datetime]:
        current = AttemptLease.model_validate(self)
        if any(
            type(value) is not UUID
            for value in (attempt_id, worker_session_id, lease_token)
        ):
            raise TypeError("Lease ownership fields must be UUID instances.")
        if (attempt_id, worker_session_id, lease_token) != (
            current.attempt_id,
            current.worker_session_id,
            current.lease_token,
        ):
            raise LeaseOwnershipError("Attempt lease ownership does not match.")
        observed = _utc(observed_at)
        if observed < current.last_renewed_at:
            raise LeaseClockRegressionError("Attempt lease clock moved backwards.")
        if observed >= current.lease_expires_at:
            raise LeaseExpiredError("Attempt lease expired.")
        return current, observed

    def require_valid_owner(
        self,
        *,
        attempt_id: UUID,
        worker_session_id: UUID,
        lease_token: UUID,
        observed_at: datetime,
    ) -> None:
        """Check this snapshot only; transactions must check current attempt state."""
        self._checked_observation(
            attempt_id=attempt_id,
            worker_session_id=worker_session_id,
            lease_token=lease_token,
            observed_at=observed_at,
        )

    def renew(
        self,
        *,
        attempt_id: UUID,
        worker_session_id: UUID,
        lease_token: UUID,
        observed_at: datetime,
        lease_seconds: int,
    ) -> "AttemptLease":
        """Return a monotonic renewal without reviving expiry or rotating identity."""
        if (
            type(lease_seconds) is not int
            or not 1 <= lease_seconds <= MAX_LEASE_SECONDS
        ):
            raise ValueError("Lease duration must be an integer from 1 through 86400.")
        current, observed = self._checked_observation(
            attempt_id=attempt_id,
            worker_session_id=worker_session_id,
            lease_token=lease_token,
            observed_at=observed_at,
        )
        try:
            deadline = max(
                current.lease_expires_at, observed + timedelta(seconds=lease_seconds)
            )
        except OverflowError as exc:
            raise ValueError(
                "Lease deadline is outside the supported UTC range."
            ) from exc
        return AttemptLease.model_validate(
            {
                **current.model_dump(),
                "last_renewed_at": observed,
                "lease_expires_at": deadline,
            }
        )
