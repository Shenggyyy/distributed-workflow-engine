"""Lease ownership, exclusive deadlines and immutable renewal boundaries."""

from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from workflow_engine.domain.lease import (
    AttemptLease,
    LeaseClockRegressionError,
    LeaseExpiredError,
    LeaseOwnershipError,
)

START = datetime(2026, 1, 1, tzinfo=UTC)


def lease() -> AttemptLease:
    return AttemptLease(
        attempt_id=uuid4(),
        worker_session_id=uuid4(),
        lease_token=uuid4(),
        acquired_at=START,
        last_renewed_at=START,
        lease_expires_at=START + timedelta(seconds=30),
    )


def owner(value: AttemptLease) -> dict[str, UUID]:
    return {
        "attempt_id": value.attempt_id,
        "worker_session_id": value.worker_session_id,
        "lease_token": value.lease_token,
    }


@pytest.mark.parametrize("seconds", [0, 10, 29.999999])
def test_live_owner_and_monotonic_renewal(seconds: float) -> None:
    original = lease()
    now = START + timedelta(seconds=seconds)
    original.require_valid_owner(**owner(original), observed_at=now)
    renewed = original.renew(**owner(original), observed_at=now, lease_seconds=30)
    assert renewed is not original
    assert owner(renewed) == owner(original)
    assert renewed.acquired_at == original.acquired_at
    assert renewed.last_renewed_at == now
    assert renewed.lease_expires_at == now + timedelta(seconds=30)
    assert original.last_renewed_at == START
    assert renewed.renew(**owner(renewed), observed_at=now, lease_seconds=30) == renewed


@pytest.mark.parametrize("seconds", [30, 30.000001, 60])
def test_expired_owner_cannot_renew_even_with_larger_policy(seconds: float) -> None:
    original = lease()
    now = START + timedelta(seconds=seconds)
    with pytest.raises(LeaseExpiredError):
        original.require_valid_owner(**owner(original), observed_at=now)
    with pytest.raises(LeaseExpiredError):
        original.renew(**owner(original), observed_at=now, lease_seconds=86400)
    assert original.lease_expires_at == START + timedelta(seconds=30)


@pytest.mark.parametrize("field", ["attempt_id", "worker_session_id", "lease_token"])
def test_wrong_owner_rejected_without_exposing_values(field: str) -> None:
    original = lease()
    supplied = {**owner(original), field: uuid4()}
    with pytest.raises(LeaseOwnershipError) as error:
        original.renew(**supplied, observed_at=START, lease_seconds=30)
    assert all(str(value) not in str(error.value) for value in supplied.values())
    with pytest.raises(LeaseOwnershipError):
        original.require_valid_owner(**supplied, observed_at=START)


@pytest.mark.parametrize("seconds", [-1, 9.999999])
def test_backward_clock_cannot_authorize_or_renew(seconds: float) -> None:
    original = lease()
    renewed = original.renew(
        **owner(original), observed_at=START + timedelta(seconds=10), lease_seconds=30
    )
    now = START + timedelta(seconds=seconds)
    with pytest.raises(LeaseClockRegressionError):
        renewed.require_valid_owner(**owner(renewed), observed_at=now)
    with pytest.raises(LeaseClockRegressionError):
        renewed.renew(**owner(renewed), observed_at=now, lease_seconds=30)


@pytest.mark.parametrize("duration", [1, 86400])
def test_duration_boundaries_and_shorter_policy(duration: int) -> None:
    original = lease()
    now = START + timedelta(seconds=10)
    renewed = original.renew(**owner(original), observed_at=now, lease_seconds=duration)
    assert renewed.lease_expires_at == max(
        original.lease_expires_at, now + timedelta(seconds=duration)
    )
    assert renewed.last_renewed_at == now


@pytest.mark.parametrize("duration", [True, False, 0, -1, 86401, "30", 30.0, None])
def test_invalid_duration_is_not_coerced(duration: Any) -> None:
    original = lease()
    with pytest.raises(ValueError, match="Lease duration"):
        original.renew(**owner(original), observed_at=START, lease_seconds=duration)


@pytest.mark.parametrize("now", [START.replace(tzinfo=None), "2026-01-01", 0, None])
def test_observation_requires_aware_datetime(now: Any) -> None:
    original = lease()
    with pytest.raises(ValueError, match="aware datetime"):
        original.require_valid_owner(**owner(original), observed_at=now)


@pytest.mark.parametrize("field", ["attempt_id", "worker_session_id", "lease_token"])
def test_owner_checks_do_not_coerce_uuid_strings(field: str) -> None:
    original = lease()
    supplied: dict[str, Any] = owner(original)
    supplied[field] = str(supplied[field])
    with pytest.raises(TypeError):
        original.require_valid_owner(**supplied, observed_at=START)


@pytest.mark.parametrize("field", list(AttemptLease.model_fields))
def test_required_frozen_fields_and_strict_python_values(field: str) -> None:
    original = lease()
    data = original.model_dump()
    data.pop(field)
    with pytest.raises(ValidationError):
        AttemptLease.model_validate(data)
    with pytest.raises(ValidationError):
        original.__setattr__(field, original.model_dump()[field])
    with pytest.raises(ValidationError):
        AttemptLease.model_validate(
            {**original.model_dump(), field: str(original.model_dump()[field])}
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"acquired_at": START + timedelta(seconds=1)},
        {"last_renewed_at": START - timedelta(seconds=1)},
        {"lease_expires_at": START},
        {"lease_expires_at": START - timedelta(seconds=1)},
        {"acquired_at": START.replace(tzinfo=None)},
        {"last_renewed_at": START.replace(tzinfo=None)},
        {"lease_expires_at": START.replace(tzinfo=None)},
        {"status": "RUNNING"},
    ],
)
def test_invalid_snapshot_is_rejected(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        AttemptLease.model_validate({**lease().model_dump(), **changes})


def test_offset_times_are_normalized_and_json_round_trips() -> None:
    original = lease()
    offset = timezone(timedelta(hours=8))
    restored = AttemptLease.model_validate(
        {
            **original.model_dump(),
            "acquired_at": START.astimezone(offset),
            "last_renewed_at": START.astimezone(offset),
            "lease_expires_at": original.lease_expires_at.astimezone(offset),
        }
    )
    assert restored == original
    assert restored.acquired_at.tzinfo is UTC
    assert AttemptLease.model_validate_json(restored.model_dump_json()) == original
    renewed = restored.renew(
        **owner(restored),
        observed_at=(START + timedelta(seconds=10)).astimezone(offset),
        lease_seconds=30,
    )
    assert renewed.last_renewed_at == START + timedelta(seconds=10)
    assert renewed.last_renewed_at.tzinfo is UTC


def test_deadline_overflow_is_explicit() -> None:
    end = datetime.max.replace(tzinfo=UTC)
    original = AttemptLease.model_validate(
        {
            **lease().model_dump(),
            "acquired_at": end - timedelta(seconds=2),
            "last_renewed_at": end - timedelta(seconds=2),
            "lease_expires_at": end,
        }
    )
    with pytest.raises(ValueError, match="supported UTC range"):
        original.renew(
            **owner(original), observed_at=end - timedelta(seconds=1), lease_seconds=30
        )


def test_operations_revalidate_bypassed_snapshots() -> None:
    original = lease()
    forged = original.model_copy(update={"lease_expires_at": START})
    with pytest.raises(ValidationError):
        forged.require_valid_owner(**owner(original), observed_at=START)
    with pytest.raises(ValidationError):
        forged.renew(**owner(original), observed_at=START, lease_seconds=30)
