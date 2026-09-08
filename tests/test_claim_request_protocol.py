"""Freeze advisory lock compatibility across independently deployed processes."""

from uuid import UUID

from workflow_engine.repositories.claim_requests import (
    _CLAIM_LOCK_NAMESPACE,
    _claim_lock_key,
)


def test_claim_namespace_is_stable() -> None:
    assert _CLAIM_LOCK_NAMESPACE == 0x44574543


def test_lock_hash_wire_vectors() -> None:
    assert _claim_lock_key(UUID(int=0), UUID(int=0)) == -106232236
    assert (
        _claim_lock_key(
            UUID("00112233-4455-6677-8899-aabbccddeeff"),
            UUID("ffeeddcc-bbaa-9988-7766-554433221100"),
        )
        == -1043028150
    )
