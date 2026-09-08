"""Pure validation tests for exact request identities."""

import pytest

from workflow_engine.domain.idempotency import (
    InvalidIdempotencyKeyError,
    validate_idempotency_key,
)


@pytest.mark.parametrize("key", ["a", "0", "ABC_123.:-", "a" * 128])
def test_valid_keys_are_preserved(key: str) -> None:
    assert validate_idempotency_key(key) == key


@pytest.mark.parametrize(
    "key",
    [
        "",
        " ",
        " a",
        "a ",
        "a b",
        "a\n",
        "a\r\n",
        "a/b",
        "-a",
        "_a",
        "\u4e2d\u6587",
        "a" * 129,
        None,
        123,
        b"abc",
        True,
    ],
)
def test_invalid_keys_are_rejected_without_echoing_input(key: object) -> None:
    with pytest.raises(InvalidIdempotencyKeyError) as error:
        validate_idempotency_key(key)
    assert str(error.value).startswith(
        "Idempotency key must be 1-128 ASCII characters:"
    )
