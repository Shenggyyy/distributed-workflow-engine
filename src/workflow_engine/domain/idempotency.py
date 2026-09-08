"""Exact, case-sensitive keys for the single-tenant run-creation operation."""

import re

_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")


class InvalidIdempotencyKeyError(ValueError):
    """A key violates the supported format; the error never echoes the key."""


def validate_idempotency_key(value: object) -> str:
    """Validate without trimming, case folding, coercion or Unicode normalization."""
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or _KEY.fullmatch(value) is None
    ):
        raise InvalidIdempotencyKeyError(
            "Idempotency key must be 1-128 ASCII characters: start with a letter "
            "or digit, followed by letters, digits, dot, underscore, colon or hyphen."
        )
    return value
