"""Keep configuration tests independent of the developer's environment."""

import os

import pytest


@pytest.fixture(autouse=True)
def isolate_application_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove only application settings; pytest restores them after each test."""
    for name in tuple(os.environ):
        if name.upper().startswith("DWE_"):
            monkeypatch.delenv(name)
