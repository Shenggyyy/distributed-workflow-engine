"""Cross-container comparisons require equal kernel and monotonic origins."""

from unittest.mock import patch
from uuid import uuid4

import pytest

from workflow_engine.demo.handlers import clock_domain


def domain(boot: str, offset: str, namespace: str) -> str:
    with (
        patch(
            "workflow_engine.demo.handlers.Path.read_text", side_effect=[boot, offset]
        ),
        patch("workflow_engine.demo.handlers.os.readlink", return_value=namespace),
    ):
        return clock_domain()


def test_different_namespaces_compare_only_with_equal_offsets_and_boot() -> None:
    boot = str(uuid4())
    first = domain(boot, "monotonic 0 0\nboottime 0 0", "time:[1]")
    assert first == domain(boot, "monotonic 0 0", "time:[2]")
    assert first != domain(boot, "monotonic 2 0", "time:[1]")
    assert first != domain(str(uuid4()), "monotonic 0 0", "time:[1]")


@pytest.mark.parametrize(
    "offset",
    ["", "monotonic 0", "monotonic 0 1000000000", "monotonic 0 0\nmonotonic 1 0"],
)
def test_invalid_clock_evidence_fails_closed(offset: str) -> None:
    with pytest.raises(ValueError):
        domain(str(uuid4()), offset, "time:[1]")
