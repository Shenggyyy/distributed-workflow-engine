"""Fault injection is restricted to named, labelled demo Worker containers."""

from uuid import uuid4

import pytest
from scripts.demo import MARKER, verify_worker, worker_name


@pytest.mark.parametrize(
    "changed", ["name", "project", "service", "run", "marker", "id"]
)
def test_fault_target_requires_all_identity_checks(changed: str) -> None:
    run = uuid4()
    labels = {
        "com.docker.compose.project": "dwe-demo",
        "com.docker.compose.service": "worker",
        MARKER: "1",
        "io.dwe.run": str(run),
    }
    info = {
        "Name": "/" + worker_name(run, "a"),
        "Id": "a" * 64,
        "Config": {"Labels": labels},
    }
    assert verify_worker(info, run, "a") == "a" * 64
    if changed == "name":
        info["Name"] = "/user-worker"
    elif changed == "id":
        info["Id"] = "bad"
    else:
        field = {
            "project": "com.docker.compose.project",
            "service": "com.docker.compose.service",
            "run": "io.dwe.run",
            "marker": MARKER,
        }[changed]
        labels[field] = "other"
    with pytest.raises(ValueError):
        verify_worker(info, run, "a")
