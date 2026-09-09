"""Exact identities for the dedicated local demonstration containers."""

from typing import Any
from uuid import UUID

PROJECT = "dwe-demo"
MARKER = "io.dwe.demo"


def worker_name(run_id: UUID, slot: str) -> str:
    if slot not in ("a", "b"):
        raise ValueError("Unknown demo Worker slot.")
    return f"dwe-demo-{run_id.hex}-{slot}"


def verify_worker(info: dict[str, Any], run_id: UUID, slot: str) -> str:
    labels = info.get("Config", {}).get("Labels", {})
    expected = {
        "com.docker.compose.project": PROJECT,
        "com.docker.compose.service": "worker",
        MARKER: "1",
        "io.dwe.run": str(run_id),
    }
    if info.get("Name") != "/" + worker_name(run_id, slot) or any(
        labels.get(k) != v for k, v in expected.items()
    ):
        raise ValueError("Refusing to operate on an unrecognized demo Worker.")
    identity = info.get("Id", "")
    if len(identity) != 64 or any(c not in "0123456789abcdef" for c in identity):
        raise ValueError("Invalid container identity.")
    return str(identity)
