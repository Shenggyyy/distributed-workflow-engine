"""Trusted timed callable with real, bounded execution samples."""

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID

from workflow_engine.config import Settings
from workflow_engine.database import database_engine
from workflow_engine.demo.observations import append, start
from workflow_engine.domain.completion import CompletionOutcome, CompletionResult
from workflow_engine.worker.handlers import (
    HandlerContext,
    HandlerRegistration,
    HandlerRegistry,
)


def clock_domain() -> str:
    """Same kernel and frozen MONOTONIC offset imply comparable raw readings."""
    boot = UUID(Path("/proc/sys/kernel/random/boot_id").read_text().strip())
    if os.readlink("/proc/self/ns/time") != os.readlink(
        "/proc/self/ns/time_for_children"
    ):
        raise ValueError("Unsupported pending time namespace transition.")
    rows = [
        line.split()
        for line in Path("/proc/self/timens_offsets").read_text().splitlines()
    ]
    offsets = [row for row in rows if row and row[0] == "monotonic"]
    if len(offsets) != 1 or len(offsets[0]) != 3:
        raise ValueError("No unambiguous Linux monotonic offset.")
    seconds, nanos = int(offsets[0][1]), int(offsets[0][2])
    if not 0 <= nanos < 1_000_000_000:
        raise ValueError("Invalid Linux monotonic offset.")
    return f"linux:{boot}:monotonic-offset:{seconds}:{nanos}"


@dataclass(frozen=True)
class ObservedHandler:
    settings: Settings = field(repr=False)
    seconds: float = 8

    def __post_init__(self) -> None:
        if not 0 < self.seconds <= 60:
            raise ValueError("Demo duration must be in (0, 60] seconds.")

    def __call__(self, context: HandlerContext) -> CompletionResult:
        # Both samples occur INSIDE the callable. Receipt latency is included in
        # this observed lifetime; neither sample claims the exact Python boundary.
        domain = clock_domain()
        first = time.monotonic_ns()
        with database_engine(self.settings) as engine:
            invocation = start(engine, context, domain, first)
            deadline = first + int(self.seconds * 1_000_000_000)
            while time.monotonic_ns() < deadline:
                time.sleep(0.5)
                append(engine, invocation, "PULSE", time.monotonic_ns())
            # No finally: a killed process has an unknown end, not a FINISH.
            append(engine, invocation, "FINISH", time.monotonic_ns())
        return CompletionResult(outcome=CompletionOutcome.SUCCEEDED)


def registry(settings: Settings) -> HandlerRegistry:
    return HandlerRegistry(
        [
            HandlerRegistration("demo.observe", ObservedHandler(settings, 8)),
            HandlerRegistration("demo.recover", ObservedHandler(settings, 20)),
            HandlerRegistration("demo.join", ObservedHandler(settings, 2)),
        ]
    )
