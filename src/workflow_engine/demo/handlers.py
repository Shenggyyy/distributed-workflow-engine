"""Trusted timed callable with real, bounded execution samples."""

import os
import time
from dataclasses import dataclass, field
from pathlib import Path

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
    """Linux boot + time namespace; unsupported hosts cannot claim comparability."""
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    namespace = os.readlink("/proc/self/ns/time")
    return f"linux:{boot}:{namespace}"


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
