"""Scheduler retries only transient failures and respects stop/once boundaries."""

from threading import Event
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError

from workflow_engine.scheduler.service import SchedulerService


def test_missing_database_configuration_is_safe(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from workflow_engine.config import Settings
    from workflow_engine.scheduler.entrypoint import run_scheduler

    assert run_scheduler(Settings(), uuid4(), once=True) == 1
    assert "scheduler_failed" in capsys.readouterr().err


class DriverError(Exception):
    def __init__(self, state: str) -> None:
        self.sqlstate = state
        super().__init__("private-driver-message")


@pytest.mark.parametrize(
    "state", ["40001", "40P01", "55P03", "57014", "08006", "57P01"]
)
def test_transient_failures_retry_fresh_pass(
    monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    engine = create_engine("sqlite://")
    service = SchedulerService(engine, poll_seconds=0.001)
    stop = Event()
    calls = 0

    def reconcile(run_id: object) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise DBAPIError("private-sql", {}, DriverError(state))
        stop.set()
        return 3

    monkeypatch.setattr(service, "reconcile", reconcile)
    try:
        assert service.run(uuid4(), stop) == 3
        assert calls == 2
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "once,state", [(True, "40001"), (False, "23514"), (False, "42P01")]
)
def test_once_and_nontransient_errors_exit(
    monkeypatch: pytest.MonkeyPatch, once: bool, state: str
) -> None:
    engine = create_engine("sqlite://")
    service = SchedulerService(engine)

    def reconcile(run_id: object) -> int:
        raise DBAPIError("private-sql", {}, DriverError(state))

    monkeypatch.setattr(service, "reconcile", reconcile)
    try:
        with pytest.raises(DBAPIError):
            service.run(uuid4(), Event(), once=once)
    finally:
        engine.dispose()


def test_stop_and_once(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = create_engine("sqlite://")
    service = SchedulerService(engine)
    monkeypatch.setattr(service, "reconcile", lambda run_id: 2)
    try:
        assert service.run(uuid4(), Event(), once=True) == 2
        stop = Event()
        stop.set()
        assert service.run(uuid4(), stop) == 0
    finally:
        engine.dispose()
