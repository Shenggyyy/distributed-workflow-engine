"""One-shot HTTP operations; callers retain identity across uncertain delivery."""

import http.client
import json
import math
from typing import Protocol, Self
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from workflow_engine.domain.completion import AttemptCompletion, CompletionResult
from workflow_engine.domain.lease import AttemptLease
from workflow_engine.domain.runtime import (
    AttemptStatus,
    TaskAttempt,
    TaskRun,
    TaskStatus,
)
from workflow_engine.domain.worker import WorkerSession
from workflow_engine.domain.workflow import TaskDefinition

MAX_RESPONSE_BYTES = 1_048_576


class TransportUnavailable(RuntimeError):
    """Delivery may have committed; never replace request identity on this error."""


class ProtocolError(RuntimeError):
    """Invalid/unexpected response; do not admit execution or assume rollback."""


class WorkerAPIError(RuntimeError):
    def __init__(self, status: int, code: str) -> None:
        self.status = status
        self.code = code
        super().__init__(f"Worker API rejected operation (HTTP {status}).")

    @property
    def retryable(self) -> bool:
        return self.status in (408, 429, 502, 503, 504)


class Sender(Protocol):
    def __call__(self, method: str, path: str, body: bytes) -> tuple[int, bytes]: ...


class HTTPSender:
    """Direct HTTP, without redirects or proxies; bounded response size."""

    def __init__(self, base_url: str, *, timeout: float = 5.0) -> None:
        url = urlsplit(base_url)
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.path not in ("", "/")
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "Worker API URL must be an HTTP(S) origin without credentials."
            )
        if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("HTTP timeout must be positive and finite.")
        self._host, self._port = url.hostname, url.port
        self._https, self._timeout = url.scheme == "https", timeout

    def __call__(self, method: str, path: str, body: bytes) -> tuple[int, bytes]:
        connection_type = (
            http.client.HTTPSConnection if self._https else http.client.HTTPConnection
        )
        connection = connection_type(self._host, self._port, timeout=self._timeout)
        try:
            connection.request(method, path, body, {"Content-Type": "application/json"})
            response = connection.getresponse()
            content = response.read(MAX_RESPONSE_BYTES + 1)
            if len(content) > MAX_RESPONSE_BYTES:
                raise ProtocolError("Worker API response exceeded size limit.")
            return response.status, content
        except (OSError, http.client.HTTPException):
            raise TransportUnavailable("Worker API delivery is uncertain.") from None
        finally:
            connection.close()


class WireModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class ClaimPoll(WireModel):
    """Create once for a logical poll; reuse the object after a lost response."""

    run_id: UUID
    request_id: UUID


class WorkerObservation(WireModel):
    session: WorkerSession
    created_at: AwareDatetime
    last_heartbeat_at: AwareDatetime
    heartbeat_expires_at: AwareDatetime

    @model_validator(mode="after")
    def times(self) -> Self:
        if not self.created_at <= self.last_heartbeat_at < self.heartbeat_expires_at:
            raise ValueError("Invalid heartbeat observation.")
        return self


class ClaimedTask(WireModel):
    workflow_version_id: UUID
    task: TaskRun
    attempt: TaskAttempt
    lease: AttemptLease = Field(repr=False)
    definition: TaskDefinition = Field(repr=False)

    @model_validator(mode="after")
    def identities(self) -> Self:
        if (
            self.attempt.task_id != self.task.id
            or self.lease.attempt_id != self.attempt.id
            or self.definition.task_id != self.task.task_key
            or self.attempt.status is not AttemptStatus.RUNNING
            or self.task.status is not TaskStatus.RUNNING
        ):
            raise ValueError("Invalid claim identity or state.")
        return self


class ClaimObservation(WireModel):
    worker_session_id: UUID
    run_id: UUID
    request_id: UUID
    claim: ClaimedTask | None = Field(repr=False)


class LeaseObservation(WireModel):
    lease: AttemptLease = Field(repr=False)


class CompletionObservation(WireModel):
    attempt: TaskAttempt
    worker_session_id: UUID
    result: CompletionResult = Field(repr=False)
    accepted_at: AwareDatetime


class DiscoveryPage(WireModel):
    run_ids: tuple[UUID, ...] = Field(max_length=1)
    next_after: UUID | None

    @model_validator(mode="after")
    def cursor(self) -> Self:
        if self.next_after is not None and self.run_ids != (self.next_after,):
            raise ValueError("Invalid discovery continuation.")
        return self


class WorkerTransport:
    """No implicit retries, sleeps, execution or session rotation inside transport."""

    def __init__(self, session: WorkerSession, sender: Sender) -> None:
        self._session = WorkerSession.model_validate(session)
        self._sender = sender

    @property
    def session(self) -> WorkerSession:
        return self._session

    def _request[M: BaseModel](
        self,
        method: str,
        suffix: str,
        body: object,
        model: type[M],
        *,
        root: bool = False,
    ) -> M:
        encoded = json.dumps(body, separators=(",", ":")).encode()
        path = suffix if root else f"/worker-sessions/{self.session.id}" + suffix
        status, raw = self._sender(method, path, b"" if method == "GET" else encoded)
        if status != 200:
            try:
                value = json.loads(raw)
                code = value["error"]["code"]
                if not isinstance(code, str):
                    raise ValueError()
            except (ValueError, TypeError, KeyError):
                if status in (408, 429, 502, 503, 504):
                    raise WorkerAPIError(status, "http_unavailable") from None
                raise ProtocolError("Worker API error envelope is invalid.") from None
            raise WorkerAPIError(status, code)
        try:
            return model.model_validate_json(raw)
        except ValueError:
            raise ProtocolError("Worker API success payload is invalid.") from None

    def _worker(self, method: str, suffix: str, body: object) -> WorkerObservation:
        value = self._request(method, suffix, body, WorkerObservation)
        if (
            value.session.id != self.session.id
            or value.session.worker_name != self.session.worker_name
            or value.session.max_concurrency != self.session.max_concurrency
        ):
            raise ProtocolError("Worker API returned a different session.")
        return value

    def register(self) -> WorkerObservation:
        return self._worker(
            "PUT",
            "",
            {
                "worker_name": self.session.worker_name,
                "max_concurrency": self.session.max_concurrency,
            },
        )

    def heartbeat(self) -> WorkerObservation:
        return self._worker("POST", "/heartbeat", {})

    def discover(self, after: UUID | None = None) -> DiscoveryPage:
        if after is not None and not isinstance(after, UUID):
            raise TypeError("Discovery cursor must be a UUID.")
        path = "/runs?limit=1&ready_only=true"
        if after is not None:
            path += f"&after={after}"
        page = self._request("GET", path, None, DiscoveryPage, root=True)
        if after is not None and page.run_ids and page.run_ids[0] <= after:
            raise ProtocolError("Discovery did not advance past its cursor.")
        return page

    def claim(self, poll: ClaimPoll) -> ClaimObservation:
        poll = ClaimPoll.model_validate(poll)
        value = self._request(
            "POST", "/claims", poll.model_dump(mode="json"), ClaimObservation
        )
        if (value.worker_session_id, value.run_id, value.request_id) != (
            self.session.id,
            poll.run_id,
            poll.request_id,
        ) or (
            value.claim is not None
            and (
                value.claim.task.run_id != poll.run_id
                or value.claim.lease.worker_session_id != self.session.id
            )
        ):
            raise ProtocolError("Worker API returned a different claim identity.")
        return value

    def renew(self, lease: AttemptLease) -> AttemptLease:
        lease = AttemptLease.model_validate(lease)
        if lease.worker_session_id != self.session.id:
            raise ValueError("Cannot renew another session's lease.")
        value = self._request(
            "POST",
            f"/attempts/{lease.attempt_id}/renew",
            {"lease_token": str(lease.lease_token)},
            LeaseObservation,
        ).lease
        if (
            (
                value.attempt_id,
                value.worker_session_id,
                value.lease_token,
                value.acquired_at,
            )
            != (
                lease.attempt_id,
                lease.worker_session_id,
                lease.lease_token,
                lease.acquired_at,
            )
            or value.last_renewed_at < lease.last_renewed_at
            or value.lease_expires_at < lease.lease_expires_at
        ):
            raise ProtocolError("Worker API returned invalid renewed ownership.")
        return value

    def complete(self, report: AttemptCompletion) -> CompletionObservation:
        report = AttemptCompletion.model_validate(report)
        if report.worker_session_id != self.session.id:
            raise ValueError("Cannot complete another session's Attempt.")
        value = self._request(
            "POST",
            f"/attempts/{report.attempt_id}/complete",
            {
                "lease_token": str(report.lease_token),
                "result": report.result.model_dump(mode="json"),
            },
            CompletionObservation,
        )
        if (
            value.attempt.id != report.attempt_id
            or value.worker_session_id != report.worker_session_id
            or value.result != report.result
            or value.attempt.status.value != report.result.outcome.value
        ):
            raise ProtocolError("Worker API returned a different completion.")
        return value
