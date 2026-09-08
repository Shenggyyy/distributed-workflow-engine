"""Delivery uncertainty, response admission, redaction and HTTP origin rules."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from uuid import uuid4

import pytest

from workflow_engine.domain.worker import WorkerSession
from workflow_engine.worker.transport import (
    MAX_RESPONSE_BYTES,
    ClaimPoll,
    HTTPSender,
    ProtocolError,
    TransportUnavailable,
    WorkerAPIError,
    WorkerTransport,
)


@pytest.fixture
def session() -> WorkerSession:
    return WorkerSession(id=uuid4(), worker_name="test", max_concurrency=1)


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com",
        "http://",
        "http://user:secret@example.com",
        "http://example.com/path",
        "http://example.com?token=x",
        "http://example.com#x",
    ],
)
def test_invalid_origin(url: str) -> None:
    with pytest.raises(ValueError):
        HTTPSender(url)


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True])
def test_invalid_timeout(timeout: float) -> None:
    with pytest.raises(ValueError):
        HTTPSender("http://localhost:8000", timeout=timeout)


def test_lost_claim_response_preserves_identity(session: WorkerSession) -> None:
    calls: list[tuple[str, str, bytes]] = []
    poll = ClaimPoll(run_id=uuid4(), request_id=uuid4())

    def send(method: str, path: str, body: bytes) -> tuple[int, bytes]:
        calls.append((method, path, body))
        if len(calls) == 1:
            raise TransportUnavailable("Uncertain delivery.")
        return 200, json.dumps(
            {
                **poll.model_dump(mode="json"),
                "worker_session_id": str(session.id),
                "claim": None,
            }
        ).encode()

    transport = WorkerTransport(session, send)
    with pytest.raises(TransportUnavailable):
        transport.claim(poll)
    assert transport.claim(poll).claim is None
    assert calls[0] == calls[1]


@pytest.mark.parametrize("status", [408, 429, 502, 503, 504])
def test_retryable_proxy_errors(session: WorkerSession, status: int) -> None:
    transport = WorkerTransport(session, lambda *args: (status, b"private-proxy-error"))
    with pytest.raises(WorkerAPIError) as error:
        transport.heartbeat()
    assert error.value.retryable
    assert "private" not in str(error.value)


@pytest.mark.parametrize("status", [400, 404, 409, 422, 500])
def test_rejections_never_implicitly_retry(session: WorkerSession, status: int) -> None:
    calls: list[bytes] = []

    def send(method: str, path: str, body: bytes) -> tuple[int, bytes]:
        calls.append(body)
        return status, b'{"error":{"code":"lease_expired","message":"private"}}'

    with pytest.raises(WorkerAPIError) as error:
        WorkerTransport(session, send).heartbeat()
    assert not error.value.retryable and len(calls) == 1
    assert error.value.code == "lease_expired" and "private" not in str(error.value)


@pytest.mark.parametrize("raw", [b"private", b"null", b"{}", b"[]"])
def test_invalid_success_is_never_admitted(session: WorkerSession, raw: bytes) -> None:
    with pytest.raises(ProtocolError, match="success payload"):
        WorkerTransport(session, lambda *args: (200, raw)).heartbeat()


@pytest.mark.parametrize("field", ["request_id", "run_id", "worker_session_id"])
def test_mismatched_claim_envelope(session: WorkerSession, field: str) -> None:
    poll = ClaimPoll(run_id=uuid4(), request_id=uuid4())
    response = {
        **poll.model_dump(mode="json"),
        "worker_session_id": str(session.id),
        "claim": None,
        field: str(uuid4()),
    }
    with pytest.raises(ProtocolError, match="identity"):
        WorkerTransport(
            session, lambda *args: (200, json.dumps(response).encode())
        ).claim(poll)


def test_redirect_is_not_success(session: WorkerSession) -> None:
    with pytest.raises(ProtocolError):
        WorkerTransport(session, lambda *args: (307, b"redirect")).register()


@pytest.mark.parametrize("scenario", ["success", "redirect", "oversized", "disconnect"])
def test_real_http_sender(scenario: str, monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[tuple[str, bytes]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            requests.append(
                (self.path, self.rfile.read(int(self.headers["Content-Length"])))
            )
            if scenario == "disconnect":
                self.close_connection = True
                return
            self.send_response(307 if scenario == "redirect" else 200)
            if scenario == "redirect":
                self.send_header("Location", "/another-origin")
            self.end_headers()
            self.wfile.write(
                b"x" * (MAX_RESPONSE_BYTES + 1) if scenario == "oversized" else b"{}"
            )

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    monkeypatch.setenv("HTTP_PROXY", "http://invalid-proxy:1")
    try:
        sender = HTTPSender(f"http://127.0.0.1:{server.server_port}")
        if scenario in ("oversized", "disconnect"):
            with pytest.raises(
                ProtocolError if scenario == "oversized" else TransportUnavailable
            ):
                sender("POST", "/operation", b"{}")
        else:
            assert sender("POST", "/operation", b"{}") == (
                307 if scenario == "redirect" else 200,
                b"{}",
            )
        assert requests == [("/operation", b"{}")]
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
