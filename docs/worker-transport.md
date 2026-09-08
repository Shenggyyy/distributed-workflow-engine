# Worker HTTP transport

M2.4b implements `worker/transport.py`, separate from API handlers and repositories.
The transport depends only on domain models and Python's HTTP client. Each call
opens a direct HTTP(S) connection, uses a finite socket timeout, bounds the response
to 1 MiB and closes the connection. It follows no redirects or environment proxies.
Configure an API origin without credentials, paths, queries or fragments. HTTPS
uses standard certificate verification. The existing API remains trusted/local.

The immutable Worker session identity is chosen once per process incarnation.
Registration retries send the same name, capacity and UUID. `ClaimPoll` is frozen:
retain it across uncertain delivery; create another request UUID only after the
previous poll is settled. No-work is a retained result, so a new poll needs a new
UUID. Completion retries retain the original `AttemptCompletion`, including result,
Attempt, session and token. Neither operation retries automatically or changes
identity inside the transport. Heartbeat and renewal retries are new server clock
observations; they preserve ownership but do not replay timestamps.

Success is accepted only after parsing strict models and checking expected session,
Run, request, Task/Attempt/lease relationships and states. Renewals must preserve
owner/token/acquisition and cannot regress renewal or expiry timestamps. Completion
must match the reported identity and result. These checks do not establish remaining
lease time or authorize handler execution: the control loop must do that separately.

Network/socket/protocol-disconnect failures raise `TransportUnavailable`: the
operation **may already have committed**. HTTP 408/429/502/503/504 are retryable
`WorkerAPIError`s; other rejections need caller handling. Invalid response models
or envelopes raise `ProtocolError`; they never imply rollback. Error messages omit
response bodies and exception details. A 500 storage invariant error is not silently
retried. The sender's timeout is per socket operation, not a hard execution deadline;
M2.4c must isolate transport from handler execution and use monotonic control timing.

The transport holds no durable client outbox. After process loss, an unknown claim
remains reserved until lease recovery (M4); a new process uses a new session. This
is consistent with at-least-once execution, not exactly-once business effects.

Validation includes a real local HTTP server (redirect refusal, proxy independence,
size limit and connection loss), plus PostgreSQL-backed API calls that deliberately
discard responses after successful registration, claim, renewal and completion.
Replays retain one Attempt and one completion receipt.

```console
uv run --locked pytest tests/test_worker_transport.py
uv run --locked pytest tests/integration/test_worker_transport.py --database-env-file .env.database-test
```

Next: M2.4c connects the handler contract to transport with an execution/control
lifecycle. This subtask introduces no background process, scheduling or migration.
