# Worker registration and heartbeat HTTP API

M2.1d exposes the existing [registration](worker-registration.md) and
[heartbeat](worker-heartbeat.md) transactions through the API process. Schema
head stays `0005`. No worker process, heartbeat loop, expiry scanner, task claim
or execution starts as a result of registration.

## Endpoints and bodies

| Method/path | Request body | Success |
| --- | --- | --- |
| PUT /worker-sessions/{session_id} | `{"worker_name":"worker-a","max_concurrency":2}` | 200 with the committed current session snapshot, on both first registration and replay. |
| POST /worker-sessions/{session_id}/heartbeat | `{}` (required) | 200 with the committed renewed session snapshot. |

The process chooses a new UUID on every start and reuses it when retrying that
registration. Names follow the existing ASCII Identifier contract (1–64
characters, letter first, then letters/digits/underscore/hyphen). Capacity is a
strict JSON integer from 1 to 2147483647; booleans, strings and floating-point
numbers are rejected. Capacity is declared metadata; slot enforcement is later.
Unknown body fields are rejected. Clients cannot supply timestamps, status,
heartbeat timeout or ownership through these requests.

Response shape (timestamps below illustrate the default 30-second policy):

```json
{
  "session": {
    "id": "d806a9ac-ec8c-4b38-bd50-19732f16e3f4",
    "worker_name": "worker-a",
    "max_concurrency": 2,
    "status": "ACTIVE"
  },
  "created_at": "2026-09-08T00:00:00Z",
  "last_heartbeat_at": "2026-09-08T00:00:00Z",
  "heartbeat_expires_at": "2026-09-08T00:00:30Z"
}
```

There is no `Location` header or Worker GET route in this milestone. OpenAPI at
`/openapi.json` and interactive `/docs` describe both typed operations. There is
no public expiry or arbitrary status mutation endpoint; expiry belongs to the
internal recovery process and currently has a per-session Python operation only.

## Replay, heartbeat and failure semantics

Registration identifies the operation by session UUID plus immutable name and
capacity. Same identity/fields return current persisted state without changing
any timestamps or reopening the session. Different fields return 409. A replay
after a heartbeat may return newer timestamps; a replay of an expired ACTIVE,
LOST or STOPPED session still returns 200 with that existing snapshot. This is
idempotency of the registration effect, not replay of a frozen HTTP receipt.

Heartbeat is a new liveness observation. The repository locks the row, checks
ACTIVE, samples the database clock, rejects backwards time, and requires
`observed_time < heartbeat_expires_at`. A valid observation advances heartbeat
metadata; the deadline cannot shrink. Equality with the deadline is expired.
Late heartbeat rejection writes nothing, including no implicit LOST transition.
Heartbeat retries may therefore succeed with a newer observation or fail because
the session expired while the response was lost.

Both endpoints reject any `Idempotency-Key` header with 400, including an empty
value. They do not use the Run API's durable key/receipt mechanism. Use the same
UUID and body to retry uncertain registration. On expired/inactive heartbeat,
the current session cannot resume; a restarted Worker needs a new UUID. Attempt
ownership cannot be transferred by registering that new UUID.

Each synchronous handler opens a short transaction, calls the repository and
validates the response inside it, then returns success only after COMMIT. A
rollback or deferred constraint failure never returns 200. A network failure
during/after commit can still leave the caller uncertain whether the transaction
committed; HTTP cannot provide exactly-once delivery. There are no hidden retries.
The returned snapshot is not a continuing guarantee of liveness: its deadline
may already have elapsed by response time after a long transaction/network delay.

### Error envelope

```json
{"error":{"code":"worker_expired","message":"Worker session heartbeat expired.","details":[]}}
```

| HTTP | Code | Meaning |
| --- | --- | --- |
| 400 | idempotency_not_supported | An Idempotency-Key header was supplied. |
| 404 | worker_not_found | Heartbeat found no visible session with this UUID. |
| 409 | worker_registration_conflict | Existing UUID has different immutable registration fields. |
| 409 | worker_inactive | Heartbeat targets LOST or STOPPED. |
| 409 | worker_expired | ACTIVE session's heartbeat deadline has elapsed. |
| 422 | invalid_request | Invalid path UUID/body, missing heartbeat body, extra fields or invalid capacity. |
| 500 | storage_error | Storage invariant/schema error, including a failed deferred COMMIT. |
| 503 | database_not_configured | The API has no configured database engine. |
| 503 | database_unavailable | Connection/pool/operational failure; a commit outcome may be uncertain. |
| 503 | worker_clock_regression | Database time precedes the last accepted heartbeat; no renewal occurs. |

Errors follow the [shared API envelope](api.md). Validation exposes field
locations/types, not input values; database details and stored values are not
included in responses or structured error logs. An initial registration in an
uncommitted transaction may still be invisible to a simultaneous heartbeat,
which can return 404; wait for registration success before heartbeating.

## Server configuration

`DWE_WORKER_HEARTBEAT_TIMEOUT_SECONDS` defaults to 30, with inclusive bounds
1–86400. Environment variables override an explicitly loaded dotenv file, then
defaults apply. The API factory receives one immutable Settings snapshot; requests
do not reload the environment. Restart/recreate the API to apply policy changes.
Compose forwards this value to the API container. A local API can load it with
`engine api --env-file .env`.

All API instances should use the same policy. During changes, a shorter policy
cannot reduce an already persisted deadline; a larger policy extends it on the
next valid heartbeat. The setting controls API repository construction; standalone
Python repository examples continue to use their constructor's 30-second default.
It is neither an HTTP timeout, a heartbeat send interval nor a task lease/timeout.
Worker heartbeat interval and scheduling jitter will be addressed with the actual
Worker loop. Clock jumps and false crash suspicions remain documented limitations.

## Local HTTP example

With Docker running, credentials initialized and the development database port
set as described in [local development](local-development.md), rebuild the API:

```powershell
docker compose up --build --wait --wait-timeout 120
docker compose exec api alembic upgrade head
docker compose exec api alembic current
docker compose exec api alembic check
```

Run the following together, before the default 30-second heartbeat deadline:

```powershell
$baseUrl = "http://127.0.0.1:8000"
$sessionId = [guid]::NewGuid().ToString()
$body = @{ worker_name = "worker-a"; max_concurrency = 2 } | ConvertTo-Json
$uri = "$baseUrl/worker-sessions/$sessionId"
Invoke-RestMethod -Method Put -Uri $uri -ContentType "application/json" -Body $body
Invoke-RestMethod -Method Put -Uri $uri -ContentType "application/json" -Body $body
Invoke-RestMethod -Method Post -Uri "$uri/heartbeat" -ContentType "application/json" -Body '{}'
```

The second PUT preserves the first response's timestamps. Heartbeat updates the
observation; a subsequent PUT returns that new snapshot without renewing again.
These requests exercise session control only. The API currently assumes trusted
clients on the local development boundary; UUIDs are identifiers, not credentials.
Authentication, admission quotas and retained-session cleanup are not implemented.

## Verification

```console
uv run --locked pytest tests/test_worker_api.py
uv run --locked pytest tests/integration/test_worker_http.py --database-env-file .env.database-test
uv run --locked python scripts/check_worker_api.py
```

The unit tests verify strict/no-database validation, required empty heartbeat body,
unsupported headers, missing configuration and OpenAPI. PostgreSQL tests verify
committed visibility, server policy injection, concurrent duplicates/conflicts,
expired/terminal/clock-regression errors, rollback after INSERT/deferred COMMIT
failures, sanitized corrupt-state errors and liveness during a registration lock
wait. Exact clock boundaries and heartbeat/expiry interleavings remain covered by
the repository tests.

The standalone smoke script uses real HTTP against the built API, checks
registration/replay/heartbeat and 409/404/422/400 responses, and runs in container
CI after migration. It accepts `--base-url` for a different port. Each invocation
creates one retained session; use disposable infrastructure for repeated runs.
No test here claims to execute tasks or inject physical Worker/network crashes.
