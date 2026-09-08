# Run creation and query HTTP API

M1.9b exposes the existing keyed creation protocol and Run queries. It requires
schema revision `0004` or later; current head `0005` also stores Worker sessions.
The HTTP milestone introduced no migration. OpenAPI at
`/openapi.json` and Swagger UI at `/docs` describe these routes.

| Method/path | Success | Contract |
| --- | --- | --- |
| POST /runs | 201 + receipt + Location | Create a version-pinned run or replay its original receipt. |
| GET /runs/{run_id} | 200 + run metadata | Read the currently persisted run status. |
| GET /runs/{run_id}/tasks | 200 + run and tasks | Read both from one SQL statement snapshot. |

## Creation and replay

Send `Content-Type: application/json`, exactly one `Idempotency-Key` header,
and a body with one field:

```json
{"workflow_version_id":"1e81ec81-a01f-4212-a7f5-0e4713d7dd61"}
```

The UUID must refer to a published immutable version. Extra body fields are
rejected. There is no implicit latest-version resolution or unkeyed HTTP mode.

The key is case-sensitive ASCII, 1–128 characters, with an alphanumeric first
character and subsequent characters from letters, digits, `.`, `_`, `:`, `-`.
It is not trimmed or normalized. Missing, malformed, repeated headers (even
identical values), and comma-combined values are rejected with 422. Header names
are case-insensitive; key values are case-sensitive.

Keys share the current single-tenant Run-creation namespace. They do not expire.
Choose a fresh key for each intended new run; persist and reuse that key with the
same version when retrying. Workflow publication via `POST /workflows` remains
unkeyed and rejects this header with 400.

Successful first creation and same-key/version replay both return:

```http
HTTP/1.1 201 Created
Location: /runs/180d861c-6bc8-4f93-bcfe-4d8b5f8c3713
Content-Type: application/json

{"run_id":"180d861c-6bc8-4f93-bcfe-4d8b5f8c3713","workflow_version_id":"1e81ec81-a01f-4212-a7f5-0e4713d7dd61"}
```

The example UUIDs are illustrative. The replay deliberately reproduces the
original creation result, including 201 and Location; it does not create another
run. The receipt contains no mutable execution state or timestamp. Query the
resource to inspect its persisted status. Reusing a key with a different version
returns 409, including when that other version does not exist.

The API validates the receipt inside the transaction, exits the transaction
successfully, and only then sets Location and returns success. Key binding, run,
and every DAG task commit atomically. Initialization or deferred COMMIT failure
produces an error without Location. If the database committed but the response
was lost, retry the same key/version to recover the receipt. Neither the API nor
repository automatically retries database failures or guesses uncertain outcomes.

A fresh run is RUNNING, its root tasks are READY, other tasks are PENDING, and no
attempts exist. RUNNING here means the run was activated; scheduling and handler
execution are not implemented yet. Receipt replay never resets task state.

See [the storage idempotency protocol](run-idempotency.md) for concurrent key
reservation, rollback takeover, retention, and uncertain-commit semantics.
Run-creation deduplication does not guarantee exactly-once task side effects.

## Query responses and consistency

The metadata route returns `id`, `workflow_version_id`, `status`, and
`created_at` (an ISO 8601 timestamp with timezone). The tasks route returns:

```json
{
  "run": {
    "id": "180d861c-6bc8-4f93-bcfe-4d8b5f8c3713",
    "workflow_version_id": "1e81ec81-a01f-4212-a7f5-0e4713d7dd61",
    "status": "RUNNING",
    "created_at": "2026-09-08T00:00:00Z"
  },
  "tasks": [
    {
      "id": "d0c61c09-e3be-473f-a9b3-4b72b1b74973",
      "run_id": "180d861c-6bc8-4f93-bcfe-4d8b5f8c3713",
      "task_key": "A",
      "status": "READY",
      "created_at": "2026-09-08T00:00:00Z"
    }
  ]
}
```

This illustrative response uses a one-node workflow. The `run` wrapper is
intentional: clients can display run/task state from one statement snapshot.
Separate HTTP calls can observe different committed states, and a response can
become stale immediately. The read performs no aggregation or lifecycle mutation.

Tasks use deterministic case-sensitive ASCII key order, not execution order.
The supported maximum is 1000 tasks; corrupt oversized storage raises a fixed
500 error rather than silently truncating the result. An existing run with zero
tasks returns an empty array for diagnosis; supported creation always initializes
all tasks atomically. Attempt history, pagination and listing all runs are outside
this API. See [query storage semantics](run-queries.md) for consistency boundaries.

## Errors and transaction boundaries

Errors use the shared envelope:

```json
{"error":{"code":"run_not_found","message":"Run was not found.","details":[]}}
```

| HTTP status | Code | Meaning |
| --- | --- | --- |
| 404 | version_not_found | First creation references an absent version. |
| 404 | run_not_found | A queried Run UUID is absent. |
| 409 | idempotency_conflict | Key is already bound to another version. |
| 422 | invalid_request | Invalid/missing body, UUID or key, or extra body fields. |
| 500 | storage_error | Invalid stored runtime/definition data or schema/invariant failure. |
| 503 | database_not_configured | No database credentials were configured. |
| 503 | database_unavailable | Operational, connection or pool availability failure. |

Validation details contain locations and error types, not input values.
Application storage errors and structured logs omit raw SQL, driver messages,
request values and keys. Framework routing errors retain the existing FastAPI
contract. A 503 is not proof of rollback and does not promise a successful retry.

Synchronous database work runs in the server thread pool. The async dependency
only retrieves the engine reference; it performs no connection checkout.
A blocked key reservation therefore does not itself block the event loop or
liveness route. Connection/statement/pool timeouts remain the existing database
limits, not workflow or task execution timeouts. The application has no new
admission-control or complete backpressure mechanism in this milestone.

## Run locally

From the repository root, with Docker Desktop running and local secrets already
initialized as described in [local development](local-development.md):

```powershell
$env:DWE_POSTGRES_PUBLISHED_PORT = "15432"
docker compose up --build --wait --wait-timeout 120
docker compose exec api alembic upgrade head
$definition = Get-Content examples/diamond.json -Raw
$published = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/workflows -ContentType "application/json" -Body $definition
$body = @{ workflow_version_id = $published.id } | ConvertTo-Json
$headers = @{ "Idempotency-Key" = [guid]::NewGuid().ToString() }
$receipt = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/runs -Headers $headers -ContentType "application/json" -Body $body
$replayed = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/runs -Headers $headers -ContentType "application/json" -Body $body
$receipt.run_id -eq $replayed.run_id
Invoke-RestMethod "http://127.0.0.1:8000/runs/$($receipt.run_id)"
Invoke-RestMethod "http://127.0.0.1:8000/runs/$($receipt.run_id)/tasks" | ConvertTo-Json -Depth 10
```

The comparison prints True. For this diamond, A is READY and B/C/D are PENDING.
The process does not execute tasks. Keep `$headers` and `$body` unchanged for a
retry of this intended creation; re-running the key-generation line begins a new
request identity.

## Verification

```console
uv run --locked pytest tests/test_run_api.py
uv run --locked pytest tests/integration/test_run_http.py --database-env-file .env.database-test
uv run --locked python scripts/check_run_api.py --base-url http://127.0.0.1:8000
```

The explicit integration configuration is documented in [database tests](database.md#tests).
Unit tests cover input/header validation, sanitized errors, unconfigured storage
and OpenAPI. PostgreSQL tests cover commit visibility, replay after state changes,
concurrent duplicates/conflicts, missing versions and key reuse after rollback,
initialization and deferred COMMIT failures, corrupt stored states, pool exhaustion
and liveness while a creation request waits on another transaction.

The network smoke script runs against Uvicorn in container CI. It verifies
creation, replay, both query routes, and 409/404/422 responses. It creates a unique
workflow with two versions and one run; use a disposable database because it does
not delete append-only history. Expected output:

```text
HTTP run checks passed: creation, replay, queries, 409, 404 and 422.
```

The [trusted local deployment boundaries](api.md#boundaries) still apply.
Worker registration, leases, dispatch, execution and failure recovery remain
subsequent milestones.
