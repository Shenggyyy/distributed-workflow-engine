# Workflow HTTP API

M1.4 publishes and retrieves workflow definitions. M1.9b adds a separate
[Run HTTP API](run-api.md) for keyed creation and queries. M2.1d adds the
[Worker HTTP API](worker-api.md) for registration and heartbeat. These APIs do not
execute tasks.
OpenAPI is at `/openapi.json`; interactive documentation is at
`/docs`. These routes use the definition tables introduced in revision `0002`.
The current head is `0005`, adding runtime, request-binding and Worker storage without
changing these workflow routes. Workflow publication remains unkeyed; Run
creation requires an idempotency key.

## Endpoints

| Method/path | Success | Meaning |
| --- | --- | --- |
| POST /workflows | 201 + version | Publish the body as a new version; create its workflow identity by exact name if absent. |
| GET /workflows/{name} | 200 + workflow | Find an identity by case-sensitive name. |
| GET /workflow-versions/{version_id} | 200 + version | Read an immutable snapshot by version UUID. |
| GET /workflows/{workflow_id}/versions/latest | 200 + version | Read the highest version visible to this query. |
| GET /workflows/{workflow_id}/versions/{version_number} | 200 + version | Read one version by workflow UUID and number. |
| GET /health/live | 200 | Process liveness; no database query. |

Names follow the [domain identifier rules](workflows.md). Version and workflow
IDs are UUIDs; numbered paths accept integers from 1 through 2147483647.
The workflow-name lookup and UUID-based version lookup paths are intentional.
No list, update, delete, or rename endpoints are exposed.

POST accepts the existing `WorkflowDefinition` directly, including its
`schema_version`, `name`, and `tasks`. It validates all fields and the DAG.
Unknown fields are rejected. `Content-Type: application/json` is required for
the documented examples. Identical successful POSTs create distinct versions;
this endpoint is not idempotent. Sending `Idempotency-Key` returns 400 rather
than silently pretending to honor it.

A workflow response has `id`, `name`, and `created_at`. A version response has:

```json
{
  "id": "1e81ec81-a01f-4212-a7f5-0e4713d7dd61",
  "workflow_id": "180d861c-6bc8-4f93-bcfe-4d8b5f8c3713",
  "version_number": 1,
  "created_at": "2026-09-08T00:00:00Z",
  "definition": {
    "schema_version": 1,
    "name": "demo",
    "tasks": [
      {"task_id": "A", "task_type": "demo.echo", "depends_on": []}
    ]
  }
}
```

UUIDs/timestamps above are illustrative. POST also sets a root-relative
`Location: /workflow-versions/<version UUID>`. Server-returned definitions
include defaults. JSON object key order is not a contract. `created_at` is an
ISO 8601 timezone-aware database timestamp; version numbers determine ordering.
The latest version may change immediately after the query: store the returned
version UUID when you need a stable reference.

## Transactions and process lifetime

The factory accepts immutable settings and an optional borrowed SQLAlchemy engine.
An app without an injected engine creates one lazy, bounded PostgreSQL pool during
lifespan startup if a password source is configured. Shutdown disposes an app-owned
pool in the thread pool. A borrowed engine remains the caller's responsibility.
No environment or credentials are loaded at module import or factory construction.

Without credentials, liveness and OpenAPI remain available and storage routes
return 503 `database_not_configured`. An explicitly configured unreadable password
file is a startup configuration error. An unreachable database does not prevent
startup because the pool connects lazily. This behavior supports local health-only
use; it must not be interpreted as dependency readiness.

Database route handlers are regular `def` functions, which FastAPI runs in its
thread pool. The dependency returns only the shared Engine; a Connection and
Repository are created inside the handler's own transaction. Begin, queries,
response-model validation, commit, and connection return all occur there. No
connection is shared between requests or passed through a yield dependency.
See [FastAPI synchronous handlers](https://fastapi.tiangolo.com/async/).

POST sets Location and returns its response only after the transaction context
exits successfully. A database failure during COMMIT therefore cannot become a
successful 201. A client disconnect after commit can still lose the response;
network delivery of an HTTP response is not atomic with PostgreSQL commit.
An uncertain response must not be automatically replayed: another POST may
create another version. There is no automatic transaction retry.

The [repository publication protocol](workflow-storage.md#transactional-repository)
provides same-workflow coordination. Different workflow rows can progress
independently. Ordinary readers do not take publication row locks.

## Errors

Workflow validation and known application/storage failures use this envelope:

```json
{
  "error": {
    "code": "invalid_request",
    "message": "Request validation failed.",
    "details": [
      {"location": ["body"], "type": "cycle_detected"}
    ]
  }
}
```

| Status | Code | Meaning |
| --- | --- | --- |
| 400 | idempotency_not_supported | A publication included Idempotency-Key. |
| 404 | workflow_not_found | No identity matches the name. |
| 404 | version_not_found | No version matches the lookup, including an unknown workflow UUID. |
| 422 | invalid_request | Malformed JSON, field/path validation, or an invalid DAG. |
| 503 | database_not_configured | No engine is available from API lifespan. |
| 503 | database_unavailable | Connection/operational failure, SQL timeout, or pool checkout timeout. |
| 500 | storage_error | Corrupt stored definition, constraint/schema error, or invalid repository transaction. |

Details contain only validation locations and error types. No request values,
validation context/messages, credentials, SQL, or driver exception messages are
returned. Field names in locations can be user supplied. Validation codes come
from the pinned Pydantic/domain implementation; clients should primarily branch
on the top-level code. Non-validation failures use an empty details array.

The handlers also avoid raw exception messages in structured logs: the event
`api_storage_failed` records the exception type/stack locations using the existing
formatter. Request payloads and Idempotency-Key values are not logged. See
[FastAPI custom validation handlers](https://fastapi.tiangolo.com/tutorial/handling-errors/).
Framework routing errors (such as an unknown route or unsupported HTTP method)
retain FastAPI's default response; unexpected programming errors remain server
errors rather than being hidden as missing resources.

503 is an availability/error classification, not a promise that replay is safe
or will succeed. Authentication/configuration errors can also appear as operational
database errors. Missing migrations produce a fixed 500 storage error; the API
does not attempt to repair schema automatically. SQL/lock and pool timeouts are
not whole-request deadlines or task timeouts. Liveness remains independent of DB
availability, but extreme process load can affect any route.

## Local execution and tests

For Compose:

```powershell
$env:DWE_POSTGRES_PUBLISHED_PORT = "15432"
python scripts/init_dev_secrets.py
docker compose up --build --wait --wait-timeout 120
docker compose exec api alembic upgrade head
uv run --locked python scripts/check_workflow_api.py
```

Linux users must first apply the
[secret permissions](local-development.md#linux-secret-permissions) after secret
initialization and before starting Compose. The API connects to `postgres:5432`
inside the network even when PostgreSQL is published on host port 15432.

For a host-run API, set `DWE_DATABASE_PORT=15432` and
`DWE_DATABASE_PASSWORD_FILE=secrets/postgres_password.txt`, then run
`uv run --locked engine api` (stop the Compose API first if it occupies port 8000).
An explicit `--env-file` also works. See the README for PowerShell POST/GET examples.

```console
uv run --locked pytest tests/test_api.py tests/test_workflow_api.py
uv run --locked pytest tests/integration/test_workflow_http.py --database-env-file .env.database-test
uv run --locked python scripts/check_workflow_api.py --base-url http://127.0.0.1:8000
```

The integration configuration is described in [database.md](database.md#tests).
Unit HTTP tests do not connect to PostgreSQL except the deliberate unreachable-port
test. Integration tests apply real migrations to private schemas. They verify
visibility after HTTP success, history, missing/invalid input, corrupt storage,
four concurrent publishers, pool exhaustion, and liveness while a request waits
on a row lock. A deferred PostgreSQL constraint trigger fails only at COMMIT to
verify rollback and absence of a 201 response.

The network smoke script runs against an actual Uvicorn container in CI, after
explicit migrations. It creates a uniquely named workflow and two versions in
the target DB and checks all lookup paths, 404, and 422. Use a disposable database;
the script does not remove append-only history.

## Boundaries

The API is for a trusted local development environment: no authentication,
authorization, tenancy, or handler execution is implemented. Compose publishes
only on loopback. The DAG has task/edge limits, but no overall HTTP body-size
limit, request admission control, or rate limit is implemented yet. The existing
thread pool and database pool are not a complete backpressure design. Production
deployment needs those controls and separate runtime/migration database roles.

No readiness endpoint, task scheduling, cancellation or event log is exposed.
M1.9b exposes [Run HTTP creation and queries](run-api.md), using the M1.8
[keyed creation protocol](run-idempotency.md) and M1.9a [queries](run-queries.md).
Workflow publication through this API remains unkeyed.
