# Completion HTTP API

M2.3d exposes [atomic completion/replay](completion-transactions.md) using schema
`0008`, without a new migration. OpenAPI is at `/openapi.json`; interactive docs
are at `/docs`. No handler is executed by this endpoint.

## Request and response

`POST /worker-sessions/{session_id}/attempts/{attempt_id}/complete` accepts:

```json
{
  "lease_token": "b45d72c8-dac5-4cf9-b67e-d737d46c89e0",
  "result": {"outcome": "SUCCEEDED"}
}
```

The UUID above illustrates shape only; use the original claim's token in memory.
For failure, use `{"outcome":"FAILED","error_code":"handler_failed"}`. A failure
requires the existing 1–64 character Identifier-format error code; success forbids
a non-null code. Outcome is case-sensitive. LOST/TIMED_OUT, result payloads, client
timestamps, retry decisions and extra fields are rejected. Path IDs and token must
parse as UUIDs. The token belongs in JSON, never the URL or a command-line argument.

The Attempt ID is the deduplication key. Idempotency-Key headers, including empty
or repeated headers, return 400. A new completion and an identical replay both
return **200**, with exactly these fields:

| Field | Meaning |
| --- | --- |
| attempt | Original terminal TaskAttempt snapshot. |
| worker_session_id | Original reporting Worker session UUID. |
| result | Accepted outcome and error_code (null for success). |
| accepted_at | Original locked database observation in UTC. |

There is no token, live Task/Run snapshot or replay-time timestamp in the response.
Success sets `Cache-Control: no-store` without a Location header. The API retains
the [trusted local access boundary](claim-api.md#server-configuration-and-access-boundary);
this endpoint does not introduce authentication.

## Commit, ownership and retry

The synchronous handler runs in the thread pool, constructs a validated domain
report, opens one transaction, calls CompletionRepository, validates the full
internal receipt and builds the public response **before COMMIT**. It returns
success only after the transaction exits successfully. Write, response validation
or deferred COMMIT failures roll back and cannot return successful completion.
Storage-error logs omit exception messages, bound values and tokens.

First acceptance requires current RUNNING Run/Task/Attempt and a non-STOPPED owner
with valid session/token and lease at the post-lock database observation. A stale
heartbeat or LOST Worker alone does not revoke an unexpired Attempt lease. At the
exact lease deadline or [fixed execution deadline](timeouts.md), a new report is
rejected. Failure keeps the Attempt FAILED;
the Task enters RETRY_WAIT within its pinned policy budget, otherwise FAILED.
The immutable retry time and receipt commit together. See [retry semantics](retry-policy.md).

An existing valid receipt is compared before current-state/deadline admission.
Resending the exact original report confirms its accepted result even after lease
expiry or Worker shutdown. A conflicting result fails; changing tokens or sessions
does not inherit ownership. Replay does not extend a lease, execute a handler,
repeat Task transitions or release capacity twice. Claim replay of that terminal
Attempt is no longer available as executable ownership.

If a COMMIT result or response is uncertain, retain and retry the original report.
Do not fabricate a new Attempt or report failure merely because a successful
response was lost. The endpoint does not retry automatically. Historical confirmation
is not exactly-once execution or business idempotency. Run aggregation and downstream
readiness still require later scheduling work.

## Error mapping

Errors use the existing token-free `error.code` / `error.message` envelope.

| HTTP | Code | Meaning |
| --- | --- | --- |
| 400 | idempotency_not_supported | Idempotency-Key was supplied. |
| 404 | completion_not_found | No owned Attempt exists, including unleased history. |
| 409 | completion_inactive | No receipt, and Run/Task/Attempt is not RUNNING or Worker is STOPPED. |
| 409 | completion_conflict | Same owner/Attempt, different accepted result. |
| 409 | lease_ownership_mismatch | Submitted session/token differs from ownership. |
| 409 | lease_expired | New acceptance is at or beyond the lease deadline. |
| 422 | invalid_request | Invalid JSON, path, UUID, result or extra field. |
| 500 | storage_error | Corrupt state, schema, response validation or commit constraint failure. |
| 503 | database_not_configured | API has no database configuration. |
| 503 | database_unavailable | Database connection/pool/lock/statement timeout or unavailability. |
| 503 | lease_clock_regression | New acceptance precedes the last lease renewal. |

State checks for first acceptance precede ownership/time checks. A terminal Attempt
without a receipt cannot be silently treated as a duplicate. Repository ordering,
rollback and time limits remain documented in [completion transactions](completion-transactions.md).

## Run and verify

With the current API image and PostgreSQL at head:

```console
uv run --locked python scripts/check_completion_api.py
```

Use `--base-url` for a different API port. This self-contained script creates
disposable Runs/sessions and submits simulated success and failure. It confirms
original receipt replay, conflicting-result rejection, terminal claim rejection,
and capacity reuse, then completes the second task too. Tokens are neither printed
nor returned in completion responses. Run state remains RUNNING pending aggregation.

```console
uv run --locked pytest tests/test_completion_api.py
uv run --locked pytest tests/integration/test_completion_http.py --database-env-file .env.database-test
```

HTTP tests cover requests, OpenAPI, token omission, committed visibility, replay,
owner/conflict/state/time errors, concurrent reports and write/deferred-commit or
invalid-response rollback. CI runs the real HTTP script in addition to existing
Workflow, Run, Worker, claim and renewal checks.

Next, M2.4 connects a bounded single-worker execution path. Handler admission,
claim reuse, heartbeat/renewal and completion delivery will be split into small
subtasks before expanding concurrency in M3 and resilience in M4.
