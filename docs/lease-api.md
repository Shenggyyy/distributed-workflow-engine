# Attempt lease renewal HTTP API

M2.2d.2b exposes [persisted lease renewal](lease-renewal.md) using schema `0007`
without a migration. OpenAPI is at `/openapi.json`; interactive documentation is
at `/docs`. This endpoint renews ownership metadata; it does not run a handler,
finish an Attempt, release capacity or recover expired work.

## Request and response

`POST /worker-sessions/{session_id}/attempts/{attempt_id}/renew` requires a JSON
object containing only `lease_token`, the UUID returned by the original claim.
Both path identifiers must be UUIDs. Unknown fields, including duration, request
ID, timestamps or another owner, return 422. `Idempotency-Key` is unsupported and
returns 400, including empty or duplicate headers.

Success is **200** with one required `lease` object containing attempt_id,
worker_session_id, lease_token, acquired_at, last_renewed_at and lease_expires_at.
UUIDs are strings and timestamps include a timezone. The first four fields are
unchanged; only renewal time and deadline may advance. Successful responses use
`Cache-Control: no-store` and no Location header.

Keep the token in memory and send it only in the JSON body, never in a URL or
command-line argument. The response model hides its lease from repr; validation
errors and storage logs do not echo input values, tokens or SQL parameters. The
API retains the [trusted local access boundary](claim-api.md#server-configuration-and-access-boundary);
this endpoint does not add authentication.

## Transaction and retry semantics

The synchronous handler uses a thread-pool slot and one fresh transaction. The
repository locks `Run -> stored Worker -> Task -> Attempt -> lease`, checks current
relationships/state, then observes database time after every lock has been acquired.
The response model is validated inside the transaction. Success is returned only
after COMMIT, so write, validation and deferred commit failures cannot publish a
successful renewal or retain a partial timestamp update.

Run, Task and Attempt must be RUNNING; a STOPPED owner is rejected. State checks
precede token/time checks, so a terminal Attempt with a wrong token returns
`lease_inactive`. An ACTIVE Worker with an expired heartbeat, or a LOST Worker,
may still renew an unexpired owned Attempt. Renewal does not refresh heartbeat,
reactivate a Worker, admit new claims or reset an execution timeout.

The supplied session/token must match stored ownership. A different session,
including a nonexistent session UUID, returns an ownership conflict against an
existing lease rather than a Worker lookup error. At the exact lease deadline
renewal is rejected; observation before last_renewed_at is rejected as clock
regression. There is no implicit takeover, token rotation or expired-lease revival.

Repeated requests make fresh observations, not receipt replays. A lost response
may be retried using the same session/Attempt/token tuple; the retry can extend
the deadline again, or fail if execution has become inactive or expired. After an
uncertain COMMIT, retain that tuple rather than assuming rollback or allocating
replacement work. The API does not automatically retry transactions or requests.
Concurrent renewals serialize and re-read committed timestamps. A subsequent
[claim replay](claim-api.md) reads the updated lease without renewing it itself.

Validity is established at the locked database observation, not at response
delivery. A slow transaction/network can deliver an already expired deadline.
Wall-clock jumps remain a trade-off; leases neither terminate an old process nor
guarantee exactly-once external effects. Future completion/recovery must cooperate
with this ownership protocol.

## Server configuration

`DWE_ATTEMPT_LEASE_SECONDS` controls both new HTTP claim leases and HTTP renewal,
default 30 seconds, integer range 1–86400. Settings are frozen at API startup and
forwarded by Compose. A renewal computes
`max(current_deadline, locked_observation + configured_duration)`; a shorter policy
cannot shorten existing ownership. Changing configuration alone does not renew
anything. Claim replay ignores this duration and reads stored metadata.

Worker heartbeat policy and eventual task execution timeout are independent.
Python repositories/examples keep their explicit constructor policies.

## Error mapping

Errors use the shared `{"error": {"code": "...", "message": "..."}}` envelope
without a lease or token.

| HTTP | Code | Meaning |
| --- | --- | --- |
| 400 | idempotency_not_supported | Idempotency-Key was supplied. |
| 404 | lease_not_found | Attempt has no visible ownership path, including an unowned historical Attempt. |
| 409 | lease_inactive | Run/Task/Attempt is not RUNNING, or owner is STOPPED. |
| 409 | lease_ownership_mismatch | Submitted session/token does not match ownership. |
| 409 | lease_expired | Database observation is at or beyond the deadline. |
| 422 | invalid_request | Invalid path, JSON, UUID, required field or extra field. |
| 500 | storage_error | Storage invariant, schema, response validation or commit failure. |
| 503 | database_not_configured | API has no database configuration. |
| 503 | database_unavailable | Connection, pool, lock or statement timeout/unavailability. |
| 503 | lease_clock_regression | Database observation precedes the last accepted renewal. |

## Run and verify

With API/PostgreSQL running and migrations at head, this self-contained check
creates disposable data, claims through HTTP, renews twice, then replays the claim:

```console
uv run --locked python scripts/check_lease_api.py
```

Use `--base-url http://127.0.0.1:18001` for an isolated API. Expected output:

```text
HTTP lease checks passed: renewal, retry, claim replay, 409, 404, 422 and 400.
Tokens stayed in memory; no handler executed or capacity released.
```

For manual PowerShell use, immediately after a **fresh successful** `$first` claim
in [the claim example](claim-api.md#local-example-and-tests), retain the token in variables:

```powershell
$lease = $first.claim.lease
$renewBody = @{ lease_token = $lease.lease_token } | ConvertTo-Json
$renewUri = "$baseUrl/worker-sessions/$sessionId/attempts/$($lease.attempt_id)/renew"
$renewed = Invoke-RestMethod -Method Post -Uri $renewUri -ContentType 'application/json' -Body $renewBody
$renewed.lease.lease_expires_at
```

Renew before the current deadline; an old example grant may already be expired.
These examples retain a RUNNING Attempt and its capacity reservation until future
completion/recovery exists. Repeating the smoke script creates a new Run/session.

```console
uv run --locked pytest tests/test_lease_api.py
uv run --locked pytest tests/integration/test_lease_http.py --database-env-file .env.database-test
```

Tests cover strict requests, OpenAPI, configuration snapshots, monotonic deadlines,
identity/state/time conflicts, stale heartbeat and LOST owners, current claim
replay, concurrent requests, post-lock expiry, responsive health during lock waits,
lock timeouts, invalid stored/response data, write/deferred-commit rollback and
sanitized errors. CI also runs the smoke script through the built API container.

After this commit/push and three passing CI jobs, M2.3a will define the Attempt
completion result and replay contract. Persistence, completion HTTP and handler
execution will follow as separate verified commits.
