# Single-run task claim transactions

M2.2c.1 adds `ClaimRepository.claim_next(run_id, worker_session_id)` using schema
`0006`. A successful claim atomically advances one READY Task to RUNNING and
creates a RUNNING Attempt and its lease. It does not execute a handler, renew a
lease, schedule dependencies/retries, expose a claim HTTP route or run recovery.

## Python contract

```python
from workflow_engine.repositories.claims import ClaimRepository

with engine.begin() as connection:
    result = ClaimRepository(connection, lease_seconds=30).claim_next(
        run_id, worker_session_id
    )
# Only a successful exit from engine.begin() makes the result committed.
```

Use a fresh PostgreSQL READ COMMITTED transaction for each invocation, with driver
autocommit disabled. The repository validates its original transaction on every
call and cannot be reused after commit/rollback or shared across threads. The
caller must not begin this operation while holding Worker/Task/Attempt locks from
another operation. The repository cannot detect arbitrary locks acquired by its
caller; do not combine claims for different Runs in one transaction.

IDs must be Python UUID instances. The optional `lease_seconds` constructor policy
is a strict integer from 1 through 86400, default 30. It is independent of Worker
heartbeat timeout; no new environment setting or configuration reload is added.

| Outcome | Meaning |
| --- | --- |
| TaskClaim | Provisional pinned version UUID, RUNNING Task/Attempt snapshots, AttemptLease and immutable TaskDefinition. |
| None | Worker capacity is occupied, or this Run has no READY Task. These reasons intentionally share one result. |
| ClaimRunNotFoundError | Run does not exist in this transaction's visible state. |
| ClaimRunInactiveError | Run is not RUNNING. |
| WorkerSessionNotFoundError | Worker session does not exist. |
| WorkerSessionInactiveError | Session is LOST or STOPPED. |
| WorkerSessionExpiredError | Session heartbeat deadline has elapsed. |
| WorkerClockRegressionError | Clock precedes the last heartbeat or the earlier observation in this claim. |
| AttemptNumberExhaustedError | The next attempt number would exceed PostgreSQL INTEGER. |
| StoredRuntimeError / StoredWorkerError / StoredDefinitionError | Persisted state cannot safely authorize this claim. |
| RepositoryTransactionError | Unsupported or expired transaction context. |

SQL failures propagate. The caller must exit/roll back the transaction or its
savepoint on failure; there is no internal commit, savepoint, exception swallowing
or automatic retry. Even a returned TaskClaim is not durable if later COMMIT
fails. Its lease and definition are omitted from dataclass repr to avoid casually
logging ownership tokens or handler data.

## Lock and allocation protocol

1. Lock the requested WorkflowRun row and decode its identity/status. Require
   RUNNING; a stopped or missing Run is an error rather than an empty poll.
2. Lock the WorkerSession row, validate its snapshot and require ACTIVE. Sample
   `clock_timestamp()` after that lock. The observation must be at least the last
   accepted heartbeat and strictly before its deadline.
3. In a separate statement after the Worker lock, count its leases joined to
   RUNNING Attempts across all Runs. If count >= max_concurrency, return None.
4. Select one READY Task in this Run, ordered by task_key then UUID, and lock it.
   Return None if none is READY. No SKIP LOCKED, global polling or priority policy
   is introduced in this single-run primitive.
5. Load the Run's pinned immutable version. Require a matching Task definition
   and verify every declared dependency exists in this Run and is SUCCEEDED.
   A READY row with unsatisfied dependencies is corrupt state, not permission to
   execute. This check does not itself promote PENDING tasks to READY.
6. Require no existing RUNNING Attempt for that Task. Allocate one plus the maximum
   historical attempt number (or 1 for a first attempt), bounded to 2147483647.
7. Sample the database clock again after the Task lock and reads. Reject backwards
   time or a now-expired Worker. Create fresh attempt/token UUIDs and a lease whose
   acquired/last-renewed time is this observation and deadline is observation plus
   server duration. Atomically update the Task and insert Attempt and lease.
8. Return the provisional snapshots; caller COMMIT must succeed before publishing
   an executable grant. No handler runs under a database lock.

The order is `Run -> Worker -> Task -> new Attempt/lease`. Existing Attempts need
no additional explicit row lock during allocation because the Run lock serializes
cooperating writers. The existing uniqueness, foreign-key and state guards remain
backstops against invalid writes. `task_attempts.created_at` is existing audit
metadata using transaction-start time; lease acquisition uses the later clock
sample. Neither timestamp proves a handler started.

## Concurrency and failure semantics

Run locking prevents two participating callers from authorizing the same READY
Task and serializes attempt-number allocation. Worker locking separately protects
capacity across different Runs; the post-lock count sees the preceding claimant's
commit under READ COMMITTED. Completion/recovery must use the same Worker lock
when changing outstanding ownership. Direct SQL that bypasses this protocol is
not covered by the capacity guarantee.

An expired lease with a still-RUNNING Attempt continues to occupy capacity. The
claim path does not silently reclaim it. Recovery must persist the Attempt's
terminal outcome before that slot becomes available. Historical Attempts without
leases cannot be attributed to a Worker and are not counted as its ownership;
they are not executable grants. A READY Task with any RUNNING Attempt, including
an unowned historical one, is rejected as inconsistent.

The Task update, Attempt insert and lease insert share a transaction. Failures at
any write, caller abort and deferred COMMIT failure roll back the allocation.
A waiting claimant can proceed after its predecessor rolls back. Lock/statement
timeouts propagate; no partial allocation should be used as a result.

Repeated calls are new claim attempts, **not request replays**. If two tasks are
READY and capacity permits, two calls may allocate different tasks. If a commit
succeeds but its response is lost, ownership survives; do not blindly retry claim
over HTTP. M2.2d.1a adds [binding storage and the replay protocol](claim-requests.md);
M2.2d.1b implements [keyed claim transactions](idempotent-claims.md) in a separate
repository. The primitive on this page remains unkeyed. No network delivery
guarantee is claimed before the claim HTTP interface.

The returned snapshot can already be expired after a slow commit or delayed
delivery. Future Worker execution must honor the lease/renewal protocol; a Python
result alone is not continuing authority. Lease expiry cannot stop an old process
or undo business effects. See [Attempt leases](attempt-leases.md) for the separate
fencing, idempotency and timeout boundaries.

Selection is deterministic rather than fair. A corrupt first READY Task or an
exhausted attempt counter fails the call rather than skipping that Task. This
favors visible invariant failures over hiding bad state. Per-run coordination
limits control-plane throughput for a large Run, while handlers can later execute
concurrently outside those short transactions.

## Local example

With API/PostgreSQL running on the existing development ports and migrations at
the current head, run the following together so registration is followed promptly
by the claim:

```powershell
$env:DWE_DATABASE_PORT = "15432"
$env:DWE_DATABASE_PASSWORD_FILE = "secrets/postgres_password.txt"
$baseUrl = "http://127.0.0.1:8000"
$definition = Get-Content examples/diamond.json -Raw
$version = Invoke-RestMethod -Method Post -Uri "$baseUrl/workflows" -ContentType "application/json" -Body $definition
$runBody = @{ workflow_version_id = $version.id } | ConvertTo-Json
$headers = @{ "Idempotency-Key" = [guid]::NewGuid().ToString() }
$run = Invoke-RestMethod -Method Post -Uri "$baseUrl/runs" -Headers $headers -ContentType "application/json" -Body $runBody
$sessionId = [guid]::NewGuid().ToString()
$workerBody = @{ worker_name = "claim-demo"; max_concurrency = 2 } | ConvertTo-Json
Invoke-RestMethod -Method Put -Uri "$baseUrl/worker-sessions/$sessionId" -ContentType "application/json" -Body $workerBody | Out-Null
uv run --locked python examples/claim_task.py --run-id $run.run_id --session-id $sessionId
```

Expect Task A, Attempt number 1, handler `demo.echo`, a deadline and the message
`Claim committed; no handler executed. Token intentionally not printed.`
The example also accepts `--env-file` for a separately configured database. API
and CLI must address the same database. It never prints the lease token and cannot
be used as a Worker execution client.

**The example persists RUNNING ownership and reserves capacity.** Expiry recovery
and completion are not implemented yet, so this demo does not cleanly finish a
Workflow or release its slot automatically. Prefer disposable Runs/sessions for
experiments; do not manually alter production state to make the example reusable.

## Verification

```console
uv run --locked pytest tests/integration/test_claims.py --database-env-file .env.database-test
```

Tests run against migrated private PostgreSQL schemas. They cover committed and
uncommitted visibility, fixed-version definitions, dependency checks, retries with
fresh ownership, same-task contention, cross-run capacity, expired lease capacity,
Run/Worker admission, both clock observations, Task-lock waiting, rollback with a
waiting claimant, lock timeout, each failed write and deferred COMMIT failure.
Test-only state changes simulate successful/failed earlier Attempts; they are not
an implementation of completion, dependency scheduling or retry backoff.

M2.2c.2 adds [lease renewal](lease-renewal.md) against current persisted ownership,
with stale-owner and race checks. M2.2d.1a adds binding storage; M2.2d.1b adds
[keyed claims](idempotent-claims.md). Claim HTTP remains later work.
