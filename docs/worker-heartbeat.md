# Worker heartbeat and expiry transactions

M2.1c.2 adds WorkerRepository.heartbeat(session_id) and expire(session_id).
Both use schema revision 0005 and caller-owned PostgreSQL READ COMMITTED
transactions. No migration, HTTP route, background scanner or worker process is
introduced. Each call targets one known UUID.

## Contracts

| Operation | Condition after row lock | Result |
| --- | --- | --- |
| heartbeat | ACTIVE and database time < heartbeat_expires_at | Persist renewal and return the new snapshot. |
| heartbeat | ACTIVE and database time >= heartbeat_expires_at | WorkerSessionExpiredError; no writes. |
| heartbeat | LOST or STOPPED | WorkerSessionInactiveError; no writes. |
| expire | ACTIVE and database time >= heartbeat_expires_at | Transition ACTIVE → LOST; preserve every timestamp. |
| expire | ACTIVE and database time < heartbeat_expires_at | Return unchanged snapshot. |
| expire | LOST or STOPPED | Return unchanged terminal snapshot; repeated expiry is harmless. |
| either | No visible row for that UUID | WorkerSessionNotFoundError. |

Both methods require a Python UUID and the repository's original active
transaction. Malformed IDs fail before SQL. StoredWorkerError reports corrupt
persisted state with a fixed message. M2.1d maps these errors through the
[Worker HTTP API](worker-api.md).

Returned StoredWorkerSession values use the same immutable snapshot type as
registration. They are provisional until the caller commits and can become stale
afterward. The operations do not alter registration identity, capacity, tasks or
attempts. LOST remains a session status, not proof that a handler stopped.

A rejected heartbeat does not implicitly mark the row LOST. This avoids a method
both raising an error and expecting callers to commit its side effects. The
separate expiry transaction records LOST. Until then, an expired ACTIVE row
still cannot renew through this repository. Future claims must check deadlines
rather than relying solely on status or waiting for a recovery scan.

## Lock, clock and renewal ordering

Each operation uses SELECT FOR UPDATE on the UUID, decodes the locked row, and
then samples PostgreSQL clock_timestamp() for an ACTIVE session. It never trusts
a pre-lock snapshot or a client timestamp. No registration advisory lock is
acquired; existing-row operations must not reverse registration's advisory-lock
then row-lock order.

The deadline is exclusive: one microsecond before it may renew, equality is
expired. The database observation is sampled once per active operation.

Heartbeat writes:

```text
last_heartbeat_at = observed database time
heartbeat_expires_at = max(previous deadline, observed time + server timeout)
```

The timeout is the existing constructor policy (default 30 seconds, strict integer
1–86400). A smaller policy cannot shorten an already accepted deadline; it takes
effect as observations advance. Equal observations are permitted and may produce
an unchanged SQL row. These metadata writes do not emit an ACTIVE-to-ACTIVE
domain lifecycle event.

If the observation precedes last_heartbeat_at, both active operations raise
WorkerClockRegressionError and write nothing. The repository does not manufacture
a timestamp, clamp backwards time or violate monotonic storage constraints.
Terminal expiry replay and terminal heartbeat rejection need no clock sample.

This is failure detection based on database wall-clock time, not a perfect crash
detector. Forward clock jumps can cause false expiry; backward adjustments can
temporarily reject observations or delay expiry. A renewal acknowledges one
observation, not continued health of a task subprocess.

Decisions are made at the clock sample, not at commit or response time. A long
transaction can commit a renewal whose deadline has already elapsed. Keep the
transaction short; follow-up heartbeat/claim operations recheck authoritative time.
Pool/lock/statement timeouts still apply, and none is a task execution timeout.

## Races and failures

| Interleaving | Result |
| --- | --- |
| Renewal locks first and commits a future deadline | Waiting expiry sees that deadline and leaves ACTIVE unchanged if still live. |
| Renewal locks first but rolls back | Waiting expiry sees the original deadline and may record LOST. |
| Expiry commits LOST first | Waiting heartbeat sees terminal state and is rejected. |
| Expiry rolls back | Waiting heartbeat sees the old ACTIVE row, but still rejects it if its deadline has elapsed. |
| Multiple concurrent heartbeats | Row locks serialize observations and preserve monotonic times. |
| Multiple concurrent expirers | First due transition records LOST; subsequent calls return the terminal snapshot. |
| Caller aborts or deferred COMMIT fails | State and timestamps roll back; a returned provisional snapshot is not durable. |
| Lock timeout | Database error propagates; no hidden retries or partial state writes. |

The methods do not create new transactions or swallow SQL failures. Callers must
propagate database errors or deliberately roll back a savepoint. If a successful
heartbeat response is lost, a later heartbeat is a new observation, not replay of
an immutable receipt; it may fail if the session has since expired. Expiry is
repeatable because terminal state is retained.

The current expiry operation is deliberately per UUID. A future recovery loop
will discover candidates using the existing partial deadline index and invoke
authoritative expiry checks. Candidate selection alone cannot authorize expiry,
and no guarantee of eventual scanning exists in this milestone. Multi-row batches
need bounded work and a lock-order/skip policy. Prefer one session operation per
transaction until that protocol exists.

Worker expiry neither requeues tasks nor terminates handlers. Attempt lease
validation, fencing and recovery remain separate work.

## Runnable example

With PostgreSQL configured and migrated to 0005:

```powershell
$env:DWE_DATABASE_PORT = "15432"
$env:DWE_DATABASE_PASSWORD_FILE = "secrets/postgres_password.txt"
$sessionId = [guid]::NewGuid().ToString()
uv run --locked python examples/register_worker.py --session-id $sessionId
uv run --locked python examples/worker_heartbeat.py heartbeat --session-id $sessionId
uv run --locked python examples/worker_heartbeat.py expire --session-id $sessionId
```

Run the heartbeat within 30 seconds of registration. Immediate expiry normally
shows ACTIVE because the session is still live. After its displayed deadline has
passed without another heartbeat, running expire again shows LOST; repeating it
preserves LOST. A subsequent heartbeat raises WorkerSessionInactiveError.

The example accepts --env-file and prints only after commit:

```text
Session ID: <UUID>
Stored status: <ACTIVE or LOST>
Last heartbeat: <last accepted database observation>
Heartbeat expires at: <persisted deadline>
Decision committed; no background scan or task ownership changes.
```

This is one operation per invocation, not a running worker or scheduler.

## Verification

```console
uv run --locked pytest tests/integration/test_worker_heartbeat.py --database-env-file .env.database-test
```

Boundary tests override only the private database-clock seam; they use real
PostgreSQL locks, row versions, constraints and COMMIT. They cover the exact
deadline, one microsecond on either side, clock regression, shorter timeout
policy, terminal replay, invalid/missing IDs, transaction lifetime, caller rollback
and injected deferred COMMIT failure.

Race tests observe actual blockers through pg_blocking_pids and verify the
contender does not sample its clock before acquiring the row lock. They cover
both lock winners and commit/rollback outcomes without depending on a sleep to
create the interleaving. Separate tests use the real PostgreSQL clock for four
concurrent heartbeats and check the final persisted timestamps.

M2.1d exposes Worker registration and heartbeat through the [HTTP API](worker-api.md).
Automatic expiry
candidate scanning belongs to the later scheduler/recovery process.
