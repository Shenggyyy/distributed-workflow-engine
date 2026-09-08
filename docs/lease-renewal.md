# Persisted attempt lease renewal

M2.2c.2 adds `LeaseRepository.renew()` on schema `0006`. It renews only current,
RUNNING execution ownership in a caller-owned PostgreSQL transaction. No migration,
HTTP route, Worker loop, completion handler or expiry recovery is introduced.
M2.2d.2b subsequently exposes this repository through the [lease HTTP API](lease-api.md).

## Python contract

```python
from workflow_engine.repositories.leases import LeaseRepository

with engine.begin() as connection:
    lease = LeaseRepository(connection, lease_seconds=30).renew(
        attempt_id,
        worker_session_id=worker_session_id,
        lease_token=lease_token,
    )
# Use the new deadline only after this transaction commits successfully.
```

All three identifiers must be UUID instances. The constructor accepts a strict
integer duration from 1 to 86400 seconds, default 30. This server policy is
independent of Worker heartbeat timeout. The Python constructor policy remains
explicit; the later HTTP adapter supplies `DWE_ATTEMPT_LEASE_SECONDS` from Settings.
Each operation uses a fresh READ COMMITTED transaction, with driver autocommit
disabled. A repository cannot outlive its original transaction or be shared across
threads. Callers must not hold locks that reverse the ordering below.

The returned immutable AttemptLease is provisional until COMMIT. Its attempt,
owner, token and acquired time never change. Only last_renewed_at and
lease_expires_at may advance. The duration starts at the locked database
observation; a shorter policy cannot shrink an existing deadline.

## Discovery, locking and validation

1. Read the immutable Attempt -> Task -> Run and lease -> Worker references in
   one non-locking query. Missing ownership, including a historical Attempt without
   a lease, returns LeaseNotFoundError. This query discovers lock targets only;
   its snapshot does not authorize renewal.
2. Lock and re-read `Run -> Worker -> Task -> Attempt -> lease`, using the stored
   owner rather than whichever Worker ID the caller supplied. Revalidate the
   models and relationships after the locks.
3. Require Run, Task and Attempt all RUNNING. The existing partial unique index
   permits at most one RUNNING Attempt for the Task; a terminal Attempt cannot
   borrow a replacement's ownership. Reject a STOPPED Worker.
4. Only after every lock is held, sample PostgreSQL `clock_timestamp()` and call
   the pure [lease model](attempt-leases.md) with the submitted tuple. It requires
   matching attempt/session/token, time >= last_renewed_at and time strictly less
   than lease_expires_at. Equality at the deadline is expired.
5. Persist the two renewed timestamps and validate the returned row. Propagate
   errors so the caller rolls back. No state transitions, task execution or
   implicit lease takeover occur.

The immutable references and history guards make the discovery targets stable
for cooperating writers. A mismatch after locking is still rejected as corrupt
storage. Deliberate schema/trigger bypasses by database administrators are outside
the supported transaction protocol.

The Worker row lock is retained even though renewal does not change capacity.
This gives claim, renewal and future completion/recovery a common lock order.
It serializes control operations sharing a Run or Worker and may limit throughput;
benchmark before relaxing the coordination rules.

## Worker liveness is separate from attempt ownership

An ACTIVE Worker with an expired heartbeat, or a Worker marked LOST, may renew
its still-valid RUNNING attempt. Heartbeat expiry stops new claims; it does not
invalidate a lease or prove that the task process stopped. Renewal does not
refresh the Worker heartbeat, reactivate LOST status, or enable new claims.

STOPPED is different: it represents orderly shutdown, whose eventual transaction
must ensure there are no active owned Attempts. A STOPPED owner is rejected even
if raw SQL left it with a RUNNING Attempt. The graceful-shutdown transaction itself
is not implemented yet. Restarting with a new session or the same display name
does not transfer existing ownership.

## Error and retry behavior

| Error | Meaning |
| --- | --- |
| LeaseNotFoundError | No visible ownership path exists for this Attempt, including an Attempt without a lease. |
| StoredLeaseError | A locked lease/reference/model is missing, invalid or inconsistent. |
| LeaseInactiveError | Run/Task/Attempt is not RUNNING, or Worker is STOPPED. |
| LeaseOwnershipError | Supplied attempt/session/token does not match the stored lease. |
| LeaseClockRegressionError | Observation precedes the last accepted lease observation. |
| LeaseExpiredError | Observation is at or beyond the current lease deadline. |
| RepositoryTransactionError | Unsupported or no longer active transaction context. |
| TypeError / ValueError | Invalid UUID inputs or server duration. |

Messages do not echo tokens or supplied identities. State checks precede the
domain ownership/time check; a terminal Attempt may return LeaseInactiveError even
when its token is also wrong. Driver, lock and statement timeout errors propagate
without automatic retries. A successful Python return is not enough if a deferred
COMMIT then fails. Exit/roll back the transaction or savepoint on any failure.

Renewal retries make new observations rather than replaying a stored receipt. A
lost response can be followed by another renewal with the same tuple; that later
operation may extend the lease, or fail if ownership expired or became terminal.
No code here guarantees retry delivery. Claim request idempotency is implemented
separately and exposed through [claim HTTP](claim-api.md).

Concurrent renewals serialize and re-read the latest timestamps. If a preceding
renewal rolls back, its timestamps disappear. If a preceding operation commits a
terminal Attempt, a waiting renewal rejects it without reopening the Attempt.
The recovery/completion actions in these tests are controlled database mutations;
production completion and recovery are still future milestones.

Deadlines are checked at the database observation, not continuously until response
delivery. A slow commit can leave a returned deadline already expired. Locks are
held only for metadata work, and execution must happen outside transactions.
Wall-clock jumps can cause false expiry or temporary rejection. A renewed lease
does not prove handler progress, terminate an old process or prevent duplicate
external effects.

Task execution timeout has no persisted deadline field yet. Later timeout handling
must apply its own absolute deadline; renewing a lease must never reset that
execution deadline. This stage does not claim to enforce task timeout or provide
exactly-once execution.

## Runnable example

Create a **fresh** Run and Worker session using the setup in
[the claim example](task-claims.md#local-example), but replace its final
`claim_task.py` invocation with:

```powershell
uv run --locked python examples/claim_and_renew.py --run-id $run.run_id --session-id $sessionId
```

This claims one task, waits for that claim to commit, keeps its token in memory,
and renews with a 60-second policy in a separate transaction. It uses ownership
from its own claim, not a token copied from an arbitrary database row. It prints
the Attempt ID, original/new deadlines, `Ownership preserved: True`, and a
commit confirmation. Tokens are never printed or accepted as command-line input.
Use `--env-file` for an explicitly configured database if needed.

The example still reserves capacity and does not execute or finish the task.
There is no automatic recovery to release its slot. Run experiments with disposable
Runs/sessions; repeating the example is another claim, not replay of the first one.

## Verification and next step

```console
uv run --locked pytest tests/integration/test_lease_renewal.py --database-env-file .env.database-test
```

Tests cover committed/uncommitted visibility, identity preservation, shorter
policy, deadline equality/microsecond boundaries, clock regression, wrong tokens,
historical unowned Attempts, all terminal Attempt outcomes, Run/Task shutdown,
Worker LOST/STOPPED, replacement Attempts, concurrent renewals, waiting at each
of the five lock positions, committed/rolled-back predecessors, write/deferred
COMMIT failures, explicit abort, corrupted storage and transaction lifetime.

M2.2d.1a now defines [claim request bindings](claim-requests.md), with immutable
input identity and the replay/outer-lock protocol. M2.2d.1b implements
[keyed transactions](idempotent-claims.md). Renewal and replay share the internal
ordered ownership reader; renewal keeps its existing errors and timestamp rules.
M2.2d.2a adds [claim HTTP](claim-api.md); M2.2d.2b adds [renewal HTTP](lease-api.md)
with commit/error mapping and real HTTP claim/renew/replay checks.
