# Atomic completion and historical replay

M2.3c.1 adds `CompletionRepository.complete()` on schema `0008`. It uses the
[completion domain contract](attempt-completion.md) and [receipt storage](completion-storage.md)
to settle one Attempt and Task in a caller-owned transaction. This is a Python
repository API; no completion HTTP route or handler execution is added.

## Python contract

```python
from workflow_engine.repositories.completions import CompletionRepository

with engine.begin() as connection:
    receipt = CompletionRepository(connection).complete(report)
# Only publish the receipt after leaving this context successfully.
```

`report` is an immutable AttemptCompletion containing Attempt ID, Worker session,
lease token and CompletionResult. Input is revalidated before operation SQL, including
instances forged through model_copy. Use one report per fresh PostgreSQL READ
COMMITTED transaction, with driver autocommit disabled. The repository cannot
outlive its original transaction or be shared across threads. It does not commit,
retry requests, sleep, execute a handler or hold locks across external work.

The returned receipt is provisional until COMMIT. Roll back the transaction or
containing savepoint on **any** failure; do not catch a validation error after a
partial write and then commit the remaining changes. The future HTTP adapter must
validate its outgoing response and complete the transaction before returning success.

## First completion

1. Discover immutable ownership references, then lock and read
   `Run -> stored Worker -> Task -> Attempt -> lease`. The existing ownership helper
   validates relationships and model shapes. The supplied Worker ID does not choose
   which Worker row to lock.
2. Read a receipt for the Attempt after these locks. Cooperating writers serialize
   on the Run; no extra request advisory lock is needed. No receipt means a first
   completion, not an assumed duplicate based on terminal status alone.
3. Require Run/Task/Attempt RUNNING and Worker not STOPPED. Observe database
   `clock_timestamp()` after all ownership locks. Validate session/token and the
   interval `last_renewed_at <= observation < lease_expires_at` through the domain
   helper. Heartbeat expiry or LOST Worker status alone does not revoke a live lease.
4. Apply the explicit Attempt SUCCEED/FAIL event. Success applies Task
   ATTEMPT_SUCCEEDED; failure currently applies FAIL_PERMANENTLY. The Worker cannot
   specify retry time, LOST or TIMED_OUT. M4 will add a server retry policy before
   selecting RETRY_SCHEDULED; this version deliberately has no implicit retry.
5. Persist Attempt and Task statuses and compare returned identities/statuses with
   the expected snapshots. Insert the receipt, reconstruct and validate it, and
   require exact equality with the proposed receipt. Unexpected trigger/write
   results raise an error so the caller rolls back.

Attempt, Task and receipt become visible together at COMMIT. Capacity is counted
from RUNNING Attempts, so ending the Attempt releases its slot without a separate
counter update. Lease metadata, Worker heartbeat, claim bindings, other Tasks and
Run status are unchanged. A new poll can use freed capacity for another READY Task.
Downstream dependency resolution and Run aggregation remain separate milestones:
even a completed one-task Run still shows RUNNING until aggregation exists.

## Receipt replay

Read and validate the retained receipt **before** checking current running state.
Its original owner must match the lease, its result must match the immutable
terminal Attempt, and accepted_at must be a valid historical observation within
the retained lease interval. Broken relationships, result shape or historical time
produce StoredCompletionError, never a successful replay.

Then compare the submitted Attempt/session/token and normalized result with the
receipt. Matching reports return the original terminal Attempt and accepted_at.
The code neither samples a current clock nor writes anything on replay. Worker
shutdown, an expired lease or a settled Run does not prevent historical confirmation.
The Task's current state is not copied into the receipt; later retry policy may
advance a failed Task while keeping its earlier Attempt receipt intact.

Replaying an old receipt after the Worker claims another task cannot release that
new task's slot. Claim replay is different: a terminal allocation returns
ClaimReplayUnavailableError and is no longer executable ownership. Completion
replay confirms history; it does not issue a grant.

If COMMIT succeeded but the response was lost, resend the exact original report.
Do not replace its error code, switch success to failure, or generate another
Attempt. A mismatched owner raises LeaseOwnershipError; a changed result raises
CompletionConflictError. The database primary key enforces one receipt but is not
itself the comparison/replay implementation.

## Errors and limits

| Error | Meaning |
| --- | --- |
| CompletionNotFoundError | Requested Attempt has no visible ownership, including unleased historical Attempts. |
| CompletionInactiveError | No receipt exists and Run/Task/Attempt is not RUNNING, or Worker is STOPPED. |
| LeaseOwnershipError | Submitted session/token does not match first acceptance or retained receipt. |
| LeaseExpiredError | New completion observes time at or beyond the exclusive deadline. |
| LeaseClockRegressionError | New completion observes time before the latest lease renewal. |
| CompletionConflictError | Same owned Attempt was reported with a different result. |
| StoredCompletionError | Stored ownership, receipt or returned transition is invalid/inconsistent. |
| RepositoryTransactionError | Missing, ended/replaced or unsupported transaction. |
| ValidationError | Invalid submitted model, rejected before operation SQL. |

First-completion state checks precede submitted ownership/time checks. A terminal
Attempt without a receipt cannot be treated as a successful duplicate, including
an Attempt ended by recovery as LOST/TIMED_OUT. No implicit lease takeover occurs.

Database errors, connection failures and timeouts propagate without retries. Domain
storage-error messages do not echo tokens or row values, but raw driver exceptions
can contain SQL parameters. A later HTTP adapter must use sanitized error logging;
do not log internal receipt dumps or driver exception messages. Tokens remain in
the internal receipt for ownership comparison and are hidden from its repr.

Database wall-clock jumps and delayed COMMIT/response delivery remain lease
trade-offs. accepted_at is an observation timestamp, not a proof that the handler
finished then. The implementation does not enforce a task execution timeout,
terminate a process or guarantee exactly-once external effects.

## Runnable database example

With PostgreSQL migrated to `0008` and a dedicated application dotenv file:

```console
uv run --locked python examples/complete_attempt.py --env-file .env
uv run --locked python examples/complete_attempt.py --env-file .env --outcome FAILED
```

Each invocation creates a fresh one-task Workflow/Run/Worker, claims its task,
submits a **simulated** outcome, commits, and replays the same report in another
transaction. It never borrows a token from an unrelated row or prints one. Expect
`Original receipt replayed: True`. Attempt and Task settle; Run stays RUNNING.
These are metadata examples and do not invoke demo.echo or any business handler.

Repeating the program creates new demo data, not a retry of an interrupted earlier
invocation. If interrupted, retained data and possibly a RUNNING Attempt may remain;
there is no automatic recovery scanner yet. The program keeps its request identity
and token only in memory and is not a crash-resilient Worker implementation.

## Verification and remaining transaction work

```console
uv run --locked pytest tests/integration/test_completions.py --database-env-file .env.database-test
```

M2.3c.1 tests committed/uncommitted visibility, success/failure Task settlement,
capacity reuse without duplicate release, current claim rejection, exact time and
ownership boundaries, terminal history, expired heartbeat/LOST owner, historical
confirmation, competing same/different reports, write/deferred-COMMIT failure,
explicit abort, unexpected trigger results, corrupt receipts and transaction modes.

M2.3c.2 adds `tests/integration/test_completion_races.py` with 19 controlled cases:

- Block each of the five ownership locks, assert the clock is not sampled while
  waiting, then release the lock and reject completion at the exact deadline.
- Time out at each lock, roll back, and successfully retry the same report.
- Let renewal commit or roll back before a waiting completion: only the committed
  deadline extension authorizes completion at the original expiry boundary.
- Let completion commit or roll back before renewal or a conflicting report.
- Simulate recovery at expiry: a committed LOST outcome fences the old report;
  rollback leaves the original lease expired and still rejects the report.
- Let completion commit before recovery's state check; the latter cannot overwrite
  the terminal Attempt or remove its replayable receipt.

Tests coordinate with held locks and events, not timing sleeps. Recovery-like SQL
is test-only and does not implement a recovery scanner. No production behavior
needed changing. M2.3d now exposes [completion HTTP](completion-api.md), including
commit-before-success responses and token-free historical receipts.
