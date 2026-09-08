# Idempotent claim transactions and current ownership replay

M2.2d.1b implements the protocol in [claim request storage](claim-requests.md), on
schema `0007`. `ClaimRequestRepository` wraps the existing single-Run allocator
and adds durable request lookup, current-ownership replay and conflict handling.
This repository commit adds no migration, automatic retry or Worker loop.
M2.2d.2a now exposes it through the [claim HTTP API](claim-api.md).

## Python contract

```python
from workflow_engine.repositories.claim_requests import ClaimRequestRepository

# Generate request_id once before a new poll; retain it across uncertain retries.
with engine.begin() as connection:
    result = ClaimRequestRepository(connection, lease_seconds=30).claim_next(
        run_id, worker_session_id, request_id=request_id
    )
# Do not publish/use result until this transaction commits successfully.
```

All three IDs must be UUID instances. The duration is a server-owned strict
integer, 1 through 86400 seconds, default 30. It applies to new allocations only.
Every operation uses a fresh PostgreSQL READ COMMITTED transaction with driver
autocommit disabled. Instances cannot outlive their original transaction or be
shared between threads. The caller must roll back the transaction/savepoint after
any failure. A Python return remains provisional until COMMIT.

| Outcome | Meaning |
| --- | --- |
| TaskClaim | New allocation or current valid ownership of the original allocation. |
| None | A newly committed no-work decision, or the same retained no-work result. |
| ClaimRequestConflictError | This session/request identity is already bound to another Run. |
| ClaimReplayUnavailableError | The bound lease expired, execution is not RUNNING, or the Worker is STOPPED. |
| StoredClaimRequestError | Invalid request data, missing/corrupt ownership, or Run/Worker mismatch. |
| LeaseClockRegressionError | The replay observation precedes the last accepted lease observation. |

New allocations retain the [unkeyed allocator's admission errors](task-claims.md),
including missing/inactive Run or Worker, expired heartbeat, invalid dependency
state and exhausted attempt numbers. Stored workflow errors, SQL/lock/commit
failures and transaction errors propagate. Error messages do not echo tokens or
supplied IDs. HTTP status mappings are documented in [claim API errors](claim-api.md#error-mapping).

## Request locking and atomic allocation

The outer advisory lock uses PostgreSQL's **two signed 32-bit integer** interface:

- Namespace: `0x44574543` (DWEC). Worker registration uses `0x44574557`; migrations
  use PostgreSQL's separate bigint advisory-key space.
- Key: BLAKE2s with digest_size=4 over `worker_session_id.bytes + request_id.bytes`,
  interpreted as a signed big-endian integer. Both UUID byte strings are 16 bytes.

The namespace, input order and hash must remain stable across deployments sharing
the database. Tests freeze known vectors. Hash collisions may reduce concurrency
but do not merge requests: a post-lock query uses the full composite primary key.
The lock is transaction-scoped, so commit/rollback or connection loss releases it.
It is not a durable receipt; the binding table is the durable record.

The sequence is `request advisory lock -> binding lookup -> Run -> Worker -> Task
-> Attempt -> lease`. A lookup statement after the advisory wait sees the previous
transaction's commit under READ COMMITTED. On a miss, the existing allocator makes
its admission/capacity/dependency checks. One final binding is inserted in that
same transaction, with a fresh database-clock audit time. There is no persistent
IN_PROGRESS reservation and no internal commit.

Never begin this operation while already holding other control locks, combine
unrelated requests into one transaction, or execute handlers under these locks.
The repository cannot inspect arbitrary locks acquired by the caller. Other
operations retain their Run/Worker/Task/Attempt/lease order and do not acquire the
request lock. Schema guards remain backstops; bypassing the protocol with raw SQL
is outside the concurrency guarantee.

## Replay is allocation identity, not a new execution

A matching no-work decision stays no-work when capacity frees up or READY work
appears. It may be replayed after the Run or Worker becomes terminal because it
grants no execution authority. The next actual poll uses a new request UUID.
A different Run under the same identity is always a conflict, even for no-work or
expired execution. Generating a fresh key after an uncertain response would skip
the very deduplication this API provides.

For an Attempt binding, the shared `_ownership` helper discovers immutable
references, locks/re-reads Run, stored Worker, Task, Attempt and lease, and checks
their models/relationships. This is the same helper used by lease renewal.
The bound Run and Worker must match. Run, Task and Attempt must be RUNNING, and
Worker must not be STOPPED. The one-RUNNING-Attempt-per-Task index establishes
current Attempt identity; a replacement's lease cannot authorize the old Attempt.

The repository reads the Task definition from the pinned immutable workflow,
then samples `clock_timestamp()` after all locks and reads. It requires observation
>= last_renewed_at and < lease_expires_at. Equality at the deadline is expired.
It neither allocates nor updates lease/request timestamps on a hit. Existing live
ownership can replay despite an expired heartbeat or LOST Worker; those conditions
still prohibit new allocations. Replay does not reactivate or heartbeat a Worker.

An independently committed renewal may change the returned lease snapshot; its
token and Attempt remain the same. Responses are therefore not byte-identical
receipts. Expired/terminal ownership returns ClaimReplayUnavailableError, never
None or a replacement grant. No lease is resurrected. Clock regression is a
separate error because it does not establish that the stored lease expired.

The schema deliberately does not enforce Run membership, so a mismatched binding
is rejected as corruption. Request IDs are not credentials. The local repository assumes its caller is authorized to retrieve the session's
grants. The HTTP API currently shares that trusted local boundary; authentication
and per-session access control remain future work.

## Failure boundaries and costs

An INSERT failure, explicit abort or deferred COMMIT failure rolls back binding,
Task transition, Attempt and lease together. A waiting duplicate can then allocate
after rollback. If the commit succeeds but the response is lost, retrying the same
identity retrieves its retained outcome. A lock timeout propagates and the caller
must roll back; no partial result is executable. There is no automatic retry loop.

The database clock is checked at observation time, not continuously through commit
or response delivery. A returned snapshot may already be expired upon arrival.
Future Worker execution must respect lease/renewal rules and avoid starting another
local handler for a replayed Attempt. This commit prevents duplicate allocation
for a retained request, not duplicate handler execution or external side effects.

Per-Run and per-Worker locks still limit control-plane concurrency. Hash collisions
can add unrelated waits. Every no-work poll creates retained history; no cleanup,
retention window, polling backoff or throughput claim is introduced. Task timeout,
completion and recovery remain separate work. Downgrading/removing bindings loses
deduplication guarantees as described in [storage](claim-requests.md).

## Local example and verification

Create a fresh Run and Worker session with [the claim setup](task-claims.md#local-example).
Replace its final `claim_task.py` command with these lines, so the request ID stays
available for a retry:

```powershell
$requestId = [guid]::NewGuid().ToString()
uv run --locked python examples/claim_idempotent.py --run-id $run.run_id --session-id $sessionId --request-id $requestId
```

The example commits one claim, then issues the same request in another transaction.
Expect `Same request replayed the same Attempt: True`, followed by confirmation
that no handler executed or lease renewed. Tokens are not printed. It also accepts
`--env-file`. Re-running with the same IDs replays that allocation while it is
valid; after expiry it raises ClaimReplayUnavailableError. Use disposable Runs
and sessions: successful claims reserve capacity until future completion/recovery.

```console
uv run --locked pytest tests/test_claim_request_protocol.py
uv run --locked pytest tests/integration/test_claim_requests.py --database-env-file .env.database-test
```

Tests cover lost responses after commit, atomic rollback/deferred failures,
duplicate and conflicting concurrent requests, forced hash collisions, scoped
identities, sticky no-work, post-lock clocks at all six lock positions, current
renewal snapshots, terminal/replacement Attempts, liveness boundaries, corruption,
timeouts and transaction lifetime. Terminal/retry mutations simulate future
control operations; production completion/recovery is not implemented by tests.

M2.2d.2a adds the [claim HTTP contract](claim-api.md) and commit/error mapping.
M2.2d.2b adds [renewal HTTP](lease-api.md) in its own independently reviewable
commit. Claim replay still reads the current lease without extending it.
