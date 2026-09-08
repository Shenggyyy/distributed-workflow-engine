# Attempt leases and task claim protocol

M2.2a implements a pure `AttemptLease` snapshot, ownership/time checks, monotonic
renewal, unit tests and an in-memory example. M2.2b adds
[lease storage](lease-storage.md) in revision `0006`. M2.2c.1 implements the
[single-run claim transaction](task-claims.md), and M2.2c.2 adds
[persisted lease renewal](lease-renewal.md). M2.2d.2a adds [claim HTTP](claim-api.md).
Renewal HTTP and
execution loops remain later work.
The transaction protocol below is the design for subsequent commit-sized steps.

## Implemented model

| Field | Meaning |
| --- | --- |
| attempt_id | UUID of the TaskAttempt whose execution is authorized. |
| worker_session_id | UUID of the owning Worker process incarnation. |
| lease_token | Independent UUID identifying this ownership assignment. |
| acquired_at | Database observation when ownership was allocated. |
| last_renewed_at | Last accepted lease observation; initially acquired_at. |
| lease_expires_at | Exclusive deadline for ownership. |

All fields are required and immutable. Python construction requires UUID objects
and aware datetime objects; JSON round trips accept UUID/ISO timestamp strings.
Times normalize to UTC before comparison, with
`acquired_at <= last_renewed_at < lease_expires_at`. Naive times, unknown fields
and unrepresentable UTC instants are rejected. The model does not generate IDs,
read a clock, validate foreign keys or allocate ownership.

`require_valid_owner(...)` compares the supplied attempt UUID, session UUID and
token with this snapshot, then checks the supplied observation. Wrong identity
raises `LeaseOwnershipError`; time before `last_renewed_at` raises
`LeaseClockRegressionError`; time at/after the deadline raises `LeaseExpiredError`.
Messages do not include supplied tokens. Method arguments do not coerce UUID
strings or local/naive times. This is a predicate on a snapshot, not authentication
or permission to execute a state transition.

`renew(..., lease_seconds=...)` applies the same checks and returns a new snapshot:

```text
last_renewed_at = observed_at
lease_expires_at = max(old_deadline, observed_at + lease_seconds)
```

The duration is a strict integer from 1 to 86400 seconds. It is a caller-supplied
server policy, separate from the Worker heartbeat setting. A shorter policy
cannot reduce an existing deadline. An equal observation is allowed; an expired
lease cannot be revived by asking for a longer duration. Datetime arithmetic
overflow is rejected explicitly. Every operation revalidates the snapshot, so a
structurally invalid `model_copy(update=...)` cannot bypass checks. A forged but
structurally valid snapshot is indistinguishable without authoritative storage.

Renewal keeps the attempt, session, token and acquired time unchanged. Lease
expiry is not a mutation on this object: recovery will persist the existing
`AttemptEvent.LEASE_EXPIRED` transition to LOST and decide the Task outcome in
one transaction. Metadata can remain in storage after an attempt is terminal;
the model alone cannot know that it must no longer renew.

## Ownership, execution and fencing

Every new claimed attempt will get a fresh attempt UUID, sequential per-task
attempt number, fresh random lease token and owning Worker session UUID. A retry
creates a new attempt and lease; it never resets an old attempt. There is no
in-place transfer between sessions and no token rotation during renewal.

M2.2c.2 checks renewal against persisted ownership. Both renewal and future
result handling must load the
current RUNNING Task and its RUNNING Attempt under locks, verify their relation,
load that attempt's lease and check the submitted tuple against database time.
An obsolete attempt/token cannot authorize a newer attempt. The existing unique
RUNNING-attempt-per-task index is a backstop, not a substitute for these checks.

The UUID token supports equality-based rejection of stale engine requests. It is
not a monotonically increasing fencing number that an external storage service
can order. Per-task attempt numbers are also not a global fencing sequence.
External side effects require a cooperating idempotency key, such as the logical
TaskRun UUID, or a separately designed external fencing protocol. Avoid logging
lease tokens or including them in general-purpose public run queries.

An unexpired lease does not prove that a handler started, is progressing or is
the only process still running. After expiry, a replacement attempt may overlap
with an old handler. A successful lease check on an in-memory snapshot is not
proof of at-least-once delivery, exactly-once execution or durable ownership.

## Planned transaction boundaries

Start with a single-run, one-task claim primitive. Global candidate discovery,
batch claims, routing and fairness are separate work. The planned lock order is:

```text
WorkflowRun row -> WorkerSession row -> TaskRun row -> TaskAttempt / lease row
```

Use one Run and one Worker per transaction initially. Every operation that changes
an owned attempt's state, including completion and recovery, follows the same
order. A read used to discover the immutable run/session identities is only a
hint; re-read state after the locks. Never lock an attempt first and then its Run.
Cross-run operations must finish their transaction before choosing another Run.

Existing Worker registration takes its registration advisory lock then the Worker
row; heartbeat/expiry take only the Worker row. They must not acquire a Run lock
while holding that Worker lock. This avoids a reverse edge against claim/completion.
Future graceful shutdown must obey the same rule rather than locking all owned
Runs after its Worker. All paths need real PostgreSQL race tests when implemented.

M2.2c.1 implements the following claim transaction; see [task claims](task-claims.md)
for exact results, errors and implemented verification:

1. Lock the Run and require RUNNING.
2. Lock the Worker session and sample database time after all relevant waits.
   Require ACTIVE and an unexpired heartbeat; registration replay is insufficient.
3. Under the Worker lock, count its outstanding RUNNING owned attempts across
   Runs. They occupy capacity until completion/recovery commits, even if a lease
   deadline has passed. All creators/finalizers of owned attempts must participate
   in this lock protocol. Reject or return no claim if capacity is exhausted.
4. Select and lock one READY task in this Run, recheck its state, and allocate
   the next attempt number without exceeding the PostgreSQL integer range. A task
   in RUNNING cannot be claimed just because its prior lease appears expired.
5. Sample authoritative time after the acquired locks, recheck session admission,
   and atomically persist READY -> RUNNING, the new RUNNING Attempt and its lease.
6. Commit before returning an execution grant. Handler work happens after commit,
   outside the database transaction. Return no executable grant on rollback.

Capacity counting deliberately favors avoiding oversubscription over immediate
slot reuse after lease expiry. It requires recovery to release abandoned attempts.
Per-run locking also limits control-plane throughput within a large Run; handler
execution remains concurrent because locks are held only during metadata changes.

Under READ COMMITTED, subsequent statements can observe newly committed changes;
the protocol requires separate post-lock reads rather than trusting one earlier
joined snapshot. See [PostgreSQL transaction isolation](https://www.postgresql.org/docs/18/transaction-iso.html).
Use `clock_timestamp()` after lock acquisition for deadlines; transaction-start
time can be stale after a wait. See [PostgreSQL current date/time functions](https://www.postgresql.org/docs/18/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT).
Future multi-run polling may use `SKIP LOCKED` for bounded candidates. It offers
neither a consistent global view nor fairness: an empty poll is not proof of no
ready work. See [PostgreSQL locking clauses](https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE).

## Failure and retry contracts for later storage/HTTP work

| Scenario | Required behavior |
| --- | --- |
| Claim transaction rolls back or API crashes before commit | No grant is published; Task remains claimable and no owned Attempt survives. |
| Claim commits but response is lost | The Attempt and capacity reservation survive. The keyed repository and claim HTTP retain a binding; retry the same request ID to retrieve current valid ownership, or an unavailable outcome. |
| Worker crashes after receiving a grant | Lease recovery eventually finalizes the abandoned Attempt and releases capacity; no guarantee until that scanner exists. |
| Worker heartbeat expires but attempt lease is still valid | Reject new claims. The original owner may renew/report against its still-valid attempt lease, subject to Task/Attempt state and future task timeout checks. |
| Lease expires while heartbeat remains healthy | Reject renewal/new completion; recovery decides LOST and retry/failure. Heartbeat does not extend task leases. |
| Old completion races recovery | Serialize on the same authoritative state; accept a new result only with current RUNNING ownership and a valid lease. |
| Successful completion response is lost | Later result handling must identify an identical persisted terminal result and replay it without applying another transition; conflicting results must fail. |
| Clock moves backwards | Reject observation; do not fabricate a newer timestamp or renew silently. Forward jumps may cause false expiry. |
| A lease check succeeds but the transaction/network is slow | Deadline validity was established at observation time, not forever through response delivery; keep transactions bounded. |

The [keyed claim repository](idempotent-claims.md) and [HTTP endpoint](claim-api.md)
now implement durable `(worker_session_id, request_id)` bindings, retained no-work,
current ownership replay and the outer request-lock order. Expired grants are
never returned as executable ownership. These are transaction-level guarantees,
not guarantees of the pure `AttemptLease` model.

A task execution timeout is an independent absolute deadline. Lease renewal must
not reset it; exact start time and simultaneous timeout/lease-expiry outcome
precedence belong to the timeout milestone. This model has no timeout field and
does not claim to enforce execution duration. Physical process termination and
business idempotency remain separate responsibilities.

## Commit-sized implementation plan

| Subtask | Scope and verification |
| --- | --- |
| M2.2a (implemented) | Pure lease snapshot, owner/time/renewal boundaries, example and this protocol. |
| M2.2b (implemented) | Attempt lease storage and migration, identity/time/history guards, foreign keys and PostgreSQL tests. Explicitly handle pre-existing Attempts without inventing owners. |
| M2.2c.1 (implemented) | Single-run claim transaction, worker admission/capacity and atomic Task/Attempt/lease creation; concurrency/rollback tests. |
| M2.2c.2 (current) | Lease renewal against current persisted ownership; post-lock clock and stale-owner tests. |
| M2.2d.1a | [Durable claim request schema and replay protocol](claim-requests.md), complete. |
| M2.2d.1b | [Keyed claim/replay transactions](idempotent-claims.md) with uncertain-outcome tests, complete. |
| M2.2d.2a | [Claim HTTP](claim-api.md), commit/error mapping and real HTTP checks, complete. |
| M2.2d.2b | Renewal HTTP contracts, error mapping and real HTTP checks. |

Only proceed after each subtask's tests, owner commit/push and CI confirmation.
Completion, handler execution, recovery and task retry/timeout each need their own
subtasks. No Worker should launch a handler before durable claims and completion
contracts are connected end to end.

## Runnable example and verification

```console
uv run --locked python examples/attempt_lease.py
uv run --locked pytest tests/test_lease.py
```

Expected output:

```text
Renewal preserves token: True
Deadline after renewal: 40s
Exact deadline rejects the old owner.
A restarted Worker cannot inherit the lease.
In-memory lease checks only; no task claim, persistence or execution.
```

No Docker or PostgreSQL is needed. Tests use explicit times rather than sleeping:
exact deadline and one microsecond on either side, ownership mismatch, backwards
time, duration limits/coercion, shortened policy, UTC normalization, datetime
overflow, frozen required fields, JSON round trips and bypass revalidation.
Storage constraint/concurrent-write tests are now covered in M2.2b.
Claim protocol races are covered in M2.2c.1; renewal races are covered in M2.2c.2.
