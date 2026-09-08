# Transactional Worker registration

M2.1c.1 implements `WorkerRepository.register()` using revision `0005`.
It adds no migration, HTTP route, heartbeat loop, expiry scan or task execution.

## Request and result

```python
from workflow_engine.repositories.workers import WorkerRepository

with engine.begin() as connection:
    snapshot = WorkerRepository(connection, heartbeat_timeout_seconds=30).register(
        session_id, worker_name="worker_local", max_concurrency=2
    )
# Only here has the caller committed.
print(snapshot.session.id)
```

The UUID identifies one process start. It must be a Python UUID object. Name and
capacity use the strict [Worker domain rules](workers.md). The method always
creates ACTIVE for a new UUID; callers cannot submit a status or timestamps.
Invalid fields are rejected before a registration lock or write.

The frozen StoredWorkerSession result contains:

- session: the immutable WorkerSession (id, worker_name, max_concurrency, status).
- created_at, last_heartbeat_at and heartbeat_expires_at: timezone-aware datetimes.

Same UUID/name/capacity returns the currently stored snapshot. It does not update
any column. A different name (including different case) or capacity raises
WorkerRegistrationConflictError with a fixed message. Distinct UUIDs with the
same name are distinct sessions and may have different capacities.

A replay is **not an immutable original response receipt**: it can observe a
later persisted status or heartbeat time. Registration itself never causes that
change. In particular, LOST/STOPPED stays terminal and an expired ACTIVE row stays
expired. Replaying registration is not a successful liveness renewal or proof of
claim eligibility. The [heartbeat repository](worker-heartbeat.md) checks deadlines;
future claims must do so separately too.

Corrupt stored domain fields or time ordering raise StoredWorkerError without
echoing stored values. Driver/schema failures propagate separately. No new HTTP
error mapping is introduced before the Worker HTTP milestone.

## Atomic registration and clock ordering

The repository requires its original active PostgreSQL READ COMMITTED transaction,
with DBAPI autocommit disabled. This mirrors the existing repository contract.
The registration operation needs a read-write transaction because it takes row
locks and may insert. A repository instance is never shared across threads or
reused after commit/rollback, including inside a replacement transaction.

Registration proceeds as follows:

1. Validate inputs and acquire a transaction-level advisory lock for the session.
2. In a separate SQL statement, SELECT that UUID FOR UPDATE.
3. If present, validate and compare its immutable fields, then return the snapshot
   or raise a conflict. No fresh clock is sampled and no state is changed.
4. If absent, sample PostgreSQL clock_timestamp() once after lock acquisition.
   Use it for both created_at and last_heartbeat_at. Add the configured timeout
   to produce heartbeat_expires_at, insert the ACTIVE row, and decode RETURNING.
5. The caller commits or rolls back. All locks are released when that transaction
   ends; returning from register() is not a successful COMMIT.

The initial timeout is server policy injected through the repository constructor:
a strict integer from 1 to 86400 seconds, default 30. The one-day limit bounds this
initial liveness window, not task duration. It is not a worker-supplied request
field. Changing the server policy does not conflict with an existing registration
and cannot change its deadline. Runtime environment settings will be wired when
the API/heartbeat components consume this policy.

The clock is sampled after registration contention. If an owner rolls back,
a waiting contender inserts with a fresh observation instead of using its old
transaction-start time. If the owner commits, the waiter sees and returns that
owner's record through the next READ COMMITTED statement snapshot.

The deadline still begins at the database observation, not commit acknowledgment.
A caller can hold its transaction long enough that the new row is already expired
at commit. Keep transactions short. Database wall-clock changes and process pauses
remain liveness limitations; registration never promises the worker is alive.

## Advisory lock identity and trade-offs

The advisory key uses PostgreSQL's two-int form:

```text
namespace = 0x44574557
key = signed 32-bit BLAKE2s digest of the UUID's 16 bytes
```

The namespace/hash are stable protocol choices; Python's randomized hash() is
not used. The two-int advisory space is separate from the one-bigint migration
lock. Lock scope is the database; it is not restricted by a SQL schema.

Hash collisions serialize unrelated registrations; they do not merge records or
cause false registration conflicts because row lookup and comparison use the full
UUID. Tests deliberately force a collision. Most distinct sessions proceed
independently, and names never determine the lock key.

The advisory lock is needed before a session row exists. Locking a SELECT for an
absent UUID alone would not coordinate concurrent first registration. Choosing
INSERT ON CONFLICT with already-computed times would risk a stale observation
after waiting for a competing insert that rolls back.

This costs one extra SQL lock round trip and holds an advisory lock even on
replay. Existing-row FOR UPDATE additionally coordinates snapshot reads with
heartbeat writes. It is accepted at the expected MVP registration rate.
The primary key remains the database uniqueness backstop.

All cooperating registrants must use this protocol and acquire locks in this
order: registration advisory lock, then session row lock. Heartbeat/expiry code
only locks an existing row and must not subsequently request its registration
advisory lock. Prefer one registration per transaction. A future batch operation
must define global lock ordering; none is implemented here.

Direct SQL writers that ignore advisory locks are outside the protocol. They
cannot violate the UUID primary key, but can cause an IntegrityError between our
lookup and insert; that error is propagated rather than silently retried.
Ordinary table locks, pool limits and existing database timeouts still apply.

## Failure behavior

| Scenario | Behavior |
| --- | --- |
| Concurrent identical registrations | One insert; waiters return its committed snapshot. |
| Concurrent conflicting registrations | One request wins; incompatible requests raise conflict. |
| Owner rolls back before commit | UUID remains available; waiter can create with fresh database time. |
| Insert or deferred COMMIT fails | Transaction rolls back; same UUID can be tried again. |
| Lock timeout | Database error propagates; caller must roll back, no automatic retry. |
| Commit succeeded but response was lost | Retry same UUID/name/capacity to retrieve current state. |
| Replayed old or terminal registration | Return stored state; never renew or reopen. |

SQL errors must leave the transaction or be handled with an explicit savepoint.
Do not swallow errors and treat provisional results as durable. There is no
automatic COMMIT-outcome inference. Tests cover acknowledged commit followed by
replay and injected COMMIT failure; they do not simulate a physical network loss
during commit.

## Runnable example and checks

With PostgreSQL configured and migrated to 0005, run from the repository root:

```powershell
$env:DWE_DATABASE_PORT = "15432"
$env:DWE_DATABASE_PASSWORD_FILE = "secrets/postgres_password.txt"
$sessionId = [guid]::NewGuid().ToString()
uv run --locked python examples/register_worker.py --session-id $sessionId
uv run --locked python examples/register_worker.py --session-id $sessionId
```

Both calls show the same UUID, creation time and deadline unless another writer
has advanced the session heartbeat in between. Explicit heartbeat calls can now
change these times; no background heartbeat loop runs yet.
Changing --name or --max-concurrency while retaining the UUID raises a conflict.
Use a new UUID for a new process start. The example persists a session and prints
only after commit; it does not launch a long-running worker or delete history.

The example also accepts --env-file, like other database examples. Expected
output shape:

```text
Session ID: <UUID>
Worker name: worker_local
Stored status: ACTIVE
Created at: <database timestamp>
Heartbeat expires at: <database timestamp plus 30 seconds>
Registration committed; replay does not renew liveness or prove eligibility.
No heartbeat loop, task claim or execution.
```

```console
uv run --locked pytest tests/integration/test_worker_registration.py --database-env-file .env.database-test
```

Tests cover concurrent identical/conflicting requests, owner commit/rollback,
fresh time after lock wait, timeout propagation, deliberate hash collisions,
independent UUIDs, expired/terminal replay, changed server policy, validation,
transaction lifetime/isolation, injected insert/commit failures and corrupt state.
General CI discovers these tests without a new workflow step.

M2.1c.2 implements [heartbeat renewal and expiry transactions](worker-heartbeat.md).
Worker HTTP endpoints remain M2.1d.
