# Worker session storage

M2.1b adds revision `0005` and the `worker_sessions` table. It implements
database invariants for the [M2.1a session model](workers.md), not the registration
or heartbeat protocol by itself. M2.1c.1 now provides
[transactional registration](worker-registration.md), M2.1c.2 adds
[heartbeat/expiry transactions](worker-heartbeat.md), and M2.1d exposes
[registration/heartbeat HTTP routes](worker-api.md). Background expiry is later work.

## Columns and constraints

| Column | SQL type | Contract |
| --- | --- | --- |
| id | UUID primary key | One process incarnation; caller supplies identity. |
| worker_name | varchar(64), C collation | Existing ASCII Identifier rules; deliberately not unique. |
| max_concurrency | integer | Positive declared capacity; immutable, not enforced task admission. |
| status | varchar(16), C collation | ACTIVE (default), LOST or STOPPED. |
| created_at | timestamptz | Finite registration timestamp, immutable. |
| last_heartbeat_at | timestamptz | Finite last accepted liveness observation. |
| heartbeat_expires_at | timestamptz | Finite session heartbeat deadline, not a task lease. |

Every column is NOT NULL. PostgreSQL INTEGER supplies the upper capacity bound
2147483647. The Python model additionally rejects booleans and numeric coercions;
SQL constraints do not replace validation of external requests.

The clock fields have **no defaults**. The registration transaction must
sample database time after acquiring its required locks and explicitly supply
created_at, last_heartbeat_at and the computed deadline. Independent defaults
would obscure the observation used to derive that deadline. Registration should
initially use the same observation for created_at and last_heartbeat_at.

Row checks require:

```text
isfinite(created_at), isfinite(last_heartbeat_at), isfinite(heartbeat_expires_at)
created_at <= last_heartbeat_at < heartbeat_expires_at
```

Infinity values cannot create an immortal or undecodable session. The checks
compare stored values, not the current clock. An ACTIVE row with a deadline in
the past is valid storage; future admission and expiry transactions must handle
it. No clock-dependent CHECK or implicit state change runs during a read.

No foreign key to attempts is introduced: attempt ownership will need its own
atomic claim migration/protocol. No stable worker-name ownership or global
capacity limit is inferred from this table.

## Update and retention guards

The migration installs a row-level BEFORE UPDATE trigger:

- id, worker_name, max_concurrency and created_at cannot change.
- Status may change only ACTIVE → LOST or ACTIVE → STOPPED.
- While both old and new status are ACTIVE, heartbeat times may stay equal or
  advance, but neither may move backwards. Row ordering still applies.
- Heartbeat times cannot change in the same update that terminates a session.
  Once LOST/STOPPED, all its fields remain unchanged.
- An exactly unchanged SQL row is allowed. This is not a repeated domain event
  or a registration/heartbeat replay implementation.

The trigger inspects the row version PostgreSQL actually updates. Tests hold an
uncommitted terminal update while another connection attempts heartbeat renewal;
after the owner commits, that waiting renewal is rejected rather than writing
fresh times into the terminal session. This establishes a structural serialization
property, not a complete race-tested heartbeat repository.

Separate BEFORE DELETE OR TRUNCATE statement triggers protect retained session
identities, including DELETE WHERE false and empty-table operations. The MVP has
no TTL or purge path; retaining old UUIDs also allows later registration replay
logic to distinguish an old session from an unknown one. Long-term cleanup needs
an explicit retention and identity-reuse contract.

INSERT may store any valid status and ordered finite timestamps, including a
terminal historical snapshot. Like the other runtime tables, this supports
rehydration and does not prove historical transitions. The registration
repository explicitly chooses ACTIVE. UPDATE cannot bypass terminal-state
rules. A table owner/superuser able to alter triggers is outside these guards.

Guard violations use SQLSTATE 55000 with fixed messages; NOT NULL, CHECK and
primary-key violations use normal PostgreSQL constraint errors. Transaction
callers must propagate failures or deliberately roll back a savepoint. The schema
does not implement request retry.

## Deadline index and trade-offs

A non-unique partial B-tree index covers:

```sql
(heartbeat_expires_at, id) WHERE status = 'ACTIVE'
```

It supports future bounded expiry scans ordered by deadline with an ID tie-breaker;
terminal rows leave the index. It does not run a scan, reserve a batch or ensure
that a small-table query planner chooses it. Updating a deadline adds index write
work and can prevent HOT updates. This is the accepted cost of indexing the
specific recovery predicate; no speculative worker-name or capability indexes
are added.

Monotonic stored heartbeat times reject regressions caused by a stale writer or
backwards wall-clock adjustment. M2.1c.2 [heartbeat/expiry transactions](worker-heartbeat.md)
reject a database observation earlier than the last heartbeat and check the
previous deadline after locking. The constraints alone do not solve clock skew.
Advancing an already expired ACTIVE row through direct SQL is structurally
possible; the heartbeat repository rejects that action.

Graceful shutdown needs a coordinated no-active-attempts check; LOST needs an
authoritative expiry check. Neither is a trigger precondition yet. The table
does not enforce live slot counts, lease ownership, worker identity authentication
or exactly-once external effects.

## Migration and verification

With local credentials initialized and Docker Desktop running:

```powershell
$env:DWE_POSTGRES_PUBLISHED_PORT = "15432"
docker compose up --build --wait --wait-timeout 120
docker compose exec api alembic upgrade head
docker compose exec api alembic current
docker compose exec api alembic check
```

Rebuild the image before running its migration commands so it contains revision
0005 or later. See [migrations](migrations.md) for the current head;
expect `No new upgrade operations detected.`
The upgrade creates only the new table, index and guard functions/triggers.
Existing definitions, runs, tasks, attempts and request bindings are preserved.

**Downgrading 0005 → 0004 deletes all Worker session data.** It preserves the older
workflow/runtime/idempotency tables. Re-upgrading creates an empty session table;
it cannot restore session history. Do not use downgrade as crash recovery.
Future attempt foreign keys may require a different downgrade plan.

```console
uv run --locked pytest tests/integration/test_worker_schema.py --database-env-file .env.database-test
```

Use the dedicated test configuration in [database.md](database.md#tests).
Tests migrate private schemas and verify defaults/rehydration, same-name sessions,
nulls, invalid values, finite/ordered time boundaries, every status pair,
immutability, heartbeat monotonicity, terminal freeze, retained history, rollback,
concurrent duplicate UUIDs, blocked renewal after termination, the partial index,
metadata comparison and a populated 0004 → 0005 → 0004 → 0005 round trip.

The round trip preserves an existing workflow, run, task and request binding,
including replaying the old run-creation key. General migration tests also cover
DDL rollback and revision locking. Alembic `check` alone does not compare all
triggers and CHECK constraints; behavioral tests are required. Always use
migrations rather than `metadata.create_all()`, which omits the trigger guards.

M2.1c.1 implements [transactional registration](worker-registration.md) using
this table. M2.1c.2 adds [heartbeat renewal/expiry](worker-heartbeat.md).
M2.1d adds [Worker registration and heartbeat HTTP endpoints](worker-api.md).
