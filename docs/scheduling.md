# Transactional dependency scheduling

M2.5a implements pure readiness proposals and `SchedulingRepository.reconcile`.
It reuses schema `0008`; no new table, queue or migration is required. READY Task
rows are the durable dispatch queue consumed by the existing claim transaction.

## One Run, one short transaction

1. Require the original active PostgreSQL READ COMMITTED transaction and a UUID.
2. Lock the Run with SELECT FOR UPDATE. Missing Run raises a lookup error;
   non-RUNNING Runs produce no changes.
3. Load its immutable pinned Workflow version. Lock all Task rows in task-key
   order, bounded by the existing 1000-task limit. Require an exact bijection
   between Task snapshots and DAG nodes, without mixed Run identities.
4. For each PENDING Task, require every direct dependency to be SUCCEEDED.
   Use the existing `DEPENDENCIES_SUCCEEDED` event to propose READY. A parent
   merely becoming READY never releases its own children in the same pass.
5. Apply the proposed PENDING -> READY changes in one UPDATE and validate all
   returned identities/states. Results remain provisional until caller COMMIT.

The lock path is Run -> Tasks. It skips Worker/Attempt/lease because this operation
does not touch them, and **must not acquire them later in the same transaction**.
Every execution writer also locks the Run first. Completion and scheduling thus
observe coherent Task states; a Worker cannot claim provisional READY rows before
the scheduling transaction commits. Use one fresh transaction per Run/pass.

Repeated reconciliation is idempotent. Two schedulers serialize on the Run, and
the second sees already-READY state. A crash or statement/commit failure rolls back
the whole pass; after commit, READY remains discoverable even if the scheduler dies
before acknowledging its own progress. No ephemeral notification is required for
correctness. Locks and existing SQL timeouts bound contention, not execution time.

This trades per-Run scheduling throughput for straightforward consistency. The
transaction reads the bounded DAG/Task set and updates readiness in bulk; unrelated
Runs do not share a scheduler lock. M3 adds distributed scan/concurrency behavior.
Task execution remains outside these transactions.

## Current scope and verification

This subtask changes only readiness. Failed/SKIPPED parents do not satisfy a success
dependency; failure propagation and Run aggregation are M5. It creates no Attempt,
executes no handler, applies no retry/timeout, and runs no background scanner.
M2.5b adds a scheduler process and CLI to repeatedly invoke reconciliation.

```console
uv run --locked pytest tests/test_readiness.py
uv run --locked pytest tests/integration/test_scheduling.py --database-env-file .env.database-test
```

Tests cover all parent states, multiple parents, identity mismatch, repeated scans,
a complete diamond's claim/completion/readiness sequence, concurrent schedulers,
completion commit/rollback races, Worker claim after scheduling commit/rollback,
write/deferred-commit failure, lock timeout retry and original-transaction guards.
