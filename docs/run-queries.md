# Run queries and statement snapshots

M1.9a adds read operations to RunRepository. M1.9b exposes them through the
[Run HTTP API](run-api.md). Neither adds a migration; Worker session storage later
advances the current database head to 0005.

## Read contracts

| Method | Result | Missing run |
| --- | --- | --- |
| get_run(run_id) | StoredRun: id, workflow_version_id, RunStatus, created_at | None |
| get_run_with_tasks(run_id) | RunTaskSnapshot: StoredRun and tuple of StoredTaskRun | None |

StoredTaskRun contains id, run_id, task_key, TaskStatus and created_at. Returned
records are frozen dataclasses; status values use the existing domain enums.
They are detached snapshots, not ORM entities, subscriptions or mutable handles.
Database timestamp values remain timezone-aware.

Both methods require a Python UUID and the repository's original active
PostgreSQL READ COMMITTED transaction. They work in a read-only transaction.
Wrong input types raise TypeError before issuing the read. Reusing a repository
after its original transaction ends or is replaced raises RepositoryTransactionError.

```python
from workflow_engine.repositories.runs import RunRepository

with engine.begin() as connection:
    snapshot = RunRepository(connection).get_run_with_tasks(run_id)
if snapshot is not None:
    print(snapshot.run.status.value)
    for task in snapshot.tasks:
        print(task.task_key, task.status.value)
```

These methods never commit, retry, change state, allocate an attempt or run a
handler. The caller owns the transaction and should propagate database failures.

## Consistency and concurrency

get_run uses one SELECT. get_run_with_tasks uses one SELECT with a LEFT JOIN on
task_runs.run_id = workflow_runs.id. Run metadata and task rows therefore come
from one statement snapshot, including the caller's own earlier writes.

This avoids fetching a run and then fetching its tasks after another transaction
commits. Two separate public method calls do not share a fixed snapshot under
READ COMMITTED; their results can differ. A later get_run_with_tasks call in the
same transaction can observe a newer committed version.
See [PostgreSQL transaction isolation](https://www.postgresql.org/docs/18/transaction-iso.html).

The query acquires no row locks and does not wait for ordinary uncommitted row
updates. Such updates become visible on subsequent statements after commit.
Normal relation locks still exist, and DDL or other database conditions can block
a read. Existing statement/connection/pool timeouts apply.

A writer committing after the SELECT executes does not change the rows already
read. A returned snapshot can nevertheless become stale immediately. Do not use
a read result alone to authorize task claims, completion or cancellation; those
future actions need authoritative state/ownership checks in coordinated write
transactions.

## Ordering and bounds

Tasks are ordered by task_key using the existing C collation, giving deterministic
case-sensitive ASCII order. This is a display/read order, not DAG topological
order or an execution sequence. Only tasks belonging to the requested run are
returned.

A supported workflow contains at most 1000 nodes. The detailed query requests at
most MAX_TASKS + 1 rows and raises StoredRuntimeError if that extra row exists.
It never presents the first 1000 tasks as a complete result for an oversized run.
This bounds returned rows and decoding, not every possible database execution
cost or waiting time. The run-only query remains available for diagnosis.

The existing unique (run_id, task_key) index supports the parent lookup and task
ordering. No speculative new index, pagination or schema change is required for
this bounded single-run query. Repeating the small run metadata in joined rows is
an accepted cost for one coherent query without database JSON aggregation.

Attempt history, listing all runs, workflow-wide history and pagination are
outside this subtask; attempt counts do not have the DAG node bound.

## Stored data and diagnostic behavior

The decoder checks identities, task-key format and statuses using the existing
runtime domain models. Unsupported statuses or invalid task keys raise
StoredRuntimeError with a fixed message, without echoing stored values.
SQL/driver errors propagate separately. SQL constraints and column types provide
the normal timestamp/UUID integrity; this reader does not replace migrations.

The LEFT JOIN distinguishes an absent run from an existing run with no tasks.
An existing empty run returns its metadata and an empty tuple. Supported creation
always initializes all nodes atomically, but a diagnostic reader should not hide
an incomplete run inserted through direct SQL or visible within the caller's own
unfinished transaction.

The reader does not load/revalidate the pinned DAG, check task coverage against
that DAG, or enforce relationships among current run/task statuses. It reports
persisted run status rather than calculating workflow aggregation. For example,
if direct SQL leaves all tasks SUCCEEDED and the run RUNNING, the query returns
those exact states. A statement snapshot is a consistency boundary, not proof
that every application invariant holds.

A creation receipt from create_idempotent stays stable over time. Use these
queries to inspect the current persisted state; replaying a receipt must not reset
or regenerate task state.

## Runnable example

After configuring the database and creating a run, use the printed Run ID:

```powershell
$env:DWE_DATABASE_PORT = "15432"
$env:DWE_DATABASE_PASSWORD_FILE = "secrets/postgres_password.txt"
uv run --locked python examples/query_run.py --run-id "<Run ID from creation>"
```

The script also accepts --env-file for an explicit configuration. A freshly
initialized diamond run prints:

```text
Run ID: <requested UUID>
Version ID: <pinned UUID>
Run status: RUNNING
Created at: <timestamp with timezone>
Task A: READY
Task B: PENDING
Task C: PENDING
Task D: PENDING
Read-only statement snapshot; no tasks executed or statuses changed.
```

An absent Run ID exits with code 1 and prints "Run was not found." on stderr.
The script performs no writes. HTTP equivalents are documented in [run-api.md](run-api.md).

## Verification

```console
uv run --locked pytest tests/integration/test_run_queries.py --database-env-file .env.database-test
```

Tests cover metadata and enum decoding, deterministic ordering, isolation from
other runs, missing versus empty runs, visibility of uncommitted creation,
read-only transactions, reads during pending writes, stored-status reporting
without aggregation, the inclusive task limit and rejected overflow, invalid
stored values, invalid IDs and transaction lifetime guards.

The statement-snapshot test commits a separate writer after the reader's SQL
executes but before result decoding. It verifies one SELECT returns a coherent
old snapshot, while the next SELECT observes the committed new state. This is
deterministic transaction interleaving, not a timing-only stress test.

M1.9b exposes keyed Run creation and these queries through HTTP with explicit
[response/error contracts](run-api.md).
