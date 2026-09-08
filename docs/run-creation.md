# Transactional run creation

M1.7 introduces `RunRepository.create(workflow_version_id)` in
`repositories/runs.py`. It initializes one run from an existing immutable
version. Its original schema requirement is revision `0003`; the current head
`0004` adds [request-binding storage](run-idempotency.md). The create method here
remains unkeyed. M1.9b exposes [Run HTTP creation](run-api.md) through the separate
keyed repository method.

## Contract and transaction ownership

```python
from workflow_engine.repositories.runs import RunRepository

with engine.begin() as connection:
    created = RunRepository(connection).create(version_id)
# Only after this block succeeds is creation committed.
```

The caller supplies a UUID for a concrete version, not a workflow name or "latest".
The method returns a frozen CreatedRun containing a WorkflowRun snapshot and a
tuple of TaskRun snapshots in deterministic topological order. These are creation
results, not live state subscriptions. M1.9a adds [Run queries](run-queries.md)
for current persisted metadata and tasks, including database audit timestamps.

Use PostgreSQL READ COMMITTED with DBAPI autocommit disabled. The repository
composes WorkflowRepository for validated version reads and its transaction
guard. RunRepository also checks its own original transaction before keyed or
unkeyed operations; an ended or replaced transaction is rejected before writes. Do not share repositories or
connections between threads.

There is no internal commit, rollback, savepoint or retry. Let failures escape
the caller's transaction context so the whole operation rolls back. Do not catch
an exception after writes and then commit that transaction. A returned object is
provisional until commit succeeds; a later caller failure invalidates its
persistence claim.

## Initialization protocol

1. Read the requested version through WorkflowRepository and validate its stored
   fields and complete DAG again. An absent/invisible version raises
   WorkflowVersionNotFoundError; invalid stored data raises StoredDefinitionError.
   Both checks occur before runtime writes.
2. Allocate a fresh run UUID and a fresh task UUID for every node. Build PENDING
   domain snapshots and resolve the legal activation transitions before writing.
3. Insert the PENDING run, then all PENDING tasks in a bounded batch. Task keys
   exactly cover the pinned definition, including disconnected branches.
4. Update only roots from PENDING to READY through the existing lifecycle edges.
   A validated nonempty DAG always has at least one root. Non-root tasks stay
   PENDING because no prerequisite has executed.
5. Update the run from PENDING to RUNNING. Return initialization snapshots;
   the caller commits all writes together.

The domain state machine determines transitions and the database guards verify
the persisted edges. There is no arbitrary initial-status parameter. No attempt
is created during initialization.

RUNNING here means the run is activated and waiting for execution progress.
READY means dependencies are satisfied, not that a worker has claimed the task.
With no scheduler/worker implemented, a newly created run remains in this state.

For the existing diamond example:

| Record | Committed state |
| --- | --- |
| Run | RUNNING |
| A, with no prerequisites | READY |
| B, depending on A | PENDING |
| C, depending on B | PENDING |
| D, depending on B and C | PENDING |
| Attempts | No records |

## Concurrency and failure semantics

Newly inserted rows remain private to the creating transaction until commit.
Other connections cannot observe an empty run or a partially initialized task
set from this operation. Initialization does not need to acquire a shared
per-run coordination lock because this run has no previously committed state.
Later scheduling/completion transactions must coordinate existing runs.

There is also no workflow-wide allocation lock: creators use independent UUIDs
and pin a version whose contents cannot change through ordinary SQL. Concurrent
calls for the same version produce complete, distinct runs. Publishing a later
version does not change a run's pinned definition or require creators to wait
for that publisher's workflow-row lock.

| Scenario | Result |
| --- | --- |
| Missing or invalid version | Fail before writing any runtime records. |
| Task insertion, root activation or run activation fails | Propagate the failure; outer rollback removes the run and every inserted task. |
| Caller fails after create returns but before commit | Outer rollback removes initialization; returned snapshots are not durable. |
| Connection/process disappears before commit | PostgreSQL rolls back once it detects disconnect; detection may not be immediate. |
| Connection is lost during COMMIT | Caller may not know whether creation committed. No automatic replay is attempted. |
| Same version is submitted again, including concurrently | Create new run/task UUIDs; no request deduplication. |
| Process exits after successful commit | Initialized records remain durable; execution/recovery loops are still future work. |

These guarantees assume the documented transaction pattern. They do not repair
records inserted by unsupported direct SQL writers, enforce handler capability,
or prove worker availability. Creation does not implement dispatch, leases,
retries, runtime dependency resolution after completion, or status aggregation.

**Do not retry an uncertain unkeyed creation as if it were idempotent.** M1.8b
adds create_idempotent() and a [separate keyed example](run-idempotency.md) for
safe same-key/version retries. The create() method described here remains
unkeyed. Business side-effect idempotency remains a separate concern.

## Runnable example

With a configured local database and migrations applied, publish the example
using `examples/publish_workflow.py`, then use its printed Version ID:

```powershell
$env:DWE_DATABASE_PORT = "15432"
$env:DWE_DATABASE_PASSWORD_FILE = "secrets/postgres_password.txt"
uv run --locked alembic upgrade head
uv run --locked python examples/publish_workflow.py
uv run --locked python examples/create_run.py --version-id "<Version ID from publication>"
```

Both scripts accept `--env-file .env.database-test` for an explicit configuration.
Each publication appends a version; each creation invocation makes another run.

Expected creation output:

```text
Run ID: <generated UUID>
Version ID: <requested UUID>
Run status: RUNNING
Task A: READY
Task B: PENDING
Task C: PENDING
Task D: PENDING
Run committed; no attempts created and no tasks executed.
```

Success is printed only after the transaction commits. This is a developer
example, not an HTTP error contract or a production task runner.

## Verification

```console
uv run --locked pytest tests/integration/test_run_repository.py --database-env-file .env.database-test
```

Tests use real migrated PostgreSQL schemas and independent connections. They
cover a single node, the diamond, multiple roots/disconnected branches and the
1000-node limit; root-only readiness and complete committed records; invisibility
before commit; version pinning and repeated submissions; missing/corrupt versions;
caller rollback; database fault injection at task insertion, root activation and
run activation; four concurrent creators; progress while a later publication
holds the workflow lock; and transaction/autocommit guard enforcement.

The existing CI container job discovers these integration tests automatically.
