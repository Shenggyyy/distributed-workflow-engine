# Run the Scheduler

M2.5b provides a separate process for [transactional readiness](scheduling.md).
M3.1b adds automatic discovery and cooperative multi-Scheduler scanning.
It uses the existing PostgreSQL credentials and no HTTP endpoint. Apply migrations
and create a Run first. One pass locks/reconciles one Run and commits before
reporting progress. All Task execution remains in Worker subprocesses.

```console
uv run --locked engine scheduler --env-file .env.database-test
uv run --locked engine scheduler --run-id <RUN_UUID> --env-file .env.database-test
uv run --locked engine scheduler --run-id <RUN_UUID> --env-file .env.database-test --once
```

Replace the placeholder with the Run UUID and use an appropriate explicit database
configuration. The test file shown is suitable only for a disposable test database.
`DWE_SCHEDULER_POLL_SECONDS` defaults to 0.5 (range 0.05–60). An unbounded invocation
continues until SIGINT/SIGTERM; inactive Runs are harmless no-ops. With `--run-id`,
`--once` performs one transaction and exits, propagating errors as nonzero status.

Without `--run-id`, each iteration discovers one bounded page of active Runs,
releases the discovery transaction, then reconciles each Run in its own fresh
transaction. `DWE_SCHEDULER_PAGE_SIZE` defaults to 50 (1–100). A poll delay follows
each page; the UUID cursor wraps at the end. Global `--once` processes **one page**,
not every Run in the database. Restarting begins a new traversal.

Automatic reconciliation uses `FOR UPDATE SKIP LOCKED` on the Run row only.
Busy/missing Runs produce no changes this pass; the next traversal revisits them.
Task rows still form a complete locked dependency snapshot, never a skipped subset.
No process retains multiple Run locks. Multiple Scheduler processes can therefore
make progress without a leader, although scans can overlap and do duplicate reads.
This is cooperative best-effort distribution, not an exclusive partition or fairness
guarantee. A continuously busy Run may be delayed. Explicit Run mode retains its
blocking reconciliation semantics. All modes preserve commit-before-progress.

Continuous mode retries connection loss, serialization/deadlock, lock/statement
timeout, pool timeout and database restart errors with a fresh transaction and a
poll delay. Constraint, schema and domain corruption errors stop the process.
An uncertain COMMIT can be retried because readiness transitions are idempotent;
no external task effect happens inside reconciliation. Logs contain fixed events
and Run IDs, and readiness logs happen only after commit.

Stop is checked between transactions. Existing database connection/pool/statement
timeouts bound blocking; signal handling does not interrupt an SQL statement.
Default Compose shutdown grace is 15 seconds. A forced process/container exit may
roll back an active transaction, and a restarted Scheduler reconstructs decisions
from persisted state. The process holds no durable local progress cursor.

## Container

After building the API image and starting/migrating PostgreSQL:

```console
docker compose run --rm --no-deps scheduler scheduler --run-id <RUN_UUID>
docker compose --profile scheduler up -d scheduler
```

The optional `scheduler` profile shares the same non-root image and uses an init
process. It mounts the existing database secret and serves no health HTTP endpoint.
Scheduler `--once` also works inside this container. Starting the profile without
a Run ID now selects automatic scanning.

## Complete DAG demonstration

These commands publish a fresh `A -> B/C -> D` DAG, create its Run, start a separate
Scheduler, execute four Tasks through the Worker CLI and check all persisted Task
outcomes. They stop their own Scheduler afterwards.

```console
uv run --locked python scripts/check_dag_execution.py --database-env-file .env.database-test
uv run --locked python scripts/check_dag_execution.py --scheduler-container
uv run --locked python scripts/check_dag_execution.py --scheduler-container --automatic-scheduler
```

Use `--base-url` for a non-default host API port. The first command starts the
Scheduler on the host; the second starts it inside Compose while the Worker runs
on the host. Both run in CI, alongside the existing container Worker check.

M2 executes a complete successful DAG with one Worker slot. Automatic Scheduler
discovery is available; Worker discovery and parallel capacity follow in M3. M4 adds
retry/timeout/crash recovery; M5 adds failed-dependency propagation and Run status
aggregation. Currently failed dependencies leave descendants PENDING, and even
all-successful Task sets leave their Run RUNNING. This is not final MVP acceptance.
