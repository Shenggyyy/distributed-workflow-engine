# Run the Scheduler

M2.5b provides a separate process for [transactional readiness](scheduling.md).
It uses the existing PostgreSQL credentials and no HTTP endpoint. Apply migrations
and create a Run first. One pass locks/reconciles one Run and commits before
reporting progress. All Task execution remains in Worker subprocesses.

```console
uv run --locked engine scheduler --run-id <RUN_UUID> --env-file .env.database-test
uv run --locked engine scheduler --run-id <RUN_UUID> --env-file .env.database-test --once
```

Replace the placeholder with the Run UUID and use an appropriate explicit database
configuration. The test file shown is suitable only for a disposable test database.
`DWE_SCHEDULER_POLL_SECONDS` defaults to 0.5 (range 0.05–60). An unbounded invocation
continues observing this Run until SIGINT/SIGTERM; inactive Runs are harmless no-ops.
`--once` performs one transaction and exits, propagating errors as nonzero status.

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
```

The optional `scheduler` profile shares the same non-root image and uses an init
process. It mounts the existing database secret and serves no health HTTP endpoint.
The profile is intended for `compose run` with arguments. Scheduler `--once` also
works inside this container. Do not start the profile without supplying a Run ID.

## Complete DAG demonstration

These commands publish a fresh `A -> B/C -> D` DAG, create its Run, start a separate
Scheduler, execute four Tasks through the Worker CLI and check all persisted Task
outcomes. They stop their own Scheduler afterwards.

```console
uv run --locked python scripts/check_dag_execution.py --database-env-file .env.database-test
uv run --locked python scripts/check_dag_execution.py --scheduler-container
```

Use `--base-url` for a non-default host API port. The first command starts the
Scheduler on the host; the second starts it inside Compose while the Worker runs
on the host. Both run in CI, alongside the existing container Worker check.

M2 now executes a complete successful DAG with one Worker slot. Run discovery,
parallel Worker capacity and multi-Scheduler scan distribution are M3. M4 adds
retry/timeout/crash recovery; M5 adds failed-dependency propagation and Run status
aggregation. Currently failed dependencies leave descendants PENDING, and even
all-successful Task sets leave their Run RUNNING. This is not final MVP acceptance.
