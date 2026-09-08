# Run a Worker locally or in Docker

M2.4d provides `engine worker`. Start PostgreSQL and the API, apply migrations to
head, publish a Workflow and create a Run using the [API](run-api.md). The Worker
talks only to the API; it needs no PostgreSQL password or database connection.

## Host process

PowerShell, with an existing Run UUID:

```powershell
$env:DWE_WORKER_API_URL = "http://127.0.0.1:8000"
$env:DWE_WORKER_NAME = "worker_one"
uv run --locked engine worker --run-id <RUN_UUID>
```

Replace `<RUN_UUID>` with the created Run ID; angle brackets are placeholders.
Use `--env-file .env` to load configuration explicitly. A `.env` file is not
automatically read by the Python command. Use `--max-tasks 2` to stop after two
confirmed outcomes, including failures. Without a bound the Worker polls until
the Run is terminal or the process is stopped. No READY tasks does not mean done.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| DWE_WORKER_API_URL | http://127.0.0.1:8000 | HTTP(S) origin without credentials/path/query. |
| DWE_WORKER_NAME | worker | Human-readable label; a new session UUID is generated each start. |
| DWE_WORKER_HTTP_TIMEOUT_SECONDS | 5 | Per-socket-operation timeout, 0.05–60 seconds. |
| DWE_WORKER_POLL_SECONDS | 0.5 | Delay after a confirmed empty poll, 0.05–60 seconds. |
| DWE_WORKER_RETRY_SECONDS | 0.5 | Transient delivery retry delay, 0.05–60 seconds. |

The control tick is 0.05 seconds. Heartbeat and lease duration are API-side policies
already configured separately. Do not use extreme server durations that leave no
room for request latency, spawn startup and supervisor scheduling. Local control
timing remains advisory; the database decides accepted ownership.

SIGINT (Ctrl+C) and SIGTERM request shutdown. The direct handler child is terminated
and joined, and previous signal handlers are restored. Unfinished Attempts are not
marked successful or failed. Exit code 0 means normal stop/limit reached/terminal
Run; exit 1 means control or execution failure, and CLI validation errors exit 2.
Normal exit does not imply that all Tasks succeeded. The server session stays
ACTIVE until expiry/recovery; no stop-session endpoint is introduced here.

## Container process

Build/start API and PostgreSQL and migrate first, as documented in the README.
The optional `workers` profile provides a one-off Worker using the same runtime
image, non-root UID and an init process. It has no database secret mount and no
API healthcheck, because it serves no HTTP endpoint.

```console
docker compose run --rm --no-deps worker worker --run-id <RUN_UUID> --max-tasks 2
```

The first `worker` names the Compose service; the second is the `engine` command.
Its API origin is `http://api:8000` inside the Compose network, independently of
the host-published port. This profile is intended for `compose run` with a Run ID,
not bare `--profile workers up`. Rebuild the API image after code changes before
running this service. Docker Desktop must be running for container execution.

## Self-contained checks

With a migrated API running:

```console
uv run --locked python scripts/check_worker_execution.py
uv run --locked python scripts/check_worker_execution.py --container
```

Use `--base-url` for a different host API port. These scripts create a fresh Run
with independent success/failure Tasks, execute the installed Worker command and
verify persisted outcomes. CI runs both host and container variants. They print
no ownership tokens. This check uses independent roots; Run aggregation remains
M5 work. For dependent Tasks, run the separate [Scheduler](running-scheduler.md).
Retry and crash recovery follow in M4.
