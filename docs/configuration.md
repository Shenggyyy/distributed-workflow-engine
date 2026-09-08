# Configuration and structured logging

Settings use [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
and the `DWE_` environment prefix.

| Variable | Default | Accepted values |
| --- | --- | --- |
| `DWE_ENVIRONMENT` | `development` | `development`, `test`, `production` |
| `DWE_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `DWE_API_HOST` | `127.0.0.1` | An IPv4 or IPv6 address literal |
| `DWE_API_PORT` | `8000` | An integer from 1 through 65535 |
| `DWE_WORKER_HEARTBEAT_TIMEOUT_SECONDS` | `30` | API session heartbeat window, integer 1–86400 seconds |
| `DWE_ATTEMPT_LEASE_SECONDS` | `30` | HTTP claim and renewal lease duration, integer 1–86400 seconds |

Environment variable names are case-insensitive; enum values use the exact
spelling shown above. Empty values are validated rather than silently ignored.
The API address is validated syntactically; no port binding or network check
takes place.

Configuration precedence is **environment variables > explicitly selected
dotenv file > defaults**. No dotenv file is loaded automatically, and no parent
directories are searched. An explicitly selected file must exist and be a
readable UTF-8 file. Relative paths are resolved against the current directory.

To use a local file in PowerShell:

```powershell
Copy-Item .env.example .env
uv run --locked engine check-config --env-file .env
```

Or validate only environment variables and defaults:

```console
uv run --locked engine check-config
```

Success prints `Configuration is valid.` to stdout and exits with code 0. It also
emits a `configuration_validated` INFO log to stderr when the log level permits.
Invalid settings or an unreadable file exit with code 2. Validation errors report field names and
error types, without echoing input values or printing the complete settings.
Help and version commands do not load settings.

Use a dedicated dotenv file: unknown keys in that file are errors, which catches
typos. Unrecognized process environment variables are ignored, including unknown
`DWE_` names; this is a limitation of the environment source. Never put real
credentials in `.env.example`; local `.env` files remain ignored by Git.

Application code calls `load_settings()` at startup and passes the resulting
immutable `Settings` object to the components that need it. Each explicit call
loads a fresh snapshot; imports do not load settings, and existing snapshots do
not change when the environment changes. There is no hot reload or global cache.
Logging is initialized after successful configuration validation. API settings
are used by `engine api`; `check-config` does not start a server.
Database settings are documented in [database.md](database.md).
`DWE_WORKER_HEARTBEAT_TIMEOUT_SECONDS` controls the API's session heartbeat
window (default 30 seconds, range 1–86400), loaded once at startup. Compose forwards
it into the API container. It is separate from task leases and heartbeat send
intervals; see [Worker API configuration](worker-api.md#server-configuration).
`DWE_ATTEMPT_LEASE_SECONDS` controls new HTTP claim leases and HTTP renewal with
the same default and range, independently of heartbeats and task execution timeout.
Changing configuration alone does not renew existing claims; claim replay reads
stored metadata. Explicit Python repository policies remain separate. See
[lease API configuration](lease-api.md#server-configuration).

## Structured logging

Engine logs use the Python standard library and are written to stderr as one JSON
object per physical line. Command results remain on stdout. Try:

```console
uv run --locked engine check-config --env-file .env.example
```

At the default INFO level, stderr contains a record with these fields
(timestamp varies):

```json
{"timestamp":"2026-09-07T00:00:00.000Z","level":"INFO","logger":"workflow_engine.cli","component":"cli","environment":"development","event":"configuration_validated","message":"Configuration validation completed."}
```

Every record includes `timestamp` (UTC), `level`, `logger`, `component`,
`environment`, `event`, and `message`. Newlines and Unicode are JSON-escaped so
the stream remains parseable on Windows and Linux. `DWE_LOG_LEVEL` controls the
handler as well as the application logger, so verbose child loggers cannot
bypass the configured threshold.

At startup, call `configure_logging(settings, component="worker")` once.
Application modules use `logging.getLogger(__name__)` and pass context per event:

```python
logger.info(
    "Task completed",
    extra={
        "event": "task_completed",
        "run_id": run_id,
        "task_id": task_id,
        "attempt_id": attempt_id,
        "worker_session_id": worker_session_id,
        "duration_ms": duration_ms,
    },
)
```

Worker and Scheduler processes use this logging pattern.
Supported optional identifiers are `request_id`, `run_id`,
`task_id`, `attempt_id`, and `worker_session_id` (strings or UUIDs).
`duration_ms` accepts finite, non-negative numbers. Unsupported extra fields
and incorrectly typed optional values are omitted. A missing event name becomes
`log`. Core component/environment fields cannot be overwritten through extras.

Context is passed explicitly, without shared mutable request/task state.
This does not create distributed traces. Worker protocols explicitly carry the
Run/Task/Attempt identities used in execution logs.

`logger.exception(...)` adds the current exception type and stack locations
(filename, line, function). Exception messages, source lines, local variables,
and chained exceptions are deliberately omitted. Caller-provided messages and
accepted identifiers are still logged verbatim: keep credentials, URLs containing
credentials, configuration dumps, and business payloads out of these fields.
This formatter is not a general-purpose secret redactor.

Only the `workflow_engine` logger hierarchy is configured. Root and third-party
handlers are left alone, and engine records do not propagate to root handlers.
Sequential initialization replaces the engine-owned handler, rather than adding
duplicates; configure it before starting concurrent work, once in each process.
Help/version and invalid-configuration errors retain their normal CLI output
because logging starts only after settings validate.

API lifespan events are `api_startup_complete` and `api_shutdown_complete`.
The startup event means application initialization finished, not that the server
has successfully bound its socket; use an HTTP request to verify reachability.
Uvicorn retains its own text service/error logs on stderr, with access logging
disabled. The API process stderr therefore contains both engine JSON records
and Uvicorn text. Unifying server logs and adding request logging are later work.

Logs are synchronous diagnostic output, not a durable event log. File rotation,
central collection, and queued logging are deferred until their operational need
is established.
