# PostgreSQL connectivity and transaction boundaries

SQLAlchemy 2 Core with psycopg 3 provides an explicit engine lifetime. The
connectivity module does not create tables; [explicit Alembic commands](migrations.md)
apply the schema. [HTTP workflow routes](api.md) use an application-owned lazy pool.
The API liveness endpoint remains independent of the database.

## Configuration and CLI

The existing Settings object accepts these additional environment variables:

| Variable | Default | Constraint |
| --- | --- | --- |
| `DWE_DATABASE_HOST` | `127.0.0.1` | Nonempty hostname/address |
| `DWE_DATABASE_PORT` | `5432` | 1..65535 |
| `DWE_DATABASE_NAME` | `workflow` | Nonempty |
| `DWE_DATABASE_USER` | `workflow_admin` | Nonempty |
| `DWE_DATABASE_PASSWORD` | Unset | Nonempty secret, without NUL |
| `DWE_DATABASE_PASSWORD_FILE` | Unset | Explicit UTF-8 password file |
| `DWE_DATABASE_POOL_SIZE` | `5` | 1..20 connections per process |
| `DWE_DATABASE_POOL_TIMEOUT_SECONDS` | `5` | 1..60 seconds |
| `DWE_DATABASE_CONNECT_TIMEOUT_SECONDS` | `5` | 2..60 seconds |
| `DWE_DATABASE_STATEMENT_TIMEOUT_MS` | `10000` | 1..600000 milliseconds |

Each setting follows environment > explicitly loaded dotenv > default.
Choose exactly one password source when using the database. Supplying both is an
error, including when a password comes from the environment and a password-file
path remains in the dotenv file. Credentials may be omitted for API liveness,
help, and configuration-only checks.

Passwords are represented by Pydantic SecretStr in settings. Password files are
read once when constructing an engine, not during imports or configuration-only
validation. Relative file paths resolve against the current working directory.
Trailing CR/LF characters are removed; other password characters are preserved.
Missing, unreadable, empty, NUL-containing, or invalid UTF-8 files prevent engine
creation. Engine creation does not contact PostgreSQL.

For the local Compose database, run in PowerShell from the repository root:

```powershell
$env:DWE_POSTGRES_PUBLISHED_PORT = "15432"
python scripts/init_dev_secrets.py
docker compose up -d --wait --wait-timeout 120 postgres

$env:DWE_DATABASE_PORT = "15432"
$env:DWE_DATABASE_PASSWORD_FILE = "secrets/postgres_password.txt"
uv run --locked engine check-db
```

`DWE_POSTGRES_PUBLISHED_PORT` controls Docker's host port mapping;
`DWE_DATABASE_PORT` controls where the application connects. They must match
when the CLI runs on the Windows host. The API inside Compose connects to
`postgres:5432`, independent of published host ports.

`check-db` also accepts `--env-file <path>`. Success prints
`Database connection is valid.` and exits 0. Missing/invalid credentials or
configuration exit 2. Database connection/query errors exit 1 with a fixed
`database_check_failed` JSON error event. No raw database exception, connection
URL, or credentials are printed. `SELECT 1` proves authenticated connectivity,
not schema readiness, migrations, write privileges, or workflow health.

`engine check-config` continues to validate configuration without reading
password files or connecting to a database.

## Engine and transaction ownership

```python
from sqlalchemy import text

from workflow_engine.config import load_settings
from workflow_engine.database import database_engine

settings = load_settings()
with database_engine(settings) as engine:
    with engine.begin() as connection:
        value = connection.execute(text("SELECT 1")).scalar_one()
```

The process startup layer owns one engine/pool and passes it to modules.
A successful `engine.begin()` block commits. An exception rolls back and returns
the connection to the pool. Use SQLAlchemy bind parameters for application values.
Do not manually commit inside a transaction block intended to be atomic.
See [SQLAlchemy transaction semantics](https://docs.sqlalchemy.org/en/20/tutorial/dbapi_transactions.html).

Connections and transactions are never shared between concurrent tasks or threads.
The engine/pool may serve multiple threads. Do not share an engine across a
process fork; construct it within each process. On shutdown, finish/close active
transactions before leaving `database_engine()`, which disposes idle connections.
Disposal is not cancellation of already checked-out connections.

Synchronous database calls suit the initial short transactions and synchronous
scheduler loops. M1.4's FastAPI database routes use synchronous handlers in the
thread pool, with commit completed before returning HTTP success. Direct calls
from an async handler would block the event loop. Async infrastructure should be introduced if measured
I/O concurrency warrants it.

## Limits and failure semantics

- SQLAlchemy uses a PostgreSQL/psycopg URL object rather than interpolating a URL
  string. Passwords containing characters such as `@`, `/`, and `%` do not
  need manual URL encoding.
- Core write transactions use READ COMMITTED. A transaction alone does not prevent
  concurrent scheduling races: repositories combine ordered locks, constraints,
  state checks and post-lock clock observations. See the
  [architecture and consistency boundaries](architecture.md). The optional demo
  uses a separate read-only REPEATABLE READ snapshot for its combined evidence view.
- Pool overflow is disabled. Each process opens at most its configured pool size;
  additional callers wait up to the pool timeout. Multiple processes multiply
  this connection budget. This does not yet implement task backpressure.
- Connection establishment and each SQL statement have separate timeout settings.
  A statement timeout also limits time spent waiting on database locks. These
  are not an end-to-end request deadline; DNS resolution and multiple connection
  attempts can affect elapsed time. Idle transactions require separate lifecycle
  controls and must not surround task execution.
- Pre-ping detects stale pooled connections on checkout. It cannot repair a
  transaction interrupted by a database/network failure.
- The connectivity layer does not replay transactions. Losing a connection during
  COMMIT can leave the outcome unknown; callers must preserve the protocol's
  [Run](run-idempotency.md), [claim](idempotent-claims.md) or
  [completion](completion-transactions.md) identity when resolving that uncertainty.
  A SQL timeout is not a workflow/task timeout.
- SQL echo is disabled and bound parameters are hidden in SQLAlchemy errors.
  This is not general redaction of driver errors, literal SQL, or arbitrary user
  logging. The CLI emits fixed failure messages; callers must not log raw
  exceptions or unmasked credentials.
- The binary psycopg distribution bundles client libraries for consistent local
  and image installation. Dependency updates also need to account for those
  bundled libraries. See [psycopg installation options](https://www.psycopg.org/psycopg3/docs/basic/install.html).
- TLS behavior currently follows libpq defaults/environment; verified TLS policy,
  separate application/migration roles, and production configuration remain
  future work. The supplied Compose role is a development administrator.

## Tests

Ordinary `uv run --locked pytest` runs unit/CLI tests and explicitly skips the
PostgreSQL integration tests. To enable them, provide a dedicated test database
configuration file. Do not target a production database.

For a local development database that permits disposable test tables, create an
ignored `.env.database-test` file with:

```dotenv
DWE_ENVIRONMENT=test
DWE_DATABASE_HOST=127.0.0.1
DWE_DATABASE_PORT=15432
DWE_DATABASE_NAME=workflow
DWE_DATABASE_USER=workflow_admin
DWE_DATABASE_PASSWORD_FILE=secrets/postgres_password.txt
```

Run:

```console
uv run --locked engine check-db --env-file .env.database-test
uv run --locked pytest tests/integration --database-env-file .env.database-test
```

To run the whole suite with PostgreSQL enabled, use `uv run --locked pytest tests --database-env-file .env.database-test`. Include the explicit `tests` path so pytest loads the custom option before parsing the configuration-file argument.

The test configuration uses the existing secret file, not a password in the
dotenv file. Tests isolate themselves from the shell's application environment
and read the explicit file. Supplying a bad file or unreachable database causes
failure rather than a skip.

Integration tests create uniquely named `dwe_test_<uuid>` tables and remove them
in fixture cleanup. They verify authenticated access, READ COMMITTED, committed
data visible through a separate engine, rollback on error, pool exhaustion, and
statement timeout followed by connection reuse. An interrupted test process can
leave its test table behind; a disposable database is preferred.

The existing Ubuntu Container integration CI job runs these tests against its
fresh Compose database, alongside the M0.6 packaging and volume-persistence
checks. Linux and Windows quality jobs continue to run without a database.
