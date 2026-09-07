# Container development environment

This stage packages the existing API and provisions PostgreSQL. The API does not
connect to PostgreSQL yet. M0.7a adds CLI/database-module connectivity and tests;
see [database configuration](database.md). Migrations and API dependency
readiness are subsequent work. A healthy container pair does not execute workflows.

## Start locally

Install Docker Desktop with Linux containers and Docker Compose v2 or later.
Run commands from the repository root. Python 3.13 is sufficient for the
credential helper; it has no third-party dependencies.

```console
python scripts/init_dev_secrets.py
docker compose config --quiet
docker compose up --build --wait --wait-timeout 120
docker compose ps
```

The helper creates an unpredictable password in ignored
`secrets/postgres_password.txt`. Repeated execution preserves an existing,
nonempty secret; an empty file is an error. The value is never printed.
POSIX creation permissions are 0600; on Windows access follows the directory's
ACL. Keep this directory private. Compose secrets mount a local file; this is
not an encrypted secret manager.

Both services publish ports only on the host loopback interface:

| Service | Host address | Check |
| --- | --- | --- |
| API | `127.0.0.1:8000` | `GET /health/live` returns `{"status":"ok"}` |
| PostgreSQL | `127.0.0.1:5432` | Database `workflow`, user `workflow_admin` |

From PowerShell:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health/live
docker compose logs --tail 50 api postgres
docker compose exec postgres psql -h 127.0.0.1 -U workflow_admin -d workflow -W -c "SELECT 1;"
```

For the last command, enter the password from the local secret file at the
interactive prompt; do not put it in a command line or commit it.
API documentation is at [Swagger UI](http://127.0.0.1:8000/docs).

To resolve occupied host ports, set shell variables before Compose commands:

```powershell
$env:DWE_API_PUBLISHED_PORT = "18000"
$env:DWE_POSTGRES_PUBLISHED_PORT = "15432"
docker compose up --build --wait --wait-timeout 120
```

The API still listens on port 8000 inside its container. Compose explicitly sets
the application's four settings; it does not pass an application dotenv file
into the container. The published-port variables belong to Compose, not the
application's validated settings. Keep them in the shell, outside the
application's dedicated dotenv file.

## Stop and preserve data

```console
docker compose down
```

This stops/removes the containers and network but retains the named PostgreSQL
volume. A subsequent `docker compose up --wait` reuses that database.
Do not add `--volumes` unless you intend to delete the local database.
Changing the Compose project name or moving the repository can select a
different named volume; use `docker volume ls` when investigating missing data.

The password file initializes a new database only. Replacing the file does not
rotate an existing database role's password. Do not regenerate credentials to fix
authentication errors against an existing volume. Back up data before database
major-version upgrades; changing the image tag is not a migration procedure.

## Design decisions and limits

- The API uses a multi-stage image. uv installs locked runtime dependencies and a
  noneditable application into a virtual environment. The runtime image includes
  neither uv nor the development dependency group.
- The API runs as UID/GID 10001 with an exec-form entrypoint. Compose permits
  15 seconds to stop, longer than Uvicorn's 10-second graceful-shutdown window.
- The API binds to `0.0.0.0` inside its container so published ports work. Host
  bindings remain loopback-only. This is a local development configuration.
- The build context is an allowlist of package inputs. Local environments,
  credentials, Git data, tests, and tool caches are excluded.
- Python `3.13.15-slim-bookworm`, uv `0.12.5`, and PostgreSQL
  `18.6-bookworm` are version-pinned. Tags can still change; digest pinning and
  automated image-update policy are deferred. Python dependencies use `uv.lock`.
- PostgreSQL 18's named volume mounts at `/var/lib/postgresql`, with versioned
  data underneath it. Earlier major versions used a different recommended mount.
  See the [official PostgreSQL image documentation](https://hub.docker.com/_/postgres).
- The database receives its password using `POSTGRES_PASSWORD_FILE` and a
  [Compose secret](https://docs.docker.com/compose/how-tos/use-secrets/).
  `workflow_admin` is an initialization/admin role. Separate runtime privileges
  will be addressed when the application gains database access.
- There is no API `depends_on`: the API currently has no database dependency.
  Its healthcheck is liveness only. PostgreSQL's `pg_isready` checks acceptance
  of connections; it does not verify credentials or application schema.
- A named volume survives container replacement, not host/disk loss. Backups,
  replication, automatic restart policies, and production deployment are outside
  this subtask.

## Container acceptance checks

The CI container job runs after both Python quality jobs. It:

1. Generates disposable credentials and validates Compose configuration.
2. Builds the image and waits for both services to become healthy.
3. Checks HTTP liveness/OpenAPI, the non-root runtime user, and absence of
   development tools in the runtime environment.
4. Connects to PostgreSQL over TCP with the mounted password and writes a probe row.
5. Recreates only the PostgreSQL container and verifies the row still exists.
6. Removes the probe and, in this disposable CI environment only, removes volumes.

These checks establish container packaging, authenticated connectivity, and
persistence across replacement. They do not establish task recovery, storage
availability after host loss, or application database integration.
For failures, inspect the Actions job's service logs. Never include password
files or full secret-bearing environment dumps in diagnostics.
