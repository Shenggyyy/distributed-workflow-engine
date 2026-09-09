# Testing and verification

Run the same checks as CI:

```console
uv sync --locked
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked mypy
uv run --locked pytest
node --test tests/demo-evidence.test.mjs tests/demo-i18n.test.mjs tests/demo-page.test.mjs tests/demo-dag.test.mjs
uv run --locked python scripts/check_docs.py
uv build
```

Ruff checks Python errors, imports, modernization rules, and common bug patterns.
It also owns formatting (88-column target). mypy uses strict mode with the
Pydantic plugin for `src/`, `tests/`, `scripts/`, and `examples/`. All configuration
lives in `pyproject.toml`; tool versions are recorded in `uv.lock`.

To apply formatting locally:

```console
uv run --locked ruff format .
```

CI only checks formatting; it does not edit or commit files.
Pytest defaults to short tracebacks to avoid expanding third-party frame arguments
that can contain database credentials. This is not a general secret redactor;
do not enable verbose tracebacks or local-variable dumps in shared CI logs.

## Continuous integration

[CI workflow runs](https://github.com/Shenggyyy/distributed-workflow-engine/actions/workflows/ci.yml)

The workflow runs on pushes to `main`, pull requests, and manual dispatch.
Each Linux/Windows job installs Python from `.python-version`, syncs locked
dependencies, and runs lint, format, type, test, and package build checks.
Python jobs have a 10-minute timeout; newer runs cancel superseded runs for the same
ref. Actions are pinned to commit SHAs and repository permissions are read-only.
After both Python jobs pass, a 15-minute Ubuntu container job builds and starts
the Compose services, checks the API/runtime image, runs the application's
PostgreSQL and HTTP integration tests, applies and checks the current revision,
executes a real HTTP publication/query smoke test, and verifies that data survives
database container replacement. Its credentials
and volumes are disposable. No deployment or publishing is performed.

Local checks do not establish a successful GitHub run. Inspect the exact pushed
commit's Actions result. Branch protection/rulesets are a separate repository
setting; this workflow does not enable them.

## Database and real-process verification

The default pytest run skips integration tests unless a test database is explicitly
configured. Use a dedicated test database, never a database containing valuable
work. Tests create isolated schemas and remove those schemas after validation.
Follow [test database setup](database.md#tests) and [migration checks](migrations.md).

```console
uv run --locked pytest tests/integration --database-env-file .env.database-test
```

Do not commit that private configuration. The CI container job provisions its own
disposable PostgreSQL environment and exercises the same integration suite, real
HTTP smoke scripts, multiple Workers/Schedulers, crash recovery and persistence.

The [demo guide](demo.md) runs real dedicated scenarios using
`uv run python -m scripts.demo_acceptance`. It writes evidence for new demo Runs;
passing that command does not replace browser acceptance. See dated
[core review](mvp-review.md), [original demo review](demo-review.md) and
[six-step review](demo-flow-review.md) for observations and limits at those commits.
Test counts in those records are historical, not a live badge.

## Build the package

```console
uv build
```

The source distribution and wheel are written to `dist/`, which is ignored by Git.
