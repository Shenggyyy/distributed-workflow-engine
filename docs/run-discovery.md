# Bounded Run discovery

M3.1a adds read-only `GET /runs` and `RunDiscoveryRepository.active`, supported by
the additive `0009` migration. This discovery API remains within the existing
trusted/local API boundary. It neither schedules nor grants execution ownership.

```console
curl "http://127.0.0.1:8000/runs?limit=50&ready_only=true"
```

The response contains `run_ids` (at most 100 UUIDs) and `next_after` (UUID or null).
Only RUNNING Runs appear. `limit` defaults to 50 and accepts 1–100; `after` is an
exclusive UUID ordering boundary, not a row that must exist. `ready_only=true`
requires at least one READY Task at the read snapshot; default false lists every
RUNNING Run. Success is 200 with `Cache-Control: no-store`; invalid query values
return the existing sanitized 422 envelope.

Follow `next_after` until null for one traversal. Repeat from an omitted cursor
for later traversals. Each page is its own READ COMMITTED statement snapshot.
New Runs or newly eligible Runs below a previous cursor appear on the next
traversal; statuses can change between discovery and claim. There is no consistent
multi-page snapshot, claim guarantee, global FIFO ordering or durable cursor.
UUID order is stable identity order, not submission-time order.

The query fetches at most `limit + 1` rows, uses an EXISTS filter instead of a join
that could duplicate Run IDs, and acquires no row locks. It will not wait behind
execution row locks, although normal database/table-level contention and timeouts
still apply. Workers must preserve their claim request identity after choosing a
Run and independently handle empty/terminal/stale results from the claim endpoint.

Revision `0009` adds `ix_workflow_runs_active_id` (Run UUID where RUNNING) and
`ix_task_runs_ready_run_id` (Run UUID where Task READY). Queries use fixed SQL
status literals so partial-index eligibility is retained with prepared statements.
The migration changes no table data; downgrade removes only these indexes. Index
creation uses the existing transactional migration runner and can block concurrent
writes while building. Large production datasets would need a measured concurrent
index deployment strategy; this project currently uses small local/test databases.

```console
uv run --locked alembic -x env_file=.env.database-test upgrade head
uv run --locked pytest tests/integration/test_run_discovery.py --database-env-file .env.database-test
uv run --locked python scripts/check_run_api.py
```

Tests cover bounded pagination, terminal filtering, readiness changes, invalid
queries, empty results, cursor boundaries, nonblocking row-lock reads and populated
upgrade/downgrade/reupgrade. M3.1b implements bounded Scheduler scan coordination;
M3.1c connects Worker automatic Run discovery.
