# M0–M5 completion and correctness review

The agreed MVP is complete. V2/V3 remain explicitly outside this milestone.
The review combines existing [M2](m2-review.md), [M3](m3-review.md) and
[M4](m4-review.md) evidence with settlement and business-idempotency acceptance.

## Final validation

- **1508 tests passed** on Windows/Python 3.13 with disposable PostgreSQL 18:
  668 non-integration tests and 840 database/integration cases. The one warning is
  Starlette's deprecated AnyIO BlockingPortal alias. Test counts are evidence of
  exercised cases, not a completeness or throughput claim.
- Ruff lint/format, strict mypy for Linux and Windows, actionlint, source and wheel
  builds passed. Alembic upgraded to `0010`; metadata drift check reported no new
  operations. Build artifacts were checked for private/local files.
- A fresh isolated database and rebuilt non-root image passed 15 HTTP/process/
  container smokes: automatic host/container discovery, six API protocols,
  container Worker execution, host Scheduler DAG, four container Scheduler
  scenarios and two-Worker/two-Scheduler execution. Final Run outcomes are asserted.
- Crash tests terminate a real Worker after a durable claim or committed business
  effect. Fresh recovery/replacement execution preserves one business increment,
  records both Attempts and rejects stale completion after Run settlement.
- GitHub Actions checks Linux/Windows quality and PostgreSQL/container integration.
  [M5.3a acceptance CI](https://github.com/Shenggyyy/distributed-workflow-engine/actions/runs/34221730368)
  passed all three jobs, including database-container recreation and persistence.
  Each implementation commit is separately pushed; newer pushes can cancel older
  in-progress runs. A canceled run is not a successful validation.

Reproduce the core checks from the repository root:

```console
uv sync --locked
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked mypy
uv run --locked pytest tests --database-env-file .env.database-test
uv run --locked alembic -x env_file=.env.database-test upgrade head
uv run --locked alembic -x env_file=.env.database-test check
uv build
```

Use a dedicated test database and private config as described in
[database setup](database.md). Without explicit database configuration integration
tests skip; a green unit-only run is not full MVP acceptance. See
[failure scenarios](failure-scenarios.md) for real HTTP/container commands.

## Final integration review

| Area | Conclusion |
| --- | --- |
| DAG/versioning | Validation rejects cycles, missing/repeated/self edges and unsupported schemas. Immutable versions pin policy; every Run has a complete bounded Task set. |
| State and persistence | Domain events and database guards preserve legal edges and terminal history. Repositories never commit; HTTP and Scheduler publish success only after caller COMMIT. |
| Locks | Run -> Worker -> Task -> Attempt -> lease remains consistent across claims, renewals, completion and recovery. Scheduler Run -> Tasks never takes later Worker locks. Worker expiry transactions are separate. |
| Concurrent scheduling | Advisory UUID pages and SKIP LOCKED can delay busy/new work but do not authorize mutations. Locked re-reads and durable READY/retry rows support restart and overlapping scanners. |
| Ownership | One current persisted RUNNING Attempt per Task plus immutable session/token ownership fences stale state updates. Heartbeat and lease have distinct admission roles. |
| Retry/timeout | Latest failed Attempt authorizes only its own persisted equal-jitter retry. Fixed execution deadlines cannot be renewed. Earlier lease/deadline decides LOST/TIMED_OUT; retries consume the total configured budget. |
| Recovery/replay | Recovered Attempts have no fabricated Worker receipt. Exact historical completion can replay after terminal Run aggregation; an old unaccepted result cannot settle a replacement. |
| Failure propagation | Only permanent FAILED/SKIPPED parents skip PENDING descendants. RETRY_WAIT delays children; independent branches finish. Task skips and final Run outcome commit together. |
| Business effects | The destination receipt and counter share one separate transaction, with conflict checks. Process crash acceptance shows repeated execution with a single retained effect. It is not exactly-once execution. |
| Backpressure | Worker slots include uncertain reservations, one request per slot and a shared heartbeat. Database pools, page/task bounds and per-session capacity are explicit. Global admission/fairness remain future work. |
| Packaging/operations | Locked dependencies, explicit migrations, loopback bindings, mounted private secrets and non-root runtime are reproducible. No secret or local instruction file belongs in Git or distributions. |

No unresolved correctness failure was found in the exercised contracts. This is
not proof against every adversarial interleaving or infrastructure failure.

## Remaining limits and next milestones

- No API authentication, multi-tenant security, arbitrary-code sandbox or general
  process-tree containment; deploy only among trusted components.
- No task payload/output propagation, dynamic DAGs, cancellation, scheduled jobs,
  routing, priority, global concurrency/rate limits or dead-letter workflow.
- No measured load/scaling claim, database HA/failover certification or production
  recovery-time SLA. Polling, lock waits and shared queue/storage load affect latency.
- Database clock regression rejects unsafe operations or defers recovery; some
  control errors stop a process and require operator restart supervision. Historical
  unleased Attempts are outside automatic lease recovery.
- Append-only history has no garbage collector. Receipt retention and business
  key design must cover the application's duplicate-delivery horizon.
- Metrics, tracing, richer event logs and operational dashboards follow in V2;
  measured horizontal scaling and storage lifecycle follow in V3.

For a portfolio, describe the implemented protocols and reproducible fault tests.
Do not claim benchmark numbers, exactly-once execution, production usage or high
availability that this repository has not demonstrated.
