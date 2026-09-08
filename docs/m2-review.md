# M2 execution milestone review

M2.5b closes the first scheduling/execution loop. The implementation accepts a
static DAG, creates durable Task rows, registers a Worker, grants and renews Attempt
ownership, executes trusted handlers in child processes, retains completion
receipts and releases satisfied dependencies through a separate Scheduler.

## Validation at this checkpoint

- 1356 tests passed on Windows/Python 3.13 with a disposable PostgreSQL 18 database.
  The only warning is the existing Starlette/AnyIO deprecated BlockingPortal alias.
- Ruff lint/format, strict mypy, actionlint and source/wheel builds passed.
- Ten real execution checks passed: Workflow, Run, Worker, claim, renewal,
  completion HTTP; host/container Worker; host/container Scheduler diamond DAGs.
- Runtime images use the non-root application UID. Worker has no database secret
  mount. Scheduler uses the existing explicit secret/configuration boundary.
- Packaged artifacts contain Worker and Scheduler entry points and exclude local
  instructions, private credentials and caches.

## Correctness review

| Boundary | Evidence and conclusion |
| --- | --- |
| Transaction publication | Repositories do not commit. HTTP/service layers publish after successful COMMIT; deferred failure tests verify rollback. |
| Lock ordering | Execution uses Run -> Worker -> Task -> Attempt -> lease. Readiness uses Run -> Tasks and acquires no lower-ranked ownership locks afterwards. Controlled wait/rollback tests verify handoff. |
| Duplicate delivery | Stable claim poll identity and immutable completion receipts survive committed response loss. Replays create neither a second Attempt nor a second receipt. |
| Ownership | Renewal/completion validate exact session/token, structural state and post-lock database time. Local Worker admission requires a fresh renewal. |
| Child lifecycle | Supervisor checks conservative monotonic windows while HTTP runs separately. Child exit/lease loss does not fabricate business completion; direct-child cleanup is tested. |
| Scheduler crash/retry | Readiness is reconstructible from committed Task state. Concurrent/repeated scans apply each PENDING -> READY transition once; rollback leaves no provisional dispatch. |
| DAG execution | Real independent processes complete A -> B/C -> D; D is not released by only one successful parent. |

## Remaining roadmap boundaries

This checkpoint is **not the finished MVP**. Worker capacity is one and Run IDs are
explicit; M3 adds discovery and parallel/distributed execution acceptance. M4 must
recover abandoned reservations, implement retries/backoff and hard execution
timeouts. M5 must propagate failed dependencies, aggregate Run outcomes, and
demonstrate business-side idempotency and end-to-end failure recovery.

All-successful Tasks currently leave their Run RUNNING. A failed parent leaves
dependent Tasks PENDING. A crashed Worker can retain capacity until the future
recovery scanner expires its Attempt. Killing a child cannot undo an external
effect; stable Task identity is only the input to cooperating business idempotency.
No exactly-once execution or general external-effect fencing guarantee is claimed.
