# M3 concurrent execution review

M3 provides bounded Run discovery, cooperative Scheduler scanning, automatic
Worker selection and 1–32 parallel execution slots per Worker process. PostgreSQL
remains the durable queue and authority for ownership; no external broker or
leader-election service is introduced.

## Validation

- 1398 tests passed on Windows/Python 3.13 and disposable PostgreSQL 18. The only
  warning remains Starlette's deprecated AnyIO BlockingPortal alias.
- Ruff lint/format, mypy for both Linux and Windows, actionlint and source/wheel
  builds passed. Packaged resources and local-file exclusions were checked.
- Fourteen real HTTP/process/container checks passed: the existing six API checks,
  explicit and automatic host/container Workers, host/container Scheduler DAGs,
  and two-Worker/two-Scheduler host/container execution.
- The multi-process PostgreSQL test synchronizes four handler children across two
  Worker processes, while two independent Scheduler processes scan the same Run.
  Six Tasks produce six Attempts, leases and receipts, with three per Worker.

## Correctness and trade-offs

| Area | Review conclusion |
| --- | --- |
| Discovery | READ COMMITTED keyset pages are bounded advisory snapshots. New eligibility behind the cursor waits for the next traversal. UUID ordering is not FIFO. |
| Scheduler distribution | Each Run has its own transaction. Automatic scans skip busy Run locks, then lock the complete Task set. Overlapping scans are safe but can duplicate reads; a busy Run can be delayed. |
| Lock order | Execution keeps Run -> Worker -> Task -> Attempt -> lease. Readiness holds Run -> Tasks and acquires no later Worker ownership locks. Discovery retains no row locks across reconciliation. |
| Local backpressure | Each slot retains one claim/report identity; unresolved claims consume capacity. Finite completion limits count active reservations, preventing excess claims. One shared heartbeat and at most one work request per slot bound network concurrency. |
| Parallel execution | Separate spawned children overlap actual execution. Child cleanup is polled rather than joined inside normal supervision, so another slot can complete/renew while cleanup proceeds. Shutdown signals all children before joins. |
| Crash and duplicate boundaries | Existing post-lock clock/fencing checks and completion replay remain unchanged. Lost responses do not replace claim identity or rerun a completed handler. This does not prevent duplicate external effects after ownership loss. |
| Failure isolation | Fatal slot control/execution errors conservatively stop that Worker and its direct children. Other Worker/Scheduler processes remain independent. M4 must recover abandoned Attempts. |
| Resource boundaries | Runtime slot count is capped at 32; it is not a throughput guarantee. Database pool contention, API request limits, OS scheduling and process startup still affect latency. Stronger fleet-wide backpressure and measured scaling remain later work. |

## Remaining MVP work

M4 adds persisted retry policy/deadlines, exponential backoff with jitter, hard
Attempt timeout, expired ownership recovery and Worker crash detection. M5 adds
failed-dependency propagation, final Run aggregation, a cooperating business
idempotency example, failure scenarios and final end-to-end acceptance.

At this checkpoint, failed parents still leave descendants PENDING, all-successful
Tasks leave the Run RUNNING, and abandoned Attempts require future recovery. There
is no exactly-once execution guarantee or external side-effect fencing guarantee.
