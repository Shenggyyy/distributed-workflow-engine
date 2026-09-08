# M4 retry, timeout and recovery review

M4 is complete. Immutable schema 2 policies, append-only retry schedules and
post-lock database-clock decisions survive Scheduler and Worker restarts.

## Validation

- 1480 tests passed on Windows/Python 3.13 and disposable PostgreSQL 18, including
  two spawned Workers that exit immediately after a durable HTTP claim. A new
  Scheduler and replacement Worker finish both lease-loss and hard-timeout cases;
  the old owner's completion is rejected.
- Host and container Scheduler abandoned-claim checks passed, along with container
  retry exhaustion and two-Worker/two-Scheduler DAG execution.
- Ruff, Linux/Windows mypy, actionlint and distribution builds passed. The existing
  Starlette/AnyIO deprecation warning remains. M4.4b's three CI jobs passed.

## Correctness review

| Area | Contract |
| --- | --- |
| Retry | Total Attempt budget includes the first claim. One immutable eligibility record belongs to one failed Attempt. Only the latest Attempt authorizes RETRY_WAIT -> READY. Equal jitter is sampled once inside failure settlement. |
| Atomicity | Attempt outcome, Task outcome, retry schedule and any Worker receipt commit together. A lost completion response replays the original receipt without sampling another delay. |
| Timeout | Fixed deadline derives from first lease acquisition and pinned policy; renewal cannot extend it. New completion, renewal and claim replay reject at the deadline. Historical receipts remain replayable. |
| Recovery | The earlier deadline decides LOST versus TIMED_OUT, with timeout winning a tie. Re-reading under Run -> Worker -> Task -> Attempt -> lease locks fences stale decisions. Recovery creates no Worker receipt. |
| Heartbeat | Registry liveness controls new claims; an expired heartbeat alone does not revoke an independently valid Attempt lease. |
| Scheduling | Discovery is advisory and bounded. Each mutation uses a fresh transaction. Busy Run locks are skipped; Worker expiry never retains its lock while taking a Run lock. |
| Worker | Local monotonic supervision terminates direct handler children conservatively. The server remains authoritative, including after blocked HTTP or process death. |

External side effects can still happen twice. Fencing engine state cannot undo a
remote effect; M5 supplies a cooperating business-idempotency demonstration.
Recovery latency includes scan/pool contention; this is not a deadline SLA.
Clock regression defers unsafe admission/recovery. Historical unleased Attempts
are not automatically recovered. Process cleanup is not a security sandbox.

M5 completes failed-dependency propagation, terminal Run aggregation, business
idempotency acceptance and the final MVP audit.
