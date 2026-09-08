# Execution policy and retry semantics

M4.1a defines a pure, immutable `ExecutionPolicy` and `retry_delay`. It does not yet
change execution or persistence. Later M4 commits pin policies to Workflow versions
and atomically persist retry eligibility with Attempt settlement.

| Field | Default | Range / meaning |
| --- | --- | --- |
| max_attempts | 1 | 1–100 total Attempts, including the first; 1 disables retries. |
| timeout_seconds | 300 | 1–86400 seconds per Attempt, measured from lease acquisition; renewal must not reset it. |
| initial_backoff_ms | 1000 | 1–86400000 milliseconds before exponential growth. |
| max_backoff_ms | 60000 | 1–86400000 milliseconds, at least the initial delay. |

After failed Attempt number `n`, retry is allowed only if `n < max_attempts`.
Compute `cap = min(max_backoff_ms, initial_backoff_ms * 2**(n - 1))`, then select
equal jitter in `[cap / 2, cap]`, rounding up to integer milliseconds. A caller
supplies a finite sample in `[0, 1]`. The pure function performs no random sampling,
clock reads, sleep, state changes or I/O. Budget checks precede exponentiation so
large historical Attempt numbers cannot cause unbounded computation.

Equal jitter retains a positive delay and spreads retries after shared outages.
Its trade-off is extra latency versus zero-delay retries. The eventual transaction
must persist `database_observed_at + delay` once; repeated scans and completion
replays must not move that eligibility time. HTTP transport retries remain separate
from Task retries and keep their existing identities.

A failed Attempt remains terminal. Retrying moves its Task from RUNNING to
RETRY_WAIT, later to READY, and creates a new Attempt only on a subsequent claim.
Exhausted tasks become FAILED. Stable Task identity is retained across Attempts;
external effects still require cooperating business idempotency.

## M4 implementation sequence

1. M4.1a: execution policy and bounded backoff helpers (implemented).
2. M4.1b: immutable Workflow policy publication and compatibility (implemented).
3. M4.2a: append-only retry schedule schema (implemented); M4.2b: atomic failure
   settlement (implemented); M4.2c: due-task promotion (implemented);
   M4.2d: HTTP and Worker retry acceptance.
4. M4.3: hard Attempt timeout admission and expired ownership recovery.
5. M4.4: Worker crash scanning, recovery coordination, stale-result races and
   end-to-end failure acceptance, followed by the M4 milestone review.

Each larger step will be split into independent commits for its schema, transaction
behavior and acceptance checks. Policies apply to business failure, timeout and lost
ownership within the configured attempt budget; failure categories remain distinct
in Attempt history.

## Durable retry records (M4.2a)

Revision `0010` adds `task_retry_schedules`, keyed by Attempt ID, with terminal
failure outcome, `scheduled_at` and `available_at`. A deferred composite FK binds
the record to the Attempt's actual FAILED/TIMED_OUT/LOST status. Times are finite
and eligibility strictly follows scheduling. All UPDATE/DELETE/TRUNCATE operations
are rejected, including empty statements. Existing executions are retained.

This is history, not a queue copy: the eventual reconciler must select only the
latest Attempt of a RETRY_WAIT Task. An old schedule cannot authorize another
retry. Per-Run scanning uses existing Task/Attempt indexes; a global deadline
index is unnecessary for this bounded scan design. Downgrade discards eligibility
records and is a planned, destructive rollback requiring stopped writers.

## Atomic failure settlement (M4.2b)

A new FAILED completion reads the pinned policy under existing ownership locks.
Within its budget, it writes an immutable retry record using the completion's
post-lock database timestamp and moves the Task to RETRY_WAIT. Otherwise the Task
becomes FAILED. Attempt status and completion receipt remain FAILED in either case.
All writes share one caller-owned transaction, including deferred COMMIT checks.
Existing receipt replay returns before policy evaluation or entropy sampling.
No extra Attempt is allocated here, and retries cannot yet become READY until
M4.2c installs the due-time reconciler. Default schema 1 failures remain permanent.

## Due-task promotion (M4.2c)

Scheduler reconciliation now samples `clock_timestamp()` after locking the Run
and its Tasks. RETRY_WAIT Tasks require a matching failure schedule on their latest
Attempt, within the pinned attempt budget. At `observed_at >= available_at` they
become READY; old schedules are retained but cannot authorize later Attempts.
Missing/inconsistent retry history aborts the transaction rather than silently
losing a Task. The same Run lock serializes simultaneous Scheduler proposals;
rollback preserves RETRY_WAIT. A new claim allocates the next Attempt, retaining
the Task's business idempotency key. No timer or cursor must survive a restart.
