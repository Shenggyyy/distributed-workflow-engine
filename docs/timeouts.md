# Attempt timeout and ownership expiry

M4.3a enforces fixed execution deadlines on the server. Deadline equals immutable
`lease.acquired_at + pinned_task.execution_policy.timeout_seconds`. It includes
dispatch, handler startup, execution and result delivery. Renewal extends ownership
only; it never resets this deadline. Schema 1 uses the fixed 300-second default.

New completion, lease renewal and claim replay require a post-lock observation
strictly before this deadline, as well as ordinary lease ownership validation.
Completion/renewal return HTTP 409 `attempt_timed_out`; claim replay returns its
existing unavailable-allocation conflict. A valid historical completion receipt
still replays after either deadline. HTTP rejection itself does not settle the
Attempt: recovery performs that in a separate ordered transaction.

Deadline is derived from two immutable persisted inputs, avoiding a redundant
mutable field. PostgreSQL time is authoritative; Worker monotonic time only bounds
local execution conservatively. This cannot undo external side effects, so timeout
does not imply a handler did nothing or guarantee exactly-once effects.

## M4.3 sequence

1. M4.3a: server timeout admission (implemented).
2. M4.3b: Worker deadline supervision and child termination (implemented).
3. M4.3c: ordered expiry settlement and stale-result races (implemented).
4. M4.4: bounded recovery scans, Worker crash detection and fault acceptance.

M4.4 is split into M4.4a advisory expiry discovery, M4.4b Scheduler recovery
coordination, and M4.4c crash/timeout end-to-end acceptance and milestone review.

M4.4a and M4.4b are implemented.

## Worker supervision

After mandatory renewal, the Worker maps the remaining fixed server duration to
`request_started_monotonic + 0.9 * remaining`. Subsequent renewals may shorten this
local deadline but cannot extend it. The main supervisor checks it while execution
or cleanup is active, including while an HTTP request is stalled. Expiry stops the
Worker and terminates all its handler children using the existing bounded cleanup.
This conservative incarnation-wide shutdown trades throughput for simple safety.

The Worker does not manufacture FAILED or TIMED_OUT reports. A completion already
sent with uncertain acceptance may still be retried after local execution ended:
only the server's historical receipt or current admission rules decide the result.
Server recovery remains necessary after process crashes and local abandonment.

## Ordered recovery transaction

`RecoveryRepository.recover` re-reads one Attempt under Run → Worker → Task →
Attempt → lease locks and samples the database clock after all locks. A terminal
Attempt is already settled and is skipped. A RUNNING Attempt expires at the earlier
of lease expiry and fixed execution timeout. Earliest timeout (including a tie)
means TIMED_OUT; earliest lease expiry means LOST, even if scanning happens later.
Clock regression before the last renewal defers recovery conservatively.

Recovery atomically settles Attempt/Task and, within budget, writes a retry plan.
It never fabricates a Worker completion receipt or modifies ownership history.
Worker heartbeat state alone cannot revoke a live lease; even a STOPPED owner is
recoverable once its deadline expires. Repeated recovery cannot reschedule a settled
Attempt. Competing completion and renewal use the same locks and recheck admission;
old results cannot settle a replacement Attempt. Automatic scans follow in M4.4.

## Bounded discovery (M4.4a)

Discovery returns up to 100 expired ACTIVE Worker IDs using the existing deadline
index. Per active Run it examines at most 1000 current owned Attempts, deriving
hard deadlines from the pinned version, and returns only due IDs. It takes no row
locks and releases its transaction before mutation. Renewal/completion may make a
hint stale; each subsequent recovery operation must re-read under ownership locks.
Historical unleased Attempts from pre-Worker storage are outside automatic lease
recovery; all current claim paths atomically create leases. No global timer queue
or new index is needed for this bounded per-Run approach.

## Scheduler coordination (M4.4b)

Each global Scheduler page expires a bounded batch of Worker sessions, discovers
active Runs, then recovers due Attempts before readiness/retry reconciliation.
Discovery, each Worker expiry, each Attempt recovery and each Run reconciliation
use separate transactions. In particular, Worker expiry never retains a lock while
acquiring a Run lock, and recovery never follows Task locks in the same transaction.
Stop signals are checked between candidates. `--once` handles one page; repeated
passes resume from persisted state. Selected-Run mode recovers that Run's Attempts
without changing the global Worker registry. Transient database errors retry a new
pass using existing Scheduler policy; already committed decisions remain durable.
Recovery first tries the Run lock with SKIP LOCKED, so a busy Run cannot hold up
recovery/readiness for independent Runs. The next scan revisits skipped work.

Multiple Schedulers can observe the same candidates, but only the first valid
settlement changes state. Recovery logs are emitted after COMMIT. No leader or
distributed in-memory timer is required. Discovery scans bound memory, while
per-Run coordination limits throughput for very wide hot DAGs.

## Crash acceptance (M4.4c)

`tests/integration/test_crash_recovery.py` starts a real Worker process, commits
its HTTP claim and exits the process before completion. Independent replacement
execution succeeds after lease loss or hard timeout, and stale completion fails.
Run `uv run --locked python scripts/check_dag_execution.py --scheduler-container
--abandon-claim` for the container recovery smoke. See [the M4 review](m4-review.md).
