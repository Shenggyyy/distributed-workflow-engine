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
3. M4.3c: ordered expiry settlement and stale-result races.
4. M4.4: bounded recovery scans, Worker crash detection and fault acceptance.

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
