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
2. M4.3b: Worker deadline supervision and child termination.
3. M4.3c: ordered expiry settlement and stale-result races.
4. M4.4: bounded recovery scans, Worker crash detection and fault acceptance.
