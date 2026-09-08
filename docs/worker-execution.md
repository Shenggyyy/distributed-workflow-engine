# Handler process lifecycle

M2.4c.1 introduces `ProcessExecution`: one trusted handler in a Python `spawn`
process, compatible with Windows and Linux. The parent passes the immutable
registration snapshot and business-only context; no lease token, connection or
transport client enters the child. Registered functions must be pickleable
module-level callables. Spawn failure is explicit and sanitized.

The child invokes the registry once and sends only a bounded `CompletionResult`
over a one-way pipe. Python stdout/stderr are redirected to the null device;
exceptions are not printed or returned. This is not a sandbox: trusted code can
still perform I/O, use OS file descriptors and create external side effects.

The parent polls without blocking for handler completion. It validates received
results and detects EOF/abrupt exit as `ExecutionLost`, never fabricating a FAILED
business receipt. Repeated polls retain the same result. Cleanup terminates and
joins the child, escalates to kill if needed, and closes process/pipe handles.
Cleanup is idempotent; failure to stop a child is explicit. This controls the direct
child, not arbitrary subprocess trees that a handler might create.

This layer has no lease admission, hard task timeout or automatic re-execution.
The next subtask, M2.4c.2, owns claim/renew/heartbeat control and invokes cleanup on
loss of ownership or shutdown. M4 adds server-enforced timeout and recovery.
Terminating a process cannot undo an external effect; business idempotency remains
required. Never run a handler while holding a database transaction.

```console
uv run --locked pytest tests/test_execution.py tests/test_handlers.py
```

Tests spawn real processes for success, failure, unknown task types, process exit,
unserializable registration and a hanging handler, and verify child cleanup.
