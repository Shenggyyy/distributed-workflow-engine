# Trusted task handlers

M2.4a defines an execution boundary without starting a Worker. `worker/handlers.py`
contains an immutable registry snapshot of explicit `HandlerRegistration` entries.
Workflow `task_type` values are exact registry keys, never Python import paths,
shell commands or uploaded code. Duplicate and malformed registrations fail startup.

A synchronous handler receives a frozen `HandlerContext` with Run/version, Task,
Attempt and DAG node identities. It returns the existing `CompletionResult`:
SUCCEEDED, or FAILED with a bounded error code. The context carries no lease token,
database connection or API client. Execution happens outside database transactions.
Only the Worker control loop may claim, renew and report completion.

`context.idempotency_key` is the Task UUID: stable across retries of the same Task,
different for a new Run. A handler must atomically bind that key to its business
effect in the destination system. Supplying a key alone does not deduplicate work;
two calls to the registry can execute the same handler twice. External effects are
not rolled back when a result report fails or a lease expires.

The builtins are `demo.echo` (successful no-op) and `demo.fail` (deterministic
`demo_failure`). Schema v1 has no input/output payload or data passing between DAG
nodes. Unknown keys produce `unknown_task_type`, unexpected Exceptions produce
`handler_exception`, and invalid returned results produce `invalid_handler_result`.
Exception messages are neither logged nor sent as completion errors. Process-level
interrupts escape instead of being mislabeled as business completion.

Handlers are trusted application code. The registry provides no sandbox, timeout,
lease admission or exactly-once guarantee. M2.4c will keep network control separate
from handler execution; M4 will add timeout/recovery semantics. Side-effect
idempotency is demonstrated in M5. Worker routing by capabilities remains V2, so a
Worker may currently claim an unsupported type and report failure.

Verify without Docker: `uv run --locked pytest tests/test_handlers.py`.
Next: M2.4b adds Worker HTTP transport and stable identities for uncertain responses.
