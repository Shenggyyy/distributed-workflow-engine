# Single-slot Worker control

M2.4c.2 connects the handler subprocess and HTTP transport for one explicit Run.
It requires a fresh Worker session with `max_concurrency=1`. A Worker incarnation
can run only once. CLI/container entry points follow in M2.4d; run discovery and
parallel capacity are later milestones.

## Admission and operation

1. Register with one stable session UUID. Registration retry does not renew a
   heartbeat, so obtain a fresh successful heartbeat before claiming.
2. Poll with a frozen ClaimPoll. Retry uncertain delivery with that same identity;
   create a new request ID after confirmed no-work or completed execution.
3. Even a successful claim replay may be old. Renew its lease before executing.
   Validate the renewal, then create exactly one local handler subprocess.
4. Maintain heartbeat separately from the work request. Renew a running Attempt
   periodically. Poll the child without blocking; completion never overlaps an
   in-flight renewal. Once the child returns, retain its result and close the child.
5. Send the original completion until confirmed, rejected, stopped or local Worker
   liveness expires. No new claim is issued while that completion is uncertain.
   A lost response does not invoke the handler again or change its result.

At most two daemon HTTP request threads exist per incarnation: one heartbeat and
one work operation. The supervisor remains responsive if either request blocks.
It checks a stop Event each tick and terminates its direct child on exit. An already
sent request can still commit after local stop; its late response cannot restart
execution. Daemon request threads are abandoned on process exit, not forcibly
cancelled in Python. Reusing a stopped incarnation is prohibited.

## Clocks and failures

Fresh heartbeat/renewal responses provide a duration from server observation to
deadline. The local conservative deadline is **request-start monotonic time plus
90% of that duration**, with renewal due after one third. Request latency consumes
the window. A delayed renewal with no remaining window never admits a handler.
The supervisor checks local expiry even while a request is blocked. This estimate
assumes sufficiently stable clock rates; server post-lock database checks remain
authoritative. No local estimate can prevent duplicate external effects during
clock jumps, process pauses or partitions.

The 10% margin is a conservative operational choice, not a fencing guarantee.
Tick interval, OS scheduling and process termination can delay actual stopping.
Only server ownership checks fence accepted engine results; external systems must
cooperate with Task idempotency keys. Renewal does not extend the future M4 hard
execution deadline. The current loop has no hard task timeout.

Transient transport/HTTP errors retain operation identity and use bounded-rate
fixed retry intervals. Terminal ownership errors, invalid protocols, child loss,
local heartbeat expiry and lease expiry stop this incarnation without fabricating
a completion. A stale heartbeat causes conservative local shutdown even though
the server may still accept completion under a valid lease. `run_inactive` during
claim ends normally. Stop is immediate cleanup, not a durable graceful-drain
protocol; server recovery is responsible for unfinished reservations in M4.

The loop currently targets one known Run and keeps polling when it has no READY
task; no-work does not prove workflow completion. `max_tasks` bounds demonstrations
by confirmed completion count, including failed handlers. There is no client
outbox. Logs contain fixed events and Attempt IDs, never tokens or handler text.

## Verify

```console
uv run --locked pytest tests/test_worker_loop.py tests/test_execution.py
uv run --locked pytest tests/integration/test_worker_loop.py --database-env-file .env.database-test
```

Tests cover lost responses, stable poll/result identities, fresh-renewal admission,
slow/stalled control requests, periodic renewal during execution, shutdown, child
loss, and real spawned success/failure handlers against the HTTP API and PostgreSQL.
Next is M2.4d: configuration, CLI and container execution checks.
