# Failure scenarios and reproducible evidence

The engine assumes trusted processes and a durable PostgreSQL authority. Liveness
requires eventually reachable storage, running Schedulers and suitable Workers.
Recovery is bounded polling, not an instantaneous failure detector.

| Failure window | Durable behavior | Evidence |
| --- | --- | --- |
| Duplicate Run submission | Same key/version replays the same Run; different version conflicts. | `test_run_idempotency.py`, `check_run_api.py` |
| Claim committed, response lost | Same session/request replays its valid allocation; unresolved claims keep a Worker slot reserved. Empty claims are sticky. | `test_claim_requests.py`, `test_worker_transport.py` |
| Completion committed, response lost | Exact owner/result replays the historical receipt, even after Run settlement. No new execution or delay sample. | `test_completions.py`, `test_settlement.py`, `test_worker_loop.py` |
| Worker dies after claim | Lease/hard deadline expires; Scheduler records LOST/TIMED_OUT and retry or permanent failure. | `test_crash_recovery.py`, `check_dag_execution.py --abandon-claim` |
| Worker dies after business commit | New Attempt executes again with the same Task key. Cooperating destination suppresses its duplicate increment. | `test_crash_recovery.py -k effect`, `test_business_idempotency.py` |
| Scheduler dies before COMMIT | PostgreSQL rolls back its provisional changes. Another pass re-reads durable state. | `test_scheduling.py`, `test_settlement.py`, recovery rollback tests |
| Scheduler dies after COMMIT | Retry schedules, READY rows and terminal states persist; a fresh Scheduler continues without process memory. | `test_retry_scheduling.py`, `test_recovery_service.py`, process crash acceptance |
| Two Schedulers recover the same Attempt | Ownership locks serialize the decision; the loser sees terminal history and makes no second retry schedule. | `test_recovery_service.py`, `test_recovery.py` |
| Renewal/completion races expiry | Run/Worker/Task/Attempt/lease locks and a post-lock clock determine one valid outcome. Late/stale results are rejected. | `test_completion_races.py`, `test_recovery.py`, `test_attempt_timeout.py` |
| Heartbeat expires while lease is valid | New claims are denied; the valid Attempt may renew/complete. Registry loss alone does not duplicate execution. | `test_recovery_service.py`, `test_worker_heartbeat.py` |
| Handler exceeds fixed timeout | Local supervisor terminates direct children; server rejects late admission and recovers independently. Renewals cannot extend the deadline. | `test_worker_timeout.py`, `test_attempt_timeout.py` |
| Task exhausts retry budget | Task FAILED; pending descendants SKIPPED; independent branches finish before Run FAILED. | `test_settlement.py`, `check_dag_execution.py --failed-branch` / `--retry-failure` |
| Database unavailable / lock timeout | No success is published before commit. Classified transient transport/storage errors retry with stable identity or a fresh Scheduler pass. | `test_scheduler_service.py`, `test_worker_transport.py`, HTTP commit-failure tests |
| Busy Run or saturated Worker | Automatic scans skip busy Run locks; Worker reservations and persisted capacity prevent overclaiming. Polling resumes later. | `test_recovery_service.py`, `test_claims.py`, `test_worker_parallel.py` |

Test filenames above are under `tests/integration/` unless they exercise pure or
Worker control code under `tests/`. Smoke scripts are under `scripts/`. The tests
combine PostgreSQL lock/commit barriers, injected response loss and real spawned
process exits. They do not constitute exhaustive network-partition or database
failover testing. Model the exact fault window before drawing broader conclusions.

## Run the acceptance cases

Start the API/database and apply migrations as shown in the README. Then:

```console
uv run --locked python scripts/check_dag_execution.py --scheduler-container
uv run --locked python scripts/check_dag_execution.py --scheduler-container --failed-branch
uv run --locked python scripts/check_dag_execution.py --scheduler-container --retry-failure
uv run --locked python scripts/check_dag_execution.py --scheduler-container --abandon-claim
uv run --locked python scripts/check_distributed_execution.py --container
uv run --locked pytest tests/integration/test_crash_recovery.py --database-env-file .env.database-test
```

Use a disposable database for experiments and point host test settings at its
published port. These scripts create fresh runs and stop only helper processes
they start. Automatic Worker discovery tests require an otherwise empty READY
queue. Do not delete leases, receipts or retry history to manually retry work.

## What recovery does not guarantee

Engine fencing cannot retract an external side effect, kill a remote process,
or guarantee exactly-once execution. Finite retries can exhaust during repeated
infrastructure failures. A paused or isolated old Worker may resume concurrently
with a replacement; a destination must use an idempotency/fencing contract suited
to its operation. Direct child cleanup does not contain arbitrary descendants.
No cancellation, dead-letter handling, tenant isolation, HA database failover or
global rate limits are implied by the MVP tests.
