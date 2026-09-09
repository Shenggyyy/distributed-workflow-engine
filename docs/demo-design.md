# Demonstration design and evidence contracts

The optional demo explains the completed engine using actual executions. It lives
in the existing package/API process and serves local vanilla HTML/CSS/JavaScript.
There is no additional dashboard service, broker, frontend build or shell-command
HTTP interface. This document consolidates the enduring phase D/E decisions;
[original acceptance](demo-review.md) and [six-step acceptance](demo-flow-review.md)
retain dated observations and screenshots. The current [guide](demo.md) explains
how to run it.

## Isolation and trusted work

A standalone Compose project, PostgreSQL database, secret and loopback API port
isolate demonstrations. The local CLI checks a local Docker context and exact
project/service/Run labels and immutable container identity before stopping a
Worker. Each scenario creates a fresh Run; existing data is retained. Workers
register with the existing protocol, are scoped to their Run, and execute only
explicitly registered trusted timed Handlers. The stock engine roles do not enable
the demo. Commands do not truncate, downgrade, reset or remove volumes.

The Scheduler uses normal durable discovery in the dedicated demo database.
Workers pull work; their ownership is decided by the normal claim transaction,
not assigned by the browser or preconfigured per task. The script starts replacement
Worker B after a scoped SIGKILL. This is not autoscaling or checkpoint resume.

## Evidence and clocks

- `acquired_at`: authoritative database time when the Attempt was allocated.
- Handler START/PULSE/FINISH: samples taken inside the actual trusted callable.
  `monotonic_ns` comes from Linux CLOCK_MONOTONIC. A clock domain identifies the
  kernel boot ID and frozen MONOTONIC offset; compare only identical domains.
  Docker Desktop can give containers different time namespace IDs with identical
  offsets. Read `/proc/self/timens_offsets`; fail closed if unavailable or if the
  current and child namespaces differ. Raw sample values are never rewritten.
  See [Linux time namespaces](https://man7.org/linux/man-pages/man7/time_namespaces.7.html).
- `recorded_at`: database time receiving each Handler observation, distinct from
  its original sample. It is not the precise Handler start or COMMIT instant.
- Completion `accepted_at`: database post-lock admission timestamp, not Handler
  return time or exact COMMIT timestamp.

Timeline bars use only observed Handler samples. A missing FINISH remains unknown;
the bar ends at the last observed PULSE, never at guessed lease expiry. Live bars
advance on new real samples, not browser interpolation. Completed overlapping
intervals in a common domain prove overlapping callable lifetimes (including their
intentional timed work/waits), not simultaneous CPU utilization. This is a single
machine/multiple Linux-container demonstration, not multi-machine validation.

## Storage, consistency and migration

Additive Alembic `0011` creates demo Run/session membership and append-only Handler
observations with foreign keys to existing identities. No existing row is changed,
backfilled or removed. Observations have a separate invocation UUID, sequence and
phase; no fabricated finish on crash. Demo registrations bound scope. Unknown or
non-demo identities are rejected. Observations are evidence, never authorization:
late evidence cannot renew a lease, settle an Attempt or modify core state.

Observation writes use independent short transactions. No observation writer takes
core ownership locks; no core writer reads observations. Existing lock order stays
Run -> Worker -> Task -> Attempt -> lease. Snapshot queries use a read-only REPEATABLE
READ transaction for a coherent DAG/Task/Attempt/session/evidence view, without row
locks. Polling every 500 ms may miss brief state transitions; persisted Attempt and
retry history remains visible. No claims of zero-delay or globally synchronized UTC.

The default API/Worker do not enable demo routes/handlers. Downgrade of `0011`
would remove demo history and is an explicit operator action; the demo commands
never downgrade, truncate or remove volumes. Fresh scenarios use new identities.


## Six steps, one real snapshot

1. Submission: Run ID, scenario, published definition and aggregate state.
2. Dependencies: top-to-bottom DAG, parallel roots and the join.
3. Claimable view: actual PostgreSQL READY rows and PENDING dependencies. Explain
   that satisfied dependencies do not themselves change PENDING to READY: a later
   scheduling transaction must do so. RETRY_WAIT is distinct from dependency wait.
4. Workers: parallel cards show confirmed RUNNING ownership, configured slots,
   observed Handler evidence, heartbeat and latest lease renewal/deadline. A count
   of allocations is not proof of running handlers or an OS slot identifier.
   Worker pull asks for work with free capacity; transactions authorize ownership.
5. Results and another scheduling pass: persisted completion/retry evidence, current
   dependency checks and links back to steps 3/4. Old LOST -> backoff -> new Attempt
   is a fresh execution, not a handoff/resume. Script-started replacement is not
   autoscaling. Historical retry schedules remain visible after eventual success.
6. Completion: aggregate state, work per owner, real sampled timeline and Attempt
   details/raw data. Normal Worker exit and later heartbeat expiry do not imply a
   failed Task. Terminal lease timestamps do not describe current ownership.

All dependency explanations are derived from the same snapshot and labelled as
current conditions, not a historical event log. Deadline comparisons use its DB
timestamp, never the browser clock. Missing evidence stays unknown. Different
clock domains remain separate; no sample interpolation or exactly-once claim.

## Minimal design change

Expose existing `attempt_leases.last_renewed_at` as an additive nullable snapshot
field. No schema change, migration, write path, new service or framework. Preserve
MVCC, all lock ordering, lease/retry/idempotency and stale-result contracts.
Pure JavaScript read-model helpers make waiting/recovery semantics testable.

Keep clearly labelled trusted timed demonstration Handlers. A real sales report
would require explicit artifact transfer/storage and retry-safe output contracts;
DAG edges alone do not pass data. That extra feature is outside this phase.


## Repeatable scenarios and acceptance

The distribution CLI starts both one-slot containers with `--cohort-size 2`.
Before claiming, each demo runtime uses a 60-second wait budget for its own session
and two distinct scoped Worker names to be ACTIVE with fresh database-clock
heartbeats. It renews its own heartbeat while waiting and releases each read
transaction before sleeping. This is a demo startup rendezvous, not task routing
or a persistent quorum guarantee; a Worker can still fail after the check. The
normal engine claim transaction remains the only ownership authority.
Existing bounded HTTP/database operations can delay timeout reporting; an expired
budget or rejected heartbeat prevents entry into the Worker loop.

- Parallel: one Worker, two execution slots, four timed roots followed by Join.
- Distribution: two independent one-slot Workers claim from one Run; the observed
  owners and sampled overlap establish the result, not an assumed assignment.
- Recovery: stop the dedicated Worker after actual START evidence, observe saved
  loss/retry/new allocation and final success. A missing FINISH remains unknown.

Default demonstration windows are six seconds for heartbeat/lease and a persisted
5–10 second jittered retry delay. These are visualization settings, not an SLA.
Core timeout, locking, idempotency and stale-result fencing are unchanged.

Browser acceptance must see real overlap, ownership, waiting dependencies and the
recovery loop. Polling can miss brief states. There is no complete network trace,
physical slot ID, Docker stop event or READY transition history; only confirmed
allocation and current conditions are displayed. History and evidence remain in
PostgreSQL; storage lifecycle and multi-machine performance are outside this demo.

## Presentation language

`static/messages.js` owns the English and Simplified Chinese text, including
dynamic explanations. `static/i18n.js` selects the primary browser language
(Chinese tags use zh-CN; other or missing tags use English). A valid explicit
choice in `dwe.demo.language` takes precedence. Storage access, including the
localStorage getter itself, is optional and guarded; failure keeps the in-memory
choice usable. Translation inserts parameters as text, never HTML.

The page integration redraws the existing snapshot in the chosen language. It
must not create/restart Runs, change the selected Run, trigger additional polling,
or modify evidence. Raw names, identifiers, JSON, status codes and UTC/monotonic
clock meanings stay intact. Static details elements retain their expanded state;
the visible section's offset is preserved where text reflow permits. Matching
catalog keys and parameters, preference precedence and blocked storage are covered
by Node tests without a browser or frontend build dependency.
