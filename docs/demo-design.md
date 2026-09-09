# Demonstration design and evidence contracts

The optional demo explains the completed engine using actual executions. It lives
in the existing package/API process and serves local vanilla HTML/CSS/JavaScript.
There is no additional dashboard service, broker, frontend build or shell-command
HTTP interface. This document consolidates the enduring phase D/E decisions and
the current diamond scenario contracts;
[original acceptance](demo-review.md) and [six-step acceptance](demo-flow-review.md)
retain dated observations and screenshots. The current [guide](demo.md) explains
how to run it. New diamond execution/browser acceptance is pending G6; earlier
reviews and screenshots describe the saved definitions used at their own dates.

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
not assigned by the browser or preconfigured per task. Recovery starts two Workers
before work begins and stops C's actual owner. B's surviving Worker later pulls
C's new Attempt. No replacement starts, no interrupted execution resumes, and no
autoscaling is involved. Worker labels `a`/`b` do not assign Task keys A/B/C/D.

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

The diamond change needs no new schema, query API or core write path. It publishes
fresh schema-version-2 Workflow definitions using new `demo.diamond.*` Handler keys.
Legacy Handler meanings and existing immutable Workflow versions remain unchanged.
Snapshot rendering uses each Run's saved DAG, never the latest scenario factory.


## Six steps, one real snapshot

1. Submission: Run ID, scenario, published definition and aggregate state.
2. Dependencies: the saved top-to-bottom DAG. New Runs show root A, parallel B/C
   branches and join D; historical Runs retain their original graph.
3. Claimable view: actual PostgreSQL READY rows and PENDING dependencies. Explain
   that satisfied dependencies do not themselves change PENDING to READY: a later
   scheduling transaction must do so. RETRY_WAIT is distinct from dependency wait.
4. Workers: parallel cards show confirmed RUNNING ownership, configured slots,
   observed Handler evidence, heartbeat and latest lease renewal/deadline. A count
   of allocations is not proof of running handlers or an OS slot identifier.
   Worker pull asks for work with free capacity; transactions authorize ownership.
5. Results and another scheduling pass: persisted completion/retry evidence, current
   dependency checks and links back to steps 3/4. Old LOST -> backoff -> new Attempt
   is a fresh execution, not a handoff/resume. Current recovery reuses the surviving
   sibling's Worker; historical replacement startup remains explicitly described
   as a script action. Retry schedules remain visible after eventual success.
6. Completion: aggregate state, work per owner, real sampled timeline and Attempt
   details/raw data. Normal Worker exit and later heartbeat expiry do not imply a
   failed Task. Terminal lease timestamps do not describe current ownership.

All dependency explanations are derived from the same snapshot and labelled as
current conditions, not a historical event log. Deadline comparisons use its DB
timestamp, never the browser clock. Missing evidence stays unknown. Different
clock domains remain separate; no sample interpolation or exactly-once claim.

## Read-model boundary

Expose existing `attempt_leases.last_renewed_at` as an additive nullable snapshot
field. No schema change, migration, write path, new service or framework. Preserve
MVCC, all lock ordering, lease/retry/idempotency and stale-result contracts.
Pure JavaScript read-model helpers make waiting/recovery semantics testable.

Keep clearly labelled trusted timed demonstration Handlers. A real sales report
would require explicit artifact transfer/storage and retry-safe output contracts;
DAG edges alone do not pass data. That extra feature is outside this phase.


## Repeatable scenarios

All new scenarios use A -> B/C -> D: A has no dependencies, B/C depend on A, and
D depends on both branches. Planned waits are A=6s, B=8s, C=14s, D=3s; recovery
uses C=20s. Actual sampled lifetimes include observation/processing latency.

For distribution and recovery, the CLI starts both one-slot containers with
`--cohort-size 2`.
Before claiming, each demo runtime uses a 60-second wait budget for its own session
and two distinct scoped Worker names to be ACTIVE with fresh database-clock
heartbeats. It renews its own heartbeat while waiting and releases each read
transaction before sleeping. This is a demo startup rendezvous, not task routing
or a persistent quorum guarantee; a Worker can still fail after the check. The
normal engine claim transaction remains the only ownership authority.
Existing bounded HTTP/database operations can delay timeout reporting; an expired
budget or rejected heartbeat prevents entry into the Worker loop.

- Parallel: one Worker, two slots. A completes before B/C execute concurrently;
  B succeeds before C, leaving D visibly blocked until both branches succeed.
- Distribution: two independent one-slot Workers pull from the same Run. Actual
  distinct B/C owners and sampled overlap establish distribution, not assignment.
- Recovery: stop C's actual Worker while B/C first Attempts overlap. B's surviving
  Worker finishes B once, then can claim C #2 after Lease expiry and retry backoff.
  C restarts from the beginning. D remains blocked until both branches succeed.

Default demonstration windows are six seconds for heartbeat/lease and a persisted
5–10 second jittered retry delay. These are visualization settings, not an SLA.
Core timeout, locking, idempotency and stale-result fencing are unchanged.

## Scoped fault command

`run recovery` waits for both registrations to be fresh, not for Handler START.
Call `fail --run-id RUN_ID` immediately afterward. Its 60-second wait budget allows
legitimate pre-start snapshots; it does not authorize a guessed current execution.
The guard requires the exact saved recovery diamond, successful A, unclaimed D,
and B/C Attempt #1 RUNNING on different active one-slot Workers with valid leases.
Each branch needs a single contiguous START/PULSE invocation, no FINISH, and at
least one second of overlap in one clock domain. Latest receipts must be no more
than two database-clock seconds old. C's last observed elapsed time must be at most
15 seconds, leaving a conservative margin within its 20-second trusted wait.
Completed/retried branches, unexpected identity/definition, stale observations or
expired ownership cause refusal; missing START only permits valid startup waiting.

The target slot is resolved from C's actual scoped Worker name. The command checks
Docker project/service/Run labels, an unpaused running container and immutable ID,
fetches a fresh snapshot identifying the same target, then reinspects by that ID.
It exclusively writes `RUN_ID-fault-before.json` before attempting one SIGKILL to
that exact ID. `RUN_ID-fault.json` records matching Run/Task/Attempt/Worker/invocation
identities, snapshot time and Docker acknowledgement only after success. Neither
file is overwritten. A failed or uncertain KILL is never automatically retried.

The final snapshot request, reinspection and before-file write must fit a
two-second monotonic freshness budget checked immediately before KILL, as well as
the overall wait budget. Fault-specific Docker calls have five-second timeouts.
Database reads and Docker cannot form an atomic transaction; these checks bound
staleness but cannot remove that race. The receipt proves a local command was
acknowledged, not a database event, Handler end timestamp or automatic handoff.

## Acceptance evidence

The automated runner retains actual snapshots under ignored
`.uv-cache/demo-acceptance/`. A successful final Run is insufficient. Required
checkpoints are `root` (A RUNNING with real START, no B/C/D Attempts), `branches`
(first B/C RUNNING with observed overlap) and `join_wait` (B SUCCEEDED, C unfinished,
D PENDING without an Attempt). Recovery also requires `retry` (C #1 LOST while
Task C is RETRY_WAIT and still in backoff, D unclaimed), the exact `fault_before`
snapshot and acknowledged command receipt. Final snapshot is `RUN_ID.json`.

The fixed-diamond validator checks:

- Exact saved schema/definition, stable Run/version/Task identities and immutable
  Attempt ownership and sample prefixes across checkpoints. Every final Attempt
  has one invocation with contiguous START/PULSE/FINISH observations in a common
  clock domain, nondecreasing monotonic values and database receipts.
- A/B/D each succeed on Attempt #1. Normal C succeeds once; recovery C #1 is LOST
  with no FINISH or completion admission, and C #2 succeeds on B's existing Worker.
  B's complete successful record is unchanged from `join_wait` to final.
- B/C first invocations overlap for at least one second. Successful observations
  span at least the trusted Handler duration. A's FINISH/admission precede branch
  START/claims; both successful branches' FINISH/admission precede D START/claim.
  Claim precedes START receipt, FINISH receipt precedes completion admission, and
  observed database times are not later than the snapshot. `created_at` is unused.
- Parallel has one two-slot owner; distribution/recovery have two one-slot owners
  with different B/C first owners. In recovery, B completes before its Worker
  claims/starts C #2, and old C's last observation precedes the new invocation.
  Old Worker is LOST; old Lease expiry <= retry scheduling < availability <= new
  claim. The real retry checkpoint precedes eligibility; no extra retry is hidden.
- The fault receipt matches the exact C #1 target and pre-fault snapshot, records
  SIGKILL/Docker acknowledgement, and includes a valid immutable container ID.

Missing or inconsistent evidence fails the check. Peak overlap uses observed
intervals; it never extends an unfinished interval to Lease expiry. Unit fixtures
are synthetic test inputs, not demonstration evidence. Live browser acceptance of
the current diamond scenarios remains pending G6.

Browser acceptance must see real overlap, ownership, waiting dependencies and the
recovery loop. Polling can miss brief states. There is no complete network trace,
physical slot ID, database Docker-stop event or READY transition history; only confirmed
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
