# D: local demonstration and visualization

Core M0–M5 completion remains a separate acceptance result. Demonstration completion
requires seeing real parallel execution, ownership across independent Workers and
recovery in a browser, with reproducible commands and real screenshots.

## Read-only findings and design

The core stores lease acquisition/renewal, completion acceptance, retry eligibility
and Worker heartbeat timestamps. It does not record actual Handler execution.
Neither READY/RUNNING nor `created_at` proves execution overlap. The demo will add
instrumentation inside explicitly registered trusted Handler bodies; core Worker
control, state machines and ownership transactions remain unchanged.

Use one optional module in the existing package: an API factory adds demonstration
routes and packaged vanilla HTML/CSS/JS to the existing API process. No frontend
build system, broker, dashboard service or command-execution HTTP endpoint.
Dedicated Compose project, database, secret, ports and selected-Run Workers isolate
demonstration activity. A local CLI creates fixed scenarios, starts named containers
and validates their Compose project/service/Run labels before fault injection.

## Evidence and clocks

- `acquired_at`: authoritative database time when the Attempt was allocated.
- Handler START/PULSE/FINISH: samples taken inside the actual trusted callable.
  `monotonic_ns` comes from Linux CLOCK_MONOTONIC. A clock domain identifies the
  kernel boot ID and time namespace; compare overlaps only in the same domain.
  Standard Docker containers share this domain, which the acceptance checks verify.
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

## Commit-sized steps

1. D0: design, evidence semantics and acceptance plan (this document).
2. D1a: additive demo membership/evidence schema and migration tests.
3. D1b: instrumented trusted Handler and observation validation/tests.
4. D2: fixed-scenario creation and scoped coherent query API/tests.
5. D3: dedicated Compose environment, selected-Run processes and local fault CLI.
6. D4: lightweight DAG, Worker cards, Attempt table and sampled execution timeline.
7. D5: browser/container acceptance of all three scenarios, genuine screenshots,
   README and three-minute walkthrough, full regression and final review.

Each step is implemented, validated, reviewed, committed and pushed independently.
If a step grows, split it further before mixing independent changes.

## Scenarios

- Parallel: several multi-second roots with two execution slots, then a join.
- Distribution: one workflow, at least two independent Worker containers; each has
  one slot and bounded work duration, allowing clear recorded ownership and overlap.
- Recovery: start a selected-Run Worker, stop only that container with a verified
  local command after a START sample, then start the replacement. Short server
  heartbeat/lease windows and a visible retry backoff reveal LOST, RETRY_WAIT,
  replacement Attempt and final success. Old execution has no invented FINISH.

## Acceptance

Keep the page open during real runs. Confirm DAG state changes, full Run/Task/Attempt
and Worker identities, heartbeats with snapshot freshness, overlapping measured
execution bars, ownership by two independent sessions and the recovery sequence.
Check browser errors, persisted evidence and core invariants, capture screenshots
from those runs and document exact commands/addresses. Existing core tests must
continue to pass. V2/V3 scheduling, metrics platforms and multi-machine claims are
outside this phase.

D1a verified: revision 0011 adds four append-only demo tables. Upgrade preserves
existing Attempts; downgrade/re-upgrade and metadata comparison pass. 28 focused
migration tests and strict type checking pass. No core state transaction changed.

D1b verified: opt-in trusted timed handlers persist START/PULSE/FINISH from inside
the callable. Eight PostgreSQL tests cover scope, identity, monotonic ordering,
interruption and separation from Attempt completion. Sampling failure fails the
handler; it never invents evidence. Linux boot/time namespace is required. Only
the invocation evidence row is locked when appending; core ownership is untouched.

D2 verified: opt-in POST /demo/runs accepts only three fixed scenarios (each POST
creates a fresh Run); GET /demo/runs lists the latest 50 demo Runs; GET
/demo/runs/{id} returns the coherent read-only snapshot. Six HTTP/database tests
verify real DAG initialization, scope, no tokens, lossless nanoseconds and exclusion
of a concurrent observation committed after the snapshot began.

D3 split: D3a packages opt-in process roles and standalone Compose; D3b adds the
local scenario/fault CLI. The demo Scheduler uses normal global discovery inside
the dedicated demo database, including Worker expiry. Workers remain Run-scoped.

D3a verified: standalone image build, revision 0011 migration, API/Scheduler startup
and a real two-slot Worker run completed successfully with Handler samples from a
shared Linux clock domain. The demo PostgreSQL port is not published; API binds
127.0.0.1:18080. Stock process roles and development Compose are unchanged.
