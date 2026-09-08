# E: explain the real execution flow

## Read-only findings

The existing scoped REPEATABLE READ snapshot contains the pinned DAG, current Task
states, confirmed Attempt allocation/ownership, latest heartbeat/lease deadlines,
immutable retry schedules, completion admission, and actual Handler samples.
The lease table also has `last_renewed_at`, not yet selected by the demo API.

There is no complete transport trace, READY transition history, Docker kill event,
physical slot identity, or exact Handler crash timestamp. Claim receipts exist in
the core database, but this page needs only confirmed allocated Attempts, not a
new request trace. Do not draw animated requests or infer missing historical events.

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

## Independent commits and acceptance

- E0: this inspected design and scope.
- E1: expose renewal evidence; pure dependency/ownership/recovery projections and
  focused API/JavaScript tests, including missing samples and normal Worker expiry.
- E2: six-step page, vertical DAG and visible rescheduling loop; validate real
  container scenarios and browser layout/behavior before committing.
- E3: three fresh browser scenarios, actual screenshots, README/startup/three-minute
  walkthrough and review; regression/CI and verified remote HEAD.

Each commit is separately reviewed, committed, pushed and verified. Final acceptance
requires a reader to explain readiness, ownership, measured overlap, loss detection,
retry and replacement from the running page. Tests alone do not satisfy this gate.

## Completion

E0–E3 are complete. The read-only extension, eight evidence calculation tests,
six-step page, real scenario/browser inspection, screenshots and refreshed guide
are recorded in [flow acceptance](demo-flow-review.md). Full regression remains
1551 passing Python tests. Existing core write paths and schema are unchanged.
