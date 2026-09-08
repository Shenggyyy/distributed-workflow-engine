# Six-step flow acceptance — 2026-09-09

Phase E improves explanation of the existing engine, independently of the completed
M0–M5 correctness gate and phase D execution-evidence gate. The Chinese page follows
submission -> dependencies -> PostgreSQL READY view -> Worker pull -> rescheduling
loop -> final outcome/evidence. No unrelated V2/V3 feature was added.

## Real browser evidence

Screenshots are unmodified viewport captures on Windows with Docker Desktop Linux
containers. Local date is 2026-09-09 Australia/Sydney; database UTC dates are Sep 8.
They are real engine data, not fixtures or a replay. The Run selector/header/raw
JSON preserve full identities even where a scrolled screenshot omits the header.

| Scenario | Run ID | Observed result |
| --- | --- | --- |
| Parallel | `43a22941-cd17-467f-ac79-17cda0b8e871` | One Worker, two configured slots, two confirmed concurrent allocations; measured overlap 2, 78 samples, all five Tasks succeeded. |
| Distribution | `7b28ac8c-5b7a-4177-992f-35e6d7efb43b` | Two one-slot Worker containers claimed from the same Run; measured overlap 2, 78 samples, all five Tasks succeeded. |
| Recovery | `1441744a-8c79-4f59-9023-d87c9ea58596` | Live RETRY_WAIT, old Attempt LOST, different Worker executing Attempt 2, final success; 51 samples. |

Parallel captures: `flow-dependencies.png`, `flow-parallel.png`, `flow-overlap.png`.
Distribution: `flow-distribution.png`. The four recovery captures `flow-retry.png`,
`flow-recovery-workers.png`, `flow-replacement.png`, `flow-recovery-result.png` all
belong to the same recovery Run above. They appear in [the walkthrough](demo.md).

The recovery Run's old session last heartbeat was `22:48:13.928Z`, with heartbeat
deadline `22:48:19.928Z`. Last lease renewal was `22:48:14.032Z`, lease deadline
`22:48:20.032Z`. The recovery transaction recorded the retry at `22:48:20.376Z`,
eligible at `22:48:25.627Z` (5.251 seconds backoff). Another Worker claimed Attempt
2 at `22:48:26.171Z`; its successful completion was admitted at `22:48:47.385Z`.
The old execution has 1.12 seconds of samples, no FINISH and no accepted completion.
Its actual end is unknown; neither heartbeat nor lease deadline fills that gap.

The unchanged acceptance script also completed all three fresh scenarios during
page development (`d004a1de-ac65-4993-bbb9-be46e168121c`,
`8f62b14c-7188-4f29-b7d2-4903f31ecc70`,
`fda37ab0-31ef-40c9-986c-f09b731cb5f3`). The final screenshot Runs were independently
checked with the same validator. Raw final/retry snapshots remain in ignored
`.uv-cache/demo-acceptance/`; they are not committed.

## What a viewer can verify

- Definition and current state identify what was submitted. Downward edges show
  real dependencies; Join names every unfinished parent in its waiting reason.
- READY is a database view. Satisfied dependencies while PENDING still await a
  scheduling transaction; the browser does not invent a READY transition.
- Worker cards show committed allocations, actual sampling evidence, configured
  capacity, heartbeat and renewal/deadline. Task ownership is not predetermined.
- The rescheduling loop links back to claimable work/Workers. Persisted retry
  records distinguish old loss, eligibility and the next actual allocation.
- The outcome lists work by owner and keeps measured timeline/Attempt receipts.
  Expired heartbeats after normal exit do not turn successful Tasks into failures.

The browser was inspected during real parallel/distributed execution, dependency
waiting, RETRY_WAIT, replacement execution and final success. All six sections,
loop navigation and live polling worked with no JavaScript runtime errors.

## Validation and correctness review

- Full Python regression: **1551 passed** in 265.15 seconds; one existing
  Starlette/AnyIO deprecation warning. Uses a separate test database, not demo data.
- **8 Node.js tests passed**, covering actual overlap, missing FINISH, duplicate
  invocations, dependency vs scheduling wait, microsecond deadline boundaries,
  retry vs new allocation, normal heartbeat expiry and vertical DAG layering.
- Six focused PostgreSQL/API tests passed, including additive renewal evidence,
  scope/token exclusion, packaged page assets and coherent MVCC observations.
- Ruff lint/format, strict mypy for Linux/Windows and actionlint passed. Image
  startup succeeded; wheel/sdist contain five demo assets and exclude local
  instructions, private env files, credentials and caches (`.env.example` is public).
- No schema/migration, core write transaction, lock order, lease ownership,
  retry policy, handler or CLI behavior changed. Only the existing renewal field
  was added to the read-only snapshot. Unknown worker identities are not relabelled
  as Worker 1. No command-execution API or new service/framework was introduced.
- Commits are pushed independently; the final GitHub CI and remote HEAD are
  verified after the documentation commit, not assumed from local test results.

## Explicit limits

This is a single-machine container demonstration. Timed trusted Handlers include
waits; overlapping lifetimes are not CPU throughput or multi-machine evidence.
There is no sales report, task artifact transfer, autoscaling, checkpoint resume,
exactly-once execution, complete network trace or recorded Docker kill event.
Replacement startup and SIGKILL belong to the local script/terminal evidence.
Dependency reasons are current snapshot projections, not event history. Brief
transitions may be missed at 500 ms polling; persisted retry/allocation records
remain available. The page never extrapolates Handler execution beyond samples.
