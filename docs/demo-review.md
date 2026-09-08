# Demonstration acceptance — 2026-09-08

## Two independent acceptance gates

| Layer | Acceptance |
| --- | --- |
| Core M0–M5 | Durable workflow execution and failure contracts, documented in the historical [MVP review](mvp-review.md). |
| Demonstration D0–D5 | Open the real page, observe overlapping Handler execution, distinguish Worker ownership, inject a scoped crash, and see retry/replacement/final success. Real screenshots and commands are required; passing tests alone is insufficient. |

Both gates are complete. No unrelated V2/V3 functionality was added.

## Actual browser runs

Windows host, Docker Desktop Linux containers, PostgreSQL 18.6, Python 3.13 runtime.
The original screenshots linked below are unmodified viewport captures of live engine
data. They show the DAG/Worker/timeline region; the page header and JSON endpoint
provide the full Run ID. This is a single-machine container demonstration.

| Scenario | Screenshot Run ID | Verified result |
| --- | --- | --- |
| Parallel | `50d26df3-a906-4cfe-9140-1da802be71f5` | One Worker, two slots, measured peak overlap 2; all five Tasks succeeded; 78 samples. |
| Distribution | `09d055c3-8f34-4e7b-9ce5-40613e877ce0` | Two independent one-slot Worker containers; measured peak overlap 2; all five Tasks succeeded; 78 samples. |
| Recovery | `fcc17f9d-eecc-41ec-85f3-94b9bd4bd2a9` | Actual RETRY_WAIT screenshot; Attempt 1 LOST without FINISH, Attempt 2 on another Worker succeeded, then Join succeeded; 51 samples. |

Retained phase D captures: [parallel](images/demo-parallel.png),
[distribution](images/demo-distribution.png), [retry wait](images/demo-retry.png),
and [final recovery](images/demo-recovery.png). These preserve the original
acceptance evidence; the current page's layout and language may differ.

For the parallel Run, A's observed interval was
`[52816858644399, 52825237888920]` ns and B's was
`[52816866482502, 52825238601718]` ns: a positive intersection of about 8.371 seconds.
For distribution, A was `[52842170071715, 52850352587284]` ns and B was
`[52842895177626, 52851062627274]` ns: about 7.457 seconds of observed overlap.
Both comparisons use matching kernel boot IDs and frozen monotonic offsets, not
RUNNING state or creation timestamps. These are sample observations, not benchmarks.

Recovery was recorded at `12:57:10.559Z`; persisted retry eligibility was
`12:57:18.036Z` (7.477 seconds later). Replacement claim was `12:57:18.411Z`,
and completion admission was `12:57:39.432Z`. Old execution has only 1.10 seconds
of observed samples and an unknown end; it has no accepted completion. Timelines
do not stretch that old execution to lease expiry or to the replacement start.

## Verification and review

- Full Python regression: **1551 passed**, including PostgreSQL integration tests;
  one existing Starlette/AnyIO deprecation warning. Isolated test schemas/database
  were used, separate from demo and development data.
- Three Node.js tests verify positive overlap, separate domains, exact nanosecond
  handling, missing FINISH, and distinct duplicate invocations.
- Ruff lint/format, strict mypy on Linux and Windows targets, and actionlint pass.
- Runtime image build, packaged assets, additive migrations and real API/Scheduler/
  Worker execution pass. The repeatable container script completed all scenarios.
- Browser inspection observed live parallel/distributed execution and RETRY_WAIT,
  followed by durable replacement success. No JavaScript runtime errors were seen.
- Documented `down`/`up` was executed: page disconnection reported frozen/stale data,
  and all screenshot Runs and their evidence survived restart.
- Review preserved Run -> Worker -> Task -> Attempt -> lease locking. Evidence
  cannot complete/recover/renew a core Attempt, and the snapshot API excludes tokens.
- Every fault target was a dedicated labelled demo Worker; no existing data was
  deleted. Local instructions, passwords, private snapshots and caches stay out of Git.

The final GitHub CI result is checked after the documentation commit; CI verifies
quality, database and core container contracts. The browser demo acceptance is
performed locally by the documented script and real browser inspection.

## Boundaries and trade-offs

Trusted timed Handlers intentionally spend several seconds inside the callable,
including waits. Their overlap demonstrates concurrent execution lifetimes, not
CPU parallelism or throughput. Container clock offsets are explicitly checked;
different domains remain separate. No multi-machine or synchronized-UTC claim.

Observation writes add database traffic and their failure fails the demo Handler.
This is useful demonstration evidence, not a production telemetry architecture.
No garbage collection is provided. Commands retain Run/evidence history and stopped
containers. Worker heartbeat expiry after a normal Run exit is not a task failure.
The dashboard has no authentication or tenant model and stays on loopback.

At-least-once execution and business idempotency requirements are unchanged. Core
tests cover stale-result rejection; this visual crash scenario demonstrates actual
loss/retry/reassignment and does not claim to test every stale-message interleaving.
