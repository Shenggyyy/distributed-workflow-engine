# First-version preparation and bilingual acceptance

Historical evidence: these Runs use parallel roots feeding Join and root-A
recovery with a script-started replacement. They are not acceptance of the current
diamond scenarios. Existing Runs and screenshots remain unchanged; see the
[current guide](demo.md) and [diamond phase](diamond-plan.md).

Reviewed on **2026-09-09 (Australia/Sydney)**. This is the R0–R5 documentation and
presentation phase, not a new engine feature release or a production certification.
The implementation under test is `a6768b4`; this final documentation change records
its evidence. The [plan](release-plan.md) defines the audit and commit boundaries.

## Repository decisions

| Decision | Result and retained information |
| --- | --- |
| Removed after merging | Only `docs/demo-plan.md` and `docs/demo-flow-plan.md`. Their durable design, clocks, isolation, observation schema, consistency and six-step projections are in [demo-design.md](demo-design.md). Their superseded implementation checklists are preserved by Git history. |
| Extracted from the old homepage | Unique configuration/logging details are in [configuration.md](configuration.md); test/CI/build instructions are in [testing.md](testing.md). The homepage's repeated module walkthrough and development diary were replaced with reader-oriented links. |
| Kept | Core code, necessary tests, every migration, CI, Docker/Compose, lockfiles, configuration, runnable examples/scripts, all 12 earlier real screenshots and dated milestone/demo reviews. Similar domain/storage/transaction documents retain different contracts and were not mechanically merged. |
| Retained pending judgment | Older layered design and review documents with distinct technical or historical value. Age alone did not establish that they were obsolete. |

Before removing the two plans, tracked code, scripts, examples, CI, packaging and
Markdown references were checked. Only documentation links referenced them; those
links now point to the merged design. The local link/anchor checker runs in CI.
No local private configuration, password, database or volume was cleaned or reset.
Existing demo history remains; acceptance added only dedicated demo Runs.

## Final documentation navigation

- Project homepages: [English](../README.md) | [简体中文](../README.zh-CN.md).
- Startup and three-minute guide: [English](demo.md) | [简体中文](demo.zh-CN.md).
- [Documentation index](README.md): quick start/local development; architecture and
  core contracts; API/usage; demonstration; testing/failures/acceptance; known limits.
- Detailed technical references remain English and are labelled accordingly in
  Chinese entry points. No nonexistent translated documents are linked.
- Earlier evidence remains in [core acceptance](mvp-review.md),
  [original demo acceptance](demo-review.md) and [six-step acceptance](demo-flow-review.md).

Paired homepages and guides have matching section structures, runnable command
blocks and walkthrough steps. The documented `uv sync --locked` and
`uv run python scripts/demo.py up` completed against the existing dedicated demo
environment; its schema remained at 0011. The page was opened at
**http://127.0.0.1:18080/demo/**. No database reset was needed.

## Real scenario evidence

`uv run python -m scripts.demo_acceptance` created and verified these fresh Runs:

| Scenario | Run ID | Measured peak overlap | Attempt owners | Samples |
| --- | --- | --- | --- | --- |
| Parallel | `d227cf80-43ba-4279-8d48-2734ac008f89` | 2 | 1 | 78 |
| Distribution | `47727d6d-6349-4bc8-9ad7-9dbd9ecb7cc7` | 2 | 2 | 78 |
| Recovery | `e69d9250-3c05-4f04-8984-f48c3a83ad55` | 1 | 2 | 52 |

All three reached SUCCEEDED. Recovery actually passed through RETRY_WAIT, captured
as a separate checkpoint; it was not inferred from the final result. The scoped
fault command stopped the dedicated Worker and started its replacement. Old A #1
became LOST, with 1.61 seconds of observed samples and no FINISH or accepted
completion. Its historical Lease ended at `00:17:39.536Z`; the retry plan was
recorded at `00:17:39.564Z` and became eligible at `00:17:48.816Z`. A #2 was acquired
by a different Worker at `00:17:49.662Z`, completed a new 20.33-second sampled
interval, and its report was admitted at `00:18:10.728Z`. Join then succeeded.
These are DB UTC admission/eligibility times, not exact commit or Handler end times.

An additional `run distribution` produced
`846b2f66-ec25-412e-9a83-7ea3ee8d4706` for bilingual live captures. It also reached
SUCCEEDED and passed the same evidence validator: peak overlap 2, two owners and
78 samples. Both Workers were visibly executing C/D with real PULSE observations
while the language changed. No task-to-Worker mapping was preassigned.

The common sample clock domain was
`linux:fd260483-2d24-40f4-b350-1aec0060300d:monotonic-offset:0:0`.
Raw snapshots, including the recovery wait checkpoint, are retained locally under
the ignored `.uv-cache/demo-acceptance/`; the page exposes raw evidence for these
Runs in the existing demo database. Fresh clones must generate their own Runs.

## Browser captures

All images below are unmodified real browser viewport captures. Paired live views
were taken sequentially, so state and sample times can advance between languages.
The page reads the engine; none of these images is used to simulate execution.

| Evidence | English | 简体中文 |
| --- | --- | --- |
| Parallel DAG | [C/D RUNNING, Join waiting](images/release-dag-en.png) | [Roots completed, Join now RUNNING](images/release-dag-zh-CN.png) |
| Parallel result | [One owner; two overlapping Handler intervals](images/release-parallel-en.png) | [相同 Run 的重叠时间线](images/release-parallel-zh-CN.png) |
| Distributed execution | [Two owners executing C/D](images/release-distribution-en.png) | [相同 Run 的两个 Worker 正在执行](images/release-distribution-zh-CN.png) |
| Recovery waiting | [A RETRY_WAIT, old owner LOST, replacement unallocated](images/release-recovery-wait-en.png) | See the persisted checkpoint described above; no matching Chinese wait capture is claimed. |
| Recovery reallocation | See the final outcome below. | [新 Attempt #2 已经 RUNNING](images/release-replacement-zh-CN.png) |
| Replacement progressing | See the final outcome below. | [A #2 已成功，替代 Worker 正执行 Join](images/release-workers-zh-CN.png) |
| Recovery result | [Old interval unfinished; new Attempt and Join succeeded](images/release-recovery-en.png) | [相同 Run 的恢复结果与真实时间线](images/release-recovery-zh-CN.png) |

## Language and layout acceptance

- First visit selected Chinese from the browser's primary language. Manual English
  selection survived reload. Primary-language fallback and blocked local-storage
  access are covered by unit tests; browser storage was not disabled during this run.
- Actual language switches were inspected during RUNNING parallel, distribution
  and recovery work. Selected Run, follow choice and active execution continued.
  Switching did not create or restart a Run. Controller tests separately verify
  that translation issues no fetch and does not change the polling timer.
- Completed-history selection, open Attempt details, raw identities, UTC labels and
  measured intervals were preserved. Native pointer switching retained the reading
  section's offset while translated text reflowed. Raw API data is not translated.
- Desktop 1280×900 and narrow 390×844 viewport overrides were inspected in both
  languages, then reset. All six sections, DAG, Worker cards, timeline and Attempt
  table remained usable without page-level horizontal overflow. The wide DAG and
  Attempt table scroll inside their own containers on narrow screens.
- Titles, flow labels, controls, accessibility text, waits, recovery explanations,
  empty states and error messages use the local catalog. Tests check catalog parity
  and dynamic explanations. No Chinese explanatory prose remained in the English
  scenario views; the native 中文 switch and unmodified API identifiers are intentional.
- No JavaScript runtime errors were observed. Empty/error cases were tested through
  the controller; no outage of the live demo service was introduced for that check.

## Regression and scope review

- Local default Python suite: **680 passed, 871 skipped** in 42.22 seconds. The
  skipped tests require an explicit test database; they were not claimed as local
  passes. One existing Starlette/AnyIO deprecation warning remains.
- **21 Node tests passed**: overlap/evidence, language preference and guarded
  storage, complete catalogs, dynamic translations, preserved page state and errors.
- Ruff lint/format, strict mypy, actionlint and documentation links passed. Wheel
  and sdist builds passed; all seven served demo assets matched the checkout and
  archives excluded local instructions, private env/secrets and caches.
- [Implementation CI 34294224862](https://github.com/Shenggyyy/distributed-workflow-engine/actions/runs/34294224862)
  passed on exact SHA `a6768b4c33f1ec62a21fe6654f66664c0b1482cd`: Windows/Linux
  each passed 680 default Python and 21 Node tests; the container job passed the
  other 871 PostgreSQL tests. HTTP, migrations, multi-Worker/Scheduler, durable
  retries, crash recovery, settlement and persistence scenarios passed as well.
- Core Python engine, migration/schema, Docker configuration and dependency
  lockfiles were unchanged throughout this phase. No transaction/lock order, Lease,
  retry/timeout, ownership, idempotency or stale-result rule changed. UI catalogs and
  rendering are local assets; no framework, translator service or new backend API.
- Final documentation commit CI and remote HEAD are checked after push. The
  [CI workflow history](https://github.com/Shenggyyy/distributed-workflow-engine/actions/workflows/ci.yml)
  records that separate result; the implementation result above is not substituted
  for verification of the final commit.

## Acceptance boundaries and remaining limits

Core acceptance concerns execution correctness and recovery contracts. Demo
acceptance additionally requires visible real overlap, multiple owners and actual
failure recovery. This phase adds paired startup documentation, complete UI
translation, preserved interaction state and real bilingual browser evidence.
Passing tests alone does not establish the latter two gates.

The environment is **one Windows host with independent Linux containers**. Timed
trusted Handlers include waits; overlap is not a CPU benchmark or multi-machine
validation. At-least-once execution still needs business idempotency. No production
SLA, exactly-once effects, autoscaling, task-output transfer or checkpoint resume is
claimed. Polling can miss brief states; Lease expiry is not a Handler finish, and
ordinary Worker heartbeat expiry after success does not imply task failure. Deep
technical references and raw logs/API JSON remain English or their original data.
