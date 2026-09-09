# Diamond execution and browser acceptance

Reviewed on **2026-09-09 (Australia/Sydney)** using implementation `554e291`.
G0–G5 introduced the fixed diamond demonstration in independent commits; G6 records
its actual execution and browser evidence. The [plan](diamond-plan.md) defines the
scope. This is a demonstration acceptance gate, separate from the completed
[M0–M5 core review](mvp-review.md), and not a production certification.

| Phase | Commit | Independent change |
| --- | --- | --- |
| G0 | `fa303db` | Bounded diamond design and implementation plan. |
| G1 | `5493231` | Fresh scoped Worker cohort startup. |
| G2 | `cdc6f04` | Trusted diamond definitions and distinct Handler keys. |
| G3 | `6bcec9c` | Actual-owner branch fault guard and command evidence. |
| G4 | `1a95cf3` | Strict execution/checkpoint validation. |
| G5 | `554e291` | Bilingual scenario, CLI and page activation. |

Each was pushed to `main` and its remote hash verified. G6 is this evidence and
documentation commit; its hash is available in this file's Git history and final
delivery, rather than a self-referential commit hash embedded here.

## Actual executions

The image was rebuilt with `uv run python scripts/demo.py up`. Each scenario below
was run once through the acceptance runner against the real local demo API and
passed the strict checkpoint validator. No assertion was relaxed or unsuccessful
Run replaced to obtain these results. This was one Windows machine using Docker
Desktop Linux containers and the dedicated demonstration database.

| Scenario | Run ID | Actual owners / slots | B/C first-Attempt overlap | Samples |
| --- | --- | --- | --- | --- |
| Parallel | `ddfa6764-20ca-4d72-95a0-ddc4d8241e8c` | One Worker, two slots | 8,035,012,317 ns | 70 |
| Distribution | `3437e68c-4b3f-481a-89f4-9b9ed8b68709` | Two Workers, one slot each | 8,045,502,603 ns | 70 |
| Recovery | `832ffa23-9623-4100-81af-3716a537e6d5` | Two Workers, one slot each | 1,613,997,989 ns before loss | 86 |

Peak measured overlap was **2 in all three Runs**. The common observation domain
was `linux:fd260483-2d24-40f4-b350-1aec0060300d:monotonic-offset:0:0`. These intervals
measure overlapping trusted Handler lifetimes, including timed waits, rather than
CPU utilization, throughput or multi-machine clock synchronization.

Every Run retained actual `root`, `branches` and `join_wait` snapshots. They establish
A execution before branch allocation, both first branches executing, then B success
while C remained unfinished and D had no Attempt. Recovery additionally retained
`retry`, `fault_before` and the acknowledged command receipt. Final status alone
was insufficient: saved definitions, identity continuity, immutable sample prefixes,
admission/claim ordering and full successful Handler durations were also checked.
The final JSON and named checkpoint files remain in ignored
`.uv-cache/demo-acceptance/`; new invocations produce their own evidence rather than
replaying these Runs. [Validation contracts](demo-design.md#acceptance-evidence).

## Recovery without a replacement Worker

In the recovery Run, the actual C #1 owner was
`dwe-demo-832ffa239623410081af3716a537e6d5-b`, session
`bcb7b2c0-7974-46c7-a7f9-e3dc461f1c08`. The guarded command resolved that ownership
from the snapshot and stopped immutable container
`8d8ba953c26b4df61d6e42a310f98e94a34ca2ad6ed2f004b03a48f809b09070` with one SIGKILL.
Its exclusive before-snapshot and Docker acknowledgement identify C #1 Attempt
`42ed45ab-84cf-4c37-8326-fef63a8a4ba6` and invocation
`a8d5b6bc-d484-4f44-a2f6-f3317a76f591`. The receipt is local command evidence;
its `snapshot_at` is the pre-fault database observation, not the time of SIGKILL.

B's original Worker, `dwe-demo-832ffa239623410081af3716a537e6d5-a`, session
`c69b321d-0136-4e8d-a610-f902f9d3eabf`, completed B once, later pulled C #2 and then
executed D. No third Worker or replacement container was started. B Attempt
`99d8bf18-7569-42db-ae4f-577f20636993` remained SUCCEEDED with its complete record
unchanged; C #2 was `bd042ad6-8bcd-412f-b9b2-dd9859c7c3a6` on the survivor.

Read-only Docker inspection confirmed exactly the five Workers from these three
Runs, with matching names, immutable IDs and project/service/demo/Run labels.
The four normally completed containers exited with code 0. The targeted C #1
container exited with code 137 and `OOMKilled=false`; its ID matched the fault
acknowledgement. No replacement container existed for this recovery Run.

The following are database timestamps on **2026-09-09 UTC**, not Handler monotonic
readings or exact transaction COMMIT times:

| Evidence | UTC time | Interpretation |
| --- | --- | --- |
| Final pre-fault snapshot | 01:34:26.661381 | A succeeded; B/C #1 RUNNING, real overlap observed, D unclaimed. |
| C #1 Lease deadline | 01:34:32.330571 | Ownership deadline; it is not an observed Handler end. |
| Retry scheduled | 01:34:32.573428 | Engine recorded C #1 LOST and durable backoff. |
| RETRY_WAIT snapshot | 01:34:32.582436 | C waits; B still RUNNING, D PENDING with no Attempt. |
| B completion admitted | 01:34:32.886735 | B's first successful Attempt is retained. |
| Join-wait snapshot | 01:34:33.042438 | B SUCCEEDED, C RETRY_WAIT, D still unclaimed. |
| Retry eligible | 01:34:41.993428 | Persisted availability, after 9.420 seconds of backoff. |
| C #2 claimed | 01:34:42.384063 | B's existing Worker acquired a new Attempt. |
| C #2 START receipt | 01:34:43.111953 | Database received the new Handler's actual START sample. |
| C #2 FINISH receipt | 01:35:03.290751 | Successful new invocation's last observation received. |
| C #2 completion admitted | 01:35:03.397145 | The new Attempt's successful report was admitted. |
| D claimed | 01:35:04.051207 | Both B and C had accepted successful results. |
| D completion admitted | 01:35:07.972592 | Final task succeeded; the Run subsequently aggregated to SUCCEEDED. |

C #1's last observation was a PULSE received at 01:34:26.539039 UTC. It has no
FINISH or accepted completion. Its displayed interval therefore stops at that
sample; neither its actual end nor a handoff is invented. The new C invocation
starts from the beginning. Worker-session loss and Attempt loss were observed
separately; heartbeat expiry alone does not prove the local fault action.

## Browser inspection and historical continuity

The real page was inspected at 1280 × 900 and 390 × 844. Both languages retained
the six-step layout, saved DAG, Worker cards, timeline, all five recovery Attempt
rows and the raw-data link. There was no page-level horizontal overflow; wide
mobile DAG/table content deliberately scrolls within its own container. The
browser error log was empty.

Language switches were observed during live distribution and recovery execution
and kept the Run identity. Separate post-completion checks retained collapsed
Attempt details. Near the page bottom, scroll offset shifted from approximately
2734.7 to 2680.7 pixels with text reflow; there was no jump to the top, but exact
pixel preservation is not claimed.
Parallel was inspected in the browser after completion; its intermediate execution
states are evidenced by retained real checkpoints, not a claimed live browser tour.

The following historical snapshots were compared before and after all three new
Runs and were equal except for the fresh `snapshot_at`:

- Earlier parallel: `d227cf80-43ba-4279-8d48-2734ac008f89`.
- Earlier distribution: `846b2f66-ec25-412e-9a83-7ea3ee8d4706`.
- Earlier recovery: `e69d9250-3c05-4f04-8984-f48c3a83ad55`.

The old recovery still rendered A -> Join in English and Chinese, with its explicit
historical script-started replacement note. Reload retained Chinese and the same
old Run. Existing Handler meanings, persisted definitions and evidence were not
rewritten. All earlier [demo](demo-review.md), [flow](demo-flow-review.md) and
[release](release-review.md) screenshots remain historical evidence.

## Browser captures

All 15 files are unmodified real browser captures from a requested 1280 × 900
viewport. The browser capture tool returned JPEG rasters of 1265 × 889 pixels;
the `.jpg` extension matches their actual bytes, with no image conversion or
resizing. A language pair may show different instants because execution continued
during switching. Filenames alone do not identify the state; use the captions below.

| Capture | Run / state actually visible |
| --- | --- |
| [Root, Chinese](images/diamond-root-zh-CN.jpg) | Distribution: A RUNNING, B/C and D waiting for dependencies. |
| [Branches, English](images/diamond-branches-en.jpg) | Distribution: B/C RUNNING on different actual owners, D PENDING. |
| [DAG, English](images/diamond-dag-en.jpg) | Parallel: completed A -> B/C -> D. |
| [Parallel, English](images/diamond-parallel-en.jpg) · [Chinese](images/diamond-parallel-zh-CN.jpg) | Final single-Worker timeline: B/C intervals overlap, D follows both. |
| [Distribution Workers, English](images/diamond-distribution-en.jpg) | D has just been claimed; this is not a live B/C execution screenshot. |
| [Distribution timeline, English](images/diamond-distribution-timeline-en.jpg) · [Chinese](images/diamond-distribution-timeline-zh-CN.jpg) | Final timeline and actual work across two owners. |
| [Recovery Workers, Chinese](images/diamond-recovery-workers-zh-CN.jpg) | B/C first Attempts executing before the fault on separate Workers. |
| [Recovery Workers, English](images/diamond-recovery-workers-en.jpg) | Old Worker LOST, B already successful, C RETRY_WAIT, D blocked. |
| [Retry loop, English](images/diamond-retry-en.jpg) · [Chinese](images/diamond-retry-zh-CN.jpg) | C #2 already RUNNING on B's surviving Worker; B retained, D waiting. These are after RETRY_WAIT. |
| [Recovery DAG, Chinese](images/diamond-recovery-dag-zh-CN.jpg) | B SUCCEEDED, C #2 RUNNING, D PENDING. |
| [Final recovery, English](images/diamond-recovery-en.jpg) · [Chinese](images/diamond-recovery-zh-CN.jpg) | Final success: old C has no FINISH; C #2 completes on the survivor before D. |

## Validation and scope review

The read-only phase audit found no changes to core Schema/migrations, repositories,
WorkerLoop, lock ordering, transaction boundaries, Lease, retry, idempotency or
stale-result rejection. Startup readiness reads scoped sessions and maintains its
own heartbeat; it does not assign tasks. Fault injection checks the exact saved
diamond, actual C #1 owner, labels and immutable container ID, with conservative
freshness checks and exclusive evidence files. Database reads and Docker remain
non-atomic; an uncertain command is not retried automatically.

`uv build --offline` succeeded at `554e291`. Both wheel and source distribution
contained all seven static assets and five changed/new demo modules byte-for-byte;
the source distribution also contained all five CLI/helper files. Archive members
contained no local instructions, private environment files, secrets, caches or
database/build artifacts. The wheel intentionally packages `workflow_engine`, not
the repository's standalone CLI scripts.

The exact implementation `554e291` passed
[CI run 34299366570](https://github.com/Shenggyyy/distributed-workflow-engine/actions/runs/34299366570).
The final local regression used an isolated temporary PostgreSQL database:
`uv run --locked pytest tests --database-env-file .uv-cache/diamond-test.env`
completed with **1,678 passed, zero skipped and zero failed** in 242.90 seconds.
The one warning was the existing Starlette/AnyIO alias deprecation. Ruff check
passed; formatting checked 262 files; mypy checked 201 sources; all 25 Node tests
and actionlint passed. These are dated validation results, not maintained test
counts on the homepage.

The final documentation commit's CI and remote hash must be verified after that
commit is pushed; the linked CI run above is for G5.

## Limits

The results establish these three actual Runs, not a performance or recovery SLA.
Polling can miss brief transitions, so the saved checkpoint records complement
browser evidence. The page has no complete request/event history or Docker stop
event; it never invents a claim request, physical slot ID or missing FINISH.
At-least-once execution still needs business idempotency for external effects.
Multi-machine deployment, exactly-once effects, automatic scaling, checkpoint resume,
artifact transfer and production availability were not established.
