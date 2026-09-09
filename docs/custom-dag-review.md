# Custom DAG execution and browser acceptance

Verified on **2026-09-09**, after runtime/editor gates H1–H5b and capture gate
`55e266c` (H6b). This is a dated acceptance record, not a performance guarantee.
The [design](custom-dag-plan.md) records the additive schema, submission transaction,
demo-only limits and independent commits. Use the current guides for commands and
copyable JSON: [English](demo.md#custom-dag-editor) |
[简体中文](demo.zh-CN.md#自定义-dag-编辑器).

## Browser-created custom Runs

Both definitions were typed into the actual local page, validated by the backend,
previewed, and explicitly submitted. Neither used a preassigned task owner or a
fixed A/B/C/D renderer. The browser then showed READY roots, blocked descendants,
no Worker/Attempt history, and the exact local startup command. The separate
`custom_demo_acceptance --start-workers` command saved that waiting snapshot before
calling the normal two-Worker startup. The page observed live execution and completion.

| Definition | Actual Run ID | Result | Registered / executing Workers | Sample peak / count |
| --- | --- | --- | --- | --- |
| `CustomBranches`: 6 tasks, 6 edges, two roots and two joins | `9a8ab95c-6d7b-40c8-b231-05126fa0e70f` | SUCCEEDED, six first Attempts | 2 / 2 | 2 / 144 |
| `CustomSerial`: `OnlyOne → ThenNext → Finally` | `4ad787d9-b6e2-4613-9a5d-cbcc1ade799f` | SUCCEEDED, three first Attempts | 2 / 1 | 1 / 44 |

The branch Run's version is `76d5738f-cce7-4311-accb-4bc4d90057f6`; the serial
version is `96657cbd-bd59-4431-a97f-e9954c56c020`. All four independent one-slot
Worker containers passed exact name/label/immutable-ID checks and exited normally
with code 0. Their full identities remain in the locally retained evidence.

For `CustomBranches`, Worker 1 is session
`0bd87a94-1f2f-42d2-9a88-56ee38ef0c69` (container suffix `a`), and Worker 2 is
`3b16eb4f-bf5e-4e81-9d2c-fe514fd1f84b` (suffix `b`). The assignment was observed:

| Task | Owner | Claim (`acquired_at`, DB UTC) | Sampled Handler lifetime | Completion admission (`accepted_at`, DB UTC) |
| --- | --- | --- | --- | --- |
| Seed | Worker 2 | 03:36:37.499645Z | 8.258s | 03:36:46.592478Z |
| SideLane | Worker 1 | 03:36:37.832932Z | 20.366s | 03:36:59.048868Z |
| QuickLane | Worker 2 | 03:36:47.245810Z | 14.225s | 03:37:02.320302Z |
| SlowLane | Worker 1 | 03:36:59.153437Z | 20.267s | 03:37:20.258672Z |
| Merge | Worker 1 | 03:37:20.357317Z | 2.126s | 03:37:23.220457Z |
| End | Worker 2 | 03:37:23.407141Z | 2.117s | 03:37:26.270540Z |

All times in the table are on 2026-09-09. `Seed` completed before either child was
claimed. `SlowLane` remained READY while both slots were occupied; eligibility did
not imply immediate execution. The browser later showed `Merge` waiting only for
`SlowLane`, and `End` waiting for `Merge`. Every parent completion preceded its
child's claim; the independent sample-order check also passed for every edge.

Actual START-to-FINISH intervals had **10,949,510,487 ns** of maximum overlap between
distinct owners. The shared domain was
`linux:fd260483-2d24-40f4-b350-1aec0060300d:monotonic-offset:0:0`.
Each one-slot Worker's own intervals and claim/completion sequence were serial.
Database receipt times, claim times and completion admission were checked separately;
none were used as substitutes for Handler execution time or exact COMMIT time.

For `CustomSerial`, session `49072d43-335f-497d-bf00-eb36a7a010db` happened to claim
all three tasks. The other registered session stayed idle. Sampled lifetimes were
8.228s, 8.177s and 3.136s in dependency order, with **zero overlapping intervals**.
The page correctly reported one executing owner and peak one despite two available
Workers. This is valid pull behavior; there is no promise of fair alternation.

## Browser checks and captures

The live Chinese-to-English switch during `CustomBranches` retained the selected
Run, receipt and actual state; the final result was inspected in both languages.
Reload restored the confirmed receipt without creating another Run. Preview
invalidation, template-copy loading and backend self-dependency rejection were
checked in H5b; this pass additionally checked real unsupported-Handler and malformed
JSON errors in the editor. Creation stayed disabled, the preview stayed absent,
and these validations did not increase the Run count. Browser error logs were empty.

At desktop width, the six-step layout, DAG, Worker cards, timeline and Attempt table
were inspected. At 480px viewport width, the page itself did not overflow; the DAG
and wide evidence table retained their local scrolling, with all labels present.
Raw names, IDs, statuses, clock labels and JSON remained unchanged by translation.

These are actual browser captures of the Run above, not generated mockups:

| Evidence | English | 简体中文 |
| --- | --- | --- |
| Backend-validated preview, before creation | [Preview](images/custom-dag-preview-en.jpg) | [预览](images/custom-dag-preview-zh.jpg) |
| Completed branch Run and actual sample intervals | [Result](images/custom-dag-result-en.jpg) | [结果](images/custom-dag-result-zh.jpg) |
| No execution resources after creation | — | [Waiting and exact command](images/custom-dag-waiting-zh.jpg) |
| Two Workers during execution | [Live ownership](images/custom-dag-live-en.jpg) | — |
| Separate serial Run, no execution overlap | — | [Serial result](images/custom-serial-result-zh.jpg) |

## Predefined scenarios and history

The unchanged strict diamond acceptance script ran all three scenarios **once**;
all succeeded without weakening assertions or repeating an uncertain fault:

| Scenario | Run ID | Actual B/C overlap | Owners / samples |
| --- | --- | --- | --- |
| Parallel | `7c86fea7-49fc-4468-a7cd-50a300cbe9c0` | 7,753,532,423 ns | 1 / 70 |
| Distribution | `96ba80be-a7df-41db-bb78-7d6c6e0a09f2` | 7,826,462,706 ns | 2 / 70 |
| Recovery | `ba061072-b95a-4d86-81d8-b27aec5ebd64` | 1,685,420,140 ns before fault | 2 / 86 |

Browser inspection retained each scenario's meaning. Recovery stopped C's actual
owner during branch execution. C Attempt #1 became LOST, the persisted RETRY_WAIT
checkpoint was captured, B remained successful on Attempt #1, and the surviving
Worker claimed C #2. The browser showed D PENDING during that retry, then final
success. No replacement Worker was started and the custom entry gained no fault API.

All **31 pre-custom historical snapshots** retained identical Run/definition, Task,
Attempt, Worker and sample data (query time excluded). The earlier H3 custom smoke
Run also remained unchanged. The browser opened historical Run
`e69d9250-3c05-4f04-8984-f48c3a83ad55` and rendered its actual `A → Join` definition,
without forcing a diamond or renaming Join. There were 37 demo Runs after this pass;
no existing Run, version, database or volume was deleted or rewritten.

## Validation and retained evidence

- Full Python/PostgreSQL suite: **1904 passed, zero skipped**, 284.54s. One existing
  Starlette/AnyIO deprecation warning remains. The database was a separate disposable
  test service; it was not the demo database.
- All five Node test files: **60 passed**. Tests cover preview invalidation, frozen
  submission identity, repeat clicks, explicit uncertain-result resolution, storage
  failure, language updates and generic graph rendering. PostgreSQL/HTTP tests cover
  concurrent same-key submissions, conflict/rollback, commit-before-response, a lost
  successful HTTP response and subsequent replay, membership and Worker limits.
- Ruff, formatting, full mypy and actionlint passed. Wheel/sdist build and inspection
  included all ten demo static files and twelve migrations, without AGENTS.md,
  private environment files, caches, databases or secrets. Final Markdown links and
  matching bilingual JSON/commands were checked separately.

For each new custom Run, `.uv-cache/custom-acceptance/<run_id>/` retains exclusive
`waiting.json`, observed `checkpoint-*.json`, `final.json`, minimal verified
`containers.json` and `report.json`. The three template Runs retain their existing
format under `.uv-cache/demo-acceptance/`, including the scoped fault receipt and
retry checkpoint. These raw local artifacts are ignored by Git; the screenshots,
Run IDs and measured summaries above are the committed evidence. Reproduce with
a new browser-created Run; the command refuses existing history/output rather
than overwriting or automatically resuming it.

## Boundaries

This verifies a trusted local deployment on **one machine with multiple Linux
containers**. The handlers perform bounded timed waits, including sampling overhead;
the measurements do not establish CPU parallelism or production throughput. No
multi-machine clock comparison, exactly-once external effect, automatic scale-out,
checkpoint resume, output transfer or arbitrary-code execution is claimed.

Custom limits apply to this demo entry and each Run, not a global admission quota
or tenant security system. The UI has no Docker socket/command endpoint. Partial
Worker startup is refused and retained for diagnosis. Custom acceptance here is
fault-free; the dedicated predefined recovery scenario remains the failure baseline.
Storage-unavailable submission recovery is limited to the current page unless the
user copies the shown operation. Historical heartbeats may expire after normal
Worker exit; this does not turn a successful Task into a failure.
