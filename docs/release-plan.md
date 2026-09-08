# First-version preparation: repository and bilingual presentation

This phase changes documentation and the optional demonstration UI only. It does
not change engine semantics, publish a release/tag, or clean local secrets, private
configuration, databases or volumes. Real acceptance creates only dedicated demo
Runs through the existing scripts. Local instructions remain excluded from Git.

## Read-only inventory and decisions

| Decision | Scope | Reason / retained information |
| --- | --- | --- |
| Keep | `src/`, `tests/`, all Alembic revisions, CI, Compose, Dockerfile, lock/config files | Runtime and correctness/build contracts; not cleanup targets. |
| Keep | `examples/`, `scripts/` | Runnable usage and real process/HTTP/container validation. Link by reader purpose rather than repeat their implementations on the homepage. |
| Keep | Domain, storage, transaction and HTTP documents | Similar names describe different layers and failure guarantees. Do not merge mechanically or discard details. |
| Keep | Dated milestone/release reviews and real screenshots | Historical evidence and explicitly dated limitations, not current test-count advertisements. |
| Merge | `docs/demo-plan.md` and `docs/demo-flow-plan.md` -> `docs/demo-design.md` | One current design: isolation, instrumentation, clocks, MVCC, additive schema and six-step projections. D/E acceptance remains in the dated reviews. |
| Remove after merge | The two superseded plan files only | Their useful design information is retained in the merged design; chronological commit/test updates are already in Git and reviews. Inbound links are Markdown only and will be repaired. |
| Extract | README configuration/logging -> `docs/configuration.md`; quality/CI -> `docs/testing.md` | Keep unique operational details; replace the oversized homepage with concise paired entry points. |
| Remove duplicate prose | README's module-by-module walkthrough, exhaustive tree and development diary | Existing topic docs/examples preserve runnable details; compact navigation replaces duplicate excerpts. Correct outdated head 0009; do not promise unimplemented readiness. |
| Retain pending judgment | Older layered design/review documents | They retain distinctive constraints/evidence; no need to delete based on age or filename. |

Before deleting, check tracked-file references, anchors, examples/scripts, CI and
packaging. Keep all real evidence images. No unrelated cleanup or history rewrite.

## Reader-oriented documentation

English and Chinese homepages link clearly to one another. They share structure:
problem/status, architecture/responsibilities, distributed contracts, quick start,
three demonstrations/evidence, tests, limitations and detailed reading.
`docs/README.md` groups deeper English references into getting started, architecture,
API/usage, demonstration, testing/failure acceptance and limitations. The bilingual
demo guides include startup and a three-minute walkthrough. They identify English-
only deeper references honestly; no nonexistent translated links.

## UI language design

One local JavaScript catalog owns static and dynamic explanatory text. `en` and
`zh-CN` have the same keys and interpolation parameters; use DOM text, not HTML
translation injection. Translate title, six steps, controls, accessible labels,
empty/error states, DAG legend, waiting/retry/ownership and evidence descriptions.
Never translate raw keys, IDs, Task names, protocol states, JSON or log details.

First visit uses the primary browser language (Chinese -> zh-CN, otherwise English).
A valid manual choice wins on later visits. Storage reads/writes are guarded so
disabled storage does not break the page. Language switch uses the latest existing
snapshot, does not fetch/submit/restart work, and keeps Run/follow selection and
details open state. Preserve the visible section's offset where possible despite
different text heights. Update document lang/title and button pressed state.
All timestamp formatting, UTC markings, clocks, leases and evidence remain unchanged.

## Commit-sized work

1. R0: inventory and this bounded plan.
2. R1: consolidate design; extract operational references; add topic navigation and
   repair/check local links and anchors. Preserve runnable examples and contracts.
3. R2: concise English/Chinese README and matched bilingual startup/demo guides.
4. R3: centralized complete language catalog, preference/translation functions and
   focused Node tests; existing page still works before integration.
5. R4: wire all static/dynamic text and visible switch; cover state preservation,
   missing translations, unavailable storage and accurate evidence semantics.
6. R5: actual browser switching during all three real scenarios, English/Chinese
   screenshots, relevant regression, packaging/link checks and dated release review.

Each subtask is separately validated, reviewed, committed, pushed and verified.
Final acceptance includes browser operation and exact final CI/remote verification,
not just passing tests or a claim of production/multi-machine/exactly-once readiness.
