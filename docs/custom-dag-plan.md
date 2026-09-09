# Custom DAG submission and observation

This independent phase starts after diamond acceptance at `0685ce5`. The three
predefined scenarios remain the repeatable baseline; custom input adds a second
entry to the same engine. No core execution semantics or historical definitions
are changed. Implementation proceeds through the small gates below.

H0–H1 are complete: the opt-in demo app serves its trusted catalog and bounded,
read-only validation/preview endpoint. Custom persistence, Worker startup and the
browser editor remain the later gates below; the predefined demonstrations work
unchanged during this preparation.

## Read-only findings

`WorkflowDefinition` already validates identifiers, duplicate nodes/dependencies,
missing parents and cycles. `WorkflowRepository.publish` always appends a version;
it explicitly does not support idempotency. `RunRepository.create_idempotent`
binds a key to an already concrete version, so blindly repeating publication before
calling it is incorrect. The demo currently creates a version, Run and membership
in one transaction, but that endpoint is intentionally non-idempotent.

Snapshots join `demo_runs` and saved versions, and never expose ownership tokens.
Its scenario CHECK allows only parallel/distribution/recovery. Worker membership,
observations, core scheduling and pull execution can be reused. The CLI needs an
operation that starts Workers for an existing custom Run, without creating a Run.
The page already derives dependency reasons and evidence from snapshots, but its
fixed-width DAG nodes need improvement for arbitrary valid names and topologies.

## Definition, limits and validation

Accept the existing Workflow JSON format and exact ASCII identifier rules. A
demo-only normalizer enforces 1–12 tasks and a 16 KiB request-body byte limit. The
JSON reader also rejects non-finite numbers, duplicate fields and nesting beyond
16 container levels (valid Workflow definitions are much shallower). The
whitelist is the eight currently registered timed demo keys: `demo.observe` (8s),
`demo.recover` (20s), `demo.join` (2s), `demo.diamond.a` (6s), `.b` (8s), `.c` (14s),
`.recover` (20s), and `.d` (3s), with the `demo.diamond` prefix on the abbreviated
keys. Names do not turn timed waits into business processing.

Normalize schema 1 or 2 to explicit schema 2 with fixed execution policy:
`max_attempts=2`, `timeout_seconds=90`, and both backoff bounds 10000 ms (the
engine's existing equal jitter gives 5–10 seconds). Fill omitted policies; reject
explicitly different policies instead of silently rewriting them. Preview and
creation use exactly the same normalizer. Object-key order/omitted defaults do
not change request identity; task/dependency array order remains significant.

`POST /demo/custom/validate` validates without writing or starting anything and
returns the canonical definition and its actual DAG layers. The server is
authoritative. Reject oversized streamed bodies even without Content-Length;
errors expose stable codes/field locations, not submitted JSON. There are no
paths, URLs, imports, arbitrary code, dependency installation or container options.
Core task limits and execution policy flexibility remain unchanged.

## Atomic submission and migration

`POST /demo/custom/runs` requires an `Idempotency-Key`. Add migration 0012 to allow
the explicit `custom` membership value and create an append-only
`demo_custom_submissions` receipt table: exact key, canonical JSONB definition,
unique Run FK and database creation timestamp. Keep 0011 unchanged. Downgrade
must refuse when custom history exists; it must not relabel or delete that history.

Within one fresh READ COMMITTED transaction, take a namespaced transaction advisory
lock derived from the submission key before any Workflow/core row locks. Use a
new two-int namespace, separate from existing Worker/claim gates and migration
bigint locks. Hash collisions only serialize unrelated keys: compare the complete
key and canonical definition in the durable receipt. Read that receipt in a new
statement before publication. Same key/body replays the original Run/version;
different body returns 409. Otherwise call the existing publication and Run
creation repositories, insert `custom` membership and the receipt, then return
success only after commit. Failure rolls everything back, with no placeholder or
orphan version. Do not retry database writes automatically. Core public API
idempotency behavior remains unchanged.

## Workers and observation

Add `demo.py workers --run-id UUID`: verify a nonterminal, explicitly custom Run,
the saved constrained definition and unused scoped Worker identities; start two
independent named containers with one slot each and the existing readiness gate.
No fixed task-to-Worker mapping. Repeat/partial starts fail explicitly instead of
silently adding Workers. Runtime guards and tests retain bounded custom startup.
The HTTP app gets no Docker socket or command-execution endpoint. Existing fault
injection remains limited to the exact predefined recovery template.

The editor and sample loader do not replace the six-step observation view.
Templates copy a definition into a new draft without touching its published Run.
Preview renders only verified structure, without synthetic task states. Every edit
invalidates the old preview and late validation responses are ignored. Explicit
creation freezes canonical body/key before sending; double clicks cannot send a
second operation. Save the pending operation locally when possible. An unknown
result offers an explicit same-key/body retry, never an automatic new publication.
Storage failure is an explicit same-page fallback, not a claim of reload recovery.
Language changes preserve draft, selection and submission identity.

After the confirmed receipt, select and query the real Run. With no Worker, explain
the real waiting state and show the exact local startup command (including port).
Render arbitrary saved nodes/edges, long names, serial chains, current blockers,
actual owners, Attempts and sample intervals. No invented request traffic, overlap,
output transfer or generic fault action. Keep raw identifiers and clock semantics.

## Independent implementation gates

1. H0: this inspected design and bounded plan.
2. H1: constrained definition normalization and read-only validation/preview API,
   request-size enforcement and tests.
3. H2a: additive receipt/membership migration, preservation/downgrade tests and
   migration-head checks. H2b: atomic idempotent custom submission, with rollback,
   conflict and concurrent replay tests. These are separate schema and API commits.
4. H3: scoped two-Worker startup for an existing custom Run, safety guards and tests.
5. H4: shared definition-driven DAG rendering and generic custom observation,
   including long identifiers, different topology and bilingual waiting guidance.
6. H5a: isolated editor/submission state machine and asynchronous/replay tests.
7. H5b: bilingual editor, template loading, preview and explicit creation UI wired
   to real APIs, with page integration tests.
8. H6: generic custom execution acceptance evidence for parallel and serial DAGs;
   retain the strict predefined diamond/fault baseline.
9. H7: real browser input-to-completion acceptance, predefined regression, screenshots,
   concise bilingual guides/README updates, full review and final CI verification.

Each gate remains independently runnable/testable and receives its own commit.
No new V2/V3 engine features, generic fault button, authentication system or front-end
framework are part of this phase. Observation remains scoped to demo membership in
the dedicated trusted local deployment. Per-Run limits are not a global admission
quota or tenant isolation system.

## Acceptance

Use the actual browser to enter a non-diamond DAG with different names and topology,
validate, preview, create a real Run, observe it waiting with no Worker, start the
two scoped Workers and observe genuine execution/ownership/dependency advancement.
Retain evidence that root admission precedes dependent claims and all joins wait.
Also execute a serial custom DAG and require peak observed overlap of one. Exercise
invalid definitions, stale previews, same-key replay and both UI languages. Re-run
the three predefined scenarios and inspect saved history. Sampled intervals include
timed waits; this is single-machine multi-container evidence, not CPU performance,
multi-machine validation, exactly-once, autoscaling or checkpoint resume.
