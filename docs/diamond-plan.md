# Diamond demonstration transition

This phase changes the optional demo, not engine semantics. Starting point:
`f98d4e7`, with the completed bilingual page and a clean working tree. Existing
Workflow versions, Runs, Handler types and historical captures remain intact.

## Read-only findings and design

The demo API constructs workflows in `demo/api.py`; the snapshot already reads the
Run's immutable saved definition. DAG drawing and dependency/retry explanations
are definition-driven. No schema, migration, new API, core transaction or locking
change is needed. The CLI currently starts recovery with one Worker and kills the
owner of root A; its validator also assumes that old shape. Those assumptions must
change together when the new definitions are activated.

All three new scenarios use `A -> B/C -> D`: A has no dependencies, B/C depend on A,
and D depends on both branches. New explicitly registered timed Handler keys keep
old Handler meanings unchanged. Intended durations are A=6s, B=8s, C=14s, D=3s;
recovery uses C=20s. Actual sampled lifetimes include observation/processing latency.
These are bounded teaching waits, not business data processing or timing SLAs.

Parallel uses one Worker with two slots. Distribution and recovery use two
independent one-slot Workers. A demo-only startup rendezvous waits for both scoped
sessions to be registered and fresh, renewing heartbeat while waiting, before
entering the unchanged Worker loop. This is startup coordination, not task routing.
Timeout or missing peers fail clearly; duration alone is not a readiness check.

Recovery targets the longer C branch, resolves its actual Attempt/Worker from the
snapshot, and never assumes a container owns C. Both branches must have genuinely
started. A must already have succeeded and D must remain unclaimed. The surviving
Worker finishes B, retains that success, then can claim a new Attempt for C after
normal Lease expiry and durable retry backoff. No replacement or autoscaling is
needed. Any unexpected claim, completion or identity makes fault injection refuse.

The local fault operation validates saved definition, Run membership, current
Task/Attempt #1, real START/PULSE without FINISH, live ownership and exact container
name/labels/immutable ID, then rechecks immediately before SIGKILL. Database reads
and Docker cannot form one atomic transaction. Preserve pre-fault evidence and
strictly verify the result; do not hide a race by relaxing assertions or rerunning
until lucky. No HTTP shell interface is introduced.

Execution proof uses matching-domain monotonic START/PULSE/FINISH intervals,
specifically B/C overlap. DB claim/admission/retry timestamps prove dependency and
retry ordering. Lease expiry is never a Handler end. The validator also checks D
is unclaimed while retrying, a real RETRY_WAIT checkpoint, one successful sibling
Attempt, different recovery ownership and final success. Keep all raw identifiers.

## Small independent commits

1. G0: this bounded design and audit record.
2. G1: bounded demo Worker startup rendezvous, heartbeat/readiness checks and tests;
   use it for existing distribution without changing existing workflow shapes.
3. G2: diamond factory and new fixed trusted Handler registrations with tests;
   prepare the new contract before changing the public scenario factory.
4. G3: strict branch fault selection/revalidation and failure evidence helpers,
   independently tested before switching the CLI's active recovery command.
5. G4: diamond evidence/checkpoint validation with positive and negative tests,
   independently tested before switching automatic scenario acceptance.
6. G5: activate the new factory, two-Worker recovery and guarded fault command;
   wire the validator, update bilingual descriptions/guides and preserve old views.
7. G6: run all three actual scenarios, inspect both browser languages and history,
   capture new diamond evidence, update current README/guide images, review and CI.

Each commit gets relevant validation, complete diff/staged review, an explicit
local-instruction exclusion check, push and remote hash verification. Intermediate
preparatory commits leave the existing demonstrations runnable. Final acceptance
requires actual browser inspection and exact final CI, not test output alone.

## Documentation and history

Keep the current bilingual homepages and reader-oriented navigation. Update the
current design and commands in place at cutover. Current screenshots become
`diamond-*`; older `demo-*`, `flow-*` and `release-*` images stay in their dated
reviews, explicitly identified as earlier definitions. Detailed final evidence
will be in `docs/diamond-review.md`. No prior Run is redrawn using the new factory.

This remains one machine with multiple Linux containers, at-least-once execution
and business idempotency requirements. No multi-machine validation, exactly-once,
autoscaling, checkpoint resume or artifact transfer is claimed.
