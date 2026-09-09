# Optional demonstration API

Enabled only by `python -m workflow_engine.demo api`, alongside the existing core
API. The stock `engine api` does not serve this module. The standalone Compose file
binds it to loopback and a dedicated database; this is a trusted local tool without
authentication, not a public dashboard. No Docker socket is mounted into the API.

| Endpoint | Contract |
| --- | --- |
| `POST /demo/runs` | JSON `{"scenario":"parallel"}`; choices are parallel, distribution, recovery. Every explicit POST creates a fresh Run; unknown fields/scenarios return 422. No automatic POST retry. |
| `GET /demo/runs` | Latest 50 registered demo Runs, newest first. Ordinary Runs are excluded. |
| `GET /demo/runs/{uuid}` | Coherent snapshot of one registered demo Run; 404 for ordinary/unknown Runs, 422 for invalid UUIDs. |
| `GET /demo/` | Packaged HTML/CSS/JavaScript; no frontend build or separate UI service. |
| `GET /demo/custom/catalog` | Read-only trusted Handler keys/durations, limits and editable copies of current diamond templates. No publication or Run creation. |
| `POST /demo/custom/validate` | A Workflow JSON body; returns canonical `definition`, dependency `layers` and demo `limits`. No database access, writes or execution. |
| `POST /demo/custom/runs` | The same bounded Workflow JSON plus one `Idempotency-Key`. Atomically publishes and creates a custom demo Run, or replays its committed receipt. Does not start Workers. |

Creation publishes the fixed definition, creates all Tasks and inserts demo Run
membership in one core READ COMMITTED transaction. Success follows COMMIT. HTTP
requests do not start containers: the local CLI creates labelled Run-scoped Workers.

## Custom definition preview

The [custom phase](custom-dag-plan.md) separates read-only validation from explicit
submission. The editor and custom Worker command arrive in later gates; the
existing three scenarios remain usable.
Send `Content-Type: application/json` with the core definition shape, for example:

```json
{
  "name": "my_preview",
  "tasks": [
    {"task_id": "Read", "task_type": "demo.join"},
    {"task_id": "Inspect", "task_type": "demo.observe", "depends_on": ["Read"]}
  ]
}
```

The backend uses the core identifier and DAG validation rules. Demo-only limits
are 12 tasks, a 16 KiB actual UTF-8 body, registered catalog Handlers (2–20 seconds),
and the fixed execution policy shown in the normalized response: at most two
Attempts, 90-second timeout, 10000 ms backoff bounds. Omitted policies are filled;
explicitly different policies are rejected. The response is schema 2 even for
schema 1 input. Names and array order are preserved. Validation has no side effects.

Malformed, duplicate-field or non-finite JSON and core DAG errors return 422.
JSON nesting is limited to 16 container levels, above the supported shape's needs;
constraint details contain only `location` and stable `type`. Additional types are
`demo_task_limit`, `demo_handler_not_allowed` and `demo_execution_policy`.
Oversized bodies return 413 `demo_body_too_large`, including streamed input without
a length header; unsupported content types return 415 `demo_json_required`.
Never interpret a node name as implemented business behavior or dependency edges
as automatic output transfer. The public core API retains its broader contracts.

## Custom submission

After preview, explicitly POST its canonical `definition` to `/demo/custom/runs`
with one case-sensitive `Idempotency-Key`: 1–128 ASCII characters, starting with
a letter/digit, followed by letters, digits, `.`, `_`, `:` or `-`. Reuse the core
key validator, including rejection of duplicate headers. Invalid input returns
422 `invalid_request`; no submitted values appear in the error details.

Both first creation and replay return HTTP 201 and the same receipt:

```json
{"run_id":"<real UUID>","workflow_version_id":"<real UUID>","scenario":"custom"}
```

`Location` points to `/demo/runs/{run_id}`. The receipt identifies a committed
operation; it does not claim execution has started. Query that scoped snapshot
for current state. Initially roots are READY, dependent Tasks are PENDING, and
there are no Attempts or Workers. Creation never invokes Docker or a Handler.

The demo owns one fresh READ COMMITTED transaction. A two-int advisory gate in
namespace `DWED` (`0x44574544`) serializes each key before any core row locks;
its second value is a signed BLAKE2s 32-bit digest. Hash collisions only serialize
unrelated submissions: the full key and normalized JSON are compared in the
immutable receipt. The existing publication and Run repositories then create
the version and Tasks, followed by `custom` membership and the receipt, within
that transaction. Success follows commit. A rejected statement or rejected COMMIT
rolls back all writes. If the connection disappears during COMMIT, the client
cannot infer whether it committed; resolve that unknown outcome with the same key.
No core transaction, ownership, retry or completion contract changes.

Same key plus the same canonical definition returns the original version and Run
without republishing, even after execution progresses. Object-key order and
omitted defaults normalize identically; task/dependency array order remains
significant. Same key with different content returns 409
`demo_submission_conflict`. A different key deliberately creates a new Run/version.

Freeze the key and canonical body before sending. If a response is lost, the
outcome is unknown: explicitly retry **that same key and body** to obtain the
receipt; never silently generate a new key. There is no automatic database retry.
The receipt key namespace is demo-local and separate from `/runs`. The existing
`/workflows` publication and predefined `POST /demo/runs` remain non-idempotent.
Migration [0012](migrations.md) preserves all earlier data and refuses downgrade
when custom membership or receipts exist.

## Observation snapshots

Snapshot fields:

- `snapshot_at`: database clock sampled in a read-only REPEATABLE READ transaction.
  All following rows use that transaction's MVCC snapshot; concurrent later commits
  are excluded. This is not an exact COMMIT or atomic wall-clock state instant.
- `run`: Run identity, pinned definition, status and scenario. Its `created_at` is
  creation metadata only. `tasks`: real Task UUID/key/state rows.
- `attempts`: Attempt identity/number/state, owning session, `acquired_at`, latest
  lease `last_renewed_at` and expiry, optional completion `accepted_at`, and optional
  retry `scheduled_at` / `available_at`. Engine-generated loss has no fabricated
  completion receipt.
- `workers`: registered demo session identities, names, slots, registry status,
  last heartbeat and expiry. Freshness is evaluated against the snapshot's DB time.
- `samples`: invocation UUID, Attempt UUID, clock domain, sequence, phase,
  `monotonic_ns` as a decimal string, and DB receipt `recorded_at`. Strings preserve
  nanosecond integers for JavaScript BigInt; samples are never interpolated.

The Handler's START and FINISH are samples **inside** the callable, not exact Python
entry/return boundaries. Receipt latency and intentional timed waits are part of
the observed lifetime. A missing FINISH gives only a lower bound through the last
PULSE; it cannot establish the actual crash/end time. Database receipt time is not
substituted for execution time. Compare only identical boot/monotonic-offset domains.

No telemetry endpoint grants ownership or accepts arbitrary state mutation.
Observations use separate short transactions and lock only their own invocation
row for ordering. Core locking, lease admission, retries and stale-result rejection
remain unchanged. Migration 0011 is additive; existing rows are never backfilled or
rewritten. Old clock-domain evidence remains separate. See [design](demo-design.md).

The page polls sequentially with a 500 ms delay between completed fetch cycles;
network/database latency adds to that interval. Brief states may be missed, while
Attempt/retry history remains. Queries are scoped to bounded demo DAGs; this
read model is not a global operational event log or high-volume monitoring API.

Phase E adds only the existing nullable lease renewal timestamp to this snapshot.
No migration or write transaction changes. The six-step page derives current
dependency blockers from the pinned definition and Task rows in the same snapshot.
It does not infer READY transition times, request transmission, physical slot IDs
or Docker lifecycle events. Heartbeat/lease deadline comparisons use snapshot DB
time with sub-millisecond precision. An expired deadline does not mutate the
displayed persisted status; terminal Attempts show leases as historical evidence.
