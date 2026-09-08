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

Creation publishes the fixed definition, creates all Tasks and inserts demo Run
membership in one core READ COMMITTED transaction. Success follows COMMIT. HTTP
requests do not start containers: the local CLI creates labelled Run-scoped Workers.

Snapshot fields:

- `snapshot_at`: database clock sampled in a read-only REPEATABLE READ transaction.
  All following rows use that transaction's MVCC snapshot; concurrent later commits
  are excluded. This is not an exact COMMIT or atomic wall-clock state instant.
- `run`: Run identity, pinned definition, status and scenario. Its `created_at` is
  creation metadata only. `tasks`: real Task UUID/key/state rows.
- `attempts`: Attempt identity/number/state, owning session, `acquired_at`, latest
  lease expiry, optional completion `accepted_at`, and optional retry `scheduled_at`
  / `available_at`. Engine-generated loss has no fabricated completion receipt.
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
rewritten. Old clock-domain evidence remains separate. See [design](demo-plan.md).

The page polls sequentially with a 500 ms delay between completed fetch cycles;
network/database latency adds to that interval. Brief states may be missed, while
Attempt/retry history remains. Queries are scoped to small fixed demo DAGs; this
read model is not a global operational event log or high-volume monitoring API.
