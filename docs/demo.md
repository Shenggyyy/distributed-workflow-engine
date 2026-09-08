# Local demonstration

Requires Docker Desktop running Linux containers, Python 3.13 and uv. Run from the
repository root. This is one machine with several independent containers, not a
multi-machine deployment test. Development Compose and existing data are untouched.

```powershell
uv sync --locked
uv run python scripts/demo.py up
```

This builds the image, creates an ignored password only if missing, starts the
dedicated PostgreSQL database, applies additive migrations and starts API/Scheduler.
The default address is `http://127.0.0.1:18080/demo/`.
Existing installations use the same `up` command to rebuild the latest page, then
refresh the browser. No database reset or new migration is needed for phase E.
Keep Docker Desktop running while viewing/running scenarios. The six numbered
sections are in Chinese with engine state names retained in English.
If needed set `$env:DWE_DEMO_PORT = "18081"` before **all** demo commands; the
printed address reflects it. The database port is not exposed to the host.

```powershell
uv run python scripts/demo.py run parallel
uv run python scripts/demo.py run distribution
uv run python scripts/demo.py run recovery
```

Each command creates a fresh Run and prints its ID and page address. Run scenarios
one at a time for a clear demonstration. Parallel uses one Worker with two slots;
distribution uses two separate one-slot Worker containers. Each root takes about
eight seconds, followed by a two-second join. Recovery uses a twenty-second root.

Immediately after starting recovery, copy its Run ID into this local command:

```powershell
uv run python scripts/demo.py fail --run-id <RECOVERY_RUN_ID>
```

It waits for actual root Handler START evidence, verifies container identity and
scope, sends SIGKILL to that container and starts replacement Worker B. If the root
already finished, create a fresh recovery Run. Six-second lease/heartbeat windows
and a persisted 5–10 second jittered backoff expose recovery. The old Attempt is
LOST; its actual finish is unknown. The retry has a new Attempt and Worker identity.
The UI never controls Docker and never accepts shell commands over HTTP.

```powershell
uv run python scripts/demo.py down
```

`down` stops only labelled demo Workers and the dedicated demo services. It retains
containers, volume, password and all Run/evidence history. `up` resumes the services;
new scenario commands use new identities. Do not delete the password while keeping
the database volume. Commands never truncate, downgrade, reset, remove volumes or
operate on development Workers. A failed scenario-creation POST is not retried
automatically: inspect `http://127.0.0.1:18080/demo/runs` for any created Run.

Evidence semantics and acceptance boundaries: [demo design](demo-design.md).

## Repeatable evidence checks

After `up`, keep the page open with **Follow newest Run** enabled:

```powershell
uv run python -m scripts.demo_acceptance
```

This creates three real Runs, checks positive measured overlap in a common clock
domain, verifies two Worker owners, and injects a scoped recovery failure. It must
observe RETRY_WAIT before accepting recovery success. It retains raw snapshots
(including the retry checkpoint) in ignored `.uv-cache/demo-acceptance/`.
For one scenario use `--scenario recovery`; for a different port use `--port 18081`.
This script checks engine evidence; browser inspection remains a separate gate.
Pure timeline calculations can be tested with Node.js 22+:
`node --test tests/demo-evidence.test.mjs`. Node.js is not needed to run the demo.

## Three-minute walkthrough

Before starting the timer, run `up` and open `http://127.0.0.1:18080/demo/`.
First-time image downloads/builds are setup time. Keep **跟随最新 Run / Follow newest
Run** checked. Scroll down through the numbered sections or use the sticky links;
use the loop links in step 05 to revisit 03/04. No network traffic animation is shown.

| Time | Action and what to explain |
| --- | --- |
| 0:00 | Execute `uv run python -m scripts.demo_acceptance`. In 01, identify the Run, scenario and real published definition. Tasks are explicitly timed demo Handlers, not a sales report. |
| 0:05 | In 02, follow downward DAG edges. A/B/C/D have no dependencies and may overlap; Join requires all four successes. In 03, point to actual READY rows and Join's named blockers. This is PostgreSQL state, not an extra queue. |
| 0:10 | In 04, one Worker has two configured slots and two confirmed Attempts. Claim, Handler sample receipt and lease renewal are separate. In 06, overlapping sampled intervals prove concurrent Handler lifetimes; RUNNING alone does not. |
| 0:30 | Distribution starts. In 04, compare two container/session IDs and the tasks actually claimed by each. Explain Worker pull and transactional allocation; task-to-Worker assignment is not prearranged. |
| 0:45 | In 05, successful roots satisfy Join's dependencies. Scheduling makes subsequent work READY and Workers pull again. Follow the visible link back to 03/04. A satisfied dependency is not an invented READY event. |
| 1:00 | Recovery starts. The terminal confirms the scoped SIGKILL and script-started replacement. In 04, the old heartbeat/renewal stops advancing and their deadlines pass. Registry loss and Attempt loss can occur in different snapshots. |
| 1:10 | In 05, read old LOST, the saved retry time and RETRY_WAIT. Expiry alone did not end the Handler; the engine confirms loss transactionally. The old timeline has no FINISH. |
| 1:25 | In 04/05, observe a new Attempt number and owner. It restarts the Handler from the beginning; the old Worker did not transfer it. Replacement startup is a script action, not automatic scaling. |
| 2:00 | In 06, read Run SUCCEEDED, per-owner work and the gap before the replacement interval. In Attempt details, compare claim, execution evidence and completion admission. Old Attempt has no accepted completion. |
| 2:30 | Explain at-least-once/business idempotency and the single-machine boundary. Normal Worker exit after success later expires its heartbeat; that alone is not a task failure. Open raw JSON or select a previous Run. |

Timings are approximate, not a recovery SLA. Individual `run` and `fail` commands
above give manual control. A pinned `?run=...` page stops following new Runs; use
the selector or re-enable **Follow newest Run**. API requests and snapshot schemas
are described in [demo API](demo-api.md); interactive OpenAPI is at
`http://127.0.0.1:18080/docs`.

Workers exit normally when their selected Run finishes. Their registry heartbeat
later expires to LOST even on a successful Run; this alone is not evidence of a
failed Task. The recovery evidence is the old LOST **Attempt**, retained retry
schedule, explicit fault command and replacement Attempt. Browser disconnection
freezes the last view and displays a stale-data warning.

## Actual captures

Unmodified browser screenshots from the current six-step page, captured on
2026-09-09 (Australia/Sydney; database timestamps displayed in UTC). Run IDs and
checks are recorded in [flow acceptance](demo-flow-review.md). Fresh runs get new
identities; no screenshot state is replayed into the page.

Dependencies and current PostgreSQL waiting conditions:

![Vertical DAG and waiting reasons](images/flow-dependencies.png)

One Worker with two confirmed Attempts, followed by actual overlap evidence:

![Two allocations in one Worker](images/flow-parallel.png)

![Measured concurrent Handler lifetimes](images/flow-overlap.png)

Two independent containers with ownership chosen by real claim transactions:

![Two Workers pulling from the same Run](images/flow-distribution.png)

Recovery in the same Run: real RETRY_WAIT, then a new allocation, then completion:

![Persisted retry wait](images/flow-retry.png)

![Old session lost and replacement executing Attempt 2](images/flow-recovery-workers.png)

![Rescheduling loop and new confirmed allocation](images/flow-replacement.png)

![Final recovery outcome and sampled intervals](images/flow-recovery-result.png)

The original phase D screenshots/acceptance remain in [historical review](demo-review.md).
