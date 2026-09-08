# Failure propagation and Run settlement

Every edge uses all-success dependency semantics. A permanently FAILED or SKIPPED
parent makes a PENDING child SKIPPED. A topological pass cascades that decision
through descendants without executing them. RETRY_WAIT is not failure: descendants
wait until the Task succeeds or exhausts its budget.

Independent branches continue. A Run remains RUNNING while any Task is PENDING,
READY, RUNNING or RETRY_WAIT. Once every Task is terminal, all SUCCEEDED means
Run SUCCEEDED; otherwise Run FAILED. There is no fail-fast cancellation. Existing
state-machine events express these transitions; no schema change is required.

The complete, validated Task snapshot must match its immutable DAG and Run. The
pure planner preserves identities and returns proposals without mutating input.
Persistence uses the existing Run -> Tasks lock order and one transaction for
skip propagation, retry/readiness updates and Run aggregation. A completion may
precede the next Scheduler pass, so terminal Run visibility is eventually updated.
Terminal Runs leave discovery. Receipt replay must remain available after settlement.

## Commit-sized MVP completion steps

- M5.1a: pure failure propagation and aggregation decisions (implemented).
- M5.1b: transactional Scheduler settlement, concurrency/rollback/HTTP acceptance (implemented).
- M5.2a: cooperating business-idempotency example (implemented).
- M5.2b: real process crash after a committed effect, recovery and duplicate acceptance (implemented).
- M5.3a: terminal Run and failure-branch container acceptance (implemented).
- M5.3b: failure documentation, final M0–M5 validation and correctness audit (complete; see [review](mvp-review.md)).

Larger steps may be split further; V2/V3 features stay outside the MVP.

The repository checks every returned Task and Run snapshot against its proposal.
Skipped changes, ready changes and Run outcome share the caller's COMMIT. The
existing return value and Scheduler counters still count only newly READY Tasks;
skips and terminal Runs are visible through query APIs, not readiness counters.
The Scheduler does not acquire Worker/Attempt/lease locks after locking Tasks.
