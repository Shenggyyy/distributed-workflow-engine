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
Persistence will use the existing Run -> Tasks lock order and one transaction for
skip propagation, retry/readiness updates and Run aggregation. A completion may
precede the next Scheduler pass, so terminal Run visibility is eventually updated.
Terminal Runs leave discovery. Receipt replay must remain available after settlement.

## Commit-sized MVP completion steps

- M5.1a: pure failure propagation and aggregation decisions (implemented).
- M5.1b: transactional Scheduler settlement, concurrency/rollback/HTTP acceptance.
- M5.2: cooperating business-idempotency example and crash-after-effect acceptance.
- M5.3: end-to-end container acceptance, failure documentation and final M0–M5 audit.

Larger steps may be split further; V2/V3 features stay outside the MVP.
