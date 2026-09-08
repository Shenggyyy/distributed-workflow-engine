# Business idempotency at the destination

The engine delivers at least once. A Worker can commit an external effect and
crash before its completion reaches the API. Its replacement receives a new
Attempt ID but the same `HandlerContext.idempotency_key` (Task UUID). A new Run
has different Task IDs and intentionally performs new effects.

`examples/idempotent_effect.py` models a cooperating PostgreSQL destination.
It has its own receipt table and a business counter, outside engine metadata and
Alembic migrations. `record_effect` inserts the Task key and input using a UNIQUE
constraint; only the winning insertion increments the counter. Both writes share
one READ COMMITTED transaction. Concurrent duplicates wait for the winner, then
read its committed receipt. Different input under the same key is a conflict.
Rollback removes both writes; a lost response after COMMIT can replay safely.

Use an explicitly configured disposable PostgreSQL database for this example:

```console
uv run --locked python examples/idempotent_effect.py --database-env-file .env.database-test
uv run --locked pytest tests/integration/test_business_idempotency.py --database-env-file .env.database-test
```

Expected example output: `Two invocations; one committed business increment for
this Task key.` The example explicitly creates `demo_effect_receipts` and
`demo_business_counter` and retains them. Tests use isolated temporary schemas.
`EffectHandler` is a trusted custom registry entry; it is not enabled in the
stock Worker or selectable through arbitrary Workflow-supplied Python code.
Its database settings come from the operator, never Workflow payloads or source.

This guarantees one increment per retained key in this destination under this
protocol, not exactly-once handler execution. Every writer must cooperate and
receipts must outlive possible duplicate delivery. Deleting a receipt or using a
different key can repeat an effect. An HTTP provider needs its own idempotency
contract; a database receipt cannot atomically wrap a nontransactional remote call.
General resource fencing and business key selection are application decisions.

M5.2a implements the example and concurrent/rollback/conflict tests. M5.2b verifies
the crash-after-effect window with real Worker processes and engine recovery.
