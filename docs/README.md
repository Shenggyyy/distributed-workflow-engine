# Documentation guide

Start with the [project homepage](../README.md). Detailed technical references are
in **English** unless marked otherwise. The runnable commands are executed from
the repository root. Historical reviews describe their named commits/milestones;
their counts and revisions are not claims about the current checkout.

## Quick start and local development

- [Demo startup and three-minute walkthrough](demo.md): the fastest way to see the engine.
- [Container development](local-development.md): credentials, ports and persistence.
- [Configuration and structured logging](configuration.md): precedence, validation and diagnostics.
- [Database connections](database.md) and [migrations](migrations.md).
- [Run Workers](running-workers.md) and [run Schedulers](running-scheduler.md).

## Architecture and core contracts

| Topic | Model / behavior | Persistence / transactions |
| --- | --- | --- |
| Architecture | [Components, consistency and limits](architecture.md) | [Runtime storage](runtime-storage.md) |
| Workflow DAG | [Definitions and validation](workflows.md) | [Version storage](workflow-storage.md) |
| Run and Task | [State machines](runtime.md), [dependency scheduling](scheduling.md), [settlement](settlement.md) | [Creation](run-creation.md), [idempotency](run-idempotency.md), [queries](run-queries.md), [discovery](run-discovery.md) |
| Worker sessions | [Model](workers.md), [registration](worker-registration.md), [heartbeat](worker-heartbeat.md) | [Storage](worker-storage.md) |
| Claim and Lease | [Ownership model](attempt-leases.md), [claiming](task-claims.md), [replay](idempotent-claims.md) | [Lease storage](lease-storage.md), [claim receipts](claim-requests.md), [renewal](lease-renewal.md) |
| Completion | [Result/replay model](attempt-completion.md) | [Receipt storage](completion-storage.md), [atomic completion](completion-transactions.md) |
| Execution | [Trusted Handlers](handlers.md), [process supervision](worker-execution.md), [Worker loop](worker-loop.md), [HTTP transport](worker-transport.md) | [Retry policy](retry-policy.md), [timeouts and recovery](timeouts.md) |

## API and usage

- HTTP: [Workflow](api.md), [Run](run-api.md), [Worker](worker-api.md),
  [claim](claim-api.md), [renew Lease](lease-api.md), [complete Attempt](completion-api.md).
- OpenAPI is `/openapi.json`; interactive docs are `/docs` on the running API.
- [Examples directory](../examples): executable Python examples for each layer,
  including [validation](../examples/validate_workflow.py),
  [publication](../examples/publish_workflow.py),
  [idempotent submission](../examples/create_idempotent_run.py),
  [claim and renewal](../examples/claim_and_renew.py) and
  [completion](../examples/complete_attempt.py). The topic documents describe setup.
- [Business-effect idempotency](business-idempotency.md) explains why engine fencing
  cannot make arbitrary external effects exactly once.

## Demonstration

- [Startup and walkthrough](demo.md), [design and evidence](demo-design.md),
  [scoped query API](demo-api.md).
- [Original real execution evidence](demo-review.md) and
  [six-step flow evidence](demo-flow-review.md), including retained screenshots.

## Tests, failures and acceptance

- [Testing and CI](testing.md), [failure scenarios](failure-scenarios.md).
- [MVP final review](mvp-review.md); earlier [M2](m2-review.md),
  [M3](m3-review.md), [M4](m4-review.md) reviews preserve historical evidence.
- [Smoke scripts](../scripts) verify real HTTP, Worker/Scheduler processes and containers.

## Known limitations

[Architecture limits](architecture.md#explicit-mvp-limits),
[failure boundaries](failure-scenarios.md),
[business idempotency](business-idempotency.md) and
[demo evidence limits](demo-design.md) are authoritative starting points.
This trusted-deployment MVP does not establish production availability, multi-machine
performance, exactly-once effects, autoscaling or automatic artifact transfer.
