# Architecture — CP1

## Scope boundary

CP1 provides infrastructure only. It deliberately does **not** implement `Money`, `Wallet`, `WagerTransaction`, `Ledger`, `Inbox`, or `Outbox`; no financial tables or placeholder migrations exist. Those components remain a CP2 decision after review.

## Module direction

```text
interfaces (HTTP) -> application -> domain
interfaces (HTTP) -> infrastructure (composition/readiness adapters)
infrastructure -> domain ports (when CP2 introduces ports)
```

Current directories reserve the boundaries:

- `src/domain`: framework-free financial model in CP2.
- `src/application`: use cases and ports in CP2.
- `src/infrastructure`: MikroORM, SQS, LocalStack-oriented configuration and health adapters.
- `src/interfaces`: HTTP controllers and transport concerns.

The health endpoint is an operational interface and composes infrastructure probes without creating financial-domain dependencies.

## Persistence and migrations

MikroORM 6 with the PostgreSQL driver is configured. Because CP1 intentionally has no entities, discovery is explicitly configured not to reject the empty entity list. Migrations are transactional and `allOrNothing`; the repository has no migrations in CP1 because schema creation would violate the checkpoint boundary. `migration:up` and `migration:down` run MikroORM's versioned migration lifecycle and are ready for the first approved schema migration.

## Messaging

AWS SDK v3 communicates with SQS. LocalStack initializes three FIFO queues. `wager-transactions.fifo` is configured with a queue-level dead-letter redrive policy to `wager-transactions-dlq.fifo` after five receives. FIFO ordering/deduplication is treated as transport behavior only; the required financial idempotency and concurrency guarantees must be implemented in PostgreSQL in CP2.

## Readiness

Liveness has no external dependency. Readiness calls PostgreSQL (`SELECT 1`) through the active MikroORM connection and SQS (`ListQueues`) through AWS SDK v3. A failed dependency produces HTTP 503 and identifies the failed dependency; it does not report the service ready from cached state.

## Money and concurrency decision record

No financial values are handled in CP1. When CP2 is authorized, public and event contracts will retain monetary amounts as decimal strings and domain arithmetic will use an arbitrary-precision `Decimal` type, never JavaScript `number`. The PostgreSQL schema will enforce uniqueness and non-negative/concurrency invariants; SQS FIFO will not be used as the correctness mechanism.

## Authentication

The source challenge makes authentication optional. CP1 deliberately implements neither authentication nor a no-op guard because no financial endpoint exists yet. Health endpoints will remain unauthenticated by contract. The CP2 HTTP boundary will be the extension point for an external OIDC provider rather than a custom credential store.

## Multi-instance preparation

The application process is stateless. Infrastructure configuration comes from environment variables. No in-memory state is used for readiness or messaging guarantees. CP2 persistence workflows must use database transactions and row-level/optimistic concurrency suitable for concurrent replicas.
