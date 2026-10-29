# Distributed Wagering Processor

This repository implements the financial processing, HTTP API, PostgreSQL ledger, SQS inbox/outbox delivery, operational pending-reference sweep, and reconciliation workflow required by the Jungle Gaming challenge. The full assignment remains unchanged in [CHALLENGE.md](./CHALLENGE.md).

> **Scope:** Authentication is intentionally not implemented because it is optional for this challenge. An API gateway or NestJS guard can be introduced at the HTTP boundary without changing financial contracts or persisted effects.

## Stack

| Component | Version |
| --- | --- |
| Bun runtime / package manager / test runner | 1.3.2 |
| TypeScript | 5.9.2 (strict) |
| NestJS | 11.1.6 |
| MikroORM core/PostgreSQL/migrations | 6.5.9 |
| PostgreSQL image | 17.2-alpine |
| LocalStack image | 3.8.1 |
| AWS SDK SQS | 3.806.0 |

## CP2A domain and persistence

The domain is independent of NestJS and MikroORM:

- `Money` uses `decimal.js`, accepts and exposes decimal strings, is immutable, and never accepts JavaScript `number` as a monetary contract.
- `Wallet` opens at version `1`, owns balance movement, creates an opening ledger entry for a positive opening balance, and increments its in-memory version only when balance changes.
- `WagerTransaction` models `OPENING`, `BET`, `WIN`, `LOSS`, `REFUND`, and `ROLLBACK`, including terminal-state protection and reference compatibility rules.
- `WalletLedgerEntry` is immutable and validates debit/credit arithmetic.
- Each aggregate exposes `rehydrate` for reconstruction of persisted state without replaying transition validation.

MikroORM registers these five CP2A schemas explicitly in both the CLI configuration and NestJS composition:

1. `wallets`
2. `wager_transactions`
3. `wallet_ledger_entries`
4. `inbox_messages`
5. `outbox_messages`

`numeric(20,2)` columns map through `DecimalType('string')`; no decimal mapping uses `number`.

## Database guarantees

`Migration20261028140000` is transactional and reversible. It supplies:

- unique wallet identity `(player_id, currency)`, non-negative balance, ISO currency format, and version starting at `1`;
- a wallet-version trigger: version is unchanged for non-financial updates and must advance exactly once when `balance` changes;
- idempotency-key uniqueness and provider-scoped external transaction uniqueness;
- a wallet/player/currency foreign key from transactions plus reference-transaction and ledger foreign keys;
- one ledger entry per `(wallet_id, transaction_id)` and ledger arithmetic checks;
- immutable ledger `UPDATE` and `DELETE` triggers;
- a ledger-insert trigger requiring the referenced transaction to be `PROCESSED`;
- terminal transaction-state protection; and
- a partial unique index preventing a second processed `REFUND` or a second processed `ROLLBACK` for the same reference transaction. A `REFUND` and a `ROLLBACK` remain distinct reversal kinds under this CP2A policy.

The ledger trigger permits the required ordered sequence inside one SQL transaction: persist transaction, change it to `PROCESSED`, then insert its ledger entry.

## Local stack and health

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
curl http://localhost:3000/health/live
curl http://localhost:3000/health/ready
```

The Compose stack starts PostgreSQL, LocalStack SQS, a one-shot `migrate` job, NestJS and the three workers (`sqs-consumer`, `outbox-publisher`, `pending-reference-sweeper`). `/health/live` only checks process liveness. `/health/ready` performs a real PostgreSQL `SELECT 1` through MikroORM and a real SQS `ListQueues`; it returns HTTP 503 if either dependency fails.

### Production runtime: one image, five compiled entrypoints

Every runtime process is a file under `src/` compiled by `nest build`, so the production image (which ships only `dist` and `node_modules` — no sources, no `package.json`) runs the same typechecked artifacts the suites exercise:

| Process | Development | Production image |
| --- | --- | --- |
| HTTP API | `bun run start` | `bun run dist/main.js` |
| Schema migrations | `bun run migrate` | `bun run dist/migrate.js` |
| SQS wager consumer | `bun run sqs-consumer` | `bun run dist/sqs-consumer.js` |
| Outbox publisher | `bun run outbox-publisher` | `bun run dist/outbox-publisher.js` |
| Pending-reference sweeper | `bun run pending-reference-sweeper` | `bun run dist/pending-reference-sweeper.js` |

`scripts/cp3a-sqs-consumer.ts` and `scripts/cp3b-outbox-publisher.ts` remain as thin wrappers that import `src/sqs-consumer.ts` and `src/outbox-publisher.ts`, so the existing integration suites keep spawning those paths while there is exactly one functional implementation of each worker. `scripts/cp2b1-process-worker.ts` stays a test-only concurrency helper and is deliberately not a production service.

`dist/migrate.js` applies pending migrations and nothing else: it never runs `down`, it fails explicitly when its migration directory ships no migration file or when migrations remain pending afterwards, and re-running it on an up-to-date database applies nothing and exits `0`. Compose runs it as a job and gates the API and every worker behind `service_completed_successfully`, so no process touches data before the schema is in place. Ports are configurable (`POSTGRES_PORT`, `LOCALSTACK_PORT`, `APP_PORT`) to keep disposable stacks from colliding.

## Quality, migrations, and isolated PostgreSQL integration tests

Run normal gates with Bun 1.3.2:

```bash
bun run typecheck
bun run build
bun test
bun run migration:up
bun run migration:down
```

The financial integration suite must use a disposable isolated PostgreSQL database, never a development database. Set `DATABASE_URL` in the execution environment to that isolated database, then run:

```bash
bun test test/integration/financial-schema.spec.ts
```

The suite applies the migration, proves real PostgreSQL SQLSTATE failures for constraints and triggers, exercises an ORM flush/clear/reload precision round trip, proves `UP/DOWN/UP`, and drops the financial schema in teardown. Use the repository's Bun Docker image when Bun is unavailable on the host; pass the connection string only through the environment, not the command history or documentation.

## CP2B boundary

CP2B must provide the transactional `ProcessWager` workflow, per-wallet concurrency control and retry policy, atomic coordination of wallet/ledger/inbox/outbox, idempotent replay responses, reference reprocessing, SQS acknowledgement/DLQ behavior, outbox publication, financial HTTP interfaces, and reconciliation. CP2A's materialized-balance/ledger relationship is modeled and constrained locally, but full cross-row consistency and multi-instance concurrency require that CP2B transaction coordinator.

## Reset local services

```bash
docker compose down -v
```

This is destructive for the Compose development volume. It is not required for CP2A integration tests.

## CP2B-1 financial processing

CP2B-1 adds HTTP endpoints for wallet opening and **BET only**:

- `POST /wallets` and `GET /wallets/:walletId`;
- `POST /wagering/transactions` with required `Idempotency-Key`;
- `GET /wagering/transactions/:transactionId`; and
- `GET /providers/:providerId/wagering/transactions/:externalTransactionId`.

`WIN`, `LOSS`, `REFUND`, `ROLLBACK`, and `PENDING_REFERENCE` are not exposed by this checkpoint. Unsupported kinds are rejected instead of acknowledged.

Wallet opening, BET processing, ledger persistence, terminal snapshots, and outbox persistence occur inside one MikroORM SQL transaction. BET acquires a pessimistic write lock on its wallet (`SELECT ... FOR UPDATE`), then persists the wallet balance/version, terminal transaction, optional ledger row, and typed outbox rows before commit. An insufficient-balance BET is committed as `REJECTED` with `INSUFFICIENT_BALANCE`, its observed `resulting_balance`, no balance/version update, no debit ledger, and a rejection event.

The durable idempotency identity is PostgreSQL. The canonical SHA-256 payload includes `providerId`, `externalTransactionId`, `walletId`, `playerId`, `roundId`, `gameId`, `kind`, and `{ amount, currency }`, recursively sorted by JSON object key. It excludes `Idempotency-Key`, correlation IDs, and transport metadata. A matching key/hash returns the original persisted response with `idempotentReplay: true`; the historical `resulting_balance` is not recalculated after subsequent movements. A changed payload for the same key and a duplicate `(providerId, externalTransactionId)` are conflicts.

CP2B-1 records typed, versioned integration events atomically in the outbox (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`) but does not publish SQS messages. Publisher, retry, and DLQ work remain CP3.

The terminal transaction delete trigger was added to the existing CP2A migration because the migration is not integrated into `main` and has no production deployment. It blocks application-role deletion of `PROCESSED`, `REJECTED`, and `FAILED` rows—including rows without a ledger—while migration down remains reversible in isolated databases.

Run the CP2B-1 PostgreSQL suites only against a disposable database:

```bash
bun test test/integration/financial-processing.spec.ts   # BET, concurrency, replay, 3 processes
bun test test/integration/financial-atomicity.spec.ts    # rollback by fault injection, transient failures
bun test test/integration/financial-lock-sql.spec.ts     # captured SELECT ... FOR UPDATE evidence
bun test test/integration/financial-http.spec.ts         # real NestJS server over HTTP
bun run test                                             # unit + integration, 60s hook timeout
```

It covers zero/positive opening, ledger creation, durable rejection, delete protection, idempotent replay, 50 concurrent identical calls, same-wallet balance contention, independent wallets, historical replay snapshots, duplicate provider transactions, and three separate Bun worker processes sharing one PostgreSQL database.

### CP2B-1-R1 closure

Transient PostgreSQL conflicts (`40001` serialization failure, `40P01` deadlock) retry the complete transaction at most three times (`MAX_BET_ATTEMPTS`). This is safe because no effect leaves the database before commit: outbox rows are persisted, never published. Unique violations (`23505`) are resolved as idempotent replays or conflicts, and every other failure propagates unchanged — a transient infrastructure error is never converted into a financial `REJECTED`.

Rollback is proven with a deterministic test-only interception point, `MikroOrmFinancialProcessing.interceptUnitOfWork`, a protected no-op in production that integration tests override by subclassing. No header, body field, or environment variable can trigger a failure through the API.

HTTP contract observed end to end against a real server:

| Case | Status | Body |
| --- | --- | --- |
| wallet created | 201 | wallet view |
| BET processed | 200 | `PROCESSED`, new balance |
| BET replayed | 200 | same `transactionId`, `idempotentReplay: true` |
| same key, other payload | 409 | `IDEMPOTENCY_CONFLICT` |
| insufficient balance | 200 | durable `REJECTED` + `INSUFFICIENT_BALANCE` |
| missing `Idempotency-Key` | 400 | `IDEMPOTENCY_KEY_REQUIRED` |
| numeric monetary amount | 400 | `INVALID_PAYLOAD` |
| unknown wallet / transaction | 404 | `WALLET_NOT_FOUND` / `TRANSACTION_NOT_FOUND` |
| currency the wallet does not hold | 400 | `CURRENCY_MISMATCH` |
| kind outside CP2B-1 | 400 | `INVALID_TRANSACTION` |

An insufficient-balance BET is a committed financial outcome, not a transport error, so it answers 200 with the terminal record instead of 422.

The HTTP suite starts the production entrypoint `src/main.ts` in a separate process, because `NestFactory.create` cannot be hosted inside the `bun test` runner. Readiness degradation is verified by starting a second server process against an unreachable SQS endpoint, never by stopping shared infrastructure.

### CP2B-1-R3 closure

Identifier validation accepts every RFC 9562 UUID version (1-8) with a valid variant, not only UUIDv4. CHALLENGE.md identifies players with UUIDv7 (`0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1`), which the previous `[1-5]` version range rejected with 400 on `POST /wallets`. Structure, version nibble, and variant bits are still enforced, so arbitrary strings, the nil UUID, version 0/9, wrong variants, wrong lengths, non-hexadecimal characters, and values with an appended SQL fragment remain rejected.

The outbox payload is the serialized envelope of CHALLENGE.md section 11: `eventId`, `eventType`, `aggregateId`, `correlationId`, optional `causationId`, ISO-8601 `occurredAt`, `version`, and the typed `data`. `eventType` and `version` live in the event subclass instead of a string at the call site, and events are built by the static factories `WagerTransactionProcessed.from`, `WagerTransactionRejected.from`, and `WalletBalanceChanged.from(wallet, entry, ctx)`. `causationId` is omitted from the JSON when absent rather than serialized as `null`.

The row and the envelope cannot diverge: `persistOutbox` derives `outbox_messages.id` from `eventId`, `aggregate_id` from `aggregateId`, and `event_type` from `eventType` of the event itself. The aggregate is the transaction for `WagerTransactionProcessed`/`WagerTransactionRejected` and the wallet for `WalletBalanceChanged`. When the caller supplies no `correlationId`, it defaults to that aggregate identifier.

`WalletBalanceChanged.data` carries `walletId`, `transactionId`, `direction`, `money`, `balanceBefore`, `balanceAfter`, and `walletVersion`, all monetary fields as `MoneyProps` decimal strings. `walletVersion` is read from the aggregate after the movement, so it equals the version persisted by the same unit of work: 1 for a positive opening and 2 after the first BET.

R3 gates: `bun run typecheck` and `bun run build` clean, and `bun run test` at 74 passing tests / 352 assertions across 14 files, including two new suites — `test/integration/financial-uuid.spec.ts` (UUID acceptance/rejection plus a UUIDv7 player persisted through opening and BET) and `test/integration/financial-outbox-envelope.spec.ts` (envelope keys, row/envelope agreement, rejection without `WalletBalanceChanged`, and no duplicate envelopes on idempotent replay).

Known limitations unchanged by R3: there is no SQS publisher, so `published_at` stays `null` and publication, retry, and DLQ remain CP3; the HTTP layer does not yet read correlation/causation headers, so over REST both fields fall back to the aggregate identifier and `causationId` is only reachable through the application port; and `WagerTransactionProcessed`/`WagerTransactionRejected` keep their previous `data` shape, which CHALLENGE.md does not specify.

## CP2B-2 wager operations and reference recovery

CP2B-2 accepts `WIN`, `LOSS`, `REFUND`, and `ROLLBACK` through the same idempotent transaction boundary as BET. WIN is one `CREDIT`; LOSS changes neither wallet nor ledger (including a valid `0.00` LOSS); REFUND reverses only a compatible BET with one `CREDIT`; and ROLLBACK reverses BET with `CREDIT`, or WIN/REFUND with `DEBIT`. Reversals require matching provider, wallet, player, currency, round, and amount. The partial PostgreSQL uniqueness policy permits only one direct economic reversal for a reference, including concurrent REFUND/ROLLBACK attempts. A debit that would make the wallet negative becomes terminal `REJECTED` with its dedicated failure code and no ledger movement.

The canonical idempotency hash keeps CP2B-1 compatibility: BET payloads retain the CP2B-1 field set and therefore retain their historical SHA-256 value. `referenceExternalTransactionId` is included only for `REFUND` and `ROLLBACK`, where it is economically material; distinct reversal references remain distinct payloads. Persisted hashes are immutable and a changed payload for the same idempotency key remains a conflict.

A missing reversal-reference field is invalid input. A supplied reference that has not yet arrived is persisted as `PENDING_REFERENCE`: it has no wallet movement, ledger row, or `WalletBalanceChanged`, but it does receive a `WagerTransactionPendingReference` outbox event and a due retry timestamp. Sweeps use `FOR UPDATE SKIP LOCKED`, are batch-limited (default 50), and are idempotent. The retry delay is exponential from 5 seconds, capped at 5 minutes; resolution ends after 8 attempts or 24 hours, whichever occurs first, as terminal `REJECTED` with `REFERENCE_NOT_FOUND`. Arrival of the original reference followed by a sweep performs exactly one reversal transactionally.

Run the CP2B-2 gates against disposable PostgreSQL and LocalStack services. `docker compose up -d postgres localstack` provides `SQS_ENDPOINT=http://localstack:4566` within Compose; host callers use `http://127.0.0.1:4566`. The sample credentials (`test`/`test`) are LocalStack-only placeholders, not production credentials. CP2B-2 persists outbox messages but intentionally does not implement an SQS publisher, consumer, acknowledgement loop, or DLQ behavior; those remain CP3.

## CP3A SQS transactional inbox consumer

CP3A adds the `WagerTransactionRequested` FIFO consumer only; it deliberately does **not** publish the outbox. Start the worker as a separate process after the Compose dependencies are healthy:

```bash
bun run sqs-consumer                 # development: src/sqs-consumer.ts
bun run dist/sqs-consumer.js         # container: emitted by `nest build`
bun run scripts/cp3a-sqs-consumer.ts # legacy wrapper kept for the existing suites
```

The worker uses `wager-transactions.fifo` by default. Configure `WAGER_TRANSACTIONS_QUEUE_URL` for an isolated queue (the preferred integration-test path), or `WAGER_TRANSACTIONS_QUEUE_NAME` to resolve a named queue. `SQS_CONSUMER_CONCURRENCY` defaults to `4`; `SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS` defaults to `5`. The worker long-polls SQS and uses each delivery's current `ReceiptHandle` for deletion.

A producer must send a FIFO `MessageGroupId` equal to `walletId`; this reduces same-wallet broker reordering but is **not** the financial correctness boundary. PostgreSQL still serializes and validates wallet effects. The body is a strict JSON envelope:

```json
{
  "messageId": "logical-delivery-id",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-10-29T03:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "external-1",
    "idempotencyKey": "financial-key-1",
    "playerId": "uuid",
    "walletId": "uuid",
    "roundId": "round-1",
    "gameId": "game-1",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

`messageId` above—not SQS's transport `MessageId`—is the durable inbox identity. The primary key remains `(consumer_name, message_id)`. The consumer canonicalizes and hashes this application envelope, inserts/verifies the inbox row, runs the existing financial processor, and persists wallet/transaction/ledger/outbox effects in **one PostgreSQL transaction**. A matching redelivery is an idempotent replay and receives an ACK without new effects; a changed payload for the same logical `messageId` is a permanent inbox conflict: it produces no movement, leaves the original inbox row immutable, and is not ACKed.

ACK is `DeleteMessage` strictly after successful database commit. `PROCESSED`, durable `REJECTED`, and `PENDING_REFERENCE` are all commit-success outcomes and are ACKed. PostgreSQL/deadlock failures, permanent inbox conflicts, and invalid envelopes remain unacknowledged so the FIFO queue's native redrive policy (`maxReceiveCount=5`) moves them to `wager-transactions-dlq.fifo`. Permanent inbox conflicts emit JSON with only the logical `messageId` and stable `inbox_payload_conflict` reason. No in-memory deduplication is used.

On `SIGTERM`/`SIGINT`, the worker stops accepting polls, aborts an outstanding long poll, waits for already-running deliveries, then closes NestJS resources. It never ACKs before a commit; a crash after commit and before ACK is recovered by the inbox on the next delivery.

Run all checks against a disposable PostgreSQL database and isolated disposable FIFO queues—never the development database or shared queue:

```bash
bun run typecheck
bun run build
bun run test
```

`test/integration/sqs-inbox.spec.ts` uses real PostgreSQL and LocalStack to cover valid delivery, redelivery, crash-after-commit-before-ACK across two worker processes, pre-commit rollback, durable rejection, pending reference/recovery, real redrive to FIFO DLQ, three worker processes, and HTTP-equivalent-to-SQS financial replay. The `CP3A_TEST_CRASH_AFTER_COMMIT_BEFORE_ACK` and `SQS_CONSUMER_EXIT_AFTER_ACKS` environment variables are test-only process controls; they are not application APIs.

## CP3B transactional outbox publisher

CP3B publishes the financial outbox to the FIFO queue `wager-events.fifo`. Start it as a separate process only after PostgreSQL and SQS are healthy:

```bash
bun run outbox-publisher                 # development: src/outbox-publisher.ts
bun run dist/outbox-publisher.js         # container: emitted by `nest build`
bun scripts/cp3b-outbox-publisher.ts     # legacy wrapper kept for the existing suites
```

The default mode is continuous: an empty outbox is temporary, so the process waits and continues claiming when a later committed event becomes eligible. `SIGINT` and `SIGTERM` stop new claims, allow work already claimed to finish its current controlled path, then close SQS and MikroORM resources. For a bounded batch/CI invocation, set `OUTBOX_PUBLISHER_STOP_WHEN_IDLE=true`; it exits only after no eligible row remains and no currently claimed batch is still being processed.

Configuration defaults are intentionally production-safe:

| Variable | Default | Meaning |
| --- | --- | --- |
| `WAGER_EVENTS_QUEUE_NAME` | `wager-events.fifo` | FIFO queue name when a URL is not supplied |
| `WAGER_EVENTS_QUEUE_URL` | unset | Explicit queue URL; preferred for isolated tests |
| `OUTBOX_PUBLISHER_BATCH_SIZE` | `20` | Maximum ready rows claimed in one iteration (1–100) |
| `OUTBOX_PUBLISHER_LEASE_MS` | `30000` | Durable lease duration for a claim |
| `OUTBOX_PUBLISHER_RETRY_BASE_MS` | `1000` | Retry base delay and idle wait cap |
| `OUTBOX_PUBLISHER_RETRY_MAX_MS` | `60000` | Maximum exponential retry delay |

Every claim is selected in a short PostgreSQL transaction with `FOR UPDATE SKIP LOCKED`. The publisher records a lease and unique fencing token, commits that claim, and only then calls SQS; it never holds a SQL transaction open over the network call. Confirmation and retry updates require the same `(id, claim_token)` fencing pair, so an expired owner cannot overwrite a newer claim. A failure schedules a persisted exponential delay with bounded jitter, increments `attempts`, and releases the lease. A crash after claim is recovered after lease expiry; a crash after SQS accepts a message but before `published_at` is stored is recovered by republishing the exact persisted envelope with the same `eventId`.

Per-wallet ordering is database-enforced by persisted `enqueue_sequence`: a row is claimable only when no older unpublished row for its `walletId` exists. SQS receives `MessageGroupId = walletId` and `MessageDeduplicationId = eventId`. The published body is the stored event envelope; the publisher does not regenerate `eventId`, `occurredAt`, or monetary values, and does not reconstruct an event from current wallet balance.

Delivery is **at least once**, not exactly once. A post-acceptance crash can produce a duplicate delivery despite FIFO's five-minute deduplication window; downstream consumers must durably deduplicate on the stable application `eventId`. FIFO ordering and broker deduplication reduce transport disorder and duplication but do not replace durable consumer idempotency.

Run CP3B validation only against a disposable isolated PostgreSQL database and disposable LocalStack FIFO queues:

```bash
bun run typecheck
bun run build
bun test test/integration/sqs-outbox-publisher.spec.ts --timeout 60000
bun run test
```

The CP3B integration suite uses real PostgreSQL and LocalStack for committed-row publication, pre-commit exclusion, concurrent publishers, crash recovery before and after SQS acceptance, persisted retry, wallet ordering, restart recovery, exact envelope preservation, and durable downstream `eventId` deduplication. Migration reversibility (`UP → DOWN → UP`) must likewise be checked only on a disposable database; never run migration down against a development or production database.

## CP4 ledger, reconciliation, and pending-reference sweep

`GET /wallets/:walletId/ledger?limit=50&cursor=...` reads only persisted `wallet_ledger_entries`. It orders ascending by `(created_at, id)` and uses an opaque base64url cursor containing version `1`, wallet identity, and the last keyset position. A cursor for a different wallet or a malformed cursor returns `400`; a missing wallet returns `404`. `limit` defaults to `50` and must be an integer from `1` to `100`. Monetary fields are decimal strings.

Pagination is forward keyset pagination, not a multi-request snapshot: rows committed after a page can appear in a later page only if their `(created_at, id)` sorts after the cursor. Rows before or equal to the cursor are never repeated. Consumers requiring a snapshot must retain their own upper-bound cursor or query a stable reporting replica.

`POST /wallets/:walletId/reconciliation` locks the wallet for a consistent read and reconstructs its balance from persisted ledger rows (`CREDIT` adds and `DEBIT` subtracts) with PostgreSQL `numeric` and `decimal.js`. It returns stored/calculated balances and signed difference as two-decimal strings. It never repairs a divergence; it emits a structured `wallet_reconciliation_inconsistent` log for operational alerting.

Start the pending-reference runner separately — from sources in development, or from the compiled artifact the production image ships:

```bash
bun run pending-reference-sweeper      # development: src/pending-reference-sweeper.ts
bun run dist/pending-reference-sweeper.js   # container: emitted by `nest build`
```

It invokes the existing durable `reprocessPendingReferences` algorithm at `PENDING_REFERENCE_SWEEP_INTERVAL_MS` (default `5000`) in batches controlled by `PENDING_REFERENCE_SWEEP_BATCH_SIZE` (default `50`). Its in-process overlap guard prevents local overlap; `FOR UPDATE SKIP LOCKED` in the existing persistence algorithm makes multiple runner processes safe. The Docker Compose service `pending-reference-sweeper` runs it from the same production image as the API. SIGINT/SIGTERM prevents the next batch and closes the database connection after the active batch completes; the runner logs `pending_reference_sweeper_started`, `pending_reference_sweep_completed`, `pending_reference_sweeper_stopping` and `pending_reference_sweeper_stopped` as structured JSON lines.

## CP4 distributed Prometheus observability

`GET /metrics` on the API returns Prometheus text format `0.0.4`. It is an operational endpoint, not a public provider API: keep it behind the internal network, gateway policy, or scraper allowlist. It never includes credentials, monetary values, payload bodies, or identifier-valued labels.

Every worker has an independent optional metrics listener. Set `WORKER_METRICS_PORT` when starting a single worker; the listener provides `GET /metrics` and `GET /health/live`. In Compose the listeners are internal-only (no host `ports` mapping): `sqs-consumer:9465`, `outbox-publisher:9466`, and `pending-reference-sweeper:9467`. The API remains `app:3000/metrics`. Scrape these targets from a monitoring process on the Compose network; do not expose them indiscriminately on a public ingress.

| Family | Metric(s) | Source and scope |
| --- | --- | --- |
| transaction status/failure | `wagering_transactions_state`, `wagering_transaction_failures_state` | read-only PostgreSQL aggregates (`global_postgresql`) |
| idempotency and outcomes | `wagering_duplicate_submissions_total`, `wagering_wager_outcomes_total` | process-local counters |
| retries and SQL conflicts | `wagering_retries_total`, `wagering_lock_conflicts_total` | process-local counters; reasons are bounded (`40001`, `40P01`, flow names) |
| outbox | `wagering_outbox_messages_state`, `wagering_outbox_lag_seconds` | read-only PostgreSQL aggregates (`global_postgresql`) |
| SQS/DLQ | `wagering_sqs_queue_messages_approximate`, `wagering_dlq_messages_approximate` | SQS queue attributes (`approximate_sqs`); values are approximate by broker contract |
| latency | `wagering_processing_duration_seconds` | process-local seconds histogram with cumulative buckets, `_sum`, and `_count` |
| reconciliation | `wagering_reconciliation_divergent_wallets_state`, `wagering_reconciliation_checks_total` | PostgreSQL aggregate plus process-local result counter |

`process_local` counters and histograms belong only to the process that handled work; aggregate those series in Prometheus, never by making the API pretend to be a global worker view. `global_postgresql` gauges are recomputed read-only on each scraper target and must not be summed across targets. `approximate_sqs` series report SQS `Approximate*` attributes and may lag broker reality. The pending-reference sweeper intentionally has no SQS/DLQ collector because it does not consume a queue.

If the SQS collector is unavailable, the API continues exposing PostgreSQL and process-local series and increments `wagering_metrics_collection_errors_total{collector="sqs"}`. A collection failure is not reported as a healthy zero queue depth. Worker listener startup fails before the worker begins processing if its configured port cannot bind; absence of `WORKER_METRICS_PORT` keeps the listener disabled.

Example local inspection (use a disposable environment and an internal address):

```bash
curl http://127.0.0.1:3000/metrics
curl http://sqs-consumer:9465/health/live
curl http://outbox-publisher:9466/metrics
curl http://pending-reference-sweeper:9467/metrics
```

For a focused observability gate, run `bun test test/integration/observability-metrics.spec.ts` and `bun test test/integration/worker-metrics-server.spec.ts` after `bun run build`; all financial and SQS integration tests still require isolated PostgreSQL and LocalStack.

### CP4 closure: causal metric evidence M1-M8

Every metric family above is closed by a test that proves *causality* — a real operation is executed and the exposition is compared before and after — never by asserting that a series merely exists:

| Evidence | Test | What the real operation is |
| --- | --- | --- |
| M1 | `test/integration/cp4-r3b1-causal-metrics.spec.ts` | BET/WIN/REFUND plus a durable `INSUFFICIENT_BALANCE` rejection move the PostgreSQL status/failure gauges with no extra financial movement |
| M2 | same file | idempotent replay and payload conflict move only the API-local duplicate counters and create no ledger or outbox row |
| M3 | same file | a real SQS failure, a persisted retry and a successful publication bracket the publisher counters and drive `wagering_outbox_lag_seconds` positive then back to `0` |
| M4 | same file | a real FIFO redelivery advances `ApproximateReceiveCount`, moves the consumer-local retry counter and the DLQ gauge read from `GetQueueAttributes` |
| M5 | `test/integration/financial-atomicity.spec.ts` | PostgreSQL raises real `40001`/`40P01`; `wagering_lock_conflicts_total` separates `retried` from `exhausted` in the same process and no financial effect is duplicated |
| M6 | `test/integration/cp4-r3b1-causal-metrics.spec.ts` | one real wager adds exactly one `wagering_processing_duration_seconds` sample; buckets stay cumulative, `le="+Inf"` equals `_count`, and the value is seconds bounded by the HTTP round trip |
| M7 | same file | a controlled divergence injected only in the disposable database is reported by `POST /wallets/:id/reconciliation` and by `wagering_reconciliation_divergent_wallets_state`, is never repaired automatically, and is restored only by the test's own mechanism |
| M8 | same file | the network path to real LocalStack is cut for one API process: `wagering_metrics_collection_errors_total{collector="sqs"}` grows, the `approximate_sqs` series are absent instead of a fabricated zero, the PostgreSQL scope keeps answering, and restoring connectivity brings the real queue depth back and freezes the error counter |

Run the whole closure with `bun run test` (the suite discovers M1-M8 automatically) against the Compose `postgres` and `localstack` services. M8 needs no privileged Docker access: it proxies the real SQS endpoint and closes that proxy, so the outage and the recovery are real network events against real LocalStack.

Migration reversibility is checked as `UP → DOWN → UP` **only** on a throwaway database, never against development or production data:

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d postgres -c 'create database cp4_migration_probe'
DATABASE_URL=postgresql://.../cp4_migration_probe bun run src/migrate.ts   # UP, DOWN to 0, UP again
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d postgres -c 'drop database cp4_migration_probe'
```

Do not use the MikroORM schema generator (`schema:update`/`getUpdateSchemaSQL`) on this project. The durable guarantees live in hand-written SQL that entity metadata cannot express — check constraints, partial and expression indexes, composite identity foreign keys and the immutability/version triggers — so the generator proposes dropping them. Migrations are the only supported path for schema change.
