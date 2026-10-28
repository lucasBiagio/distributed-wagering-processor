# Distributed Wagering Processor

CP2A delivers the framework-free financial domain and its PostgreSQL persistence boundary for the Jungle Gaming challenge. The full assignment remains unchanged in [CHALLENGE.md](./CHALLENGE.md).

> **Scope:** CP2A does not implement financial HTTP endpoints, transaction-processing use cases, SQS consumers/workers, outbox publication, reconciliation, or the CP2B concurrency workflow. Existing health endpoints remain the CP1 operational surface.

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

The Compose stack starts PostgreSQL, LocalStack SQS, and NestJS. `/health/live` only checks process liveness. `/health/ready` performs a real PostgreSQL `SELECT 1` through MikroORM and a real SQS `ListQueues`; it returns HTTP 503 if either dependency fails.

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

CP2B-2 accepts `WIN`, `LOSS`, `REFUND`, and `ROLLBACK` through the same idempotent transaction boundary as BET. WIN is one `CREDIT`; LOSS changes neither wallet nor ledger; REFUND reverses only a compatible BET with one `CREDIT`; and ROLLBACK reverses BET with `CREDIT`, or WIN/REFUND with `DEBIT`. Reversals require matching provider, wallet, player, currency, round, and amount. The partial PostgreSQL uniqueness policy permits only one direct economic reversal for a reference, including concurrent REFUND/ROLLBACK attempts. A debit that would make the wallet negative becomes terminal `REJECTED` with its dedicated failure code and no ledger movement.

A missing reversal-reference field is invalid input. A supplied reference that has not yet arrived is persisted as `PENDING_REFERENCE`: it has no wallet movement, ledger row, or `WalletBalanceChanged`, but it does receive a `WagerTransactionPendingReference` outbox event and a due retry timestamp. Sweeps use `FOR UPDATE SKIP LOCKED`, are batch-limited (default 50), and are idempotent. The retry delay is exponential from 5 seconds, capped at 5 minutes; resolution ends after 8 attempts or 24 hours, whichever occurs first, as terminal `REJECTED` with `REFERENCE_NOT_FOUND`. Arrival of the original reference followed by a sweep performs exactly one reversal transactionally.

Run the CP2B-2 gates against disposable PostgreSQL and LocalStack services. `docker compose up -d postgres localstack` provides `SQS_ENDPOINT=http://localstack:4566` within Compose; host callers use `http://127.0.0.1:4566`. The sample credentials (`test`/`test`) are LocalStack-only placeholders, not production credentials. CP2B-2 persists outbox messages but intentionally does not implement an SQS publisher, consumer, acknowledgement loop, or DLQ behavior; those remain CP3.
