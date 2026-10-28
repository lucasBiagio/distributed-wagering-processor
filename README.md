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
