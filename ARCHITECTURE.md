# Architecture — CP2A

## Scope boundary

CP2A adds a pure financial domain and a PostgreSQL persistence schema to the CP1 executable foundation. It does **not** implement financial HTTP endpoints, `ProcessWager`, inbox consumption, outbox publication, scheduled reference retry, reconciliation, or a multi-instance concurrency coordinator. Those workflows belong to CP2B.

The existing health module remains operational infrastructure. It is not evidence that financial endpoints or workers exist.

## Module direction

```text
interfaces (HTTP) -> application -> domain
interfaces (HTTP) -> infrastructure (composition/readiness adapters)
infrastructure -> domain ports (when CP2B introduces ports)
```

The CP2A domain remains dependency-free: it imports neither NestJS nor MikroORM. Persistence contains record classes and `EntitySchema` mappings rather than annotations on domain objects.

## Domain model

- `Money` wraps `decimal.js`, is immutable, exposes money as `{ amount: string, currency: string }`, and rejects non-string, negative, scientific, non-finite, and scale-overflow input contracts. Arithmetic rejects mixed currencies.
- `Wallet` has a private constructor and static `open`/`rehydrate`. It starts at version `1`; its `credit` and `debit` operations produce immutable ledger entries and advance version only when balance changes.
- `WagerTransaction` has explicit states. `PROCESSED`, `REJECTED`, and `FAILED` are terminal. `OPENING` is internal, and `REFUND`/`ROLLBACK` require compatible processed references with equal amount and currency.
- `WalletLedgerEntry` has no mutable operations and validates `balanceBefore ± amount = balanceAfter`.
- `rehydrate` reconstructs persisted state without applying business-transition validation again.

`OPENING` can be represented with an internal transaction whose external id is null and a matching initial `CREDIT` ledger entry.

## Money and MikroORM mapping

Monetary persistence uses PostgreSQL `numeric(20,2)`. Every monetary `EntitySchema` property uses `DecimalType('string')`, preserving exact values through flush, clear, and reload. The domain never depends on MikroORM decimal types.

Five schemas are declared explicitly in both `src/infrastructure/persistence/mikro-orm.config.ts` (CLI) and `PersistenceModule` (NestJS): wallets, wager transactions, ledger entries, inbox messages, and outbox messages. NestJS obtains the same schemas through `InfrastructureModule -> PersistenceModule`, rather than relying solely on `autoLoadEntities`.

## Migration and database invariants

`Migration20261028140000` is transactional, all-or-nothing, and reversible.

| Object | Database guarantee |
| --- | --- |
| `wallets` | `balance >= 0`; unique `(player_id, currency)`; currency/timestamp checks; `version` default `1`; composite identity target for transaction ownership |
| `wager_transactions` | idempotency-key uniqueness; provider/external-id partial uniqueness; wallet/player/currency FK; reference FK; kind/status/reference/processed-time checks |
| `wallet_ledger_entries` | wallet and transaction FKs; one row per wallet/transaction; positive amount/non-negative balances; debit/credit arithmetic check |
| `inbox_messages` | `(consumer_name, message_id)` primary key |
| `outbox_messages` | non-negative attempts and partial pending-publication index |

Triggers provide the parts that check constraints cannot express:

1. `wallets_version_balance_guard` rejects a version change without a balance change and rejects any balance change that does not increment the version exactly once.
2. `wallet_ledger_entries_no_update` and `wallet_ledger_entries_no_delete` make the ledger immutable.
3. `wallet_ledger_entries_processed_transaction_guard` allows ledger insertion only after the related transaction is `PROCESSED`.
4. `wager_transactions_terminal_status_guard` prevents terminal status transitions.

The version column is intentionally **not** marked as MikroORM's automatic `version: true`: that behavior increments on a timestamp-only ORM update and violates the wallet rule. CP2A keeps it as a persisted concurrency token guarded by PostgreSQL. CP2B must choose and implement the operational concurrency protocol (for example, a row lock or compare-and-swap/retry) around that token.

## Reversal policy and ordering

The partial unique index on `(reference_transaction_id, kind)` blocks a second processed reversal of the **same kind**. CP2A deliberately permits one processed `REFUND` and one processed `ROLLBACK` for a reference because the challenge leaves this overlap ambiguous; the application policy must resolve it in CP2B before it is exposed externally.

For a balance-affecting operation, CP2B must persist and transition the transaction to `PROCESSED` before inserting the ledger entry, inside one SQL transaction. The current trigger validates this ordering. It does not itself guarantee that the wallet materialized balance equals the complete ledger: that cross-row invariant requires the CP2B transaction coordinator.

## Concurrency and transaction plan for CP2B

The unit of concurrency is `walletId`. CP2B must perform wallet balance update, version advance, ledger entry, inbox record when applicable, outbox record, and terminal transaction state in one database transaction, using a wallet-scoped row lock or an equivalent conditional update with bounded retry. Broker FIFO semantics remain an optimization, not a correctness mechanism. No global lock or in-memory deduplication is acceptable.

## Existing CP1 infrastructure

MikroORM connects PostgreSQL; SQS uses AWS SDK v3 against LocalStack locally. Liveness is process-only. Readiness runs a real `SELECT 1` and `ListQueues`. The CP1 Compose stack defines FIFO queues and redrive configuration, but CP2A has not implemented message processing or publication.

## Verification boundary

The CP2A integration suite runs on an isolated PostgreSQL database. It proves migration `UP/DOWN/UP`, all five tables, SQLSTATE failures for required uniqueness/check/FK/trigger paths, legal transaction transitions, ordered ledger creation, reversal uniqueness, OPENING representation, wallet-version behavior, and MikroORM numeric precision after clear/reload. It does not prove CP2B worker, HTTP, recovery, or race behavior.
