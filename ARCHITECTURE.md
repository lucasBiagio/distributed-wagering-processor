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
- `WagerTransaction` has explicit states. `PROCESSED`, `REJECTED`, and `FAILED` are terminal. `OPENING` is internal, and `REFUND`/`ROLLBACK` require compatible processed references with equal amount and currency. A `ROLLBACK` reverses BET with `CREDIT`, and WIN or REFUND with `DEBIT`; it does not recursively resolve the referenced transaction's own reference.
- `LOSS` accepts zero or positive money and never produces a ledger entry. All other transaction kinds require a positive amount.
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
| `wallets` | `balance >= 0`; unique `(player_id, currency)`; currency/timestamp checks; `version` default `1`; composite identity targets for transaction and ledger ownership |
| `wager_transactions` | idempotency-key uniqueness; provider/external-id partial uniqueness; wallet/player/currency FK; reference FK; `LOSS >= 0` while every other kind is `> 0`; processed reversals require a resolved internal reference; terminal historical-balance snapshot column; kind/status/reference/processed-time checks |
| `wallet_ledger_entries` | composite `(transaction_id, wallet_id, currency)` FK ties every entry to its transaction and composite `(wallet_id, currency)` FK ties it to the wallet; one row per wallet/transaction; positive amount/non-negative balances; debit/credit arithmetic check |
| `inbox_messages` | `(consumer_name, message_id)` primary key |
| `outbox_messages` | non-negative attempts and partial pending-publication index |

Triggers provide the parts that check constraints cannot express:

1. `wallets_version_balance_guard` rejects a version change without a balance change and rejects any balance change that does not increment the version exactly once.
2. `wallet_ledger_entries_no_update` and `wallet_ledger_entries_no_delete` make the ledger immutable.
3. `wallet_ledger_entries_transaction_consistency_guard` requires a processed transaction, prohibits `LOSS`, and validates the ledger amount and direction against the transaction (including ROLLBACK direction). Composite foreign keys reject a mismatched wallet or currency before persistence.
4. `wager_transactions_terminal_immutability_guard` rejects every update to a `PROCESSED`, `REJECTED`, or `FAILED` row, including identifiers, money, references, payload hash, failure code, and `resulting_balance`.

`resulting_balance` is nullable while a transaction is pending and must remain null for `PENDING`/`PENDING_REFERENCE`. CP2B must write the observed post-operation wallet balance in the same transition that terminalizes the transaction; after that transition the trigger preserves the historical result for idempotent replay. This schema deliberately does not update the materialized wallet balance itself.

The version column is intentionally **not** marked as MikroORM's automatic `version: true`: that behavior increments on a timestamp-only ORM update and violates the wallet rule. CP2A keeps it as a persisted concurrency token guarded by PostgreSQL. CP2B must choose and implement the operational concurrency protocol (for example, a row lock or compare-and-swap/retry) around that token.

## Reversal policy and ordering

A processed BET may have only one direct economic reversal: the partial unique index on `reference_transaction_id` spans both `REFUND` and `ROLLBACK`. A `ROLLBACK` of a processed REFUND remains valid because its reference is the REFUND, not the BET. PostgreSQL owns this policy; no cache is involved.

For a balance-affecting operation, CP2B must persist and transition the transaction to `PROCESSED` before inserting the ledger entry, inside one SQL transaction. The current trigger validates this ordering. It does not itself guarantee that the wallet materialized balance equals the complete ledger: that cross-row invariant requires the CP2B transaction coordinator.

## Concurrency and transaction plan for CP2B

The unit of concurrency is `walletId`. CP2B must perform wallet balance update, version advance, ledger entry, inbox record when applicable, outbox record, and terminal transaction state in one database transaction, using a wallet-scoped row lock or an equivalent conditional update with bounded retry. Broker FIFO semantics remain an optimization, not a correctness mechanism. No global lock or in-memory deduplication is acceptable.

## Existing CP1 infrastructure

MikroORM connects PostgreSQL; SQS uses AWS SDK v3 against LocalStack locally. Liveness is process-only. Readiness runs a real `SELECT 1` and `ListQueues`. The CP1 Compose stack defines FIFO queues and redrive configuration, but CP2A has not implemented message processing or publication.

## Verification boundary

The CP2A integration suite runs on an isolated PostgreSQL database. It proves migration `UP/DOWN/UP`, all five tables, SQLSTATE failures for required uniqueness/check/FK/trigger paths, legal transaction transitions, ordered ledger creation, reversal uniqueness, OPENING representation, wallet-version behavior, and MikroORM numeric precision after clear/reload. It does not prove CP2B worker, HTTP, recovery, or race behavior.

## CP2B-1 implementation

CP2B-1 introduces `OpenWalletUseCase` and `ProcessWagerUseCase` over a financial-processing application port. The MikroORM implementation owns transaction boundaries and domain/persistence mapping; Nest controllers only validate transport contracts and map errors. The domain still imports neither NestJS nor MikroORM.

For a BET, the adapter validates the monetary string and identity fields, canonicalizes the business payload, and calculates SHA-256 over the recursively key-sorted JSON containing `providerId`, `externalTransactionId`, `walletId`, `playerId`, `roundId`, `gameId`, `kind`, and `money`. It intentionally excludes idempotency/correlation/transport fields. A SQL transaction then checks persisted idempotency, locks the wallet with MikroORM `PESSIMISTIC_WRITE` (`SELECT ... FOR UPDATE`), checks idempotency again under the lock, rehydrates the aggregate, and commits the wallet transition, terminal transaction, exact `resulting_balance`, optional ledger entry, and outbox records together.

The wallet is the containment unit: there is no global lock or in-memory idempotency structure. PostgreSQL unique constraints remain the final cross-process protection for the idempotency key and provider/external transaction identity. A matching stored hash returns the historical transaction/snapshot; a changed hash is a conflict. Insufficient balance is a committed `REJECTED` transaction with `INSUFFICIENT_BALANCE`, unchanged wallet/version, no ledger, and a rejection outbox record—not an exception that rolls back the audit record.

Positive wallet opening persists an internal `OPENING`/`CREDIT` pair in the same transaction; zero opening creates neither. `WalletBalanceChanged` is persisted only for a balance movement. CP2B-1 persists typed event envelopes but does not publish SQS messages; publication and retry are CP3.

The CP2A migration now additionally rejects `DELETE` of terminal wager rows. This was amended before integration to `main` or production deployment. It protects rejected rows with no ledger as well as processed rows. Reversibility remains supported by dropping the trigger/function in migration down; disposable test cleanup uses migration/schema teardown rather than deleting terminal history.

## CP2B-1-R1 closure decisions

`MikroOrmFinancialProcessing` receives the global MikroORM provider by plain constructor injection. `@InjectMikroORM('default')` was wrong: `MikroOrmModule.forRoot` without a context name never registers a `default_MikroORM` token, so the Nest container could not build `FinancialHttpModule` at runtime. Type checking and compilation cannot detect this class of defect, so a real server boot is now part of the gate set.

Failure atomicity is observable without weakening production code. `interceptUnitOfWork(stage, em)` is a protected no-op with two stages inside the BET unit of work — after the wallet/transaction flush and after the ledger/outbox flush, both still before commit. Only a subclass can replace it, which the integration suite does; no runtime input reaches it.

Transient conflict handling is explicit. `40001` and `40P01` are retried up to `MAX_BET_ATTEMPTS = 3` for the whole transaction, never for a partial unit of work and never for definitive business or uniqueness violations. Retrying is safe only because the outbox is a database table: nothing is published before commit, so a retried attempt cannot emit a duplicate external effect. When the limit is exhausted the original SQLSTATE propagates as an infrastructure failure and the wallet is left exactly as it was — a transient fault must never be recorded as a financial `REJECTED`.

Lookup endpoints treat a malformed identifier as "not found" instead of letting PostgreSQL raise `22P02` as a 500. Currency incompatibility between the wallet and the requested money is a `CURRENCY_MISMATCH` domain rule error, and `Money` validation failures map to `INVALID_MONEY`, so no monetary input can surface as an unhandled server error.

## CP2B-1-R3 closure decisions

UUID validation is version-agnostic within RFC 9562. `isUuid` enforces structure, a version nibble in `1-8`, and the `[89ab]` variant bits; it is not narrowed to UUIDv4, because the challenge's own player identifiers are UUIDv7 and an internal identifier choice must not constrain an external one. Narrowing the accepted set to v4/v7 only remains possible, but it is a product decision rather than a correctness fix. `assertUuid` keeps the same predicate and the same `INVALID_TRANSACTION` mapping, so no previous rejection was weakened.

The event envelope is a typed contract, not an ad-hoc object literal. `IntegrationEvent<T>` owns `eventId`, `aggregateId`, `correlationId`, optional `causationId`, `occurredAt`, and `data`; each subclass declares `eventType` and `version`; `toJSON()` produces the serialized envelope with `occurredAt` as ISO-8601 and omits an absent `causationId`. Construction goes through static factories, and `WalletBalanceChanged.from(wallet, entry, ctx)` reads the aggregate and the ledger entry it produced, which makes `walletVersion` structurally equal to the version persisted by that unit of work instead of a value recomputed at the call site.

Envelope/row divergence is eliminated by construction rather than by assertion. `persistOutbox(em, event)` is the single writer of `outbox_messages` and takes `id`, `aggregate_id`, `event_type`, `payload`, and `occurred_at` from the event, so the column and the JSON cannot drift. `eventContext()` mints one fresh `eventId` per event, carries the caller's `correlationId` — defaulting to the aggregate identifier — and propagates `causationId` only when provided. Events are persisted inside the same financial transaction as the wallet, the terminal transaction, and the ledger, so a rollback leaves no orphan message and an idempotent replay returns before any event is created.

Correlation propagation stops at the application port in this checkpoint. Adding `X-Correlation-Id`/`X-Causation-Id` ingestion would change the HTTP contract and the observability surface described in CHALLENGE.md section 12, which belongs with the publisher work in CP3 rather than with a validation and envelope fix.

## CP2B-2 reversal and pending-reference design

CP2B-2 extends the same SQL unit of work to WIN, LOSS, REFUND, and ROLLBACK. The persistence adapter locks the wallet and resolves reversal references inside the transaction. Compatibility is validated against provider, wallet, player, currency, round, and amount before any balance mutation. Processed REFUND/ROLLBACK rows rely on PostgreSQL's direct-reference uniqueness constraint, while wallet locks and unique constraints prevent an additional economic effect under concurrent REFUND/ROLLBACK requests. A successful operation persists wallet snapshot, transaction, optional ledger row, and typed outbox events atomically; LOSS persists its terminal transaction and processed event without a balance event or ledger row.

For an out-of-order REFUND or ROLLBACK, a supplied-but-unresolved external reference produces `PENDING_REFERENCE`, not a rejection and not a financial movement. The migration adds `reference_attempts` and `reference_next_attempt_at`, constrained to pending-reference rows and indexed by due time. `reprocessPendingReferences` claims due rows with `FOR UPDATE SKIP LOCKED`, making independently running sweeps safe. Retry delays are exponential from 5 seconds and cap at 5 minutes; the record expires at 8 attempts or 24 hours, whichever comes first, as a stable `REFERENCE_NOT_FOUND` rejection. A later resolved sweep routes through the ordinary reversal workflow, preserving the same wallet/ledger/outbox atomicity and replay semantics.

CP2B-2 retains the outbox-only boundary. LocalStack remains a reproducible dependency for readiness and endpoint configuration (`SQS_ENDPOINT=http://localstack:4566` in Compose); no publisher, consumer, acknowledgement flow, or DLQ implementation is added before CP3.

## CP3A FIFO delivery, transactional inbox, and recovery

CP3A introduces `SqsWagerConsumer` as a standalone Nest application context (`scripts/cp3a-sqs-consumer.ts`). It resolves the configured FIFO queue, long-polls with bounded local concurrency, and gives each received delivery to the pre-existing `MikroOrmFinancialProcessing` adapter. It does not create a second financial algorithm and does not publish the outbox.

```text
SQS FIFO delivery (ReceiptHandle)
  -> strict WagerTransactionRequested parse
  -> processWagerWithInbox(command, { consumerName, envelope messageId, payloadHash })
       -> one PostgreSQL transaction
            inbox_messages insert/verify
            wallet lock + existing financial state transition
            wager_transactions + optional ledger + outbox rows
       -> COMMIT
  -> DeleteMessage with the delivery ReceiptHandle
```

The inbox identity is the application-envelope `messageId`, not SQS's transport `MessageId`: the latter changes on redelivery and is only diagnostic transport metadata. `(consumer_name, message_id)` is checked inside the same SQL transaction as the financial transition. Equal canonical-envelope hashes return the persisted financial result without a new effect; a divergent hash raises an `InboxPayloadConflictError`, produces no movement, leaves the original inbox row immutable, and emits a safe JSON `inbox_payload_conflict` record containing only the logical `messageId` and stable reason. This is durable cross-process deduplication; no in-memory state participates.

The consumer calls `DeleteMessage` only after `processWagerWithInbox` resolves, which means the SQL transaction has committed. A committed `PROCESSED`, durable `REJECTED`, or `PENDING_REFERENCE` result is acknowledged. A transient failure, permanent inbox conflict, or invalid envelope exits without deletion, so SQS redelivers after visibility timeout and its FIFO redrive policy moves it to `wager-transactions-dlq.fifo` after the configured receive count. A crash between commit and delete leaves an inbox record and no ACK; a new worker replays it idempotently and then deletes the new delivery receipt.

FIFO `MessageGroupId = walletId` is required for test producers to reduce broker-level same-wallet reordering, but it is not considered financial correctness. PostgreSQL wallet locking, constraints, and the transaction boundary remain the authority under concurrent workers or cross-transport replay.

Shutdown first disables acceptance and aborts the outstanding long poll, then waits for deliveries already in progress. It does not manufacture acknowledgements. Therefore an interrupted pre-commit delivery has no persisted inbox/effect and is safely redelivered; an interrupted post-commit/pre-ACK delivery follows the inbox recovery path.

The CP3A real integration suite uses disposable PostgreSQL databases plus disposable LocalStack FIFO source/DLQ queues. It covers commit/ACK ordering, duplicate delivery, forced pre-commit rollback, insufficient-balance rejection, pending reference sweep, native DLQ redrive, a deterministic crash after commit before ACK across worker processes, three independent worker processes, and HTTP-to-SQS replay. Full outbox publishing, publisher retries, and reconciliation remain reserved for CP3B.
