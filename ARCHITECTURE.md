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

CP3A introduces `SqsWagerConsumer` as a standalone Nest application context (`src/sqs-consumer.ts`, compiled to `dist/sqs-consumer.js`; `scripts/cp3a-sqs-consumer.ts` is a wrapper that imports it so the existing suites keep spawning that path against one single implementation). It resolves the configured FIFO queue, long-polls with bounded local concurrency, and gives each received delivery to the pre-existing `MikroOrmFinancialProcessing` adapter. It does not create a second financial algorithm and does not publish the outbox.

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

## CP3B transactional outbox publisher

CP3B closes the outbox boundary by publishing persisted event envelopes to `wager-events.fifo` in a standalone process. Its execution path is intentionally separate from financial processing:

```text
committed PostgreSQL transaction
  -> outbox_messages (immutable event envelope + wallet_id + enqueue_sequence)
  -> short claim transaction: FOR UPDATE SKIP LOCKED + claim_token + lease_expires_at
  -> COMMIT claim
  -> SQS SendMessage (MessageGroupId = walletId, MessageDeduplicationId = eventId)
  -> fenced confirmation: UPDATE ... WHERE id = ? AND claim_token = ?
```

The network call occurs after the claim transaction commits; no SQL transaction remains open during SQS I/O. The claim query excludes an event while any earlier unpublished `enqueue_sequence` for that same `wallet_id` remains, which supplies durable per-wallet ordering independently of equal `occurredAt` timestamps. Independent wallets may progress concurrently. The SQS FIFO group is a matching transport-ordering constraint, not the sole ordering authority.

`claim_token` is a fencing token. Both confirmation and retry scheduling include the token in their `WHERE` clause, so a prior owner cannot clear, publish, or reschedule a row that has been reclaimed after lease expiry. Retry state (`attempts`, `next_attempt_at`) is persisted and uses capped exponential backoff with jitter. A crash after claim leaves the lease for a later publisher to reclaim. A crash after SQS accepts the send but before database confirmation leaves `published_at` empty; recovery deliberately republishes the unchanged persisted envelope and stable `eventId`.

The publisher normally runs continuously. An empty claim result sleeps for a bounded interval before retrying; it does not terminate or spin in a busy loop. `OUTBOX_PUBLISHER_STOP_WHEN_IDLE=true` is the explicit bounded-run mode, used by CI and batch jobs, and exits only after an empty eligible claim. `SIGINT`/`SIGTERM` disable new claims and permit the in-flight controlled path to reach its durable outcome before resources close.

This is **at-least-once** delivery. SQS FIFO's deduplication window can reduce duplicate broker acceptance but cannot prove exactly-once publication across a post-acceptance crash. The downstream consumer must persist and enforce deduplication by application `eventId`. The publisher sends the exact persisted envelope; it never regenerates IDs/timestamps/money or derives an event from current wallet state.

The CP3B integration suite runs against disposable PostgreSQL and LocalStack resources. It validates real SQS publication, commit boundary, competing publishers, crash recovery before/after SQS acceptance, persisted retry, ordering, restart recovery, persisted envelope identity, and durable downstream `eventId` deduplication. CP3B migration reversibility is checked as `UP → DOWN → UP` only on a disposable database.

## CP4 ledger and reconciliation

The HTTP ledger projection is an immutable read of `wallet_ledger_entries`, joined to the originating transaction only to expose its kind. It never derives entries from transaction state or a current wallet balance. Keyset traversal is `(created_at, id)` ascending; the opaque versioned cursor embeds the wallet ID so a cursor cannot cross wallet boundaries. It is intentionally not a cross-request snapshot: post-cursor commits may appear on later pages while pre-cursor rows do not reappear.

Reconciliation obtains a PostgreSQL transaction, locks the wallet with a shared pessimistic lock, and aggregates that wallet's persisted ledger with exact `numeric` arithmetic. Its application serialization uses `decimal.js`, retaining two-decimal strings at the boundary. The reported `difference` is `storedBalance - calculatedBalance`. A mismatch is diagnostic only: no wallet, transaction, ledger, or outbox record is changed. The service logs `wallet_reconciliation_inconsistent` with identifiers and balances but no payload or credentials.

## CP4 pending-reference operation and security boundary

`src/pending-reference-sweeper.ts` supplies an independent operational loop for the pre-existing pending-reference resolver, compiled by `nest build` into `dist/pending-reference-sweeper.js` so the production image runs the same checked and compiled artifact as the API entrypoint. It has configurable batch size and interval, does not overlap iterations in one process, and honors SIGINT/SIGTERM. Multiple processes remain safe because durable due-row claims use `FOR UPDATE SKIP LOCKED`; retry/TTL and financial resolution remain inside the existing transactional algorithm. The system remains at-least-once across SQS and uses inbox/outbox durability for recovery, never an exactly-once claim.

### Runtime packaging

The production image ships only `dist` and `node_modules`, so every runtime process is a `src/` entrypoint compiled by `nest build`: `dist/main.js` (API), `dist/migrate.js` (schema), `dist/sqs-consumer.js` (CP3A), `dist/outbox-publisher.js` (CP3B) and `dist/pending-reference-sweeper.js` (CP4). The two files kept under `scripts/` for CP3A/CP3B are wrappers that import those modules, so the integration suites that spawn them exercise the same code the image runs; `scripts/cp2b1-process-worker.ts` stays a test-only concurrency helper.

MikroORM resolves its migration directory from the configuration module's own URL (`import.meta.url`) with `path === pathTs`. The previous cwd-relative `path` plus a distinct `pathTs` was the root cause of silent zero-migration runs inside the image: MikroORM selects `pathTs` under any TypeScript runtime, Bun is always detected as one, and the image contains no `src/`. `dist/migrate.js` applies pending migrations only — never `down` — and fails explicitly when no migration file is discovered or when migrations remain pending afterwards, so a packaging regression cannot be reported as success. Compose runs it as a one-shot job and gates the API and all three workers behind its successful completion, keeping schema application strictly before any data processing.

Monitoring topology is explicit: the API serves its own `/metrics`; each worker serves an optional process-local listener only when `WORKER_METRICS_PORT` is configured. Compose keeps `sqs-consumer:9465`, `outbox-publisher:9466`, and `pending-reference-sweeper:9467` internal to the Compose network, with `/health/live` healthchecks and no host port mapping. The API does not aggregate worker counters: process-local counters/histograms are scraped from each process and aggregated by Prometheus; PostgreSQL gauges are read-only global snapshots and must not be summed across targets; SQS queue attributes are broker-provided approximations. Metric labels exclude wallet, player, message, transaction and monetary values. A failed SQS collection is exposed as a collector error without converting queue values to healthy zeros or suppressing PostgreSQL series. Worker listener binding completes before processing begins and a bind failure terminates startup; absence of the variable keeps the listener disabled. Restart policy remains an operational deployment choice: a stopped worker delays processing without losing or double-applying effects because inbox/outbox durability, claims, fencing and `FOR UPDATE SKIP LOCKED` preserve recovery semantics.

Authentication is intentionally outside the evaluated CP4 scope. The extension point is a gateway or NestJS guard at HTTP controllers, before commands are mapped into application contracts. It must attach authenticated actor/correlation context without mutating idempotency, money, wallet locks, ledger persistence, inbox, or outbox rules. Historical migration ambiguity must be assessed operationally on a disposable database with `UP → DOWN → UP`; migration down is never run against development or production data.

### Causal observability evidence and its test mechanisms

Metric evidence is causal by construction: each of M1-M8 executes a real operation against real PostgreSQL and real LocalStack and compares the exposition before and after, so a series that exists but never moves cannot pass. Two mechanisms exist only to create the adverse condition, and both are confined to the test layer:

- **Conflict injection (M5).** A test-only processing adapter makes PostgreSQL itself raise `40001`/`40P01` at a chosen stage of the unit of work. The retry policy under test is production code; `wagering_lock_conflicts_total` must distinguish a conflict that was retried from one that exhausted the bound, and the financial snapshot must be identical before and after an exhausted attempt.
- **SQS outage injection (M8).** One additional API process is started whose only route to LocalStack is a byte-forwarding TCP proxy owned by the test. Closing the proxy is a real transport failure for every `GetQueueUrl`/`GetQueueAttributes`, and reopening it restores the real service. Nothing in production code is replaced, and the shared LocalStack container is never stopped, so the outage is real without making the rest of the stack flaky. During the outage the `approximate_sqs` series must be absent rather than zeroed, and the `global_postgresql` scope must keep answering from the database.

Divergence evidence (M7) follows the same rule in the opposite direction: the divergence is injected directly into a disposable database — and still has to satisfy the wallet version trigger, which rejects any balance change that does not increment the version exactly once — while reconciliation stays read-only. The wallet is reported as divergent on every subsequent scrape and check, is never corrected by the application, and returns to a consistent state only through the test's own injection path.

The MikroORM schema generator is not part of any supported path. The durable guarantees are hand-written SQL that entity metadata cannot represent (check constraints, partial and expression indexes, composite identity foreign keys, immutability and version triggers), so `getUpdateSchemaSQL` proposes dropping them; schema change happens exclusively through migrations, whose reversibility is verified as `UP → DOWN → UP` on a throwaway database.
