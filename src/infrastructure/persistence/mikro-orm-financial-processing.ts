import { createHash, randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import { LockMode, type EntityManager, MikroORM } from '@mikro-orm/core';

import { Decimal } from 'decimal.js';

import {
  FinancialConflictError,
  FinancialNotFoundError,
  InboxPayloadConflictError,
  InvalidLedgerCursorError,
  type FinancialProcessingPort,
  type InboxMessageCommand,
  type LedgerEntryView,
  type OpenWalletCommand,
  type PendingReferenceSweepOptions,
  type PendingReferenceSweepResult,
  type ProcessBetCommand,
  type ProcessWagerCommand,
  type ProcessWagerResult,
  type TransactionView,
  type WalletLedgerPage,
  type WalletReconciliation,
  type WagerKindInput,
  type WalletView,
} from '../../application/financial-contracts.js';
import {
  type EventContext,
  IntegrationEvent,
  WalletBalanceChanged,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
} from '../../application/integration-events.js';
import {
  duplicateSubmissions,
  lockConflicts,
  observeDuration,
  pendingReferenceSweeps,
  reconciliationChecks,
  retries,
  wagerOutcomes,
} from '../observability/wagering-metrics.js';
import { sqlState, transientConflict } from './transient-errors.js';
import { DomainRuleError } from '../../domain/domain-error.js';
import { Money, type MoneyProps } from '../../domain/money.js';
import { Wallet } from '../../domain/wallet.js';
import { LedgerDirection, type WalletLedgerEntry } from '../../domain/wallet-ledger-entry.js';
import {
  FailureCode,
  TransactionOrigin,
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/wager-transaction.js';
import {
  InboxMessageRecord,
  OutboxMessageRecord,
  WalletLedgerEntryRecord,
  WalletRecord,
  WagerTransactionRecord,
} from './financial-entities.js';

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(',')}]`;
  }

  const object = value as Record<string, unknown>;
  return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`).join(',')}}`;
}

function payloadHash(command: ProcessWagerCommand): string {
  const businessPayload = {
    amount: command.money.amount,
    currency: command.money.currency,
    externalTransactionId: command.externalTransactionId,
    gameId: command.gameId,
    kind: command.kind,
    playerId: command.playerId,
    providerId: command.providerId,
    ...(command.kind === 'REFUND' || command.kind === 'ROLLBACK'
      ? { referenceExternalTransactionId: command.referenceExternalTransactionId }
      : {}),
    roundId: command.roundId,
    walletId: command.walletId,
  };
  return createHash('sha256').update(canonicalJson(businessPayload)).digest('hex');
}

function assertNonBlank(value: string, name: string): void {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new DomainRuleError('INVALID_TRANSACTION', `${name} is required`);
  }
}

function assertUuid(value: string, name: string): void {
  assertNonBlank(value, name);
  if (!isUuid(value)) {
    throw new DomainRuleError('INVALID_TRANSACTION', `${name} must be a UUID`);
  }
}

function uniqueViolation(error: unknown): boolean {
  return sqlState(error) === '23505';
}

function constraintName(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) {
    return undefined;
  }
  const candidate = error as { constraint?: unknown; cause?: { constraint?: unknown } };
  if (typeof candidate.constraint === 'string') {
    return candidate.constraint;
  }
  return typeof candidate.cause?.constraint === 'string' ? candidate.cause.constraint : undefined;
}

/**
 * PostgreSQL deadlock (40P01) and serialization failure (40001) are transient: the whole
 * unit of work is rolled back by PostgreSQL and no external effect has been produced, so a
 * bounded retry of the complete transaction cannot duplicate a debit. Definitive business
 * or uniqueness violations are never retried. The classification itself lives in
 * `transient-errors.ts` because the HTTP layer needs exactly the same definition to decide
 * between 503 and 500.
 */
const MAX_BET_ATTEMPTS = 3;

/**
 * CP2B-2 out-of-order reference policy.
 *
 * A reversal whose reference has not arrived yet is parked as PENDING_REFERENCE and retried
 * by `reprocessPendingReferences`. The retry schedule is bounded on two independent axes, so
 * nothing can wait forever: the attempt budget and the age TTL, whichever is reached first.
 * Backoff doubles from the base delay and is capped, which keeps the sweep away from
 * aggressive polling while staying well inside the TTL.
 */
export const REFERENCE_BACKOFF_BASE_MS = 5_000;
export const REFERENCE_BACKOFF_MAX_MS = 300_000;
export const MAX_REFERENCE_ATTEMPTS = 8;
export const REFERENCE_TTL_MS = 24 * 60 * 60 * 1_000;
const DEFAULT_SWEEP_LIMIT = 50;

function referenceBackoffMs(attempts: number): number {
  return Math.min(REFERENCE_BACKOFF_BASE_MS * 2 ** attempts, REFERENCE_BACKOFF_MAX_MS);
}

const KIND_BY_INPUT: Readonly<Record<WagerKindInput, WagerTransactionKind>> = {
  BET: WagerTransactionKind.Bet,
  WIN: WagerTransactionKind.Win,
  LOSS: WagerTransactionKind.Loss,
  REFUND: WagerTransactionKind.Refund,
  ROLLBACK: WagerTransactionKind.Rollback,
};

const REVERSAL_KINDS: readonly WagerTransactionKind[] = [
  WagerTransactionKind.Refund,
  WagerTransactionKind.Rollback,
];

/**
 * A reference whose transaction exists but can never satisfy the reversal is a definitive
 * rejection; the domain rule code decides which failure code is persisted. Waiting states
 * are handled before this mapping, so nothing retriable reaches it.
 */
function referenceFailureCode(code: string): FailureCode {
  switch (code) {
    case FailureCode.ReferenceAmountMismatch:
      return FailureCode.ReferenceAmountMismatch;
    case FailureCode.ReferenceAlreadyReversed:
      return FailureCode.ReferenceAlreadyReversed;
    default:
      return FailureCode.ReferenceIncompatible;
  }
}

type ReferenceResolution =
  | { readonly outcome: 'pending' }
  | { readonly outcome: 'rejected'; readonly failureCode: FailureCode }
  | { readonly outcome: 'resolved'; readonly reference: WagerTransactionRecord; readonly direction: LedgerDirection };

type PendingResolutionOutcome = 'processed' | 'rejected' | 'rescheduled' | 'skipped';

/**
 * Metric outcome label for a wager result. Bounded set of lowercase words derived from the
 * persisted status — never from the amount, the wallet or any identifier.
 */
function wagerOutcomeLabel(result: ProcessWagerResult): string {
  if (result.idempotentReplay) {
    return 'idempotent_replay';
  }
  switch (result.status) {
    case WagerTransactionStatus.Processed:
      return 'processed';
    case WagerTransactionStatus.Rejected:
      return 'rejected';
    case WagerTransactionStatus.PendingReference:
      return 'pending_reference';
    default:
      return 'pending';
  }
}

/**
 * RFC 9562 identifiers: CHALLENGE.md uses UUIDv7 (`0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1`),
 * while internally generated identifiers are UUIDv4. Structure, version (1-8) and the
 * RFC variant bits are all enforced, so arbitrary strings remain rejected.
 */
function isUuid(value: string): boolean {
  return typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function asMoney(amount: string, currency: string): MoneyProps {
  return Money.from({ amount, currency }).toJSON();
}

/**
 * Deterministic interception points inside the BET unit of work. Production code never
 * fails here: `interceptUnitOfWork` is a no-op and can only be replaced by subclassing the
 * adapter, which integration tests do. No environment variable, header or request field can
 * reach it, so there is no failure backdoor in the HTTP API.
 */
export type UnitOfWorkStage =
  | 'wallet-and-transaction-flushed'
  | 'before-commit'
  | 'reference-resolution-before-commit';

@Injectable()
export class MikroOrmFinancialProcessing implements FinancialProcessingPort {
  public constructor(private readonly orm: MikroORM) {}

  protected async interceptUnitOfWork(_stage: UnitOfWorkStage, _em: EntityManager): Promise<void> {
    return Promise.resolve();
  }

  public async openWallet(command: OpenWalletCommand): Promise<WalletView> {
    assertUuid(command.playerId, 'playerId');
    const initialBalance = Money.from(command.initialBalance);
    const now = new Date();
    const walletId = randomUUID();
    const openingTransactionId = initialBalance.isPositive() ? randomUUID() : undefined;
    const openingLedgerEntryId = initialBalance.isPositive() ? randomUUID() : undefined;
    const wallet = initialBalance.isPositive()
      ? Wallet.open({
        id: walletId,
        playerId: command.playerId,
        initialBalance,
        openingTransactionId: openingTransactionId!,
        openingLedgerEntryId: openingLedgerEntryId!,
        at: now,
      })
      : Wallet.open({
        id: walletId,
        playerId: command.playerId,
        initialBalance,
        at: now,
      });

    try {
      await this.orm.em.transactional(async (em) => {
        em.persist(em.create(WalletRecord, {
          id: wallet.id,
          playerId: wallet.playerId,
          currency: wallet.currency,
          balance: wallet.balance.toJSON().amount,
          version: wallet.version,
          createdAt: now,
          updatedAt: now,
        }));
        await em.flush();

        if (!wallet.openingLedgerEntry || !openingTransactionId) {
          return;
        }

        const opening = WagerTransaction.create({
          id: openingTransactionId,
          providerId: 'INTERNAL_WALLET_OPENING',
          idempotencyKey: `opening:${wallet.id}`,
          payloadHash: createHash('sha256').update(`opening:${wallet.id}:${initialBalance.toString()}`).digest('hex'),
          walletId: wallet.id,
          playerId: wallet.playerId,
          roundId: `opening:${wallet.id}`,
          gameId: 'WALLET_OPENING',
          kind: WagerTransactionKind.Opening,
          money: initialBalance,
          createdAt: now,
          origin: TransactionOrigin.Internal,
        });
        opening.markProcessed(undefined, now);
        em.persist(em.create(WagerTransactionRecord, {
          id: opening.id,
          providerId: opening.providerId,
          referenceAttempts: 0,
          referenceNextAttemptAt: null,
          externalTransactionId: null,
          idempotencyKey: opening.idempotencyKey,
          payloadHash: opening.payloadHash,
          walletId: opening.walletId,
          playerId: opening.playerId,
          roundId: opening.roundId,
          gameId: opening.gameId,
          kind: opening.kind,
          amount: opening.money.toJSON().amount,
          currency: opening.money.currency,
          referenceTransactionId: null,
          referenceExternalTransactionId: null,
          status: opening.status,
          failureCode: null,
          processedAt: now,
          resultingBalance: wallet.balance.toJSON().amount,
          createdAt: now,
        }));
        await em.flush();

        em.persist(em.create(WalletLedgerEntryRecord, {
          id: wallet.openingLedgerEntry.id,
          walletId: wallet.openingLedgerEntry.walletId,
          transactionId: wallet.openingLedgerEntry.transactionId,
          direction: wallet.openingLedgerEntry.direction,
          amount: wallet.openingLedgerEntry.money.toJSON().amount,
          currency: wallet.openingLedgerEntry.money.currency,
          balanceBefore: wallet.openingLedgerEntry.balanceBefore.toJSON().amount,
          balanceAfter: wallet.openingLedgerEntry.balanceAfter.toJSON().amount,
          createdAt: now,
        }));
        this.persistOutbox(em, WagerTransactionProcessed.from(
          opening.id,
          wallet.id,
          wallet.balance.toJSON(),
          this.eventContext(command, wallet.id, now),
        ));
        this.persistOutbox(em, WalletBalanceChanged.from(
          wallet,
          wallet.openingLedgerEntry,
          this.eventContext(command, wallet.id, now),
        ));
        await em.flush();
      });
    } catch (error) {
      if (uniqueViolation(error)) {
        throw new FinancialConflictError('A wallet already exists for this player and currency');
      }
      throw error;
    }

    return this.toWalletView(wallet.id, wallet.playerId, wallet.balance.toJSON(), wallet.version);
  }

  public async processBet(command: ProcessBetCommand): Promise<ProcessWagerResult> {
    return this.processWager(command);
  }

  /**
   * Single entry point for every externally submitted wager transaction. BET keeps the exact
   * CP2B-1 behaviour; WIN/LOSS/REFUND/ROLLBACK reuse the same unit of work, the same
   * pessimistic wallet lock and the same durable idempotency, so there is no second financial
   * path to keep consistent.
   */
  public async processWager(command: ProcessWagerCommand): Promise<ProcessWagerResult> {
    return this.processWagerWithOptionalInbox(command);
  }

  /**
   * SQS calls this path so inbox identity, wallet/ledger changes, wager state and outbox rows
   * commit as one PostgreSQL transaction. HTTP deliberately uses the same implementation with
   * no inbox command, preserving its contract and idempotency semantics.
   */
  public async processWagerWithInbox(
    command: ProcessWagerCommand,
    inbox: InboxMessageCommand,
  ): Promise<ProcessWagerResult> {
    return this.processWagerWithOptionalInbox(command, inbox);
  }

  private async processWagerWithOptionalInbox(
    command: ProcessWagerCommand,
    inbox?: InboxMessageCommand,
  ): Promise<ProcessWagerResult> {
    return this.processWagerInstrumented(command, inbox ? 'sqs' : 'http', inbox);
  }

  /**
   * `source` identifies the entry point for the process-local metrics only; it has no effect
   * on the financial unit of work, which is byte-for-byte the same for HTTP and SQS.
   *
   * Instrumentation contract:
   * - one duration observation per *real* attempt of the unit of work (a retry adds its own
   *   observation, a durable-state re-read does not);
   * - one outcome increment per returned result, recorded where the result is produced, so a
   *   nested replay is never counted twice;
   * - a payload conflict is counted as a conflict, never as a replay.
   */
  private async processWagerInstrumented(
    command: ProcessWagerCommand,
    source: 'http' | 'sqs',
    inbox?: InboxMessageCommand,
  ): Promise<ProcessWagerResult> {
    this.validateWager(command);
    const hash = payloadHash(command);

    for (let attempt = 1; ; attempt += 1) {
      try {
        const result = await observeDuration(
          'process_wager',
          (value: ProcessWagerResult) => wagerOutcomeLabel(value),
          () => this.runWagerUnitOfWork(command, hash, inbox),
        );
        return this.recordWagerOutcome(source, result);
      } catch (error) {
        if (error instanceof InboxPayloadConflictError) {
          duplicateSubmissions.inc({ source, outcome: 'inbox_payload_conflict' });
          throw error;
        }
        if (error instanceof FinancialConflictError) {
          duplicateSubmissions.inc({ source, outcome: 'payload_conflict' });
          throw error;
        }
        if (uniqueViolation(error)) {
          if (inbox) {
            const duplicate = await this.orm.em.fork().findOne(InboxMessageRecord, {
              consumerName: inbox.consumerName,
              messageId: inbox.messageId,
            });
            if (duplicate) {
              if (duplicate.payloadHash !== inbox.payloadHash) {
                duplicateSubmissions.inc({ source, outcome: 'inbox_payload_conflict' });
                throw new InboxPayloadConflictError('Inbox messageId was already used with a different payload');
              }
              duplicateSubmissions.inc({ source, outcome: 'inbox_replay' });
              // The nested call records the outcome of the result it returns.
              return await this.processWagerInstrumented(command, source);
            }
          }
          try {
            // A durable-state lookup, not a new unit of work: no duration is observed here.
            return this.recordWagerOutcome(source, await this.resolveUniqueViolation(command, hash, error));
          } catch (resolutionError) {
            if (resolutionError instanceof FinancialConflictError) {
              duplicateSubmissions.inc({ source, outcome: 'payload_conflict' });
            }
            throw resolutionError;
          }
        }
        if (transientConflict(error)) {
          const state = sqlState(error) ?? 'unknown';
          if (attempt < MAX_BET_ATTEMPTS) {
            lockConflicts.inc({ sqlstate: state, resolution: 'retried' });
            retries.inc({
              flow: 'financial_unit_of_work',
              reason: state === '40P01' ? 'deadlock' : 'serialization_failure',
            });
            continue;
          }
          lockConflicts.inc({ sqlstate: state, resolution: 'exhausted' });
        }
        throw error;
      }
    }
  }

  private recordWagerOutcome(source: 'http' | 'sqs', result: ProcessWagerResult): ProcessWagerResult {
    const outcome = wagerOutcomeLabel(result);
    wagerOutcomes.inc({ source, outcome });
    if (result.idempotentReplay) {
      duplicateSubmissions.inc({ source, outcome: 'idempotent_replay' });
    }
    return result;
  }

  private async runWagerUnitOfWork(
    command: ProcessWagerCommand,
    hash: string,
    inbox?: InboxMessageCommand,
  ): Promise<ProcessWagerResult> {
    return this.orm.em.transactional(async (em) => {
      if (inbox) {
        const existingInbox = await em.findOne(InboxMessageRecord, {
          consumerName: inbox.consumerName,
          messageId: inbox.messageId,
        });
        if (existingInbox) {
          if (existingInbox.payloadHash !== inbox.payloadHash) {
            throw new InboxPayloadConflictError('Inbox messageId was already used with a different payload');
          }
        } else {
          const receivedAt = new Date();
          em.persist(em.create(InboxMessageRecord, {
            consumerName: inbox.consumerName,
            messageId: inbox.messageId,
            payloadHash: inbox.payloadHash,
            receivedAt,
            processedAt: receivedAt,
          }));
        }
      }
        const existing = await em.findOne(WagerTransactionRecord, { idempotencyKey: command.idempotencyKey });
        if (existing) {
          return this.replay(existing, hash);
        }

        const walletRecord = await em.findOne(WalletRecord, command.walletId, { lockMode: LockMode.PESSIMISTIC_WRITE });
        if (!walletRecord) {
          throw new FinancialNotFoundError('Wallet not found');
        }
        if (walletRecord.playerId !== command.playerId) {
          throw new DomainRuleError('INVALID_TRANSACTION', 'Wallet does not belong to player');
        }

        const afterLock = await em.findOne(WagerTransactionRecord, { idempotencyKey: command.idempotencyKey });
        if (afterLock) {
          return this.replay(afterLock, hash);
        }

        const money = Money.from(command.money);
        if (walletRecord.currency !== money.currency) {
          throw new DomainRuleError(
            'CURRENCY_MISMATCH',
            `Wallet currency ${walletRecord.currency} does not accept ${money.currency}`,
          );
        }
        const wallet = this.rehydrateWallet(walletRecord);
        const now = new Date();
        const transaction = WagerTransaction.create({
          id: randomUUID(),
          providerId: command.providerId,
          externalTransactionId: command.externalTransactionId,
          idempotencyKey: command.idempotencyKey,
          payloadHash: hash,
          walletId: command.walletId,
          playerId: command.playerId,
          roundId: command.roundId,
          gameId: command.gameId,
          kind: KIND_BY_INPUT[command.kind],
          money,
          ...(command.referenceExternalTransactionId === undefined
            ? {}
            : { referenceExternalTransactionId: command.referenceExternalTransactionId }),
          createdAt: now,
        });

        let reference: WagerTransactionRecord | undefined;
        let direction: LedgerDirection | undefined;

        if (transaction.requiresReference()) {
          const resolution = await this.resolveReference(em, transaction);
          if (resolution.outcome === 'pending') {
            return await this.persistPendingReference(em, command, transaction, now);
          }
          if (resolution.outcome === 'rejected') {
            return await this.persistRejection(em, command, transaction, wallet, resolution.failureCode, now);
          }
          reference = resolution.reference;
          direction = resolution.direction;
        } else if (transaction.affectsBalance()) {
          direction = transaction.ledgerDirectionFor();
        }

        /**
         * LOSS is terminal with no financial movement: no ledger entry, no balance change and
         * no wallet version bump, therefore no WalletBalanceChanged event either.
         */
        if (!transaction.affectsBalance()) {
          transaction.markProcessed(undefined, now);
          const settled = this.persistTransaction(em, transaction, wallet.balance.toJSON().amount, now);
          this.persistOutbox(em, WagerTransactionProcessed.from(
            transaction.id,
            wallet.id,
            wallet.balance.toJSON(),
            this.eventContext(command, transaction.id, now),
          ));
          await em.flush();
          await this.interceptUnitOfWork('before-commit', em);
          return this.toTransactionView(settled, false);
        }

        if (direction === LedgerDirection.Debit && wallet.balance.isLessThan(money)) {
          /**
           * A debit that cannot be covered is rejected, never clamped. A reversal gets its own
           * failure code: an under-funded ROLLBACK is an operational inconsistency, not a
           * player with insufficient funds for a new bet.
           */
          const failureCode = transaction.kind === WagerTransactionKind.Bet
            ? FailureCode.InsufficientBalance
            : FailureCode.ReversalWouldCauseNegativeBalance;
          return await this.persistRejection(em, command, transaction, wallet, failureCode, now);
        }

        transaction.markProcessed(reference?.id, now);
        const ledger = direction === LedgerDirection.Debit
          ? wallet.debit({ amount: money, transactionId: transaction.id, ledgerEntryId: randomUUID(), at: now })
          : wallet.credit({ amount: money, transactionId: transaction.id, ledgerEntryId: randomUUID(), at: now });
        const processed = this.persistTransaction(em, transaction, wallet.balance.toJSON().amount, now);
        walletRecord.balance = wallet.balance.toJSON().amount;
        walletRecord.version = wallet.version;
        walletRecord.updatedAt = now;
        await em.flush();
        await this.interceptUnitOfWork('wallet-and-transaction-flushed', em);

        this.persistLedgerEntry(em, ledger, now);
        this.persistOutbox(em, WagerTransactionProcessed.from(
          transaction.id,
          wallet.id,
          wallet.balance.toJSON(),
          this.eventContext(command, transaction.id, now),
        ));
        this.persistOutbox(em, WalletBalanceChanged.from(
          wallet,
          ledger,
          this.eventContext(command, transaction.id, now),
        ));
        await em.flush();
        await this.interceptUnitOfWork('before-commit', em);
        return this.toTransactionView(processed, false);
    });
  }

  /**
   * Resolution sweep for out-of-order references. Each pending transaction is claimed in its
   * own transaction with `FOR UPDATE SKIP LOCKED`, so parallel sweeps (or several instances)
   * can run without ever resolving the same pending twice and without blocking each other.
   * The clock is injected: no test has to wait in real time.
   */
  public async reprocessPendingReferences(options: PendingReferenceSweepOptions): Promise<PendingReferenceSweepResult> {
    const limit = options.limit ?? DEFAULT_SWEEP_LIMIT;
    const due = await this.orm.em.fork().find(
      WagerTransactionRecord,
      {
        status: WagerTransactionStatus.PendingReference,
        referenceNextAttemptAt: { $lte: options.now },
      },
      { orderBy: { referenceNextAttemptAt: 'asc' }, limit, fields: ['id'] },
    );

    let claimed = 0;
    let processed = 0;
    let rejected = 0;
    let rescheduled = 0;

    for (const candidate of due) {
      const outcome = await observeDuration(
        'pending_reference_resolution',
        (value: PendingResolutionOutcome) => value,
        () => this.resolvePendingReference(candidate.id, options.now),
      );
      pendingReferenceSweeps.inc({ outcome });
      if (outcome === 'skipped') {
        continue;
      }
      claimed += 1;
      if (outcome === 'processed') {
        processed += 1;
      } else if (outcome === 'rejected') {
        rejected += 1;
      } else {
        /**
         * A reschedule is a real additional attempt against the same pending row, so it is
         * counted as a retry exactly once per sweep pass. A row merely re-read and skipped
         * (claimed by another sweeper) is not a retry and is not counted as one.
         */
        retries.inc({ flow: 'pending_reference_sweep', reason: 'reference_unresolved' });
        rescheduled += 1;
      }
    }

    return { claimed, processed, rejected, rescheduled };
  }

  private async resolvePendingReference(transactionId: string, now: Date): Promise<PendingResolutionOutcome> {
    return this.orm.em.transactional(async (em) => {
      const record = await em.findOne(
        WagerTransactionRecord,
        {
          id: transactionId,
          status: WagerTransactionStatus.PendingReference,
          referenceNextAttemptAt: { $lte: now },
        },
        { lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE },
      );
      if (!record) {
        return 'skipped';
      }

      const walletRecord = await em.findOne(WalletRecord, record.walletId, { lockMode: LockMode.PESSIMISTIC_WRITE });
      if (!walletRecord) {
        throw new FinancialNotFoundError('Wallet not found');
      }

      const wallet = this.rehydrateWallet(walletRecord);
      const transaction = this.rehydrateTransaction(record);
      const money = transaction.money;
      const resolution = await this.resolveReference(em, transaction);
      let outcome: PendingResolutionOutcome;

      if (resolution.outcome === 'pending') {
        outcome = this.rescheduleOrExpire(em, record, transaction, wallet, now);
      } else if (resolution.outcome === 'rejected') {
        this.rejectPending(em, record, transaction, wallet, resolution.failureCode, now);
        outcome = 'rejected';
      } else if (resolution.direction === LedgerDirection.Debit && wallet.balance.isLessThan(money)) {
        this.rejectPending(em, record, transaction, wallet, FailureCode.ReversalWouldCauseNegativeBalance, now);
        outcome = 'rejected';
      } else {
        transaction.markProcessed(resolution.reference.id, now);
        const ledger = resolution.direction === LedgerDirection.Debit
          ? wallet.debit({ amount: money, transactionId: transaction.id, ledgerEntryId: randomUUID(), at: now })
          : wallet.credit({ amount: money, transactionId: transaction.id, ledgerEntryId: randomUUID(), at: now });

        record.status = transaction.status;
        record.referenceTransactionId = resolution.reference.id;
        record.processedAt = now;
        record.resultingBalance = wallet.balance.toJSON().amount;
        record.referenceNextAttemptAt = null;
        walletRecord.balance = wallet.balance.toJSON().amount;
        walletRecord.version = wallet.version;
        walletRecord.updatedAt = now;
        await em.flush();

        this.persistLedgerEntry(em, ledger, now);
        this.persistOutbox(em, WagerTransactionProcessed.from(
          transaction.id,
          wallet.id,
          wallet.balance.toJSON(),
          this.eventContext({}, transaction.id, now),
        ));
        this.persistOutbox(em, WalletBalanceChanged.from(
          wallet,
          ledger,
          this.eventContext({}, transaction.id, now),
        ));
        outcome = 'processed';
      }

      await em.flush();
      await this.interceptUnitOfWork('reference-resolution-before-commit', em);
      return outcome;
    });
  }

  /**
   * Still unresolved: consume one attempt and either reschedule with exponential backoff or
   * close the transaction as REJECTED once the attempt budget or the TTL is exhausted.
   */
  private rescheduleOrExpire(
    em: EntityManager,
    record: WagerTransactionRecord,
    transaction: WagerTransaction,
    wallet: Wallet,
    now: Date,
  ): PendingResolutionOutcome {
    const attempts = record.referenceAttempts + 1;
    const expired = now.getTime() - record.createdAt.getTime() >= REFERENCE_TTL_MS;
    record.referenceAttempts = attempts;

    if (attempts >= MAX_REFERENCE_ATTEMPTS || expired) {
      this.rejectPending(em, record, transaction, wallet, FailureCode.ReferenceNotFound, now);
      return 'rejected';
    }

    record.referenceNextAttemptAt = new Date(now.getTime() + referenceBackoffMs(attempts));
    return 'rescheduled';
  }

  private rejectPending(
    em: EntityManager,
    record: WagerTransactionRecord,
    transaction: WagerTransaction,
    wallet: Wallet,
    failureCode: FailureCode,
    now: Date,
  ): void {
    transaction.reject(failureCode);
    record.status = transaction.status;
    record.failureCode = failureCode;
    record.resultingBalance = wallet.balance.toJSON().amount;
    record.referenceNextAttemptAt = null;
    this.persistOutbox(em, WagerTransactionRejected.from(
      transaction.id,
      wallet.id,
      wallet.balance.toJSON(),
      failureCode,
      this.eventContext({}, transaction.id, now),
    ));
  }

  /**
   * The referenced transaction decides the outcome: absent or still non-terminal means wait,
   * terminal but incompatible means reject now, and an already reversed reference can never
   * be reversed again. Lookup is scoped by provider, so a reversal can never reach another
   * provider's transaction.
   */
  private async resolveReference(em: EntityManager, transaction: WagerTransaction): Promise<ReferenceResolution> {
    const referenceExternalId = transaction.referenceExternalTransactionId;
    if (!referenceExternalId) {
      return { outcome: 'rejected', failureCode: FailureCode.ReferenceIncompatible };
    }

    const reference = await em.findOne(WagerTransactionRecord, {
      providerId: transaction.providerId,
      externalTransactionId: referenceExternalId,
    });

    if (!reference) {
      return { outcome: 'pending' };
    }

    if (
      reference.status === WagerTransactionStatus.Pending ||
      reference.status === WagerTransactionStatus.PendingReference
    ) {
      return { outcome: 'pending' };
    }

    if (reference.status !== WagerTransactionStatus.Processed) {
      return { outcome: 'rejected', failureCode: FailureCode.ReferenceIncompatible };
    }

    const alreadyReversed = await em.findOne(WagerTransactionRecord, {
      referenceTransactionId: reference.id,
      status: WagerTransactionStatus.Processed,
      kind: { $in: REVERSAL_KINDS },
    });
    if (alreadyReversed) {
      return { outcome: 'rejected', failureCode: FailureCode.ReferenceAlreadyReversed };
    }

    try {
      return {
        outcome: 'resolved',
        reference,
        direction: transaction.ledgerDirectionFor(this.rehydrateTransaction(reference)),
      };
    } catch (error) {
      if (error instanceof DomainRuleError) {
        return { outcome: 'rejected', failureCode: referenceFailureCode(error.code) };
      }
      throw error;
    }
  }

  private async persistPendingReference(
    em: EntityManager,
    command: ProcessWagerCommand,
    transaction: WagerTransaction,
    now: Date,
  ): Promise<ProcessWagerResult> {
    transaction.markPendingReference();
    const record = this.persistTransaction(em, transaction, null, now);
    const nextAttemptAt = new Date(now.getTime() + referenceBackoffMs(0));
    record.referenceAttempts = 0;
    record.referenceNextAttemptAt = nextAttemptAt;
    this.persistOutbox(em, WagerTransactionPendingReference.from(
      transaction.id,
      transaction.walletId,
      transaction.referenceExternalTransactionId!,
      0,
      nextAttemptAt,
      this.eventContext(command, transaction.id, now),
    ));
    await em.flush();
    return this.toTransactionView(record, false);
  }

  private async persistRejection(
    em: EntityManager,
    command: ProcessWagerCommand,
    transaction: WagerTransaction,
    wallet: Wallet,
    failureCode: FailureCode,
    now: Date,
  ): Promise<ProcessWagerResult> {
    transaction.reject(failureCode);
    const rejected = this.persistTransaction(em, transaction, wallet.balance.toJSON().amount, now);
    this.persistOutbox(em, WagerTransactionRejected.from(
      transaction.id,
      wallet.id,
      wallet.balance.toJSON(),
      failureCode,
      this.eventContext(command, transaction.id, now),
    ));
    await em.flush();
    return this.toTransactionView(rejected, false);
  }

  private async resolveUniqueViolation(
    command: ProcessWagerCommand,
    hash: string,
    error: unknown,
  ): Promise<ProcessWagerResult> {
    const winner = await this.orm.em.fork().findOne(WagerTransactionRecord, { idempotencyKey: command.idempotencyKey });
    if (winner) {
      return this.replay(winner, hash);
    }
    const externalWinner = await this.orm.em.fork().findOne(WagerTransactionRecord, {
      providerId: command.providerId,
      externalTransactionId: command.externalTransactionId,
    });
    if (externalWinner) {
      throw new FinancialConflictError('Provider external transaction identifier already exists');
    }
    /**
     * Two concurrent reversals of the same reference: the partial unique index is the last
     * line of defence behind the in-transaction check, so the loser is reported as a conflict
     * instead of leaking a driver error.
     */
    if (constraintName(error)?.includes('economic_reversal')) {
      throw new FinancialConflictError('The referenced transaction was already reversed');
    }
    throw error;
  }

  public async getWalletLedger(walletId: string, cursor: string | undefined, limit: number): Promise<WalletLedgerPage> {
    if (!isUuid(walletId) || !await this.orm.em.fork().findOne(WalletRecord, walletId)) {
      throw new FinancialNotFoundError(`Wallet ${walletId} does not exist`);
    }
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
      throw new InvalidLedgerCursorError('limit must be an integer between 1 and 100');
    }

    const position = cursor === undefined ? undefined : this.decodeLedgerCursor(cursor, walletId);
    const rows = await this.orm.em.getConnection().execute(
      `select l.id::text as id, l.transaction_id::text as transaction_id, t.kind, l.direction,
              l.amount::text as amount, l.currency, l.balance_before::text as balance_before,
              l.balance_after::text as balance_after, l.created_at
         from wallet_ledger_entries l
         join wager_transactions t on t.id = l.transaction_id
        where l.wallet_id = ?
          and (?::timestamptz is null or (l.created_at, l.id) > (?::timestamptz, ?::uuid))
        order by l.created_at asc, l.id asc
        limit ?`,
      [walletId, position?.createdAt ?? null, position?.createdAt ?? null, position?.id ?? null, limit + 1],
    ) as Array<{ id: string; transaction_id: string; kind: string; direction: string; amount: string; currency: string; balance_before: string; balance_after: string; created_at: Date }>;
    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const entries: LedgerEntryView[] = page.map((row) => ({
      id: row.id,
      transactionId: row.transaction_id,
      kind: row.kind,
      direction: row.direction,
      money: { amount: new Decimal(row.amount).toFixed(2), currency: row.currency },
      balanceBefore: new Decimal(row.balance_before).toFixed(2),
      balanceAfter: new Decimal(row.balance_after).toFixed(2),
      createdAt: new Date(row.created_at).toISOString(),
    }));
    const final = page.at(-1);
    return {
      walletId,
      entries,
      hasMore,
      nextCursor: hasMore && final ? this.encodeLedgerCursor(walletId, new Date(final.created_at).toISOString(), final.id) : null,
    };
  }

  public async reconcileWallet(walletId: string): Promise<WalletReconciliation> {
    if (!isUuid(walletId)) {
      throw new FinancialNotFoundError(`Wallet ${walletId} does not exist`);
    }
    return await this.orm.em.transactional(async (em) => {
      const wallet = await em.findOne(WalletRecord, walletId, { lockMode: LockMode.PESSIMISTIC_READ });
      if (!wallet) {
        throw new FinancialNotFoundError(`Wallet ${walletId} does not exist`);
      }
      const reconciliationRows = await em.getConnection().execute(
        `select coalesce(sum(case when direction = 'CREDIT' then amount else -amount end), 0)::text as calculated_balance,
                count(*)::text as checked_entries
           from wallet_ledger_entries
          where wallet_id = ?`,
        [walletId],
      ) as Array<{ calculated_balance: string; checked_entries: string }>;
      const row = reconciliationRows[0];
      if (!row) {
        throw new Error('Reconciliation aggregate query returned no row');
      }
      const storedBalance = new Decimal(wallet.balance).toFixed(2);
      const calculatedBalance = new Decimal(row.calculated_balance).toFixed(2);
      const difference = new Decimal(storedBalance).minus(calculatedBalance).toFixed(2);
      const consistent = new Decimal(difference).isZero();
      reconciliationChecks.inc({ result: consistent ? 'consistent' : 'inconsistent' });
      if (!consistent) {
        process.stdout.write(`${JSON.stringify({ event: 'wallet_reconciliation_inconsistent', walletId, storedBalance, calculatedBalance, difference })}\n`);
      }
      return { walletId, storedBalance, calculatedBalance, difference, consistent, checkedEntries: Number(row.checked_entries) };
    });
  }

  private encodeLedgerCursor(walletId: string, createdAt: string, id: string): string {
    return Buffer.from(JSON.stringify({ v: 1, walletId, createdAt, id }), 'utf8').toString('base64url');
  }

  private decodeLedgerCursor(cursor: string, walletId: string): { createdAt: string; id: string } {
    try {
      const value = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as Record<string, unknown>;
      if (value.v !== 1 || value.walletId !== walletId || typeof value.createdAt !== 'string' || !isUuid(String(value.id)) || Number.isNaN(Date.parse(value.createdAt))) {
        throw new Error('invalid cursor payload');
      }
      return { createdAt: value.createdAt, id: String(value.id) };
    } catch {
      throw new InvalidLedgerCursorError('cursor is invalid or belongs to a different wallet');
    }
  }

  public async getWallet(walletId: string): Promise<WalletView | null> {
    if (!isUuid(walletId)) {
      return null;
    }
    const record = await this.orm.em.fork().findOne(WalletRecord, walletId);
    return record ? this.toWalletView(record.id, record.playerId, asMoney(record.balance, record.currency), record.version) : null;
  }

  public async getTransaction(transactionId: string): Promise<TransactionView | null> {
    if (!isUuid(transactionId)) {
      return null;
    }
    const record = await this.orm.em.fork().findOne(WagerTransactionRecord, transactionId);
    return record ? this.toTransactionView(record, false) : null;
  }

  public async getProviderTransaction(providerId: string, externalTransactionId: string): Promise<TransactionView | null> {
    const record = await this.orm.em.fork().findOne(WagerTransactionRecord, { providerId, externalTransactionId });
    return record ? this.toTransactionView(record, false) : null;
  }

  private validateWager(command: ProcessWagerCommand): void {
    assertNonBlank(command.idempotencyKey, 'Idempotency-Key');
    assertNonBlank(command.providerId, 'providerId');
    assertNonBlank(command.externalTransactionId, 'externalTransactionId');
    assertUuid(command.walletId, 'walletId');
    assertUuid(command.playerId, 'playerId');
    assertNonBlank(command.roundId, 'roundId');
    assertNonBlank(command.gameId, 'gameId');
    if (!(command.kind in KIND_BY_INPUT)) {
      throw new DomainRuleError('INVALID_TRANSACTION', `Unsupported transaction kind ${command.kind}`);
    }
    const kind = KIND_BY_INPUT[command.kind];
    const money = Money.from(command.money);
    if (!money.isPositive() && !(kind === WagerTransactionKind.Loss && money.isZero())) {
      throw new DomainRuleError('INVALID_TRANSACTION', `${command.kind} amount must be positive`);
    }
    const needsReference = REVERSAL_KINDS.includes(kind);
    if (needsReference) {
      assertNonBlank(command.referenceExternalTransactionId ?? '', 'referenceExternalTransactionId');
    } else if (command.referenceExternalTransactionId !== undefined) {
      throw new DomainRuleError(
        'INVALID_TRANSACTION',
        `${command.kind} must not reference another transaction`,
      );
    }
  }

  private rehydrateWallet(record: WalletRecord): Wallet {
    return Wallet.rehydrate({
      id: record.id,
      playerId: record.playerId,
      currency: record.currency,
      balance: Money.from({ amount: record.balance, currency: record.currency }),
      version: record.version,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
    });
  }

  /**
   * Rebuilds the aggregate from its row so reference compatibility and reversal direction are
   * decided by the domain rules, never by ad-hoc SQL comparisons.
   */
  private rehydrateTransaction(record: WagerTransactionRecord): WagerTransaction {
    return WagerTransaction.rehydrate({
      id: record.id,
      providerId: record.providerId,
      ...(record.externalTransactionId === null ? {} : { externalTransactionId: record.externalTransactionId }),
      idempotencyKey: record.idempotencyKey,
      payloadHash: record.payloadHash,
      walletId: record.walletId,
      playerId: record.playerId,
      roundId: record.roundId,
      gameId: record.gameId,
      kind: record.kind as WagerTransactionKind,
      money: Money.from({ amount: record.amount, currency: record.currency }),
      ...(record.referenceExternalTransactionId === null
        ? {}
        : { referenceExternalTransactionId: record.referenceExternalTransactionId }),
      status: record.status as WagerTransactionStatus,
      ...(record.referenceTransactionId === null ? {} : { referenceTransactionId: record.referenceTransactionId }),
      ...(record.failureCode === null ? {} : { failureCode: record.failureCode as FailureCode }),
      ...(record.processedAt === null ? {} : { processedAt: record.processedAt }),
      createdAt: record.createdAt,
    });
  }

  private persistLedgerEntry(em: EntityManager, ledger: WalletLedgerEntry, now: Date): void {
    em.persist(em.create(WalletLedgerEntryRecord, {
      id: ledger.id,
      walletId: ledger.walletId,
      transactionId: ledger.transactionId,
      direction: ledger.direction,
      amount: ledger.money.toJSON().amount,
      currency: ledger.money.currency,
      balanceBefore: ledger.balanceBefore.toJSON().amount,
      balanceAfter: ledger.balanceAfter.toJSON().amount,
      createdAt: now,
    }));
  }

  private persistTransaction(
    em: EntityManager,
    transaction: WagerTransaction,
    resultingBalance: string | null,
    now: Date,
  ): WagerTransactionRecord {
    const record = em.create(WagerTransactionRecord, {
      id: transaction.id,
      providerId: transaction.providerId,
      externalTransactionId: transaction.externalTransactionId ?? null,
      idempotencyKey: transaction.idempotencyKey,
      payloadHash: transaction.payloadHash,
      walletId: transaction.walletId,
      playerId: transaction.playerId,
      roundId: transaction.roundId,
      gameId: transaction.gameId,
      kind: transaction.kind,
      amount: transaction.money.toJSON().amount,
      currency: transaction.money.currency,
      referenceTransactionId: transaction.referenceTransactionId ?? null,
      referenceExternalTransactionId: transaction.referenceExternalTransactionId ?? null,
      status: transaction.status,
      failureCode: transaction.failureCode ?? null,
      processedAt: transaction.processedAt ?? null,
      resultingBalance,
      referenceAttempts: 0,
      referenceNextAttemptAt: null,
      createdAt: now,
    });
    em.persist(record);
    return record;
  }

  /**
   * One event context per emitted event: a fresh stable eventId, the caller correlationId
   * (defaulting to the aggregate it belongs to) and the optional causationId.
   */
  private eventContext(
    command: { readonly correlationId?: string; readonly causationId?: string },
    fallbackCorrelationId: string,
    occurredAt: Date,
  ): EventContext {
    return {
      eventId: randomUUID(),
      correlationId: command.correlationId ?? fallbackCorrelationId,
      ...(command.causationId === undefined ? {} : { causationId: command.causationId }),
      occurredAt,
    };
  }

  private persistOutbox(em: EntityManager, event: IntegrationEvent<unknown>): void {
    const walletId = (event.data as { readonly walletId?: unknown }).walletId;
    if (typeof walletId !== 'string' || !isUuid(walletId)) {
      throw new Error(`Integration event ${event.eventType} is missing its persisted walletId`);
    }

    em.persist(em.create(OutboxMessageRecord, {
      id: event.eventId,
      aggregateId: event.aggregateId,
      walletId,
      eventType: event.eventType,
      payload: event.toJSON(),
      occurredAt: event.occurredAt,
      attempts: 0,
      nextAttemptAt: null,
      claimToken: null,
      leaseExpiresAt: null,
      publishedAt: null,
    }));
  }

  private replay(record: WagerTransactionRecord, hash: string): ProcessWagerResult {
    if (record.payloadHash !== hash) {
      throw new FinancialConflictError('Idempotency-Key was already used with a different payload');
    }
    return this.toTransactionView(record, true);
  }

  private toWalletView(id: string, playerId: string, balance: MoneyProps, version: number): WalletView {
    return { id, playerId, balance, version };
  }

  private toTransactionView(record: WagerTransactionRecord, idempotentReplay: boolean): ProcessWagerResult {
    return {
      transactionId: record.id,
      providerId: record.providerId,
      externalTransactionId: record.externalTransactionId,
      walletId: record.walletId,
      playerId: record.playerId,
      roundId: record.roundId,
      gameId: record.gameId,
      kind: record.kind,
      status: record.status as WagerTransactionStatus,
      balance: record.resultingBalance ? asMoney(record.resultingBalance, record.currency) : null,
      failureCode: record.failureCode as FailureCode | null,
      referenceExternalTransactionId: record.referenceExternalTransactionId,
      referenceTransactionId: record.referenceTransactionId,
      idempotentReplay,
    };
  }
}

export { canonicalJson, isUuid, payloadHash };
