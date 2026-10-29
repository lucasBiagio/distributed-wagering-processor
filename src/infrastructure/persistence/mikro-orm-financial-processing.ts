import { createHash, randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import { LockMode, type EntityManager, MikroORM } from '@mikro-orm/core';

import {
  FinancialConflictError,
  FinancialNotFoundError,
  type FinancialProcessingPort,
  type OpenWalletCommand,
  type PendingReferenceSweepOptions,
  type PendingReferenceSweepResult,
  type ProcessBetCommand,
  type ProcessWagerCommand,
  type ProcessWagerResult,
  type TransactionView,
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

function sqlState(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) {
    return undefined;
  }
  const candidate = error as { code?: unknown; cause?: { code?: unknown } };
  if (typeof candidate.code === 'string') {
    return candidate.code;
  }
  return typeof candidate.cause?.code === 'string' ? candidate.cause.code : undefined;
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
 * or uniqueness violations are never retried.
 */
const TRANSIENT_SQLSTATES = new Set(['40001', '40P01']);
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

function transientConflict(error: unknown): boolean {
  const state = sqlState(error);
  return state !== undefined && TRANSIENT_SQLSTATES.has(state);
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
    this.validateWager(command);
    const hash = payloadHash(command);

    for (let attempt = 1; ; attempt += 1) {
      try {
        return await this.runWagerUnitOfWork(command, hash);
      } catch (error) {
        if (uniqueViolation(error)) {
          return await this.resolveUniqueViolation(command, hash, error);
        }
        if (transientConflict(error) && attempt < MAX_BET_ATTEMPTS) {
          continue;
        }
        throw error;
      }
    }
  }

  private async runWagerUnitOfWork(command: ProcessWagerCommand, hash: string): Promise<ProcessWagerResult> {
    return this.orm.em.transactional(async (em) => {
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
      const outcome = await this.resolvePendingReference(candidate.id, options.now);
      if (outcome === 'skipped') {
        continue;
      }
      claimed += 1;
      if (outcome === 'processed') {
        processed += 1;
      } else if (outcome === 'rejected') {
        rejected += 1;
      } else {
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
    em.persist(em.create(OutboxMessageRecord, {
      id: event.eventId,
      aggregateId: event.aggregateId,
      eventType: event.eventType,
      payload: event.toJSON(),
      occurredAt: event.occurredAt,
      attempts: 0,
      nextAttemptAt: null,
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
