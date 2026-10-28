import { createHash, randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import { LockMode, type EntityManager, MikroORM } from '@mikro-orm/core';

import {
  FinancialConflictError,
  FinancialNotFoundError,
  type FinancialProcessingPort,
  type OpenWalletCommand,
  type ProcessBetCommand,
  type ProcessWagerResult,
  type TransactionView,
  type WalletView,
} from '../../application/financial-contracts.js';
import {
  type EventContext,
  IntegrationEvent,
  WalletBalanceChanged,
  WagerTransactionProcessed,
  WagerTransactionRejected,
} from '../../application/integration-events.js';
import { DomainRuleError } from '../../domain/domain-error.js';
import { Money, type MoneyProps } from '../../domain/money.js';
import { Wallet } from '../../domain/wallet.js';
import { LedgerDirection } from '../../domain/wallet-ledger-entry.js';
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

function payloadHash(command: ProcessBetCommand): string {
  const businessPayload = {
    amount: command.money.amount,
    currency: command.money.currency,
    externalTransactionId: command.externalTransactionId,
    gameId: command.gameId,
    kind: command.kind,
    playerId: command.playerId,
    providerId: command.providerId,
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

/**
 * PostgreSQL deadlock (40P01) and serialization failure (40001) are transient: the whole
 * unit of work is rolled back by PostgreSQL and no external effect has been produced, so a
 * bounded retry of the complete transaction cannot duplicate a debit. Definitive business
 * or uniqueness violations are never retried.
 */
const TRANSIENT_SQLSTATES = new Set(['40001', '40P01']);
const MAX_BET_ATTEMPTS = 3;

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
export type UnitOfWorkStage = 'wallet-and-transaction-flushed' | 'before-commit';

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
    this.validateBet(command);
    const hash = payloadHash(command);

    for (let attempt = 1; ; attempt += 1) {
      try {
        return await this.runBetUnitOfWork(command, hash);
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

  private async runBetUnitOfWork(command: ProcessBetCommand, hash: string): Promise<ProcessWagerResult> {
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
        const wallet = Wallet.rehydrate({
          id: walletRecord.id,
          playerId: walletRecord.playerId,
          currency: walletRecord.currency,
          balance: Money.from({ amount: walletRecord.balance, currency: walletRecord.currency }),
          version: walletRecord.version,
          createdAt: walletRecord.createdAt,
          updatedAt: walletRecord.updatedAt,
        });
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
          kind: WagerTransactionKind.Bet,
          money,
          createdAt: now,
        });

        if (wallet.balance.isLessThan(money)) {
          transaction.reject(FailureCode.InsufficientBalance);
          const rejected = this.persistTransaction(em, transaction, wallet.balance.toJSON().amount, now);
          this.persistOutbox(em, WagerTransactionRejected.from(
            transaction.id,
            wallet.id,
            wallet.balance.toJSON(),
            FailureCode.InsufficientBalance,
            this.eventContext(command, transaction.id, now),
          ));
          await em.flush();
          return this.toTransactionView(rejected, false);
        }

        transaction.markProcessed(undefined, now);
        const ledger = wallet.debit({
          amount: money,
          transactionId: transaction.id,
          ledgerEntryId: randomUUID(),
          at: now,
        });
        const processed = this.persistTransaction(em, transaction, wallet.balance.toJSON().amount, now);
        walletRecord.balance = wallet.balance.toJSON().amount;
        walletRecord.version = wallet.version;
        walletRecord.updatedAt = now;
        await em.flush();
        await this.interceptUnitOfWork('wallet-and-transaction-flushed', em);

        em.persist(em.create(WalletLedgerEntryRecord, {
          id: ledger.id,
          walletId: ledger.walletId,
          transactionId: ledger.transactionId,
          direction: LedgerDirection.Debit,
          amount: ledger.money.toJSON().amount,
          currency: ledger.money.currency,
          balanceBefore: ledger.balanceBefore.toJSON().amount,
          balanceAfter: ledger.balanceAfter.toJSON().amount,
          createdAt: now,
        }));
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

  private async resolveUniqueViolation(
    command: ProcessBetCommand,
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

  private validateBet(command: ProcessBetCommand): void {
    assertNonBlank(command.idempotencyKey, 'Idempotency-Key');
    assertNonBlank(command.providerId, 'providerId');
    assertNonBlank(command.externalTransactionId, 'externalTransactionId');
    assertUuid(command.walletId, 'walletId');
    assertUuid(command.playerId, 'playerId');
    assertNonBlank(command.roundId, 'roundId');
    assertNonBlank(command.gameId, 'gameId');
    if (command.kind !== 'BET') {
      throw new DomainRuleError('INVALID_TRANSACTION', 'Only BET is supported in CP2B-1');
    }
    if (!Money.from(command.money).isPositive()) {
      throw new DomainRuleError('INVALID_TRANSACTION', 'BET amount must be positive');
    }
  }

  private persistTransaction(
    em: EntityManager,
    transaction: WagerTransaction,
    resultingBalance: string,
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
      idempotentReplay,
    };
  }
}

export { canonicalJson, isUuid, payloadHash };
