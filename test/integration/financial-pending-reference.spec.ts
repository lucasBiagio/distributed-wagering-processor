import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { MikroORM, type EntityManager } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import {
  MikroOrmFinancialProcessing,
  MAX_REFERENCE_ATTEMPTS,
  REFERENCE_BACKOFF_BASE_MS,
  REFERENCE_BACKOFF_MAX_MS,
  REFERENCE_TTL_MS,
  type UnitOfWorkStage,
} from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';
import {
  FailureCode,
  WagerTransactionStatus,
} from '../../src/domain/wager-transaction.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

/**
 * CP2B-2 out-of-order references: a REFUND/ROLLBACK whose reference has not arrived yet is
 * persisted as PENDING_REFERENCE and reprocessed by a scheduled sweep with exponential
 * backoff, a bounded attempt count and a TTL. The clock is injected, so no test sleeps.
 */
let orm: MikroORM<PostgreSqlDriver>;
let processing: MikroOrmFinancialProcessing;
let sequence = 0;

const T0 = new Date('2026-11-01T12:00:00.000Z');

function player(suffix: number): string {
  return `00000000-0000-4000-8000-${String(suffix).padStart(12, '0')}`;
}

async function wallet(balance: string): Promise<{ id: string; playerId: string }> {
  sequence += 1;
  const playerId = player(sequence);
  const view = await processing.openWallet({
    playerId,
    initialBalance: { amount: balance, currency: 'BRL' },
  });
  return { id: view.id, playerId };
}

interface PendingRow {
  readonly id: string;
  readonly status: string;
  readonly failure_code: string | null;
  readonly reference_attempts: number;
  /** raw timestamptz columns come back from `execute` as ISO strings, never as Date */
  readonly reference_next_attempt_at: string | null;
  readonly reference_transaction_id: string | null;
  readonly resulting_balance: string | null;
}

async function pendingRow(id: string): Promise<PendingRow> {
  const rows = await orm.em.getConnection().execute(
    `select id::text as id, status, failure_code, reference_attempts, reference_next_attempt_at,
            reference_transaction_id::text as reference_transaction_id, resulting_balance::text as resulting_balance
       from wager_transactions where id = ?`,
    [id],
  ) as PendingRow[];
  return rows[0]!;
}

/** When the sweep may claim the pending transaction again, as a real Date. */
async function dueAt(transactionId: string): Promise<Date> {
  const row = await pendingRow(transactionId);
  return new Date(row.reference_next_attempt_at!);
}

async function walletRow(id: string): Promise<{ balance: string; version: number }> {
  const rows = await orm.em.getConnection().execute(
    `select balance::text as balance, version from wallets where id = ?`,
    [id],
  ) as Array<{ balance: string; version: number }>;
  return rows[0]!;
}

async function ledgerCount(transactionId: string): Promise<number> {
  const rows = await orm.em.getConnection().execute(
    `select count(*)::int as total from wallet_ledger_entries where transaction_id = ?`,
    [transactionId],
  ) as Array<{ total: number }>;
  return rows[0]!.total;
}

async function eventsFor(aggregateId: string): Promise<string[]> {
  const rows = await orm.em.getConnection().execute(
    `select event_type from outbox_messages where aggregate_id = ? order by occurred_at asc, event_type asc`,
    [aggregateId],
  ) as Array<{ event_type: string }>;
  return rows.map((row) => row.event_type);
}

async function pendingEventPayload(aggregateId: string): Promise<Record<string, unknown>> {
  const rows = await orm.em.getConnection().execute(
    `select payload from outbox_messages
      where aggregate_id = ? and event_type = 'WagerTransactionPendingReference'`,
    [aggregateId],
  ) as Array<{ payload: Record<string, unknown> }>;
  return rows[0]!.payload;
}

async function processedBet(target: { id: string; playerId: string }, suffix: string, amount: string, round: string): Promise<string> {
  const bet = await processing.processWager({
    idempotencyKey: `bet-${suffix}`,
    providerId: 'provider-a',
    externalTransactionId: `ext-bet-${suffix}`,
    walletId: target.id,
    playerId: target.playerId,
    roundId: round,
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: { amount, currency: 'BRL' },
  });
  expect(bet.status).toBe(WagerTransactionStatus.Processed);
  return bet.transactionId;
}

describe('CP2B-2 out-of-order references', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
    processing = new MikroOrmFinancialProcessing(orm);
  });

  beforeEach(async () => {
    // Each scenario needs an isolated pending queue: otherwise an intentionally unresolved
    // reference from a previous test is also eligible for the next controlled-clock sweep.
    await orm.em.getConnection().execute(
      'truncate table outbox_messages, wallet_ledger_entries, wager_transactions, wallets cascade',
    );
  });

  afterAll(async () => {
    await orm.getMigrator().down({ to: 0 });
    await orm.close(true);
  });

  test('a REFUND with an unknown reference is persisted as PENDING_REFERENCE without financial effect', async () => {
    const target = await wallet('100.00');

    const refund = await processing.processWager({
      idempotencyKey: 'pending-1',
      providerId: 'provider-a',
      externalTransactionId: 'ext-pending-1',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-pending-1',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-never-arrived',
    });

    expect(refund.status).toBe(WagerTransactionStatus.PendingReference);
    expect(refund.failureCode).toBeNull();
    expect(refund.balance).toBeNull();

    const row = await pendingRow(refund.transactionId);
    expect(row.status).toBe('PENDING_REFERENCE');
    expect(row.reference_attempts).toBe(0);
    expect(row.reference_transaction_id).toBeNull();
    expect(row.resulting_balance).toBeNull();
    expect(row.reference_next_attempt_at).not.toBeNull();

    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 1 });
    expect(await ledgerCount(refund.transactionId)).toBe(0);

    expect(await eventsFor(refund.transactionId)).toEqual(['WagerTransactionPendingReference']);
    const payload = await pendingEventPayload(refund.transactionId);
    expect(payload.eventType).toBe('WagerTransactionPendingReference');
    expect(payload.data).toMatchObject({
      transactionId: refund.transactionId,
      walletId: target.id,
      referenceExternalTransactionId: 'ext-never-arrived',
      attempts: 0,
    });
  });

  test('a reference owned by another provider does not resolve and stays pending', async () => {
    const target = await wallet('100.00');
    await processedBet(target, 'other-provider', '25.00', 'round-other-provider');

    const refund = await processing.processWager({
      idempotencyKey: 'pending-other-provider',
      providerId: 'provider-b',
      externalTransactionId: 'ext-refund-other-provider',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-other-provider',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-bet-other-provider',
    });

    expect(refund.status).toBe(WagerTransactionStatus.PendingReference);
    expect(await walletRow(target.id)).toEqual({ balance: '75.00', version: 2 });
    expect(await ledgerCount(refund.transactionId)).toBe(0);
  });

  test('the sweep does not touch a pending transaction before its next attempt is due', async () => {
    const target = await wallet('100.00');

    const refund = await processing.processWager({
      idempotencyKey: 'pending-not-due',
      providerId: 'provider-a',
      externalTransactionId: 'ext-pending-not-due',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-not-due',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-missing-not-due',
    });

    const before = await dueAt(refund.transactionId);
    const sweep = await processing.reprocessPendingReferences({
      now: new Date(before.getTime() - 1),
    });

    expect(sweep.claimed).toBe(0);
    const after = await pendingRow(refund.transactionId);
    expect(after.reference_attempts).toBe(0);
    expect(after.status).toBe('PENDING_REFERENCE');
  });

  test('a reference that arrives later is resolved by the sweep and applied financially', async () => {
    const target = await wallet('100.00');

    const refund = await processing.processWager({
      idempotencyKey: 'late-refund',
      providerId: 'provider-a',
      externalTransactionId: 'ext-late-refund',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-late',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-bet-late',
    });
    expect(refund.status).toBe(WagerTransactionStatus.PendingReference);

    // the BET arrives out of order, after its own refund
    const betId = await processedBet(target, 'late', '25.00', 'round-late');
    expect(await walletRow(target.id)).toEqual({ balance: '75.00', version: 2 });

    const due = await dueAt(refund.transactionId);
    const sweep = await processing.reprocessPendingReferences({ now: due });

    expect(sweep).toMatchObject({ claimed: 1, processed: 1, rejected: 0, rescheduled: 0 });

    const row = await pendingRow(refund.transactionId);
    expect(row.status).toBe('PROCESSED');
    expect(row.reference_transaction_id).toBe(betId);
    expect(row.resulting_balance).toBe('100.00');
    expect(row.reference_next_attempt_at).toBeNull();

    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });
    expect(await ledgerCount(refund.transactionId)).toBe(1);
    expect(await eventsFor(refund.transactionId)).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionProcessed',
    ]);
    expect(await eventsFor(target.id)).toContain('WalletBalanceChanged');

    const view = await processing.getTransaction(refund.transactionId);
    expect(view?.status).toBe(WagerTransactionStatus.Processed);
  });

  test('a replay of a pending idempotency key returns the pending result without duplicating it', async () => {
    const target = await wallet('100.00');

    const command = {
      idempotencyKey: 'pending-replay',
      providerId: 'provider-a',
      externalTransactionId: 'ext-pending-replay',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-pending-replay',
      gameId: 'fortune-chimp',
      kind: 'REFUND' as const,
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-missing-replay',
    };

    const first = await processing.processWager(command);
    const replay = await processing.processWager(command);

    expect(first.status).toBe(WagerTransactionStatus.PendingReference);
    expect(replay).toEqual({ ...first, idempotentReplay: true });
    expect(await eventsFor(first.transactionId)).toEqual(['WagerTransactionPendingReference']);
  });

  test('exhausting the attempt budget rejects the pending transaction with exponential backoff', async () => {
    const target = await wallet('100.00');

    const refund = await processing.processWager({
      idempotencyKey: 'exhausted',
      providerId: 'provider-a',
      externalTransactionId: 'ext-exhausted',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-exhausted',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-missing-forever',
    });

    const delays: number[] = [];
    let clock = await dueAt(refund.transactionId);

    for (let attempt = 1; attempt <= MAX_REFERENCE_ATTEMPTS; attempt += 1) {
      const sweep = await processing.reprocessPendingReferences({ now: clock });
      expect(sweep.claimed).toBe(1);

      const row = await pendingRow(refund.transactionId);
      if (attempt < MAX_REFERENCE_ATTEMPTS) {
        expect(sweep).toMatchObject({ processed: 0, rejected: 0, rescheduled: 1 });
        expect(row.status).toBe('PENDING_REFERENCE');
        expect(row.reference_attempts).toBe(attempt);
        const next = new Date(row.reference_next_attempt_at!);
        delays.push(next.getTime() - clock.getTime());
        clock = next;
      } else {
        expect(sweep).toMatchObject({ processed: 0, rejected: 1, rescheduled: 0 });
        expect(row.status).toBe('REJECTED');
        expect(row.failure_code).toBe(FailureCode.ReferenceNotFound);
        expect(row.reference_next_attempt_at).toBeNull();
        expect(row.reference_attempts).toBe(MAX_REFERENCE_ATTEMPTS);
      }
    }

    // exponential growth, capped
    expect(delays[0]).toBe(Math.min(REFERENCE_BACKOFF_BASE_MS * 2, REFERENCE_BACKOFF_MAX_MS));
    for (let index = 1; index < delays.length; index += 1) {
      const previous = delays[index - 1]!;
      const current = delays[index]!;
      expect(current).toBe(Math.min(previous * 2, REFERENCE_BACKOFF_MAX_MS));
    }

    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 1 });
    expect(await ledgerCount(refund.transactionId)).toBe(0);
    expect(await eventsFor(refund.transactionId)).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionRejected',
    ]);
  });

  test('the TTL rejects a pending transaction even before the attempt budget is exhausted', async () => {
    const target = await wallet('100.00');

    const refund = await processing.processWager({
      idempotencyKey: 'ttl',
      providerId: 'provider-a',
      externalTransactionId: 'ext-ttl',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-ttl',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-missing-ttl',
    });

    const scheduled = await dueAt(refund.transactionId);
    const sweep = await processing.reprocessPendingReferences({
      now: new Date(scheduled.getTime() + REFERENCE_TTL_MS),
    });

    expect(sweep).toMatchObject({ claimed: 1, processed: 0, rejected: 1, rescheduled: 0 });
    const rejected = await pendingRow(refund.transactionId);
    expect(rejected.status).toBe('REJECTED');
    expect(rejected.failure_code).toBe(FailureCode.ReferenceNotFound);
    expect(rejected.reference_attempts).toBe(1);
    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 1 });
  });

  test('concurrent sweeps never apply the same pending resolution twice', async () => {
    const target = await wallet('100.00');

    const refund = await processing.processWager({
      idempotencyKey: 'concurrent-pending',
      providerId: 'provider-a',
      externalTransactionId: 'ext-concurrent-pending',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-concurrent',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-bet-concurrent',
    });

    await processedBet(target, 'concurrent', '25.00', 'round-concurrent');
    const due = await dueAt(refund.transactionId);

    const sweeps = await Promise.all([
      processing.reprocessPendingReferences({ now: due }),
      processing.reprocessPendingReferences({ now: due }),
      processing.reprocessPendingReferences({ now: due }),
    ]);

    const processed = sweeps.reduce((total, sweep) => total + sweep.processed, 0);
    expect(processed).toBe(1);

    const row = await pendingRow(refund.transactionId);
    expect(row.status).toBe('PROCESSED');
    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });
    expect(await ledgerCount(refund.transactionId)).toBe(1);
    expect((await eventsFor(refund.transactionId)).filter((type) => type === 'WagerTransactionProcessed').length).toBe(1);
  });

  test('concurrent sweeps consume only one retry attempt for an unresolved reference', async () => {
    const target = await wallet('100.00');
    const refund = await processing.processWager({
      idempotencyKey: 'concurrent-reschedule',
      providerId: 'provider-a',
      externalTransactionId: 'ext-concurrent-reschedule',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-concurrent-reschedule',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-missing-concurrent-reschedule',
    });
    const due = await dueAt(refund.transactionId);

    const sweeps = await Promise.all([
      processing.reprocessPendingReferences({ now: due }),
      processing.reprocessPendingReferences({ now: due }),
      processing.reprocessPendingReferences({ now: due }),
    ]);

    expect(sweeps.reduce((total, sweep) => total + sweep.claimed, 0)).toBe(1);
    expect(sweeps.reduce((total, sweep) => total + sweep.rescheduled, 0)).toBe(1);
    const pending = await pendingRow(refund.transactionId);
    expect(pending.status).toBe('PENDING_REFERENCE');
    expect(pending.reference_attempts).toBe(1);
  });

  test('a failure while committing a pending resolution leaves no partial effect', async () => {
    const target = await wallet('100.00');

    const refund = await processing.processWager({
      idempotencyKey: 'pending-rollback',
      providerId: 'provider-a',
      externalTransactionId: 'ext-pending-rollback',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-pending-rollback',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-bet-pending-rollback',
    });

    await processedBet(target, 'pending-rollback', '25.00', 'round-pending-rollback');
    const due = await dueAt(refund.transactionId);

    class FailingResolution extends MikroOrmFinancialProcessing {
      protected override async interceptUnitOfWork(stage: UnitOfWorkStage, _em: EntityManager): Promise<void> {
        if (stage === 'reference-resolution-before-commit') {
          throw new Error('injected failure before commit');
        }
        return Promise.resolve();
      }
    }

    const failing = new FailingResolution(orm);
    await expect(failing.reprocessPendingReferences({ now: due })).rejects.toThrow('injected failure before commit');

    const row = await pendingRow(refund.transactionId);
    expect(row.status).toBe('PENDING_REFERENCE');
    expect(row.reference_transaction_id).toBeNull();
    expect(await walletRow(target.id)).toEqual({ balance: '75.00', version: 2 });
    expect(await ledgerCount(refund.transactionId)).toBe(0);
    expect(await eventsFor(refund.transactionId)).toEqual(['WagerTransactionPendingReference']);

    // the pending transaction is still resolvable afterwards
    const sweep = await processing.reprocessPendingReferences({ now: due });
    expect(sweep).toMatchObject({ claimed: 1, processed: 1 });
    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });
  });

  test('a pending transaction whose reference arrives incompatible is rejected by the sweep', async () => {
    const target = await wallet('100.00');

    const refund = await processing.processWager({
      idempotencyKey: 'pending-incompatible',
      providerId: 'provider-a',
      externalTransactionId: 'ext-pending-incompatible',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-incompatible',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-bet-incompatible',
    });

    // the reference finally arrives, but for a different round
    await processing.processWager({
      idempotencyKey: 'bet-incompatible',
      providerId: 'provider-a',
      externalTransactionId: 'ext-bet-incompatible',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-another',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });

    const due = await dueAt(refund.transactionId);
    const sweep = await processing.reprocessPendingReferences({ now: due });

    expect(sweep).toMatchObject({ claimed: 1, processed: 0, rejected: 1, rescheduled: 0 });
    const row = await pendingRow(refund.transactionId);
    expect(row.status).toBe('REJECTED');
    expect(row.failure_code).toBe(FailureCode.ReferenceIncompatible);
    expect(await ledgerCount(refund.transactionId)).toBe(0);
    expect(await walletRow(target.id)).toEqual({ balance: '75.00', version: 2 });
  });

  test('the sweep honours its batch limit and leaves the remaining pendings for the next run', async () => {
    const target = await wallet('100.00');
    const now = new Date(T0.getTime() + REFERENCE_TTL_MS * 10);

    // drain whatever earlier tests left pending, so the batch arithmetic is exact
    while ((await processing.reprocessPendingReferences({ now, limit: 100 })).claimed > 0) {
      continue;
    }

    for (let index = 1; index <= 3; index += 1) {
      const pending = await processing.processWager({
        idempotencyKey: `batch-${index}`,
        providerId: 'provider-a',
        externalTransactionId: `ext-batch-${index}`,
        walletId: target.id,
        playerId: target.playerId,
        roundId: `round-batch-${index}`,
        gameId: 'fortune-chimp',
        kind: 'REFUND',
        money: { amount: '25.00', currency: 'BRL' },
        referenceExternalTransactionId: `ext-missing-batch-${index}`,
      });
      expect(pending.status).toBe(WagerTransactionStatus.PendingReference);
    }

    const first = await processing.reprocessPendingReferences({ now, limit: 2 });
    expect(first.claimed).toBe(2);

    const second = await processing.reprocessPendingReferences({ now, limit: 10 });
    expect(second.claimed).toBe(1);

    const third = await processing.reprocessPendingReferences({ now, limit: 10 });
    expect(third.claimed).toBe(0);
  });
});
