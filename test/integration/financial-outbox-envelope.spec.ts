import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { MikroOrmFinancialProcessing } from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

/**
 * CP2B-1-R3 FIX-02: the persisted outbox envelope must match the contract declared in
 * CHALLENGE.md section 11 — eventId, eventType, aggregateId, correlationId, optional
 * causationId, ISO occurredAt, version and a typed `data` payload with decimal strings.
 * Terminal transactions cannot be deleted (DB guard), so every test uses its own player
 * and filters the outbox by its own correlationId instead of truncating tables.
 */
interface OutboxRow {
  readonly id: string;
  readonly aggregate_id: string;
  readonly event_type: string;
  readonly payload: Record<string, unknown>;
  readonly occurred_at: string;
  readonly published_at: Date | null;
}

const ENVELOPE_KEYS = ['aggregateId', 'correlationId', 'data', 'eventId', 'eventType', 'occurredAt', 'version'];
const PLAYER_OPENING = '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1';
const PLAYER_PROCESSED = '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a2';
const PLAYER_REJECTED = '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a3';
const PLAYER_REPLAY = '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a4';

let orm: MikroORM<PostgreSqlDriver>;
let processing: MikroOrmFinancialProcessing;

async function outboxFor(correlationId: string): Promise<OutboxRow[]> {
  return await orm.em.getConnection().execute(
    `select id::text as id, aggregate_id::text as aggregate_id, event_type, payload, occurred_at, published_at
       from outbox_messages
      where payload->>'correlationId' = '${correlationId}'
      order by occurred_at asc, event_type asc`,
  ) as OutboxRow[];
}

async function openingTransactionId(walletId: string): Promise<string> {
  const rows = await orm.em.getConnection().execute(
    `select id::text as id from wager_transactions where wallet_id = '${walletId}' and kind = 'OPENING'`,
  ) as Array<{ id: string }>;
  return rows[0]!.id;
}

function envelopeData(
  row: OutboxRow,
  expected: { eventType: string; aggregateId: string; correlationId: string },
): Record<string, unknown> {
  const envelope = row.payload;

  expect(Object.keys(envelope).sort()).toEqual(ENVELOPE_KEYS);
  expect(envelope.eventId).toBe(row.id);
  expect(envelope.eventType).toBe(expected.eventType);
  expect(envelope.eventType).toBe(row.event_type);
  expect(envelope.aggregateId).toBe(expected.aggregateId);
  // the outbox column must agree with the aggregateId declared inside the envelope
  expect(envelope.aggregateId).toBe(row.aggregate_id);
  expect(envelope.correlationId).toBe(expected.correlationId);
  expect(envelope.version).toBe(1);
  expect(typeof envelope.occurredAt).toBe('string');
  expect(envelope.occurredAt).toBe(new Date(row.occurred_at).toISOString());
  expect(row.published_at).toBeNull();

  return envelope.data as Record<string, unknown>;
}

describe('CP2B-1-R3 FIX-02 outbox envelope contract', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
    processing = new MikroOrmFinancialProcessing(orm);
  });

  afterAll(async () => {
    await orm.getMigrator().down();
    await orm.close(true);
  });

  test('OPENING: a positive wallet opening persists the CHALLENGE.md envelope for both events', async () => {
    const wallet = await processing.openWallet({
      playerId: PLAYER_OPENING,
      initialBalance: { amount: '100.00', currency: 'BRL' },
      correlationId: 'corr-opening',
    });
    const openingId = await openingTransactionId(wallet.id);

    const rows = await outboxFor('corr-opening');
    expect(rows.map((row) => row.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);

    const processedData = envelopeData(rows[0]!, {
      eventType: 'WagerTransactionProcessed',
      aggregateId: openingId,
      correlationId: 'corr-opening',
    });
    expect(processedData).toMatchObject({
      transactionId: openingId,
      walletId: wallet.id,
      balance: { amount: '100.00', currency: 'BRL' },
    });

    const changedData = envelopeData(rows[1]!, {
      eventType: 'WalletBalanceChanged',
      aggregateId: wallet.id,
      correlationId: 'corr-opening',
    });
    expect(changedData).toEqual({
      walletId: wallet.id,
      transactionId: openingId,
      direction: 'CREDIT',
      money: { amount: '100.00', currency: 'BRL' },
      balanceBefore: { amount: '0.00', currency: 'BRL' },
      balanceAfter: { amount: '100.00', currency: 'BRL' },
      walletVersion: 1,
    });
    expect(wallet.version).toBe(1);
  });

  test('BET processed: WalletBalanceChanged carries the financial data and the persisted wallet version', async () => {
    const wallet = await processing.openWallet({
      playerId: PLAYER_PROCESSED,
      initialBalance: { amount: '100.00', currency: 'BRL' },
      correlationId: 'corr-processed-opening',
    });

    const bet = await processing.processBet({
      idempotencyKey: 'envelope-bet-key',
      providerId: 'provider-envelope',
      externalTransactionId: 'external-envelope-bet',
      walletId: wallet.id,
      playerId: PLAYER_PROCESSED,
      roundId: 'round-envelope',
      gameId: 'game-envelope',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
      correlationId: 'corr-bet',
      causationId: 'cause-bet',
    });

    const rows = await outboxFor('corr-bet');
    expect(rows.map((row) => row.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);

    const processed = rows[0]!;
    const changed = rows[1]!;

    expect(Object.keys(processed.payload).sort()).toEqual([...ENVELOPE_KEYS, 'causationId'].sort());
    expect(processed.payload.causationId).toBe('cause-bet');
    expect(processed.payload.eventId).toBe(processed.id);
    expect(processed.payload.aggregateId).toBe(bet.transactionId);
    expect(processed.payload.aggregateId).toBe(processed.aggregate_id);
    expect(processed.payload.correlationId).toBe('corr-bet');
    expect(processed.payload.version).toBe(1);
    expect(processed.payload.data).toMatchObject({
      transactionId: bet.transactionId,
      walletId: wallet.id,
      balance: { amount: '75.00', currency: 'BRL' },
    });

    expect(changed.payload.causationId).toBe('cause-bet');
    expect(changed.payload.eventId).toBe(changed.id);
    expect(changed.payload.aggregateId).toBe(wallet.id);
    expect(changed.payload.aggregateId).toBe(changed.aggregate_id);
    expect(changed.payload.data).toEqual({
      walletId: wallet.id,
      transactionId: bet.transactionId,
      direction: 'DEBIT',
      money: { amount: '25.00', currency: 'BRL' },
      balanceBefore: { amount: '100.00', currency: 'BRL' },
      balanceAfter: { amount: '75.00', currency: 'BRL' },
      walletVersion: 2,
    });

    const persistedWallet = await processing.getWallet(wallet.id);
    expect(persistedWallet?.version).toBe((changed.payload.data as { walletVersion: number }).walletVersion);
  });

  test('BET rejected: only WagerTransactionRejected is persisted, with the failure code in data', async () => {
    const wallet = await processing.openWallet({
      playerId: PLAYER_REJECTED,
      initialBalance: { amount: '10.00', currency: 'BRL' },
      correlationId: 'corr-rejected-opening',
    });

    const rejected = await processing.processBet({
      idempotencyKey: 'envelope-rejected-key',
      providerId: 'provider-envelope',
      externalTransactionId: 'external-envelope-rejected',
      walletId: wallet.id,
      playerId: PLAYER_REJECTED,
      roundId: 'round-envelope',
      gameId: 'game-envelope',
      kind: 'BET',
      money: { amount: '80.00', currency: 'BRL' },
      correlationId: 'corr-rejected',
    });

    const rows = await outboxFor('corr-rejected');
    expect(rows.map((row) => row.event_type)).toEqual(['WagerTransactionRejected']);

    const data = envelopeData(rows[0]!, {
      eventType: 'WagerTransactionRejected',
      aggregateId: rejected.transactionId,
      correlationId: 'corr-rejected',
    });

    expect(data).toMatchObject({
      transactionId: rejected.transactionId,
      walletId: wallet.id,
      balance: { amount: '10.00', currency: 'BRL' },
      failureCode: 'INSUFFICIENT_BALANCE',
    });
  });

  test('replay: a repeated Idempotency-Key does not duplicate envelopes', async () => {
    const wallet = await processing.openWallet({
      playerId: PLAYER_REPLAY,
      initialBalance: { amount: '100.00', currency: 'BRL' },
      correlationId: 'corr-replay-opening',
    });

    const command = {
      idempotencyKey: 'envelope-replay-key',
      providerId: 'provider-envelope',
      externalTransactionId: 'external-envelope-replay',
      walletId: wallet.id,
      playerId: PLAYER_REPLAY,
      roundId: 'round-envelope',
      gameId: 'game-envelope',
      kind: 'BET' as const,
      money: { amount: '25.00', currency: 'BRL' },
      correlationId: 'corr-replay',
    };

    const first = await processing.processBet(command);
    const replay = await processing.processBet(command);

    expect(first.idempotentReplay).toBe(false);
    expect(replay.idempotentReplay).toBe(true);

    const rows = await outboxFor('corr-replay');
    expect(rows.length).toBe(2);
    expect(new Set(rows.map((row) => row.id)).size).toBe(2);
    expect(rows.map((row) => row.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
  });
});
