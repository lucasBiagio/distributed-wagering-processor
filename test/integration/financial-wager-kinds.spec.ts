import { createHash, randomUUID } from 'node:crypto';

import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { MikroOrmFinancialProcessing } from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';
import { FinancialConflictError } from '../../src/application/financial-contracts.js';
import {
  FailureCode,
  WagerTransactionStatus,
} from '../../src/domain/wager-transaction.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

/**
 * CP2B-2: WIN, LOSS, REFUND and ROLLBACK processing over the CP2B-1 unit of work.
 * Every assertion reads the persisted PostgreSQL state (wallet, transaction, ledger,
 * outbox), never only the in-memory result.
 */
interface LedgerRow {
  readonly transaction_id: string;
  readonly direction: string;
  readonly amount: string;
  readonly balance_before: string;
  readonly balance_after: string;
}

interface TransactionRow {
  readonly id: string;
  readonly kind: string;
  readonly status: string;
  readonly failure_code: string | null;
  readonly reference_transaction_id: string | null;
  readonly reference_external_transaction_id: string | null;
  readonly resulting_balance: string | null;
  readonly processed_at: Date | null;
}

let orm: MikroORM<PostgreSqlDriver>;
let processing: MikroOrmFinancialProcessing;
let sequence = 0;

function player(suffix: number): string {
  return `00000000-0000-4000-8000-${String(suffix).padStart(12, '0')}`;
}

function nextSuffix(): number {
  sequence += 1;
  return sequence;
}

/** Exact CP2B-1 BET hash fixture: referenceExternalTransactionId did not exist in this payload. */
function legacyCp2b1BetPayloadHash(command: {
  readonly providerId: string;
  readonly externalTransactionId: string;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly money: { readonly amount: string; readonly currency: string };
}): string {
  const payload = {
    amount: command.money.amount,
    currency: command.money.currency,
    externalTransactionId: command.externalTransactionId,
    gameId: command.gameId,
    kind: 'BET',
    playerId: command.playerId,
    providerId: command.providerId,
    roundId: command.roundId,
    walletId: command.walletId,
  };
  const canonical = `{${Object.keys(payload).sort().map((key) => `${JSON.stringify(key)}:${JSON.stringify(payload[key as keyof typeof payload])}`).join(',')}}`;
  return createHash('sha256').update(canonical).digest('hex');
}

async function wallet(balance: string): Promise<{ id: string; playerId: string }> {
  const playerId = player(nextSuffix());
  const view = await processing.openWallet({
    playerId,
    initialBalance: { amount: balance, currency: 'BRL' },
    correlationId: `corr-open-${playerId}`,
  });
  return { id: view.id, playerId };
}

async function ledgerOf(walletId: string): Promise<LedgerRow[]> {
  return await orm.em.getConnection().execute(
    `select transaction_id::text as transaction_id, direction, amount::text as amount,
            balance_before::text as balance_before, balance_after::text as balance_after
       from wallet_ledger_entries where wallet_id = ? order by created_at asc, id asc`,
    [walletId],
  ) as LedgerRow[];
}

async function transactionRow(id: string): Promise<TransactionRow> {
  const rows = await orm.em.getConnection().execute(
    `select id::text as id, kind, status, failure_code, reference_transaction_id::text as reference_transaction_id,
            reference_external_transaction_id, resulting_balance::text as resulting_balance, processed_at
       from wager_transactions where id = ?`,
    [id],
  ) as TransactionRow[];
  return rows[0]!;
}

async function walletRow(id: string): Promise<{ balance: string; version: number }> {
  const rows = await orm.em.getConnection().execute(
    `select balance::text as balance, version from wallets where id = ?`,
    [id],
  ) as Array<{ balance: string; version: number }>;
  return rows[0]!;
}

async function eventTypes(correlationId: string): Promise<string[]> {
  const rows = await orm.em.getConnection().execute(
    `select event_type from outbox_messages where payload->>'correlationId' = ?
      order by occurred_at asc, event_type asc`,
    [correlationId],
  ) as Array<{ event_type: string }>;
  return rows.map((row) => row.event_type);
}

async function reconstructedBalance(walletId: string): Promise<string> {
  const rows = await orm.em.getConnection().execute(
    `select coalesce(sum(case when direction = 'CREDIT' then amount else -amount end), 0)::text as total
       from wallet_ledger_entries where wallet_id = ?`,
    [walletId],
  ) as Array<{ total: string }>;
  return rows[0]!.total;
}

describe('CP2B-2 wager kinds', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
    processing = new MikroOrmFinancialProcessing(orm);
  });

  afterAll(async () => {
    await orm.getMigrator().down({ to: 0 });
    await orm.close(true);
  });

  test('WIN credits the wallet, creates one CREDIT ledger entry and increments the version', async () => {
    const target = await wallet('100.00');

    const win = await processing.processWager({
      idempotencyKey: 'win-1',
      providerId: 'provider-a',
      externalTransactionId: 'ext-win-1',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-win-1',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '40.00', currency: 'BRL' },
      correlationId: 'corr-win-1',
    });

    expect(win.status).toBe(WagerTransactionStatus.Processed);
    expect(win.balance).toEqual({ amount: '140.00', currency: 'BRL' });
    expect(win.idempotentReplay).toBe(false);

    expect(await walletRow(target.id)).toEqual({ balance: '140.00', version: 2 });

    const row = await transactionRow(win.transactionId);
    expect(row.status).toBe('PROCESSED');
    expect(row.resulting_balance).toBe('140.00');
    expect(row.processed_at).not.toBeNull();
    expect(row.reference_transaction_id).toBeNull();

    const ledger = await ledgerOf(target.id);
    const winEntries = ledger.filter((entry) => entry.transaction_id === win.transactionId);
    expect(winEntries).toEqual([{
      transaction_id: win.transactionId,
      direction: 'CREDIT',
      amount: '40.00',
      balance_before: '100.00',
      balance_after: '140.00',
    }]);

    expect(await eventTypes('corr-win-1')).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
  });

  test('LOSS is PROCESSED without balance movement, ledger entry or version increment', async () => {
    const target = await wallet('100.00');

    const loss = await processing.processWager({
      idempotencyKey: 'loss-1',
      providerId: 'provider-a',
      externalTransactionId: 'ext-loss-1',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-loss-1',
      gameId: 'fortune-chimp',
      kind: 'LOSS',
      money: { amount: '30.00', currency: 'BRL' },
      correlationId: 'corr-loss-1',
    });

    expect(loss.status).toBe(WagerTransactionStatus.Processed);
    expect(loss.balance).toEqual({ amount: '100.00', currency: 'BRL' });

    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 1 });

    const ledger = await ledgerOf(target.id);
    expect(ledger.filter((entry) => entry.transaction_id === loss.transactionId)).toEqual([]);

    // no balance changed, therefore no WalletBalanceChanged event
    expect(await eventTypes('corr-loss-1')).toEqual(['WagerTransactionProcessed']);
  });

  test('LOSS with 0.00 is PROCESSED without balance movement, ledger entry or version increment', async () => {
    const target = await wallet('100.00');

    const loss = await processing.processWager({
      idempotencyKey: 'loss-zero-1',
      providerId: 'provider-a',
      externalTransactionId: 'ext-loss-zero-1',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-loss-zero-1',
      gameId: 'fortune-chimp',
      kind: 'LOSS',
      money: { amount: '0.00', currency: 'BRL' },
      correlationId: 'corr-loss-zero-1',
    });

    expect(loss).toMatchObject({
      status: WagerTransactionStatus.Processed,
      balance: { amount: '100.00', currency: 'BRL' },
      idempotentReplay: false,
    });
    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 1 });
    expect((await ledgerOf(target.id)).filter((entry) => entry.transaction_id === loss.transactionId)).toEqual([]);
    expect(await eventTypes('corr-loss-zero-1')).toEqual(['WagerTransactionProcessed']);

    const replay = await processing.processWager({
      idempotencyKey: 'loss-zero-1',
      providerId: 'provider-a',
      externalTransactionId: 'ext-loss-zero-1',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-loss-zero-1',
      gameId: 'fortune-chimp',
      kind: 'LOSS',
      money: { amount: '0.00', currency: 'BRL' },
      correlationId: 'corr-loss-zero-1',
    });
    expect(replay).toEqual({ ...loss, idempotentReplay: true });
  });

  test('REFUND of a processed BET credits the wallet once and links the internal reference', async () => {
    const target = await wallet('100.00');

    const bet = await processing.processWager({
      idempotencyKey: 'refund-bet-1',
      providerId: 'provider-a',
      externalTransactionId: 'ext-bet-refund-1',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-refund-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });
    expect(bet.status).toBe(WagerTransactionStatus.Processed);

    const refund = await processing.processWager({
      idempotencyKey: 'refund-1',
      providerId: 'provider-a',
      externalTransactionId: 'ext-refund-1',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-refund-1',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-bet-refund-1',
      correlationId: 'corr-refund-1',
    });

    expect(refund.status).toBe(WagerTransactionStatus.Processed);
    expect(refund.balance).toEqual({ amount: '100.00', currency: 'BRL' });
    expect(refund.referenceTransactionId).toBe(bet.transactionId);

    const row = await transactionRow(refund.transactionId);
    expect(row.reference_transaction_id).toBe(bet.transactionId);
    expect(row.reference_external_transaction_id).toBe('ext-bet-refund-1');

    const ledger = await ledgerOf(target.id);
    expect(ledger.filter((entry) => entry.transaction_id === refund.transactionId)).toEqual([{
      transaction_id: refund.transactionId,
      direction: 'CREDIT',
      amount: '25.00',
      balance_before: '75.00',
      balance_after: '100.00',
    }]);

    expect(await eventTypes('corr-refund-1')).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });
  });

  test('repeating the same REFUND key returns the original result without crediting twice', async () => {
    const target = await wallet('100.00');

    await processing.processWager({
      idempotencyKey: 'refund-replay-bet',
      providerId: 'provider-a',
      externalTransactionId: 'ext-bet-replay',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-replay',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });

    const command = {
      idempotencyKey: 'refund-replay',
      providerId: 'provider-a',
      externalTransactionId: 'ext-refund-replay',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-replay',
      gameId: 'fortune-chimp',
      kind: 'REFUND' as const,
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-bet-replay',
    };

    const first = await processing.processWager(command);
    const replay = await processing.processWager(command);

    expect(first.idempotentReplay).toBe(false);
    expect(replay).toEqual({ ...first, idempotentReplay: true });
    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });

    const ledger = await ledgerOf(target.id);
    expect(ledger.filter((entry) => entry.transaction_id === first.transactionId).length).toBe(1);
  });

  test('replaying a CP2B-1 persisted BET hash remains idempotent after CP2B-2', async () => {
    const walletId = randomUUID();
    const playerId = player(nextSuffix());
    const transactionId = randomUUID();
    const command = {
      idempotencyKey: 'legacy-bet-replay',
      providerId: 'provider-a',
      externalTransactionId: 'ext-legacy-bet-replay',
      walletId,
      playerId,
      roundId: 'round-legacy-bet-replay',
      gameId: 'fortune-chimp',
      kind: 'BET' as const,
      money: { amount: '25.00', currency: 'BRL' },
    };
    const legacyHash = legacyCp2b1BetPayloadHash(command);
    const connection = orm.em.getConnection();
    await connection.execute(
      `insert into wallets (id, player_id, currency, balance, version, created_at, updated_at)
       values (?, ?, 'BRL', 75.00, 2, now(), now())`,
      [walletId, playerId],
    );
    await connection.execute(
      `insert into wager_transactions (
         id, provider_id, external_transaction_id, idempotency_key, payload_hash,
         wallet_id, player_id, round_id, game_id, kind, amount, currency,
         status, processed_at, resulting_balance, reference_next_attempt_at, created_at
       ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'BET', 25.00, 'BRL', 'PROCESSED', now(), 75.00, null, now())`,
      [
        transactionId,
        command.providerId,
        command.externalTransactionId,
        command.idempotencyKey,
        legacyHash,
        walletId,
        playerId,
        command.roundId,
        command.gameId,
      ],
    );
    await connection.execute(
      `insert into wallet_ledger_entries (
         id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at
       ) values (?, ?, ?, 'DEBIT', 25.00, 'BRL', 100.00, 75.00, now())`,
      [randomUUID(), walletId, transactionId],
    );
    const before = await connection.execute(
      `select
         (select count(*)::int from wallet_ledger_entries where wallet_id = ?) as ledger_count,
         (select count(*)::int from outbox_messages) as outbox_count`,
      [walletId],
    ) as Array<{ ledger_count: number; outbox_count: number }>;

    const replay = await processing.processWager(command);

    expect(replay).toMatchObject({
      transactionId,
      status: WagerTransactionStatus.Processed,
      balance: { amount: '75.00', currency: 'BRL' },
      idempotentReplay: true,
    });
    expect(await walletRow(walletId)).toEqual({ balance: '75.00', version: 2 });
    expect(await connection.execute(
      `select
         (select count(*)::int from wallet_ledger_entries where wallet_id = ?) as ledger_count,
         (select count(*)::int from outbox_messages) as outbox_count`,
      [walletId],
    )).toEqual(before);

    await expect(processing.processWager({
      ...command,
      money: { amount: '26.00', currency: 'BRL' },
    })).rejects.toBeInstanceOf(FinancialConflictError);
  });

  test('reversal idempotency hashes include the referenced external transaction', async () => {
    const target = await wallet('100.00');
    for (const externalTransactionId of ['ext-reference-hash-a', 'ext-reference-hash-b']) {
      await processing.processWager({
        idempotencyKey: `bet-${externalTransactionId}`,
        providerId: 'provider-a',
        externalTransactionId,
        walletId: target.id,
        playerId: target.playerId,
        roundId: `round-${externalTransactionId}`,
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '10.00', currency: 'BRL' },
      });
    }
    const refund = {
      idempotencyKey: 'reference-hash-refund',
      providerId: 'provider-a',
      externalTransactionId: 'ext-reference-hash-refund',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-ext-reference-hash-a',
      gameId: 'fortune-chimp',
      kind: 'REFUND' as const,
      money: { amount: '10.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-reference-hash-a',
    };
    await processing.processWager(refund);

    await expect(processing.processWager({
      ...refund,
      referenceExternalTransactionId: 'ext-reference-hash-b',
    })).rejects.toBeInstanceOf(FinancialConflictError);
  });

  test('a second REFUND for the same BET under a new key is rejected as already reversed', async () => {
    const target = await wallet('100.00');

    await processing.processWager({
      idempotencyKey: 'double-refund-bet',
      providerId: 'provider-a',
      externalTransactionId: 'ext-bet-double',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-double',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });

    const base = {
      providerId: 'provider-a',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-double',
      gameId: 'fortune-chimp',
      kind: 'REFUND' as const,
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-bet-double',
    };

    const first = await processing.processWager({
      ...base,
      idempotencyKey: 'double-refund-1',
      externalTransactionId: 'ext-refund-double-1',
    });
    const second = await processing.processWager({
      ...base,
      idempotencyKey: 'double-refund-2',
      externalTransactionId: 'ext-refund-double-2',
      correlationId: 'corr-double-refund',
    });

    expect(first.status).toBe(WagerTransactionStatus.Processed);
    expect(second.status).toBe(WagerTransactionStatus.Rejected);
    expect(second.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);

    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });
    const ledger = await ledgerOf(target.id);
    expect(ledger.filter((entry) => entry.transaction_id === second.transactionId)).toEqual([]);
    expect(await eventTypes('corr-double-refund')).toEqual(['WagerTransactionRejected']);
  });

  test('ROLLBACK of a BET credits the wallet back', async () => {
    const target = await wallet('100.00');

    const bet = await processing.processWager({
      idempotencyKey: 'rb-bet',
      providerId: 'provider-a',
      externalTransactionId: 'ext-rb-bet',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-rb-bet',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '30.00', currency: 'BRL' },
    });

    const rollback = await processing.processWager({
      idempotencyKey: 'rb-bet-rollback',
      providerId: 'provider-a',
      externalTransactionId: 'ext-rb-bet-rollback',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-rb-bet',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '30.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-rb-bet',
    });

    expect(rollback.status).toBe(WagerTransactionStatus.Processed);
    expect(rollback.balance).toEqual({ amount: '100.00', currency: 'BRL' });
    expect(rollback.referenceTransactionId).toBe(bet.transactionId);

    const ledger = await ledgerOf(target.id);
    expect(ledger.filter((entry) => entry.transaction_id === rollback.transactionId)).toEqual([{
      transaction_id: rollback.transactionId,
      direction: 'CREDIT',
      amount: '30.00',
      balance_before: '70.00',
      balance_after: '100.00',
    }]);
  });

  test('ROLLBACK of a WIN debits the credited amount', async () => {
    const target = await wallet('100.00');

    const win = await processing.processWager({
      idempotencyKey: 'rb-win',
      providerId: 'provider-a',
      externalTransactionId: 'ext-rb-win',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-rb-win',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '50.00', currency: 'BRL' },
    });

    const rollback = await processing.processWager({
      idempotencyKey: 'rb-win-rollback',
      providerId: 'provider-a',
      externalTransactionId: 'ext-rb-win-rollback',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-rb-win',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-rb-win',
    });

    expect(rollback.status).toBe(WagerTransactionStatus.Processed);
    expect(rollback.balance).toEqual({ amount: '100.00', currency: 'BRL' });

    const ledger = await ledgerOf(target.id);
    expect(ledger.filter((entry) => entry.transaction_id === rollback.transactionId)).toEqual([{
      transaction_id: rollback.transactionId,
      direction: 'DEBIT',
      amount: '50.00',
      balance_before: '150.00',
      balance_after: '100.00',
    }]);
    expect(win.status).toBe(WagerTransactionStatus.Processed);
  });

  test('ROLLBACK of a REFUND debits the refunded amount', async () => {
    const target = await wallet('100.00');

    await processing.processWager({
      idempotencyKey: 'rb-refund-bet',
      providerId: 'provider-a',
      externalTransactionId: 'ext-rb-refund-bet',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-rb-refund',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '20.00', currency: 'BRL' },
    });

    await processing.processWager({
      idempotencyKey: 'rb-refund-refund',
      providerId: 'provider-a',
      externalTransactionId: 'ext-rb-refund-refund',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-rb-refund',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '20.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-rb-refund-bet',
    });

    const rollback = await processing.processWager({
      idempotencyKey: 'rb-refund-rollback',
      providerId: 'provider-a',
      externalTransactionId: 'ext-rb-refund-rollback',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-rb-refund',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '20.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-rb-refund-refund',
    });

    expect(rollback.status).toBe(WagerTransactionStatus.Processed);
    expect(rollback.balance).toEqual({ amount: '80.00', currency: 'BRL' });

    const ledger = await ledgerOf(target.id);
    expect(ledger.filter((entry) => entry.transaction_id === rollback.transactionId)).toEqual([{
      transaction_id: rollback.transactionId,
      direction: 'DEBIT',
      amount: '20.00',
      balance_before: '100.00',
      balance_after: '80.00',
    }]);
  });

  test('a ROLLBACK that would drive the balance negative is rejected with its own failure code', async () => {
    const target = await wallet('100.00');

    await processing.processWager({
      idempotencyKey: 'neg-win',
      providerId: 'provider-a',
      externalTransactionId: 'ext-neg-win',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-neg',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '50.00', currency: 'BRL' },
    });

    await processing.processWager({
      idempotencyKey: 'neg-bet',
      providerId: 'provider-a',
      externalTransactionId: 'ext-neg-bet',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-neg-bet',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '130.00', currency: 'BRL' },
    });

    const rollback = await processing.processWager({
      idempotencyKey: 'neg-rollback',
      providerId: 'provider-a',
      externalTransactionId: 'ext-neg-rollback',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-neg',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-neg-win',
      correlationId: 'corr-neg-rollback',
    });

    expect(rollback.status).toBe(WagerTransactionStatus.Rejected);
    expect(rollback.failureCode).toBe(FailureCode.ReversalWouldCauseNegativeBalance);
    expect(rollback.failureCode).not.toBe(FailureCode.InsufficientBalance);

    // auditable rejection: the row exists with its observed balance and no ledger entry
    const row = await transactionRow(rollback.transactionId);
    expect(row.status).toBe('REJECTED');
    expect(row.failure_code).toBe('REVERSAL_WOULD_CAUSE_NEGATIVE_BALANCE');
    expect(row.resulting_balance).toBe('20.00');

    expect(await walletRow(target.id)).toEqual({ balance: '20.00', version: 3 });
    const ledger = await ledgerOf(target.id);
    expect(ledger.filter((entry) => entry.transaction_id === rollback.transactionId)).toEqual([]);
    expect(await eventTypes('corr-neg-rollback')).toEqual(['WagerTransactionRejected']);
  });

  test('a reversal whose amount differs from the reference is rejected', async () => {
    const target = await wallet('100.00');

    await processing.processWager({
      idempotencyKey: 'mismatch-bet',
      providerId: 'provider-a',
      externalTransactionId: 'ext-mismatch-bet',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-mismatch',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });

    const refund = await processing.processWager({
      idempotencyKey: 'mismatch-refund',
      providerId: 'provider-a',
      externalTransactionId: 'ext-mismatch-refund',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-mismatch',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '10.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-mismatch-bet',
    });

    expect(refund.status).toBe(WagerTransactionStatus.Rejected);
    expect(refund.failureCode).toBe(FailureCode.ReferenceAmountMismatch);
    expect(await walletRow(target.id)).toEqual({ balance: '75.00', version: 2 });
  });

  test('a reversal pointing at another round is rejected as incompatible', async () => {
    const target = await wallet('100.00');

    await processing.processWager({
      idempotencyKey: 'round-bet',
      providerId: 'provider-a',
      externalTransactionId: 'ext-round-bet',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-original',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });

    const refund = await processing.processWager({
      idempotencyKey: 'round-refund',
      providerId: 'provider-a',
      externalTransactionId: 'ext-round-refund',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-different',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-round-bet',
    });

    expect(refund.status).toBe(WagerTransactionStatus.Rejected);
    expect(refund.failureCode).toBe(FailureCode.ReferenceIncompatible);
    expect(await walletRow(target.id)).toEqual({ balance: '75.00', version: 2 });
  });

  test('a reversal pointing at another player wallet is rejected as incompatible', async () => {
    const owner = await wallet('100.00');
    const stranger = await wallet('100.00');

    await processing.processWager({
      idempotencyKey: 'owner-bet',
      providerId: 'provider-a',
      externalTransactionId: 'ext-owner-bet',
      walletId: owner.id,
      playerId: owner.playerId,
      roundId: 'round-owner',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });

    const refund = await processing.processWager({
      idempotencyKey: 'stranger-refund',
      providerId: 'provider-a',
      externalTransactionId: 'ext-stranger-refund',
      walletId: stranger.id,
      playerId: stranger.playerId,
      roundId: 'round-owner',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-owner-bet',
    });

    expect(refund.status).toBe(WagerTransactionStatus.Rejected);
    expect(refund.failureCode).toBe(FailureCode.ReferenceIncompatible);
    expect(await walletRow(stranger.id)).toEqual({ balance: '100.00', version: 1 });
    expect(await walletRow(owner.id)).toEqual({ balance: '75.00', version: 2 });
  });

  test('a REFUND cannot reverse a WIN', async () => {
    const target = await wallet('100.00');

    await processing.processWager({
      idempotencyKey: 'kind-win',
      providerId: 'provider-a',
      externalTransactionId: 'ext-kind-win',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-kind',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '25.00', currency: 'BRL' },
    });

    const refund = await processing.processWager({
      idempotencyKey: 'kind-refund',
      providerId: 'provider-a',
      externalTransactionId: 'ext-kind-refund',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-kind',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: 'ext-kind-win',
    });

    expect(refund.status).toBe(WagerTransactionStatus.Rejected);
    expect(refund.failureCode).toBe(FailureCode.ReferenceIncompatible);
    expect(await walletRow(target.id)).toEqual({ balance: '125.00', version: 2 });
  });

  test('a reversal in a currency the wallet does not hold is refused before any movement', async () => {
    const target = await wallet('100.00');

    await processing.processWager({
      idempotencyKey: 'currency-bet',
      providerId: 'provider-a',
      externalTransactionId: 'ext-currency-bet',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-currency',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });

    await expect(processing.processWager({
      idempotencyKey: 'currency-refund',
      providerId: 'provider-a',
      externalTransactionId: 'ext-currency-refund',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-currency',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '25.00', currency: 'USD' },
      referenceExternalTransactionId: 'ext-currency-bet',
    })).rejects.toThrow(/CURRENCY/i);

    expect(await walletRow(target.id)).toEqual({ balance: '75.00', version: 2 });
  });

  test('the same key with a different payload is a conflict for WIN as well', async () => {
    const target = await wallet('100.00');

    const command = {
      idempotencyKey: 'conflict-win',
      providerId: 'provider-a',
      externalTransactionId: 'ext-conflict-win',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-conflict',
      gameId: 'fortune-chimp',
      kind: 'WIN' as const,
      money: { amount: '10.00', currency: 'BRL' },
    };

    await processing.processWager(command);

    await expect(processing.processWager({
      ...command,
      money: { amount: '11.00', currency: 'BRL' },
    })).rejects.toBeInstanceOf(FinancialConflictError);

    expect(await walletRow(target.id)).toEqual({ balance: '110.00', version: 2 });
  });

  test('changing only the kind under the same key is a conflict, not a replay', async () => {
    const target = await wallet('100.00');

    const base = {
      idempotencyKey: 'conflict-kind',
      providerId: 'provider-a',
      externalTransactionId: 'ext-conflict-kind',
      walletId: target.id,
      playerId: target.playerId,
      roundId: 'round-conflict-kind',
      gameId: 'fortune-chimp',
      money: { amount: '10.00', currency: 'BRL' },
    };

    await processing.processWager({ ...base, kind: 'LOSS' });

    await expect(processing.processWager({ ...base, kind: 'WIN' }))
      .rejects.toBeInstanceOf(FinancialConflictError);

    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 1 });
  });

  test('the ledger reconstructs the stored balance after a full BET/WIN/LOSS/REFUND/ROLLBACK history', async () => {
    const target = await wallet('100.00');

    const flow = [
      { key: 'rec-bet', ext: 'ext-rec-bet', kind: 'BET' as const, amount: '40.00', reference: undefined },
      { key: 'rec-win', ext: 'ext-rec-win', kind: 'WIN' as const, amount: '70.00', reference: undefined },
      { key: 'rec-loss', ext: 'ext-rec-loss', kind: 'LOSS' as const, amount: '15.00', reference: undefined },
      { key: 'rec-refund', ext: 'ext-rec-refund', kind: 'REFUND' as const, amount: '40.00', reference: 'ext-rec-bet' },
      { key: 'rec-rollback', ext: 'ext-rec-rollback', kind: 'ROLLBACK' as const, amount: '70.00', reference: 'ext-rec-win' },
    ];

    for (const step of flow) {
      const result = await processing.processWager({
        idempotencyKey: step.key,
        providerId: 'provider-a',
        externalTransactionId: step.ext,
        walletId: target.id,
        playerId: target.playerId,
        roundId: 'round-rec',
        gameId: 'fortune-chimp',
        kind: step.kind,
        money: { amount: step.amount, currency: 'BRL' },
        ...(step.reference === undefined ? {} : { referenceExternalTransactionId: step.reference }),
      });
      expect(result.status).toBe(WagerTransactionStatus.Processed);
    }

    const stored = await walletRow(target.id);
    expect(stored.balance).toBe('100.00');
    expect(await reconstructedBalance(target.id)).toBe('100.00');

    const ledger = await ledgerOf(target.id);
    expect(ledger.map((entry) => entry.direction)).toEqual(['CREDIT', 'DEBIT', 'CREDIT', 'CREDIT', 'DEBIT']);
    // one ledger entry per balance movement, none for LOSS
    expect(ledger.length).toBe(5);
    expect(stored.version).toBe(5);
  });
});
