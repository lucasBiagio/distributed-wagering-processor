import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { MikroOrmFinancialProcessing } from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';
import { FailureCode, WagerTransactionStatus } from '../../src/domain/wager-transaction.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

let orm: MikroORM<PostgreSqlDriver>;
let processing: MikroOrmFinancialProcessing;
let sequence = 0;

function uuid(suffix: number): string {
  return `00000000-0000-4000-8000-${String(suffix).padStart(12, '0')}`;
}

async function openWallet(balance: string): Promise<{ walletId: string; playerId: string }> {
  sequence += 1;
  const playerId = uuid(sequence + 100_000);
  const wallet = await processing.openWallet({ playerId, initialBalance: { amount: balance, currency: 'BRL' } });
  return { walletId: wallet.id, playerId };
}

function bet(walletId: string, playerId: string, idempotencyKey: string, externalTransactionId: string, amount: string) {
  return processing.processBet({
    idempotencyKey,
    providerId: 'provider-a',
    externalTransactionId,
    walletId,
    playerId,
    roundId: `round-${externalTransactionId}`,
    gameId: 'game-a',
    kind: 'BET',
    money: { amount, currency: 'BRL' },
  });
}

async function exactLedgerBalance(walletId: string): Promise<string> {
  const rows = await orm.em.getConnection().execute(`
    select coalesce(sum(case direction when 'CREDIT' then amount else -amount end), 0)::text as balance
    from wallet_ledger_entries
    where wallet_id = '${walletId}'
  `) as Array<{ balance: string }>;
  return rows[0]!.balance;
}

describe('CP2B-1 financial processing', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
    processing = new MikroOrmFinancialProcessing(orm);
  });

  afterAll(async () => {
    await orm.getMigrator().down({ to: 0 });
    await orm.close(true);
  });

  test('opens zero and positive wallets atomically with OPENING ledger only for positive balance', async () => {
    const zero = await openWallet('0.00');
    const positive = await openWallet('100.00');

    const zeroRows = await orm.em.getConnection().execute(`select count(*)::int as count from wallet_ledger_entries where wallet_id = '${zero.walletId}'`) as Array<{ count: number }>;
    const positiveRows = await orm.em.getConnection().execute(`
      select w.version, w.balance::text as balance, t.kind, t.status, l.direction
      from wallets w
      join wager_transactions t on t.wallet_id = w.id
      join wallet_ledger_entries l on l.transaction_id = t.id
      where w.id = '${positive.walletId}'
    `) as Array<{ version: number; balance: string; kind: string; status: string; direction: string }>;

    expect(zeroRows[0]!.count).toBe(0);
    expect(positiveRows).toEqual([{ version: 1, balance: '100.00', kind: 'OPENING', status: 'PROCESSED', direction: 'CREDIT' }]);
  });

  test('records insufficient balance as durable REJECTED with immutable snapshot and no ledger mutation', async () => {
    const { walletId, playerId } = await openWallet('10.00');
    const result = await bet(walletId, playerId, 'insufficient-key', 'external-insufficient', '25.00');
    const replay = await bet(walletId, playerId, 'insufficient-key', 'external-insufficient', '25.00');
    const wallet = await processing.getWallet(walletId);
    const rows = await orm.em.getConnection().execute(`select count(*)::int as count from wallet_ledger_entries where wallet_id = '${walletId}'`) as Array<{ count: number }>;

    expect(result.status).toBe(WagerTransactionStatus.Rejected);
    expect(result.failureCode).toBe(FailureCode.InsufficientBalance);
    expect(result.balance).toEqual({ amount: '10.00', currency: 'BRL' });
    expect(replay).toEqual({ ...result, idempotentReplay: true });
    expect(wallet).toMatchObject({ balance: { amount: '10.00', currency: 'BRL' }, version: 1 });
    expect(rows[0]!.count).toBe(1);
    await expect(orm.em.getConnection().execute(`delete from wager_transactions where id = '${result.transactionId}'`)).rejects.toThrow('terminal wager transactions cannot be deleted');
  });

  test('TEST-A: fifty identical concurrent BET requests create one debit and forty-nine historical replays', async () => {
    const { walletId, playerId } = await openWallet('1000.00');
    const results = await Promise.all(Array.from({ length: 50 }, () => bet(walletId, playerId, 'same-key', 'external-same', '25.00')));
    const wallet = await processing.getWallet(walletId);
    const debitCount = await orm.em.getConnection().execute(`
      select count(*)::int as count from wallet_ledger_entries where wallet_id = '${walletId}' and direction = 'DEBIT'
    `) as Array<{ count: number }>;

    expect(results.filter((result) => !result.idempotentReplay)).toHaveLength(1);
    expect(results.filter((result) => result.idempotentReplay)).toHaveLength(49);
    expect(new Set(results.map((result) => result.transactionId)).size).toBe(1);
    expect(wallet).toMatchObject({ balance: { amount: '975.00', currency: 'BRL' }, version: 2 });
    expect(debitCount[0]!.count).toBe(1);
    expect(await exactLedgerBalance(walletId)).toBe('975.00');

    const events = await orm.em.getConnection().execute(`
      select event_type, count(*)::int as count
      from outbox_messages
      where aggregate_id in ('${walletId}', '${results[0]!.transactionId}')
      group by event_type
      order by event_type
    `) as Array<{ event_type: string; count: number }>;
    expect(events).toEqual([
      { event_type: 'WagerTransactionProcessed', count: 1 },
      { event_type: 'WalletBalanceChanged', count: 2 },
    ]);
  });

  test('TEST-B: competing distinct BETs on the same balance produce exactly one debit and one durable rejection', async () => {
    const { walletId, playerId } = await openWallet('100.00');
    const results = await Promise.all([
      bet(walletId, playerId, 'race-a', 'external-race-a', '80.00'),
      bet(walletId, playerId, 'race-b', 'external-race-b', '80.00'),
    ]);
    const wallet = await processing.getWallet(walletId);
    const debitCount = await orm.em.getConnection().execute(`
      select count(*)::int as count from wallet_ledger_entries where wallet_id = '${walletId}' and direction = 'DEBIT'
    `) as Array<{ count: number }>;

    expect(results.filter((result) => result.status === 'PROCESSED')).toHaveLength(1);
    expect(results.filter((result) => result.status === 'REJECTED' && result.failureCode === 'INSUFFICIENT_BALANCE')).toHaveLength(1);
    expect(wallet).toMatchObject({ balance: { amount: '20.00', currency: 'BRL' }, version: 2 });
    expect(debitCount[0]!.count).toBe(1);
    expect(await exactLedgerBalance(walletId)).toBe('20.00');
  });

  test('TEST-C and TEST-E: independent wallets proceed correctly and replay retains historical balance', async () => {
    const first = await openWallet('100.00');
    const second = await openWallet('100.00');
    const [a, b] = await Promise.all([
      bet(first.walletId, first.playerId, 'history-a', 'external-history-a', '25.00'),
      bet(second.walletId, second.playerId, 'independent-b', 'external-independent-b', '30.00'),
    ]);
    await bet(first.walletId, first.playerId, 'history-b', 'external-history-b', '10.00');
    const replay = await bet(first.walletId, first.playerId, 'history-a', 'external-history-a', '25.00');

    expect(a.balance).toEqual({ amount: '75.00', currency: 'BRL' });
    expect(b.balance).toEqual({ amount: '70.00', currency: 'BRL' });
    expect(replay).toEqual({ ...a, idempotentReplay: true });
    expect(await processing.getWallet(first.walletId)).toMatchObject({ balance: { amount: '65.00', currency: 'BRL' } });
    expect(await processing.getWallet(second.walletId)).toMatchObject({ balance: { amount: '70.00', currency: 'BRL' } });
  });

  test('TEST-D: three independent Bun processes share PostgreSQL idempotency safely', async () => {
    const { walletId, playerId } = await openWallet('1000.00');
    const command = {
      idempotencyKey: 'three-process-key',
      providerId: 'provider-a',
      externalTransactionId: 'external-three-process',
      walletId,
      playerId,
      roundId: 'round-three-process',
      gameId: 'game-a',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };
    const processes = Array.from({ length: 3 }, () => Bun.spawn(['bun', 'run', 'scripts/cp2b1-process-worker.ts'], {
      cwd: process.cwd(),
      env: { ...process.env, CP2B1_COMMAND: JSON.stringify(command) },
      stdout: 'pipe',
      stderr: 'pipe',
    }));
    const results = await Promise.all(processes.map(async (child) => {
      const stdout = await new Response(child.stdout).text();
      const stderr = await new Response(child.stderr).text();
      await child.exited;
      expect(child.exitCode, stderr).toBe(0);
      return JSON.parse(stdout) as { idempotentReplay: boolean; transactionId: string };
    }));
    const wallet = await processing.getWallet(walletId);

    expect(results.filter((result) => !result.idempotentReplay)).toHaveLength(1);
    expect(results.filter((result) => result.idempotentReplay)).toHaveLength(2);
    expect(new Set(results.map((result) => result.transactionId)).size).toBe(1);
    expect(wallet).toMatchObject({ balance: { amount: '975.00', currency: 'BRL' }, version: 2 });
    expect(await exactLedgerBalance(walletId)).toBe('975.00');
  });

  test('rejects same idempotency key with changed business payload and duplicate provider external transaction', async () => {
    const { walletId, playerId } = await openWallet('100.00');
    await bet(walletId, playerId, 'conflict-key', 'external-conflict', '10.00');
    await expect(bet(walletId, playerId, 'conflict-key', 'external-conflict', '11.00')).rejects.toThrow('different payload');
    await expect(bet(walletId, playerId, 'second-key', 'external-conflict', '10.00')).rejects.toThrow('already exists');
  });
});
