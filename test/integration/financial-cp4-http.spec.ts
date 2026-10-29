import 'reflect-metadata';

import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { resolve } from 'node:path';

import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

// Bun 1.3's test worker exposes its inherited environment via Bun.env.
const PORT = Number(Bun.env.CP4_HTTP_PORT ?? 3124);
const workspaceRoot = resolve(import.meta.dir, '../..');
let orm: MikroORM<PostgreSqlDriver>;
let server: ReturnType<typeof Bun.spawn> | undefined;
const baseUrl = `http://127.0.0.1:${PORT}`;
let sequence = 0;

interface HttpResult {
  readonly status: number;
  readonly body: Record<string, unknown>;
}

function playerId(): string {
  sequence += 1;
  return `00000000-0000-4000-8000-${String(800_000 + sequence).padStart(12, '0')}`;
}

async function waitForLive(): Promise<void> {
  const deadline = Date.now() + 60_000;
  let lastError = 'no response';
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseUrl}/health/live`);
      if (response.ok) return;
      lastError = `HTTP ${response.status}`;
    } catch (error) {
      lastError = error instanceof Error ? error.message : 'unknown error';
    }
    await Bun.sleep(250);
  }
  throw new Error(`CP4 HTTP server did not become live: ${lastError}`);
}

function startServer(): ReturnType<typeof Bun.spawn> {
  return Bun.spawn(['bun', 'run', 'src/main.ts'], {
    cwd: workspaceRoot,
    env: { ...Bun.env, PORT: String(PORT) },
    stdout: 'ignore',
    stderr: 'inherit',
  });
}

async function call(method: 'GET' | 'POST', path: string, body?: unknown, idempotencyKey?: string): Promise<HttpResult> {
  const headers: Record<string, string> = {};
  if (body !== undefined) headers['content-type'] = 'application/json';
  if (idempotencyKey !== undefined) headers['idempotency-key'] = idempotencyKey;
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const text = await response.text();
  return { status: response.status, body: text === '' ? {} : JSON.parse(text) as Record<string, unknown> };
}

async function wallet(initialBalance = '100.00'): Promise<{ id: string; playerId: string }> {
  const player = playerId();
  const opened = await call('POST', '/wallets', {
    playerId: player,
    initialBalance: { amount: initialBalance, currency: 'BRL' },
  });
  expect(opened.status).toBe(201);
  return { id: opened.body.id as string, playerId: player };
}

function wager(target: { id: string; playerId: string }, kind: string, amount: string, suffix: string, referenceExternalTransactionId?: string): Record<string, unknown> {
  return {
    providerId: 'cp4-http-provider',
    externalTransactionId: `cp4-http-${suffix}`,
    walletId: target.id,
    playerId: target.playerId,
    roundId: 'cp4-http-round',
    gameId: 'cp4-http-game',
    kind,
    money: { amount, currency: 'BRL' },
    ...(referenceExternalTransactionId === undefined ? {} : { referenceExternalTransactionId }),
  };
}

async function process(target: { id: string; playerId: string }, kind: string, amount: string, suffix: string, referenceExternalTransactionId?: string): Promise<HttpResult> {
  return call('POST', '/wagering/transactions', wager(target, kind, amount, suffix, referenceExternalTransactionId), `cp4-http-key-${suffix}`);
}

function entries(body: Record<string, unknown>): Array<Record<string, unknown>> {
  return body.entries as Array<Record<string, unknown>>;
}

describe('CP4 ledger and reconciliation HTTP integration', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
    server = startServer();
    await waitForLive();
  }, 30_000);

  beforeEach(async () => {
    await orm.em.getConnection().execute('truncate table outbox_messages, wallet_ledger_entries, wager_transactions, wallets cascade');
  });

  afterAll(async () => {
    if (server) {
      server.kill();
      await server.exited;
    }
    await orm.getMigrator().down({ to: 0 });
    await orm.close(true);
  });

  test('ledger is paginated by wallet-bound opaque keyset cursor without gaps, duplicates, or numeric money', async () => {
    const target = await wallet();
    const bet = await process(target, 'BET', '10.00', 'bet');
    const win = await process(target, 'WIN', '30.00', 'win');
    const loss = await process(target, 'LOSS', '0.00', 'loss');
    const refund = await process(target, 'REFUND', '10.00', 'refund', 'cp4-http-bet');
    const rollback = await process(target, 'ROLLBACK', '30.00', 'rollback', 'cp4-http-win');
    for (const result of [bet, win, loss, refund, rollback]) expect(result.status).toBe(200);

    // Ledger rows are immutable by design. Seed two additional valid processed WIN rows at
    // exactly the same instant instead of mutating historic entries; this exercises the UUID
    // tie-breaker while preserving every database guard.
    await orm.em.getConnection().execute(
      `insert into wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency,
        reference_transaction_id, reference_external_transaction_id, status, failure_code,
        processed_at, resulting_balance, created_at
      ) values
        ('00000000-0000-4000-8000-00000000aa01', 'cp4-http-provider', 'cp4-http-tie-1', 'cp4-http-tie-key-1', 'cp4-http-tie-hash-1', ?, ?, 'cp4-http-tie-round', 'cp4-http-game', 'WIN', 1.00, 'BRL', null, null, 'PROCESSED', null, '2099-01-01T00:00:00.000Z', 101.00, '2099-01-01T00:00:00.000Z'),
        ('00000000-0000-4000-8000-00000000aa02', 'cp4-http-provider', 'cp4-http-tie-2', 'cp4-http-tie-key-2', 'cp4-http-tie-hash-2', ?, ?, 'cp4-http-tie-round', 'cp4-http-game', 'WIN', 1.00, 'BRL', null, null, 'PROCESSED', null, '2099-01-01T00:00:00.000Z', 102.00, '2099-01-01T00:00:00.000Z')`,
      [target.id, target.playerId, target.id, target.playerId],
    );
    await orm.em.getConnection().execute(
      `insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at) values
        ('00000000-0000-4000-8000-00000000bb01', ?, '00000000-0000-4000-8000-00000000aa01', 'CREDIT', 1.00, 'BRL', 100.00, 101.00, '2099-01-01T00:00:00.000Z'),
        ('00000000-0000-4000-8000-00000000bb02', ?, '00000000-0000-4000-8000-00000000aa02', 'CREDIT', 1.00, 'BRL', 101.00, 102.00, '2099-01-01T00:00:00.000Z')`,
      [target.id, target.id],
    );
    await orm.em.getConnection().execute('update wallets set balance = 102.00, version = version + 1, updated_at = now() where id = ?', [target.id]);

    const seededLedger = await orm.em.getConnection().execute(
      'select count(*)::int as count from wallet_ledger_entries where wallet_id = ?',
      [target.id],
    ) as Array<{ count: number }>;
    expect(seededLedger[0]?.count).toBe(7);

    const first = await call('GET', `/wallets/${target.id}/ledger?limit=2`);
    expect(first.status).toBe(200);
    expect(first.body).toMatchObject({ walletId: target.id, hasMore: true });
    expect(typeof first.body.nextCursor).toBe('string');
    const decoded = JSON.parse(Buffer.from(first.body.nextCursor as string, 'base64url').toString('utf8')) as Record<string, unknown>;
    expect(decoded).toMatchObject({ v: 1, walletId: target.id });

    const second = await call('GET', `/wallets/${target.id}/ledger?limit=2&cursor=${encodeURIComponent(first.body.nextCursor as string)}`);
    const third = await call('GET', `/wallets/${target.id}/ledger?limit=100&cursor=${encodeURIComponent(second.body.nextCursor as string)}`);
    expect(second.status).toBe(200);
    expect(third.status).toBe(200);
    expect(third.body).toMatchObject({ hasMore: false, nextCursor: null });

    const all = [...entries(first.body), ...entries(second.body), ...entries(third.body)];
    expect(all).toHaveLength(7);
    expect(new Set(all.map((entry) => entry.id)).size).toBe(7);
    expect(all.map((entry) => entry.kind)).toEqual(['OPENING', 'BET', 'WIN', 'REFUND', 'ROLLBACK', 'WIN', 'WIN']);
    expect(all.map((entry) => entry.balanceBefore)).toEqual(['0.00', '100.00', '90.00', '120.00', '130.00', '100.00', '101.00']);
    expect(all.map((entry) => entry.balanceAfter)).toEqual(['100.00', '90.00', '120.00', '130.00', '100.00', '101.00', '102.00']);
    for (const entry of all) {
      expect(entry.money).toMatchObject({ currency: 'BRL' });
      expect(typeof (entry.money as Record<string, unknown>).amount).toBe('string');
      expect(typeof entry.balanceBefore).toBe('string');
      expect(typeof entry.balanceAfter).toBe('string');
      expect(typeof entry.createdAt).toBe('string');
    }

    const other = await wallet('0.00');
    const foreignCursor = await call('GET', `/wallets/${other.id}/ledger?cursor=${encodeURIComponent(first.body.nextCursor as string)}`);
    const malformed = await call('GET', `/wallets/${target.id}/ledger?cursor=not-base64-json`);
    const invalidLimit = await call('GET', `/wallets/${target.id}/ledger?limit=101`);
    const zeroLimit = await call('GET', `/wallets/${target.id}/ledger?limit=0`);
    const defaultLimit = await call('GET', `/wallets/${target.id}/ledger`);
    const missing = await call('GET', '/wallets/00000000-0000-4000-8000-000000999999/ledger');
    expect(foreignCursor).toMatchObject({ status: 400, body: { failureCode: 'INVALID_LEDGER_CURSOR' } });
    expect(malformed).toMatchObject({ status: 400, body: { failureCode: 'INVALID_LEDGER_CURSOR' } });
    expect(invalidLimit).toMatchObject({ status: 400, body: { failureCode: 'INVALID_LEDGER_CURSOR' } });
    expect(zeroLimit).toMatchObject({ status: 400, body: { failureCode: 'INVALID_LEDGER_CURSOR' } });
    expect(entries(defaultLimit.body)).toHaveLength(7);
    expect(missing).toMatchObject({ status: 404, body: { failureCode: 'WALLET_NOT_FOUND' } });
  });

  test('reconciliation returns the ledger-derived balance, detects both directions of artificial divergence, and never repairs it', async () => {
    const target = await wallet('50.00');
    expect((await process(target, 'BET', '10.00', 'reconciliation-bet')).status).toBe(200);
    expect((await process(target, 'WIN', '5.00', 'reconciliation-win')).status).toBe(200);

    const consistent = await call('POST', `/wallets/${target.id}/reconciliation`);
    expect(consistent).toMatchObject({
      status: 200,
      body: { walletId: target.id, storedBalance: '45.00', calculatedBalance: '45.00', difference: '0.00', consistent: true, checkedEntries: 3 },
    });

    await orm.em.getConnection().execute('update wallets set balance = 50.00, version = version + 1, updated_at = now() where id = ?', [target.id]);
    const positive = await call('POST', `/wallets/${target.id}/reconciliation`);
    expect(positive).toMatchObject({ status: 200, body: { storedBalance: '50.00', calculatedBalance: '45.00', difference: '5.00', consistent: false } });
    const afterPositive = await call('GET', `/wallets/${target.id}`);
    expect(afterPositive.body).toMatchObject({ balance: { amount: '50.00', currency: 'BRL' } });

    await orm.em.getConnection().execute('update wallets set balance = 40.00, version = version + 1, updated_at = now() where id = ?', [target.id]);
    const negative = await call('POST', `/wallets/${target.id}/reconciliation`);
    expect(negative).toMatchObject({ status: 200, body: { storedBalance: '40.00', calculatedBalance: '45.00', difference: '-5.00', consistent: false } });
    const afterNegative = await call('GET', `/wallets/${target.id}`);
    expect(afterNegative.body).toMatchObject({ balance: { amount: '40.00', currency: 'BRL' } });

    const missing = await call('POST', '/wallets/00000000-0000-4000-8000-000000999998/reconciliation');
    expect(missing).toMatchObject({ status: 404, body: { failureCode: 'WALLET_NOT_FOUND' } });
  });
});
