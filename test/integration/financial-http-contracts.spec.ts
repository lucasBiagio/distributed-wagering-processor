import 'reflect-metadata';

import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { resolve } from 'node:path';

import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

/**
 * CP4-R3A status-contract evidence over the real production entrypoint.  This suite does not
 * replace processing with a scripted mock: every 200/202/422 result is stored in PostgreSQL and
 * every replay crosses the same HTTP/Nest/controller/ORM boundary as a provider request.
 */
const PORT = Number(Bun.env.CP4_HTTP_CONTRACT_PORT ?? 3126);
const workspaceRoot = resolve(import.meta.dir, '../..');
const baseUrl = `http://127.0.0.1:${PORT}`;

let orm: MikroORM<PostgreSqlDriver>;
let server: ReturnType<typeof Bun.spawn> | undefined;
let sequence = 0;

interface HttpResult {
  readonly status: number;
  readonly body: Record<string, unknown>;
}

function nextUuid(): string {
  sequence += 1;
  return `00000000-0000-4000-8000-${String(910_000 + sequence).padStart(12, '0')}`;
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
  throw new Error(`CP4 HTTP contract server did not become live: ${lastError}`);
}

function startServer(): ReturnType<typeof Bun.spawn> {
  return Bun.spawn(['bun', 'run', 'src/main.ts'], {
    cwd: workspaceRoot,
    env: { ...Bun.env, PORT: String(PORT) },
    stdout: 'ignore',
    stderr: 'inherit',
  });
}

async function call(
  method: 'GET' | 'POST',
  path: string,
  body?: unknown,
  idempotencyKey?: string,
): Promise<HttpResult> {
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

async function createWallet(balance = '100.00'): Promise<{ readonly id: string; readonly playerId: string }> {
  const playerId = nextUuid();
  const response = await call('POST', '/wallets', {
    playerId,
    initialBalance: { amount: balance, currency: 'BRL' },
  });
  expect(response.status).toBe(201);
  return { id: response.body.id as string, playerId };
}

function wager(
  wallet: { readonly id: string; readonly playerId: string },
  suffix: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    providerId: 'cp4-contract-provider',
    externalTransactionId: `cp4-contract-${suffix}`,
    walletId: wallet.id,
    playerId: wallet.playerId,
    roundId: `cp4-contract-round-${suffix}`,
    gameId: 'cp4-contract-game',
    kind: 'BET',
    money: { amount: '10.00', currency: 'BRL' },
    ...overrides,
  };
}

async function count(table: 'wallet_ledger_entries' | 'outbox_messages', walletId: string): Promise<number> {
  const column = table === 'wallet_ledger_entries' ? 'wallet_id' : 'wallet_id';
  const rows = await orm.em.getConnection().execute(
    `select count(*)::int as count from ${table} where ${column} = ?`,
    [walletId],
  ) as Array<{ count: number }>;
  return rows[0]?.count ?? 0;
}

describe('CP4-R3A HTTP persisted-outcome contracts', () => {
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

  test('returns persisted 200, 422 and 202 outcomes and replays each without another ledger or outbox effect', async () => {
    const processedWallet = await createWallet();
    const processedRequest = wager(processedWallet, 'processed');
    const processed = await call('POST', '/wagering/transactions', processedRequest, 'cp4-contract-processed');
    const processedLedger = await count('wallet_ledger_entries', processedWallet.id);
    const processedOutbox = await count('outbox_messages', processedWallet.id);
    const processedReplay = await call('POST', '/wagering/transactions', processedRequest, 'cp4-contract-processed');

    expect(processed).toMatchObject({
      status: 200,
      body: { status: 'PROCESSED', idempotentReplay: false, balance: { amount: '90.00', currency: 'BRL' } },
    });
    expect(typeof processed.body.transactionId).toBe('string');
    expect(processedReplay).toMatchObject({
      status: 200,
      body: { transactionId: processed.body.transactionId, status: 'PROCESSED', idempotentReplay: true },
    });
    expect(await count('wallet_ledger_entries', processedWallet.id)).toBe(processedLedger);
    expect(await count('outbox_messages', processedWallet.id)).toBe(processedOutbox);

    const rejectedWallet = await createWallet();
    const rejectedRequest = wager(rejectedWallet, 'rejected', { money: { amount: '200.00', currency: 'BRL' } });
    const rejected = await call('POST', '/wagering/transactions', rejectedRequest, 'cp4-contract-rejected');
    const rejectedLedger = await count('wallet_ledger_entries', rejectedWallet.id);
    const rejectedOutbox = await count('outbox_messages', rejectedWallet.id);
    const rejectedReplay = await call('POST', '/wagering/transactions', rejectedRequest, 'cp4-contract-rejected');

    expect(rejected).toMatchObject({
      status: 422,
      body: {
        status: 'REJECTED',
        failureCode: 'INSUFFICIENT_BALANCE',
        idempotentReplay: false,
        balance: { amount: '100.00', currency: 'BRL' },
      },
    });
    expect(typeof rejected.body.transactionId).toBe('string');
    expect(rejectedReplay).toMatchObject({
      status: 422,
      body: {
        transactionId: rejected.body.transactionId,
        status: 'REJECTED',
        failureCode: 'INSUFFICIENT_BALANCE',
        idempotentReplay: true,
      },
    });
    expect(await count('wallet_ledger_entries', rejectedWallet.id)).toBe(rejectedLedger);
    expect(await count('outbox_messages', rejectedWallet.id)).toBe(rejectedOutbox);

    const pendingWallet = await createWallet();
    const pendingRequest = wager(pendingWallet, 'pending', {
      kind: 'REFUND',
      referenceExternalTransactionId: 'cp4-contract-missing-reference',
    });
    const pending = await call('POST', '/wagering/transactions', pendingRequest, 'cp4-contract-pending');
    const pendingLedger = await count('wallet_ledger_entries', pendingWallet.id);
    const pendingOutbox = await count('outbox_messages', pendingWallet.id);
    const pendingReplay = await call('POST', '/wagering/transactions', pendingRequest, 'cp4-contract-pending');

    expect(pending).toMatchObject({
      status: 202,
      body: {
        status: 'PENDING_REFERENCE',
        failureCode: null,
        idempotentReplay: false,
        // A parked reversal has no resulting balance yet. Its wallet is unchanged, but the
        // persisted transaction correctly represents its unresolved result as `balance: null`.
        balance: null,
      },
    });
    expect(typeof pending.body.transactionId).toBe('string');
    expect(pendingReplay).toMatchObject({
      status: 202,
      body: { transactionId: pending.body.transactionId, status: 'PENDING_REFERENCE', idempotentReplay: true },
    });
    expect(await count('wallet_ledger_entries', pendingWallet.id)).toBe(pendingLedger);
    expect(await count('outbox_messages', pendingWallet.id)).toBe(pendingOutbox);
  });

  test('returns 409 for an idempotency-key conflict and 400 for invalid payload without creating a transaction', async () => {
    const target = await createWallet();
    const request = wager(target, 'conflict');
    const first = await call('POST', '/wagering/transactions', request, 'cp4-contract-conflict');
    const conflict = await call('POST', '/wagering/transactions', wager(target, 'conflict', { money: { amount: '11.00', currency: 'BRL' } }), 'cp4-contract-conflict');
    const invalid = await call('POST', '/wagering/transactions', wager(target, 'invalid', { money: { amount: 10, currency: 'BRL' } }), 'cp4-contract-invalid');
    const rows = await orm.em.getConnection().execute(
      "select count(*)::int as count from wager_transactions where wallet_id = ? and kind <> 'OPENING'",
      [target.id],
    ) as Array<{ count: number }>;

    expect(first.status).toBe(200);
    expect(conflict).toMatchObject({ status: 409, body: { failureCode: 'IDEMPOTENCY_CONFLICT' } });
    expect(invalid).toMatchObject({ status: 400, body: { failureCode: 'INVALID_PAYLOAD' } });
    expect(rows[0]?.count).toBe(1);
  });
});
