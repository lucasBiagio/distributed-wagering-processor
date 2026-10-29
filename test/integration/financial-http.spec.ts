import 'reflect-metadata';

import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

/**
 * NestFactory.create cannot be hosted inside the `bun test` harness (the runner dies
 * silently while building the module graph), so the suite starts the real production
 * entrypoint `src/main.ts` as a separate process and drives it over real HTTP.
 */
const PORT = Number(process.env.CP2B1_HTTP_PORT ?? 3100);

let orm: MikroORM<PostgreSqlDriver>;
let server: ReturnType<typeof Bun.spawn>;
let baseUrl: string;
let wallet: { id: string; playerId: string };
let uuidV7Wallet: { id: string; playerId: string };

/** CP2B-1-R3 FIX-01: the exact UUIDv7 player identifier used by CHALLENGE.md. */
const CHALLENGE_PLAYER_ID = '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1';

function uuid(suffix: number): string {
  return `00000000-0000-4000-8000-${String(suffix).padStart(12, '0')}`;
}

async function waitForLive(url: string): Promise<void> {
  const deadline = Date.now() + 60_000;
  let lastError = 'no attempt';
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${url}/health/live`);
      if (response.ok) {
        return;
      }
      lastError = `status ${response.status}`;
    } catch (error) {
      lastError = (error as Error).message;
    }
    await Bun.sleep(250);
  }
  throw new Error(`HTTP server did not become live: ${lastError}`);
}

function startServer(port: number, extraEnv: Record<string, string> = {}): ReturnType<typeof Bun.spawn> {
  return Bun.spawn(['bun', 'run', 'src/main.ts'], {
    cwd: process.cwd(),
    env: { ...process.env, PORT: String(port), ...extraEnv },
    stdout: 'ignore',
    stderr: 'inherit',
  });
}

async function waitForServer(): Promise<void> {
  return waitForLive(baseUrl);
}

interface HttpResult {
  readonly status: number;
  readonly body: Record<string, unknown>;
}

async function call(
  method: 'GET' | 'POST',
  path: string,
  options: { body?: unknown; idempotencyKey?: string } = {},
): Promise<HttpResult> {
  const headers: Record<string, string> = {};
  if (options.body !== undefined) {
    headers['content-type'] = 'application/json';
  }
  if (options.idempotencyKey !== undefined) {
    headers['idempotency-key'] = options.idempotencyKey;
  }
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    ...(options.body === undefined ? {} : { body: JSON.stringify(options.body) }),
  });
  const text = await response.text();
  return {
    status: response.status,
    body: text.length === 0 ? {} : (JSON.parse(text) as Record<string, unknown>),
  };
}

function betBody(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    providerId: 'provider-http',
    externalTransactionId: 'external-http-base',
    walletId: wallet.id,
    playerId: wallet.playerId,
    roundId: 'round-http-base',
    gameId: 'game-http',
    kind: 'BET',
    money: { amount: '25.00', currency: 'BRL' },
    ...overrides,
  };
}

describe('CP2B-1-R1 HTTP end to end', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();

    baseUrl = `http://127.0.0.1:${PORT}`;
    server = startServer(PORT);
    await waitForServer();
  });

  afterAll(async () => {
    server.kill();
    await server.exited;
    await orm.getMigrator().down({ to: 0 });
    await orm.close(true);
  });

  test('TEST-03: health endpoints keep working after the financial modules were added', async () => {
    const live = await call('GET', '/health/live');
    const ready = await call('GET', '/health/ready');

    expect(live.status).toBe(200);
    expect(live.body).toEqual({ status: 'ok' });
    expect(ready.status).toBe(200);
    expect(ready.body).toEqual({ status: 'ok', dependencies: { postgres: 'ok', sqs: 'ok' } });
  });

  test('TEST-03: readiness reports an unavailable dependency without touching shared infrastructure', async () => {
    const degradedPort = PORT + 1;
    const degradedUrl = `http://127.0.0.1:${degradedPort}`;
    // a second server process pointed at an unreachable SQS endpoint; the real dependency
    // is never stopped, so no shared infrastructure is affected
    const degraded = startServer(degradedPort, { SQS_ENDPOINT: 'http://127.0.0.1:1' });

    try {
      await waitForLive(degradedUrl);
      const live = await fetch(`${degradedUrl}/health/live`);
      const ready = await fetch(`${degradedUrl}/health/ready`);
      const body = await ready.json() as Record<string, unknown>;

      expect(live.status).toBe(200);
      expect(ready.status).toBe(503);
      expect(body).toMatchObject({
        status: 'error',
        dependencies: { postgres: 'ok', sqs: 'error' },
      });
    } finally {
      degraded.kill();
      await degraded.exited;
    }
  });

  test('TEST-02 case 1: POST /wallets creates a wallet with 100.00 BRL', async () => {
    const playerId = uuid(700_001);
    const created = await call('POST', '/wallets', {
      body: { playerId, initialBalance: { amount: '100.00', currency: 'BRL' } },
    });

    expect(created.status).toBe(201);
    expect(created.body).toMatchObject({
      playerId,
      balance: { amount: '100.00', currency: 'BRL' },
      version: 1,
    });

    wallet = { id: created.body.id as string, playerId };

    const fetched = await call('GET', `/wallets/${wallet.id}`);
    expect(fetched.status).toBe(200);
    expect(fetched.body).toEqual(created.body);
  });

  test('TEST-02 case 2: POST /wagering/transactions debits a BET of 25.00 and returns 75.00', async () => {
    const processed = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-base-key',
      body: betBody(),
    });

    expect(processed.status).toBe(200);
    expect(processed.body).toMatchObject({
      status: 'PROCESSED',
      kind: 'BET',
      walletId: wallet.id,
      balance: { amount: '75.00', currency: 'BRL' },
      failureCode: null,
      idempotentReplay: false,
    });

    const transactionId = processed.body.transactionId as string;
    const byId = await call('GET', `/wagering/transactions/${transactionId}`);
    const byProvider = await call('GET', '/providers/provider-http/wagering/transactions/external-http-base');
    const walletState = await call('GET', `/wallets/${wallet.id}`);

    expect(byId.status).toBe(200);
    expect(byId.body).toMatchObject({ transactionId, status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' } });
    expect(byProvider.status).toBe(200);
    expect(byProvider.body).toMatchObject({ transactionId });
    expect(walletState.body).toMatchObject({ balance: { amount: '75.00', currency: 'BRL' }, version: 2 });
  });

  test('TEST-02 case 2a: POST /wagering/transactions accepts LOSS 0.00 without changing the wallet', async () => {
    const processed = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-loss-zero-key',
      body: betBody({
        kind: 'LOSS',
        externalTransactionId: 'external-http-loss-zero',
        roundId: 'round-http-loss-zero',
        money: { amount: '0.00', currency: 'BRL' },
      }),
    });
    const walletState = await call('GET', `/wallets/${wallet.id}`);

    expect(processed.status).toBe(200);
    expect(processed.body).toMatchObject({
      kind: 'LOSS',
      status: 'PROCESSED',
      balance: { amount: '75.00', currency: 'BRL' },
      idempotentReplay: false,
    });
    expect(walletState.body).toMatchObject({ balance: { amount: '75.00', currency: 'BRL' }, version: 2 });
  });

  test('TEST-02 case 3: the same Idempotency-Key and payload replays the stored result', async () => {
    const replay = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-base-key',
      body: betBody(),
    });
    const walletState = await call('GET', `/wallets/${wallet.id}`);

    expect(replay.status).toBe(200);
    expect(replay.body).toMatchObject({
      status: 'PROCESSED',
      balance: { amount: '75.00', currency: 'BRL' },
      idempotentReplay: true,
    });
    expect(walletState.body).toMatchObject({ balance: { amount: '75.00', currency: 'BRL' }, version: 2 });
  });

  test('TEST-02 case 4: the same Idempotency-Key with a different payload is a 409 conflict', async () => {
    const conflict = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-base-key',
      body: betBody({ money: { amount: '26.00', currency: 'BRL' } }),
    });

    expect(conflict.status).toBe(409);
    expect(conflict.body).toMatchObject({ failureCode: 'IDEMPOTENCY_CONFLICT' });
  });

  test('TEST-02 case 5: a BET above the balance is a durable REJECTED with failureCode', async () => {
    const rejected = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-insufficient-key',
      body: betBody({
        externalTransactionId: 'external-http-insufficient',
        roundId: 'round-http-insufficient',
        money: { amount: '500.00', currency: 'BRL' },
      }),
    });
    const walletState = await call('GET', `/wallets/${wallet.id}`);

    expect(rejected.status).toBe(200);
    expect(rejected.body).toMatchObject({
      status: 'REJECTED',
      failureCode: 'INSUFFICIENT_BALANCE',
      balance: { amount: '75.00', currency: 'BRL' },
    });
    expect(walletState.body).toMatchObject({ balance: { amount: '75.00', currency: 'BRL' }, version: 2 });
  });

  test('TEST-02 case 6: a missing Idempotency-Key is rejected before any financial work', async () => {
    const missing = await call('POST', '/wagering/transactions', {
      body: betBody({ externalTransactionId: 'external-http-no-key', roundId: 'round-http-no-key' }),
    });

    expect(missing.status).toBe(400);
    expect(missing.body).toMatchObject({ failureCode: 'IDEMPOTENCY_KEY_REQUIRED' });
  });

  test('TEST-02 case 7: a numeric monetary amount is an invalid payload', async () => {
    const numeric = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-number-key',
      body: betBody({
        externalTransactionId: 'external-http-number',
        roundId: 'round-http-number',
        money: { amount: 25.0, currency: 'BRL' },
      }),
    });

    expect(numeric.status).toBe(400);
    expect(numeric.body).toMatchObject({ failureCode: 'INVALID_PAYLOAD' });
  });

  test('TEST-02 case 8: an unknown wallet is a 404 and an unknown transaction is a 404', async () => {
    const unknownWallet = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-unknown-wallet-key',
      body: betBody({
        walletId: uuid(799_999),
        externalTransactionId: 'external-http-unknown-wallet',
        roundId: 'round-http-unknown-wallet',
      }),
    });
    const walletLookup = await call('GET', `/wallets/${uuid(799_998)}`);
    const transactionLookup = await call('GET', `/wagering/transactions/${uuid(799_997)}`);
    const providerLookup = await call('GET', '/providers/provider-http/wagering/transactions/does-not-exist');

    expect(unknownWallet.status).toBe(404);
    expect(unknownWallet.body).toMatchObject({ failureCode: 'WALLET_NOT_FOUND' });
    expect(walletLookup.status).toBe(404);
    expect(walletLookup.body).toMatchObject({ failureCode: 'WALLET_NOT_FOUND' });
    expect(transactionLookup.status).toBe(404);
    expect(transactionLookup.body).toMatchObject({ failureCode: 'TRANSACTION_NOT_FOUND' });
    expect(providerLookup.status).toBe(404);
    expect(providerLookup.body).toMatchObject({ failureCode: 'TRANSACTION_NOT_FOUND' });
  });

  test('TEST-02 case 9: a currency the wallet does not hold is rejected without touching the balance', async () => {
    const mismatch = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-currency-key',
      body: betBody({
        externalTransactionId: 'external-http-currency',
        roundId: 'round-http-currency',
        money: { amount: '10.00', currency: 'USD' },
      }),
    });
    const walletState = await call('GET', `/wallets/${wallet.id}`);

    expect(mismatch.status).toBe(400);
    expect(mismatch.body).toMatchObject({ failureCode: 'CURRENCY_MISMATCH' });
    expect(walletState.body).toMatchObject({ balance: { amount: '75.00', currency: 'BRL' }, version: 2 });
  });

  test('TEST-02 case 10: CP2B-2 accepts WIN and LOSS while rejecting unsupported or unreferenced operations', async () => {
    const kinds = ['OPENING', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'];
    const responses = await Promise.all(kinds.map((kind, index) => call(
      'POST',
      '/wagering/transactions',
      {
        idempotencyKey: `http-kind-${kind}`,
        body: betBody({
          kind,
          externalTransactionId: `external-http-kind-${index}`,
          roundId: `round-http-kind-${index}`,
        }),
      },
    )));
    const walletState = await call('GET', `/wallets/${wallet.id}`);
    const stored = await Promise.all(kinds.map(
      (_kind, index) => call('GET', `/providers/provider-http/wagering/transactions/external-http-kind-${index}`),
    ));

    const [opening, win, loss, refund, rollback] = responses as [
      HttpResult,
      HttpResult,
      HttpResult,
      HttpResult,
      HttpResult,
    ];
    const [storedOpening, storedWin, storedLoss, storedRefund, storedRollback] = stored as [
      HttpResult,
      HttpResult,
      HttpResult,
      HttpResult,
      HttpResult,
    ];

    expect(opening.status).toBe(400);
    expect(opening.body).toMatchObject({ failureCode: 'INVALID_TRANSACTION' });
    expect(win.status).toBe(200);
    expect(win.body).toMatchObject({ kind: 'WIN', status: 'PROCESSED' });
    expect(loss.status).toBe(200);
    expect(loss.body).toMatchObject({ kind: 'LOSS', status: 'PROCESSED' });
    for (const response of [refund, rollback]) {
      expect(response.status).toBe(400);
      expect(response.body).toMatchObject({ failureCode: 'INVALID_TRANSACTION' });
    }
    expect(storedOpening.status).toBe(404);
    expect(storedWin.status).toBe(200);
    expect(storedLoss.status).toBe(200);
    expect(storedRefund.status).toBe(404);
    expect(storedRollback.status).toBe(404);
    expect(walletState.body).toMatchObject({ balance: { amount: '100.00', currency: 'BRL' }, version: 3 });
  });

  test('TEST-02 case 11: HTTP routes persist a referenced REFUND as one reversal', async () => {
    const bet = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-referenced-refund-bet',
      body: betBody({
        externalTransactionId: 'external-http-referenced-refund-bet',
        roundId: 'round-http-referenced-refund',
        money: { amount: '10.00', currency: 'BRL' },
      }),
    });
    const refund = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-referenced-refund',
      body: betBody({
        kind: 'REFUND',
        externalTransactionId: 'external-http-referenced-refund',
        roundId: 'round-http-referenced-refund',
        money: { amount: '10.00', currency: 'BRL' },
        referenceExternalTransactionId: 'external-http-referenced-refund-bet',
      }),
    });
    const walletState = await call('GET', `/wallets/${wallet.id}`);

    expect(bet.status).toBe(200);
    expect(bet.body).toMatchObject({ kind: 'BET', status: 'PROCESSED', balance: { amount: '90.00', currency: 'BRL' } });
    expect(refund.status).toBe(200);
    expect(refund.body).toMatchObject({
      kind: 'REFUND',
      status: 'PROCESSED',
      referenceTransactionId: bet.body.transactionId,
      balance: { amount: '100.00', currency: 'BRL' },
    });
    expect(walletState.body).toMatchObject({ balance: { amount: '100.00', currency: 'BRL' }, version: 5 });
  });

  test('TEST-02 case 12 (FIX-01): POST /wallets accepts the exact UUIDv7 playerId from CHALLENGE.md', async () => {
    const created = await call('POST', '/wallets', {
      body: { playerId: CHALLENGE_PLAYER_ID, initialBalance: { amount: '100.00', currency: 'BRL' } },
    });

    expect(created.status).toBe(201);
    expect(created.body).toMatchObject({
      playerId: CHALLENGE_PLAYER_ID,
      balance: { amount: '100.00', currency: 'BRL' },
      version: 1,
    });

    uuidV7Wallet = { id: created.body.id as string, playerId: CHALLENGE_PLAYER_ID };

    const fetched = await call('GET', `/wallets/${uuidV7Wallet.id}`);
    expect(fetched.status).toBe(200);
    expect(fetched.body).toEqual(created.body);
  });

  test('TEST-02 case 12 (FIX-01): POST /wagering/transactions debits a BET for the UUIDv7 player', async () => {
    const processed = await call('POST', '/wagering/transactions', {
      idempotencyKey: 'http-uuidv7-key',
      body: {
        providerId: 'provider-http',
        externalTransactionId: 'external-http-uuidv7',
        walletId: uuidV7Wallet.id,
        playerId: CHALLENGE_PLAYER_ID,
        roundId: 'round-http-uuidv7',
        gameId: 'game-http',
        kind: 'BET',
        money: { amount: '25.00', currency: 'BRL' },
      },
    });

    expect(processed.status).toBe(200);
    expect(processed.body).toMatchObject({
      status: 'PROCESSED',
      playerId: CHALLENGE_PLAYER_ID,
      walletId: uuidV7Wallet.id,
      balance: { amount: '75.00', currency: 'BRL' },
      idempotentReplay: false,
    });

    const walletState = await call('GET', `/wallets/${uuidV7Wallet.id}`);
    expect(walletState.body).toMatchObject({
      playerId: CHALLENGE_PLAYER_ID,
      balance: { amount: '75.00', currency: 'BRL' },
      version: 2,
    });

    const persisted = await orm.em.getConnection().execute(`
      select player_id::text as player_id from wager_transactions where id = '${processed.body.transactionId as string}'
    `) as Array<{ player_id: string }>;
    expect(persisted).toEqual([{ player_id: CHALLENGE_PLAYER_ID }]);
  });
});
