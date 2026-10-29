import 'reflect-metadata';

import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { resolve } from 'node:path';

import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

/**
 * CP4-R3A observability smoke: this drives the production Nest entrypoint against disposable
 * PostgreSQL and LocalStack. It deliberately verifies one degraded SQS scrape too, because the
 * contract is to retain the remaining exposition rather than manufacture zeroes or fail /metrics.
 */
const PORT = Number(Bun.env.CP4_METRICS_PORT ?? 3127);
const workspaceRoot = resolve(import.meta.dir, '../..');
const baseUrl = `http://127.0.0.1:${PORT}`;

let orm: MikroORM<PostgreSqlDriver>;
let server: ReturnType<typeof Bun.spawn> | undefined;
let sequence = 0;

function nextPlayerId(): string {
  sequence += 1;
  return `00000000-0000-4000-8000-${String(920_000 + sequence).padStart(12, '0')}`;
}

async function waitForLive(url: string): Promise<void> {
  const deadline = Date.now() + 60_000;
  let lastError = 'no response';
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${url}/health/live`);
      if (response.ok) return;
      lastError = `HTTP ${response.status}`;
    } catch (error) {
      lastError = error instanceof Error ? error.message : 'unknown error';
    }
    await Bun.sleep(250);
  }
  throw new Error(`metrics smoke server did not become live: ${lastError}`);
}

function startServer(port: number, extraEnv: Record<string, string> = {}): ReturnType<typeof Bun.spawn> {
  return Bun.spawn(['bun', 'run', 'src/main.ts'], {
    cwd: workspaceRoot,
    env: { ...Bun.env, PORT: String(port), ...extraEnv },
    stdout: 'ignore',
    stderr: 'inherit',
  });
}

async function createWallet(playerId: string): Promise<void> {
  const response = await fetch(`${baseUrl}/wallets`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ playerId, initialBalance: { amount: '10.00', currency: 'BRL' } }),
  });
  expect(response.status).toBe(201);
}

describe('CP4-R3A GET /metrics smoke integration', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
    server = startServer(PORT);
    await waitForLive(baseUrl);
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

  test('returns Prometheus text from real PostgreSQL and LocalStack without sensitive identifier labels', async () => {
    const playerId = nextPlayerId();
    await createWallet(playerId);

    const response = await fetch(`${baseUrl}/metrics`);
    const body = await response.text();

    expect(response.status).toBe(200);
    const contentType = response.headers.get('content-type') ?? '';
    // Express may reorder MIME parameters; both Prometheus format version and text media type
    // are contractual, while the parameter order is not.
    expect(contentType).toContain('text/plain');
    expect(contentType).toContain('version=0.0.4');
    expect(body).toContain('# TYPE wagering_process_info gauge');
    expect(body).toContain('wagering_process_info{process="api"} 1');
    // These are emitted by read-only collectors that queried the real integration services.
    expect(body).toContain('# SCOPE wagering_ledger_entries_state global_postgresql');
    expect(body).toContain('wagering_ledger_entries_state 1');
    expect(body).toContain('# SCOPE wagering_sqs_queue_messages_approximate approximate_sqs');
    expect(body).toContain('wagering_sqs_queue_messages_approximate{queue="wager_transactions",state="visible"}');
    expect(body).not.toContain(playerId);
    expect(body).not.toContain('cp4r3pass');
    expect(body).not.toMatch(/(?:wallet|player|message|transaction)_?id=/i);
  });

  test('keeps healthy metrics visible and records the collector error when SQS is unavailable', async () => {
    const degradedPort = PORT + 1;
    const degradedUrl = `http://127.0.0.1:${degradedPort}`;
    const degraded = startServer(degradedPort, { SQS_ENDPOINT: 'http://127.0.0.1:1' });

    try {
      await waitForLive(degradedUrl);
      const response = await fetch(`${degradedUrl}/metrics`);
      const body = await response.text();

      expect(response.status).toBe(200);
      expect(body).toContain('wagering_metrics_collection_errors_total{collector="sqs"} 1');
      // PostgreSQL state remains an observed value, not a fabricated zero after SQS fails.
      expect(body).toContain('# SCOPE wagering_ledger_entries_state global_postgresql');
      expect(body).toMatch(/wagering_ledger_entries_state \d+/);
      expect(body).not.toContain('cp4r3pass');
    } finally {
      degraded.kill();
      await degraded.exited;
    }
  }, 30_000);
});
