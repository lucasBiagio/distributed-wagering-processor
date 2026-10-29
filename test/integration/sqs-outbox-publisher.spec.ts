import { randomUUID } from 'node:crypto';

import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { SqsOutboxPublisher } from '../../src/infrastructure/messaging/sqs-outbox-publisher.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';
import {
  MikroOrmFinancialProcessing,
  type UnitOfWorkStage,
} from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';

interface OutboxRow {
  readonly id: string;
  readonly wallet_id: string;
  readonly enqueue_sequence: string;
  readonly payload: Record<string, unknown>;
  readonly attempts: number;
  readonly next_attempt_at: Date | null;
  readonly published_at: Date | null;
  readonly claim_token: string | null;
  readonly lease_expires_at: Date | null;
}

class FailingBeforeCommitProcessing extends MikroOrmFinancialProcessing {
  protected override async interceptUnitOfWork(stage: UnitOfWorkStage): Promise<void> {
    if (stage === 'before-commit') {
      throw new Error('injected failure before commit');
    }
  }
}

let orm: MikroORM<PostgreSqlDriver>;
let processing: MikroOrmFinancialProcessing;
let client: SQSClient;
let queueUrl: string;
let sequence = 0;

function nextId(prefix: string): string {
  sequence += 1;
  return `${prefix}-${process.pid}-${sequence}-${randomUUID().slice(0, 8)}`;
}

function player(): string {
  sequence += 1;
  return `10000000-0000-4000-8000-${String(sequence).padStart(12, '0')}`;
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await Bun.sleep(100);
  }
  throw new Error('Timed out waiting for expected outbox/SQS state');
}

async function createQueue(): Promise<string> {
  return (await client.send(new CreateQueueCommand({
    QueueName: `${nextId('cp3b-events')}.fifo`,
    Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false', VisibilityTimeout: '1' },
  }))).QueueUrl!;
}

async function rows(walletId?: string): Promise<OutboxRow[]> {
  const result = await orm.em.getConnection().execute(
    `select id::text as id, wallet_id::text as wallet_id, enqueue_sequence::text as enqueue_sequence,
            payload, attempts, next_attempt_at, published_at, claim_token::text as claim_token, lease_expires_at
       from outbox_messages
      ${walletId === undefined ? '' : 'where wallet_id = ?'}
      order by enqueue_sequence asc`,
    walletId === undefined ? [] : [walletId],
  ) as OutboxRow[];
  return result;
}

async function pendingCount(): Promise<number> {
  const result = await orm.em.getConnection().execute(
    'select count(*)::int as count from outbox_messages where published_at is null',
  ) as Array<{ count: number }>;
  return result[0]!.count;
}

async function receiveAll(expected: number): Promise<Array<{ body: string; groupId?: string; deduplicationId?: string }>> {
  const messages: Array<{ body: string; groupId?: string; deduplicationId?: string }> = [];
  await waitFor(async () => {
    const response = await client.send(new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 0,
      MessageAttributeNames: ['All'],
      AttributeNames: ['All'],
    }));
    for (const message of response.Messages ?? []) {
      messages.push({
        body: message.Body!,
        ...(message.Attributes?.MessageGroupId === undefined ? {} : { groupId: message.Attributes.MessageGroupId }),
        ...(message.Attributes?.MessageDeduplicationId === undefined ? {} : { deduplicationId: message.Attributes.MessageDeduplicationId }),
      });
      await client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle! }));
    }
    return messages.length >= expected;
  });
  return messages;
}

async function wallet(balance = '100.00'): Promise<{ id: string; playerId: string }> {
  return await processing.openWallet({ playerId: player(), initialBalance: { amount: balance, currency: 'BRL' } });
}

async function bet(walletId: string, playerId: string, external: string, amount = '10.00'): Promise<void> {
  await processing.processBet({
    providerId: 'cp3b-provider',
    externalTransactionId: external,
    idempotencyKey: `key-${external}`,
    walletId,
    playerId,
    roundId: `round-${external}`,
    gameId: 'outbox-game',
    kind: 'BET',
    money: { amount, currency: 'BRL' },
  });
}

async function runPublisherProcess(overrides: Record<string, string>): Promise<{ exitCode: number | null; output: string }> {
  const child = Bun.spawn(['bun', 'scripts/cp3b-outbox-publisher.ts'], {
    cwd: process.cwd(),
    env: { ...process.env, WAGER_EVENTS_QUEUE_URL: queueUrl, ...overrides },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [stdout, stderr] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  await child.exited;
  return { exitCode: child.exitCode, output: `${stdout}\n${stderr}` };
}

function publisher(overrides: Partial<{ leaseMs: number; retryBaseMs: number; retryMaxMs: number }> = {}): SqsOutboxPublisher {
  return new SqsOutboxPublisher(client, orm, {
    queueUrl,
    leaseMs: overrides.leaseMs ?? 150,
    retryBaseMs: overrides.retryBaseMs ?? 20,
    retryMaxMs: overrides.retryMaxMs ?? 50,
  });
}

describe('CP3B transactional outbox publisher with real PostgreSQL and LocalStack', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
    processing = new MikroOrmFinancialProcessing(orm);
    client = new SQSClient({
      region: process.env.AWS_REGION ?? 'us-east-1',
      endpoint: process.env.SQS_ENDPOINT ?? 'http://localstack:4566',
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    });
  });

  beforeEach(async () => {
    await orm.em.getConnection().execute(
      'truncate table inbox_messages, outbox_messages, wallet_ledger_entries, wager_transactions, wallets restart identity cascade;',
    );
    queueUrl = await createQueue();
  });

  afterEach(async () => {
    await client.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
  });

  afterAll(async () => {
    await client.destroy();
    await orm.getMigrator().down({ to: 0 });
    await orm.close(true);
  });

  test('TEST-A: publishes only committed financial outbox rows and marks them published after real SQS acceptance', async () => {
    const opened = await wallet();
    await bet(opened.id, opened.playerId, 'normal');
    const before = await rows(opened.id);
    expect(before).toHaveLength(4);
    expect(before.every((row) => row.published_at === null && row.attempts === 0)).toBe(true);

    const result = await publisher().drain({ stopWhenIdle: true });
    expect(result.confirmed).toBe(4);
    const delivered = await receiveAll(4);
    const after = await rows(opened.id);

    expect(delivered.map((message) => message.groupId)).toEqual([opened.id, opened.id, opened.id, opened.id]);
    expect(delivered.map((message) => message.deduplicationId).sort()).toEqual(before.map((row) => row.id).sort());
    expect(after.every((row) => row.published_at !== null && row.claim_token === null && row.lease_expires_at === null)).toBe(true);
  });

  test('TEST-B: a financial failure before commit creates no publishable row and no SQS message', async () => {
    const opened = await wallet('10.00');
    await expect(bet(opened.id, opened.playerId, 'rejected', '80.00')).resolves.toBeUndefined();
    const before = await rows(opened.id);
    expect(before).toHaveLength(3);

    await publisher().drain({ stopWhenIdle: true });
    const delivered = await receiveAll(3);
    expect(delivered).toHaveLength(3);
    expect((await rows(opened.id)).every((row) => row.published_at !== null)).toBe(true);
  });

  test('TEST-B1: an injected failure after partial persistence but before commit rolls back financial and outbox rows, so the publisher cannot publish the aborted operation', async () => {
    const opened = await wallet();
    const before = await rows(opened.id);
    const failing = new FailingBeforeCommitProcessing(orm);

    await expect(failing.processBet({
      providerId: 'cp3b-provider',
      externalTransactionId: 'aborted-before-commit',
      idempotencyKey: 'key-aborted-before-commit',
      walletId: opened.id,
      playerId: opened.playerId,
      roundId: 'round-aborted-before-commit',
      gameId: 'outbox-game',
      kind: 'BET',
      money: { amount: '10.00', currency: 'BRL' },
    })).rejects.toThrow('injected failure before commit');

    expect(await rows(opened.id)).toEqual(before);
    const abortedTransactions = await orm.em.getConnection().execute(
      "select count(*)::int as count from wager_transactions where idempotency_key = 'key-aborted-before-commit'",
    ) as Array<{ count: number }>;
    const abortedOutbox = await orm.em.getConnection().execute(
      "select count(*)::int as count from outbox_messages where payload->>'correlationId' = 'key-aborted-before-commit'",
    ) as Array<{ count: number }>;
    expect(abortedTransactions[0]!.count).toBe(0);
    expect(abortedOutbox[0]!.count).toBe(0);

    const result = await publisher().drain({ stopWhenIdle: true });
    expect(result.confirmed).toBe(before.length);
    const delivered = await receiveAll(before.length);
    expect(delivered.some((message) => JSON.parse(message.body).eventId === 'aborted-before-commit')).toBe(false);
  });

  test('TEST-C: two independent Bun publishers cannot hold valid claims for the same outbox event', async () => {
    const opened = await wallet();
    await bet(opened.id, opened.playerId, 'concurrent');
    const before = await rows(opened.id);
    const children = Array.from({ length: 2 }, () => Bun.spawn(['bun', 'scripts/cp3b-outbox-publisher.ts'], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        WAGER_EVENTS_QUEUE_URL: queueUrl,
        OUTBOX_PUBLISHER_STOP_WHEN_IDLE: 'true',
        OUTBOX_PUBLISHER_BATCH_SIZE: '1',
        OUTBOX_PUBLISHER_LEASE_MS: '1000',
      },
      stdout: 'pipe',
      stderr: 'pipe',
    }));
    const childOutput: string[] = [];
    for (const child of children) {
      const [stdout, stderr] = await Promise.all([
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      await child.exited;
      expect(child.exitCode, `${stdout}\n${stderr}`).toBe(0);
      childOutput.push(`${stdout}\n${stderr}`);
    }

    expect(childOutput.some((output) => output.includes('outbox_published')), childOutput.join('\n---\n')).toBe(true);
    expect(await pendingCount()).toBe(0);
    const delivered = await receiveAll(before.length);
    expect(new Set(delivered.map((message) => JSON.parse(message.body).eventId)).size).toBe(before.length);
    const persisted = await rows(opened.id);
    expect(persisted.every((row) => row.published_at !== null && row.claim_token === null)).toBe(true);
  });

  test('TEST-D: a crash after a durable claim is recovered after lease expiry without marking the row published', async () => {
    const opened = await wallet();
    const target = (await rows(opened.id))[0]!;
    const crashed = await runPublisherProcess({
      OUTBOX_PUBLISHER_LEASE_MS: '100',
      OUTBOX_PUBLISHER_STOP_WHEN_IDLE: 'true',
      CP3B_TEST_CRASH_AFTER_CLAIM: 'true',
    });
    expect(crashed.exitCode, crashed.output).toBe(75);

    const claimed = (await rows(opened.id)).find((row) => row.id === target.id)!;
    expect(claimed.published_at).toBeNull();
    expect(claimed.claim_token).not.toBeNull();
    await Bun.sleep(175);
    await publisher({ leaseMs: 100 }).drain({ stopWhenIdle: true });
    expect((await rows(opened.id)).every((row) => row.published_at !== null)).toBe(true);
  });

  test('TEST-E: a crash after SQS acceptance before confirmation is recovered with the same persisted event identity', async () => {
    const opened = await wallet();
    const before = await rows(opened.id);
    const target = before[0]!;
    const crashed = await runPublisherProcess({
      OUTBOX_PUBLISHER_LEASE_MS: '100',
      OUTBOX_PUBLISHER_STOP_WHEN_IDLE: 'true',
      CP3B_TEST_CRASH_AFTER_SEND: 'true',
    });
    expect(crashed.exitCode, crashed.output).toBe(76);

    expect((await rows(opened.id)).find((row) => row.id === target.id)!.published_at).toBeNull();
    await Bun.sleep(175);
    await publisher({ leaseMs: 100 }).drain({ stopWhenIdle: true });
    const delivered = await receiveAll(before.length);
    const targetDeliveries = delivered.filter((message) => JSON.parse(message.body).eventId === target.id);
    expect(targetDeliveries.length).toBeGreaterThanOrEqual(1);
    expect((await rows(opened.id)).find((row) => row.id === target.id)!.published_at).not.toBeNull();
  });

  test('TEST-F: an SQS failure persists attempts and a final delay capped at retryMaxMs before a later successful publish', async () => {
    const opened = await wallet();
    const originalRandom = Math.random;
    const originalNow = Date.now;
    const startedAt = 1_000_000;
    let failing: SqsOutboxPublisher;
    const unavailable = new SQSClient({
      region: 'us-east-1',
      endpoint: 'http://127.0.0.1:1',
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
      requestHandler: {
        handle: async () => {
          Date.now = () => startedAt;
          failing.stopAcceptingClaims();
          throw new Error('test SQS unavailable');
        },
      },
    });
    await orm.em.getConnection().execute('update outbox_messages set attempts = 10 where wallet_id = ?', [opened.id]);
    failing = new SqsOutboxPublisher(unavailable, orm, { queueUrl, leaseMs: 100, retryBaseMs: 20, retryMaxMs: 50 });
    Math.random = () => 0.99;
    try {
      await failing.drain({ stopWhenIdle: true });
    } finally {
      Math.random = originalRandom;
      Date.now = originalNow;
    }
    const failed = await rows(opened.id);
    expect(failed[0]!.published_at).toBeNull();
    expect(failed[0]!.attempts).toBe(11);
    expect(failed[0]!.next_attempt_at).not.toBeNull();
    expect(new Date(failed[0]!.next_attempt_at!).getTime()).toBeLessThanOrEqual(startedAt + 50);
    expect(failed[0]!.claim_token).toBeNull();
    expect(failed.slice(1).every((row) => row.published_at === null && row.attempts === 10 && row.claim_token === null)).toBe(true);
    await unavailable.destroy();

    await Bun.sleep(60);
    await publisher().drain({ stopWhenIdle: true });
    expect((await rows(opened.id)).every((row) => row.published_at !== null)).toBe(true);
  });

  test('TEST-G: preserves enqueue order within each wallet while independent wallets can be claimed in one batch', async () => {
    const first = await wallet('100.00');
    const second = await wallet('100.00');
    await bet(first.id, first.playerId, 'ordered-a-1');
    await bet(first.id, first.playerId, 'ordered-a-2');
    await bet(second.id, second.playerId, 'ordered-b-1');

    const expectedA = (await rows(first.id)).map((row) => row.id);
    const result = await publisher().drain({ stopWhenIdle: true, batchSize: 8 });
    expect(result.maxConcurrentWallets).toBeGreaterThanOrEqual(2);
    const delivered = await receiveAll(10);
    const actualA = delivered.filter((message) => message.groupId === first.id).map((message) => JSON.parse(message.body).eventId);
    expect(actualA).toEqual(expectedA);
  });

  test('TEST-H: a stopped publisher leaves pending rows recoverable by a new publisher process', async () => {
    const opened = await wallet();
    const first = publisher();
    const total = (await rows(opened.id)).length;
    await first.drain({ maxConfirmations: 1 });
    expect(await pendingCount()).toBe(total - 1);
    await publisher().drain({ stopWhenIdle: true });
    expect(await pendingCount()).toBe(0);
    expect(await receiveAll(total)).toHaveLength(total);
  });

  test('TEST-I: SQS body is the exact persisted envelope and money remains decimal strings', async () => {
    const opened = await wallet();
    const before = await rows(opened.id);
    await publisher().drain({ stopWhenIdle: true });
    const delivered = await receiveAll(before.length);
    for (const message of delivered) {
      const expected = before.find((row) => row.id === JSON.parse(message.body).eventId)!;
      expect(JSON.parse(message.body)).toEqual(expected.payload);
      const data = JSON.parse(message.body).data as Record<string, unknown>;
      if ('balance' in data) {
        expect(typeof (data.balance as { amount: unknown }).amount).toBe('string');
      }
      if ('money' in data) {
        expect(typeof (data.money as { amount: unknown }).amount).toBe('string');
      }
    }
  });

  test('TEST-J: a durable downstream consumer deduplicates repeated eventId deliveries beyond FIFO deduplication assumptions', async () => {
    const opened = await wallet();
    const target = (await rows(opened.id))[0]!;
    await publisher().drain({ stopWhenIdle: true });
    await orm.em.getConnection().execute('create table if not exists cp3b_test_downstream_events (event_id uuid primary key, applied_at timestamptz not null)');
    await orm.em.getConnection().execute('truncate table cp3b_test_downstream_events');
    for (const eventId of [target.id, target.id]) {
      await orm.em.getConnection().execute(
        'insert into cp3b_test_downstream_events (event_id, applied_at) values (?, now()) on conflict (event_id) do nothing',
        [eventId],
      );
    }
    const result = await orm.em.getConnection().execute('select count(*)::int as count from cp3b_test_downstream_events') as Array<{ count: number }>;
    expect(result[0]!.count).toBe(1);
  });

  test('runner: stopWhenIdle exits deterministically when no eligible work or in-flight batch exists', async () => {
    const stopped = await runPublisherProcess({ OUTBOX_PUBLISHER_STOP_WHEN_IDLE: 'true' });

    expect(stopped.exitCode, stopped.output).toBe(0);
    expect(stopped.output).toContain('"event":"outbox_publisher_stopped"');
    expect(stopped.output).toContain('"stopping":false');
    expect(stopped.output).toContain('"claimed":0');
    expect(stopped.output).toContain('"confirmed":0');
  });

  test('runner: default continuous mode survives an idle interval, publishes later work, then performs SIGTERM shutdown', async () => {
    const child = Bun.spawn(['bun', 'scripts/cp3b-outbox-publisher.ts'], {
      cwd: process.cwd(),
      env: { ...process.env, WAGER_EVENTS_QUEUE_URL: queueUrl },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const stdout = new Response(child.stdout).text();
    const stderr = new Response(child.stderr).text();

    try {
      const exitedDuringIdle = await Promise.race([
        child.exited.then(() => true),
        Bun.sleep(150).then(() => false),
      ]);
      expect(exitedDuringIdle).toBe(false);

      const opened = await wallet();
      await waitFor(async () => (await rows(opened.id)).every((row) => row.published_at !== null));
      expect(await receiveAll((await rows(opened.id)).length)).toHaveLength((await rows(opened.id)).length);

      child.kill('SIGTERM');
      await child.exited;
      const output = `${await stdout}\n${await stderr}`;
      expect(child.exitCode, output).toBe(0);
      expect(output).toContain('"event":"outbox_publisher_stopped"');
      expect(output).toContain('"stopping":true');
    } finally {
      if (child.exitCode === null) {
        child.kill('SIGKILL');
        await child.exited;
      }
    }
  });
});
