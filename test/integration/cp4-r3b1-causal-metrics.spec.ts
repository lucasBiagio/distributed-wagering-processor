import 'reflect-metadata';

import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SetQueueAttributesCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import type { Socket, TCPSocketListener } from 'bun';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

const API_PORT = Number(Bun.env.CP4_R3B1_API_PORT ?? 3131);
const PUBLISHER_METRICS_PORT = API_PORT + 1;
const CONSUMER_METRICS_PORT = API_PORT + 2;
const DEGRADED_API_PORT = API_PORT + 3;
const SQS_PROXY_PORT = API_PORT + 4;
const baseUrl = `http://127.0.0.1:${API_PORT}`;
const workspaceRoot = resolve(import.meta.dir, '../..');

/**
 * M6 keeps the declared bucket contract explicit: the exposition must advertise exactly the
 * seconds boundaries declared in src/infrastructure/observability/wagering-metrics.ts.
 */
const DURATION_BUCKETS = '0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2.5,5,10';

interface ProxyState {
  upstream?: Socket<undefined>;
  readonly pending: Uint8Array[];
  closed: boolean;
}

/**
 * M8 needs a *real* SQS outage, not a mocked client. LocalStack itself is shared by the whole
 * Compose stack, so instead of stopping the container this proxy is the only network path the
 * degraded API process knows: closing it makes every GetQueueUrl/GetQueueAttributes fail at
 * the TCP layer against real LocalStack, and reopening it restores the real service. Bytes are
 * forwarded verbatim, so the SigV4-signed requests reaching LocalStack are unmodified.
 */
function startSqsProxy(port: number): TCPSocketListener<ProxyState> {
  const endpoint = new URL(Bun.env.SQS_ENDPOINT ?? 'http://localstack:4566');
  const upstreamHost = endpoint.hostname;
  const upstreamPort = Number(endpoint.port === '' ? '4566' : endpoint.port);
  return Bun.listen<ProxyState>({
    hostname: '127.0.0.1',
    port,
    socket: {
      open(socket) {
        socket.data = { pending: [], closed: false };
        void Bun.connect<undefined>({
          hostname: upstreamHost,
          port: upstreamPort,
          socket: {
            open(upstream) {
              socket.data.upstream = upstream;
              for (const chunk of socket.data.pending) {
                upstream.write(chunk);
              }
              socket.data.pending.length = 0;
              if (socket.data.closed) {
                upstream.end();
              }
            },
            data(_upstream, chunk) {
              socket.write(chunk);
            },
            close() {
              socket.end();
            },
            error() {
              socket.end();
            },
          },
        }).catch(() => {
          socket.end();
        });
      },
      data(socket, chunk) {
        const upstream = socket.data.upstream;
        if (upstream === undefined) {
          socket.data.pending.push(new Uint8Array(chunk));
          return;
        }
        upstream.write(chunk);
      },
      close(socket) {
        socket.data.closed = true;
        socket.data.upstream?.end();
      },
      error(socket) {
        socket.data.upstream?.end();
      },
    },
  });
}

function queueNameOf(url: string): string {
  return url.slice(url.lastIndexOf('/') + 1);
}

interface QueueSet {
  readonly eventsUrl: string;
  readonly transactionsUrl: string;
  readonly transactionsDlqUrl: string;
}

interface MetricSample {
  readonly labels: Record<string, string>;
  readonly value: number;
}

interface HttpResult {
  readonly status: number;
  readonly body: Record<string, unknown>;
}

let orm: MikroORM<PostgreSqlDriver>;
let sqs: SQSClient;
let queues: QueueSet;
let api: ReturnType<typeof Bun.spawn> | undefined;
let publisher: ReturnType<typeof Bun.spawn> | undefined;
let consumer: ReturnType<typeof Bun.spawn> | undefined;
let degradedApi: ReturnType<typeof Bun.spawn> | undefined;
let sqsProxy: TCPSocketListener<ProxyState> | undefined;
let sequence = 0;

function nextName(prefix: string): string {
  sequence += 1;
  return `${prefix}-${process.pid}-${sequence}-${randomUUID().slice(0, 8)}`;
}

function nextPlayerId(): string {
  sequence += 1;
  return `00000000-0000-4000-8000-${String(970_000 + sequence).padStart(12, '0')}`;
}

function workerEnv(extra: Record<string, string> = {}): Record<string, string> {
  return {
    ...Bun.env,
    WAGER_EVENTS_QUEUE_URL: queues.eventsUrl,
    WAGER_TRANSACTIONS_QUEUE_URL: queues.transactionsUrl,
    WAGER_TRANSACTIONS_DLQ_QUEUE_URL: queues.transactionsDlqUrl,
    WAGER_EVENTS_QUEUE_NAME: 'cp4-r3b1-events.fifo',
    WAGER_TRANSACTIONS_QUEUE_NAME: 'cp4-r3b1-transactions.fifo',
    WAGER_TRANSACTIONS_DLQ_QUEUE_NAME: 'cp4-r3b1-transactions-dlq.fifo',
    ...extra,
  };
}

async function waitFor(predicate: () => Promise<boolean>, message: string, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(100);
  }
  throw new Error(`Timed out waiting for ${message}`);
}

async function waitForLive(url: string): Promise<void> {
  await waitFor(async () => {
    try {
      return (await fetch(`${url}/health/live`)).ok;
    } catch {
      return false;
    }
  }, `${url}/health/live`);
}

async function stop(child: ReturnType<typeof Bun.spawn> | undefined): Promise<void> {
  if (child === undefined || child.exitCode !== null) return;
  child.kill('SIGTERM');
  await Promise.race([
    child.exited,
    Bun.sleep(5_000).then(() => {
      if (child.exitCode === null) child.kill('SIGKILL');
    }),
  ]);
  if (child.exitCode === null) await child.exited;
}

function metricSamples(exposition: string, name: string): MetricSample[] {
  const expression = new RegExp(`^${name}(?:\\{([^}]*)\\})?\\s+([^\\s]+)$`, 'm');
  return exposition.split('\n').flatMap((line) => {
    const match = line.match(expression);
    if (!match) return [];
    const labels: Record<string, string> = {};
    for (const part of (match[1] ?? '').matchAll(/([A-Za-z_][A-Za-z0-9_]*)="((?:\\.|[^"])*)"/g)) {
      const key = part[1];
      const value = part[2];
      if (key !== undefined && value !== undefined) {
        labels[key] = value.replaceAll('\\"', '"').replaceAll('\\\\', '\\');
      }
    }
    const numericValue = match[2];
    return numericValue === undefined ? [] : [{ labels, value: Number(numericValue) }];
  });
}

function metric(exposition: string, name: string, labels: Record<string, string> = {}): number {
  const sample = metricSamples(exposition, name).find((entry) => Object.entries(labels).every(([key, value]) => entry.labels[key] === value));
  if (!sample) throw new Error(`Missing metric ${name} ${JSON.stringify(labels)} in exposition`);
  return sample.value;
}

function metricOrZero(exposition: string, name: string, labels: Record<string, string> = {}): number {
  return metricSamples(exposition, name).find((entry) => Object.entries(labels).every(([key, value]) => entry.labels[key] === value))?.value ?? 0;
}

async function metrics(url = baseUrl): Promise<string> {
  const response = await fetch(`${url}/metrics`);
  expect(response.status).toBe(200);
  const body = await response.text();
  expect(body).toContain('# TYPE');
  return body;
}

async function call(path: string, body?: unknown, idempotencyKey?: string): Promise<HttpResult> {
  const response = await fetch(`${baseUrl}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      ...(idempotencyKey === undefined ? {} : { 'idempotency-key': idempotencyKey }),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const text = await response.text();
  return { status: response.status, body: text.length === 0 ? {} : JSON.parse(text) as Record<string, unknown> };
}

async function wallet(balance = '100.00'): Promise<{ id: string; playerId: string }> {
  const playerId = nextPlayerId();
  const opened = await call('/wallets', {
    playerId,
    initialBalance: { amount: balance, currency: 'BRL' },
  });
  expect(opened.status).toBe(201);
  return { id: opened.body.id as string, playerId };
}

function wager(target: { id: string; playerId: string }, kind: string, amount: string, external: string, referenceExternalTransactionId?: string): Record<string, unknown> {
  return {
    providerId: 'cp4-r3b1-provider',
    externalTransactionId: external,
    walletId: target.id,
    playerId: target.playerId,
    roundId: referenceExternalTransactionId === undefined ? `round-${external}` : `round-${referenceExternalTransactionId}`,
    gameId: 'cp4-r3b1-game',
    kind,
    money: { amount, currency: 'BRL' },
    ...(referenceExternalTransactionId === undefined ? {} : { referenceExternalTransactionId }),
  };
}

async function wagerHttp(target: { id: string; playerId: string }, kind: string, amount: string, suffix: string, referenceExternalTransactionId?: string, key = `key-${suffix}`): Promise<HttpResult> {
  return call('/wagering/transactions', wager(target, kind, amount, `external-${suffix}`, referenceExternalTransactionId), key);
}

async function createQueues(): Promise<QueueSet> {
  const dlqUrl = (await sqs.send(new CreateQueueCommand({
    QueueName: `${nextName('cp4-r3b1-dlq')}.fifo`,
    Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
  }))).QueueUrl!;
  const dlqArn = (await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlqUrl, AttributeNames: ['QueueArn'] }))).Attributes!.QueueArn!;
  const transactionsUrl = (await sqs.send(new CreateQueueCommand({
    QueueName: `${nextName('cp4-r3b1-transactions')}.fifo`,
    Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false', VisibilityTimeout: '1' },
  }))).QueueUrl!;
  await sqs.send(new SetQueueAttributesCommand({
    QueueUrl: transactionsUrl,
    Attributes: { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: '5' }), VisibilityTimeout: '1' },
  }));
  const eventsUrl = (await sqs.send(new CreateQueueCommand({
    QueueName: `${nextName('cp4-r3b1-events')}.fifo`,
    Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
  }))).QueueUrl!;
  return { eventsUrl, transactionsUrl, transactionsDlqUrl: dlqUrl };
}

async function queueVisibleCount(url: string): Promise<number> {
  const attributes = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ['ApproximateNumberOfMessages'] }));
  return Number(attributes.Attributes?.ApproximateNumberOfMessages ?? '0');
}

async function outboxRows(walletId: string): Promise<Array<{ id: string; attempts: number; published_at: Date | null; next_attempt_at: Date | null }>> {
  return await orm.em.getConnection().execute(
    `select id::text as id, attempts, published_at, next_attempt_at
       from outbox_messages where wallet_id = ? order by enqueue_sequence`,
    [walletId],
  ) as Array<{ id: string; attempts: number; published_at: Date | null; next_attempt_at: Date | null }>;
}

async function financialCounts(walletId: string): Promise<{ ledger: number; transactions: number; outbox: number; balance: string }> {
  const rows = await orm.em.getConnection().execute(
    `select
      (select count(*)::int from wallet_ledger_entries where wallet_id = ?) as ledger,
      (select count(*)::int from wager_transactions where wallet_id = ?) as transactions,
      (select count(*)::int from outbox_messages where wallet_id = ?) as outbox,
      (select balance::text from wallets where id = ?) as balance`,
    [walletId, walletId, walletId, walletId],
  ) as Array<{ ledger: number; transactions: number; outbox: number; balance: string }>;
  return rows[0]!;
}

describe('CP4-R3B.1 causal metric evidence M1-M8', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
    sqs = new SQSClient({
      region: Bun.env.AWS_REGION ?? 'us-east-1',
      endpoint: Bun.env.SQS_ENDPOINT ?? 'http://localstack:4566',
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    });
    queues = await createQueues();
    api = Bun.spawn(['bun', 'run', 'src/main.ts'], {
      cwd: workspaceRoot,
      env: workerEnv({ PORT: String(API_PORT) }),
      stdout: 'ignore',
      stderr: 'inherit',
    });
    await waitForLive(baseUrl);
  }, 30_000);

  beforeEach(async () => {
    await stop(publisher);
    await stop(consumer);
    publisher = undefined;
    consumer = undefined;
    await orm.em.getConnection().execute('truncate table inbox_messages, outbox_messages, wallet_ledger_entries, wager_transactions, wallets restart identity cascade');
  });

  afterAll(async () => {
    await stop(publisher);
    await stop(consumer);
    await stop(degradedApi);
    await stop(api);
    sqsProxy?.stop(true);
    sqsProxy = undefined;
    if (sqs !== undefined && queues !== undefined) {
      await sqs.send(new DeleteQueueCommand({ QueueUrl: queues.eventsUrl }));
      await sqs.send(new DeleteQueueCommand({ QueueUrl: queues.transactionsUrl }));
      await sqs.send(new DeleteQueueCommand({ QueueUrl: queues.transactionsDlqUrl }));
      await sqs.destroy();
    }
    if (orm !== undefined) {
      await orm.getMigrator().down({ to: 0 });
      await orm.close(true);
    }
  });

  test('M1: PostgreSQL gauges change after BET, WIN, REFUND and durable insufficient-balance rejection without extra financial movement', async () => {
    const target = await wallet('100.00');
    const before = await metrics();
    expect(before).toContain('# TYPE wagering_transactions_state gauge');
    expect(before).toContain('# SCOPE wagering_transactions_state global_postgresql');
    expect(before).toContain('# TYPE wagering_transaction_failures_state gauge');

    const processedBefore = metricOrZero(before, 'wagering_transactions_state', { status: 'PROCESSED', kind: 'BET' });
    const failureBefore = metricSamples(before, 'wagering_transaction_failures_state').find((sample) => sample.labels.failure_code === 'INSUFFICIENT_BALANCE')?.value ?? 0;

    expect((await wagerHttp(target, 'BET', '10.00', 'm1-bet')).status).toBe(200);
    expect((await wagerHttp(target, 'WIN', '30.00', 'm1-win')).status).toBe(200);
    expect((await wagerHttp(target, 'BET', '5.00', 'm1-refund-bet')).status).toBe(200);
    expect((await wagerHttp(target, 'REFUND', '5.00', 'm1-refund', 'external-m1-refund-bet')).status).toBe(200);
    const afterSuccessfulTransactions = await financialCounts(target.id);
    const rejected = await wagerHttp(target, 'BET', '500.00', 'm1-insufficient');
    expect(rejected).toMatchObject({ status: 422, body: { status: 'REJECTED', failureCode: 'INSUFFICIENT_BALANCE' } });

    const after = await metrics();
    expect(metric(after, 'wagering_transactions_state', { status: 'PROCESSED', kind: 'BET' })).toBe(processedBefore + 2);
    expect(metric(after, 'wagering_transactions_state', { status: 'PROCESSED', kind: 'WIN' })).toBe(1);
    expect(metric(after, 'wagering_transactions_state', { status: 'PROCESSED', kind: 'REFUND' })).toBe(1);
    expect(metric(after, 'wagering_transactions_state', { status: 'REJECTED', kind: 'BET' })).toBe(1);
    expect(metric(after, 'wagering_transaction_failures_state', { failure_code: 'INSUFFICIENT_BALANCE' })).toBe(failureBefore + 1);

    const persisted = await financialCounts(target.id);
    expect(persisted.ledger).toBe(afterSuccessfulTransactions.ledger);
    expect(persisted.balance).toBe(afterSuccessfulTransactions.balance);
    expect(persisted.transactions).toBe(afterSuccessfulTransactions.transactions + 1);
    const rejectedRows = await orm.em.getConnection().execute(
      "select count(*)::int as count from wallet_ledger_entries l join wager_transactions t on t.id = l.transaction_id where t.failure_code = 'INSUFFICIENT_BALANCE'",
    ) as Array<{ count: number }>;
    expect(rejectedRows[0]!.count).toBe(0);
  });

  test('M2: API-local duplicate and conflict counters change only in the HTTP process and duplicates create no ledger or outbox rows', async () => {
    const target = await wallet('100.00');
    const firstPayload = wager(target, 'BET', '10.00', 'm2-duplicate');
    const key = 'm2-same-key';
    const before = await metrics();
    expect(before).toContain('# SCOPE wagering_duplicate_submissions_total process_local');
    const replayBefore = metricSamples(before, 'wagering_duplicate_submissions_total').find((sample) => sample.labels.source === 'http' && sample.labels.outcome === 'idempotent_replay')?.value ?? 0;
    const conflictBefore = metricSamples(before, 'wagering_duplicate_submissions_total').find((sample) => sample.labels.source === 'http' && sample.labels.outcome === 'payload_conflict')?.value ?? 0;

    expect((await call('/wagering/transactions', firstPayload, key)).status).toBe(200);
    const afterFirst = await financialCounts(target.id);
    const replay = await call('/wagering/transactions', firstPayload, key);
    expect(replay).toMatchObject({ status: 200, body: { idempotentReplay: true } });
    const changedPayload = { ...firstPayload, money: { amount: '11.00', currency: 'BRL' } };
    expect(await call('/wagering/transactions', changedPayload, key)).toMatchObject({ status: 409, body: { failureCode: 'IDEMPOTENCY_CONFLICT' } });
    expect(await financialCounts(target.id)).toEqual(afterFirst);

    const after = await metrics();
    expect(metric(after, 'wagering_duplicate_submissions_total', { source: 'http', outcome: 'idempotent_replay' })).toBe(replayBefore + 1);
    expect(metric(after, 'wagering_duplicate_submissions_total', { source: 'http', outcome: 'payload_conflict' })).toBe(conflictBefore + 1);
    for (const sample of metricSamples(after, 'wagering_duplicate_submissions_total')) {
      expect(Object.keys(sample.labels).sort()).toEqual(['outcome', 'source']);
    }
  });

  test('M3: publisher-local failure and publication counters bracket a durable retry and PostgreSQL lag transitions positive to zero', async () => {
    const target = await wallet('100.00');
    // Wallet creation emits its own events. Drain those first so the before-scrape is a true
    // empty-outbox baseline for the operation under test.
    publisher = Bun.spawn(['bun', 'run', 'src/outbox-publisher.ts'], {
      cwd: workspaceRoot,
      env: workerEnv({ WORKER_METRICS_PORT: String(PUBLISHER_METRICS_PORT), OUTBOX_PUBLISHER_RETRY_BASE_MS: '20', OUTBOX_PUBLISHER_RETRY_MAX_MS: '50' }),
      stdout: 'ignore',
      stderr: 'ignore',
    });
    await waitForLive(`http://127.0.0.1:${PUBLISHER_METRICS_PORT}`);
    await waitFor(async () => (await outboxRows(target.id)).every((row) => row.published_at !== null), 'wallet-created events published');
    await stop(publisher);
    publisher = undefined;
    const before = await metrics();
    expect(metric(before, 'wagering_outbox_lag_seconds')).toBe(0);

    publisher = Bun.spawn(['bun', 'run', 'src/outbox-publisher.ts'], {
      cwd: workspaceRoot,
      env: workerEnv({
        WORKER_METRICS_PORT: String(PUBLISHER_METRICS_PORT),
        SQS_ENDPOINT: 'http://127.0.0.1:1',
        OUTBOX_PUBLISHER_RETRY_BASE_MS: '5000',
        OUTBOX_PUBLISHER_RETRY_MAX_MS: '5000',
      }),
      stdout: 'ignore',
      stderr: 'ignore',
    });
    const failedPublisherUrl = `http://127.0.0.1:${PUBLISHER_METRICS_PORT}`;
    await waitForLive(failedPublisherUrl);
    const publisherBefore = await metrics(failedPublisherUrl);
    expect(publisherBefore).toContain('wagering_process_info{process="outbox-publisher"} 1');
    const failuresBefore = metricSamples(publisherBefore, 'wagering_outbox_publications_total').find((sample) => sample.labels.outcome === 'send_failed')?.value ?? 0;

    expect((await wagerHttp(target, 'BET', '10.00', 'm3-event')).status).toBe(200);
    const stableEventId = (await outboxRows(target.id)).find((row) => row.published_at === null)!.id;
    await waitFor(async () => (await outboxRows(target.id)).some((row) => row.attempts > 0 && row.next_attempt_at !== null), 'persisted SQS retry');
    const failedRows = await outboxRows(target.id);
    expect(failedRows.some((row) => row.id === stableEventId && row.published_at === null && row.attempts > 0 && row.next_attempt_at !== null)).toBe(true);
    const lagAfterFailure = await metrics();
    expect(metric(lagAfterFailure, 'wagering_outbox_messages_state', { state: 'pending' })).toBeGreaterThan(0);
    expect(metric(lagAfterFailure, 'wagering_outbox_retry_scheduled_state')).toBeGreaterThan(0);
    expect(metric(lagAfterFailure, 'wagering_outbox_lag_seconds')).toBeGreaterThan(0);
    const publisherAfterFailure = await metrics(failedPublisherUrl);
    expect(metric(publisherAfterFailure, 'wagering_outbox_publications_total', { outcome: 'send_failed' })).toBeGreaterThan(failuresBefore);

    // Hold the durable retry long enough to scrape the replacement process before it can publish.
    // Without this gate, startup timing can publish the already-pending event before `goodBefore`.
    await orm.em.getConnection().execute(
      "update outbox_messages set next_attempt_at = now() + interval '5 seconds' where id = ? and published_at is null",
      [stableEventId],
    );
    expect((await outboxRows(target.id)).find((row) => row.id === stableEventId)?.published_at).toBeNull();

    await stop(publisher);
    publisher = Bun.spawn(['bun', 'run', 'src/outbox-publisher.ts'], {
      cwd: workspaceRoot,
      env: workerEnv({ WORKER_METRICS_PORT: String(PUBLISHER_METRICS_PORT), OUTBOX_PUBLISHER_RETRY_BASE_MS: '20', OUTBOX_PUBLISHER_RETRY_MAX_MS: '50' }),
      stdout: 'ignore',
      stderr: 'ignore',
    });
    const goodPublisherUrl = `http://127.0.0.1:${PUBLISHER_METRICS_PORT}`;
    await waitForLive(goodPublisherUrl);
    const goodBefore = await metrics(goodPublisherUrl);
    const publishedBefore = metricSamples(goodBefore, 'wagering_outbox_publications_total').find((sample) => sample.labels.outcome === 'published')?.value ?? 0;
    await waitFor(async () => (await outboxRows(target.id)).every((row) => row.published_at !== null), 'durable outbox publication');

    const publishedRows = await outboxRows(target.id);
    expect(publishedRows.find((row) => row.id === stableEventId)?.published_at).not.toBeNull();
    const afterSuccess = await metrics();
    expect(metric(afterSuccess, 'wagering_outbox_messages_state', { state: 'pending' })).toBe(0);
    expect(metric(afterSuccess, 'wagering_outbox_lag_seconds')).toBe(0);
    const goodAfter = await metrics(goodPublisherUrl);
    expect(metric(goodAfter, 'wagering_outbox_publications_total', { outcome: 'published' })).toBeGreaterThan(publishedBefore);
  }, 45_000);

  test('M4: real FIFO redelivery advances ReceiveCount, consumer-local retry changes, and LocalStack DLQ gauge comes from GetQueueAttributes', async () => {
    const target = await wallet('100.00');
    const financialBeforePoison = await financialCounts(target.id);
    const poison = { messageId: 'bad', type: 'unknown' };
    await sqs.send(new SendMessageCommand({
      QueueUrl: queues.transactionsUrl,
      MessageBody: JSON.stringify(poison),
      MessageGroupId: target.id,
      MessageDeduplicationId: randomUUID(),
    }));

    const first = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queues.transactionsUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 0, MessageSystemAttributeNames: ['ApproximateReceiveCount'] }));
    expect(first.Messages).toHaveLength(1);
    expect(first.Messages![0]!.Attributes?.ApproximateReceiveCount).toBe('1');
    await sqs.send(new ChangeMessageVisibilityCommand({
      QueueUrl: queues.transactionsUrl,
      ReceiptHandle: first.Messages![0]!.ReceiptHandle!,
      VisibilityTimeout: 1,
    }));
    let secondMessage: NonNullable<typeof first.Messages>[number] | undefined;
    await waitFor(async () => {
      const received = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queues.transactionsUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 0, MessageSystemAttributeNames: ['ApproximateReceiveCount'] }));
      secondMessage = received.Messages?.[0];
      return secondMessage !== undefined;
    }, 'second real SQS receive after visibility timeout', 5_000);
    expect(Number(secondMessage!.Attributes?.ApproximateReceiveCount)).toBeGreaterThan(1);
    await sqs.send(new ChangeMessageVisibilityCommand({
      QueueUrl: queues.transactionsUrl,
      ReceiptHandle: secondMessage!.ReceiptHandle!,
      VisibilityTimeout: 1,
    }));

    consumer = Bun.spawn(['bun', 'run', 'src/sqs-consumer.ts'], {
      cwd: workspaceRoot,
      env: workerEnv({
        WORKER_METRICS_PORT: String(CONSUMER_METRICS_PORT),
        SQS_CONSUMER_CONCURRENCY: '1',
        SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS: '1',
      }),
      stdout: 'ignore',
      stderr: 'ignore',
    });
    const consumerUrl = `http://127.0.0.1:${CONSUMER_METRICS_PORT}`;
    await waitForLive(consumerUrl);
    const consumerBefore = await metrics(consumerUrl);
    expect(consumerBefore).toContain('wagering_process_info{process="sqs-consumer"} 1');
    const redeliveryBefore = metricSamples(consumerBefore, 'wagering_retries_total').find((sample) => sample.labels.flow === 'sqs_consumer' && sample.labels.reason === 'redelivery')?.value ?? 0;

    await waitFor(async () => (await queueVisibleCount(queues.transactionsDlqUrl)) > 0, 'FIFO DLQ transfer', 20_000);
    const consumerAfter = await metrics(consumerUrl);
    expect(metric(consumerAfter, 'wagering_retries_total', { flow: 'sqs_consumer', reason: 'redelivery' })).toBeGreaterThan(redeliveryBefore);

    const apiAfter = await metrics();
    expect(apiAfter).toContain('# SCOPE wagering_dlq_messages_approximate approximate_sqs');
    await waitFor(async () => metric(await metrics(), 'wagering_dlq_messages_approximate', { queue: 'wager_transactions_dlq' }) > 0, 'API scrape of LocalStack DLQ attributes');
    const exposedDlqDepth = metric(await metrics(), 'wagering_dlq_messages_approximate', { queue: 'wager_transactions_dlq' });
    const attributes = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: queues.transactionsDlqUrl, AttributeNames: ['ApproximateNumberOfMessages'] }));
    expect(exposedDlqDepth).toBe(Number(attributes.Attributes?.ApproximateNumberOfMessages ?? '0'));
    expect(await financialCounts(target.id)).toEqual(financialBeforePoison);
  }, 45_000);

  test('M6: a real financial operation observes exactly one latency sample in seconds with cumulative histogram buckets', async () => {
    const target = await wallet('100.00');
    const labels = { operation: 'process_wager', outcome: 'processed' };
    const before = await metrics();
    expect(before).toContain('# TYPE wagering_processing_duration_seconds histogram');
    expect(before).toContain('# SCOPE wagering_processing_duration_seconds process_local');
    expect(before).toContain('# UNIT wagering_processing_duration_seconds seconds');
    expect(before).toContain(`# BUCKETS wagering_processing_duration_seconds ${DURATION_BUCKETS}`);
    const countBefore = metricOrZero(before, 'wagering_processing_duration_seconds_count', labels);
    const sumBefore = metricOrZero(before, 'wagering_processing_duration_seconds_sum', labels);

    const startedAt = Date.now();
    expect((await wagerHttp(target, 'BET', '10.00', 'm6-bet')).status).toBe(200);
    const wallClockSeconds = (Date.now() - startedAt) / 1000;
    const persisted = await financialCounts(target.id);
    expect(persisted.ledger).toBeGreaterThan(0);
    expect(persisted.balance).toBe('90.00');

    const after = await metrics();
    const countAfter = metric(after, 'wagering_processing_duration_seconds_count', labels);
    const sumAfter = metric(after, 'wagering_processing_duration_seconds_sum', labels);
    expect(countAfter).toBe(countBefore + 1);
    const observed = sumAfter - sumBefore;
    expect(observed).toBeGreaterThan(0);
    // The sample is the duration of the unit of work, in seconds: it cannot exceed the HTTP
    // round trip that contained it, which also rules out a millisecond value exposed as seconds.
    expect(observed).toBeLessThanOrEqual(wallClockSeconds);

    const buckets = metricSamples(after, 'wagering_processing_duration_seconds_bucket')
      .filter((sample) => sample.labels.operation === 'process_wager' && sample.labels.outcome === 'processed');
    const finite = buckets
      .filter((sample) => sample.labels.le !== '+Inf')
      .sort((left, right) => Number(left.labels.le) - Number(right.labels.le));
    expect(finite.map((sample) => sample.labels.le).join(',')).toBe(DURATION_BUCKETS);
    let previous = 0;
    for (const sample of finite) {
      expect(Number.isInteger(sample.value)).toBe(true);
      expect(sample.value).toBeGreaterThanOrEqual(previous);
      previous = sample.value;
    }
    const infinite = buckets.find((sample) => sample.labels.le === '+Inf');
    expect(infinite?.value).toBe(countAfter);
    // Every observation is below the largest declared boundary, so the cumulative 10s bucket
    // must already hold the whole population: a negative or non-seconds value could not.
    expect(metric(after, 'wagering_processing_duration_seconds_bucket', { ...labels, le: '10' })).toBe(countAfter);
    for (const sample of metricSamples(after, 'wagering_processing_duration_seconds_sum')) {
      expect(sample.value).toBeGreaterThanOrEqual(0);
    }
  }, 30_000);

  test('M7: a controlled ledger divergence is reported by reconciliation and by the PostgreSQL gauge, and is never repaired automatically', async () => {
    const target = await wallet('100.00');
    expect((await wagerHttp(target, 'BET', '10.00', 'm7-bet')).status).toBe(200);
    const baseline = await financialCounts(target.id);
    expect(baseline.balance).toBe('90.00');

    const before = await metrics();
    expect(before).toContain('# SCOPE wagering_reconciliation_divergent_wallets_state global_postgresql');
    expect(before).toContain('# SCOPE wagering_reconciliation_checks_total process_local');
    expect(metric(before, 'wagering_reconciliation_divergent_wallets_state')).toBe(0);
    const consistentBefore = metricOrZero(before, 'wagering_reconciliation_checks_total', { result: 'consistent' });
    const inconsistentBefore = metricOrZero(before, 'wagering_reconciliation_checks_total', { result: 'inconsistent' });

    const consistentCheck = await call(`/wallets/${target.id}/reconciliation`, {});
    expect(consistentCheck).toMatchObject({
      status: 200,
      body: { consistent: true, storedBalance: '90.00', calculatedBalance: '90.00', difference: '0.00' },
    });
    const checkedEntries = consistentCheck.body.checkedEntries as number;
    expect(checkedEntries).toBe(baseline.ledger);
    expect(metric(await metrics(), 'wagering_reconciliation_checks_total', { result: 'consistent' })).toBe(consistentBefore + 1);

    /**
     * Controlled divergence injected only by this test, directly in the disposable database:
     * the stored balance is moved without touching the immutable ledger. Production code has
     * no path that does this, which is exactly why the divergence must be *observable*.
     *
     * The version bump is not cosmetic: `enforce_wallet_version_on_balance_change` rejects any
     * balance change that does not increment the version exactly once, so even this controlled
     * injection has to respect the durable optimistic-locking invariant.
     */
    await orm.em.getConnection().execute(
      'update wallets set balance = balance + 25, version = version + 1 where id = ?',
      [target.id],
    );
    const divergentState = await financialCounts(target.id);
    expect(divergentState.balance).toBe('115.00');
    expect(divergentState.ledger).toBe(baseline.ledger);

    const duringDivergence = await metrics();
    expect(metric(duringDivergence, 'wagering_reconciliation_divergent_wallets_state')).toBe(1);
    expect(metric(duringDivergence, 'wagering_ledger_entries_state')).toBe(baseline.ledger);

    const divergentCheck = await call(`/wallets/${target.id}/reconciliation`, {});
    expect(divergentCheck).toMatchObject({
      status: 200,
      body: {
        walletId: target.id,
        consistent: false,
        storedBalance: '115.00',
        calculatedBalance: '90.00',
        difference: '25.00',
        checkedEntries,
      },
    });
    const afterDiagnosis = await metrics();
    expect(metric(afterDiagnosis, 'wagering_reconciliation_checks_total', { result: 'inconsistent' })).toBe(inconsistentBefore + 1);

    // No automatic repair: the diagnosis changed no durable financial state, and the wallet is
    // still reported as divergent on the next scrape.
    expect(await financialCounts(target.id)).toEqual(divergentState);
    expect(metric(afterDiagnosis, 'wagering_reconciliation_divergent_wallets_state')).toBe(1);
    expect((await call(`/wallets/${target.id}/reconciliation`, {})).body).toMatchObject({ consistent: false, difference: '25.00' });
    expect(await financialCounts(target.id)).toEqual(divergentState);

    // Restoration happens only through this test's injection mechanism, never through the API,
    // and it is subject to the same version guard as the injection.
    await orm.em.getConnection().execute(
      'update wallets set balance = balance - 25, version = version + 1 where id = ?',
      [target.id],
    );
    expect(await financialCounts(target.id)).toEqual(baseline);
    expect((await call(`/wallets/${target.id}/reconciliation`, {})).body).toMatchObject({ consistent: true, difference: '0.00' });
    expect(metric(await metrics(), 'wagering_reconciliation_divergent_wallets_state')).toBe(0);
  }, 30_000);

  test('M8: a real SQS outage increments the collector error, keeps PostgreSQL metrics real, fabricates no queue depth, and recovers', async () => {
    const target = await wallet('100.00');
    expect((await wagerHttp(target, 'BET', '10.00', 'm8-bet')).status).toBe(200);
    const financialBefore = await financialCounts(target.id);

    sqsProxy = startSqsProxy(SQS_PROXY_PORT);
    const degradedUrl = `http://127.0.0.1:${DEGRADED_API_PORT}`;
    // This process reaches real LocalStack only through the proxy and resolves its queues by
    // name, so cutting the proxy is a genuine SQS unavailability for it.
    const env: Record<string, string> = {
      ...Bun.env,
      PORT: String(DEGRADED_API_PORT),
      SQS_ENDPOINT: `http://127.0.0.1:${SQS_PROXY_PORT}`,
      WAGER_EVENTS_QUEUE_NAME: queueNameOf(queues.eventsUrl),
      WAGER_TRANSACTIONS_QUEUE_NAME: queueNameOf(queues.transactionsUrl),
      WAGER_TRANSACTIONS_DLQ_QUEUE_NAME: queueNameOf(queues.transactionsDlqUrl),
    };
    delete env.WAGER_EVENTS_QUEUE_URL;
    delete env.WAGER_TRANSACTIONS_QUEUE_URL;
    delete env.WAGER_TRANSACTIONS_DLQ_QUEUE_URL;
    degradedApi = Bun.spawn(['bun', 'run', 'src/main.ts'], {
      cwd: workspaceRoot,
      env,
      stdout: 'ignore',
      stderr: 'ignore',
    });
    await waitForLive(degradedUrl);

    const healthy = await metrics(degradedUrl);
    expect(healthy).toContain('wagering_process_info{process="api"} 1');
    expect(healthy).toContain('# SCOPE wagering_sqs_queue_messages_approximate approximate_sqs');
    expect(metricOrZero(healthy, 'wagering_metrics_collection_errors_total', { collector: 'sqs' })).toBe(0);
    expect(metric(healthy, 'wagering_sqs_queue_messages_approximate', { queue: 'wager_transactions', state: 'visible' }))
      .toBe(await queueVisibleCount(queues.transactionsUrl));
    expect(metric(healthy, 'wagering_ledger_entries_state')).toBe(financialBefore.ledger);

    sqsProxy.stop(true);
    sqsProxy = undefined;

    const degraded = await metrics(degradedUrl);
    expect(metric(degraded, 'wagering_metrics_collection_errors_total', { collector: 'sqs' })).toBeGreaterThan(0);
    // No fabricated healthy depth: the approximate_sqs series are absent, not zeroed.
    expect(metricSamples(degraded, 'wagering_sqs_queue_messages_approximate')).toHaveLength(0);
    expect(metricSamples(degraded, 'wagering_dlq_messages_approximate')).toHaveLength(0);
    // PostgreSQL scope keeps answering with real durable state during the SQS outage.
    expect(degraded).toContain('# SCOPE wagering_ledger_entries_state global_postgresql');
    expect(metric(degraded, 'wagering_ledger_entries_state')).toBe(financialBefore.ledger);
    expect(metric(degraded, 'wagering_transactions_state', { status: 'PROCESSED', kind: 'BET' })).toBeGreaterThan(0);
    const errorsAtOutage = metric(degraded, 'wagering_metrics_collection_errors_total', { collector: 'sqs' });
    expect(metric(await metrics(degradedUrl), 'wagering_metrics_collection_errors_total', { collector: 'sqs' })).toBeGreaterThan(errorsAtOutage);

    sqsProxy = startSqsProxy(SQS_PROXY_PORT);
    await waitFor(
      async () => metricSamples(await metrics(degradedUrl), 'wagering_sqs_queue_messages_approximate').length > 0,
      'SQS gauges after LocalStack connectivity is restored',
    );
    const recovered = await metrics(degradedUrl);
    const errorsAfterRecovery = metric(recovered, 'wagering_metrics_collection_errors_total', { collector: 'sqs' });
    expect(metric(recovered, 'wagering_sqs_queue_messages_approximate', { queue: 'wager_transactions', state: 'visible' }))
      .toBe(await queueVisibleCount(queues.transactionsUrl));
    expect(metric(recovered, 'wagering_ledger_entries_state')).toBe(financialBefore.ledger);
    // A recovered collector stops failing: the counter is frozen across a further scrape.
    expect(metric(await metrics(degradedUrl), 'wagering_metrics_collection_errors_total', { collector: 'sqs' })).toBe(errorsAfterRecovery);
    expect(await financialCounts(target.id)).toEqual(financialBefore);

    await stop(degradedApi);
    degradedApi = undefined;
  }, 60_000);
});
