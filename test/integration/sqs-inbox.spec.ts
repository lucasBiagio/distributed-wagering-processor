import { randomUUID } from 'node:crypto';

import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SetQueueAttributesCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { WagerTransactionStatus } from '../../src/domain/wager-transaction.js';
import { WAGER_TRANSACTION_CONSUMER_NAME, SqsWagerConsumer } from '../../src/infrastructure/messaging/sqs-wager-consumer.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';
import { MikroOrmFinancialProcessing, type UnitOfWorkStage } from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';

interface QueueSet {
  readonly queueUrl: string;
  readonly dlqUrl: string;
}

interface EnvelopeOverrides {
  readonly messageId?: string;
  readonly data?: Partial<{
    providerId: string;
    externalTransactionId: string;
    idempotencyKey: string;
    playerId: string;
    walletId: string;
    roundId: string;
    gameId: string;
    kind: 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
    money: { amount: string; currency: string };
    referenceExternalTransactionId: string;
  }>;
}

let orm: MikroORM<PostgreSqlDriver>;
let processing: MikroOrmFinancialProcessing;
let client: SQSClient;
let queues: QueueSet;
let sequence = 0;

function nextId(prefix: string): string {
  sequence += 1;
  return `${prefix}-${process.pid}-${sequence}-${randomUUID().slice(0, 8)}`;
}

function player(suffix: number): string {
  return `00000000-0000-4000-8000-${String(suffix).padStart(12, '0')}`;
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await Bun.sleep(100);
  }
  throw new Error('Timed out waiting for expected SQS/PostgreSQL state');
}

async function createQueues(): Promise<QueueSet> {
  const dlqName = `${nextId('cp3a-dlq')}.fifo`;
  const queueName = `${nextId('cp3a-wager')}.fifo`;
  const dlqUrl = (await client.send(new CreateQueueCommand({
    QueueName: dlqName,
    Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
  }))).QueueUrl!;
  const dlqArn = (await client.send(new GetQueueAttributesCommand({
    QueueUrl: dlqUrl,
    AttributeNames: ['QueueArn'],
  }))).Attributes!.QueueArn!;
  const queueUrl = (await client.send(new CreateQueueCommand({
    QueueName: queueName,
    Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false', VisibilityTimeout: '1' },
  }))).QueueUrl!;
  await client.send(new SetQueueAttributesCommand({
    QueueUrl: queueUrl,
    Attributes: {
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: '5' }),
      VisibilityTimeout: '1',
    },
  }));
  return { queueUrl, dlqUrl };
}

function envelope(overrides: EnvelopeOverrides = {}): { messageId: string; type: string; occurredAt: string; data: Record<string, unknown> } {
  const suffix = nextId('delivery');
  return {
    messageId: overrides.messageId ?? nextId('message'),
    type: 'WagerTransactionRequested',
    occurredAt: '2026-10-29T03:00:00.000Z',
    data: {
      providerId: 'sqs-provider',
      externalTransactionId: `external-${suffix}`,
      idempotencyKey: `key-${suffix}`,
      playerId: '',
      walletId: '',
      roundId: `round-${suffix}`,
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
      ...overrides.data,
    },
  };
}

async function publish(payload: unknown, groupId: string, deduplicationId = randomUUID()): Promise<void> {
  await client.send(new SendMessageCommand({
    QueueUrl: queues.queueUrl,
    MessageBody: JSON.stringify(payload),
    MessageGroupId: groupId,
    MessageDeduplicationId: deduplicationId,
  }));
}

async function openWallet(balance = '100.00'): Promise<{ id: string; playerId: string }> {
  const view = await processing.openWallet({
    playerId: player(++sequence),
    initialBalance: { amount: balance, currency: 'BRL' },
  });
  return { id: view.id, playerId: view.playerId };
}

async function counts(walletId: string): Promise<{ inbox: number; ledger: number; outbox: number; transactions: number }> {
  const rows = await orm.em.getConnection().execute(
    `select
       (select count(*)::int from inbox_messages where consumer_name = ?) as inbox,
       (select count(*)::int from wallet_ledger_entries where wallet_id = ?) as ledger,
       (select count(*)::int from outbox_messages) as outbox,
       (select count(*)::int from wager_transactions where wallet_id = ?) as transactions`,
    [WAGER_TRANSACTION_CONSUMER_NAME, walletId, walletId],
  ) as Array<{ inbox: number; ledger: number; outbox: number; transactions: number }>;
  return rows[0]!;
}

async function queueIsEmpty(queueUrl: string): Promise<boolean> {
  const message = await client.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, WaitTimeSeconds: 0, MaxNumberOfMessages: 1 }));
  return (message.Messages ?? []).length === 0;
}

class FailingBeforeCommitProcessing extends MikroOrmFinancialProcessing {
  public failed = false;

  protected override async interceptUnitOfWork(stage: UnitOfWorkStage): Promise<void> {
    if (stage === 'before-commit') {
      this.failed = true;
      throw new Error('test-only failure before commit');
    }
  }
}

describe('CP3A SQS transactional inbox with real PostgreSQL and LocalStack', () => {
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
    queues = await createQueues();
    process.env.WAGER_TRANSACTIONS_QUEUE_URL = queues.queueUrl;
    process.env.SQS_CONSUMER_CONCURRENCY = '1';
    process.env.SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS = '1';
  });

  afterEach(async () => {
    await client.send(new DeleteQueueCommand({ QueueUrl: queues.queueUrl }));
    await client.send(new DeleteQueueCommand({ QueueUrl: queues.dlqUrl }));
    delete process.env.WAGER_TRANSACTIONS_QUEUE_URL;
  });

  afterAll(async () => {
    await client.destroy();
    await orm.getMigrator().down({ to: 0 });
    await orm.close(true);
  });

  test('TEST-A/B: valid delivery commits inbox, financial effects and outbox once; redelivery is acknowledged without duplicates', async () => {
    const wallet = await openWallet();
    const message = envelope({ data: { walletId: wallet.id, playerId: wallet.playerId } });
    await publish(message, wallet.id);

    await new SqsWagerConsumer(client, processing).run({ maxAcknowledgements: 1 });
    const first = await processing.getProviderTransaction('sqs-provider', message.data.externalTransactionId as string);
    expect(first).toMatchObject({ status: WagerTransactionStatus.Processed, balance: { amount: '75.00', currency: 'BRL' } });
    expect(await counts(wallet.id)).toEqual({ inbox: 1, ledger: 2, outbox: 4, transactions: 2 });
    expect(await queueIsEmpty(queues.queueUrl)).toBe(true);

    await publish(message, wallet.id, randomUUID());
    await new SqsWagerConsumer(client, processing).run({ maxAcknowledgements: 1 });
    expect(await counts(wallet.id)).toEqual({ inbox: 1, ledger: 2, outbox: 4, transactions: 2 });
    expect(await processing.getWallet(wallet.id)).toMatchObject({ balance: { amount: '75.00', currency: 'BRL' }, version: 2 });
    expect(await queueIsEmpty(queues.queueUrl)).toBe(true);
  });

  test('TEST-C: a real worker crash after commit before ACK is recovered by another process without duplicate effects', async () => {
    const wallet = await openWallet();
    const message = envelope({ data: { walletId: wallet.id, playerId: wallet.playerId } });
    await publish(message, wallet.id);

    const crash = Bun.spawn(['bun', 'run', 'scripts/cp3a-sqs-consumer.ts'], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        WAGER_TRANSACTIONS_QUEUE_URL: queues.queueUrl,
        SQS_CONSUMER_CONCURRENCY: '1',
        SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS: '1',
        CP3A_TEST_CRASH_AFTER_COMMIT_BEFORE_ACK: message.messageId,
      },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const crashStderr = await new Response(crash.stderr).text();
    await crash.exited;
    expect(crash.exitCode, crashStderr).toBe(86);
    expect(await counts(wallet.id)).toEqual({ inbox: 1, ledger: 2, outbox: 4, transactions: 2 });

    await Bun.sleep(1_100);
    const recovery = Bun.spawn(['bun', 'run', 'scripts/cp3a-sqs-consumer.ts'], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        WAGER_TRANSACTIONS_QUEUE_URL: queues.queueUrl,
        SQS_CONSUMER_CONCURRENCY: '1',
        SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS: '1',
        SQS_CONSUMER_EXIT_AFTER_ACKS: '1',
      },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const recoveryStderr = await new Response(recovery.stderr).text();
    await recovery.exited;
    expect(recovery.exitCode, recoveryStderr).toBe(0);
    expect(await counts(wallet.id)).toEqual({ inbox: 1, ledger: 2, outbox: 4, transactions: 2 });
    expect(await queueIsEmpty(queues.queueUrl)).toBe(true);
  });

  test('TEST-D: a failure before commit leaves no inbox or financial partial effect and redelivery succeeds', async () => {
    const wallet = await openWallet();
    const message = envelope({ data: { walletId: wallet.id, playerId: wallet.playerId } });
    await publish(message, wallet.id);

    const failing = new FailingBeforeCommitProcessing(orm);
    const failingConsumer = new SqsWagerConsumer(client, failing);
    const run = failingConsumer.run();
    await waitFor(async () => failing.failed);
    await failingConsumer.stop();
    await run;
    expect(await counts(wallet.id)).toEqual({ inbox: 0, ledger: 1, outbox: 2, transactions: 1 });

    await Bun.sleep(1_100);
    await new SqsWagerConsumer(client, processing).run({ maxAcknowledgements: 1 });
    expect(await counts(wallet.id)).toEqual({ inbox: 1, ledger: 2, outbox: 4, transactions: 2 });
  });

  test('TEST-E/F: durable insufficient-balance rejection and PENDING_REFERENCE are both acknowledged after their atomic commits', async () => {
    const rejectedWallet = await openWallet('10.00');
    const rejected = envelope({ data: { walletId: rejectedWallet.id, playerId: rejectedWallet.playerId, money: { amount: '25.00', currency: 'BRL' } } });
    await publish(rejected, rejectedWallet.id);
    await new SqsWagerConsumer(client, processing).run({ maxAcknowledgements: 1 });
    expect(await processing.getProviderTransaction('sqs-provider', rejected.data.externalTransactionId as string)).toMatchObject({
      status: WagerTransactionStatus.Rejected,
      failureCode: 'INSUFFICIENT_BALANCE',
    });
    expect(await queueIsEmpty(queues.queueUrl)).toBe(true);

    const pendingWallet = await openWallet();
    const refund = envelope({ data: {
      walletId: pendingWallet.id,
      playerId: pendingWallet.playerId,
      kind: 'REFUND',
      externalTransactionId: 'refund-before-bet',
      idempotencyKey: 'refund-before-bet-key',
      referenceExternalTransactionId: 'bet-arrives-later',
    } });
    await publish(refund, pendingWallet.id);
    await new SqsWagerConsumer(client, processing).run({ maxAcknowledgements: 1 });
    expect(await processing.getProviderTransaction('sqs-provider', 'refund-before-bet')).toMatchObject({ status: WagerTransactionStatus.PendingReference });
    expect(await queueIsEmpty(queues.queueUrl)).toBe(true);

    await processing.processWager({
      providerId: 'sqs-provider', externalTransactionId: 'bet-arrives-later', idempotencyKey: 'bet-arrives-later-key',
      walletId: pendingWallet.id, playerId: pendingWallet.playerId, roundId: refund.data.roundId as string,
      gameId: 'fortune-chimp', kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
    });
    const swept = await processing.reprocessPendingReferences({ now: new Date(Date.now() + 6_000) });
    expect(swept.processed).toBe(1);
    expect(await processing.getWallet(pendingWallet.id)).toMatchObject({ balance: { amount: '100.00', currency: 'BRL' } });
  });

  test('TEST-G: a permanently malformed message reaches the real FIFO DLQ without financial movement', async () => {
    const wallet = await openWallet();
    await publish({ messageId: 'bad', type: 'unknown' }, wallet.id);
    const consumer = new SqsWagerConsumer(client, processing);
    const run = consumer.run();
    let dlqMessageId: string | undefined;
    await waitFor(async () => {
      const delivery = await client.send(new ReceiveMessageCommand({
        QueueUrl: queues.dlqUrl,
        WaitTimeSeconds: 0,
        MaxNumberOfMessages: 1,
      }));
      dlqMessageId = delivery.Messages?.[0]?.MessageId;
      return dlqMessageId !== undefined;
    }, 15_000);
    await consumer.stop();
    await run;
    expect(await counts(wallet.id)).toEqual({ inbox: 0, ledger: 1, outbox: 2, transactions: 1 });
    expect(dlqMessageId).toBeDefined();
  });

  test('TEST-H: three real consumer processes share SQS/PostgreSQL without duplicate debit, ledger or outbox', async () => {
    const wallet = await openWallet('1000.00');
    const base = envelope({ data: {
      walletId: wallet.id, playerId: wallet.playerId, externalTransactionId: 'three-consumer-external', idempotencyKey: 'three-consumer-key',
    } });
    const deliveries = [base, { ...base, messageId: `${base.messageId}-2` }, { ...base, messageId: `${base.messageId}-3` }];
    for (const delivery of deliveries) {
      await publish(delivery, wallet.id, randomUUID());
    }
    const processes = Array.from({ length: 3 }, () => Bun.spawn(['bun', 'run', 'scripts/cp3a-sqs-consumer.ts'], {
      cwd: process.cwd(),
      env: {
        ...process.env, WAGER_TRANSACTIONS_QUEUE_URL: queues.queueUrl, SQS_CONSUMER_CONCURRENCY: '1',
        SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS: '1', SQS_CONSUMER_EXIT_AFTER_ACKS: '1',
      },
      stdout: 'pipe', stderr: 'pipe',
    }));
    for (const child of processes) {
      const stderr = await new Response(child.stderr).text();
      await child.exited;
      expect(child.exitCode, stderr).toBe(0);
    }
    expect(await counts(wallet.id)).toEqual({ inbox: 3, ledger: 2, outbox: 4, transactions: 2 });
    expect(await processing.getWallet(wallet.id)).toMatchObject({ balance: { amount: '975.00', currency: 'BRL' }, version: 2 });
  });

  test('TEST-I: HTTP-equivalent processing followed by SQS delivery preserves the original financial identity while recording the SQS inbox', async () => {
    const wallet = await openWallet();
    const command = {
      providerId: 'sqs-provider', externalTransactionId: 'cross-http-sqs', idempotencyKey: 'cross-http-sqs-key',
      walletId: wallet.id, playerId: wallet.playerId, roundId: 'cross-http-sqs-round', gameId: 'fortune-chimp',
      kind: 'BET' as const, money: { amount: '25.00', currency: 'BRL' },
    };
    const http = await processing.processWager(command);
    const message = envelope({ data: command });
    await publish(message, wallet.id);
    await new SqsWagerConsumer(client, processing).run({ maxAcknowledgements: 1 });
    const replay = await processing.getProviderTransaction(command.providerId, command.externalTransactionId);
    expect(replay).toMatchObject({ transactionId: http.transactionId, idempotentReplay: false, balance: { amount: '75.00', currency: 'BRL' } });
    expect(await counts(wallet.id)).toEqual({ inbox: 1, ledger: 2, outbox: 4, transactions: 2 });
  });
});
