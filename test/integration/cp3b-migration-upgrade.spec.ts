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

import { WagerTransactionStatus } from '../../src/domain/wager-transaction.js';
import { SqsOutboxPublisher } from '../../src/infrastructure/messaging/sqs-outbox-publisher.js';
import { MikroOrmFinancialProcessing } from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

interface LegacyEvent {
  readonly id: string;
  readonly walletId: string;
  readonly transactionId: string;
  readonly eventType: 'WagerTransactionProcessed' | 'WagerTransactionPendingReference' | 'WalletBalanceChanged';
  readonly occurredAt: Date;
}

interface SequencedRow {
  readonly id: string;
  readonly wallet_id: string;
  readonly enqueue_sequence: string;
}

let orm: MikroORM<PostgreSqlDriver>;
let client: SQSClient;
let queueUrl: string;

function event(
  walletId: string,
  transactionId: string,
  eventType: LegacyEvent['eventType'],
  occurredAt: Date,
): LegacyEvent {
  return { id: randomUUID(), walletId, transactionId, eventType, occurredAt };
}

function payload(legacy: LegacyEvent): Record<string, unknown> {
  return {
    eventId: legacy.id,
    eventType: legacy.eventType,
    aggregateId: legacy.eventType === 'WalletBalanceChanged' ? legacy.walletId : legacy.transactionId,
    correlationId: legacy.transactionId,
    occurredAt: legacy.occurredAt.toISOString(),
    version: 1,
    data: {
      transactionId: legacy.transactionId,
      walletId: legacy.walletId,
    },
  };
}

async function persistLegacyTransaction(
  walletId: string,
  playerId: string,
  transactionId: string,
  kind: 'OPENING' | 'BET' | 'LOSS',
  amount: string,
  balance: string,
  createdAt: Date,
): Promise<void> {
  const connection = orm.em.getConnection();
  await connection.execute(
    `insert into wager_transactions (
       id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount, currency, reference_transaction_id, reference_external_transaction_id,
       status, failure_code, processed_at, resulting_balance, reference_attempts, reference_next_attempt_at, created_at
     ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'BRL', null, null, 'PROCESSED', null, ?, ?, 0, null, ?)`,
    [
      transactionId,
      'cp3b-upgrade-provider',
      kind === 'OPENING' ? null : `external-${transactionId}`,
      `legacy-${transactionId}`,
      `legacy-hash-${transactionId}`,
      walletId,
      playerId,
      `round-${transactionId}`,
      'cp3b-upgrade-game',
      kind,
      amount,
      createdAt,
      balance,
      createdAt,
    ],
  );

  if (kind === 'LOSS') {
    return;
  }

  const direction = kind === 'BET' ? 'DEBIT' : 'CREDIT';
  const before = kind === 'BET' ? (Number(balance) + Number(amount)).toFixed(2) : '0.00';
  await connection.execute(
    `insert into wallet_ledger_entries (
       id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at
     ) values (?, ?, ?, ?, ?, 'BRL', ?, ?, ?)`,
    [randomUUID(), walletId, transactionId, direction, amount, before, balance, createdAt],
  );
}

async function persistLegacyPendingRefund(
  walletId: string,
  playerId: string,
  transactionId: string,
  referenceExternalTransactionId: string,
  createdAt: Date,
): Promise<void> {
  await orm.em.getConnection().execute(
    `insert into wager_transactions (
       id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount, currency, reference_transaction_id, reference_external_transaction_id,
       status, failure_code, processed_at, resulting_balance, reference_attempts, reference_next_attempt_at, created_at
     ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'REFUND', '10.00', 'BRL', null, ?, 'PENDING_REFERENCE', null, null, null, 0, ?, ?)`,
    [
      transactionId,
      'cp3b-upgrade-provider',
      `external-${transactionId}`,
      `legacy-${transactionId}`,
      `legacy-hash-${transactionId}`,
      walletId,
      playerId,
      `round-${transactionId}`,
      'cp3b-upgrade-game',
      referenceExternalTransactionId,
      createdAt,
      createdAt,
    ],
  );
}

async function resolveLegacyRefund(
  walletId: string,
  transactionId: string,
  referenceTransactionId: string,
  balance: string,
  resolvedAt: Date,
): Promise<void> {
  const connection = orm.em.getConnection();
  await connection.execute(
    `update wager_transactions
        set status = 'PROCESSED', reference_transaction_id = ?, processed_at = ?, resulting_balance = ?, reference_next_attempt_at = null
      where id = ?`,
    [referenceTransactionId, resolvedAt, balance, transactionId],
  );
  await connection.execute(
    `insert into wallet_ledger_entries (
       id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at
     ) values (?, ?, ?, 'CREDIT', '10.00', 'BRL', '90.00', ?, ?)`,
    [randomUUID(), walletId, transactionId, balance, resolvedAt],
  );
}

async function persistLegacyEvents(events: readonly LegacyEvent[]): Promise<void> {
  const connection = orm.em.getConnection();
  for (const legacy of events) {
    await connection.execute(
      `insert into outbox_messages (id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at, published_at)
       values (?, ?, ?, ?::jsonb, ?, 0, null, null)`,
      [
        legacy.id,
        legacy.eventType === 'WalletBalanceChanged' ? legacy.walletId : legacy.transactionId,
        legacy.eventType,
        JSON.stringify(payload(legacy)),
        legacy.occurredAt,
      ],
    );
  }
}

async function sequences(walletId: string): Promise<SequencedRow[]> {
  return await orm.em.getConnection().execute(
    `select id::text as id, wallet_id::text as wallet_id, enqueue_sequence::text as enqueue_sequence
       from outbox_messages
      where wallet_id = ?
      order by enqueue_sequence asc`,
    [walletId],
  ) as SequencedRow[];
}

async function outboxEventsFor(transactionId: string): Promise<Array<{ id: string; event_type: string }>> {
  return await orm.em.getConnection().execute(
    `select id::text as id, event_type
       from outbox_messages
      where payload #>> '{data,transactionId}' = ?
      order by enqueue_sequence asc`,
    [transactionId],
  ) as Array<{ id: string; event_type: string }>;
}

async function receive(expected: number): Promise<string[]> {
  const messages: string[] = [];
  const deadline = Date.now() + 20_000;
  while (messages.length < expected && Date.now() < deadline) {
    const response = await client.send(new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 0,
    }));
    for (const message of response.Messages ?? []) {
      messages.push(message.Body!);
      await client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle! }));
    }
    if (messages.length < expected) {
      await Bun.sleep(100);
    }
  }
  expect(messages).toHaveLength(expected);
  return messages;
}

async function upgradeToCp3b(): Promise<void> {
  await orm.getMigrator().up({ to: 'Migration20261029170000' });
}

async function assertCp3bColumns(expected: boolean): Promise<void> {
  const columns = await orm.em.getConnection().execute(
    `select column_name
       from information_schema.columns
      where table_schema = 'public'
        and table_name = 'outbox_messages'
        and column_name in ('wallet_id', 'enqueue_sequence', 'claim_token', 'lease_expires_at')
      order by column_name`,
  ) as Array<{ column_name: string }>;
  expect(columns).toHaveLength(expected ? 4 : 0);
}

describe('CP3B historical outbox migration upgrade with real PostgreSQL and LocalStack', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up({ to: 'Migration20261029150000' });
    client = new SQSClient({
      region: process.env.AWS_REGION ?? 'us-east-1',
      endpoint: process.env.SQS_ENDPOINT ?? 'http://localstack:4566',
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    });
  });

  beforeEach(async () => {
    await orm.getMigrator().down({ to: 'Migration20261029150000' });
    await orm.getMigrator().up({ to: 'Migration20261029150000' });
    await orm.em.getConnection().execute(
      'truncate table outbox_messages, wallet_ledger_entries, wager_transactions, wallets cascade',
    );
  });

  afterEach(async () => {
    if (queueUrl) {
      await client.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
      queueUrl = undefined as unknown as string;
    }
  });

  afterAll(async () => {
    await client.destroy();
    await orm.getMigrator().down({ to: 0 });
    await orm.close(true);
  });

  test('upgrades legacy outbox rows in historical financial order, publishes that order, and remains reversible', async () => {
    const connection = orm.em.getConnection();
    const walletA = randomUUID();
    const playerA = randomUUID();
    const walletB = randomUUID();
    const playerB = randomUUID();
    const openingA = randomUUID();
    const betA1 = randomUUID();
    const betA2 = randomUUID();
    const lossA = randomUUID();
    const openingB = randomUUID();
    const base = new Date('2026-10-29T12:00:00.000Z');
    const at = (seconds: number) => new Date(base.getTime() + seconds * 1_000);

    await connection.execute(
      `insert into wallets (id, player_id, currency, balance, version, created_at, updated_at)
       values (?, ?, 'BRL', '70.00', 3, ?, ?), (?, ?, 'BRL', '80.00', 1, ?, ?)`,
      [walletA, playerA, at(0), at(3), walletB, playerB, at(0), at(0)],
    );
    await persistLegacyTransaction(walletA, playerA, openingA, 'OPENING', '100.00', '100.00', at(0));
    await persistLegacyTransaction(walletA, playerA, betA1, 'BET', '10.00', '90.00', at(1));
    await persistLegacyTransaction(walletA, playerA, betA2, 'BET', '20.00', '70.00', at(2));
    await persistLegacyTransaction(walletA, playerA, lossA, 'LOSS', '0.00', '70.00', at(3));
    await persistLegacyTransaction(walletB, playerB, openingB, 'OPENING', '80.00', '80.00', at(0));

    const historicalA = [
      event(walletA, openingA, 'WagerTransactionProcessed', at(0)),
      event(walletA, openingA, 'WalletBalanceChanged', at(0)),
      event(walletA, betA1, 'WagerTransactionProcessed', at(1)),
      event(walletA, betA1, 'WalletBalanceChanged', at(1)),
      event(walletA, betA2, 'WagerTransactionProcessed', at(2)),
      event(walletA, betA2, 'WalletBalanceChanged', at(2)),
      event(walletA, lossA, 'WagerTransactionProcessed', at(3)),
    ];
    const historicalB = [
      event(walletB, openingB, 'WagerTransactionProcessed', at(0)),
      event(walletB, openingB, 'WalletBalanceChanged', at(0)),
    ];

    // Insert reverse physical order to prove the migration uses financial history, not heap order.
    await persistLegacyEvents([...historicalB, ...historicalA].reverse());
    await upgradeToCp3b();
    await assertCp3bColumns(true);

    expect((await sequences(walletA)).map((row) => row.id)).toEqual(historicalA.map((legacy) => legacy.id));
    expect((await sequences(walletB)).map((row) => row.id)).toEqual(historicalB.map((legacy) => legacy.id));

    const sequenceState = await connection.execute(
      `select last_value::text as last_value, is_called
         from outbox_messages_enqueue_sequence_seq`,
    ) as Array<{ last_value: string; is_called: boolean }>;
    expect(sequenceState).toEqual([{ last_value: '7', is_called: true }]);

    const processing = new MikroOrmFinancialProcessing(orm);
    const newBet = await processing.processWager({
      idempotencyKey: `post-upgrade-bet-${randomUUID()}`,
      providerId: 'cp3b-upgrade-provider',
      externalTransactionId: `post-upgrade-external-${randomUUID()}`,
      walletId: walletA,
      playerId: playerA,
      roundId: 'post-upgrade-round',
      gameId: 'cp3b-upgrade-game',
      kind: 'BET',
      money: { amount: '5.00', currency: 'BRL' },
    });
    expect(newBet.status).toBe(WagerTransactionStatus.Processed);
    const newBetEvents = await outboxEventsFor(newBet.transactionId);
    expect(newBetEvents.map((row) => row.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
    const walletAAfterNewBet = await sequences(walletA);
    expect(walletAAfterNewBet.map((row) => row.id)).toEqual([
      ...historicalA.map((legacy) => legacy.id),
      ...newBetEvents.map((row) => row.id),
    ]);
    expect(walletAAfterNewBet.slice(-2).map((row) => Number(row.enqueue_sequence))).toEqual([8, 9]);
    expect(new Set(walletAAfterNewBet.map((row) => row.enqueue_sequence)).size).toBe(walletAAfterNewBet.length);

    queueUrl = (await client.send(new CreateQueueCommand({
      QueueName: `cp3b-upgrade-${randomUUID()}.fifo`,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
    }))).QueueUrl!;
    const publisher = new SqsOutboxPublisher(client, orm, { queueUrl, leaseMs: 100, retryBaseMs: 20, retryMaxMs: 50 });
    const drained = await publisher.drain({ stopWhenIdle: true, batchSize: 10 });
    expect(drained.confirmed).toBe(historicalA.length + historicalB.length + newBetEvents.length);
    const delivered = (await receive(historicalA.length + historicalB.length + newBetEvents.length)).map((body) => JSON.parse(body) as { eventId: string; data: { walletId: string } });
    expect(delivered.filter((message) => message.data.walletId === walletA).map((message) => message.eventId)).toEqual([
      ...historicalA.map((legacy) => legacy.id),
      ...newBetEvents.map((row) => row.id),
    ]);
    expect(delivered.filter((message) => message.data.walletId === walletB).map((message) => message.eventId)).toEqual(historicalB.map((legacy) => legacy.id));

    await orm.getMigrator().down({ to: 'Migration20261029150000' });
    await assertCp3bColumns(false);
    await upgradeToCp3b();
    await assertCp3bColumns(true);
    expect((await sequences(walletA)).map((row) => row.id)).toEqual([
      ...historicalA.map((legacy) => legacy.id),
      ...newBetEvents.map((row) => row.id),
    ]);
    expect((await connection.execute(
      `select last_value::text as last_value, is_called
         from outbox_messages_enqueue_sequence_seq`,
    ) as Array<{ last_value: string; is_called: boolean }>)).toEqual([{ last_value: '9', is_called: true }]);
  });

  test('orders a resolved pending reference by persisted outbox chronology rather than original transaction creation', async () => {
    const connection = orm.em.getConnection();
    const walletId = randomUUID();
    const playerId = randomUUID();
    const refundId = randomUUID();
    const betId = randomUUID();
    const base = new Date('2026-10-29T13:00:00.000Z');
    const at = (seconds: number) => new Date(base.getTime() + seconds * 1_000);

    await connection.execute(
      `insert into wallets (id, player_id, currency, balance, version, created_at, updated_at)
       values (?, ?, 'BRL', '100.00', 3, ?, ?)`,
      [walletId, playerId, at(0), at(2)],
    );
    await persistLegacyPendingRefund(walletId, playerId, refundId, `external-${betId}`, at(0));
    await persistLegacyTransaction(walletId, playerId, betId, 'BET', '10.00', '90.00', at(1));
    await resolveLegacyRefund(walletId, refundId, betId, '100.00', at(2));

    const historical = [
      event(walletId, refundId, 'WagerTransactionPendingReference', at(0)),
      event(walletId, betId, 'WagerTransactionProcessed', at(1)),
      event(walletId, betId, 'WalletBalanceChanged', at(1)),
      event(walletId, refundId, 'WagerTransactionProcessed', at(2)),
      event(walletId, refundId, 'WalletBalanceChanged', at(2)),
    ];
    await persistLegacyEvents([...historical].reverse());

    await upgradeToCp3b();
    expect((await sequences(walletId)).map((row) => row.id)).toEqual(historical.map((legacy) => legacy.id));

    queueUrl = (await client.send(new CreateQueueCommand({
      QueueName: `cp3b-pending-reference-${randomUUID()}.fifo`,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
    }))).QueueUrl!;
    const publisher = new SqsOutboxPublisher(client, orm, { queueUrl, leaseMs: 100, retryBaseMs: 20, retryMaxMs: 50 });
    expect((await publisher.drain({ stopWhenIdle: true, batchSize: 10 })).confirmed).toBe(historical.length);
    const delivered = (await receive(historical.length)).map((body) => JSON.parse(body) as { eventId: string; eventType: string });
    expect(delivered.map((message) => message.eventId)).toEqual(historical.map((legacy) => legacy.id));
    expect(delivered.map((message) => message.eventType)).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
    ]);
  });

  test('rejects genuinely ambiguous legacy chronology instead of inventing an order', async () => {
    const connection = orm.em.getConnection();
    const walletId = randomUUID();
    const playerId = randomUUID();
    const firstTransactionId = randomUUID();
    const secondTransactionId = randomUUID();
    const occurredAt = new Date('2026-10-29T14:00:00.000Z');

    await connection.execute(
      `insert into wallets (id, player_id, currency, balance, version, created_at, updated_at)
       values (?, ?, 'BRL', '100.00', 1, ?, ?)`,
      [walletId, playerId, occurredAt, occurredAt],
    );
    await persistLegacyTransaction(walletId, playerId, firstTransactionId, 'BET', '10.00', '90.00', occurredAt);
    await persistLegacyTransaction(walletId, playerId, secondTransactionId, 'BET', '10.00', '80.00', occurredAt);
    await persistLegacyEvents([
      event(walletId, firstTransactionId, 'WagerTransactionProcessed', occurredAt),
      event(walletId, secondTransactionId, 'WagerTransactionProcessed', occurredAt),
    ]);

    await expect(upgradeToCp3b()).rejects.toThrow(
      'CP3B outbox backfill cannot reconstruct historical order for multiple wallet transactions sharing occurred_at',
    );
  });
});
