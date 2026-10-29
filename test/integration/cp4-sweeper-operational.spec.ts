import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { FailureCode, WagerTransactionStatus } from '../../src/domain/wager-transaction.js';
import { SqsOutboxPublisher } from '../../src/infrastructure/messaging/sqs-outbox-publisher.js';
import ormConfig from '../../src/infrastructure/persistence/mikro-orm.config.js';
import {
  MAX_REFERENCE_ATTEMPTS,
  MikroOrmFinancialProcessing,
  REFERENCE_BACKOFF_BASE_MS,
  REFERENCE_BACKOFF_MAX_MS,
  REFERENCE_TTL_MS,
} from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';

/**
 * CP4 Gate 2 — operational validation of the pending-reference sweeper.
 *
 * Everything here runs against the real PostgreSQL schema and, for the publication case, a
 * real LocalStack FIFO queue. Concurrency, restart and shutdown are exercised with truly
 * separate operating-system processes running `src/pending-reference-sweeper.ts`, never with
 * mocks or sequential in-process calls, because the property under test (several instances
 * claiming the same due row) only exists across independent database sessions.
 *
 * Elapsed time is simulated by moving `reference_next_attempt_at` in the disposable database
 * instead of sleeping through the real backoff: the retry policy itself is never modified,
 * and the column stays non-null while the row is PENDING_REFERENCE, so every existing check
 * constraint remains in force.
 */
const workspaceRoot = resolve(import.meta.dir, '../..');
const SWEEP_INTERVAL_MS = 250;

let orm: MikroORM<PostgreSqlDriver>;
let processing: MikroOrmFinancialProcessing;
let sqs: SQSClient;
let sequence = 0;

interface Wallet {
  readonly id: string;
  readonly playerId: string;
}

interface PendingRow {
  readonly status: string;
  readonly failure_code: string | null;
  readonly reference_attempts: number;
  readonly reference_next_attempt_at: string | null;
  readonly reference_transaction_id: string | null;
  readonly resulting_balance: string | null;
}

interface SweeperLogLine {
  readonly event: string;
  readonly pid: number;
  readonly processed?: number;
  readonly claimed?: number;
  readonly signal?: string;
}

interface RunnerResult {
  readonly exitCode: number | null;
  readonly lines: SweeperLogLine[];
  readonly stderr: string;
}

function nextSuffix(label: string): string {
  sequence += 1;
  return `${label}-${sequence}-${randomUUID().slice(0, 8)}`;
}

async function wallet(balance = '100.00'): Promise<Wallet> {
  sequence += 1;
  const view = await processing.openWallet({
    playerId: `20000000-0000-4000-8000-${String(sequence).padStart(12, '0')}`,
    initialBalance: { amount: balance, currency: 'BRL' },
  });
  return { id: view.id, playerId: view.playerId };
}

async function pendingRefund(target: Wallet, suffix: string, amount: string, reference: string): Promise<string> {
  const refund = await processing.processWager({
    idempotencyKey: `refund-${suffix}`,
    providerId: 'cp4-sweeper',
    externalTransactionId: `ext-refund-${suffix}`,
    walletId: target.id,
    playerId: target.playerId,
    roundId: `round-${suffix}`,
    gameId: 'fortune-chimp',
    kind: 'REFUND',
    money: { amount, currency: 'BRL' },
    referenceExternalTransactionId: reference,
  });
  expect(refund.status).toBe(WagerTransactionStatus.PendingReference);
  return refund.transactionId;
}

async function processedBet(target: Wallet, suffix: string, amount: string, external: string): Promise<string> {
  const bet = await processing.processWager({
    idempotencyKey: `bet-${suffix}`,
    providerId: 'cp4-sweeper',
    externalTransactionId: external,
    walletId: target.id,
    playerId: target.playerId,
    roundId: `round-${suffix}`,
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: { amount, currency: 'BRL' },
  });
  expect(bet.status).toBe(WagerTransactionStatus.Processed);
  return bet.transactionId;
}

async function pendingRow(id: string): Promise<PendingRow> {
  const rows = await orm.em.getConnection().execute(
    `select status, failure_code, reference_attempts, reference_next_attempt_at,
            reference_transaction_id::text as reference_transaction_id,
            resulting_balance::text as resulting_balance
       from wager_transactions where id = ?`,
    [id],
  ) as PendingRow[];
  return rows[0]!;
}

async function walletRow(id: string): Promise<{ balance: string; version: number }> {
  const rows = await orm.em.getConnection().execute(
    'select balance::text as balance, version from wallets where id = ?',
    [id],
  ) as Array<{ balance: string; version: number }>;
  return rows[0]!;
}

async function ledgerCount(transactionId: string): Promise<number> {
  const rows = await orm.em.getConnection().execute(
    'select count(*)::int as total from wallet_ledger_entries where transaction_id = ?',
    [transactionId],
  ) as Array<{ total: number }>;
  return rows[0]!.total;
}

async function eventsFor(aggregateId: string): Promise<string[]> {
  const rows = await orm.em.getConnection().execute(
    'select event_type from outbox_messages where aggregate_id = ? order by enqueue_sequence asc',
    [aggregateId],
  ) as Array<{ event_type: string }>;
  return rows.map((row) => row.event_type);
}

/** Simulated elapsed backoff: the row becomes claimable now, policy untouched. */
async function makeDue(transactionId: string, offsetSeconds = 0): Promise<void> {
  await orm.em.getConnection().execute(
    `update wager_transactions
        set reference_next_attempt_at = now() + (? * interval '1 second')
      where id = ? and status = 'PENDING_REFERENCE'`,
    [offsetSeconds, transactionId],
  );
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await Bun.sleep(50);
  }
  throw new Error('timed out waiting for the sweeper to reach the expected state');
}

async function waitForStatus(transactionId: string, status: string, timeoutMs = 20_000): Promise<void> {
  await waitFor(async () => (await pendingRow(transactionId)).status === status, timeoutMs);
}

type Runner = ReturnType<typeof Bun.spawn>;

/** A real, independent operating-system process running the production runner entrypoint. */
function startRunner(intervalMs = SWEEP_INTERVAL_MS): Runner {
  return Bun.spawn(['bun', 'run', 'src/pending-reference-sweeper.ts'], {
    cwd: workspaceRoot,
    env: {
      ...Bun.env,
      PENDING_REFERENCE_SWEEP_INTERVAL_MS: String(intervalMs),
      PENDING_REFERENCE_SWEEP_BATCH_SIZE: '25',
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });
}

async function collect(runner: Runner): Promise<RunnerResult> {
  const [stdout, stderr] = await Promise.all([
    new Response(runner.stdout as ReadableStream).text(),
    new Response(runner.stderr as ReadableStream).text(),
  ]);
  await runner.exited;
  const lines = stdout
    .split('\n')
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as SweeperLogLine);
  return { exitCode: runner.exitCode, lines, stderr };
}

async function stopRunner(runner: Runner, signal: NodeJS.Signals = 'SIGTERM'): Promise<RunnerResult> {
  runner.kill(signal);
  return await collect(runner);
}

/**
 * Liveness probe for a freshly spawned runner: a pending whose reference already exists and
 * which is already due can only become PROCESSED once a real sweep ran, so this waits for
 * evidence of operation instead of guessing a boot delay.
 */
async function waitUntilSweeping(label: string): Promise<void> {
  const suffix = nextSuffix(label);
  const target = await wallet('50.00');
  const sentinel = await pendingRefund(target, suffix, '5.00', `ext-bet-${suffix}`);
  await processedBet(target, suffix, '5.00', `ext-bet-${suffix}`);
  await makeDue(sentinel);
  await waitForStatus(sentinel, 'PROCESSED', 40_000);
}

describe('CP4 Gate 2 — pending-reference sweeper operation', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(ormConfig);
    await orm.getMigrator().up();
    processing = new MikroOrmFinancialProcessing(orm);
    sqs = new SQSClient({
      region: Bun.env.AWS_REGION ?? 'us-east-1',
      endpoint: Bun.env.SQS_ENDPOINT ?? 'http://localstack:4566',
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    });
  }, 60_000);

  beforeEach(async () => {
    await orm.em.getConnection().execute(
      'truncate table outbox_messages, wallet_ledger_entries, wager_transactions, wallets cascade',
    );
  });

  afterAll(async () => {
    sqs.destroy();
    await orm.close(true);
  });

  test('TEST-S1: an out-of-order reference is only credited by the runner process after the BET exists', async () => {
    const suffix = nextSuffix('s1');
    const target = await wallet('100.00');
    const refundId = await pendingRefund(target, suffix, '25.00', `ext-bet-${suffix}`);

    const runner = startRunner();
    try {
      // the reference does not exist yet: the sweep must consume an attempt and credit nothing
      await makeDue(refundId);
      await waitFor(async () => (await pendingRow(refundId)).reference_attempts >= 1, 15_000);

      const parked = await pendingRow(refundId);
      expect(parked.status).toBe('PENDING_REFERENCE');
      expect(parked.reference_transaction_id).toBeNull();
      expect(parked.resulting_balance).toBeNull();
      expect(await ledgerCount(refundId)).toBe(0);
      expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 1 });

      // the BET finally arrives, out of order
      const betId = await processedBet(target, suffix, '25.00', `ext-bet-${suffix}`);
      expect(await walletRow(target.id)).toEqual({ balance: '75.00', version: 2 });

      await makeDue(refundId);
      await waitForStatus(refundId, 'PROCESSED');

      const resolved = await pendingRow(refundId);
      expect(resolved.reference_transaction_id).toBe(betId);
      expect(resolved.resulting_balance).toBe('100.00');
      expect(resolved.reference_next_attempt_at).toBeNull();
      expect(resolved.failure_code).toBeNull();

      expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });
      expect(await ledgerCount(refundId)).toBe(1);
      expect(await eventsFor(refundId)).toEqual([
        'WagerTransactionPendingReference',
        'WagerTransactionProcessed',
      ]);
      expect(await eventsFor(target.id)).toContain('WalletBalanceChanged');
    } finally {
      const result = await stopRunner(runner);
      expect(result.exitCode).toBe(0);
      expect(result.lines[0]?.event).toBe('pending_reference_sweeper_started');
      expect(result.lines.at(-1)?.event).toBe('pending_reference_sweeper_stopped');
    }
  }, 60_000);

  test('TEST-S2: two independent runner processes disputing the same pending produce exactly one financial effect', async () => {
    const suffix = nextSuffix('s2');
    const target = await wallet('100.00');
    const refundId = await pendingRefund(target, suffix, '40.00', `ext-bet-${suffix}`);
    const betId = await processedBet(target, suffix, '40.00', `ext-bet-${suffix}`);
    expect(await walletRow(target.id)).toEqual({ balance: '60.00', version: 2 });

    const first = startRunner(150);
    const second = startRunner(150);
    let results: RunnerResult[] = [];
    try {
      await waitUntilSweeping('s2-sentinel');
      // both were spawned together, so a short extra margin covers the boot skew
      await Bun.sleep(SWEEP_INTERVAL_MS * 8);
      await makeDue(refundId);
      await waitForStatus(refundId, 'PROCESSED');
    } finally {
      results = [await stopRunner(first), await stopRunner(second)];
    }

    // both were real, separate processes that really swept
    const pids = results.map((result) => result.lines[0]?.pid);
    expect(new Set(pids).size).toBe(2);
    for (const result of results) {
      expect(result.exitCode).toBe(0);
      expect(result.stderr).toBe('');
      expect(result.lines.some((line) => line.event === 'pending_reference_sweep_completed')).toBe(true);
    }

    // and exactly one resolution per resolvable pending: the liveness sentinel plus the
    // disputed refund, never a second claim of either one
    const processedByRunners = results
      .flatMap((result) => result.lines)
      .filter((line) => line.event === 'pending_reference_sweep_completed')
      .reduce((total, line) => total + (line.processed ?? 0), 0);
    expect(processedByRunners).toBe(2);

    const resolved = await pendingRow(refundId);
    expect(resolved.status).toBe('PROCESSED');
    expect(resolved.reference_transaction_id).toBe(betId);
    expect(resolved.resulting_balance).toBe('100.00');
    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });
    expect(await ledgerCount(refundId)).toBe(1);
    expect(await eventsFor(refundId)).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionProcessed',
    ]);
    const walletEvents = await eventsFor(target.id);
    expect(walletEvents.filter((event) => event === 'WalletBalanceChanged')).toHaveLength(3);
  }, 60_000);

  test('TEST-S3: a pending left behind by a killed process is resumed by a new process on the same database', async () => {
    const suffix = nextSuffix('s3');
    const target = await wallet('100.00');
    const refundId = await pendingRefund(target, suffix, '30.00', `ext-bet-${suffix}`);
    await processedBet(target, suffix, '30.00', `ext-bet-${suffix}`);

    // not claimable while the first process is alive
    await makeDue(refundId, 3_600);
    const killed = startRunner();
    await waitUntilSweeping('s3-sentinel');
    killed.kill('SIGKILL');
    await killed.exited;
    expect((await pendingRow(refundId)).status).toBe('PENDING_REFERENCE');

    const resumed = startRunner();
    try {
      await makeDue(refundId);
      await waitForStatus(refundId, 'PROCESSED');
    } finally {
      const result = await stopRunner(resumed);
      expect(result.exitCode).toBe(0);
      const processedAfterRestart = result.lines
        .filter((line) => line.event === 'pending_reference_sweep_completed')
        .reduce((total, line) => total + (line.processed ?? 0), 0);
      expect(processedAfterRestart).toBe(1);
    }

    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });
    expect(await ledgerCount(refundId)).toBe(1);
    expect(await eventsFor(refundId)).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionProcessed',
    ]);
  }, 60_000);

  test('TEST-S4: the existing retry budget, backoff and TTL expire a missing reference as REJECTED without any ledger effect', async () => {
    const budgetSuffix = nextSuffix('s4-budget');
    const budgetWallet = await wallet('100.00');
    const budgetRefund = await pendingRefund(budgetWallet, budgetSuffix, '25.00', `ext-never-${budgetSuffix}`);

    const delays: number[] = [];
    let clock = new Date((await pendingRow(budgetRefund)).reference_next_attempt_at!);

    for (let attempt = 1; attempt <= MAX_REFERENCE_ATTEMPTS; attempt += 1) {
      const sweep = await processing.reprocessPendingReferences({ now: clock });
      expect(sweep.claimed).toBe(1);
      const row = await pendingRow(budgetRefund);
      expect(row.reference_attempts).toBe(attempt);

      if (attempt < MAX_REFERENCE_ATTEMPTS) {
        expect(sweep).toMatchObject({ processed: 0, rejected: 0, rescheduled: 1 });
        expect(row.status).toBe('PENDING_REFERENCE');
        const next = new Date(row.reference_next_attempt_at!);
        delays.push(next.getTime() - clock.getTime());
        clock = next;
      } else {
        expect(sweep).toMatchObject({ processed: 0, rejected: 1, rescheduled: 0 });
        expect(row.status).toBe('REJECTED');
        expect(row.failure_code).toBe(FailureCode.ReferenceNotFound);
        expect(row.reference_next_attempt_at).toBeNull();
      }
    }

    expect(delays[0]).toBe(Math.min(REFERENCE_BACKOFF_BASE_MS * 2, REFERENCE_BACKOFF_MAX_MS));
    for (let index = 1; index < delays.length; index += 1) {
      expect(delays[index]).toBe(Math.min(delays[index - 1]! * 2, REFERENCE_BACKOFF_MAX_MS));
    }

    expect(await walletRow(budgetWallet.id)).toEqual({ balance: '100.00', version: 1 });
    expect(await ledgerCount(budgetRefund)).toBe(0);
    expect(await eventsFor(budgetRefund)).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionRejected',
    ]);

    // the TTL closes the transaction even before the attempt budget is consumed
    const ttlSuffix = nextSuffix('s4-ttl');
    const ttlWallet = await wallet('100.00');
    const ttlRefund = await pendingRefund(ttlWallet, ttlSuffix, '25.00', `ext-never-${ttlSuffix}`);
    const scheduled = new Date((await pendingRow(ttlRefund)).reference_next_attempt_at!);

    const expiry = await processing.reprocessPendingReferences({
      now: new Date(scheduled.getTime() + REFERENCE_TTL_MS),
    });
    expect(expiry).toMatchObject({ claimed: 1, processed: 0, rejected: 1, rescheduled: 0 });

    const expired = await pendingRow(ttlRefund);
    expect(expired.status).toBe('REJECTED');
    expect(expired.failure_code).toBe(FailureCode.ReferenceNotFound);
    expect(expired.reference_attempts).toBe(1);
    expect(expired.reference_next_attempt_at).toBeNull();
    expect(await walletRow(ttlWallet.id)).toEqual({ balance: '100.00', version: 1 });
    expect(await ledgerCount(ttlRefund)).toBe(0);
    expect(await eventsFor(ttlRefund)).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionRejected',
    ]);
  }, 60_000);

  test('TEST-S5: events written by the sweeper resolution are published once to a real FIFO queue with stable eventIds', async () => {
    const suffix = nextSuffix('s5');
    const target = await wallet('100.00');
    const refundId = await pendingRefund(target, suffix, '20.00', `ext-bet-${suffix}`);
    await processedBet(target, suffix, '20.00', `ext-bet-${suffix}`);

    const runner = startRunner();
    try {
      await makeDue(refundId);
      await waitForStatus(refundId, 'PROCESSED');
    } finally {
      expect((await stopRunner(runner)).exitCode).toBe(0);
    }

    const queueUrl = (await sqs.send(new CreateQueueCommand({
      QueueName: `${nextSuffix('cp4-sweeper-events')}.fifo`,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false', VisibilityTimeout: '1' },
    }))).QueueUrl!;

    try {
      const outbox = await orm.em.getConnection().execute(
        `select id::text as id, event_type from outbox_messages
          where aggregate_id = ? order by enqueue_sequence asc`,
        [refundId],
      ) as Array<{ id: string; event_type: string }>;
      const resolutionEventId = outbox.find((row) => row.event_type === 'WagerTransactionProcessed')!.id;

      const publisher = new SqsOutboxPublisher(sqs, orm, {
        queueUrl,
        leaseMs: 150,
        retryBaseMs: 20,
        retryMaxMs: 50,
      });
      const drained = await publisher.drain({ stopWhenIdle: true });
      expect(drained.failed).toBe(0);
      expect(drained.confirmed).toBeGreaterThanOrEqual(2);

      const delivered: Array<{ eventId: string; eventType: string; groupId?: string; deduplicationId?: string }> = [];
      await waitFor(async () => {
        const response = await sqs.send(new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
          AttributeNames: ['All'],
        }));
        for (const message of response.Messages ?? []) {
          const body = JSON.parse(message.Body!) as { eventId: string; eventType: string };
          delivered.push({
            eventId: body.eventId,
            eventType: body.eventType,
            ...(message.Attributes?.MessageGroupId === undefined ? {} : { groupId: message.Attributes.MessageGroupId }),
            ...(message.Attributes?.MessageDeduplicationId === undefined
              ? {}
              : { deduplicationId: message.Attributes.MessageDeduplicationId }),
          });
          await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle! }));
        }
        return delivered.length >= drained.confirmed;
      });

      const resolution = delivered.filter((message) => message.eventId === resolutionEventId);
      expect(resolution).toHaveLength(1);
      expect(resolution[0]!.eventType).toBe('WagerTransactionProcessed');
      expect(resolution[0]!.deduplicationId).toBe(resolutionEventId);
      expect(resolution[0]!.groupId).toBe(target.id);
      expect(new Set(delivered.map((message) => message.eventId)).size).toBe(delivered.length);

      // a reprocessing of the publisher produces no duplicate effect
      const replay = await publisher.drain({ stopWhenIdle: true });
      expect(replay).toMatchObject({ claimed: 0, confirmed: 0, failed: 0 });

      const unpublished = await orm.em.getConnection().execute(
        'select count(*)::int as total from outbox_messages where published_at is null',
      ) as Array<{ total: number }>;
      expect(unpublished[0]!.total).toBe(0);
      expect(await ledgerCount(refundId)).toBe(1);
      expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });
    } finally {
      await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    }
  }, 90_000);

  test('TEST-S6: SIGTERM stops new sweeps without partial effects and the runner restarts safely', async () => {
    const suffix = nextSuffix('s6');
    const target = await wallet('100.00');
    const refundId = await pendingRefund(target, suffix, '15.00', `ext-bet-${suffix}`);
    await processedBet(target, suffix, '15.00', `ext-bet-${suffix}`);
    // the observed pending must stay unclaimable for the whole shutdown window
    await makeDue(refundId, 3_600);

    // a resolvable sentinel proves the runner is really sweeping before SIGTERM is sent,
    // instead of guessing a boot delay
    const runnerStartedAt = Date.now();
    const runner = startRunner();
    await waitUntilSweeping('s6-sentinel');
    const result = await stopRunner(runner);
    const aliveMs = Date.now() - runnerStartedAt;

    expect(result.exitCode).toBe(0);
    expect(result.stderr).toBe('');

    const events = result.lines.map((line) => line.event);
    expect(events[0]).toBe('pending_reference_sweeper_started');
    expect(events.at(-2)).toBe('pending_reference_sweeper_stopping');
    expect(events.at(-1)).toBe('pending_reference_sweeper_stopped');
    expect(result.lines.at(-2)?.signal).toBe('SIGTERM');

    // nothing is swept after the stop decision: the connection pool closes last
    const stoppingIndex = events.indexOf('pending_reference_sweeper_stopping');
    expect(events.slice(stoppingIndex).filter((event) => event === 'pending_reference_sweep_completed')).toHaveLength(0);

    // one sweep at a time inside a process: never more cycles than the interval allows
    const sweeps = events.filter((event) => event === 'pending_reference_sweep_completed').length;
    expect(sweeps).toBeGreaterThan(0);
    expect(sweeps).toBeLessThanOrEqual(Math.ceil(aliveMs / SWEEP_INTERVAL_MS) + 1);

    // the shutdown left the pending untouched, never half-applied
    const parked = await pendingRow(refundId);
    expect(parked.status).toBe('PENDING_REFERENCE');
    expect(parked.resulting_balance).toBeNull();
    expect(parked.reference_transaction_id).toBeNull();
    expect(await ledgerCount(refundId)).toBe(0);
    expect(await walletRow(target.id)).toEqual({ balance: '85.00', version: 2 });

    const restarted = startRunner();
    try {
      await makeDue(refundId);
      await waitForStatus(refundId, 'PROCESSED');
    } finally {
      expect((await stopRunner(restarted)).exitCode).toBe(0);
    }

    expect(await walletRow(target.id)).toEqual({ balance: '100.00', version: 3 });
    expect(await ledgerCount(refundId)).toBe(1);
  }, 60_000);
});
