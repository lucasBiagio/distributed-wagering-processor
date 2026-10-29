import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { EntityManager, MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import {
  MikroOrmFinancialProcessing,
  type UnitOfWorkStage,
} from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';
import { WagerTransactionStatus } from '../../src/domain/wager-transaction.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';
import { declareProcess, renderMetrics } from '../../src/infrastructure/observability/wagering-metrics.js';

interface Fault {
  readonly stage: UnitOfWorkStage;
  readonly error: Error;
  remaining: number;
}

/**
 * The only way to inject a failure is to subclass the adapter inside the test suite.
 * The production HTTP API has no header, body field or environment variable that can
 * reach this interception point.
 */
class FaultInjectingProcessing extends MikroOrmFinancialProcessing {
  public fault: Fault | null = null;
  public attempts = 0;

  protected override async interceptUnitOfWork(stage: UnitOfWorkStage): Promise<void> {
    if (stage === 'before-commit' || stage === 'wallet-and-transaction-flushed') {
      // counted once per stage reached; 'before-commit' is the last point of each attempt
      if (stage === 'before-commit') {
        this.attempts += 1;
      }
    }
    const fault = this.fault;
    if (fault && fault.stage === stage && fault.remaining > 0) {
      fault.remaining -= 1;
      throw fault.error;
    }
    return Promise.resolve();
  }
}

class PostgreSqlStateInjectingProcessing extends MikroOrmFinancialProcessing {
  public state: '40001' | '40P01' | null = null;
  public remaining = 0;
  public attempts = 0;

  protected override async interceptUnitOfWork(stage: UnitOfWorkStage, em: EntityManager): Promise<void> {
    if (stage !== 'before-commit') return;
    this.attempts += 1;
    const state = this.state;
    if (state === null || this.remaining === 0) return;
    this.remaining -= 1;
    // PostgreSQL itself emits the SQLSTATE inside the transaction; this is not a synthetic Error.
    await em.getConnection().execute(`do $$ begin raise sqlstate '${state}' using message = 'controlled CP4 lock conflict'; end $$;`);
  }
}

function sqlError(code: string, message: string): Error {
  return Object.assign(new Error(message), { code });
}

let orm: MikroORM<PostgreSqlDriver>;
let processing: FaultInjectingProcessing;
let sequence = 0;

function uuid(suffix: number): string {
  return `00000000-0000-4000-8000-${String(suffix).padStart(12, '0')}`;
}

async function openWallet(balance: string): Promise<{ walletId: string; playerId: string }> {
  sequence += 1;
  const playerId = uuid(sequence + 300_000);
  const wallet = await processing.openWallet({ playerId, initialBalance: { amount: balance, currency: 'BRL' } });
  return { walletId: wallet.id, playerId };
}

function bet(walletId: string, playerId: string, key: string, external: string, amount: string, adapter: MikroOrmFinancialProcessing = processing) {
  return adapter.processBet({
    idempotencyKey: key,
    providerId: 'provider-atomicity',
    externalTransactionId: external,
    walletId,
    playerId,
    roundId: `round-${external}`,
    gameId: 'game-atomicity',
    kind: 'BET',
    money: { amount, currency: 'BRL' },
  });
}

async function scalar(sql: string): Promise<string> {
  const rows = await orm.em.getConnection().execute(sql) as Array<{ value: string }>;
  return rows[0]!.value;
}

async function counter(sqlstate: string, resolution: 'retried' | 'exhausted'): Promise<number> {
  const escaped = `wagering_lock_conflicts_total{sqlstate="${sqlstate}",resolution="${resolution}"}`;
  const line = (await renderMetrics()).split('\n').find((entry) => entry.startsWith(escaped));
  return line === undefined ? 0 : Number(line.slice(line.lastIndexOf(' ') + 1));
}

interface PersistedState {
  readonly balance: string;
  readonly version: string;
  readonly transactions: string;
  readonly debitEntries: string;
  readonly ledgerEntries: string;
  readonly outboxEvents: string;
  readonly ledgerBalance: string;
}

async function snapshot(walletId: string): Promise<PersistedState> {
  return {
    balance: await scalar(`select balance::text as value from wallets where id = '${walletId}'`),
    version: await scalar(`select version::text as value from wallets where id = '${walletId}'`),
    transactions: await scalar(`select count(*)::text as value from wager_transactions where wallet_id = '${walletId}' and kind = 'BET'`),
    debitEntries: await scalar(`select count(*)::text as value from wallet_ledger_entries where wallet_id = '${walletId}' and direction = 'DEBIT'`),
    ledgerEntries: await scalar(`select count(*)::text as value from wallet_ledger_entries where wallet_id = '${walletId}'`),
    outboxEvents: await scalar(`select count(*)::text as value from outbox_messages where aggregate_id = '${walletId}'`),
    ledgerBalance: await scalar(`
      select coalesce(sum(case direction when 'CREDIT' then amount else -amount end), 0)::text as value
      from wallet_ledger_entries where wallet_id = '${walletId}'
    `),
  };
}

describe('CP2B-1-R1 financial atomicity and transient failure handling', () => {
  beforeAll(async () => {
    declareProcess('test-harness');
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
    processing = new FaultInjectingProcessing(orm);
  });

  afterAll(async () => {
    await orm.getMigrator().down({ to: 0 });
    await orm.close(true);
  });

  beforeEach(() => {
    processing.fault = null;
    processing.attempts = 0;
  });

  test('TEST-01: failure injected after wallet and transaction are flushed rolls back every financial write', async () => {
    const { walletId, playerId } = await openWallet('100.00');
    const before = await snapshot(walletId);

    processing.fault = {
      stage: 'wallet-and-transaction-flushed',
      error: new Error('injected failure before ledger and outbox'),
      remaining: 1,
    };

    await expect(bet(walletId, playerId, 'atomic-early', 'external-atomic-early', '25.00'))
      .rejects.toThrow('injected failure before ledger and outbox');

    const after = await snapshot(walletId);
    expect(after).toEqual(before);
    expect(after.balance).toBe('100.00');
    expect(after.version).toBe('1');
    expect(after.transactions).toBe('0');
    expect(after.ledgerBalance).toBe('100.00');

    processing.fault = null;
    const retry = await bet(walletId, playerId, 'atomic-early', 'external-atomic-early', '25.00');
    const recovered = await snapshot(walletId);

    expect(retry.status).toBe(WagerTransactionStatus.Processed);
    expect(retry.idempotentReplay).toBe(false);
    expect(retry.balance).toEqual({ amount: '75.00', currency: 'BRL' });
    expect(recovered.balance).toBe('75.00');
    expect(recovered.version).toBe('2');
    expect(recovered.transactions).toBe('1');
    expect(recovered.debitEntries).toBe('1');
    expect(recovered.ledgerBalance).toBe('75.00');
  });

  test('TEST-01: failure injected after ledger and outbox but before commit leaves no partial state', async () => {
    const { walletId, playerId } = await openWallet('100.00');
    const before = await snapshot(walletId);

    processing.fault = {
      stage: 'before-commit',
      error: new Error('injected failure before commit'),
      remaining: 1,
    };

    await expect(bet(walletId, playerId, 'atomic-late', 'external-atomic-late', '40.00'))
      .rejects.toThrow('injected failure before commit');

    const after = await snapshot(walletId);
    expect(after).toEqual(before);
    expect(after.transactions).toBe('0');
    expect(after.debitEntries).toBe('0');
    expect(after.outboxEvents).toBe(before.outboxEvents);
    expect(after.ledgerBalance).toBe(after.balance);

    const orphanTransaction = await scalar(
      `select count(*)::text as value from wager_transactions where idempotency_key = 'atomic-late'`,
    );
    expect(orphanTransaction).toBe('0');

    processing.fault = null;
    const retry = await bet(walletId, playerId, 'atomic-late', 'external-atomic-late', '40.00');
    const recovered = await snapshot(walletId);

    expect(retry.status).toBe(WagerTransactionStatus.Processed);
    expect(recovered.balance).toBe('60.00');
    expect(recovered.version).toBe('2');
    expect(recovered.debitEntries).toBe('1');
    expect(recovered.ledgerBalance).toBe('60.00');
  });

  test('TEST-05: a single transient deadlock is retried inside the bounded limit and debits exactly once', async () => {
    const { walletId, playerId } = await openWallet('100.00');

    processing.fault = { stage: 'before-commit', error: sqlError('40P01', 'deadlock detected'), remaining: 1 };
    const result = await bet(walletId, playerId, 'transient-deadlock', 'external-transient-deadlock', '30.00');
    const after = await snapshot(walletId);

    expect(processing.attempts).toBe(2);
    expect(result.status).toBe(WagerTransactionStatus.Processed);
    expect(result.balance).toEqual({ amount: '70.00', currency: 'BRL' });
    expect(after.balance).toBe('70.00');
    expect(after.version).toBe('2');
    expect(after.transactions).toBe('1');
    expect(after.debitEntries).toBe('1');
    expect(after.ledgerBalance).toBe('70.00');
  });

  test('TEST-05: a persistent serialization failure stays an infrastructure error and is never a financial REJECTED', async () => {
    const { walletId, playerId } = await openWallet('100.00');
    const before = await snapshot(walletId);

    processing.fault = {
      stage: 'before-commit',
      error: sqlError('40001', 'could not serialize access due to concurrent update'),
      remaining: 10,
    };

    const failure = await bet(walletId, playerId, 'transient-exhausted', 'external-transient-exhausted', '30.00')
      .then(() => null, (error: unknown) => error as Error & { code?: string });

    expect(failure?.code).toBe('40001');
    expect(processing.attempts).toBe(3);

    const after = await snapshot(walletId);
    expect(after).toEqual(before);
    const rejected = await scalar(
      `select count(*)::text as value from wager_transactions where wallet_id = '${walletId}' and status = 'REJECTED'`,
    );
    expect(rejected).toBe('0');
  });

  test('M5: PostgreSQL SQLSTATE conflicts increment same-process counters and never duplicate financial effects', async () => {
    const conflicts = new PostgreSqlStateInjectingProcessing(orm);
    const playerId = uuid(sequence + 600_000);
    sequence += 1;
    const wallet = await conflicts.openWallet({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } });

    const retriedBefore = await counter('40P01', 'retried');
    conflicts.state = '40P01';
    conflicts.remaining = 1;
    const retried = await bet(wallet.id, playerId, 'm5-retried', 'm5-retried', '30.00', conflicts);
    const afterRetry = await snapshot(wallet.id);

    expect(retried.status).toBe(WagerTransactionStatus.Processed);
    expect(conflicts.attempts).toBe(2);
    expect(await counter('40P01', 'retried')).toBe(retriedBefore + 1);
    expect(afterRetry).toMatchObject({ balance: '70.00', transactions: '1', debitEntries: '1', ledgerBalance: '70.00' });

    const exhaustedBefore = await counter('40001', 'exhausted');
    const beforeExhaustion = await snapshot(wallet.id);
    conflicts.attempts = 0;
    conflicts.state = '40001';
    conflicts.remaining = 10;
    const exhausted = await bet(wallet.id, playerId, 'm5-exhausted', 'm5-exhausted', '20.00', conflicts)
      .then(() => null, (error: unknown) => error as Error & { code?: string });
    const afterExhaustion = await snapshot(wallet.id);

    expect(exhausted?.code).toBe('40001');
    expect(conflicts.attempts).toBe(3);
    expect(await counter('40001', 'exhausted')).toBe(exhaustedBefore + 1);
    expect(afterExhaustion).toEqual(beforeExhaustion);
  });

  test('TEST-05: non transient failures are not retried and definitive conflicts stay distinguishable', async () => {
    const { walletId, playerId } = await openWallet('100.00');

    processing.fault = { stage: 'before-commit', error: sqlError('23514', 'check violation'), remaining: 10 };
    await expect(bet(walletId, playerId, 'definitive-failure', 'external-definitive', '30.00')).rejects.toThrow('check violation');
    expect(processing.attempts).toBe(1);

    processing.fault = null;
    await bet(walletId, playerId, 'conflict-base', 'external-conflict-base', '10.00');

    const conflict = await bet(walletId, playerId, 'conflict-base', 'external-conflict-base', '11.00')
      .then(() => null, (error: unknown) => error as Error);
    expect(conflict?.name).toBe('FinancialConflictError');
    expect((conflict as Error & { code?: string }).code).toBeUndefined();

    const after = await snapshot(walletId);
    expect(after.balance).toBe('90.00');
    expect(after.debitEntries).toBe('1');
    expect(after.ledgerBalance).toBe('90.00');
  });
});
