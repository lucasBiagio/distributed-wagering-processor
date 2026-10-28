import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { MikroOrmFinancialProcessing } from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

const captured: string[] = [];

let orm: MikroORM<PostgreSqlDriver>;
let processing: MikroOrmFinancialProcessing;
let sequence = 0;

function uuid(suffix: number): string {
  return `00000000-0000-4000-8000-${String(suffix).padStart(12, '0')}`;
}

/** Keeps only the SQL text, so no connection string or credential can reach the log. */
function sql(): string[] {
  return captured
    .map((line) => line.replace(/\u001b\[\d+m/g, '').replace(/^\[query]\s*/, '').trim())
    .filter((line) => line.length > 0);
}

async function openWallet(balance: string): Promise<{ walletId: string; playerId: string }> {
  sequence += 1;
  const playerId = uuid(sequence + 500_000);
  const wallet = await processing.openWallet({ playerId, initialBalance: { amount: balance, currency: 'BRL' } });
  return { walletId: wallet.id, playerId };
}

function bet(walletId: string, playerId: string, key: string, external: string, amount: string) {
  return processing.processBet({
    idempotencyKey: key,
    providerId: 'provider-lock',
    externalTransactionId: external,
    walletId,
    playerId,
    roundId: `round-${external}`,
    gameId: 'game-lock',
    kind: 'BET',
    money: { amount, currency: 'BRL' },
  });
}

describe('CP2B-1-R1 real concurrency SQL', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>({
      ...config,
      debug: ['query'],
      logger: (message: string) => {
        captured.push(message);
      },
    });
    await orm.getMigrator().up();
    processing = new MikroOrmFinancialProcessing(orm);
  });

  afterAll(async () => {
    await orm.getMigrator().down();
    await orm.close(true);
  });

  test('TEST-04: concurrent BETs serialize on a wallet scoped SELECT ... FOR UPDATE inside one transaction', async () => {
    const { walletId, playerId } = await openWallet('100.00');
    captured.length = 0;

    const results = await Promise.all([
      bet(walletId, playerId, 'lock-a', 'external-lock-a', '80.00'),
      bet(walletId, playerId, 'lock-b', 'external-lock-b', '80.00'),
    ]);

    const statements = sql();
    const lockStatements = statements.filter((line) => /for update/i.test(line));
    const transcript = statements.join('\n');

    // diagnostic evidence for the checkpoint report: the lock statement, no credentials involved
    console.log(`[TEST-04] lock statement: ${lockStatements[0] ?? '<none captured>'}`);

    expect(results.filter((result) => result.status === 'PROCESSED')).toHaveLength(1);
    expect(results.filter((result) => result.status === 'REJECTED')).toHaveLength(1);

    // a lock statement exists, targets only the wallets table and only the wallet involved
    expect(lockStatements.length).toBeGreaterThanOrEqual(2);
    for (const statement of lockStatements) {
      expect(statement).toMatch(/from\s+"wallets"/i);
      expect(statement).toMatch(/where/i);
      expect(statement).not.toMatch(/\bjoin\b/i);
      expect(statement).not.toMatch(/wager_transactions|wallet_ledger_entries|outbox_messages/i);
    }

    // the lock is taken inside an open transaction
    const firstBegin = statements.findIndex((line) => /^begin/i.test(line));
    const firstLock = statements.findIndex((line) => /for update/i.test(line));
    expect(firstBegin).toBeGreaterThanOrEqual(0);
    expect(firstLock).toBeGreaterThan(firstBegin);

    // balance, version, ledger and outbox are all written before the commit of that transaction
    const firstCommit = statements.findIndex((line) => /^commit/i.test(line));
    const beforeFirstCommit = statements.slice(0, firstCommit).join('\n');
    expect(firstCommit).toBeGreaterThan(firstLock);
    expect(beforeFirstCommit).toMatch(/update\s+"wallets"[\s\S]*"balance"/i);
    expect(beforeFirstCommit).toMatch(/update\s+"wallets"[\s\S]*"version"/i);
    expect(beforeFirstCommit).toMatch(/insert into\s+"wager_transactions"/i);
    expect(beforeFirstCommit).toMatch(/insert into\s+"wallet_ledger_entries"/i);
    expect(beforeFirstCommit).toMatch(/insert into\s+"outbox_messages"/i);

    // no publication happened: outbox rows stay unpublished with zero attempts
    expect(transcript).not.toMatch(/sqs|sendmessage/i);
    const unpublished = await orm.em.getConnection().execute(`
      select count(*)::int as count from outbox_messages where published_at is not null or attempts <> 0
    `) as Array<{ count: number }>;
    expect(unpublished[0]!.count).toBe(0);
  });

  test('TEST-04: a second request with the same key while the first has not committed cannot double debit', async () => {
    const { walletId, playerId } = await openWallet('100.00');

    const [first, second] = await Promise.all([
      bet(walletId, playerId, 'inflight-key', 'external-inflight', '25.00'),
      bet(walletId, playerId, 'inflight-key', 'external-inflight', '25.00'),
    ]);

    const rows = await orm.em.getConnection().execute(`
      select
        (select count(*)::int from wager_transactions where idempotency_key = 'inflight-key') as transactions,
        (select count(*)::int from wallet_ledger_entries where wallet_id = '${walletId}' and direction = 'DEBIT') as debits,
        (select balance::text from wallets where id = '${walletId}') as balance,
        (select version::text from wallets where id = '${walletId}') as version
    `) as Array<{ transactions: number; debits: number; balance: string; version: string }>;

    expect(first.transactionId).toBe(second.transactionId);
    expect([first.idempotentReplay, second.idempotentReplay].filter(Boolean)).toHaveLength(1);
    expect(rows[0]!.transactions).toBe(1);
    expect(rows[0]!.debits).toBe(1);
    expect(rows[0]!.balance).toBe('75.00');
    expect(rows[0]!.version).toBe('2');
  });
});
