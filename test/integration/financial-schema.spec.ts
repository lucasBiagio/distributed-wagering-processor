import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { WalletRecord } from '../../src/infrastructure/persistence/financial-entities.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

let orm: MikroORM<PostgreSqlDriver>;

function sqlState(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) {
    return undefined;
  }

  const candidate = error as {
    code?: unknown;
    constraint?: unknown;
    cause?: { code?: unknown; constraint?: unknown };
  };
  if (typeof candidate.code === 'string') {
    return candidate.code;
  }

  return typeof candidate.cause?.code === 'string' ? candidate.cause.code : undefined;
}

function sqlConstraint(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) {
    return undefined;
  }

  const candidate = error as { constraint?: unknown; cause?: { constraint?: unknown } };
  if (typeof candidate.constraint === 'string') {
    return candidate.constraint;
  }

  return typeof candidate.cause?.constraint === 'string' ? candidate.cause.constraint : undefined;
}

async function expectSqlState(
  statement: string,
  expectedState: string,
  expectedConstraint?: string,
): Promise<void> {
  let failure: unknown;
  try {
    await orm.em.getConnection().execute(statement);
  } catch (error) {
    failure = error;
  }

  expect(failure).toBeDefined();
  expect(sqlState(failure)).toBe(expectedState);
  if (expectedConstraint) {
    expect(sqlConstraint(failure)).toBe(expectedConstraint);
  }
}

async function insertWalletAndTransaction(
  seed: string,
  status: 'PENDING' | 'PENDING_REFERENCE' | 'PROCESSED',
): Promise<{ walletId: string; transactionId: string }> {
  const walletId = `00000000-0000-0000-0000-000000000${seed}`;
  const playerId = `00000000-0000-0000-0000-000000000${Number(seed) + 10}`;
  const transactionId = `00000000-0000-0000-0000-000000000${Number(seed) + 20}`;
  const processedAt = status === 'PROCESSED' ? 'now()' : 'null';
  const connection = orm.em.getConnection();

  await connection.execute(`
    insert into wallets (id, player_id, currency, balance, version, created_at, updated_at)
    values ('${walletId}', '${playerId}', 'BRL', 100.00, 1, now(), now());

    insert into wager_transactions (
      id, provider_id, external_transaction_id, idempotency_key, payload_hash,
      wallet_id, player_id, round_id, game_id, kind, amount, currency,
      status, processed_at, created_at
    ) values (
      '${transactionId}', 'provider-${seed}', 'external-bet-${seed}', 'provider-${seed}:external-bet-${seed}', 'payload-hash-${seed}',
      '${walletId}', '${playerId}', 'round-${seed}', 'game-${seed}', 'BET', 10.00, 'BRL',
      '${status}', ${processedAt}, now()
    );
  `);

  return { walletId, transactionId };
}

describe('financial schema migration', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
  });

  afterAll(async () => {
    await orm.getMigrator().down();

    const result = await orm.em.getConnection().execute(
      "select to_regclass('public.wallets') as wallets, to_regclass('public.wager_transactions') as wager_transactions, to_regclass('public.wallet_ledger_entries') as wallet_ledger_entries, to_regclass('public.inbox_messages') as inbox_messages, to_regclass('public.outbox_messages') as outbox_messages",
    ) as Array<Record<string, string | null>>;

    expect(result[0]).toEqual({
      wallets: null,
      wager_transactions: null,
      wallet_ledger_entries: null,
      inbox_messages: null,
      outbox_messages: null,
    });

    await orm.close(true);
  });

  test('creates all CP2A financial tables', async () => {
    const result = await orm.em.getConnection().execute(
      "select to_regclass('public.wallets') as wallets, to_regclass('public.wager_transactions') as wager_transactions, to_regclass('public.wallet_ledger_entries') as wallet_ledger_entries, to_regclass('public.inbox_messages') as inbox_messages, to_regclass('public.outbox_messages') as outbox_messages",
    ) as Array<Record<string, string | null>>;

    expect(result[0]).toEqual({
      wallets: 'wallets',
      wager_transactions: 'wager_transactions',
      wallet_ledger_entries: 'wallet_ledger_entries',
      inbox_messages: 'inbox_messages',
      outbox_messages: 'outbox_messages',
    });
  });

  test('supports a reversible migration UP/DOWN/UP cycle', async () => {
    await orm.getMigrator().down();

    const afterDown = await orm.em.getConnection().execute(
      "select to_regclass('public.wallets') as wallets, to_regclass('public.wager_transactions') as wager_transactions, to_regclass('public.wallet_ledger_entries') as wallet_ledger_entries, to_regclass('public.inbox_messages') as inbox_messages, to_regclass('public.outbox_messages') as outbox_messages",
    ) as Array<Record<string, string | null>>;
    expect(afterDown[0]).toEqual({
      wallets: null,
      wager_transactions: null,
      wallet_ledger_entries: null,
      inbox_messages: null,
      outbox_messages: null,
    });

    await orm.getMigrator().up();
    const afterUp = await orm.em.getConnection().execute(
      "select to_regclass('public.wallets') as wallets, to_regclass('public.wager_transactions') as wager_transactions, to_regclass('public.wallet_ledger_entries') as wallet_ledger_entries, to_regclass('public.inbox_messages') as inbox_messages, to_regclass('public.outbox_messages') as outbox_messages",
    ) as Array<Record<string, string | null>>;
    expect(afterUp[0]).toEqual({
      wallets: 'wallets',
      wager_transactions: 'wager_transactions',
      wallet_ledger_entries: 'wallet_ledger_entries',
      inbox_messages: 'inbox_messages',
      outbox_messages: 'outbox_messages',
    });
  });

  test('rejects a ledger entry for a pending transaction', async () => {
    const { transactionId, walletId } = await insertWalletAndTransaction('101', 'PENDING');

    await expectSqlState(`
      insert into wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, amount, currency,
        balance_before, balance_after, created_at
      ) values (
        '00000000-0000-0000-0000-000000000131', '${walletId}',
        '${transactionId}', 'DEBIT', 10.00, 'BRL', 100.00, 90.00, now()
      );
    `, '23514');
  });

  test('preserves exact NUMERIC values as strings through the MikroORM mapping', async () => {
    const createdAt = new Date('2026-10-28T00:00:00.000Z');
    const em = orm.em.fork();
    const record = em.create(WalletRecord, {
      id: '00000000-0000-0000-0000-000000000201',
      playerId: '00000000-0000-0000-0000-000000000211',
      currency: 'BRL',
      balance: '90071992547409.91',
      version: 1,
      createdAt,
      updatedAt: createdAt,
    });

    await em.persistAndFlush(record);
    em.clear();

    const rehydrated = await em.findOneOrFail(WalletRecord, record.id);
    expect(rehydrated.balance).toBe('90071992547409.91');
  });

  test('does not increment a wallet version for a non-financial MikroORM update', async () => {
    const createdAt = new Date('2026-10-28T00:00:00.000Z');
    const em = orm.em.fork();
    const record = em.create(WalletRecord, {
      id: '00000000-0000-0000-0000-000000000202',
      playerId: '00000000-0000-0000-0000-000000000212',
      currency: 'BRL',
      balance: '100.00',
      version: 1,
      createdAt,
      updatedAt: createdAt,
    });

    await em.persistAndFlush(record);
    em.clear();

    const persisted = await em.findOneOrFail(WalletRecord, record.id);
    persisted.updatedAt = new Date('2026-10-28T00:01:00.000Z');
    await em.flush();
    em.clear();

    const rehydrated = await em.findOneOrFail(WalletRecord, record.id);
    expect(rehydrated.version).toBe(1);
  });

  test('enforces that wallet versions change only with a balance transition', async () => {
    const walletId = '00000000-0000-0000-0000-000000000203';
    await orm.em.getConnection().execute(`
      insert into wallets (id, player_id, currency, balance, created_at, updated_at)
      values ('${walletId}', '00000000-0000-0000-0000-000000000213', 'BRL', 100.00, now(), now());
    `);

    const initial = await orm.em.getConnection().execute(
      `select version from wallets where id = '${walletId}'`,
    ) as Array<{ version: number }>;
    expect(initial).toEqual([{ version: 1 }]);

    await expectSqlState(
      `update wallets set version = 2 where id = '${walletId}'`,
      '23514',
    );
    await expectSqlState(
      `update wallets set balance = 101.00 where id = '${walletId}'`,
      '23514',
    );

    await orm.em.getConnection().execute(
      `update wallets set balance = 101.00, version = 2, updated_at = now() where id = '${walletId}'`,
    );

    const result = await orm.em.getConnection().execute(
      `select balance::text, version from wallets where id = '${walletId}'`,
    ) as Array<{ balance: string; version: number }>;
    expect(result[0]).toEqual({ balance: '101.00', version: 2 });
  });

  test('enforces idempotency-key uniqueness in PostgreSQL', async () => {
    const { walletId } = await insertWalletAndTransaction('102', 'PENDING');

    await expectSqlState(`
      insert into wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency,
        status, created_at
      ) values (
        '00000000-0000-0000-0000-000000000132', 'provider-102', 'external-duplicate-102',
        'provider-102:external-bet-102', 'payload-hash-duplicate-102', '${walletId}',
        '00000000-0000-0000-0000-000000000112', 'round-102', 'game-102', 'BET', 10.00, 'BRL',
        'PENDING', now()
      );
    `, '23505', 'wager_transactions_idempotency_key_key');
  });

  test('rejects unbalanced ledger arithmetic for a processed transaction', async () => {
    const { transactionId, walletId } = await insertWalletAndTransaction('103', 'PROCESSED');

    await expectSqlState(`
      insert into wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, amount, currency,
        balance_before, balance_after, created_at
      ) values (
        '00000000-0000-0000-0000-000000000133', '${walletId}',
        '${transactionId}', 'DEBIT', 10.00, 'BRL', 100.00, 91.00, now()
      );
    `, '23514');
  });

  test('blocks mutations to the ledger and terminal transaction statuses', async () => {
    const { transactionId, walletId } = await insertWalletAndTransaction('104', 'PROCESSED');

    await orm.em.getConnection().execute(`
      insert into wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, amount, currency,
        balance_before, balance_after, created_at
      ) values (
        '00000000-0000-0000-0000-000000000134', '${walletId}',
        '${transactionId}', 'DEBIT', 10.00, 'BRL', 100.00, 90.00, now()
      );
    `);

    await expectSqlState(
      "update wallet_ledger_entries set amount = 9.00 where id = '00000000-0000-0000-0000-000000000134'",
      '55000',
    );
    await expectSqlState(
      `update wager_transactions set status = 'FAILED' where id = '${transactionId}'`,
      '55000',
    );
  });

  test('rejects DELETE of an immutable ledger entry', async () => {
    const { transactionId, walletId } = await insertWalletAndTransaction('105', 'PROCESSED');
    const ledgerId = '00000000-0000-0000-0000-000000000135';

    await orm.em.getConnection().execute(`
      insert into wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, amount, currency,
        balance_before, balance_after, created_at
      ) values (
        '${ledgerId}', '${walletId}', '${transactionId}', 'DEBIT', 10.00, 'BRL', 100.00, 90.00, now()
      );
    `);

    await expectSqlState(`delete from wallet_ledger_entries where id = '${ledgerId}'`, '55000');
  });

  test('rejects duplicate wallet identities and negative balances', async () => {
    const playerId = '00000000-0000-0000-0000-000000000401';
    await orm.em.getConnection().execute(`
      insert into wallets (id, player_id, currency, balance, created_at, updated_at)
      values ('00000000-0000-0000-0000-000000000402', '${playerId}', 'BRL', 0.00, now(), now());
    `);

    await expectSqlState(`
      insert into wallets (id, player_id, currency, balance, created_at, updated_at)
      values ('00000000-0000-0000-0000-000000000403', '${playerId}', 'BRL', 0.00, now(), now());
    `, '23505');
    await expectSqlState(`
      insert into wallets (id, player_id, currency, balance, created_at, updated_at)
      values ('00000000-0000-0000-0000-000000000404', '00000000-0000-0000-0000-000000000405', 'BRL', -0.01, now(), now());
    `, '23514');
  });

  test('enforces provider-scoped external transaction uniqueness', async () => {
    const { walletId } = await insertWalletAndTransaction('106', 'PENDING');

    await expectSqlState(`
      insert into wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency, status, created_at
      ) values (
        '00000000-0000-0000-0000-000000000136', 'provider-106', 'external-bet-106',
        'provider-106:external-duplicate-106', 'payload-hash-duplicate-external-106', '${walletId}',
        '00000000-0000-0000-0000-000000000116', 'round-106', 'game-106', 'BET', 10.00, 'BRL', 'PENDING', now()
      );
    `, '23505');
  });

  test('rejects a duplicate ledger entry for the same wallet transaction', async () => {
    const { transactionId, walletId } = await insertWalletAndTransaction('107', 'PROCESSED');
    await orm.em.getConnection().execute(`
      insert into wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, amount, currency,
        balance_before, balance_after, created_at
      ) values (
        '00000000-0000-0000-0000-000000000137', '${walletId}', '${transactionId}',
        'DEBIT', 10.00, 'BRL', 100.00, 90.00, now()
      );
    `);

    await expectSqlState(`
      insert into wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, amount, currency,
        balance_before, balance_after, created_at
      ) values (
        '00000000-0000-0000-0000-000000000138', '${walletId}', '${transactionId}',
        'DEBIT', 10.00, 'BRL', 100.00, 90.00, now()
      );
    `, '23505');
  });

  test('rejects invalid financial foreign keys', async () => {
    await expectSqlState(`
      insert into wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency, status, created_at
      ) values (
        '00000000-0000-0000-0000-000000000139', 'provider-fk', 'external-fk', 'provider-fk:external-fk', 'payload-fk',
        '00000000-0000-0000-0000-000000000499', '00000000-0000-0000-0000-000000000498',
        'round-fk', 'game-fk', 'BET', 10.00, 'BRL', 'PENDING', now()
      );
    `, '23503');
  });

  test('rejects a processed timestamp for a non-processed transaction', async () => {
    const pending = await insertWalletAndTransaction('111', 'PENDING');

    await expectSqlState(
      `update wager_transactions set processed_at = now() where id = '${pending.transactionId}'`,
      '23514',
    );
  });

  test('allows valid PENDING and PENDING_REFERENCE transitions to PROCESSED', async () => {
    const pending = await insertWalletAndTransaction('108', 'PENDING');
    const pendingReference = await insertWalletAndTransaction('109', 'PENDING_REFERENCE');

    await orm.em.getConnection().execute(`
      update wager_transactions
      set status = 'PENDING_REFERENCE'
      where id = '${pending.transactionId}';

      update wager_transactions
      set status = 'PROCESSED', processed_at = now()
      where id in ('${pending.transactionId}', '${pendingReference.transactionId}');
    `);

    const result = await orm.em.getConnection().execute(`
      select id, status from wager_transactions
      where id in ('${pending.transactionId}', '${pendingReference.transactionId}')
      order by id;
    `) as Array<{ id: string; status: string }>;
    expect(result).toEqual([
      { id: pending.transactionId, status: 'PROCESSED' },
      { id: pendingReference.transactionId, status: 'PROCESSED' },
    ]);
  });

  test('permits a processed transaction and its ledger entry in the same SQL transaction', async () => {
    const walletId = '00000000-0000-0000-0000-000000000140';
    const playerId = '00000000-0000-0000-0000-000000000150';
    const transactionId = '00000000-0000-0000-0000-000000000160';

    await orm.em.getConnection().execute(`
      begin;
      insert into wallets (id, player_id, currency, balance, created_at, updated_at)
      values ('${walletId}', '${playerId}', 'BRL', 100.00, now(), now());
      insert into wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency, status, created_at
      ) values (
        '${transactionId}', 'provider-sequence', 'external-sequence', 'provider-sequence:external-sequence', 'payload-sequence',
        '${walletId}', '${playerId}', 'round-sequence', 'game-sequence', 'BET', 10.00, 'BRL', 'PENDING', now()
      );
      update wager_transactions set status = 'PROCESSED', processed_at = now() where id = '${transactionId}';
      insert into wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, amount, currency,
        balance_before, balance_after, created_at
      ) values (
        '00000000-0000-0000-0000-000000000170', '${walletId}', '${transactionId}',
        'DEBIT', 10.00, 'BRL', 100.00, 90.00, now()
      );
      commit;
    `);

    const result = await orm.em.getConnection().execute(
      `select count(*)::int as count from wallet_ledger_entries where transaction_id = '${transactionId}'`,
    ) as Array<{ count: number }>;
    expect(result).toEqual([{ count: 1 }]);
  });

  test('represents an internal OPENING transaction with its initial credit ledger entry', async () => {
    const walletId = '00000000-0000-0000-0000-000000000141';
    const playerId = '00000000-0000-0000-0000-000000000151';
    const transactionId = '00000000-0000-0000-0000-000000000161';

    await orm.em.getConnection().execute(`
      begin;
      insert into wallets (id, player_id, currency, balance, created_at, updated_at)
      values ('${walletId}', '${playerId}', 'BRL', 100.00, now(), now());
      insert into wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency, status, processed_at, created_at
      ) values (
        '${transactionId}', 'internal', null, 'internal:opening-${walletId}', 'opening-payload',
        '${walletId}', '${playerId}', 'opening-round', 'opening', 'OPENING', 100.00, 'BRL', 'PROCESSED', now(), now()
      );
      insert into wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, amount, currency,
        balance_before, balance_after, created_at
      ) values (
        '00000000-0000-0000-0000-000000000171', '${walletId}', '${transactionId}',
        'CREDIT', 100.00, 'BRL', 0.00, 100.00, now()
      );
      commit;
    `);

    const result = await orm.em.getConnection().execute(`
      select kind, external_transaction_id, direction, balance_before::text, balance_after::text
      from wager_transactions transaction
      join wallet_ledger_entries ledger on ledger.transaction_id = transaction.id
      where transaction.id = '${transactionId}';
    `) as Array<Record<string, string | null>>;
    expect(result[0]).toEqual({
      kind: 'OPENING',
      external_transaction_id: null,
      direction: 'CREDIT',
      balance_before: '0.00',
      balance_after: '100.00',
    });
  });

  test('blocks a second processed reversal of the same kind for one reference transaction', async () => {
    const reference = await insertWalletAndTransaction('110', 'PROCESSED');
    const playerId = '00000000-0000-0000-0000-000000000120';

    await orm.em.getConnection().execute(`
      insert into wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency,
        reference_transaction_id, reference_external_transaction_id, status, processed_at, created_at
      ) values (
        '00000000-0000-0000-0000-000000000180', 'provider-110', 'external-refund-110a',
        'provider-110:external-refund-110a', 'payload-refund-110a', '${reference.walletId}', '${playerId}',
        'round-110', 'game-110', 'REFUND', 10.00, 'BRL', '${reference.transactionId}', 'external-bet-110', 'PROCESSED', now(), now()
      );
    `);

    await expectSqlState(`
      insert into wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency,
        reference_transaction_id, reference_external_transaction_id, status, processed_at, created_at
      ) values (
        '00000000-0000-0000-0000-000000000181', 'provider-110', 'external-refund-110b',
        'provider-110:external-refund-110b', 'payload-refund-110b', '${reference.walletId}', '${playerId}',
        'round-110', 'game-110', 'REFUND', 10.00, 'BRL', '${reference.transactionId}', 'external-bet-110', 'PROCESSED', now(), now()
      );
    `, '23505');
  });
});
