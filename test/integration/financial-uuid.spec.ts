import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import {
  MikroOrmFinancialProcessing,
  isUuid,
} from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';
import { WagerTransactionStatus } from '../../src/domain/wager-transaction.js';
import config from '../../src/infrastructure/persistence/mikro-orm.config.js';

/**
 * CP2B-1-R3 FIX-01: CHALLENGE.md identifies players with UUIDv7
 * (`0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1`). The validation must accept every RFC 9562
 * version while still rejecting arbitrary strings, wrong variants and malformed input.
 */
const CHALLENGE_PLAYER_ID = '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1';
const UUID_V7_OTHER = '0192f28f-5dc0-7f1a-9c6e-3b7a1d2e4f50';
const UUID_V4 = '9f1c0b2a-6d3e-4a58-8b21-0c7d5e9f3a46';
const UUID_V1 = 'c232ab00-9414-11ec-b3c8-9f6bdeced846';

let orm: MikroORM<PostgreSqlDriver>;
let processing: MikroOrmFinancialProcessing;

describe('CP2B-1-R3 FIX-01 UUID compatibility', () => {
  beforeAll(async () => {
    orm = await MikroORM.init<PostgreSqlDriver>(config);
    await orm.getMigrator().up();
    processing = new MikroOrmFinancialProcessing(orm);
  });

  afterAll(async () => {
    await orm.getMigrator().down();
    await orm.close(true);
  });

  test('accepts a valid UUIDv4', () => {
    expect(isUuid(UUID_V4)).toBe(true);
  });

  test('accepts the UUIDv7 used by CHALLENGE.md', () => {
    expect(isUuid(CHALLENGE_PLAYER_ID)).toBe(true);
    expect(isUuid(UUID_V7_OTHER)).toBe(true);
    expect(isUuid(UUID_V1)).toBe(true);
  });

  test('rejects malformed identifiers instead of accepting arbitrary strings', () => {
    const malformed = [
      'not-a-uuid',
      '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a',
      '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1a',
      '0192f28f5dc07d58bdb2814ad6a0f4a1',
      '0192f28f-5dc0-0d58-bdb2-814ad6a0f4a1',
      '0192f28f-5dc0-9d58-bdb2-814ad6a0f4a1',
      '0192f28f-5dc0-7d58-7db2-814ad6a0f4a1',
      '00000000-0000-0000-0000-000000000000',
      'zzzzzzzz-5dc0-7d58-bdb2-814ad6a0f4a1',
      '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1 or 1=1',
    ];

    for (const candidate of malformed) {
      expect(isUuid(candidate), candidate).toBe(false);
    }
  });

  test('rejects empty values', () => {
    expect(isUuid('')).toBe(false);
    expect(isUuid('   ')).toBe(false);
  });

  test('persists a UUIDv7 player through wallet opening and BET processing', async () => {
    const wallet = await processing.openWallet({
      playerId: CHALLENGE_PLAYER_ID,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });

    expect(wallet.playerId).toBe(CHALLENGE_PLAYER_ID);
    expect(wallet.version).toBe(1);

    const bet = await processing.processBet({
      idempotencyKey: 'uuidv7-bet-key',
      providerId: 'provider-uuidv7',
      externalTransactionId: 'external-uuidv7-bet',
      walletId: wallet.id,
      playerId: CHALLENGE_PLAYER_ID,
      roundId: 'round-uuidv7',
      gameId: 'game-uuidv7',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });

    expect(bet.status).toBe(WagerTransactionStatus.Processed);
    expect(bet.playerId).toBe(CHALLENGE_PLAYER_ID);
    expect(bet.balance).toEqual({ amount: '75.00', currency: 'BRL' });

    const rows = await orm.em.getConnection().execute(`
      select w.player_id::text as wallet_player_id,
             t.player_id::text as transaction_player_id,
             w.balance::text as balance
      from wallets w
      join wager_transactions t on t.wallet_id = w.id and t.kind = 'BET'
      where w.id = '${wallet.id}'
    `) as Array<{ wallet_player_id: string; transaction_player_id: string; balance: string }>;

    expect(rows).toEqual([{
      wallet_player_id: CHALLENGE_PLAYER_ID,
      transaction_player_id: CHALLENGE_PLAYER_ID,
      balance: '75.00',
    }]);
  });
});
