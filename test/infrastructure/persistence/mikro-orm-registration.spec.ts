import { describe, expect, test } from 'bun:test';

import config from '../../../src/infrastructure/persistence/mikro-orm.config.js';

const expectedTables = [
  'wallets',
  'wager_transactions',
  'wallet_ledger_entries',
  'inbox_messages',
  'outbox_messages',
];

describe('MikroORM configuration', () => {
  test('registers every CP2A financial schema for CLI discovery', () => {
    const tableNames = (config.entities as Array<{ meta: { tableName: string } }>).map(
      (entity) => entity.meta.tableName,
    );

    expect(tableNames).toEqual(expectedTables);
  });
});
