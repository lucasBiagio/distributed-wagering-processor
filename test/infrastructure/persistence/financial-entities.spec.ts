import { describe, expect, test } from 'bun:test';

import { financialEntities } from '../../../src/infrastructure/persistence/financial-entities.js';

const entityNames = financialEntities.map((entity) => entity.meta.tableName);

describe('financial MikroORM mappings', () => {
  test('registers an explicit persistence mapping for every CP2A financial table', () => {
    expect(entityNames).toEqual([
      'wallets',
      'wager_transactions',
      'wallet_ledger_entries',
      'inbox_messages',
      'outbox_messages',
    ]);
  });
});
