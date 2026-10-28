import { describe, expect, test } from 'bun:test';

import { WalletLedgerEntry, LedgerDirection } from '../../src/domain/wallet-ledger-entry.js';
import { Money } from '../../src/domain/money.js';

describe('WalletLedgerEntry', () => {
  test('creates a balanced immutable debit entry', () => {
    const entry = WalletLedgerEntry.create({
      id: 'ledger-1',
      walletId: 'wallet-1',
      transactionId: 'transaction-1',
      direction: LedgerDirection.Debit,
      money: Money.from({ amount: '25.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '75.00', currency: 'BRL' }),
      createdAt: new Date('2026-10-28T00:00:00.000Z'),
    });

    expect(entry.isBalanced()).toBe(true);
    expect(Object.isFrozen(entry)).toBe(true);
  });
});
