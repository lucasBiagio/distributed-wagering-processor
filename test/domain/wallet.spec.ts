import { describe, expect, test } from 'bun:test';

import { DomainRuleError } from '../../src/domain/domain-error.js';
import { Money } from '../../src/domain/money.js';
import { Wallet } from '../../src/domain/wallet.js';

describe('Wallet', () => {
  test('opens with a positive balance and produces the associated opening ledger entry', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
      openingTransactionId: 'opening-transaction-1',
      openingLedgerEntryId: 'opening-ledger-1',
      at: new Date('2026-10-28T00:00:00.000Z'),
    });

    expect(wallet.balance.toJSON()).toEqual({ amount: '100.00', currency: 'BRL' });
    expect(wallet.version).toBe(1);
    expect(wallet.openingLedgerEntry?.isBalanced()).toBe(true);
    expect(wallet.openingLedgerEntry?.transactionId).toBe('opening-transaction-1');
  });

  test('debits only when sufficient balance exists and increments its version with an immutable ledger entry', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
      openingTransactionId: 'opening-transaction-1',
      openingLedgerEntryId: 'opening-ledger-1',
      at: new Date('2026-10-28T00:00:00.000Z'),
    });

    const entry = wallet.debit({
      amount: Money.from({ amount: '25.00', currency: 'BRL' }),
      transactionId: 'bet-1',
      ledgerEntryId: 'ledger-1',
      at: new Date('2026-10-28T00:00:01.000Z'),
    });

    expect(wallet.balance.toJSON().amount).toBe('75.00');
    expect(wallet.version).toBe(2);
    expect(entry.isBalanced()).toBe(true);
    expect(() => wallet.debit({
      amount: Money.from({ amount: '76.00', currency: 'BRL' }),
      transactionId: 'bet-2',
      ledgerEntryId: 'ledger-2',
      at: new Date('2026-10-28T00:00:02.000Z'),
    })).toThrow(new DomainRuleError('INSUFFICIENT_BALANCE', 'Wallet balance is insufficient for this debit'));
  });
});
