import { describe, expect, test } from 'bun:test';

import { InvalidTransactionStateError } from '../../src/domain/domain-error.js';
import { Money } from '../../src/domain/money.js';
import {
  FailureCode,
  LedgerDirection,
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/wager-transaction.js';

const at = new Date('2026-10-28T00:00:00.000Z');

function createBet(): WagerTransaction {
  return WagerTransaction.create({
    id: 'transaction-1',
    providerId: 'provider-a',
    externalTransactionId: 'external-1',
    idempotencyKey: 'provider-a:external-1',
    payloadHash: 'hash-1',
    walletId: 'wallet-1',
    playerId: 'player-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind: WagerTransactionKind.Bet,
    money: Money.from({ amount: '25.00', currency: 'BRL' }),
    createdAt: at,
  });
}

describe('WagerTransaction', () => {
  test('creates a pending BET and permits its one-way processed transition', () => {
    const transaction = createBet();

    expect(transaction.status).toBe(WagerTransactionStatus.Pending);
    expect(transaction.affectsBalance()).toBe(true);
    expect(transaction.ledgerDirectionFor()).toBe(LedgerDirection.Debit);

    transaction.markProcessed(undefined, at);

    expect(transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(transaction.processedAt).toBe(at);
    expect(() => transaction.fail(FailureCode.InfrastructureFailure)).toThrow(
      new InvalidTransactionStateError(WagerTransactionStatus.Processed),
    );
  });
});
