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

function createTransaction(props: {
  id: string;
  externalTransactionId: string;
  kind: WagerTransactionKind;
  amount?: string;
  referenceExternalTransactionId?: string;
}): WagerTransaction {
  return WagerTransaction.create({
    id: props.id,
    providerId: 'provider-a',
    externalTransactionId: props.externalTransactionId,
    idempotencyKey: `provider-a:${props.externalTransactionId}`,
    payloadHash: `hash-${props.id}`,
    walletId: 'wallet-1',
    playerId: 'player-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind: props.kind,
    money: Money.from({ amount: props.amount ?? '25.00', currency: 'BRL' }),
    ...(props.referenceExternalTransactionId === undefined
      ? {}
      : { referenceExternalTransactionId: props.referenceExternalTransactionId }),
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

  test('derives rollback directions without recursively resolving a refund reference', () => {
    const bet = createBet();
    bet.markProcessed(undefined, at);

    const win = createTransaction({ id: 'win-1', externalTransactionId: 'win-external-1', kind: WagerTransactionKind.Win });
    win.markProcessed(undefined, at);

    const refund = createTransaction({
      id: 'refund-1',
      externalTransactionId: 'refund-external-1',
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'external-1',
    });
    refund.markProcessed('transaction-1', at);

    const rollbackBet = createTransaction({
      id: 'rollback-bet-1',
      externalTransactionId: 'rollback-bet-external-1',
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'external-1',
    });
    const rollbackWin = createTransaction({
      id: 'rollback-win-1',
      externalTransactionId: 'rollback-win-external-1',
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'win-external-1',
    });
    const rollbackRefund = createTransaction({
      id: 'rollback-refund-1',
      externalTransactionId: 'rollback-refund-external-1',
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'refund-external-1',
    });

    expect(rollbackBet.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
    expect(rollbackWin.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
    expect(rollbackRefund.ledgerDirectionFor(refund)).toBe(LedgerDirection.Debit);
  });

  test('rejects rollback references with an incompatible amount or unresolved state', () => {
    const reference = createBet();
    const rollback = createTransaction({
      id: 'rollback-1',
      externalTransactionId: 'rollback-external-1',
      kind: WagerTransactionKind.Rollback,
      amount: '20.00',
      referenceExternalTransactionId: 'external-1',
    });

    expect(() => rollback.ledgerDirectionFor(reference)).toThrow('Reference transaction must be processed');

    reference.markProcessed(undefined, at);
    expect(() => rollback.ledgerDirectionFor(reference)).toThrow('Reversal amount must equal its reference amount');
  });

  test('accepts zero-value LOSS consistently as a non-balance operation', () => {
    const loss = createTransaction({
      id: 'loss-1',
      externalTransactionId: 'loss-external-1',
      kind: WagerTransactionKind.Loss,
      amount: '0.00',
    });

    expect(loss.affectsBalance()).toBe(false);
    expect(() => loss.ledgerDirectionFor()).toThrow('LOSS transactions do not produce ledger entries');
  });
});
