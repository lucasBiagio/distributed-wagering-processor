import { DomainRuleError } from './domain-error.js';
import { type Money } from './money.js';

export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

export interface WalletLedgerEntryState {
  readonly id: string;
  readonly walletId: string;
  readonly transactionId: string;
  readonly direction: LedgerDirection;
  readonly money: Money;
  readonly balanceBefore: Money;
  readonly balanceAfter: Money;
  readonly createdAt: Date;
}

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {
    Object.freeze(this);
  }

  public static create(state: WalletLedgerEntryState): WalletLedgerEntry {
    const entry = new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );

    if (!entry.money.isPositive()) {
      throw new DomainRuleError('LEDGER_AMOUNT_MUST_BE_POSITIVE', 'Ledger amount must be positive');
    }

    if (entry.balanceAfter.isNegative()) {
      throw new DomainRuleError('LEDGER_BALANCE_MUST_NOT_BE_NEGATIVE', 'Ledger balance cannot be negative');
    }

    if (!entry.isBalanced()) {
      throw new DomainRuleError('LEDGER_ARITHMETIC_MISMATCH', 'Ledger balances do not match its direction and amount');
    }

    return entry;
  }

  public static rehydrate(state: WalletLedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  public isBalanced(): boolean {
    const expected = this.direction === LedgerDirection.Debit
      ? this.balanceBefore.subtract(this.money)
      : this.balanceBefore.add(this.money);

    return expected.equals(this.balanceAfter);
  }
}
