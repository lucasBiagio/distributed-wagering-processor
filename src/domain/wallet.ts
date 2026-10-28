import { DomainRuleError } from './domain-error.js';
import { Money } from './money.js';
import {
  LedgerDirection,
  WalletLedgerEntry,
  type WalletLedgerEntryState,
} from './wallet-ledger-entry.js';

export interface WalletState {
  readonly id: string;
  readonly playerId: string;
  readonly currency: string;
  readonly balance: Money;
  readonly version: number;
  readonly createdAt: Date;
  readonly updatedAt: Date;
}

export interface OpenWalletProps {
  readonly id: string;
  readonly playerId: string;
  readonly initialBalance: Money;
  readonly openingTransactionId?: string;
  readonly openingLedgerEntryId?: string;
  readonly at: Date;
}

export interface WalletMovementProps {
  readonly amount: Money;
  readonly transactionId: string;
  readonly ledgerEntryId: string;
  readonly at: Date;
}

export class Wallet {
  private _balance: Money;
  private _version: number;
  private _updatedAt: Date;

  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    balance: Money,
    version: number,
    public readonly createdAt: Date,
    updatedAt: Date,
    public readonly openingLedgerEntry?: WalletLedgerEntry,
  ) {
    this._balance = balance;
    this._version = version;
    this._updatedAt = updatedAt;
  }

  public static open(props: OpenWalletProps): Wallet {
    if (props.initialBalance.isNegative()) {
      throw new DomainRuleError('WALLET_INITIAL_BALANCE_NEGATIVE', 'Wallet initial balance cannot be negative');
    }

    const requiresOpeningEntry = props.initialBalance.isPositive();
    if (requiresOpeningEntry && (!props.openingTransactionId || !props.openingLedgerEntryId)) {
      throw new DomainRuleError('OPENING_LEDGER_REQUIRED', 'A positive initial balance requires opening transaction and ledger identifiers');
    }

    if (!requiresOpeningEntry && (props.openingTransactionId || props.openingLedgerEntryId)) {
      throw new DomainRuleError('OPENING_LEDGER_NOT_ALLOWED', 'A zero initial balance must not create an opening ledger entry');
    }

    const openingLedgerEntry = requiresOpeningEntry
      ? WalletLedgerEntry.create({
        id: props.openingLedgerEntryId!,
        walletId: props.id,
        transactionId: props.openingTransactionId!,
        direction: LedgerDirection.Credit,
        money: props.initialBalance,
        balanceBefore: Money.zero(props.initialBalance.currency),
        balanceAfter: props.initialBalance,
        createdAt: props.at,
      })
      : undefined;

    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      props.at,
      props.at,
      openingLedgerEntry,
    );
  }

  public static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  public get balance(): Money {
    return this._balance;
  }

  public get version(): number {
    return this._version;
  }

  public get updatedAt(): Date {
    return this._updatedAt;
  }

  public debit(props: WalletMovementProps): WalletLedgerEntry {
    this.assertMovement(props.amount);

    const balanceAfter = this._balance.subtract(props.amount);
    if (balanceAfter.isNegative()) {
      throw new DomainRuleError('INSUFFICIENT_BALANCE', 'Wallet balance is insufficient for this debit');
    }

    return this.applyMovement({
      id: props.ledgerEntryId,
      walletId: this.id,
      transactionId: props.transactionId,
      direction: LedgerDirection.Debit,
      money: props.amount,
      balanceBefore: this._balance,
      balanceAfter,
      createdAt: props.at,
    });
  }

  public credit(props: WalletMovementProps): WalletLedgerEntry {
    this.assertMovement(props.amount);

    return this.applyMovement({
      id: props.ledgerEntryId,
      walletId: this.id,
      transactionId: props.transactionId,
      direction: LedgerDirection.Credit,
      money: props.amount,
      balanceBefore: this._balance,
      balanceAfter: this._balance.add(props.amount),
      createdAt: props.at,
    });
  }

  private assertMovement(amount: Money): void {
    if (this.currency !== amount.currency) {
      throw new DomainRuleError('WALLET_CURRENCY_MISMATCH', 'Wallet and operation currencies must match');
    }

    if (!amount.isPositive()) {
      throw new DomainRuleError('WALLET_MOVEMENT_AMOUNT_MUST_BE_POSITIVE', 'Wallet movement amount must be positive');
    }
  }

  private applyMovement(state: WalletLedgerEntryState): WalletLedgerEntry {
    const entry = WalletLedgerEntry.create(state);
    this._balance = state.balanceAfter;
    this._version += 1;
    this._updatedAt = state.createdAt;
    return entry;
  }
}
