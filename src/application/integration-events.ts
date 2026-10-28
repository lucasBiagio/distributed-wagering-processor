import { type MoneyProps } from '../domain/money.js';
import { type Wallet } from '../domain/wallet.js';
import { type LedgerDirection, type WalletLedgerEntry } from '../domain/wallet-ledger-entry.js';

/**
 * Integration event envelope specified in CHALLENGE.md section 11 (Transactional Outbox).
 * `eventType` and `version` live in the type, never as a loose string at the call site, and
 * `data` always carries `MoneyProps` (decimal strings) so the payload stays JSON stable.
 */
export interface IntegrationEventProps<T> {
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: T;
}

/** Causation metadata shared by every event emitted inside one unit of work. */
export interface EventContext {
  readonly eventId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
}

export type IntegrationEventEnvelope<T> = {
  readonly eventId: string;
  readonly eventType: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: string;
  readonly version: number;
  readonly data: T;
};

export abstract class IntegrationEvent<T> {
  public abstract readonly eventType: string;
  public abstract readonly version: number;

  public readonly eventId: string;
  public readonly aggregateId: string;
  public readonly correlationId: string;
  public readonly causationId?: string;
  public readonly occurredAt: Date;
  public readonly data: Readonly<T>;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    if (props.causationId !== undefined) {
      this.causationId = props.causationId;
    }
    this.occurredAt = props.occurredAt;
    this.data = props.data;
  }

  /** Serialized envelope stored in the outbox payload. */
  public toJSON(): IntegrationEventEnvelope<T> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId === undefined ? {} : { causationId: this.causationId }),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data,
    };
  }
}

export interface WagerTransactionProcessedData {
  readonly transactionId: string;
  readonly walletId: string;
  readonly balance: MoneyProps;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  public readonly eventType = 'WagerTransactionProcessed';
  public readonly version = 1;

  public constructor(props: IntegrationEventProps<WagerTransactionProcessedData>) {
    super(props);
  }

  public static from(
    transactionId: string,
    walletId: string,
    balance: MoneyProps,
    ctx: EventContext,
  ): WagerTransactionProcessed {
    return new WagerTransactionProcessed({
      eventId: ctx.eventId,
      aggregateId: transactionId,
      correlationId: ctx.correlationId,
      ...(ctx.causationId === undefined ? {} : { causationId: ctx.causationId }),
      occurredAt: ctx.occurredAt,
      data: { transactionId, walletId, balance },
    });
  }
}

export interface WagerTransactionRejectedData {
  readonly transactionId: string;
  readonly walletId: string;
  readonly balance: MoneyProps;
  readonly failureCode: string;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  public readonly eventType = 'WagerTransactionRejected';
  public readonly version = 1;

  public constructor(props: IntegrationEventProps<WagerTransactionRejectedData>) {
    super(props);
  }

  public static from(
    transactionId: string,
    walletId: string,
    balance: MoneyProps,
    failureCode: string,
    ctx: EventContext,
  ): WagerTransactionRejected {
    return new WagerTransactionRejected({
      eventId: ctx.eventId,
      aggregateId: transactionId,
      correlationId: ctx.correlationId,
      ...(ctx.causationId === undefined ? {} : { causationId: ctx.causationId }),
      occurredAt: ctx.occurredAt,
      data: { transactionId, walletId, balance, failureCode },
    });
  }
}

export interface WalletBalanceChangedData {
  readonly walletId: string;
  readonly transactionId: string;
  readonly direction: LedgerDirection;
  readonly money: MoneyProps;
  readonly balanceBefore: MoneyProps;
  readonly balanceAfter: MoneyProps;
  readonly walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  public readonly eventType = 'WalletBalanceChanged';
  public readonly version = 1;

  public constructor(props: IntegrationEventProps<WalletBalanceChangedData>) {
    super(props);
  }

  /**
   * Builds the event straight from the aggregate and the ledger entry it produced, so the
   * declared `walletVersion` is always the version persisted by the same unit of work.
   */
  public static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged({
      eventId: ctx.eventId,
      aggregateId: wallet.id,
      correlationId: ctx.correlationId,
      ...(ctx.causationId === undefined ? {} : { causationId: ctx.causationId }),
      occurredAt: ctx.occurredAt,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}
