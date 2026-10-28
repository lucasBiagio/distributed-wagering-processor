import { type MoneyProps } from '../domain/money.js';

export abstract class IntegrationEvent<TPayload extends Record<string, unknown>> {
  public abstract readonly type: string;
  public readonly version = 1;

  protected constructor(
    public readonly id: string,
    public readonly correlationId: string,
    public readonly occurredAt: Date,
    public readonly payload: TPayload,
  ) {}
}

export class WagerTransactionProcessed extends IntegrationEvent<{
  readonly transactionId: string;
  readonly walletId: string;
  readonly balance: MoneyProps;
}> {
  public readonly type = 'WagerTransactionProcessed';

  public constructor(id: string, correlationId: string, occurredAt: Date, payload: {
    readonly transactionId: string;
    readonly walletId: string;
    readonly balance: MoneyProps;
  }) {
    super(id, correlationId, occurredAt, payload);
  }
}

export class WagerTransactionRejected extends IntegrationEvent<{
  readonly transactionId: string;
  readonly walletId: string;
  readonly balance: MoneyProps;
  readonly failureCode: string;
}> {
  public readonly type = 'WagerTransactionRejected';

  public constructor(id: string, correlationId: string, occurredAt: Date, payload: {
    readonly transactionId: string;
    readonly walletId: string;
    readonly balance: MoneyProps;
    readonly failureCode: string;
  }) {
    super(id, correlationId, occurredAt, payload);
  }
}

export class WalletBalanceChanged extends IntegrationEvent<{
  readonly walletId: string;
  readonly previousBalance: MoneyProps;
  readonly balance: MoneyProps;
  readonly transactionId: string;
}> {
  public readonly type = 'WalletBalanceChanged';

  public constructor(id: string, correlationId: string, occurredAt: Date, payload: {
    readonly walletId: string;
    readonly previousBalance: MoneyProps;
    readonly balance: MoneyProps;
    readonly transactionId: string;
  }) {
    super(id, correlationId, occurredAt, payload);
  }
}
