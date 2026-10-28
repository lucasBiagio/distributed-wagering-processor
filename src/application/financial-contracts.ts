import { type MoneyProps } from '../domain/money.js';
import { type FailureCode, type WagerTransactionStatus } from '../domain/wager-transaction.js';

export interface OpenWalletCommand {
  readonly playerId: string;
  readonly initialBalance: MoneyProps;
  readonly correlationId?: string;
  readonly causationId?: string;
}

export interface ProcessBetCommand {
  readonly idempotencyKey: string;
  readonly providerId: string;
  readonly externalTransactionId: string;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly kind: 'BET';
  readonly money: MoneyProps;
  readonly correlationId?: string;
  readonly causationId?: string;
}

export interface WalletView {
  readonly id: string;
  readonly playerId: string;
  readonly balance: MoneyProps;
  readonly version: number;
}

export interface TransactionView {
  readonly transactionId: string;
  readonly providerId: string;
  readonly externalTransactionId: string | null;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly kind: string;
  readonly status: WagerTransactionStatus;
  readonly balance: MoneyProps | null;
  readonly failureCode: FailureCode | null;
}

export interface ProcessWagerResult extends TransactionView {
  readonly idempotentReplay: boolean;
}

export class FinancialConflictError extends Error {
  public constructor(message: string) {
    super(message);
    this.name = 'FinancialConflictError';
  }
}

export class FinancialNotFoundError extends Error {
  public constructor(message: string) {
    super(message);
    this.name = 'FinancialNotFoundError';
  }
}

export interface FinancialProcessingPort {
  openWallet(command: OpenWalletCommand): Promise<WalletView>;
  processBet(command: ProcessBetCommand): Promise<ProcessWagerResult>;
  getWallet(walletId: string): Promise<WalletView | null>;
  getTransaction(transactionId: string): Promise<TransactionView | null>;
  getProviderTransaction(providerId: string, externalTransactionId: string): Promise<TransactionView | null>;
}
