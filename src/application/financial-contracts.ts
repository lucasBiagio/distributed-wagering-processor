import { type MoneyProps } from '../domain/money.js';
import { type FailureCode, type WagerTransactionStatus } from '../domain/wager-transaction.js';

export interface OpenWalletCommand {
  readonly playerId: string;
  readonly initialBalance: MoneyProps;
  readonly correlationId?: string;
  readonly causationId?: string;
}

export type WagerKindInput = 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';

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

/**
 * CP2B-2 submission contract. It is a widening of `ProcessBetCommand`: every BET command is
 * a valid wager command, so the CP2B-1 contract keeps working unchanged. REFUND and ROLLBACK
 * additionally carry the provider identifier of the transaction they reverse.
 */
export interface ProcessWagerCommand {
  readonly idempotencyKey: string;
  readonly providerId: string;
  readonly externalTransactionId: string;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly kind: WagerKindInput;
  readonly money: MoneyProps;
  readonly referenceExternalTransactionId?: string;
  readonly correlationId?: string;
  readonly causationId?: string;
}

export interface PendingReferenceSweepOptions {
  /** Injected clock: the sweep never reads the wall clock, which keeps tests deterministic. */
  readonly now: Date;
  readonly limit?: number;
}

export interface PendingReferenceSweepResult {
  readonly claimed: number;
  readonly processed: number;
  readonly rejected: number;
  readonly rescheduled: number;
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
  readonly referenceExternalTransactionId: string | null;
  readonly referenceTransactionId: string | null;
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
  processWager(command: ProcessWagerCommand): Promise<ProcessWagerResult>;
  reprocessPendingReferences(options: PendingReferenceSweepOptions): Promise<PendingReferenceSweepResult>;
  getWallet(walletId: string): Promise<WalletView | null>;
  getTransaction(transactionId: string): Promise<TransactionView | null>;
  getProviderTransaction(providerId: string, externalTransactionId: string): Promise<TransactionView | null>;
}
