import { DomainRuleError, InvalidTransactionStateError } from './domain-error.js';
import { type Money } from './money.js';
import { LedgerDirection } from './wallet-ledger-entry.js';

export { LedgerDirection } from './wallet-ledger-entry.js';

export enum WagerTransactionKind {
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}

export enum TransactionOrigin {
  External = 'EXTERNAL',
  Internal = 'INTERNAL',
}

export enum FailureCode {
  InsufficientBalance = 'INSUFFICIENT_BALANCE',
  ReversalWouldCauseNegativeBalance = 'REVERSAL_WOULD_CAUSE_NEGATIVE_BALANCE',
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  ReferenceIncompatible = 'REFERENCE_INCOMPATIBLE',
  ReferenceAmountMismatch = 'REFERENCE_AMOUNT_MISMATCH',
  ReferenceAlreadyReversed = 'REFERENCE_ALREADY_REVERSED',
  InvalidTransaction = 'INVALID_TRANSACTION',
  InfrastructureFailure = 'INFRASTRUCTURE_FAILURE',
}

export interface CreateWagerTransactionProps {
  readonly id: string;
  readonly providerId: string;
  readonly externalTransactionId?: string;
  readonly idempotencyKey: string;
  readonly payloadHash: string;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly kind: WagerTransactionKind;
  readonly money: Money;
  readonly referenceExternalTransactionId?: string;
  readonly createdAt: Date;
  readonly origin?: TransactionOrigin;
}

export interface WagerTransactionState extends CreateWagerTransactionProps {
  readonly status: WagerTransactionStatus;
  readonly referenceTransactionId?: string;
  readonly failureCode?: FailureCode;
  readonly processedAt?: Date;
}

const TERMINAL_STATUSES = new Set<WagerTransactionStatus>([
  WagerTransactionStatus.Processed,
  WagerTransactionStatus.Rejected,
  WagerTransactionStatus.Failed,
]);

export class WagerTransaction {
  private _status: WagerTransactionStatus;
  private _referenceTransactionId: string | undefined;
  private _failureCode: FailureCode | undefined;
  private _processedAt: Date | undefined;

  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string | undefined,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    status: WagerTransactionStatus,
    referenceTransactionId?: string,
    failureCode?: FailureCode,
    processedAt?: Date,
  ) {
    this._status = status;
    this._referenceTransactionId = referenceTransactionId;
    this._failureCode = failureCode;
    this._processedAt = processedAt;
  }

  public static create(props: CreateWagerTransactionProps): WagerTransaction {
    const origin = props.origin ?? TransactionOrigin.External;
    const needsReference = WagerTransaction.requiresReferenceFor(props.kind);

    if (props.kind === WagerTransactionKind.Opening && origin !== TransactionOrigin.Internal) {
      throw new DomainRuleError('OPENING_TRANSACTION_INTERNAL_ONLY', 'OPENING transactions are internal only');
    }

    if (origin === TransactionOrigin.External && !props.externalTransactionId) {
      throw new DomainRuleError('EXTERNAL_TRANSACTION_ID_REQUIRED', 'External transactions require an external transaction identifier');
    }

    if (needsReference && !props.referenceExternalTransactionId) {
      throw new DomainRuleError('REFERENCE_REQUIRED', `${props.kind} requires a referenced external transaction identifier`);
    }

    if (!needsReference && props.referenceExternalTransactionId) {
      throw new DomainRuleError('REFERENCE_NOT_ALLOWED', `${props.kind} cannot carry a transaction reference`);
    }

    if (!props.money.isPositive() && props.kind !== WagerTransactionKind.Loss) {
      throw new DomainRuleError('TRANSACTION_AMOUNT_MUST_BE_POSITIVE', 'Balance-affecting transactions require a positive amount');
    }

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      props.createdAt,
      WagerTransactionStatus.Pending,
    );
  }

  public static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.createdAt,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
    );
  }

  public get status(): WagerTransactionStatus {
    return this._status;
  }

  public get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  public get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  public get processedAt(): Date | undefined {
    return this._processedAt;
  }

  public markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertMutable();

    if (this.requiresReference() && !referenceTransactionId) {
      throw new DomainRuleError('REFERENCE_TRANSACTION_ID_REQUIRED', `${this.kind} requires its resolved internal reference identifier`);
    }

    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
  }

  public markPendingReference(): void {
    this.assertMutable();

    if (!this.requiresReference()) {
      throw new DomainRuleError('PENDING_REFERENCE_NOT_ALLOWED', `${this.kind} does not await a reference`);
    }

    this._status = WagerTransactionStatus.PendingReference;
  }

  public reject(code: FailureCode): void {
    this.assertMutable();
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
  }

  public fail(code: FailureCode): void {
    this.assertMutable();
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
  }

  public isTerminal(): boolean {
    return TERMINAL_STATUSES.has(this._status);
  }

  public affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  public requiresReference(): boolean {
    return WagerTransaction.requiresReferenceFor(this.kind);
  }

  public matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  public ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Loss:
        throw new DomainRuleError('LOSS_HAS_NO_LEDGER_DIRECTION', 'LOSS transactions do not produce ledger entries');
      case WagerTransactionKind.Refund:
        this.assertCompatibleReference(reference, [WagerTransactionKind.Bet]);
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback:
        this.assertCompatibleReference(reference, [
          WagerTransactionKind.Bet,
          WagerTransactionKind.Win,
          WagerTransactionKind.Refund,
        ]);
        return reference!.kind === WagerTransactionKind.Bet
          ? LedgerDirection.Credit
          : LedgerDirection.Debit;
    }
  }

  private assertMutable(): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(this._status);
    }
  }

  private assertCompatibleReference(
    reference: WagerTransaction | undefined,
    allowedKinds: readonly WagerTransactionKind[],
  ): void {
    if (!reference) {
      throw new DomainRuleError('REFERENCE_REQUIRED', `${this.kind} requires a resolved reference transaction`);
    }

    if (reference.status !== WagerTransactionStatus.Processed) {
      throw new DomainRuleError('REFERENCE_NOT_PROCESSED', 'Reference transaction must be processed');
    }

    if (!allowedKinds.includes(reference.kind)) {
      throw new DomainRuleError('REFERENCE_KIND_INCOMPATIBLE', `${this.kind} cannot reference ${reference.kind}`);
    }

    if (
      reference.providerId !== this.providerId ||
      reference.playerId !== this.playerId ||
      reference.walletId !== this.walletId ||
      reference.roundId !== this.roundId ||
      reference.externalTransactionId !== this.referenceExternalTransactionId
    ) {
      throw new DomainRuleError(FailureCode.ReferenceIncompatible, 'Reference transaction is not compatible with this transaction');
    }

    try {
      if (!reference.money.equals(this.money)) {
        throw new DomainRuleError(FailureCode.ReferenceAmountMismatch, 'Reversal amount must equal its reference amount');
      }
    } catch (error) {
      if (error instanceof DomainRuleError) {
        throw error;
      }

      throw new DomainRuleError(FailureCode.ReferenceIncompatible, 'Reference transaction currency is not compatible');
    }
  }

  private static requiresReferenceFor(kind: WagerTransactionKind): boolean {
    return kind === WagerTransactionKind.Refund || kind === WagerTransactionKind.Rollback;
  }
}
