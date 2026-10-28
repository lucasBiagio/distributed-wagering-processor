export class DomainRuleError extends Error {
  public constructor(
    public readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'DomainRuleError';
  }
}

export class InvalidTransactionStateError extends DomainRuleError {
  public constructor(status: string) {
    super('INVALID_TRANSACTION_STATE', `Transaction in ${status} is terminal and cannot transition`);
    this.name = 'InvalidTransactionStateError';
  }
}
