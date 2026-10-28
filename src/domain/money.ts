import { Decimal } from 'decimal.js';

export interface MoneyProps {
  readonly amount: string;
  readonly currency: string;
}

export class MoneyValidationError extends Error {
  public constructor(message: string) {
    super(message);
    this.name = 'MoneyValidationError';
  }
}

export class CurrencyMismatchError extends Error {
  public constructor(expected: string, received: string) {
    super(`Currency mismatch: expected ${expected}, received ${received}`);
    this.name = 'CurrencyMismatchError';
  }
}

const EXTERNAL_AMOUNT = /^(?:0|[1-9]\d*)(?:\.\d{1,2})?$/;
const ISO_4217_CURRENCIES = new Set(Intl.supportedValuesOf('currency'));

export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  public static from(props: MoneyProps): Money {
    const currency = Money.validateCurrency(props.currency);

    if (typeof props.amount !== 'string' || !EXTERNAL_AMOUNT.test(props.amount)) {
      throw new MoneyValidationError('Amount must be a non-negative decimal string with at most two decimal places');
    }

    return Money.createInternal(new Decimal(props.amount), currency);
  }

  public static zero(currency: string): Money {
    return Money.createInternal(new Decimal(0), Money.validateCurrency(currency));
  }

  public add(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.createInternal(this.value.add(other.value), this.currency);
  }

  public subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.createInternal(this.value.sub(other.value), this.currency);
  }

  public negate(): Money {
    return Money.createInternal(this.value.neg(), this.currency);
  }

  public isZero(): boolean {
    return this.value.isZero();
  }

  public isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  public isNegative(): boolean {
    return this.value.isNegative();
  }

  public isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  public equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.equals(other.value);
  }

  public toJSON(): MoneyProps {
    return { amount: this.value.toFixed(2), currency: this.currency };
  }

  public toString(): string {
    const { amount, currency } = this.toJSON();
    return `${amount} ${currency}`;
  }

  public assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }

  private static createInternal(value: Decimal, currency: string): Money {
    if (!value.isFinite()) {
      throw new MoneyValidationError('Amount must be finite');
    }

    if (value.decimalPlaces() > 2) {
      throw new MoneyValidationError('Amount must not have more than two decimal places');
    }

    return new Money(value, currency);
  }

  private static validateCurrency(currency: string): string {
    if (typeof currency !== 'string' || !ISO_4217_CURRENCIES.has(currency)) {
      throw new MoneyValidationError('Currency must be a valid uppercase ISO-4217 code');
    }

    return currency;
  }
}
