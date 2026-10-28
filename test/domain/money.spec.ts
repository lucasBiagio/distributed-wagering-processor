import { describe, expect, test } from 'bun:test';

import { Money } from '../../src/domain/money.js';

describe('Money', () => {
  test('creates an immutable fixed-scale monetary value from an external contract', () => {
    const money = Money.from({ amount: '25.00', currency: 'BRL' });

    expect(money.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
    expect(money.toString()).toBe('25.00 BRL');
    expect(money.isPositive()).toBe(true);
    expect(Object.isFrozen(money)).toBe(true);
  });
});
