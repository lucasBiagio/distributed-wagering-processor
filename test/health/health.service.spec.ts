import { describe, expect, test } from 'bun:test';

import {
  HealthReadinessService,
  HealthService,
  type ReadinessProbe,
} from '../../src/interfaces/http/health/health.service.js';

describe('HealthService', () => {
  test('reports a live process without reaching external dependencies', () => {
    expect(new HealthService().live()).toEqual({ status: 'ok' });
  });
});

describe('HealthReadinessService', () => {
  test('reports ready only after PostgreSQL and SQS probes succeed', async () => {
    const probes: ReadinessProbe[] = [
      { name: 'postgres', check: async () => undefined },
      { name: 'sqs', check: async () => undefined },
    ];

    await expect(new HealthReadinessService(probes).check()).resolves.toEqual({
      status: 'ok',
      dependencies: { postgres: 'ok', sqs: 'ok' },
    });
  });

  test('reports the failed dependency without masking a healthy one', async () => {
    const probes: ReadinessProbe[] = [
      { name: 'postgres', check: async () => undefined },
      { name: 'sqs', check: async () => { throw new Error('unreachable'); } },
    ];

    await expect(new HealthReadinessService(probes).check()).resolves.toEqual({
      status: 'error',
      dependencies: { postgres: 'ok', sqs: 'error' },
    });
  });
});
