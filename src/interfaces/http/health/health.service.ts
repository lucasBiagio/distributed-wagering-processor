import { Inject, Injectable } from '@nestjs/common';

export const READINESS_PROBES = 'READINESS_PROBES';

export type DependencyStatus = 'ok' | 'error';

export interface ReadinessProbe {
  readonly name: 'postgres' | 'sqs';
  check(): Promise<void>;
}

export interface ReadinessResult {
  readonly status: 'ok' | 'error';
  readonly dependencies: Record<ReadinessProbe['name'], DependencyStatus>;
}

@Injectable()
export class HealthService {
  live(): { status: 'ok' } {
    return { status: 'ok' };
  }
}

@Injectable()
export class HealthReadinessService {
  constructor(
    @Inject(READINESS_PROBES) private readonly probes: readonly ReadinessProbe[],
  ) {}

  async check(): Promise<ReadinessResult> {
    const results = await Promise.all(
      this.probes.map(async (probe) => {
        try {
          await probe.check();
          return [probe.name, 'ok'] as const;
        } catch {
          return [probe.name, 'error'] as const;
        }
      }),
    );

    const dependencies = Object.fromEntries(results) as ReadinessResult['dependencies'];
    const status = Object.values(dependencies).every((value) => value === 'ok')
      ? 'ok'
      : 'error';

    return { status, dependencies };
  }
}
