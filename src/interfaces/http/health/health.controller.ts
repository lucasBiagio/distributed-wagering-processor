import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';

import {
  HealthReadinessService,
  HealthService,
  type ReadinessResult,
} from './health.service.js';

@Controller('health')
export class HealthController {
  constructor(
    private readonly healthService: HealthService,
    private readonly readinessService: HealthReadinessService,
  ) {}

  @Get('live')
  live(): { status: 'ok' } {
    return this.healthService.live();
  }

  @Get('ready')
  async ready(): Promise<ReadinessResult> {
    const result = await this.readinessService.check();

    if (result.status === 'error') {
      throw new ServiceUnavailableException(result);
    }

    return result;
  }
}
