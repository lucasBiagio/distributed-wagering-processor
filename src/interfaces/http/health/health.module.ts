import { Module } from '@nestjs/common';

import { InfrastructureModule } from '../../../infrastructure/infrastructure.module.js';
import { HealthController } from './health.controller.js';
import {
  HealthReadinessService,
  HealthService,
  READINESS_PROBES,
  type ReadinessProbe,
} from './health.service.js';

@Module({
  imports: [InfrastructureModule],
  controllers: [HealthController],
  providers: [
    HealthService,
    HealthReadinessService,
    {
      provide: READINESS_PROBES,
      inject: ['POSTGRES_READINESS_PROBE', 'SQS_READINESS_PROBE'],
      useFactory: (
        postgres: ReadinessProbe,
        sqs: ReadinessProbe,
      ): readonly ReadinessProbe[] => [postgres, sqs],
    },
  ],
})
export class HealthModule {}
