import { Module } from '@nestjs/common';

import { SqsModule } from './messaging/sqs.module.js';
import { PostgresReadinessProbe, SqsReadinessProbe } from './health/readiness-probes.js';
import { PersistenceModule } from './persistence/persistence.module.js';

@Module({
  imports: [PersistenceModule, SqsModule],
  providers: [
    PostgresReadinessProbe,
    SqsReadinessProbe,
    { provide: 'POSTGRES_READINESS_PROBE', useExisting: PostgresReadinessProbe },
    { provide: 'SQS_READINESS_PROBE', useExisting: SqsReadinessProbe },
  ],
  exports: ['POSTGRES_READINESS_PROBE', 'SQS_READINESS_PROBE'],
})
export class InfrastructureModule {}
