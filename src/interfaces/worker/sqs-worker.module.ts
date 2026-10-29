import { Module } from '@nestjs/common';

import { InfrastructureModule } from '../../infrastructure/infrastructure.module.js';
import { SqsWagerConsumer } from '../../infrastructure/messaging/sqs-wager-consumer.js';

@Module({
  imports: [InfrastructureModule],
  providers: [SqsWagerConsumer],
  exports: [SqsWagerConsumer],
})
export class SqsWorkerModule {}
