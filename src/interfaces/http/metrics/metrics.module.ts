import { Module } from '@nestjs/common';

import { InfrastructureModule } from '../../../infrastructure/infrastructure.module.js';
import { MetricsController } from './metrics.controller.js';

@Module({
  imports: [InfrastructureModule],
  controllers: [MetricsController],
})
export class MetricsHttpModule {}
