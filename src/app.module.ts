import { Module } from '@nestjs/common';

import { FinancialHttpModule } from './interfaces/http/financial/financial.module.js';
import { HealthModule } from './interfaces/http/health/health.module.js';
import { MetricsHttpModule } from './interfaces/http/metrics/metrics.module.js';

@Module({
  imports: [HealthModule, MetricsHttpModule, FinancialHttpModule],
})
export class AppModule {}
