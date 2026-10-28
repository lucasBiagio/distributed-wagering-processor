import { Module } from '@nestjs/common';

import { FinancialHttpModule } from './interfaces/http/financial/financial.module.js';
import { HealthModule } from './interfaces/http/health/health.module.js';

@Module({
  imports: [HealthModule, FinancialHttpModule],
})
export class AppModule {}
