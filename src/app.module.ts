import { Module } from '@nestjs/common';

import { HealthModule } from './interfaces/http/health/health.module.js';

@Module({
  imports: [HealthModule],
})
export class AppModule {}
