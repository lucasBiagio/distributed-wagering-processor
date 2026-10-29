import { Module } from '@nestjs/common';

import { InfrastructureModule } from '../../../infrastructure/infrastructure.module.js';
import { FinancialController } from './financial.controller.js';

@Module({
  imports: [InfrastructureModule],
  controllers: [FinancialController],
})
export class FinancialHttpModule {}
