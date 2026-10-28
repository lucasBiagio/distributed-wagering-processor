import { Module } from '@nestjs/common';

import { InfrastructureModule } from '../../../infrastructure/infrastructure.module.js';
import { MikroOrmFinancialProcessing } from '../../../infrastructure/persistence/mikro-orm-financial-processing.js';
import { FinancialController } from './financial.controller.js';

@Module({
  imports: [InfrastructureModule],
  controllers: [FinancialController],
  providers: [MikroOrmFinancialProcessing],
})
export class FinancialHttpModule {}
