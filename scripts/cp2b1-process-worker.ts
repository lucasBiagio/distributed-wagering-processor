import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import type { ProcessBetCommand } from '../src/application/financial-contracts.js';
import { MikroOrmFinancialProcessing } from '../src/infrastructure/persistence/mikro-orm-financial-processing.js';
import config from '../src/infrastructure/persistence/mikro-orm.config.js';

const serialized = process.env.CP2B1_COMMAND;
if (!serialized) {
  throw new Error('CP2B1_COMMAND is required');
}

const orm = await MikroORM.init<PostgreSqlDriver>(config);
try {
  const result = await new MikroOrmFinancialProcessing(orm).processBet(JSON.parse(serialized) as ProcessBetCommand);
  process.stdout.write(`${JSON.stringify(result)}\n`);
} finally {
  await orm.close(true);
}
