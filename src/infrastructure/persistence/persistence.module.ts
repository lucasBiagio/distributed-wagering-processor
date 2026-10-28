import { Module } from '@nestjs/common';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { getAppConfig } from '../config.js';
import { financialEntities } from './financial-entities.js';

@Module({
  imports: [
    MikroOrmModule.forRoot({
      driver: PostgreSqlDriver,
      clientUrl: getAppConfig().databaseUrl,
      entities: financialEntities,
      discovery: {
        warnWhenNoEntities: false,
      },
      migrations: {
        path: 'dist/infrastructure/persistence/migrations',
        pathTs: 'src/infrastructure/persistence/migrations',
        transactional: true,
        allOrNothing: true,
      },
    }),
  ],
})
export class PersistenceModule {}
