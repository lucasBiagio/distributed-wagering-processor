import { defineConfig } from '@mikro-orm/postgresql';

import { financialEntities } from './financial-entities.js';

export default defineConfig({
  clientUrl: process.env.DATABASE_URL ?? 'postgresql://wagering:wagering@localhost:5432/wagering',
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
});
