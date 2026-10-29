import { fileURLToPath } from 'node:url';

import { defineConfig } from '@mikro-orm/postgresql';

import { financialEntities } from './financial-entities.js';

/**
 * Migrations must be discoverable from BOTH code trees:
 *
 * - sources (`src/.../migrations/*.ts`) when Bun runs the TypeScript directly (dev and tests);
 * - compiled output (`dist/.../migrations/*.js`) inside the production image, which ships no `src/`.
 *
 * The previous configuration declared `path` relative to the process working directory and a
 * separate `pathTs`. MikroORM picks `pathTs` whenever it detects a TypeScript runtime, and Bun is
 * always detected as one, so inside the production image discovery resolved to the non-existent
 * `src/infrastructure/persistence/migrations` and `migrator.up()` silently reported zero
 * migrations on an empty database.
 *
 * Resolving the directory from this module's own URL removes both failure modes: the path is
 * absolute (independent of the working directory) and always points at the migrations that sit
 * next to the configuration actually being executed, compiled or not.
 */
const migrationsPath = fileURLToPath(new URL('./migrations', import.meta.url));

export default defineConfig({
  clientUrl: process.env.DATABASE_URL ?? 'postgresql://wagering:wagering@localhost:5432/wagering',
  entities: financialEntities,
  discovery: {
    warnWhenNoEntities: false,
  },
  migrations: {
    path: migrationsPath,
    pathTs: migrationsPath,
    transactional: true,
    allOrNothing: true,
  },
});
