import { readdir } from 'node:fs/promises';

import { MikroORM } from '@mikro-orm/postgresql';

import ormConfig from './infrastructure/persistence/mikro-orm.config.js';

/**
 * CP4 production migration runner.
 *
 * It lives under `src/` so that `nest build` emits `dist/migrate.js` and the production image can
 * apply the schema without any mounted source tree, `package.json` or MikroORM CLI.
 *
 * Guarantees required by the operational gate:
 * - no silent success: the runner fails when the migration directory ships no migration file, and
 *   when migrations remain pending after `up()`;
 * - idempotent: a second run on an up-to-date database applies nothing and exits 0;
 * - never destructive: `down` is never invoked here, so a restart can never roll the schema back.
 */
function log(event: string, payload: Record<string, unknown> = {}): void {
  process.stdout.write(`${JSON.stringify({ event, pid: process.pid, ...payload })}\n`);
}

function fail(message: string): never {
  process.stderr.write(`${JSON.stringify({ event: 'migration_runner_failed', pid: process.pid, message })}\n`);
  process.exit(1);
}

const orm = await MikroORM.init(ormConfig);

try {
  const migrationsPath = orm.config.get('migrations').path;
  if (migrationsPath === undefined) {
    fail('migrations.path is not configured');
  }

  let discovered: string[] = [];
  try {
    discovered = (await readdir(migrationsPath)).filter((entry) => /^Migration\d+\.(?:js|ts)$/.test(entry)).sort();
  } catch (error) {
    fail(`migrations directory is unreadable at ${migrationsPath}: ${error instanceof Error ? error.message : 'unknown error'}`);
  }

  if (discovered.length === 0) {
    fail(`no migration files were discovered at ${migrationsPath}`);
  }

  log('migration_runner_discovered', { path: migrationsPath, files: discovered });

  const migrator = orm.getMigrator();
  const applied = await migrator.up();
  const pending = await migrator.getPendingMigrations();
  const executed = await migrator.getExecutedMigrations();

  if (pending.length > 0) {
    fail(`migrations still pending after up(): ${pending.map((migration) => migration.name).join(', ')}`);
  }

  if (executed.length < discovered.length) {
    fail(`only ${executed.length} of ${discovered.length} discovered migrations are recorded as executed`);
  }

  log('migration_runner_completed', {
    applied: applied.map((migration) => migration.name),
    executed: executed.map((migration) => migration.name),
  });
} catch (error) {
  fail(error instanceof Error ? error.message : 'unknown error');
} finally {
  await orm.close(true);
}
