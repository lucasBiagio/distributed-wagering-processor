import { MikroORM } from '@mikro-orm/postgresql';

import { getAppConfig } from './infrastructure/config.js';
import { registerDatabaseMetricsCollector } from './infrastructure/observability/database-metrics-collector.js';
import {
  closeWorkerMetricsServer,
  startWorkerMetricsServer,
} from './infrastructure/observability/worker-metrics-server.js';
import { MikroOrmFinancialProcessing } from './infrastructure/persistence/mikro-orm-financial-processing.js';
import ormConfig from './infrastructure/persistence/mikro-orm.config.js';

/**
 * CP4 operational runner for the out-of-order reference sweep.
 *
 * It lives under `src/` on purpose: that is the only tree that `tsc --noEmit` checks and the
 * only tree that `nest build` emits, so the production image ships the very same compiled
 * artifact the API is built from (`dist/pending-reference-sweeper.js`) instead of a second,
 * unchecked code path copied from `scripts/`.
 *
 * Safety properties relied upon by the Gate 2 tests:
 * - a single in-process sweep at a time (`running` guard), so one instance never overlaps itself;
 * - several instances are safe because `reprocessPendingReferences` claims each pending row with
 *   `FOR UPDATE SKIP LOCKED` inside its own transaction;
 * - SIGINT/SIGTERM stop the loop between sweeps and the ORM connection pool is closed, so a
 *   shutdown can never leave a partially applied financial effect. The handlers are installed
 *   before the ORM is initialised: a signal received during startup must still shut the
 *   process down gracefully instead of hitting the default terminating handler.
 */
let stopping = false;
let running = false;
let wakeSleep: (() => void) | undefined;

function log(event: string, payload: Record<string, unknown> = {}): void {
  process.stdout.write(`${JSON.stringify({ event, pid: process.pid, ...payload })}\n`);
}

const stop = (signal: string) => (): void => {
  stopping = true;
  wakeSleep?.();
  log('pending_reference_sweeper_stopping', { signal });
};
process.once('SIGINT', stop('SIGINT'));
process.once('SIGTERM', stop('SIGTERM'));

const config = getAppConfig();
const orm = await MikroORM.init(ormConfig);
const processing = new MikroOrmFinancialProcessing(orm);
/**
 * Worker observability: own Prometheus listener. The sweeper has no SQS client, so only the
 * PostgreSQL collector is registered here — the DLQ gauges of this endpoint are therefore
 * absent rather than fabricated, and the scraper still gets them from the API and consumer
 * endpoints. This asymmetry is documented in README.md.
 */
registerDatabaseMetricsCollector(orm);
const metricsServer = await startWorkerMetricsServer('pending-reference-sweeper', config.workerMetricsPort);

async function sweep(): Promise<void> {
  if (running || stopping) {
    return;
  }
  running = true;
  try {
    const result = await processing.reprocessPendingReferences({
      now: new Date(),
      limit: config.pendingReferenceSweepBatchSize,
    });
    log('pending_reference_sweep_completed', { ...result });
  } catch (error) {
    process.stderr.write(`${JSON.stringify({
      event: 'pending_reference_sweep_failed',
      pid: process.pid,
      message: error instanceof Error ? error.message : 'unknown error',
    })}\n`);
  } finally {
    running = false;
  }
}

async function waitForNextSweep(intervalMs: number): Promise<void> {
  await Promise.race([
    Bun.sleep(intervalMs),
    new Promise<void>((resolve) => {
      wakeSleep = resolve;
    }),
  ]);
  wakeSleep = undefined;
}

log('pending_reference_sweeper_started', {
  intervalMs: config.pendingReferenceSweepIntervalMs,
  batchSize: config.pendingReferenceSweepBatchSize,
});

try {
  while (!stopping) {
    await sweep();
    if (!stopping) {
      await waitForNextSweep(config.pendingReferenceSweepIntervalMs);
    }
  }
} finally {
  await closeWorkerMetricsServer(metricsServer);
  await orm.close(true);
  log('pending_reference_sweeper_stopped');
}
