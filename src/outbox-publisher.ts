import { SQSClient } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';

import { getAppConfig } from './infrastructure/config.js';
import { registerDatabaseMetricsCollector } from './infrastructure/observability/database-metrics-collector.js';
import { registerSqsMetricsCollector } from './infrastructure/observability/sqs-metrics-collector.js';
import {
  closeWorkerMetricsServer,
  startWorkerMetricsServer,
} from './infrastructure/observability/worker-metrics-server.js';
import { SqsOutboxPublisher } from './infrastructure/messaging/sqs-outbox-publisher.js';
import ormConfig from './infrastructure/persistence/mikro-orm.config.js';

/**
 * CP3B outbox publisher entrypoint.
 *
 * Moved from `scripts/` into `src/` so the production image runs the compiled
 * `dist/outbox-publisher.js` emitted by `nest build`. `scripts/cp3b-outbox-publisher.ts` is kept as
 * a thin wrapper importing this module, so claims, lease/fencing, retry/backoff, crash recovery
 * and FIFO ordering keep running through one single implementation — including the deterministic
 * `CP3B_TEST_CRASH_AFTER_CLAIM` / `CP3B_TEST_CRASH_AFTER_SEND` hooks used by the existing tests.
 */
function optionalPositiveInteger(name: string): number | undefined {
  const value = process.env[name];
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer when provided`);
  }
  return parsed;
}

const config = getAppConfig();
const orm = await MikroORM.init(ormConfig);
const sqs = new SQSClient({
  region: config.awsRegion,
  endpoint: config.sqsEndpoint,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
  },
});
const publisher = new SqsOutboxPublisher(sqs, orm);
/**
 * Worker observability: own Prometheus listener (see worker-metrics-server.ts). The
 * publisher's process-local counters are only visible here; outbox lag and DLQ depth are
 * recomputed from PostgreSQL/SQS, so they agree with every other endpoint.
 */
registerDatabaseMetricsCollector(orm);
registerSqsMetricsCollector(sqs);
const metricsServer = await startWorkerMetricsServer('outbox-publisher', config.workerMetricsPort);
let stopping = false;

const stop = (): void => {
  stopping = true;
  publisher.stopAcceptingClaims();
};
process.once('SIGINT', stop);
process.once('SIGTERM', stop);

const batchSize = optionalPositiveInteger('OUTBOX_PUBLISHER_BATCH_SIZE');

try {
  // `batchSize` is omitted instead of passed as `undefined` because `src/` is typechecked with
  // `exactOptionalPropertyTypes`; the publisher falls back to its configured default either way.
  const result = await publisher.drain({
    ...(batchSize === undefined ? {} : { batchSize }),
    stopWhenIdle: process.env.OUTBOX_PUBLISHER_STOP_WHEN_IDLE === 'true',
    afterClaim: () => {
      if (process.env.CP3B_TEST_CRASH_AFTER_CLAIM === 'true') {
        process.exit(75);
      }
    },
    afterSendBeforeConfirm: () => {
      if (process.env.CP3B_TEST_CRASH_AFTER_SEND === 'true') {
        process.exit(76);
      }
    },
  });
  process.stdout.write(`${JSON.stringify({ event: 'outbox_publisher_stopped', stopping, ...result })}\n`);
} finally {
  await closeWorkerMetricsServer(metricsServer);
  await sqs.destroy();
  await orm.close(true);
}
