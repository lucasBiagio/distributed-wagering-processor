import 'reflect-metadata';

import { NestFactory } from '@nestjs/core';
import { MikroORM } from '@mikro-orm/core';
import { type SQSClient } from '@aws-sdk/client-sqs';

import { getAppConfig } from './infrastructure/config.js';
import { SQS_CLIENT } from './infrastructure/messaging/sqs.module.js';
import { registerDatabaseMetricsCollector } from './infrastructure/observability/database-metrics-collector.js';
import { registerSqsMetricsCollector } from './infrastructure/observability/sqs-metrics-collector.js';
import {
  closeWorkerMetricsServer,
  startWorkerMetricsServer,
} from './infrastructure/observability/worker-metrics-server.js';
import { SqsWagerConsumer } from './infrastructure/messaging/sqs-wager-consumer.js';
import { SqsWorkerModule } from './interfaces/worker/sqs-worker.module.js';

/**
 * CP3A SQS wager consumer entrypoint.
 *
 * Moved from `scripts/` into `src/` so the production image runs the compiled
 * `dist/sqs-consumer.js` emitted by `nest build` instead of an unchecked, unpackaged source file.
 * `scripts/cp3a-sqs-consumer.ts` is kept as a thin wrapper that imports this very module, so the
 * existing integration tests keep spawning the same single implementation (ACK only after commit,
 * crash recovery, DLQ and SIGTERM semantics are untouched, including the deterministic
 * `CP3A_TEST_CRASH_AFTER_COMMIT_BEFORE_ACK` hook the tests rely on).
 */
function optionalPositiveInteger(name: string): number | undefined {
  const value = process.env[name];
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

async function bootstrap(): Promise<void> {
  const context = await NestFactory.createApplicationContext(SqsWorkerModule, { bufferLogs: true });
  const consumer = context.get(SqsWagerConsumer);
  /**
   * Worker observability: this process exposes its own Prometheus endpoint when
   * WORKER_METRICS_PORT is configured. Its process-local counters cannot be read through the
   * API's /metrics, so the scraper targets this listener directly; global gauges are
   * recomputed here from PostgreSQL and SQS and therefore match every other instance.
   */
  const config = getAppConfig();
  registerDatabaseMetricsCollector(context.get(MikroORM));
  registerSqsMetricsCollector(context.get<SQSClient>(SQS_CLIENT));
  const metricsServer = await startWorkerMetricsServer('sqs-consumer', config.workerMetricsPort);
  let shuttingDown = false;
  const shutdown = async (): Promise<void> => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    await closeWorkerMetricsServer(metricsServer);
    await consumer.stop();
    await context.close();
  };
  process.once('SIGTERM', () => { void shutdown(); });
  process.once('SIGINT', () => { void shutdown(); });

  const crashMessageId = process.env.CP3A_TEST_CRASH_AFTER_COMMIT_BEFORE_ACK;
  const maxAcknowledgements = optionalPositiveInteger('SQS_CONSUMER_EXIT_AFTER_ACKS');
  try {
    // Optional properties are omitted rather than passed as `undefined`: `src/` is typechecked with
    // `exactOptionalPropertyTypes`, and the runtime behaviour is identical for the consumer.
    await consumer.run({
      ...(maxAcknowledgements === undefined ? {} : { maxAcknowledgements }),
      stopWhenIdle: process.env.SQS_CONSUMER_STOP_WHEN_IDLE === 'true',
      ...(crashMessageId === undefined
        ? {}
        : {
          afterCommitBeforeAck: (messageId: string): void => {
            if (messageId === crashMessageId) {
              // Test-only process boundary used to prove commit-before-ACK redelivery.
              process.exit(86);
            }
          },
        }),
    });
  } finally {
    await shutdown();
  }
}

void bootstrap();
