import 'reflect-metadata';

import { NestFactory } from '@nestjs/core';

import { SqsWagerConsumer } from '../src/infrastructure/messaging/sqs-wager-consumer.js';
import { SqsWorkerModule } from '../src/interfaces/worker/sqs-worker.module.js';

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
  let shuttingDown = false;
  const shutdown = async (): Promise<void> => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    await consumer.stop();
    await context.close();
  };
  process.once('SIGTERM', () => { void shutdown(); });
  process.once('SIGINT', () => { void shutdown(); });

  const crashMessageId = process.env.CP3A_TEST_CRASH_AFTER_COMMIT_BEFORE_ACK;
  try {
    await consumer.run({
      maxAcknowledgements: optionalPositiveInteger('SQS_CONSUMER_EXIT_AFTER_ACKS'),
      stopWhenIdle: process.env.SQS_CONSUMER_STOP_WHEN_IDLE === 'true',
      afterCommitBeforeAck: crashMessageId === undefined
        ? undefined
        : (messageId) => {
          if (messageId === crashMessageId) {
            // Test-only process boundary used to prove commit-before-ACK redelivery.
            process.exit(86);
          }
        },
    });
  } finally {
    await shutdown();
  }
}

void bootstrap();
