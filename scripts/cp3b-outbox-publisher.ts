import { SQSClient } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';

import { getAppConfig } from '../src/infrastructure/config.js';
import { SqsOutboxPublisher } from '../src/infrastructure/messaging/sqs-outbox-publisher.js';
import ormConfig from '../src/infrastructure/persistence/mikro-orm.config.js';

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
let stopping = false;

const stop = (): void => {
  stopping = true;
  publisher.stopAcceptingClaims();
};
process.once('SIGINT', stop);
process.once('SIGTERM', stop);

try {
  const result = await publisher.drain({
    batchSize: optionalPositiveInteger('OUTBOX_PUBLISHER_BATCH_SIZE'),
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
  await sqs.destroy();
  await orm.close(true);
}
