export interface AppConfig {
  readonly port: number;
  readonly databaseUrl: string;
  readonly awsRegion: string;
  readonly sqsEndpoint: string;
  readonly wagerTransactionsQueueName: string;
  readonly wagerTransactionsQueueUrl?: string;
  readonly sqsConsumerConcurrency: number;
  readonly sqsConsumerVisibilityTimeoutSeconds: number;
}

function required(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function port(value: string): number {
  const parsed = Number.parseInt(value, 10);

  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
    throw new Error('PORT must be an integer between 1 and 65535');
  }

  return parsed;
}

function positiveInteger(name: string, value: string, maximum: number): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new Error(`${name} must be an integer between 1 and ${maximum}`);
  }
  return parsed;
}

export function getAppConfig(): AppConfig {
  return {
    port: port(required('PORT', '3000')),
    databaseUrl: required('DATABASE_URL'),
    awsRegion: required('AWS_REGION', 'us-east-1'),
    sqsEndpoint: required('SQS_ENDPOINT'),
    wagerTransactionsQueueName: required('WAGER_TRANSACTIONS_QUEUE_NAME', 'wager-transactions.fifo'),
    ...(process.env.WAGER_TRANSACTIONS_QUEUE_URL === undefined
      ? {}
      : { wagerTransactionsQueueUrl: required('WAGER_TRANSACTIONS_QUEUE_URL') }),
    sqsConsumerConcurrency: positiveInteger('SQS_CONSUMER_CONCURRENCY', required('SQS_CONSUMER_CONCURRENCY', '4'), 10),
    sqsConsumerVisibilityTimeoutSeconds: positiveInteger(
      'SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS',
      required('SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS', '5'),
      43_200,
    ),
  };
}
