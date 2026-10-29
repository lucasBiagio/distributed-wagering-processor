export interface AppConfig {
  readonly port: number;
  readonly databaseUrl: string;
  readonly awsRegion: string;
  readonly sqsEndpoint: string;
  readonly wagerTransactionsQueueName: string;
  readonly wagerTransactionsQueueUrl?: string;
  readonly wagerTransactionsDlqQueueName: string;
  readonly wagerTransactionsDlqQueueUrl?: string;
  readonly wagerEventsQueueName: string;
  readonly wagerEventsQueueUrl?: string;
  /**
   * Port of the worker metrics listener. Workers are separate processes, so each one exposes
   * its own Prometheus endpoint instead of pretending the API can see its in-memory counters.
   * Unset means the listener is not started.
   */
  readonly workerMetricsPort?: number;
  readonly outboxPublisherBatchSize: number;
  readonly outboxPublisherLeaseMs: number;
  readonly outboxPublisherRetryBaseMs: number;
  readonly outboxPublisherRetryMaxMs: number;
  readonly sqsConsumerConcurrency: number;
  readonly sqsConsumerVisibilityTimeoutSeconds: number;
  readonly pendingReferenceSweepIntervalMs: number;
  readonly pendingReferenceSweepBatchSize: number;
}

function required(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function port(value: string): number {
  if (!/^\d+$/.test(value)) {
    throw new Error('PORT must be an integer between 1 and 65535');
  }

  const parsed = Number(value);
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
    wagerTransactionsDlqQueueName: required('WAGER_TRANSACTIONS_DLQ_QUEUE_NAME', 'wager-transactions-dlq.fifo'),
    ...(process.env.WAGER_TRANSACTIONS_DLQ_QUEUE_URL === undefined
      ? {}
      : { wagerTransactionsDlqQueueUrl: required('WAGER_TRANSACTIONS_DLQ_QUEUE_URL') }),
    ...(process.env.WORKER_METRICS_PORT === undefined
      ? {}
      : { workerMetricsPort: port(required('WORKER_METRICS_PORT')) }),
    wagerEventsQueueName: required('WAGER_EVENTS_QUEUE_NAME', 'wager-events.fifo'),
    ...(process.env.WAGER_EVENTS_QUEUE_URL === undefined
      ? {}
      : { wagerEventsQueueUrl: required('WAGER_EVENTS_QUEUE_URL') }),
    outboxPublisherBatchSize: positiveInteger('OUTBOX_PUBLISHER_BATCH_SIZE', required('OUTBOX_PUBLISHER_BATCH_SIZE', '20'), 100),
    outboxPublisherLeaseMs: positiveInteger('OUTBOX_PUBLISHER_LEASE_MS', required('OUTBOX_PUBLISHER_LEASE_MS', '30000'), 3_600_000),
    outboxPublisherRetryBaseMs: positiveInteger('OUTBOX_PUBLISHER_RETRY_BASE_MS', required('OUTBOX_PUBLISHER_RETRY_BASE_MS', '1000'), 3_600_000),
    outboxPublisherRetryMaxMs: positiveInteger('OUTBOX_PUBLISHER_RETRY_MAX_MS', required('OUTBOX_PUBLISHER_RETRY_MAX_MS', '60000'), 86_400_000),
    sqsConsumerConcurrency: positiveInteger('SQS_CONSUMER_CONCURRENCY', required('SQS_CONSUMER_CONCURRENCY', '4'), 10),
    sqsConsumerVisibilityTimeoutSeconds: positiveInteger(
      'SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS',
      required('SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS', '5'),
      43_200,
    ),
    pendingReferenceSweepIntervalMs: positiveInteger(
      'PENDING_REFERENCE_SWEEP_INTERVAL_MS',
      required('PENDING_REFERENCE_SWEEP_INTERVAL_MS', '5000'),
      3_600_000,
    ),
    pendingReferenceSweepBatchSize: positiveInteger(
      'PENDING_REFERENCE_SWEEP_BATCH_SIZE',
      required('PENDING_REFERENCE_SWEEP_BATCH_SIZE', '50'),
      1000,
    ),
  };
}
