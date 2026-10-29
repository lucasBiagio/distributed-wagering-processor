import {
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  type QueueAttributeName,
  type SQSClient,
} from '@aws-sdk/client-sqs';

import { getAppConfig } from '../config.js';
import {
  dlqMessagesApproximate,
  metricsRegistry,
  sqsQueueMessagesApproximate,
} from './wagering-metrics.js';

/**
 * Only the three approximate depth attributes are requested. They are typed as
 * `QueueAttributeName` so a typo cannot reach LocalStack/SQS as a silently ignored name.
 */
const ATTRIBUTE_BY_STATE: ReadonlyArray<{ readonly state: string; readonly attribute: QueueAttributeName }> = [
  { state: 'visible', attribute: 'ApproximateNumberOfMessages' },
  { state: 'not_visible', attribute: 'ApproximateNumberOfMessagesNotVisible' },
  { state: 'delayed', attribute: 'ApproximateNumberOfMessagesDelayed' },
];

const COLLECTOR_TIMEOUT_MS = 2_000;

interface QueueTarget {
  /** Bounded, human-chosen role label. Never the queue URL or ARN. */
  readonly role: string;
  readonly name: string;
  readonly url?: string;
  readonly deadLetter: boolean;
}

/**
 * Reads the real SQS queue attributes, including the dead-letter queue depth.
 *
 * SQS documents `ApproximateNumberOfMessages*` as approximations: for a FIFO queue the value
 * converges quickly but is not transactional, and a message in flight can be counted on
 * either side of the boundary. That is why these gauges carry `_approximate` in the name and
 * the `approximate_sqs` scope. The alternative — counting "dead" messages from application
 * logs — would be a fabrication, because a message only reaches the DLQ after SQS exhausts
 * the redrive policy, which this application never observes.
 */
export class SqsMetricsCollector {
  private readonly urlByRole = new Map<string, string>();

  public constructor(private readonly client: SQSClient) {}

  public async collect(): Promise<void> {
    const config = getAppConfig();
    const targets: readonly QueueTarget[] = [
      {
        role: 'wager_transactions',
        name: config.wagerTransactionsQueueName,
        ...(config.wagerTransactionsQueueUrl === undefined ? {} : { url: config.wagerTransactionsQueueUrl }),
        deadLetter: false,
      },
      {
        role: 'wager_transactions_dlq',
        name: config.wagerTransactionsDlqQueueName,
        ...(config.wagerTransactionsDlqQueueUrl === undefined ? {} : { url: config.wagerTransactionsDlqQueueUrl }),
        deadLetter: true,
      },
      {
        role: 'wager_events',
        name: config.wagerEventsQueueName,
        ...(config.wagerEventsQueueUrl === undefined ? {} : { url: config.wagerEventsQueueUrl }),
        deadLetter: false,
      },
    ];

    sqsQueueMessagesApproximate.reset();
    dlqMessagesApproximate.reset();

    let failures = 0;
    for (const target of targets) {
      try {
        await this.collectQueue(target);
      } catch {
        /**
         * A queue that does not exist in this environment (for example a stack without a DLQ)
         * must not blank the whole endpoint: its series are simply absent and the failure is
         * surfaced by wagering_metrics_collection_errors_total.
         */
        failures += 1;
      }
    }
    if (failures > 0) {
      throw new Error(`${failures} SQS queue attribute reads failed`);
    }
  }

  private async collectQueue(target: QueueTarget): Promise<void> {
    const url = await this.resolveUrl(target);
    const attributes = await this.withTimeout(this.client.send(new GetQueueAttributesCommand({
      QueueUrl: url,
      AttributeNames: ATTRIBUTE_BY_STATE.map((entry) => entry.attribute),
    })));

    for (const entry of ATTRIBUTE_BY_STATE) {
      const raw = attributes.Attributes?.[entry.attribute];
      if (raw === undefined) {
        continue;
      }
      const value = Number.parseInt(raw, 10);
      if (!Number.isFinite(value)) {
        continue;
      }
      sqsQueueMessagesApproximate.set({ queue: target.role, state: entry.state }, value);
      if (target.deadLetter && entry.state === 'visible') {
        dlqMessagesApproximate.set({ queue: target.role }, value);
      }
    }
  }

  private async resolveUrl(target: QueueTarget): Promise<string> {
    if (target.url !== undefined) {
      return target.url;
    }
    const cached = this.urlByRole.get(target.role);
    if (cached !== undefined) {
      return cached;
    }
    const resolved = await this.withTimeout(this.client.send(new GetQueueUrlCommand({ QueueName: target.name })));
    if (!resolved.QueueUrl) {
      throw new Error(`SQS queue URL was not resolved for role ${target.role}`);
    }
    this.urlByRole.set(target.role, resolved.QueueUrl);
    return resolved.QueueUrl;
  }

  /** A scrape must never hang on infrastructure: the collector is bounded, not retried. */
  private async withTimeout<T>(promise: Promise<T>): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        promise,
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => reject(new Error('SQS metric collection timed out')), COLLECTOR_TIMEOUT_MS);
        }),
      ]);
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
    }
  }
}

let sqsCollectorRegistered = false;

/** Registers the SQS collector once per process. */
export function registerSqsMetricsCollector(client: SQSClient): void {
  if (sqsCollectorRegistered) {
    return;
  }
  sqsCollectorRegistered = true;
  const collector = new SqsMetricsCollector(client);
  metricsRegistry.addCollector('sqs', () => collector.collect());
}
