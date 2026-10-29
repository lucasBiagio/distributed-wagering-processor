import { createHash } from 'node:crypto';

import {
  DeleteMessageCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  type Message,
  type ReceiveMessageCommandOutput,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { Inject, Injectable, type OnApplicationShutdown } from '@nestjs/common';

import {
  FinancialConflictError,
  FinancialNotFoundError,
  InboxPayloadConflictError,
  type ProcessWagerCommand,
  type WagerKindInput,
} from '../../application/financial-contracts.js';
import { DomainRuleError } from '../../domain/domain-error.js';
import { MoneyValidationError } from '../../domain/money.js';
import { canonicalJson } from '../persistence/mikro-orm-financial-processing.js';
import { getAppConfig } from '../config.js';
import { MikroOrmFinancialProcessing } from '../persistence/mikro-orm-financial-processing.js';
import { SQS_CLIENT } from './sqs.module.js';

export const WAGER_TRANSACTION_CONSUMER_NAME = 'wager-transaction-requested-v1';

interface WagerTransactionRequestedEnvelope {
  readonly messageId: string;
  readonly type: 'WagerTransactionRequested';
  readonly occurredAt: string;
  readonly data: ProcessWagerCommand;
}

export interface SqsWagerConsumerRunOptions {
  /** Test-only deterministic interruption point after PostgreSQL commit, before SQS DeleteMessage. */
  readonly afterCommitBeforeAck?: (messageId: string) => Promise<void> | void;
  /** Lets real-process integration tests stop after a bounded number of acknowledged messages. */
  readonly maxAcknowledgements?: number;
  /** Lets an explicitly bounded test worker exit after a long-poll returns no messages. */
  readonly stopWhenIdle?: boolean;
}

function structuredLog(event: string, fields: Record<string, string | number | boolean | undefined> = {}): void {
  // Message bodies and monetary payloads are intentionally excluded from logs.
  console.info(JSON.stringify({ event, ...fields }));
}

function object(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new DomainRuleError('INVALID_TRANSACTION', `${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function string(value: unknown, label: string): string {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new DomainRuleError('INVALID_TRANSACTION', `${label} must be a non-blank string`);
  }
  return value;
}

function exactKeys(value: Record<string, unknown>, allowed: readonly string[], label: string): void {
  for (const key of Object.keys(value)) {
    if (!allowed.includes(key)) {
      throw new DomainRuleError('INVALID_TRANSACTION', `${label}.${key} is not allowed`);
    }
  }
}

function parseEnvelope(body: string): WagerTransactionRequestedEnvelope {
  let decoded: unknown;
  try {
    decoded = JSON.parse(body);
  } catch {
    throw new DomainRuleError('INVALID_TRANSACTION', 'SQS body is not valid JSON');
  }

  const envelope = object(decoded, 'envelope');
  exactKeys(envelope, ['messageId', 'type', 'occurredAt', 'data'], 'envelope');
  const messageId = string(envelope.messageId, 'messageId');
  if (envelope.type !== 'WagerTransactionRequested') {
    throw new DomainRuleError('INVALID_TRANSACTION', 'Unsupported SQS envelope type');
  }
  const occurredAt = string(envelope.occurredAt, 'occurredAt');
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/.test(occurredAt) || Number.isNaN(Date.parse(occurredAt))) {
    throw new DomainRuleError('INVALID_TRANSACTION', 'occurredAt must be ISO-8601');
  }

  const data = object(envelope.data, 'data');
  exactKeys(data, [
    'providerId', 'externalTransactionId', 'idempotencyKey', 'playerId', 'walletId', 'roundId', 'gameId',
    'kind', 'money', 'referenceExternalTransactionId',
  ], 'data');
  const money = object(data.money, 'data.money');
  exactKeys(money, ['amount', 'currency'], 'data.money');
  const kind = string(data.kind, 'data.kind');
  if (!['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'].includes(kind)) {
    throw new DomainRuleError('INVALID_TRANSACTION', 'Unsupported wager kind');
  }

  return {
    messageId,
    type: 'WagerTransactionRequested',
    occurredAt,
    data: {
      providerId: string(data.providerId, 'data.providerId'),
      externalTransactionId: string(data.externalTransactionId, 'data.externalTransactionId'),
      idempotencyKey: string(data.idempotencyKey, 'data.idempotencyKey'),
      playerId: string(data.playerId, 'data.playerId'),
      walletId: string(data.walletId, 'data.walletId'),
      roundId: string(data.roundId, 'data.roundId'),
      gameId: string(data.gameId, 'data.gameId'),
      kind: kind as WagerKindInput,
      money: {
        amount: string(money.amount, 'data.money.amount'),
        currency: string(money.currency, 'data.money.currency'),
      },
      ...(data.referenceExternalTransactionId === undefined
        ? {}
        : { referenceExternalTransactionId: string(data.referenceExternalTransactionId, 'data.referenceExternalTransactionId') }),
      correlationId: messageId,
      causationId: messageId,
    },
  };
}

function envelopeHash(envelope: WagerTransactionRequestedEnvelope): string {
  return createHash('sha256').update(canonicalJson(envelope)).digest('hex');
}

function permanentError(error: unknown): boolean {
  return error instanceof DomainRuleError
    || error instanceof MoneyValidationError
    || error instanceof FinancialNotFoundError
    || error instanceof FinancialConflictError;
}

@Injectable()
export class SqsWagerConsumer implements OnApplicationShutdown {
  private accepting = false;
  private pollAbortController: AbortController | undefined;
  private readonly inFlight = new Set<Promise<void>>();

  public constructor(
    @Inject(SQS_CLIENT) private readonly client: SQSClient,
    private readonly processing: MikroOrmFinancialProcessing,
  ) {}

  public async run(options: SqsWagerConsumerRunOptions = {}): Promise<void> {
    const config = getAppConfig();
    const queueUrl = config.wagerTransactionsQueueUrl ?? (await this.client.send(new GetQueueUrlCommand({
      QueueName: config.wagerTransactionsQueueName,
    }))).QueueUrl;
    if (!queueUrl) {
      throw new Error(`SQS queue URL was not resolved for ${config.wagerTransactionsQueueName}`);
    }

    this.accepting = true;
    let acknowledgements = 0;
    while (this.accepting && (options.maxAcknowledgements === undefined || acknowledgements < options.maxAcknowledgements)) {
      const capacity = config.sqsConsumerConcurrency - this.inFlight.size;
      if (capacity <= 0) {
        await Promise.race(this.inFlight);
        continue;
      }

      this.pollAbortController = new AbortController();
      let received: ReceiveMessageCommandOutput;
      try {
        received = await this.client.send(new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: Math.min(10, capacity),
          WaitTimeSeconds: 20,
          VisibilityTimeout: config.sqsConsumerVisibilityTimeoutSeconds,
          MessageAttributeNames: ['All'],
        }), { abortSignal: this.pollAbortController.signal });
      } catch (error) {
        if (!this.accepting && error instanceof Error && error.name === 'AbortError') {
          break;
        }
        throw error;
      } finally {
        this.pollAbortController = undefined;
      }
      const messages = received.Messages ?? [];
      if (messages.length === 0) {
        if (options.stopWhenIdle) {
          break;
        }
        continue;
      }

      for (const message of messages) {
        if (!this.accepting || (options.maxAcknowledgements !== undefined && acknowledgements >= options.maxAcknowledgements)) {
          break;
        }
        const delivery = this.consume(queueUrl, message, options).then((acknowledged) => {
          if (acknowledged) {
            acknowledgements += 1;
          }
        });
        this.inFlight.add(delivery);
        void delivery.finally(() => this.inFlight.delete(delivery));
      }
    }

    await Promise.all(this.inFlight);
  }

  public async stop(): Promise<void> {
    this.accepting = false;
    this.pollAbortController?.abort();
    await Promise.all(this.inFlight);
  }

  public async onApplicationShutdown(): Promise<void> {
    await this.stop();
  }

  private async consume(
    queueUrl: string,
    message: Message,
    options: SqsWagerConsumerRunOptions,
  ): Promise<boolean> {
    if (!message.Body || !message.ReceiptHandle) {
      structuredLog('sqs_delivery_invalid_transport', { sqsMessageId: message.MessageId });
      return false;
    }

    let envelope: WagerTransactionRequestedEnvelope;
    try {
      envelope = parseEnvelope(message.Body);
    } catch (error) {
      structuredLog('sqs_delivery_permanent_invalid', { sqsMessageId: message.MessageId });
      return false;
    }

    structuredLog('sqs_delivery_received', {
      messageId: envelope.messageId,
      walletId: envelope.data.walletId,
      providerId: envelope.data.providerId,
    });

    try {
      const result = await this.processing.processWagerWithInbox(envelope.data, {
        consumerName: WAGER_TRANSACTION_CONSUMER_NAME,
        messageId: envelope.messageId,
        payloadHash: envelopeHash(envelope),
      });
      structuredLog('sqs_delivery_committed', {
        messageId: envelope.messageId,
        transactionId: result.transactionId,
        walletId: result.walletId,
        providerId: result.providerId,
        idempotentReplay: result.idempotentReplay,
      });
      await options.afterCommitBeforeAck?.(envelope.messageId);
      await this.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }));
      structuredLog('sqs_delivery_acknowledged', {
        messageId: envelope.messageId,
        transactionId: result.transactionId,
      });
      return true;
    } catch (error) {
      if (error instanceof InboxPayloadConflictError) {
        // A changed payload under the same durable logical identity is permanent, but it is
        // deliberately left unacknowledged so the SQS redrive policy can preserve it in DLQ.
        structuredLog('sqs_delivery_inbox_conflict', {
          messageId: envelope.messageId,
          reason: 'inbox_payload_conflict',
        });
        return false;
      }
      if (permanentError(error)) {
        // Leave the message unacknowledged: SQS redrive policy owns bounded retries and DLQ.
        structuredLog('sqs_delivery_permanent_invalid', { messageId: envelope.messageId });
        return false;
      }
      structuredLog('sqs_delivery_transient_failure', { messageId: envelope.messageId });
      return false;
    }
  }
}
