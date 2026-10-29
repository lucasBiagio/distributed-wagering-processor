import { randomUUID } from 'node:crypto';

import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

import { getAppConfig } from '../config.js';

interface DatabaseOutboxRow {
  readonly id: string;
  readonly wallet_id: string;
  readonly enqueue_sequence: string;
  readonly payload: Record<string, unknown>;
  readonly attempts: number;
  readonly claim_token: string;
}

export interface ClaimedOutboxMessage {
  readonly id: string;
  readonly walletId: string;
  readonly enqueueSequence: string;
  readonly payload: Record<string, unknown>;
  readonly attempts: number;
  readonly claimToken: string;
}

export interface SqsOutboxPublisherOptions {
  readonly queueUrl?: string;
  readonly leaseMs?: number;
  readonly retryBaseMs?: number;
  readonly retryMaxMs?: number;
  readonly batchSize?: number;
}

export interface DrainOptions {
  readonly batchSize?: number;
  readonly stopWhenIdle?: boolean;
  readonly maxConfirmations?: number;
  readonly afterClaim?: (message: ClaimedOutboxMessage) => void | Promise<void>;
  readonly afterSendBeforeConfirm?: (message: ClaimedOutboxMessage) => void | Promise<void>;
}

export interface DrainResult {
  readonly claimed: number;
  readonly confirmed: number;
  readonly failed: number;
  readonly maxConcurrentWallets: number;
}

function structuredLog(event: string, fields: Record<string, unknown>): void {
  process.stdout.write(`${JSON.stringify({ event, at: new Date().toISOString(), ...fields })}\n`);
}

export class SqsOutboxPublisher {
  private readonly queueUrl: string;
  private readonly leaseMs: number;
  private readonly retryBaseMs: number;
  private readonly retryMaxMs: number;
  private readonly defaultBatchSize: number;
  private acceptingClaims = true;

  public constructor(
    private readonly sqs: SQSClient,
    private readonly orm: MikroORM<PostgreSqlDriver>,
    options: SqsOutboxPublisherOptions = {},
  ) {
    const config = getAppConfig();
    this.queueUrl = options.queueUrl ?? config.wagerEventsQueueUrl ?? `${config.sqsEndpoint}/000000000000/${config.wagerEventsQueueName}`;
    this.leaseMs = options.leaseMs ?? config.outboxPublisherLeaseMs;
    this.retryBaseMs = options.retryBaseMs ?? config.outboxPublisherRetryBaseMs;
    this.retryMaxMs = options.retryMaxMs ?? config.outboxPublisherRetryMaxMs;
    this.defaultBatchSize = options.batchSize ?? config.outboxPublisherBatchSize;
  }

  public stopAcceptingClaims(): void {
    this.acceptingClaims = false;
  }

  public async drain(options: DrainOptions = {}): Promise<DrainResult> {
    const result = { claimed: 0, confirmed: 0, failed: 0, maxConcurrentWallets: 0 };
    const batchSize = options.batchSize ?? this.defaultBatchSize;

    while (this.acceptingClaims && (options.maxConfirmations === undefined || result.confirmed < options.maxConfirmations)) {
      const remaining = options.maxConfirmations === undefined
        ? batchSize
        : Math.min(batchSize, options.maxConfirmations - result.confirmed);
      const claimed = await this.claimReady(remaining);
      if (claimed.length === 0) {
        if (options.stopWhenIdle) {
          break;
        }
        await new Promise<void>((resolve) => setTimeout(resolve, Math.min(this.retryBaseMs, 1_000)));
        continue;
      }

      result.claimed += claimed.length;
      result.maxConcurrentWallets = Math.max(result.maxConcurrentWallets, new Set(claimed.map((message) => message.walletId)).size);
      const publications = await Promise.all(claimed.map((message) => this.publishClaimed(message, options)));
      for (const publication of publications) {
        result.confirmed += publication.confirmed;
        result.failed += publication.failed;
      }
    }

    return result;
  }

  private async claimReady(limit: number): Promise<ClaimedOutboxMessage[]> {
    if (!this.acceptingClaims || limit <= 0) {
      return [];
    }

    const now = new Date();
    const leaseExpiresAt = new Date(now.getTime() + this.leaseMs);
    return await this.orm.em.transactional(async (em) => {
      const connection = em.getConnection();
      const candidates = await connection.execute(
        `select id::text, wallet_id::text, enqueue_sequence::text, payload, attempts
           from outbox_messages candidate
          where candidate.published_at is null
            and (candidate.claim_token is null or candidate.lease_expires_at <= ?)
            and (candidate.next_attempt_at is null or candidate.next_attempt_at <= ?)
            and not exists (
              select 1
                from outbox_messages earlier
               where earlier.wallet_id = candidate.wallet_id
                 and earlier.published_at is null
                 and earlier.enqueue_sequence < candidate.enqueue_sequence
            )
          order by candidate.enqueue_sequence asc
          limit ?
          for update skip locked`,
        [now, now, limit],
      ) as DatabaseOutboxRow[];

      const claimed: ClaimedOutboxMessage[] = [];
      for (const candidate of candidates) {
        const claimToken = randomUUID();
        const update = await connection.execute(
          `update outbox_messages
              set claim_token = ?, lease_expires_at = ?
            where id = ? and published_at is null
            returning id::text`,
          [claimToken, leaseExpiresAt, candidate.id],
        ) as Array<{ id: string }>;
        if (update.length === 1) {
          claimed.push({
            id: candidate.id,
            walletId: candidate.wallet_id,
            enqueueSequence: candidate.enqueue_sequence,
            payload: candidate.payload,
            attempts: candidate.attempts,
            claimToken,
          });
          structuredLog('outbox_claimed', {
            eventId: candidate.id,
            walletId: candidate.wallet_id,
            enqueueSequence: candidate.enqueue_sequence,
            leaseExpiresAt: leaseExpiresAt.toISOString(),
          });
        }
      }
      return claimed;
    });
  }

  private async publishClaimed(message: ClaimedOutboxMessage, options: DrainOptions): Promise<{ confirmed: number; failed: number }> {
    // Test and operational hooks run outside a SQL transaction: an abrupt process exit leaves the durable lease for recovery.
    await options.afterClaim?.(message);
    if (!this.acceptingClaims) {
      return { confirmed: 0, failed: 0 };
    }

    try {
      structuredLog('outbox_publish_attempt', {
        eventId: message.id,
        walletId: message.walletId,
        enqueueSequence: message.enqueueSequence,
        attempt: message.attempts + 1,
      });
      await this.sqs.send(new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(message.payload),
        MessageGroupId: message.walletId,
        MessageDeduplicationId: message.id,
      }));
    } catch (error) {
      const nextAttemptAt = await this.scheduleRetry(message, error);
      structuredLog('outbox_publish_failed', {
        eventId: message.id,
        walletId: message.walletId,
        error: error instanceof Error ? error.message : String(error),
        nextAttemptAt: nextAttemptAt?.toISOString() ?? null,
      });
      return { confirmed: 0, failed: 1 };
    }

    structuredLog('outbox_sqs_accepted', { eventId: message.id, walletId: message.walletId });
    // A crash here deliberately leaves published_at empty; a later owner republishes the same eventId.
    await options.afterSendBeforeConfirm?.(message);

    try {
      const confirmed = await this.confirm(message);
      if (confirmed) {
        structuredLog('outbox_published', { eventId: message.id, walletId: message.walletId });
        return { confirmed: 1, failed: 0 };
      }

      structuredLog('outbox_confirmation_stale', { eventId: message.id, walletId: message.walletId });
      return { confirmed: 0, failed: 0 };
    } catch (error) {
      // Database failure after SQS acceptance is intentionally not converted into success or a destructive retry.
      // The lease expiry provides recovery and may duplicate the stable eventId.
      structuredLog('outbox_confirmation_error', {
        eventId: message.id,
        walletId: message.walletId,
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }

  private async confirm(message: ClaimedOutboxMessage): Promise<boolean> {
    const result = await this.orm.em.getConnection().execute(
      `update outbox_messages
          set published_at = now(), claim_token = null, lease_expires_at = null, next_attempt_at = null
        where id = ? and claim_token = ? and published_at is null
        returning id::text`,
      [message.id, message.claimToken],
    ) as Array<{ id: string }>;
    return result.length === 1;
  }

  private async scheduleRetry(message: ClaimedOutboxMessage, error: unknown): Promise<Date | null> {
    const rawDelay = Math.min(this.retryMaxMs, this.retryBaseMs * 2 ** Math.min(message.attempts, 20));
    const jitter = Math.floor(rawDelay * (Math.random() * 0.2));
    const nextAttemptAt = new Date(Date.now() + rawDelay + jitter);
    const result = await this.orm.em.getConnection().execute(
      `update outbox_messages
          set attempts = attempts + 1,
              next_attempt_at = ?,
              claim_token = null,
              lease_expires_at = null
        where id = ? and claim_token = ? and published_at is null
        returning id::text`,
      [nextAttemptAt, message.id, message.claimToken],
    ) as Array<{ id: string }>;
    if (result.length === 0) {
      structuredLog('outbox_retry_stale', {
        eventId: message.id,
        error: error instanceof Error ? error.message : String(error),
      });
      return null;
    }
    return nextAttemptAt;
  }
}
