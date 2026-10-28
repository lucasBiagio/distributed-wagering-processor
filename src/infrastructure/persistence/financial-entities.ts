import { DecimalType, EntitySchema } from '@mikro-orm/core';

export class WalletRecord {
  public id!: string;
  public playerId!: string;
  public currency!: string;
  public balance!: string;
  public version!: number;
  public createdAt!: Date;
  public updatedAt!: Date;
}

export class WagerTransactionRecord {
  public id!: string;
  public providerId!: string;
  public externalTransactionId!: string | null;
  public idempotencyKey!: string;
  public payloadHash!: string;
  public walletId!: string;
  public playerId!: string;
  public roundId!: string;
  public gameId!: string;
  public kind!: string;
  public amount!: string;
  public currency!: string;
  public referenceTransactionId!: string | null;
  public referenceExternalTransactionId!: string | null;
  public status!: string;
  public failureCode!: string | null;
  public processedAt!: Date | null;
  public resultingBalance!: string | null;
  public createdAt!: Date;
}

export class WalletLedgerEntryRecord {
  public id!: string;
  public walletId!: string;
  public transactionId!: string;
  public direction!: string;
  public amount!: string;
  public currency!: string;
  public balanceBefore!: string;
  public balanceAfter!: string;
  public createdAt!: Date;
}

export class InboxMessageRecord {
  public messageId!: string;
  public consumerName!: string;
  public payloadHash!: string;
  public receivedAt!: Date;
  public processedAt!: Date | null;
}

export class OutboxMessageRecord {
  public id!: string;
  public aggregateId!: string;
  public eventType!: string;
  public payload!: Record<string, unknown>;
  public occurredAt!: Date;
  public attempts!: number;
  public nextAttemptAt!: Date | null;
  public publishedAt!: Date | null;
}

export const WalletRecordSchema = new EntitySchema<WalletRecord>({
  class: WalletRecord,
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'uuid', fieldName: 'player_id' },
    currency: { type: String, length: 3 },
    balance: { type: new DecimalType('string'), precision: 20, scale: 2 },
    version: { type: Number },
    createdAt: { type: Date, fieldName: 'created_at' },
    updatedAt: { type: Date, fieldName: 'updated_at' },
  },
});

export const WagerTransactionRecordSchema = new EntitySchema<WagerTransactionRecord>({
  class: WagerTransactionRecord,
  tableName: 'wager_transactions',
  properties: {
    id: { type: 'uuid', primary: true },
    providerId: { type: String, fieldName: 'provider_id' },
    externalTransactionId: { type: String, fieldName: 'external_transaction_id', nullable: true },
    idempotencyKey: { type: String, fieldName: 'idempotency_key' },
    payloadHash: { type: String, fieldName: 'payload_hash' },
    walletId: { type: 'uuid', fieldName: 'wallet_id' },
    playerId: { type: 'uuid', fieldName: 'player_id' },
    roundId: { type: String, fieldName: 'round_id' },
    gameId: { type: String, fieldName: 'game_id' },
    kind: { type: String },
    amount: { type: new DecimalType('string'), precision: 20, scale: 2 },
    currency: { type: String, length: 3 },
    referenceTransactionId: { type: 'uuid', fieldName: 'reference_transaction_id', nullable: true },
    referenceExternalTransactionId: { type: String, fieldName: 'reference_external_transaction_id', nullable: true },
    status: { type: String },
    failureCode: { type: String, fieldName: 'failure_code', nullable: true },
    processedAt: { type: Date, fieldName: 'processed_at', nullable: true },
    resultingBalance: { type: new DecimalType('string'), fieldName: 'resulting_balance', precision: 20, scale: 2, nullable: true },
    createdAt: { type: Date, fieldName: 'created_at' },
  },
});

export const WalletLedgerEntryRecordSchema = new EntitySchema<WalletLedgerEntryRecord>({
  class: WalletLedgerEntryRecord,
  tableName: 'wallet_ledger_entries',
  properties: {
    id: { type: 'uuid', primary: true },
    walletId: { type: 'uuid', fieldName: 'wallet_id' },
    transactionId: { type: 'uuid', fieldName: 'transaction_id' },
    direction: { type: String },
    amount: { type: new DecimalType('string'), precision: 20, scale: 2 },
    currency: { type: String, length: 3 },
    balanceBefore: { type: new DecimalType('string'), fieldName: 'balance_before', precision: 20, scale: 2 },
    balanceAfter: { type: new DecimalType('string'), fieldName: 'balance_after', precision: 20, scale: 2 },
    createdAt: { type: Date, fieldName: 'created_at' },
  },
});

export const InboxMessageRecordSchema = new EntitySchema<InboxMessageRecord>({
  class: InboxMessageRecord,
  tableName: 'inbox_messages',
  properties: {
    messageId: { type: String, fieldName: 'message_id', primary: true },
    consumerName: { type: String, fieldName: 'consumer_name', primary: true },
    payloadHash: { type: String, fieldName: 'payload_hash' },
    receivedAt: { type: Date, fieldName: 'received_at' },
    processedAt: { type: Date, fieldName: 'processed_at', nullable: true },
  },
});

export const OutboxMessageRecordSchema = new EntitySchema<OutboxMessageRecord>({
  class: OutboxMessageRecord,
  tableName: 'outbox_messages',
  properties: {
    id: { type: 'uuid', primary: true },
    aggregateId: { type: 'uuid', fieldName: 'aggregate_id' },
    eventType: { type: String, fieldName: 'event_type' },
    payload: { type: 'json' },
    occurredAt: { type: Date, fieldName: 'occurred_at' },
    attempts: { type: Number },
    nextAttemptAt: { type: Date, fieldName: 'next_attempt_at', nullable: true },
    publishedAt: { type: Date, fieldName: 'published_at', nullable: true },
  },
});

export const financialEntities = [
  WalletRecordSchema,
  WagerTransactionRecordSchema,
  WalletLedgerEntryRecordSchema,
  InboxMessageRecordSchema,
  OutboxMessageRecordSchema,
];
