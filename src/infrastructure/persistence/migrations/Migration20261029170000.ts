import { Migration } from '@mikro-orm/migrations';

export class Migration20261029170000 extends Migration {
  override async up(): Promise<void> {
    this.addSql('create sequence if not exists outbox_messages_enqueue_sequence_seq;');
    this.addSql('alter table "outbox_messages" add column "wallet_id" uuid null, add column "enqueue_sequence" bigint null, add column "claim_token" uuid null, add column "lease_expires_at" timestamptz null;');
    this.addSql(`update "outbox_messages"
      set "wallet_id" = nullif("payload" #>> '{data,walletId}', '')::uuid,
          "enqueue_sequence" = nextval('outbox_messages_enqueue_sequence_seq')
      where "wallet_id" is null or "enqueue_sequence" is null;`);
    this.addSql('alter table "outbox_messages" alter column "enqueue_sequence" set default nextval(\'outbox_messages_enqueue_sequence_seq\'), alter column "wallet_id" set not null, alter column "enqueue_sequence" set not null;');
    this.addSql('alter sequence outbox_messages_enqueue_sequence_seq owned by "outbox_messages"."enqueue_sequence";');
    this.addSql('create unique index "outbox_messages_wallet_sequence_unique" on "outbox_messages" ("wallet_id", "enqueue_sequence");');
    this.addSql('create index "outbox_messages_claim_ready_index" on "outbox_messages" ("wallet_id", "enqueue_sequence") where "published_at" is null;');
    this.addSql('create index "outbox_messages_lease_index" on "outbox_messages" ("lease_expires_at") where "published_at" is null and "claim_token" is not null;');
  }

  override async down(): Promise<void> {
    this.addSql('drop index if exists "outbox_messages_lease_index";');
    this.addSql('drop index if exists "outbox_messages_claim_ready_index";');
    this.addSql('drop index if exists "outbox_messages_wallet_sequence_unique";');
    this.addSql('alter table "outbox_messages" drop column if exists "lease_expires_at", drop column if exists "claim_token", drop column if exists "enqueue_sequence", drop column if exists "wallet_id";');
    this.addSql('drop sequence if exists outbox_messages_enqueue_sequence_seq;');
  }
}
