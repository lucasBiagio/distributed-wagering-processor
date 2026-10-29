import { Migration } from '@mikro-orm/migrations';

export class Migration20261029170000 extends Migration {
  override async up(): Promise<void> {
    this.addSql('create sequence if not exists outbox_messages_enqueue_sequence_seq;');
    this.addSql('alter table "outbox_messages" add column "wallet_id" uuid null, add column "enqueue_sequence" bigint null, add column "claim_token" uuid null, add column "lease_expires_at" timestamptz null;');
    this.addSql(`do $$
      begin
        if exists (
          select 1
            from outbox_messages message
            left join wager_transactions transaction
              on transaction.id = nullif(message.payload #>> '{data,transactionId}', '')::uuid
           where nullif(message.payload #>> '{data,walletId}', '') is null
              or nullif(message.payload #>> '{data,transactionId}', '') is null
              or transaction.id is null
              or transaction.wallet_id <> nullif(message.payload #>> '{data,walletId}', '')::uuid
        ) then
          raise exception 'CP3B outbox backfill requires each legacy event to reference its persisted wallet and transaction';
        end if;

        if exists (
          select 1
            from outbox_messages message
           where message.event_type not in (
             'WagerTransactionPendingReference',
             'WagerTransactionProcessed',
             'WagerTransactionRejected',
             'WalletBalanceChanged'
           )
        ) then
          raise exception 'CP3B outbox backfill cannot order an unknown legacy event type';
        end if;

        if exists (
          select 1
            from outbox_messages message
            join wager_transactions transaction
              on transaction.id = (message.payload #>> '{data,transactionId}')::uuid
           group by (message.payload #>> '{data,walletId}')::uuid, message.occurred_at
          having count(distinct transaction.id) > 1
        ) then
          raise exception 'CP3B outbox backfill cannot reconstruct historical order for multiple wallet transactions sharing occurred_at';
        end if;

        if exists (
          select 1
            from outbox_messages message
           group by (message.payload #>> '{data,walletId}')::uuid,
                    (message.payload #>> '{data,transactionId}')::uuid,
                    message.event_type
          having count(*) > 1
        ) then
          raise exception 'CP3B outbox backfill found duplicate legacy event types for one wallet transaction';
        end if;
      end;
      $$;`);
    this.addSql(`with ordered_legacy_events as (
        select message.id,
               (message.payload #>> '{data,walletId}')::uuid as wallet_id,
               row_number() over (
                 partition by (message.payload #>> '{data,walletId}')::uuid
                 order by message.occurred_at asc,
                          case message.event_type
                            when 'WagerTransactionPendingReference' then 0
                            when 'WagerTransactionProcessed' then 1
                            when 'WagerTransactionRejected' then 1
                            when 'WalletBalanceChanged' then 2
                            else 3
                          end asc
               )::bigint as enqueue_sequence
          from outbox_messages message
          join wager_transactions transaction
            on transaction.id = (message.payload #>> '{data,transactionId}')::uuid
      )
      update outbox_messages message
         set wallet_id = ordered_legacy_events.wallet_id,
             enqueue_sequence = ordered_legacy_events.enqueue_sequence
        from ordered_legacy_events
       where message.id = ordered_legacy_events.id;`);
    this.addSql(`select setval(
      'outbox_messages_enqueue_sequence_seq',
      greatest(coalesce((select max(enqueue_sequence) from outbox_messages), 0), 1),
      coalesce((select max(enqueue_sequence) is not null from outbox_messages), false)
    );`);
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
