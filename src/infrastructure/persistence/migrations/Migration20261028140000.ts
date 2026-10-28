import { Migration } from '@mikro-orm/migrations';

export class Migration20261028140000 extends Migration {
  public override async up(): Promise<void> {
    this.addSql(`
      create table wallets (
        id uuid primary key,
        player_id uuid not null,
        currency char(3) not null,
        balance numeric(20,2) not null check (balance >= 0),
        version integer not null default 1 check (version >= 1),
        created_at timestamptz not null,
        updated_at timestamptz not null,
        constraint wallets_currency_format check (currency ~ '^[A-Z]{3}$'),
        constraint wallets_timestamp_order check (updated_at >= created_at),
        constraint wallets_player_currency_unique unique (player_id, currency),
        constraint wallets_identity_currency_unique unique (id, currency),
        constraint wallets_identity_player_currency_unique unique (id, player_id, currency)
      );

      create table wager_transactions (
        id uuid primary key,
        provider_id text not null,
        external_transaction_id text null,
        idempotency_key text not null unique,
        payload_hash text not null,
        wallet_id uuid not null,
        player_id uuid not null,
        round_id text not null,
        game_id text not null,
        kind text not null,
        amount numeric(20,2) not null,
        currency char(3) not null,
        reference_transaction_id uuid null references wager_transactions(id) on delete restrict,
        reference_external_transaction_id text null,
        status text not null,
        failure_code text null,
        processed_at timestamptz null,
        resulting_balance numeric(20,2) null check (resulting_balance >= 0),
        created_at timestamptz not null,
        constraint wager_transactions_wallet_identity_fk foreign key (wallet_id, player_id, currency)
          references wallets (id, player_id, currency) on delete restrict,
        constraint wager_transactions_identity_wallet_currency_unique unique (id, wallet_id, currency),
        constraint wager_transactions_currency_format check (currency ~ '^[A-Z]{3}$'),
        constraint wager_transactions_amount_by_kind check (
          (kind = 'LOSS' and amount >= 0)
          or (kind <> 'LOSS' and amount > 0)
        ),
        constraint wager_transactions_kind check (kind in ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
        constraint wager_transactions_status check (status in ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
        constraint wager_transactions_reference_requirement check (
          (kind in ('REFUND', 'ROLLBACK') and reference_external_transaction_id is not null)
          or (kind not in ('REFUND', 'ROLLBACK') and reference_external_transaction_id is null)
        ),
        constraint wager_transactions_opening_internal check (
          (kind = 'OPENING' and external_transaction_id is null)
          or (kind <> 'OPENING' and external_transaction_id is not null)
        ),
        constraint wager_transactions_processed_at check (
          (status = 'PROCESSED' and processed_at is not null)
          or (status <> 'PROCESSED' and processed_at is null)
        ),
        constraint wager_transactions_processed_reversal_reference_required check (
          status <> 'PROCESSED'
          or kind not in ('REFUND', 'ROLLBACK')
          or reference_transaction_id is not null
        ),
        constraint wager_transactions_pending_result_snapshot check (
          status not in ('PENDING', 'PENDING_REFERENCE')
          or resulting_balance is null
        )
      );

      create unique index wager_transactions_provider_external_unique
        on wager_transactions (provider_id, external_transaction_id)
        where external_transaction_id is not null;
      create index wager_transactions_provider_external_lookup
        on wager_transactions (provider_id, external_transaction_id);
      create index wager_transactions_reference_lookup
        on wager_transactions (provider_id, reference_external_transaction_id)
        where reference_external_transaction_id is not null;
      create index wager_transactions_wallet_status_lookup
        on wager_transactions (wallet_id, status, created_at);
      create unique index wager_transactions_processed_economic_reversal_unique
        on wager_transactions (reference_transaction_id)
        where status = 'PROCESSED' and kind in ('REFUND', 'ROLLBACK') and reference_transaction_id is not null;

      create table wallet_ledger_entries (
        id uuid primary key,
        wallet_id uuid not null,
        transaction_id uuid not null,
        direction text not null check (direction in ('DEBIT', 'CREDIT')),
        amount numeric(20,2) not null check (amount > 0),
        currency char(3) not null,
        balance_before numeric(20,2) not null check (balance_before >= 0),
        balance_after numeric(20,2) not null check (balance_after >= 0),
        created_at timestamptz not null,
        constraint wallet_ledger_entries_wallet_currency_fk foreign key (wallet_id, currency)
          references wallets (id, currency) on delete restrict,
        constraint wallet_ledger_entries_transaction_identity_fk foreign key (transaction_id, wallet_id, currency)
          references wager_transactions (id, wallet_id, currency) on delete restrict,
        constraint wallet_ledger_entries_currency_format check (currency ~ '^[A-Z]{3}$'),
        constraint wallet_ledger_entries_wallet_transaction_unique unique (wallet_id, transaction_id),
        constraint wallet_ledger_entries_arithmetic check (
          (direction = 'DEBIT' and balance_before - amount = balance_after)
          or (direction = 'CREDIT' and balance_before + amount = balance_after)
        )
      );
      create index wallet_ledger_entries_wallet_created_at_idx on wallet_ledger_entries (wallet_id, created_at, id);

      create table inbox_messages (
        message_id text not null,
        consumer_name text not null,
        payload_hash text not null,
        received_at timestamptz not null,
        processed_at timestamptz null,
        primary key (consumer_name, message_id)
      );
      create index inbox_messages_processed_at_idx on inbox_messages (processed_at, received_at);

      create table outbox_messages (
        id uuid primary key,
        aggregate_id uuid not null,
        event_type text not null,
        payload jsonb not null,
        occurred_at timestamptz not null,
        attempts integer not null default 0 check (attempts >= 0),
        next_attempt_at timestamptz null,
        published_at timestamptz null
      );
      create index outbox_messages_pending_idx on outbox_messages (next_attempt_at, occurred_at)
        where published_at is null;

      create function enforce_wallet_version_on_balance_change() returns trigger language plpgsql as $$
      begin
        if new.balance is distinct from old.balance then
          if new.version <> old.version + 1 then
            raise exception 'wallet version must increment exactly once when balance changes' using errcode = '23514';
          end if;
        elsif new.version is distinct from old.version then
          raise exception 'wallet version cannot change without a balance change' using errcode = '23514';
        end if;

        return new;
      end;
      $$;

      create trigger wallets_version_balance_guard
        before update on wallets
        for each row execute function enforce_wallet_version_on_balance_change();

      create function prevent_wallet_ledger_entry_mutation() returns trigger language plpgsql as $$
      begin
        raise exception 'wallet_ledger_entries are immutable' using errcode = '55000';
      end;
      $$;

      create trigger wallet_ledger_entries_no_update
        before update on wallet_ledger_entries
        for each row execute function prevent_wallet_ledger_entry_mutation();
      create trigger wallet_ledger_entries_no_delete
        before delete on wallet_ledger_entries
        for each row execute function prevent_wallet_ledger_entry_mutation();

      create function require_consistent_transaction_for_wallet_ledger_entry() returns trigger language plpgsql as $$
      declare
        transaction_kind text;
        transaction_status text;
        transaction_amount numeric(20,2);
        reference_kind text;
        expected_direction text;
      begin
        select kind, status, amount
          into transaction_kind, transaction_status, transaction_amount
          from wager_transactions
          where id = new.transaction_id
            and wallet_id = new.wallet_id
            and currency = new.currency;

        if transaction_status is null then
          return new;
        end if;

        if transaction_status is distinct from 'PROCESSED' then
          raise exception 'wallet ledger entries require a processed wager transaction'
            using errcode = '23514', constraint = 'wallet_ledger_entries_transaction_consistency';
        end if;

        if transaction_kind in ('LOSS') then
          raise exception 'LOSS wager transactions cannot produce ledger entries'
            using errcode = '23514', constraint = 'wallet_ledger_entries_transaction_consistency';
        end if;

        if new.amount is distinct from transaction_amount then
          raise exception 'wallet ledger amount must equal wager transaction amount'
            using errcode = '23514', constraint = 'wallet_ledger_entries_transaction_consistency';
        end if;

        if transaction_kind = 'ROLLBACK' then
          select kind into reference_kind
            from wager_transactions
            where id = (
              select reference_transaction_id from wager_transactions where id = new.transaction_id
            );
          expected_direction := case when reference_kind = 'BET' then 'CREDIT' else 'DEBIT' end;
        else
          expected_direction := case
            when transaction_kind = 'BET' then 'DEBIT'
            when transaction_kind in ('OPENING', 'WIN', 'REFUND') then 'CREDIT'
            else null
          end;
        end if;

        if new.direction is distinct from expected_direction then
          raise exception 'wallet ledger direction must match wager transaction effect'
            using errcode = '23514', constraint = 'wallet_ledger_entries_transaction_consistency';
        end if;

        return new;
      end;
      $$;

      create trigger wallet_ledger_entries_transaction_consistency_guard
        before insert on wallet_ledger_entries
        for each row execute function require_consistent_transaction_for_wallet_ledger_entry();

      create function prevent_terminal_wager_transaction_mutation() returns trigger language plpgsql as $$
      begin
        if old.status in ('PROCESSED', 'REJECTED', 'FAILED') then
          raise exception 'terminal wager transactions are immutable' using errcode = '55000';
        end if;
        return new;
      end;
      $$;

      create trigger wager_transactions_terminal_immutability_guard
        before update on wager_transactions
        for each row execute function prevent_terminal_wager_transaction_mutation();
    `);
  }

  public override async down(): Promise<void> {
    this.addSql(`
      drop trigger if exists wallets_version_balance_guard on wallets;
      drop function if exists enforce_wallet_version_on_balance_change();
      drop trigger if exists wager_transactions_terminal_immutability_guard on wager_transactions;
      drop function if exists prevent_terminal_wager_transaction_mutation();
      drop trigger if exists wallet_ledger_entries_transaction_consistency_guard on wallet_ledger_entries;
      drop function if exists require_consistent_transaction_for_wallet_ledger_entry();
      drop trigger if exists wallet_ledger_entries_no_delete on wallet_ledger_entries;
      drop trigger if exists wallet_ledger_entries_no_update on wallet_ledger_entries;
      drop function if exists prevent_wallet_ledger_entry_mutation();
      drop table if exists outbox_messages;
      drop table if exists inbox_messages;
      drop table if exists wallet_ledger_entries;
      drop table if exists wager_transactions;
      drop table if exists wallets;
    `);
  }
}
