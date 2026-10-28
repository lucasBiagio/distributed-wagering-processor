import { Migration } from '@mikro-orm/migrations';

/**
 * CP2B-2: scheduling metadata for out-of-order references.
 *
 * A REFUND/ROLLBACK whose referenced transaction has not arrived yet is persisted as
 * PENDING_REFERENCE and retried by a sweep with exponential backoff. Only two columns are
 * added, both scoped to that state:
 *
 * - `reference_attempts`: resolution attempts already consumed (bounded retry budget).
 * - `reference_next_attempt_at`: when the sweep may claim the row again.
 *
 * The schedule is constrained to live only while the row waits: any terminal transition has
 * to clear it, which makes "scheduled" and "terminal" mutually exclusive at the database
 * level instead of by convention. The partial index keeps the sweep query from scanning
 * processed history.
 */
export class Migration20261029150000 extends Migration {
  public override async up(): Promise<void> {
    this.addSql(`alter table "wager_transactions"
      add column "reference_attempts" integer not null default 0,
      add column "reference_next_attempt_at" timestamptz null;`);

    this.addSql(`alter table "wager_transactions"
      add constraint "wager_transactions_reference_attempts_non_negative"
      check ("reference_attempts" >= 0);`);

    this.addSql(`alter table "wager_transactions"
      add constraint "wager_transactions_reference_schedule_pending_only"
      check (
        ("status" = 'PENDING_REFERENCE' and "reference_next_attempt_at" is not null)
        or
        ("status" <> 'PENDING_REFERENCE' and "reference_next_attempt_at" is null)
      );`);

    this.addSql(`create index "wager_transactions_pending_reference_due_idx"
      on "wager_transactions" ("reference_next_attempt_at")
      where "status" = 'PENDING_REFERENCE';`);
  }

  public override async down(): Promise<void> {
    this.addSql('drop index if exists "wager_transactions_pending_reference_due_idx";');
    this.addSql(`alter table "wager_transactions"
      drop constraint if exists "wager_transactions_reference_schedule_pending_only";`);
    this.addSql(`alter table "wager_transactions"
      drop constraint if exists "wager_transactions_reference_attempts_non_negative";`);
    this.addSql(`alter table "wager_transactions"
      drop column if exists "reference_next_attempt_at",
      drop column if exists "reference_attempts";`);
  }
}
