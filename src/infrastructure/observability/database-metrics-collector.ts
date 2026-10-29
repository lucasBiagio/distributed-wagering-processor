import { MikroORM } from '@mikro-orm/core';

import {
  inboxMessagesState,
  ledgerEntriesState,
  metricsRegistry,
  outboxLagSeconds,
  outboxMessagesState,
  outboxRetryScheduledState,
  pendingReferenceState,
  reconciliationDivergentWalletsState,
  transactionFailuresState,
  transactionsState,
} from './wagering-metrics.js';

/**
 * Refreshes every `global_postgresql` gauge from SQL aggregates.
 *
 * Why aggregates instead of in-process counters: with several API instances, several
 * consumers and a sweeper all mutating the same tables, a local counter can only describe
 * what one process happened to see since its last restart. "Transactions by status" is a
 * property of the durable state, so it is read from the durable state. Every instance
 * therefore reports the same number and a restart does not lose history.
 *
 * All queries are read-only aggregates with no lock and no `FOR UPDATE`, so a scrape can
 * never block, delay or alter financial processing.
 */
export class DatabaseMetricsCollector {
  public constructor(private readonly orm: MikroORM) {}

  public async collect(): Promise<void> {
    const connection = this.orm.em.getConnection('read');

    const byStatus = await connection.execute(
      `select status, kind, count(*)::int as count
         from wager_transactions
        group by status, kind`,
    ) as Array<{ status: string; kind: string; count: number }>;
    transactionsState.reset();
    for (const row of byStatus) {
      transactionsState.set({ status: row.status, kind: row.kind }, row.count);
    }

    const byFailureCode = await connection.execute(
      `select failure_code, count(*)::int as count
         from wager_transactions
        where status = 'REJECTED' and failure_code is not null
        group by failure_code`,
    ) as Array<{ failure_code: string; count: number }>;
    transactionFailuresState.reset();
    for (const row of byFailureCode) {
      transactionFailuresState.set({ failure_code: row.failure_code }, row.count);
    }

    const pending = await connection.execute(
      `select
         count(*) filter (where reference_next_attempt_at is not null and reference_next_attempt_at <= now())::int as due,
         count(*) filter (where reference_next_attempt_at is null or reference_next_attempt_at > now())::int as waiting
         from wager_transactions
        where status = 'PENDING_REFERENCE'`,
    ) as Array<{ due: number; waiting: number }>;
    pendingReferenceState.reset();
    pendingReferenceState.set({ readiness: 'due' }, pending[0]?.due ?? 0);
    pendingReferenceState.set({ readiness: 'waiting' }, pending[0]?.waiting ?? 0);

    const inbox = await connection.execute(
      'select count(*)::int as count from inbox_messages',
    ) as Array<{ count: number }>;
    inboxMessagesState.set({}, inbox[0]?.count ?? 0);

    const outbox = await connection.execute(
      `select
         count(*) filter (where published_at is null)::int as pending,
         count(*) filter (where published_at is not null)::int as published,
         count(*) filter (where published_at is null and attempts > 0)::int as retry_scheduled,
         coalesce(
           extract(epoch from (now() - min(occurred_at) filter (where published_at is null))),
           0
         )::float8 as lag_seconds
         from outbox_messages`,
    ) as Array<{ pending: number; published: number; retry_scheduled: number; lag_seconds: number }>;
    const outboxRow = outbox[0];
    outboxMessagesState.reset();
    outboxMessagesState.set({ state: 'pending' }, outboxRow?.pending ?? 0);
    outboxMessagesState.set({ state: 'published' }, outboxRow?.published ?? 0);
    outboxRetryScheduledState.set({}, outboxRow?.retry_scheduled ?? 0);
    /**
     * An empty outbox has no oldest row, so there is no lag to report: the gauge is exactly 0.
     * `Math.max` additionally guarantees that a row written slightly ahead of the database
     * clock can never produce a negative, physically meaningless lag.
     */
    outboxLagSeconds.set({}, Math.max(0, Number(outboxRow?.lag_seconds ?? 0)));

    const ledger = await connection.execute(
      'select count(*)::int as count from wallet_ledger_entries',
    ) as Array<{ count: number }>;
    ledgerEntriesState.set({}, ledger[0]?.count ?? 0);

    /**
     * Reconciliation divergence as durable, global state: a wallet whose stored balance does
     * not equal the signed sum of its immutable ledger entries. Exposed as an observation
     * only — nothing in this query or anywhere else corrects a balance automatically.
     */
    const divergent = await connection.execute(
      `select count(*)::int as count
         from (
           select w.id
             from wallets w
             left join wallet_ledger_entries l on l.wallet_id = w.id
            group by w.id, w.balance
           having w.balance <> coalesce(sum(case when l.direction = 'CREDIT' then l.amount else -l.amount end), 0)
         ) divergent_wallets`,
    ) as Array<{ count: number }>;
    reconciliationDivergentWalletsState.set({}, divergent[0]?.count ?? 0);
  }
}

let databaseCollectorRegistered = false;

/** Registers the PostgreSQL collector once per process. */
export function registerDatabaseMetricsCollector(orm: MikroORM): void {
  if (databaseCollectorRegistered) {
    return;
  }
  databaseCollectorRegistered = true;
  const collector = new DatabaseMetricsCollector(orm);
  metricsRegistry.addCollector('postgresql', () => collector.collect());
}
