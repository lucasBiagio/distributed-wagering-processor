import { MetricRegistry } from './metric-registry.js';

/**
 * CP4-R3 metric catalogue.
 *
 * Every metric required by CHALLENGE.md section 12 is declared here once, with an explicit
 * scope, so that the API process and each worker process expose exactly the same contract.
 *
 * Scope semantics (also rendered as a `# SCOPE` comment on the endpoint):
 *
 * - `global_postgresql` — recomputed per scrape from a SQL aggregate. Every instance reports
 *   the same value because PostgreSQL is the single source of truth; this is the only honest
 *   way to answer "how many transactions are REJECTED" in a multi-instance deployment, and it
 *   is why these are gauges and not counters. Do **not** sum them across instances.
 * - `approximate_sqs` — read from `GetQueueAttributes`. SQS documents these attributes as
 *   approximations, so the value may lag reality; it is never used to make a financial
 *   decision, only to alert.
 * - `process_local` — counters/histograms accumulated in the memory of the process being
 *   scraped. They are NOT a global total: each process (API, consumer, publisher, sweeper)
 *   exposes its own endpoint and Prometheus aggregates across instances at query time. No
 *   code here pretends to aggregate them, and no financial write depends on them.
 *
 * Instrumentation never writes money as a label or as a histogram observation: durations are
 * seconds, everything else is a count, and the registry refuses anything else.
 */
export const metricsRegistry = new MetricRegistry();

/** Buckets in seconds, documented in README/ARCHITECTURE: sub-millisecond detail is useless
 * for a unit of work that always touches PostgreSQL, while the 10s tail catches lock waits. */
export const DURATION_BUCKETS_SECONDS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10] as const;

export const processInfo = metricsRegistry.gauge({
  name: 'wagering_process_info',
  help: 'Identifies the process exposing this endpoint; process_local metrics below belong only to it.',
  scope: 'process_local',
  unit: 'info',
  labelNames: ['process'],
});

// ---------------------------------------------------------------------------
// Global state derived from PostgreSQL
// ---------------------------------------------------------------------------

export const transactionsState = metricsRegistry.gauge({
  name: 'wagering_transactions_state',
  help: 'Durable wager transaction rows grouped by status and kind (PostgreSQL aggregate, identical from every instance).',
  scope: 'global_postgresql',
  unit: 'count',
  labelNames: ['status', 'kind'],
});

export const transactionFailuresState = metricsRegistry.gauge({
  name: 'wagering_transaction_failures_state',
  help: 'Durably REJECTED wager transactions grouped by failure code (PostgreSQL aggregate).',
  scope: 'global_postgresql',
  unit: 'count',
  labelNames: ['failure_code'],
});

export const pendingReferenceState = metricsRegistry.gauge({
  name: 'wagering_pending_reference_state',
  help: 'PENDING_REFERENCE transactions split into rows already due for a sweep and rows still waiting for their backoff (PostgreSQL aggregate).',
  scope: 'global_postgresql',
  unit: 'count',
  labelNames: ['readiness'],
});

export const inboxMessagesState = metricsRegistry.gauge({
  name: 'wagering_inbox_messages_state',
  help: 'Durable inbox rows; each row is one logical SQS delivery whose effects are applied exactly once (PostgreSQL aggregate).',
  scope: 'global_postgresql',
  unit: 'count',
});

export const outboxMessagesState = metricsRegistry.gauge({
  name: 'wagering_outbox_messages_state',
  help: 'Outbox rows grouped by publication state (PostgreSQL aggregate).',
  scope: 'global_postgresql',
  unit: 'count',
  labelNames: ['state'],
});

export const outboxRetryScheduledState = metricsRegistry.gauge({
  name: 'wagering_outbox_retry_scheduled_state',
  help: 'Unpublished outbox rows that already consumed at least one publish attempt (durable retry evidence, PostgreSQL aggregate).',
  scope: 'global_postgresql',
  unit: 'count',
});

export const outboxLagSeconds = metricsRegistry.gauge({
  name: 'wagering_outbox_lag_seconds',
  help: 'Age of the oldest unpublished outbox row in seconds; exactly 0 when the outbox is empty (never negative, never synthetic).',
  scope: 'global_postgresql',
  unit: 'seconds',
});

export const ledgerEntriesState = metricsRegistry.gauge({
  name: 'wagering_ledger_entries_state',
  help: 'Immutable wallet ledger entries (PostgreSQL aggregate).',
  scope: 'global_postgresql',
  unit: 'count',
});

export const reconciliationDivergentWalletsState = metricsRegistry.gauge({
  name: 'wagering_reconciliation_divergent_wallets_state',
  help: 'Wallets whose stored balance differs from the sum of their ledger entries. Observation only: a divergence is never repaired automatically (PostgreSQL aggregate).',
  scope: 'global_postgresql',
  unit: 'count',
});

// ---------------------------------------------------------------------------
// Approximate infrastructure state read from SQS
// ---------------------------------------------------------------------------

export const sqsQueueMessagesApproximate = metricsRegistry.gauge({
  name: 'wagering_sqs_queue_messages_approximate',
  help: 'ApproximateNumberOfMessages* reported by SQS GetQueueAttributes per queue role. Approximate by SQS contract; use for alerting, never for reconciliation.',
  scope: 'approximate_sqs',
  unit: 'count',
  labelNames: ['queue', 'state'],
});

export const dlqMessagesApproximate = metricsRegistry.gauge({
  name: 'wagering_dlq_messages_approximate',
  help: 'Approximate number of messages sitting in the dead-letter queue, read from real SQS queue attributes (never inferred from logs).',
  scope: 'approximate_sqs',
  unit: 'count',
  labelNames: ['queue'],
});

// ---------------------------------------------------------------------------
// Process-local counters and histograms
// ---------------------------------------------------------------------------

export const wagerOutcomes = metricsRegistry.counter({
  name: 'wagering_wager_outcomes_total',
  help: 'Wager submissions completed by this process, grouped by durable outcome and entry point.',
  scope: 'process_local',
  unit: 'count',
  labelNames: ['source', 'outcome'],
});

export const duplicateSubmissions = metricsRegistry.counter({
  name: 'wagering_duplicate_submissions_total',
  help: 'Duplicate submissions observed by this process. An idempotent replay returned the stored result; a payload conflict is a different payload under an already used identity and is never a replay.',
  scope: 'process_local',
  unit: 'count',
  labelNames: ['source', 'outcome'],
});

export const retries = metricsRegistry.counter({
  name: 'wagering_retries_total',
  help: 'Retried attempts performed by this process, counted once per actual re-execution (a re-read of durable state is not a retry).',
  scope: 'process_local',
  unit: 'count',
  labelNames: ['flow', 'reason'],
});

export const lockConflicts = metricsRegistry.counter({
  name: 'wagering_lock_conflicts_total',
  help: 'Real PostgreSQL concurrency conflicts by SQLSTATE (40001 serialization failure, 40P01 deadlock) and how this process resolved them. Normal pessimistic-lock contention is a wait, not a conflict, and is not counted here.',
  scope: 'process_local',
  unit: 'count',
  labelNames: ['sqlstate', 'resolution'],
});

export const processingDuration = metricsRegistry.histogram({
  name: 'wagering_processing_duration_seconds',
  help: 'Wall-clock duration of a real unit of work in this process, in seconds. Buckets are documented in README.md; monetary amounts are never observed here.',
  scope: 'process_local',
  unit: 'seconds',
  labelNames: ['operation', 'outcome'],
  buckets: DURATION_BUCKETS_SECONDS,
});

export const reconciliationChecks = metricsRegistry.counter({
  name: 'wagering_reconciliation_checks_total',
  help: 'Reconciliation checks executed by this process, split by result. An inconsistent result is reported and counted, never silently corrected.',
  scope: 'process_local',
  unit: 'count',
  labelNames: ['result'],
});

export const sqsDeliveries = metricsRegistry.counter({
  name: 'wagering_sqs_deliveries_total',
  help: 'SQS deliveries handled by this consumer process, grouped by terminal outcome.',
  scope: 'process_local',
  unit: 'count',
  labelNames: ['outcome'],
});

export const outboxPublications = metricsRegistry.counter({
  name: 'wagering_outbox_publications_total',
  help: 'Outbox publication steps performed by this publisher process, grouped by outcome.',
  scope: 'process_local',
  unit: 'count',
  labelNames: ['outcome'],
});

export const pendingReferenceSweeps = metricsRegistry.counter({
  name: 'wagering_pending_reference_sweep_total',
  help: 'Pending-reference resolutions attempted by this sweeper process, grouped by outcome.',
  scope: 'process_local',
  unit: 'count',
  labelNames: ['outcome'],
});

export const metricsCollectionErrors = metricsRegistry.counter({
  name: 'wagering_metrics_collection_errors_total',
  help: 'Failures while refreshing an external collector. A non-zero value means the gauges of that scope are stale in this scrape, while the rest of the exposition stays valid.',
  scope: 'process_local',
  unit: 'count',
  labelNames: ['collector'],
});

export type WageringProcessName =
  | 'api'
  | 'sqs-consumer'
  | 'outbox-publisher'
  | 'pending-reference-sweeper'
  | 'test-harness';

let declaredProcess: WageringProcessName | undefined;

/** Declares which process owns the `process_local` metrics of this endpoint. Idempotent. */
export function declareProcess(name: WageringProcessName): void {
  if (declaredProcess === name) {
    return;
  }
  declaredProcess = name;
  processInfo.reset();
  processInfo.set({ process: name }, 1);
}

/** Measures a real unit of work. The outcome label is derived by the caller, never from money. */
export async function observeDuration<T>(
  operation: string,
  outcomeOf: (result: T) => string,
  run: () => Promise<T>,
): Promise<T> {
  const startedAt = process.hrtime.bigint();
  try {
    const result = await run();
    processingDuration.observe(
      { operation, outcome: outcomeOf(result) },
      Number(process.hrtime.bigint() - startedAt) / 1e9,
    );
    return result;
  } catch (error) {
    processingDuration.observe(
      { operation, outcome: 'error' },
      Number(process.hrtime.bigint() - startedAt) / 1e9,
    );
    throw error;
  }
}

export async function renderMetrics(): Promise<string> {
  return metricsRegistry.render((collector) => {
    metricsCollectionErrors.inc({ collector });
  });
}
