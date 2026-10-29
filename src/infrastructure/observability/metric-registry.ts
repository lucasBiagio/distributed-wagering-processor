/**
 * Minimal Prometheus-compatible metric registry.
 *
 * Written in-repo on purpose: the production image ships only `dist` plus `node_modules`, and
 * CP4-R3 explicitly forbids making the test suite depend on an external dashboard or exporter.
 * The exposition format is the Prometheus text format version 0.0.4, which every scraper and
 * `promtool` accept.
 *
 * Three invariants are enforced by construction instead of by review:
 *
 * 1. **No high-cardinality identifiers as labels.** `walletId`, `playerId`, `messageId` and
 *    `transactionId` (in any casing/underscore form) are rejected as label *names*, and any
 *    label *value* that looks like a UUID is rejected too. A violation throws at registration
 *    or observation time, so it cannot reach a scrape.
 * 2. **No money as a measurement.** Histograms are seconds-only (`unit: 'seconds'`) and the
 *    registry refuses to register a histogram whose unit is not a duration unit.
 * 3. **Declared scope.** Every metric declares whether it is derived from PostgreSQL (global
 *    and consistent across instances), read approximately from SQS, or a process-local
 *    counter/histogram. The scope is rendered as a `# SCOPE` comment next to the metric, so
 *    the distinction is machine-verifiable and not only documentation prose.
 */

export type MetricScope = 'global_postgresql' | 'approximate_sqs' | 'process_local';

export type MetricType = 'counter' | 'gauge' | 'histogram';

export type MetricUnit = 'count' | 'seconds' | 'info';

export type MetricLabels = Readonly<Record<string, string>>;

/**
 * Identifier-shaped label names are refused outright. The list covers the four names CP4-R3
 * forbids plus the other per-transaction identifiers that would explode cardinality the same
 * way (an idempotency key is as unique as a transaction id).
 */
const FORBIDDEN_LABEL_NAMES: ReadonlySet<string> = new Set([
  'walletid',
  'playerid',
  'messageid',
  'transactionid',
  'eventid',
  'idempotencykey',
  'externaltransactionid',
  'referenceexternaltransactionid',
  'correlationid',
  'causationid',
  'claimtoken',
  'roundid',
  'payloadhash',
]);

const METRIC_NAME_PATTERN = /^[a-z][a-z0-9_]*$/;
const LABEL_NAME_PATTERN = /^[a-z][a-z0-9_]*$/;
const LABEL_VALUE_PATTERN = /^[A-Za-z0-9_.:\-/]{1,64}$/;
const UUID_LIKE_PATTERN = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;

function normalizeLabelName(name: string): string {
  return name.toLowerCase().replace(/_/g, '');
}

export class MetricContractError extends Error {
  public constructor(message: string) {
    super(message);
    this.name = 'MetricContractError';
  }
}

function assertMetricName(name: string): void {
  if (!METRIC_NAME_PATTERN.test(name)) {
    throw new MetricContractError(`Metric name ${name} is not a valid Prometheus metric name`);
  }
}

function assertLabelNames(metric: string, labelNames: readonly string[]): void {
  for (const label of labelNames) {
    if (!LABEL_NAME_PATTERN.test(label)) {
      throw new MetricContractError(`Metric ${metric} declares an invalid label name ${label}`);
    }
    if (FORBIDDEN_LABEL_NAMES.has(normalizeLabelName(label))) {
      throw new MetricContractError(
        `Metric ${metric} declares the high-cardinality identifier label ${label}, which is forbidden`,
      );
    }
  }
}

function assertLabelValues(metric: string, labelNames: readonly string[], labels: MetricLabels): void {
  const provided = Object.keys(labels);
  if (provided.length !== labelNames.length || provided.some((name) => !labelNames.includes(name))) {
    throw new MetricContractError(
      `Metric ${metric} expects labels [${labelNames.join(', ')}] but received [${provided.join(', ')}]`,
    );
  }
  for (const name of labelNames) {
    const value = labels[name] ?? '';
    if (!LABEL_VALUE_PATTERN.test(value)) {
      throw new MetricContractError(`Metric ${metric} label ${name} has an unsupported value shape`);
    }
    if (UUID_LIKE_PATTERN.test(value)) {
      throw new MetricContractError(
        `Metric ${metric} label ${name} carries a UUID-shaped value, which is forbidden`,
      );
    }
  }
}

function serializeLabels(labelNames: readonly string[], labels: MetricLabels): string {
  if (labelNames.length === 0) {
    return '';
  }
  const rendered = labelNames
    .map((name) => `${name}="${(labels[name] ?? '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`)
    .join(',');
  return `{${rendered}}`;
}

function labelKey(labelNames: readonly string[], labels: MetricLabels): string {
  return labelNames.map((name) => labels[name] ?? '').join('\u0000');
}

export interface MetricDefinition {
  readonly name: string;
  readonly help: string;
  readonly scope: MetricScope;
  readonly unit: MetricUnit;
  readonly labelNames?: readonly string[];
}

abstract class Metric {
  public readonly name: string;
  public readonly help: string;
  public readonly scope: MetricScope;
  public readonly unit: MetricUnit;
  public readonly labelNames: readonly string[];

  protected constructor(definition: MetricDefinition) {
    assertMetricName(definition.name);
    assertLabelNames(definition.name, definition.labelNames ?? []);
    this.name = definition.name;
    this.help = definition.help;
    this.scope = definition.scope;
    this.unit = definition.unit;
    this.labelNames = definition.labelNames ?? [];
  }

  public abstract readonly type: MetricType;

  public abstract render(): readonly string[];

  protected validate(labels: MetricLabels): void {
    assertLabelValues(this.name, this.labelNames, labels);
  }

  protected header(): readonly string[] {
    return [
      `# HELP ${this.name} ${this.help}`,
      `# TYPE ${this.name} ${this.type}`,
      `# UNIT ${this.name} ${this.unit}`,
      `# SCOPE ${this.name} ${this.scope}`,
    ];
  }
}

export class Counter extends Metric {
  public readonly type = 'counter' as const;
  private readonly values = new Map<string, { labels: MetricLabels; value: number }>();

  public constructor(definition: MetricDefinition) {
    super(definition);
  }

  public inc(labels: MetricLabels = {}, amount = 1): void {
    if (!Number.isFinite(amount) || amount < 0) {
      throw new MetricContractError(`Counter ${this.name} can only be incremented by a finite non-negative amount`);
    }
    this.validate(labels);
    const key = labelKey(this.labelNames, labels);
    const existing = this.values.get(key);
    if (existing) {
      existing.value += amount;
      return;
    }
    this.values.set(key, { labels, value: amount });
  }

  public get(labels: MetricLabels = {}): number {
    return this.values.get(labelKey(this.labelNames, labels))?.value ?? 0;
  }

  public render(): readonly string[] {
    const lines = [...this.header()];
    for (const entry of this.values.values()) {
      lines.push(`${this.name}${serializeLabels(this.labelNames, entry.labels)} ${entry.value}`);
    }
    return lines;
  }
}

export class Gauge extends Metric {
  public readonly type = 'gauge' as const;
  private readonly values = new Map<string, { labels: MetricLabels; value: number }>();

  public constructor(definition: MetricDefinition) {
    super(definition);
  }

  public set(labels: MetricLabels, value: number): void {
    if (!Number.isFinite(value)) {
      throw new MetricContractError(`Gauge ${this.name} cannot be set to a non-finite value`);
    }
    this.validate(labels);
    this.values.set(labelKey(this.labelNames, labels), { labels, value });
  }

  /**
   * Database-derived gauges are fully recomputed on every scrape: resetting first means a
   * label combination that no longer exists in PostgreSQL disappears from the exposition
   * instead of being reported forever with a stale value.
   */
  public reset(): void {
    this.values.clear();
  }

  public get(labels: MetricLabels = {}): number | undefined {
    return this.values.get(labelKey(this.labelNames, labels))?.value;
  }

  public render(): readonly string[] {
    const lines = [...this.header()];
    for (const entry of this.values.values()) {
      lines.push(`${this.name}${serializeLabels(this.labelNames, entry.labels)} ${entry.value}`);
    }
    return lines;
  }
}

interface HistogramSeries {
  readonly labels: MetricLabels;
  readonly counts: number[];
  sum: number;
  count: number;
}

export class Histogram extends Metric {
  public readonly type = 'histogram' as const;
  public readonly buckets: readonly number[];
  private readonly series = new Map<string, HistogramSeries>();

  public constructor(definition: MetricDefinition & { readonly buckets: readonly number[] }) {
    super(definition);
    if (definition.unit !== 'seconds') {
      throw new MetricContractError(
        `Histogram ${definition.name} must measure seconds; monetary values are never valid observations`,
      );
    }
    if (definition.buckets.length === 0 || definition.buckets.some((bucket, index, all) => index > 0 && bucket <= all[index - 1]!)) {
      throw new MetricContractError(`Histogram ${definition.name} needs strictly increasing buckets`);
    }
    this.buckets = definition.buckets;
  }

  public observe(labels: MetricLabels, seconds: number): void {
    if (!Number.isFinite(seconds) || seconds < 0) {
      throw new MetricContractError(`Histogram ${this.name} only accepts finite non-negative durations`);
    }
    this.validate(labels);
    const key = labelKey(this.labelNames, labels);
    const series = this.series.get(key) ?? {
      labels,
      counts: this.buckets.map(() => 0),
      sum: 0,
      count: 0,
    };
    for (const [index, bucket] of this.buckets.entries()) {
      if (seconds <= bucket) {
        series.counts[index] = (series.counts[index] ?? 0) + 1;
      }
    }
    series.sum += seconds;
    series.count += 1;
    this.series.set(key, series);
  }

  public count(labels: MetricLabels): number {
    return this.series.get(labelKey(this.labelNames, labels))?.count ?? 0;
  }

  public sum(labels: MetricLabels): number {
    return this.series.get(labelKey(this.labelNames, labels))?.sum ?? 0;
  }

  public render(): readonly string[] {
    const lines = [...this.header(), `# BUCKETS ${this.name} ${this.buckets.join(',')}`];
    for (const series of this.series.values()) {
      for (const [index, bucket] of this.buckets.entries()) {
        const labels = serializeLabels([...this.labelNames, 'le'], { ...series.labels, le: String(bucket) });
        lines.push(`${this.name}_bucket${labels} ${series.counts[index] ?? 0}`);
      }
      const infinite = serializeLabels([...this.labelNames, 'le'], { ...series.labels, le: '+Inf' });
      lines.push(`${this.name}_bucket${infinite} ${series.count}`);
      lines.push(`${this.name}_sum${serializeLabels(this.labelNames, series.labels)} ${series.sum}`);
      lines.push(`${this.name}_count${serializeLabels(this.labelNames, series.labels)} ${series.count}`);
    }
    return lines;
  }
}

export type MetricCollector = () => Promise<void> | void;

export class MetricRegistry {
  private readonly metrics: Metric[] = [];
  private readonly collectors: Array<{ readonly name: string; readonly collect: MetricCollector }> = [];

  public counter(definition: MetricDefinition): Counter {
    return this.register(new Counter(definition));
  }

  public gauge(definition: MetricDefinition): Gauge {
    return this.register(new Gauge(definition));
  }

  public histogram(definition: MetricDefinition & { readonly buckets: readonly number[] }): Histogram {
    return this.register(new Histogram(definition));
  }

  /**
   * A collector refreshes gauges from an external source (PostgreSQL, SQS) right before a
   * scrape. A failing collector must never blank the whole endpoint: the failure is counted
   * and the remaining metrics are still exposed, which is the degraded mode an operator can
   * actually diagnose.
   */
  public addCollector(name: string, collect: MetricCollector): void {
    this.collectors.push({ name, collect });
  }

  public async render(onCollectorError?: (name: string, error: unknown) => void): Promise<string> {
    for (const collector of this.collectors) {
      try {
        await collector.collect();
      } catch (error) {
        onCollectorError?.(collector.name, error);
      }
    }
    const lines: string[] = [];
    for (const metric of this.metrics) {
      lines.push(...metric.render());
    }
    return `${lines.join('\n')}\n`;
  }

  public describe(): ReadonlyArray<{ readonly name: string; readonly type: MetricType; readonly scope: MetricScope; readonly unit: MetricUnit }> {
    return this.metrics.map((metric) => ({
      name: metric.name,
      type: metric.type,
      scope: metric.scope,
      unit: metric.unit,
    }));
  }

  private register<T extends Metric>(metric: T): T {
    if (this.metrics.some((existing) => existing.name === metric.name)) {
      throw new MetricContractError(`Metric ${metric.name} is already registered`);
    }
    this.metrics.push(metric);
    return metric;
  }
}
