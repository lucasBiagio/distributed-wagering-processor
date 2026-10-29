import { Controller, Get, Header, Inject, type OnModuleInit } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/core';
import { type SQSClient } from '@aws-sdk/client-sqs';

import { SQS_CLIENT } from '../../../infrastructure/messaging/sqs.module.js';
import { registerDatabaseMetricsCollector } from '../../../infrastructure/observability/database-metrics-collector.js';
import { registerSqsMetricsCollector } from '../../../infrastructure/observability/sqs-metrics-collector.js';
import { declareProcess, renderMetrics } from '../../../infrastructure/observability/wagering-metrics.js';

/**
 * Prometheus exposition endpoint of the API process.
 *
 * Contract: `GET /metrics` → `200 text/plain; version=0.0.4`, Prometheus text format, no
 * query parameters, no side effects on financial state. The response contains counts,
 * durations and queue depths only — never a payload, an identifier, an amount or a
 * credential — which is exactly why it can stay unauthenticated like the health probes.
 *
 * Operational note documented in README/ARCHITECTURE: the endpoint is intended for an
 * in-cluster scraper. It should not be published to the internet through the provider-facing
 * ingress, because queue depth and transaction counts are internal operational data; no
 * extra authentication scheme is introduced here, as CP4-R3 forbids it.
 */
@Controller('metrics')
export class MetricsController implements OnModuleInit {
  public constructor(
    private readonly orm: MikroORM,
    @Inject(SQS_CLIENT) private readonly sqs: SQSClient,
  ) {}

  public onModuleInit(): void {
    declareProcess('api');
    registerDatabaseMetricsCollector(this.orm);
    registerSqsMetricsCollector(this.sqs);
  }

  @Get()
  @Header('content-type', 'text/plain; version=0.0.4; charset=utf-8')
  @Header('cache-control', 'no-store')
  public async scrape(): Promise<string> {
    return renderMetrics();
  }
}
