import 'reflect-metadata';

import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';

import { MikroOrmFinancialProcessing } from '../../src/infrastructure/persistence/mikro-orm-financial-processing.js';
import { FinancialController } from '../../src/interfaces/http/financial/financial.controller.js';

const port = Number(process.env.CP4_HTTP_ERROR_PROBE_PORT ?? 3129);
const kind = process.env.CP4_HTTP_ERROR_PROBE_KIND;

if (kind !== 'transient' && kind !== 'unknown') {
  throw new Error('CP4_HTTP_ERROR_PROBE_KIND must be transient or unknown');
}

function errorForProbe(): Error & { code?: string } {
  if (kind === 'transient') {
    return Object.assign(new Error('test-only PostgreSQL connection loss'), { code: '08006' });
  }
  return new Error('test-only unclassified failure');
}

/**
 * This isolated process exercises the real Nest HTTP exception boundary. It intentionally
 * substitutes only the terminal processor call: the normal persisted-outcome tests run against
 * PostgreSQL, while a deterministic throw is the only way to prove the controller's 503-vs-500
 * classification without taking down the shared disposable database underneath its own runner.
 */
const failingProcessing = {
  async processBet(): Promise<never> {
    throw errorForProbe();
  },
} as unknown as MikroOrmFinancialProcessing;

@Module({
  controllers: [FinancialController],
  providers: [{ provide: MikroOrmFinancialProcessing, useValue: failingProcessing }],
})
class ErrorProbeModule {}

const app = await NestFactory.create(ErrorProbeModule, { logger: false });
await app.listen(port, '127.0.0.1');
process.stdout.write('ready\n');
