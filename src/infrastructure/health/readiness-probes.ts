import { Injectable, Inject } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/core';
import { ListQueuesCommand, type SQSClient } from '@aws-sdk/client-sqs';

import type { ReadinessProbe } from '../../interfaces/http/health/health.service.js';
import { SQS_CLIENT } from '../messaging/sqs.module.js';

@Injectable()
export class PostgresReadinessProbe implements ReadinessProbe {
  readonly name = 'postgres' as const;

  constructor(private readonly orm: MikroORM) {}

  async check(): Promise<void> {
    await this.orm.em.getConnection().execute('select 1');
  }
}

@Injectable()
export class SqsReadinessProbe implements ReadinessProbe {
  readonly name = 'sqs' as const;

  constructor(@Inject(SQS_CLIENT) private readonly client: SQSClient) {}

  async check(): Promise<void> {
    await this.client.send(new ListQueuesCommand({}));
  }
}
