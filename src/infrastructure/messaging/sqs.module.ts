import { Module } from '@nestjs/common';
import { SQSClient } from '@aws-sdk/client-sqs';

import { getAppConfig } from '../config.js';

export const SQS_CLIENT = 'SQS_CLIENT';

@Module({
  providers: [
    {
      provide: SQS_CLIENT,
      useFactory: () => {
        const config = getAppConfig();

        return new SQSClient({
          region: config.awsRegion,
          endpoint: config.sqsEndpoint,
          credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
          },
        });
      },
    },
  ],
  exports: [SQS_CLIENT],
})
export class SqsModule {}
