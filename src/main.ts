import 'reflect-metadata';

import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';

import { AppModule } from './app.module.js';
import { getAppConfig } from './infrastructure/config.js';

async function bootstrap(): Promise<void> {
  const config = getAppConfig();
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  app.enableShutdownHooks();
  await app.listen(config.port, '0.0.0.0');

  Logger.log(`HTTP server listening on port ${config.port}`, 'Bootstrap');
}

void bootstrap();
