import { afterEach, describe, expect, test } from 'bun:test';
import { once } from 'node:events';
import { createServer, type Server } from 'node:http';

import { startWorkerMetricsServer } from '../../src/infrastructure/observability/worker-metrics-server.js';

const ports = [31_401, 31_402, 31_403];
const servers: Server[] = [];

async function close(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => error === undefined ? resolve() : reject(error));
  });
}

afterEach(async () => {
  await Promise.all(servers.splice(0).map(close));
});

describe('worker metrics server', () => {
  test('binds before reporting readiness and exposes liveness plus Prometheus text', async () => {
    const server = await startWorkerMetricsServer('sqs-consumer', ports[0]!);
    if (server === undefined) {
      throw new Error('WORKER_METRICS_PORT must start the listener');
    }
    servers.push(server);

    const health = await fetch(`http://127.0.0.1:${ports[0]}/health/live`);
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ status: 'ok', process: 'sqs-consumer' });

    const metrics = await fetch(`http://127.0.0.1:${ports[0]}/metrics`);
    expect(metrics.status).toBe(200);
    expect(metrics.headers.get('content-type')).toContain('text/plain; version=0.0.4');
    expect(await metrics.text()).toContain('wagering_process_info{process="sqs-consumer"} 1');
  });

  test('rejects a port collision before a worker can begin processing', async () => {
    const reserved = createServer();
    reserved.listen(ports[1], '127.0.0.1');
    await once(reserved, 'listening');
    servers.push(reserved);

    await expect(startWorkerMetricsServer('outbox-publisher', ports[1]!)).rejects.toMatchObject({ code: 'EADDRINUSE' });
  });
});
