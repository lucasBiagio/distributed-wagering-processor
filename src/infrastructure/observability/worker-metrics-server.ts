import { createServer, type Server } from 'node:http';

import { declareProcess, renderMetrics, type WageringProcessName } from './wagering-metrics.js';

/**
 * Worker metrics exposure strategy (CP4-R3 GATE 2, explicit and verifiable).
 *
 * The consumer, the publisher and the sweeper are separate OS processes. Their
 * counters/histograms live in their own heap, so the API's `/metrics` physically cannot see
 * them, and summing them into a shared table would mean one extra database write per
 * financial event — which CP4-R3 forbids and which would couple the financial unit of work
 * to observability.
 *
 * The chosen strategy is therefore the standard Prometheus one: **each process exposes its
 * own endpoint**. The scraper targets four endpoints and aggregates at query time, where
 * aggregation is actually correct. Global state (transactions by status, outbox lag, DLQ
 * depth, reconciliation divergence) is identical on every endpoint because it is recomputed
 * from PostgreSQL and SQS rather than accumulated locally.
 *
 * The listener is enabled only when `WORKER_METRICS_PORT` is set, so a deployment that does
 * not scrape workers pays nothing, and an operator can never confuse an absent listener with
 * a healthy one: `/metrics` and `/health/live` answer on the same port or not at all.
 */
export async function startWorkerMetricsServer(
  processName: WageringProcessName,
  port: number | undefined,
): Promise<Server | undefined> {
  if (port === undefined) {
    return undefined;
  }

  declareProcess(processName);

  const server = createServer((request, response) => {
    const path = (request.url ?? '/').split('?')[0];
    if (request.method !== 'GET') {
      response.writeHead(405, { 'content-type': 'text/plain; charset=utf-8' });
      response.end('method not allowed\n');
      return;
    }
    if (path === '/health/live') {
      response.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
      response.end(`${JSON.stringify({ status: 'ok', process: processName })}\n`);
      return;
    }
    if (path !== '/metrics') {
      response.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
      response.end('not found\n');
      return;
    }
    renderMetrics()
      .then((body) => {
        response.writeHead(200, {
          'content-type': 'text/plain; version=0.0.4; charset=utf-8',
          'cache-control': 'no-store',
        });
        response.end(body);
      })
      .catch(() => {
        // Never let an exposition failure take the worker down.
        response.writeHead(500, { 'content-type': 'text/plain; charset=utf-8' });
        response.end('metrics rendering failed\n');
      });
  });

  await new Promise<void>((resolve, reject) => {
    const rejectOnError = (error: Error): void => {
      server.off('listening', resolve);
      reject(error);
    };
    server.once('error', rejectOnError);
    server.once('listening', () => {
      server.off('error', rejectOnError);
      resolve();
    });
    server.listen(port, '0.0.0.0');
  });
  process.stdout.write(`${JSON.stringify({
    event: 'worker_metrics_server_started',
    process: processName,
    port,
    pid: process.pid,
  })}\n`);
  // The listener must never keep the process alive on its own: shutdown is owned by the worker.
  server.unref();
  return server;
}

export async function closeWorkerMetricsServer(server: Server | undefined): Promise<void> {
  if (server === undefined || !server.listening) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    server.close((error) => error === undefined ? resolve() : reject(error));
  });
}
