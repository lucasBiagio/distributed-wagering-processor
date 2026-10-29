import { afterEach, describe, expect, test } from 'bun:test';

const PORT = Number(Bun.env.CP4_HTTP_ERROR_CONTRACT_PORT ?? 3129);
const baseUrl = `http://127.0.0.1:${PORT}`;
let probe: ReturnType<typeof Bun.spawn> | undefined;

async function waitForProbe(): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseUrl}/wagering/transactions`, { method: 'POST' });
      if (response.status > 0) return;
    } catch {
      // The freshly spawned Nest process has not bound the port yet.
    }
    await Bun.sleep(100);
  }
  throw new Error('CP4 HTTP error probe did not start');
}

async function request(): Promise<{ readonly status: number; readonly body: Record<string, unknown> }> {
  const response = await fetch(`${baseUrl}/wagering/transactions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': 'cp4-error-contract-key' },
    body: JSON.stringify({
      providerId: 'cp4-error-provider',
      externalTransactionId: 'cp4-error-external',
      walletId: 'cp4-error-wallet',
      playerId: 'cp4-error-player',
      roundId: 'cp4-error-round',
      gameId: 'cp4-error-game',
      kind: 'BET',
      money: { amount: '1.00', currency: 'BRL' },
    }),
  });
  return { status: response.status, body: await response.json() as Record<string, unknown> };
}

function start(kind: 'transient' | 'unknown'): void {
  probe = Bun.spawn(['bun', 'run', 'test/support/cp4-http-error-probe.ts'], {
    cwd: process.cwd(),
    env: {
      ...Bun.env,
      CP4_HTTP_ERROR_PROBE_PORT: String(PORT),
      CP4_HTTP_ERROR_PROBE_KIND: kind,
    },
    stdout: 'ignore',
    stderr: 'ignore',
  });
}

describe('CP4-R3A HTTP error classification contract', () => {
  afterEach(async () => {
    if (probe) {
      probe.kill();
      await probe.exited;
      probe = undefined;
    }
  });

  test('returns 503 only for an explicitly recognised transient infrastructure failure', async () => {
    start('transient');
    await waitForProbe();

    const response = await request();

    expect(response).toMatchObject({ status: 503, body: { failureCode: 'SERVICE_UNAVAILABLE' } });
  });

  test('leaves an unrecognised failure as 500 instead of falsely advertising it as retryable', async () => {
    start('unknown');
    await waitForProbe();

    const response = await request();

    expect(response.status).toBe(500);
    expect(response.body).toMatchObject({ statusCode: 500 });
    expect(response.body.failureCode).toBeUndefined();
  });
});
