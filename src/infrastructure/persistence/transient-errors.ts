/**
 * Transient infrastructure failure classification.
 *
 * CP4-R3 requires the API to answer 503 only when the failure is *effectively identified* as
 * a transient unavailability, and explicitly forbids collapsing every unknown exception into
 * 503 — a programming bug answered with "retry me" makes the provider replay a request that
 * will never succeed and hides the defect.
 *
 * The allow-list below therefore contains only states that PostgreSQL (or the driver) raises
 * when the *connection or the server* is unavailable or overloaded, plus the two concurrency
 * states that remain after the bounded in-process retry budget is exhausted. Anything else —
 * a TypeError, a constraint we failed to anticipate, a null dereference — is deliberately
 * left unclassified and surfaces as 500.
 */

/** PostgreSQL deadlock and serialization failure: the whole unit of work was rolled back. */
export const TRANSIENT_SQLSTATES: ReadonlySet<string> = new Set(['40001', '40P01']);

/**
 * Class 08 — connection exception; 53xxx — insufficient resources; 57P0x — operator or
 * server induced termination; 58030 — I/O error. All of them mean "the database was not
 * able to serve this request now", never "this request is wrong".
 */
const TRANSIENT_INFRASTRUCTURE_SQLSTATES: ReadonlySet<string> = new Set([
  '08000', '08001', '08003', '08004', '08006', '08007', '08P01',
  '53000', '53100', '53200', '53300', '53400',
  '57P01', '57P02', '57P03', '57P05',
  '58030',
]);

/** Node/undici level failures reaching us before any SQL was executed. */
const TRANSIENT_SYSTEM_CODES: ReadonlySet<string> = new Set([
  'ECONNREFUSED', 'ECONNRESET', 'EPIPE', 'ETIMEDOUT', 'EHOSTUNREACH', 'ENETUNREACH', 'ENOTFOUND',
]);

export function sqlState(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) {
    return undefined;
  }
  const candidate = error as { code?: unknown; cause?: { code?: unknown } };
  if (typeof candidate.code === 'string') {
    return candidate.code;
  }
  return typeof candidate.cause?.code === 'string' ? candidate.cause.code : undefined;
}

export function transientConflict(error: unknown): boolean {
  const state = sqlState(error);
  return state !== undefined && TRANSIENT_SQLSTATES.has(state);
}

/**
 * True only for an explicitly recognised transient unavailability. A retriable concurrency
 * conflict that already consumed its attempt budget counts as transient as well: the unit of
 * work was rolled back by PostgreSQL, so no financial effect was produced and the provider
 * may safely resubmit the same Idempotency-Key.
 */
export function isTransientInfrastructureError(error: unknown): boolean {
  const state = sqlState(error);
  if (state !== undefined && (TRANSIENT_INFRASTRUCTURE_SQLSTATES.has(state) || TRANSIENT_SQLSTATES.has(state))) {
    return true;
  }
  if (state !== undefined && TRANSIENT_SYSTEM_CODES.has(state)) {
    return true;
  }
  const name = error instanceof Error ? error.name : undefined;
  return name === 'TimeoutError';
}
