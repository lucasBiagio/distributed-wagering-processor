/**
 * Legacy CP3A entrypoint path kept for the existing integration tests that spawn
 * `bun run scripts/cp3a-sqs-consumer.ts`.
 *
 * It delegates to the single real implementation in `src/sqs-consumer.ts`, which is the file the
 * production image runs as `dist/sqs-consumer.js`. No behaviour lives here: there is exactly one
 * functional consumer code path (ACK only after commit, deterministic crash hooks, DLQ, SIGTERM).
 */
import '../src/sqs-consumer.js';
