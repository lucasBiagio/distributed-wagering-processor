/**
 * Legacy CP3B entrypoint path kept for the existing integration tests that spawn
 * `bun scripts/cp3b-outbox-publisher.ts`.
 *
 * It delegates to the single real implementation in `src/outbox-publisher.ts`, which is the file
 * the production image runs as `dist/outbox-publisher.js`. No behaviour lives here: claims,
 * lease/fencing, retry, crash recovery and FIFO ordering have exactly one code path.
 */
import '../src/outbox-publisher.js';
