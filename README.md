# Distributed Wagering Processor

CP1 establishes the executable foundation for the Jungle Gaming backend challenge. The complete original statement is preserved unchanged in [CHALLENGE.md](./CHALLENGE.md) (source commit `c7143e6d041585d6913f56eb7415105e074f83dc`).

## Stack pinned for CP1

| Component | Version |
| --- | --- |
| Bun runtime / package manager / test runner | 1.3.2 |
| TypeScript | 5.9.2 (strict) |
| NestJS | 11.1.6 |
| MikroORM core/PostgreSQL/migrations | 6.5.9 |
| PostgreSQL image | 17.2-alpine |
| LocalStack image | 3.8.1 |
| AWS SDK SQS | 3.806.0 |

## Prerequisites

- Docker Engine with Docker Compose v2.
- Bun 1.3.2 only when running commands outside Docker.

## Start the complete local stack

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
```

The stack starts PostgreSQL, LocalStack SQS, and the NestJS application. LocalStack initializes:

- `wager-transactions.fifo`
- `wager-transactions-dlq.fifo`
- `wager-events.fifo`

`wager-transactions.fifo` has a redrive policy to the DLQ after 5 receives.

## Verify health endpoints

```bash
curl http://localhost:3000/health/live
curl http://localhost:3000/health/ready
```

`/health/live` checks only that the HTTP process is alive. `/health/ready` runs a real `SELECT 1` through MikroORM and a real SQS `ListQueues` request; it returns HTTP 503 if either dependency is unavailable.

## Local development with Bun

Use host-service addresses when starting outside Docker:

```bash
export DATABASE_URL='postgresql://wagering:wagering@localhost:5432/wagering'
export SQS_ENDPOINT='http://localhost:4566'
export AWS_REGION='us-east-1'
export AWS_ACCESS_KEY_ID='test'
export AWS_SECRET_ACCESS_KEY='test'
bun install --frozen-lockfile
bun run start:dev
```

## Quality and migration commands

```bash
bun run typecheck
bun run build
bun test
bun run migration:up
bun run migration:down
```

CP1 intentionally contains no financial entities and therefore no financial migrations. The migration configuration is active and commands are present for CP2 migrations; no placeholder tables are created.

## Configuration

See `.env.example`. `DATABASE_URL` and `SQS_ENDPOINT` are mandatory. The application fails fast if either is absent or `PORT` is invalid.

## Stop and reset local services

```bash
docker compose down -v
```

This removes the disposable local PostgreSQL volume.
