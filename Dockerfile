FROM oven/bun:1.3.2 AS dependencies
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

FROM dependencies AS build
COPY nest-cli.json tsconfig.json tsconfig.build.json mikro-orm.config.ts ./
COPY src ./src
RUN bun run build

FROM oven/bun:1.3.2-slim
WORKDIR /app
ENV NODE_ENV=production
COPY --from=dependencies /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
EXPOSE 3000
CMD ["bun", "run", "dist/main.js"]
