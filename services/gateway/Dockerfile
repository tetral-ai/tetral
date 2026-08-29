FROM docker.io/oven/bun:1.3.14-alpine@sha256:5acc90a93e91ff07bf72aa90a7c9f0fa189765aec90b47bdbf2152d2196383c0 AS build

WORKDIR /app/services/gateway
COPY services/gateway/package.json services/gateway/bun.lock services/gateway/tsconfig.json ./
COPY services/gateway/packages ./packages
COPY internal/ts-observability /app/internal/ts-observability
COPY database /app/database
RUN bun install --frozen-lockfile
RUN bun run build

FROM docker.io/oven/bun:1.3.14-alpine@sha256:5acc90a93e91ff07bf72aa90a7c9f0fa189765aec90b47bdbf2152d2196383c0

WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/services/gateway/package.json ./package.json
COPY --from=build /app/services/gateway/node_modules ./node_modules
COPY --from=build /app/services/gateway/dist ./dist
USER 1000:1000
CMD ["bun", "/app/dist/provider-gateway/src-command.js"]
