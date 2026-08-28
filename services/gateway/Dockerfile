FROM oven/bun:1.3.14-alpine AS build

WORKDIR /app/services/gateway
COPY services/gateway/package.json services/gateway/bun.lock services/gateway/tsconfig.json ./
COPY services/gateway/packages ./packages
COPY internal/ts-observability /app/internal/ts-observability
COPY database /app/database
RUN bun install --frozen-lockfile
RUN bun run build

FROM oven/bun:1.3.14-alpine

WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/services/gateway/package.json ./package.json
COPY --from=build /app/services/gateway/node_modules ./node_modules
COPY --from=build /app/services/gateway/dist ./dist
USER 1000:1000
CMD ["bun", "/app/dist/provider-gateway/src-command.js"]
