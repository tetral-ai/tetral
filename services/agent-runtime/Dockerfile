FROM oven/bun:1.3.14-alpine AS build

# The build context is the repository root: this service's package manifests
# carry file: dependencies that reach out of its own directory — the Gateway's
# protocol package and internal/ts-observability. A context narrowed to
# services/agent-runtime cannot resolve them.
WORKDIR /app/services/agent-runtime
COPY services/agent-runtime/package.json services/agent-runtime/bun.lock services/agent-runtime/tsconfig.json ./
COPY services/agent-runtime/packages ./packages
COPY services/gateway/packages/protocol /app/services/gateway/packages/protocol
COPY internal/ts-observability /app/internal/ts-observability
RUN bun install --frozen-lockfile
RUN bun run build

FROM oven/bun:1.3.14-alpine

WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/services/agent-runtime/package.json ./package.json
COPY --from=build /app/services/agent-runtime/node_modules ./node_modules
COPY --from=build /app/services/agent-runtime/dist ./dist
USER 1000:1000
CMD ["bun", "/app/dist/command.js"]
