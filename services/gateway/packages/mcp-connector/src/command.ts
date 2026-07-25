/**
 * @packageDocumentation
 *
 * Composes the MCP connector process from configuration, schema access,
 * workload authentication, Bridge clients, the MCP SDK client, and its gRPC
 * and operations listeners. Startup keeps readiness false until prerequisite
 * material is validated and both listeners are bound; shutdown withdraws
 * readiness before stopping listeners, cached MCP clients, and SQL access.
 * The Bun executable entry point and startup tests call this module, which in
 * turn wires the package's config, auth, service, server, logging, and metrics
 * modules together with Gateway protocol and schema helpers.
 */

import { authenticateMcpCaller, KubernetesTokenReviewClient, validateKubernetesTokenReviewReviewerMaterial } from "./auth.js";
import { BridgeAPIManifestChangeNotifier, BridgeAPIMcpToolResultIdempotencyStore } from "./bridge-client.js";
import { McpSDKClient } from "./client.js";
import { loadMcpConnectorConfigFromProcessEnv } from "./config.js";
import { SQLGitHubMcpCredentialResolver } from "./credential.js";
import { createMcpConnectorHttpServer } from "./http-server.js";
import { createJsonLogger, startupFailureLogRecord } from "./logger.js";
import { McpConnectorMetricsRegistry } from "./metrics.js";
import { createMcpConnectorGrpcServer } from "./server.js";
import { McpConnectorServiceShell } from "./service.js";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import { verifyPostgreSQLSchema } from "../../schema/src/verify.js";
import type { McpClient, McpManifestChangeNotifier } from "./service.js";
import type { SchemaSQL } from "../../schema/src/verify.js";

/**
 * Starts the MCP connector process and keeps it alive until the supplied wait
 * operation settles.
 *
 * Production uses process environment configuration and concrete MCP, Bridge,
 * TokenReview, and SQL dependencies. Optional collaborators expose the same
 * composition boundary to startup tests. The process becomes ready only after
 * schema and reviewer-material checks pass and both listeners bind.
 */
export async function runMcpConnectorCommand(options: {
  readonly bindAddress?: string | undefined;
  readonly httpBindAddress?: string | undefined;
  readonly waitForever?: () => Promise<never>;
  readonly client?: McpClient | undefined;
  readonly manifestChangeNotifier?: McpManifestChangeNotifier | undefined;
  readonly sql?: (SchemaSQL & { readonly close?: (options?: { readonly timeout?: number }) => Promise<void> }) | undefined;
  readonly sqlFactory?: ((options: Bun.SQL.PostgresOrMySQLOptions) => SchemaSQL & { readonly close?: (options?: { readonly timeout?: number }) => Promise<void> }) | undefined;
  readonly schemaVerifier?: ((sql: SchemaSQL) => Promise<void>) | undefined;
} = {}): Promise<void> {
  const config = loadMcpConnectorConfigFromProcessEnv();
  const startupLogger = createJsonLogger({ write: (line) => process.stderr.write(line) });
  if (!config.ok) {
    startupLogger.error(startupFailureLogRecord(config.error));
    throw new Error("mcp connector config error");
  }
  const logger = createJsonLogger({
    write: (line) => process.stderr.write(line),
    deploymentEnvironment: config.config.deploymentEnvironment,
    serviceVersion: config.config.serviceVersion,
  });
  const metrics = new McpConnectorMetricsRegistry();
  let ready = false;
  let service: McpConnectorServiceShell;
  const sqlOptions = databasePoolOptions(config.config);
  const sql = options.sql ?? options.sqlFactory?.(sqlOptions) ?? new Bun.SQL(sqlOptions);
  try {
    await (options.schemaVerifier ?? verifyPostgreSQLSchema)(sql);
  } catch (error) {
    await sql.close?.({ timeout: 1 });
    throw error;
  }
  const client = options.client ?? new McpSDKClient({
    credentialResolver: new SQLGitHubMcpCredentialResolver(sql, config.config.vaultKeyHex),
    logger,
    onRefreshAttempt: (outcome) => {
      metrics.recordRefreshAttempt(outcome);
    },
    onToolsListChanged: async (input) => {
      await service.handleToolsListChangedNotification(input);
    },
  });
  const tokenReviewClient = new KubernetesTokenReviewClient({
    apiServerUrl: config.config.kubernetesApiServerUrl,
    reviewerTokenPath: config.config.tokenReviewReviewerTokenPath,
    apiServerCaCertPath: config.config.kubernetesApiCaCertPath,
  });
  service = new McpConnectorServiceShell({
    ready: () => ready,
    client,
    logger,
    runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({
      hmacKey: config.config.runtimeBindingTokenHMACKey,
    }),
    metrics,
    idempotencyStore: new BridgeAPIMcpToolResultIdempotencyStore({
      address: config.config.bridgeApiGrpcAddress,
      tokenPath: config.config.bridgeTokenPath,
    }),
    activeSessionCount: () => typeof client.connectionCount === "function" ? client.connectionCount() : 0,
    manifestChangeNotifier: options.manifestChangeNotifier ?? new BridgeAPIManifestChangeNotifier({
      address: config.config.bridgeApiGrpcAddress,
      tokenPath: config.config.bridgeTokenPath,
    }),
    authenticator: {
      authenticate: async ({ metadata, method }) =>
        await authenticateMcpCaller({
          metadata,
          method,
          tokenReviewClient,
          allowedRuntimePod: {
            namespace: config.config.allowedRuntimePod.namespace,
            name: config.config.allowedRuntimePod.serviceAccount,
          },
          allowedBridge: {
            namespace: config.config.allowedBridge.namespace,
            name: config.config.allowedBridge.serviceAccount,
          },
        }),
    },
  });
  const server = createMcpConnectorGrpcServer(service);
  let httpServer: ReturnType<typeof createMcpConnectorHttpServer> | undefined;
  await validateKubernetesTokenReviewReviewerMaterial({
    reviewerTokenPath: config.config.tokenReviewReviewerTokenPath,
    apiServerCaCertPath: config.config.kubernetesApiCaCertPath,
  });
  await server.bind(options.bindAddress ?? config.config.grpcBindAddress);
  httpServer = createMcpConnectorHttpServer(options.httpBindAddress ?? config.config.httpBindAddress, {
    health: () => ({ ok: true }),
    ready: () => ({ ready }),
    metricsText: () => service.metricsText(),
  });
  ready = true;
  const shutdown = async () => {
    ready = false;
    await httpServer?.stop();
    await server.shutdown();
    if (client instanceof McpSDKClient) {
      await client.closeAll();
    }
    await sql?.close?.();
  };
  process.once("SIGTERM", () => {
    void shutdown().then(() => process.exit(0));
  });
  process.once("SIGINT", () => {
    void shutdown().then(() => process.exit(0));
  });
  await (options.waitForever ?? waitForever)();
}

function databasePoolOptions(config: Extract<ReturnType<typeof loadMcpConnectorConfigFromProcessEnv>, { readonly ok: true }>["config"]): Bun.SQL.PostgresOrMySQLOptions {
  return {
    url: config.databaseUrl,
    max: config.databasePool.max,
    idleTimeout: config.databasePool.idleTimeout,
    maxLifetime: config.databasePool.maxLifetime,
    connectionTimeout: config.databasePool.connectionTimeout,
    connection: { statement_timeout: config.databasePool.statementTimeoutMs },
  };
}

async function waitForever(): Promise<never> {
  return await new Promise<never>(() => undefined);
}

if (import.meta.main) {
  await runMcpConnectorCommand();
}
