import { createDiagnosticStreamSink, diagnosticMetricsText, processFailureLogRecord, registerProcessSignalHandlers, runProcessEntry } from "@tetral/ts-observability";
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
import { createJsonLogger, logWorkloadStarted, recordMcpOAuthRefreshCompleted, startupFailureLogRecord } from "./logger.js";
import { McpConnectorMetricsRegistry } from "./metrics.js";
import { createMcpConnectorGrpcServer } from "./server.js";
import { McpConnectorServiceShell } from "./service.js";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import { verifyPostgreSQLReadiness } from "../../schema/src/verify.js";
import type { McpClient, McpManifestChangeNotifier } from "./service.js";
import type { SchemaSQL } from "../../schema/src/verify.js";
import type { McpConnectorHttpServer } from "./http-server.js";
import type { ProcessFailurePhase } from "@tetral/ts-observability";
import type { McpConnectorLogRecord, McpConnectorLogger } from "./logger.js";
import type { McpConnectorGrpcServer } from "./server.js";

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
  readonly logger?: McpConnectorLogger | undefined;
  readonly reviewerMaterialValidator?: typeof validateKubernetesTokenReviewReviewerMaterial | undefined;
  readonly serverFactory?: ((service: McpConnectorServiceShell) => McpConnectorGrpcServer) | undefined;
  readonly httpServerFactory?: ((
    address: string,
    state: Parameters<typeof createMcpConnectorHttpServer>[1],
  ) => McpConnectorHttpServer) | undefined;
  readonly registerSignalHandlers?: ((shutdown: () => Promise<void>) => void) | undefined;
} = {}): Promise<void> {
  const diagnosticSink = options.logger === undefined ? createDiagnosticStreamSink(process.stderr) : undefined;
  const diagnosticOwners: object[] = [];
  const diagnosticReleases: (() => void)[] = [];
  const registerDiagnosticCleanup = (owner: object): void => {
    if (diagnosticOwners.includes(owner)) return;
    diagnosticOwners.push(owner);
    try { if ("flush" in owner && typeof owner.flush === "function") diagnosticReleases.push(owner.flush.bind(owner)); } catch { /* best effort */ }
  };
  let logger: McpConnectorLogger | undefined;
  const report = (record: McpConnectorLogRecord): void => { try { logger?.error(record); } catch { /* observability does not own lifecycle */ } };
  const closes: { phase: ProcessFailurePhase; close: () => void | Promise<void> }[] = [];
  let ready = false, stopping: Promise<void> | undefined;
  const shutdown = (): Promise<void> => {
    ready = false;
    if (stopping !== undefined) return stopping;
    let resolveShutdown!: () => void, rejectShutdown!: (error: unknown) => void;
    stopping = new Promise<void>((resolve, reject) => { resolveShutdown = resolve; rejectShutdown = reject; });
    void (async () => {
      let failed = false;
      let firstFailure: unknown;
      try {
        for (const step of closes.slice().reverse()) {
          try {
            await step.close();
          } catch (error) {
            if (!failed) firstFailure = error;
            failed = true;
            report(processFailureLogRecord(step.phase, true));
          }
        }
      } finally {
        for (const release of diagnosticReleases) { try { release(); } catch { /* best effort */ } }
        diagnosticSink?.close();
      }
      if (failed) throw firstFailure;
    })().then(resolveShutdown, rejectShutdown);
    return stopping;
  };
  let phase: ProcessFailurePhase = "configuration", failed = false, failureReported = false;
  let releaseSignals: (() => void) | void = undefined;
  try {
    logger = options.logger ?? createJsonLogger({ write: (line) => diagnosticSink?.write(line), sinkFailures: () => diagnosticSink?.stats().failures ?? 0 });
    registerDiagnosticCleanup(logger);
    const config = loadMcpConnectorConfigFromProcessEnv();
    if (!config.ok) {
      failureReported = true; report(startupFailureLogRecord(config.error));
      throw new Error("mcp connector config error");
    }
    logger = options.logger ?? createJsonLogger({ write: (line) => diagnosticSink?.write(line), sinkFailures: () => diagnosticSink?.stats().failures ?? 0, deploymentEnvironment: config.config.deploymentEnvironment, diagnostics: config.config.diagnostics, serviceVersion: config.config.serviceVersion });
    registerDiagnosticCleanup(logger);
    phase = "dependency";
    const metrics = new McpConnectorMetricsRegistry();
    const configuredLogger = logger;
    let service: McpConnectorServiceShell;
    const sqlOptions = databasePoolOptions(config.config);
    const sql = options.sql ?? options.sqlFactory?.(sqlOptions) ?? new Bun.SQL(sqlOptions);
    let schemaVerified = false;
    closes.push({ phase: "database", close: () => schemaVerified ? sql.close?.() : sql.close?.({ timeout: 1 }) });
    await (options.schemaVerifier ?? verifyPostgreSQLReadiness)(sql);
    schemaVerified = true;
    const client = options.client ?? new McpSDKClient({
      credentialResolver: new SQLGitHubMcpCredentialResolver(
        sql, config.config.vaultKeyHex, undefined, undefined, undefined, undefined,
        (event) => recordMcpOAuthRefreshCompleted(configuredLogger, (outcome) => metrics.recordRefreshAttempt(outcome), event),
      ),
      logger,
      onToolsListChanged: async (input) => {
        await service.handleToolsListChangedNotification(input);
      },
    });
    if (client instanceof McpSDKClient) closes.push({ phase: "mcp_clients", close: () => client.closeAll() });
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
            allowedDiscoveryCallers: config.config.allowedDiscoveryCallers.map((caller) => ({
              namespace: caller.namespace, name: caller.serviceAccount,
            })),
          }),
      },
    });
    const server = (options.serverFactory ?? createMcpConnectorGrpcServer)(service);
    closes.push({ phase: "grpc", close: () => server.shutdown() });
    let httpServer: ReturnType<typeof createMcpConnectorHttpServer> | undefined;
    await (options.reviewerMaterialValidator ?? validateKubernetesTokenReviewReviewerMaterial)({
      reviewerTokenPath: config.config.tokenReviewReviewerTokenPath,
      apiServerCaCertPath: config.config.kubernetesApiCaCertPath,
    });
    phase = "listener";
    await server.bind(options.bindAddress ?? config.config.grpcBindAddress);
    httpServer = (options.httpServerFactory ?? createMcpConnectorHttpServer)(options.httpBindAddress ?? config.config.httpBindAddress, {
      health: () => ({ ok: true }),
      ready: () => ({ ready }),
      metricsText: () => service.metricsText() + diagnosticMetricsText(configuredLogger),
    });
    closes.push({ phase: "http", close: () => httpServer?.stop() });
    ready = true;
    logWorkloadStarted(logger);
    releaseSignals = options.registerSignalHandlers === undefined ? registerProcessSignalHandlers(shutdown) : options.registerSignalHandlers(shutdown);
    phase = "wait";
    await (options.waitForever ?? waitForever)();
  } catch (error) {
    failed = true;
    if (!failureReported) report(processFailureLogRecord(phase));
    throw error;
  } finally {
    releaseSignals?.();
    try {
      await shutdown();
    } catch (error) {
      if (!failed) throw error;
    }
  }
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
  await runProcessEntry(() => runMcpConnectorCommand());
}
