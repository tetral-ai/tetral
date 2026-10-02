import type { ExecutableProcessBoundary } from "@tetral/ts-observability";
import { ServiceLifecycleDefaults } from "@tetral/gateway-protocol/src/service-lifecycle.js";
import { openPostgresSQLOwner } from "@tetral/ts-dbconnect";
import { createDiagnosticStreamSink, processFailureLogRecord, processShutdownFailureLogRecord, registerProcessSignalHandlers, runProcessEntry } from "@tetral/ts-observability";
/**
 * @packageDocumentation
 *
 * Composes and runs the provider-gateway process from validated configuration,
 * Kubernetes workload authentication, schema-checked SQL access, credential
 * resolution, provider clients, attachment resolution, and the application
 * lifecycle. The Bun executable entry point and startup tests call this
 * module; it delegates configuration, authentication, storage, provider, and
 * listener behavior to their owning modules. Startup keeps traffic unready
 * until reviewer material is readable and the platform credential pool is
 * warm, while signal-driven shutdown withdraws readiness before closing
 * listeners and SQL access.
 */

import { KubernetesTokenReviewClient, validateKubernetesTokenReviewReviewerMaterial } from "./auth.js";
import { createProviderGatewayApp } from "./app.js";
import { semanticErrorFields } from "@tetral/ts-observability";
import { loadProviderGatewayConfigFromProcessEnv } from "./config.js";
import { BridgeAPIAttachmentResolver } from "./attachments.js";
import { createJsonLogger, startupFailureLogRecord } from "./logger.js";
import {
  CachedPlatformCredentialPool,
  ProviderCredentialResolver,
  SQLGatewayCredentialStore,
} from "./providers/credentials.js";
import { createProviderClientRegistry } from "./providers/clients.js";
import { SQLOpenAIOAuthCredentialRefreshWriter } from "./providers/openai-oauth-refresh.js";
import { SchemaVerificationError, verifyPostgreSQLReadiness } from "../../schema/src/verify.js";
import type { GatewayTokenReviewClient } from "./auth.js";
import type { ProviderGatewayApp } from "./app.js";
import type { ProviderGatewayConfig } from "./config.js";
import type { ProcessFailurePhase } from "@tetral/ts-observability";
import type { GatewayLogRecord, GatewayLogger } from "./logger.js";
import type { GatewayCredentialSQL } from "./providers/credentials.js";
import type { SchemaSQL } from "../../schema/src/verify.js";

/** Groups the process-owned application and infrastructure collaborators returned by composition. */
export interface ProviderGatewayCommandDependencies {
  readonly tokenReviewClient: GatewayTokenReviewClient;
  readonly app: ProviderGatewayApp;
  readonly credentialResolver: ProviderCredentialResolver;
  readonly close?: (deadline?: Date) => Promise<void>;
}

/** Defines process-runner overrides for logging, dependency composition, waiting, and signal registration. */
export interface ProviderGatewayCommandOptions {
  readonly processBoundary?: ExecutableProcessBoundary;
  readonly logger?: GatewayLogger;
  readonly dependencyBuilder?: (input: {
    readonly config: ProviderGatewayConfig;
    readonly logger: GatewayLogger;
  }) => Promise<ProviderGatewayCommandDependencies>;
  readonly waitForever?: () => Promise<never>;
  readonly registerSignalHandlers?: (shutdown: () => Promise<void>) => void;
}

/** Defines focused dependency-builder substitutions used to verify startup and schema behavior. */
export interface ProviderGatewayDependencyBuilderOptions {
  readonly tokenReviewClientFactory?: (config: ProviderGatewayConfig) => GatewayTokenReviewClient;
  readonly sqlFactory?: (options: Bun.SQL.PostgresOrMySQLOptions) => GatewayCredentialSQL & { readonly close?: (options?: { readonly timeout?: number }) => Promise<void> };
  readonly schemaVerifier?: (sql: SchemaSQL) => Promise<void>;
}

/**
 * Loads process configuration, builds the provider-gateway dependency graph,
 * starts its listeners, and keeps the process alive through the configured
 * wait operation.
 *
 * Configuration and startup failures are logged through bounded startup
 * records and surface only generic process errors.
 */
export async function runProviderGatewayCommand(options: ProviderGatewayCommandOptions = {}): Promise<void> {
  const diagnosticSink = options.logger === undefined ? createDiagnosticStreamSink(process.stderr) : undefined;
  const diagnosticOwners: object[] = [];
  const diagnosticReleases: (() => void)[] = [];
  const registerDiagnosticCleanup = (owner: object): void => {
    if (diagnosticOwners.includes(owner)) return;
    diagnosticOwners.push(owner);
    try { if ("flush" in owner && typeof owner.flush === "function") diagnosticReleases.push(owner.flush.bind(owner)); } catch { /* best effort */ }
  };
  let logger: GatewayLogger | undefined;
  const report = (record: GatewayLogRecord): void => {
    try {
      logger?.error(record);
    } catch {
      /* observability does not own lifecycle */
    }
  };
  let drainTimeoutMs: number = ServiceLifecycleDefaults.drainTimeoutMs,
    cancelJoinTimeoutMs: number = ServiceLifecycleDefaults.cancelJoinTimeoutMs;
  let shutdownDeadline: Date | undefined;
  let drainDeadline: Date | undefined;
  const closes: {
    phase: ProcessFailurePhase;
    close: () => void | Promise<void>;
  }[] = [];
  let stopping: Promise<void> | undefined;
  const shutdown = (): Promise<void> => {
    if (stopping !== undefined) return stopping;
    drainDeadline = new Date(Date.now() + drainTimeoutMs);
    shutdownDeadline = new Date(drainDeadline.getTime() + cancelJoinTimeoutMs);
    const disarmExit = options.processBoundary?.beginShutdown(
      shutdownDeadline.getTime(), () => report(processShutdownFailureLogRecord()),
    );
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
        disarmExit?.();
        for (const release of diagnosticReleases) { try { release(); } catch { /* best effort */ } }
        diagnosticSink?.close();
      }
      if (failed) throw firstFailure;
    })().then(resolveShutdown, rejectShutdown);
    return stopping;
  };
  let phase: ProcessFailurePhase = "configuration", failureReported = false, failed = false;
  let releaseSignals: (() => void) | void = undefined;
  try {
    logger = options.logger ?? createJsonLogger({ write: (line) => diagnosticSink?.write(line), sinkFailures: () => diagnosticSink?.stats().failures ?? 0 });
    registerDiagnosticCleanup(logger);
    const config = loadProviderGatewayConfigFromProcessEnv();
    if (!config.ok) {
      failureReported = true; report(startupFailureLogRecord(config.error));
      throw new Error("gateway service config error");
    }
    logger = options.logger ?? createJsonLogger({
      write: (line) => diagnosticSink?.write(line), sinkFailures: () => diagnosticSink?.stats().failures ?? 0,
      deploymentEnvironment: config.config.deploymentEnvironment, diagnostics: config.config.diagnostics, serviceVersion: config.config.serviceVersion,
    });
    registerDiagnosticCleanup(logger);
    drainTimeoutMs = config.config.drainTimeoutMs;
    cancelJoinTimeoutMs = config.config.cancelJoinTimeoutMs;
    phase = "dependency";
    let dependencies: ProviderGatewayCommandDependencies;
    try { dependencies = await (options.dependencyBuilder ?? buildProviderGatewayCommandDependencies)({ config: config.config, logger }); }
    catch (error) {
      failureReported = true;
      report(startupFailureLogRecord({ kind: "startup_error", message: "gateway service startup failed", causeCategory: error instanceof SchemaVerificationError ? "schema" : "dependency_readiness" }));
      throw new Error("gateway service startup error");
    }
    closes.push({
      phase: "database",
      close: () => dependencies.close?.(shutdownDeadline),
    });
    closes.push({
      phase: "app",
      close: () => dependencies.app.shutdown(shutdownDeadline, drainDeadline),
    });
    releaseSignals =
      options.registerSignalHandlers === undefined
        ? registerProcessSignalHandlers(shutdown)
        : options.registerSignalHandlers(shutdown);
    phase = "listener";
    await dependencies.app.start();
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

/**
 * Builds the concrete authentication, SQL, credential, provider, attachment,
 * and application dependencies for one provider-gateway process.
 *
 * The builder verifies the PostgreSQL schema before exposing SQL-backed
 * collaborators and closes the connection when schema verification fails.
 * Reviewer-material validation and platform-pool warming remain application
 * bootstrap work and therefore run when the returned app starts.
 */
export async function buildProviderGatewayCommandDependencies(input: {
  readonly config: ProviderGatewayConfig;
  readonly logger: GatewayLogger;
  readonly builderOptions?: ProviderGatewayDependencyBuilderOptions;
}): Promise<ProviderGatewayCommandDependencies> {
  const tokenReviewClient =
    input.builderOptions?.tokenReviewClientFactory?.(input.config) ??
    new KubernetesTokenReviewClient({
      apiServerUrl: input.config.kubernetesApiServerUrl,
      reviewerTokenPath: input.config.tokenReviewReviewerTokenPath,
      apiServerCaCertPath: input.config.kubernetesApiCaCertPath,
    });
  let schemaFailure: unknown;
  let sql;
  try {
    sql = await openPostgresSQLOwner({
      url: input.config.databaseUrl,
      pool: input.config.databasePool,
      observe: (event) => {
        if (event.kind !== "reload_failed" && event.kind !== "reload_recovered")
          return;
        const failed = event.kind === "reload_failed";
        try {
          input.logger[failed ? "error" : "info"]({
            event: failed
              ? "transport.credential_reload_failed"
              : "transport.credential_reload_recovered",
            "event.kind": failed
              ? "transport.credential_reload_failed"
              : "transport.credential_reload_recovered",
            component: "database",
            "transport.stage": "credential_reload",
            "transport.outcome": failed ? "invalid_generation" : "recovered",
            "failed.count": event.failedCount ?? 0,
          });
        } catch {
          /* sink failure cannot alter SQL generation custody */
        }
      },
      ...(input.config.databaseTLS !== undefined
        ? { tls: input.config.databaseTLS }
        : {}),
      ...(input.builderOptions?.sqlFactory !== undefined
        ? {
            sqlFactory: input.builderOptions.sqlFactory as unknown as (
              options: Bun.SQL.PostgresOrMySQLOptions,
            ) => Bun.SQL,
          }
        : {}),
      verify: async (actual) => {
        try {
          await (
            input.builderOptions?.schemaVerifier ?? verifyPostgreSQLReadiness
          )(actual);
        } catch (error) {
          schemaFailure = error;
          throw error;
        }
      },
    });
  } catch (error) {
    throw schemaFailure ?? error;
  }
  const credentialStore = new SQLGatewayCredentialStore(sql);
  const platformPool = new CachedPlatformCredentialPool({
    store: credentialStore,
    masterKeyHex: input.config.vaultKeyHex,
    poolOptions: {
      onQuarantine: (event) => {
        input.logger.error({
          event: "platform_provider_key_quarantined",
          "event.kind": "platform_provider_key_quarantined",
          operation: "platform_key_pool",
          component: "gateway",
          "provider.id": event.providerId,
          "credential.origin": "platform",
          ...(event.providerError.statusCode === undefined ? {} : { "provider.status_code": event.providerError.statusCode }),
          ...semanticErrorFields({
            errorClass: "provider_error",
            errorCode: event.providerError.code ?? "provider_error",
            messageSafe: "platform provider key quarantined",
          }),
        });
      },
    },
  });
  const credentialResolver = new ProviderCredentialResolver({
    store: credentialStore,
    platformPool,
    masterKeyHex: input.config.vaultKeyHex,
  });
  const providerStreamer = createProviderClientRegistry({
    openAIOAuthCredentialRefreshWriter: new SQLOpenAIOAuthCredentialRefreshWriter({
      sql,
      masterKeyHex: input.config.vaultKeyHex,
    }),
  });
  const attachmentResolver = new BridgeAPIAttachmentResolver({
    address: input.config.bridgeApiGrpcAddress,
    tokenPath: input.config.bridgeTokenPath,
  });
  const app = createProviderGatewayApp({
    config: input.config,
    logger: input.logger,
    tokenReviewClient,
    credentialResolver,
    attachmentResolver,
    providerStreamer,
    bootstrap: async () => {
      await validateKubernetesTokenReviewReviewerMaterial({
        reviewerTokenPath: input.config.tokenReviewReviewerTokenPath,
        apiServerCaCertPath: input.config.kubernetesApiCaCertPath,
      });
      await platformPool.warm();
    },
  });
  return {
    app,
    tokenReviewClient,
    credentialResolver,
    close: async (deadline) => {
      await attachmentResolver.close();
      await sql.close({ deadline: deadline ?? new Date(Date.now() + 5000) });
    },
  };
}

async function closeSQLAfterStartupFailure(
	sql: { readonly close?: (options?: { readonly timeout?: number }) => Promise<void> },
): Promise<void> {
	try {
		await sql.close?.({ timeout: 1 });
	} catch {
		// The verified public-safe startup error remains authoritative.
	}
}

function databasePoolOptions(config: ProviderGatewayConfig): Bun.SQL.PostgresOrMySQLOptions {
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
  await runProcessEntry((processBoundary) => runProviderGatewayCommand({ processBoundary }));
}
