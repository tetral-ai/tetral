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
import { SchemaVerificationError, verifyPostgreSQLSchema } from "../../schema/src/verify.js";
import type { GatewayTokenReviewClient } from "./auth.js";
import type { ProviderGatewayApp } from "./app.js";
import type { ProviderGatewayConfig } from "./config.js";
import type { GatewayLogger } from "./logger.js";
import type { GatewayCredentialSQL } from "./providers/credentials.js";
import type { SchemaSQL } from "../../schema/src/verify.js";

/** Groups the process-owned application and infrastructure collaborators returned by composition. */
export interface ProviderGatewayCommandDependencies {
  readonly tokenReviewClient: GatewayTokenReviewClient;
  readonly app: ProviderGatewayApp;
  readonly credentialResolver: ProviderCredentialResolver;
  readonly close?: () => Promise<void>;
}

/** Defines process-runner overrides for logging, dependency composition, waiting, and signal registration. */
export interface ProviderGatewayCommandOptions {
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
  const startupLogger = options.logger ?? createJsonLogger({ write: (line) => process.stderr.write(line) });
  const config = loadProviderGatewayConfigFromProcessEnv();
  if (!config.ok) {
    startupLogger.error(startupFailureLogRecord(config.error));
    throw new Error("gateway service config error");
  }
  const logger =
    options.logger ??
    createJsonLogger({
      write: (line) => process.stderr.write(line),
      deploymentEnvironment: config.config.deploymentEnvironment,
      serviceVersion: config.config.serviceVersion,
    });
  let dependencies: ProviderGatewayCommandDependencies;
  try {
    dependencies = await (options.dependencyBuilder ?? buildProviderGatewayCommandDependencies)({
      config: config.config,
      logger,
    });
  } catch (error) {
    logger.error(startupFailureLogRecord({
      kind: "startup_error",
      message: "gateway service startup failed",
      causeCategory: error instanceof SchemaVerificationError ? "schema" : "dependency_readiness",
    }));
    throw new Error("gateway service startup error");
  }
  const shutdown = async (): Promise<void> => {
    await dependencies.app.shutdown();
    await dependencies.close?.();
  };
  (options.registerSignalHandlers ?? registerProcessSignalHandlers)(shutdown);
  await dependencies.app.start();
  await (options.waitForever ?? waitForever)();
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
  const sqlOptions = databasePoolOptions(input.config);
  const sql = input.builderOptions?.sqlFactory?.(sqlOptions) ?? new Bun.SQL(sqlOptions);
  try {
    await (input.builderOptions?.schemaVerifier ?? verifyPostgreSQLSchema)(sql);
  } catch (error) {
    await sql.close?.({ timeout: 1 });
    throw error;
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
    close: async () => {
      await sql.close?.({ timeout: 1 });
    },
  };
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

function registerProcessSignalHandlers(shutdown: () => Promise<void>): void {
  process.once("SIGTERM", () => {
    void shutdown().then(() => process.exit(0));
  });
  process.once("SIGINT", () => {
    void shutdown().then(() => process.exit(0));
  });
}

if (import.meta.main) {
  await runProviderGatewayCommand();
}
