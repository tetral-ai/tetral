/**
 * @packageDocumentation
 *
 * Defines and loads the MCP connector's startup configuration from a closed
 * set of environment variables. It guards fail-closed startup by requiring
 * both listener addresses, workload identities, Bridge and binding material,
 * database and vault settings, and Kubernetes TokenReview inputs while
 * returning only a bounded, safe configuration error. The process command
 * reads configuration through this module, and tests call the explicit-map
 * loader to exercise the same validation without ambient environment access.
 */

import { z } from "zod/v4";

interface McpConnectorConfig {
  readonly deploymentEnvironment: string;
  readonly serviceVersion: string;
  readonly grpcBindAddress: string;
  readonly httpBindAddress: string;
  readonly allowedRuntimePod: {
    readonly namespace: string;
    readonly serviceAccount: string;
  };
  readonly allowedBridge: {
    readonly namespace: string;
    readonly serviceAccount: string;
  };
  readonly bridgeApiGrpcAddress: string;
  readonly bridgeTokenPath: string;
  readonly runtimeBindingTokenHMACKey: string;
  readonly databaseUrl: string;
  readonly databasePool: DatabasePoolConfig;
  readonly vaultKeyHex: string;
  readonly kubernetesApiServerUrl: string;
  readonly kubernetesApiCaCertPath: string;
  readonly tokenReviewReviewerTokenPath: string;
}

/** Contains the validated Bun PostgreSQL pool and statement lifetime bounds. */
export interface DatabasePoolConfig {
  readonly max: number;
  readonly idleTimeout: number;
  readonly maxLifetime: number;
  readonly connectionTimeout: number;
  readonly statementTimeoutMs: number;
}

const positiveIntegerString = (fallback: number) => z.string()
  .regex(/^[1-9][0-9]*$/)
  .transform((value) => Number.parseInt(value, 10))
  .refine(Number.isSafeInteger)
  .default(fallback);
const DatabasePoolEnvSchema = z.strictObject({
  TETRAL_DATABASE_POOL_MAX: positiveIntegerString(10),
  TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS: positiveIntegerString(30),
  TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS: positiveIntegerString(1_800),
  TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS: positiveIntegerString(30),
  TETRAL_DATABASE_STATEMENT_TIMEOUT_MS: positiveIntegerString(30_000),
});

/** Describes either a complete validated startup configuration or a safe failure. */
export type McpConnectorConfigResult =
  | { readonly ok: true; readonly config: McpConnectorConfig }
  | { readonly ok: false; readonly error: { readonly kind: "config_error"; readonly message: string } };

const ConfigKeys = [
  "TETRAL_MCP_CONNECTOR_GRPC_ADDR",
  "TETRAL_MCP_CONNECTOR_HTTP_ADDR",
  "TETRAL_DEPLOYMENT_ENVIRONMENT",
  "TETRAL_SERVICE_VERSION",
  "TETRAL_INTERNAL_GRPC_AUDIENCE",
  "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS",
  "TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS",
  "TETRAL_BRIDGE_API_GRPC_ADDR",
  "TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH",
  "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY",
  "TETRAL_DATABASE_URL",
  "TETRAL_DATABASE_POOL_MAX",
  "TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS",
  "TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS",
  "TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS",
  "TETRAL_DATABASE_STATEMENT_TIMEOUT_MS",
  "ENGINE_VAULT_KEY",
  "KUBERNETES_API_SERVER_URL",
  "KUBERNETES_API_CA_CERT_PATH",
  "KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH",
] as const;

/** Loads MCP connector startup configuration from the current process environment. */
export function loadMcpConnectorConfigFromProcessEnv(): McpConnectorConfigResult {
  return loadMcpConnectorConfigFromEnv(process.env);
}

/**
 * Projects the recognized keys from an explicit environment map and validates
 * them with the same rules used by process startup.
 */
export function loadMcpConnectorConfigFromEnv(env: Record<string, string | undefined>): McpConnectorConfigResult {
  const projected: Record<string, string | undefined> = {};
  for (const key of ConfigKeys) {
    projected[key] = env[key];
  }
  return loadMcpConnectorConfig(projected);
}

function loadMcpConnectorConfig(env: Record<string, string | undefined>): McpConnectorConfigResult {
  const allowedRuntimePod = parseSingleServiceAccount(env.TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS ?? "");
  const allowedBridge = parseSingleServiceAccount(env.TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS ?? "");
  const databasePool = DatabasePoolEnvSchema.safeParse({
    TETRAL_DATABASE_POOL_MAX: env.TETRAL_DATABASE_POOL_MAX,
    TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS: env.TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS,
    TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS: env.TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS,
    TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS: env.TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS,
    TETRAL_DATABASE_STATEMENT_TIMEOUT_MS: env.TETRAL_DATABASE_STATEMENT_TIMEOUT_MS,
  });
  if (
    !nonEmpty(env.TETRAL_MCP_CONNECTOR_GRPC_ADDR) ||
    !nonEmpty(env.TETRAL_MCP_CONNECTOR_HTTP_ADDR) ||
    !nonEmpty(env.TETRAL_DEPLOYMENT_ENVIRONMENT) ||
    !nonEmpty(env.TETRAL_SERVICE_VERSION) ||
    env.TETRAL_INTERNAL_GRPC_AUDIENCE !== "tetral-internal-grpc" ||
    allowedRuntimePod === undefined ||
    allowedBridge === undefined ||
    !databasePool.success ||
    !nonEmpty(env.TETRAL_BRIDGE_API_GRPC_ADDR) ||
    !nonEmpty(env.TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH) ||
    !nonEmpty(env.TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY) ||
    !nonEmpty(env.TETRAL_DATABASE_URL) ||
    !isVaultKey(env.ENGINE_VAULT_KEY) ||
    !nonEmpty(env.KUBERNETES_API_SERVER_URL) ||
    !nonEmpty(env.KUBERNETES_API_CA_CERT_PATH) ||
    !nonEmpty(env.KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH)
  ) {
    return { ok: false, error: { kind: "config_error", message: "invalid mcp connector config" } };
  }
  return {
    ok: true,
    config: {
      deploymentEnvironment: env.TETRAL_DEPLOYMENT_ENVIRONMENT,
      serviceVersion: env.TETRAL_SERVICE_VERSION,
      grpcBindAddress: env.TETRAL_MCP_CONNECTOR_GRPC_ADDR,
      httpBindAddress: env.TETRAL_MCP_CONNECTOR_HTTP_ADDR,
      allowedRuntimePod,
      allowedBridge,
      bridgeApiGrpcAddress: env.TETRAL_BRIDGE_API_GRPC_ADDR,
      bridgeTokenPath: env.TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH,
      runtimeBindingTokenHMACKey: env.TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY,
      databaseUrl: env.TETRAL_DATABASE_URL,
      databasePool: {
        max: databasePool.data.TETRAL_DATABASE_POOL_MAX,
        idleTimeout: databasePool.data.TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS,
        maxLifetime: databasePool.data.TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS,
        connectionTimeout: databasePool.data.TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS,
        statementTimeoutMs: databasePool.data.TETRAL_DATABASE_STATEMENT_TIMEOUT_MS,
      },
      vaultKeyHex: env.ENGINE_VAULT_KEY,
      kubernetesApiServerUrl: env.KUBERNETES_API_SERVER_URL,
      kubernetesApiCaCertPath: env.KUBERNETES_API_CA_CERT_PATH,
      tokenReviewReviewerTokenPath: env.KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH,
    },
  };
}

function nonEmpty(value: string | undefined): value is string {
  return value !== undefined && value.length > 0 && value.length <= 4096;
}

function isVaultKey(value: string | undefined): value is string {
  return value !== undefined && /^[0-9a-fA-F]{64}$/.test(value);
}

function parseSingleServiceAccount(value: string): { readonly namespace: string; readonly serviceAccount: string } | undefined {
  if (value.includes(",") || value.includes("*")) {
    return undefined;
  }
  const [namespace, serviceAccount, extra] = value.split("/");
  if (
    namespace === undefined ||
    serviceAccount === undefined ||
    extra !== undefined ||
    namespace.length === 0 ||
    serviceAccount.length === 0
  ) {
    return undefined;
  }
  return { namespace, serviceAccount };
}
