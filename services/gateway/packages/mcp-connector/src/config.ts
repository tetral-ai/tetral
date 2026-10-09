import { MCP_EXECUTION_TIMEOUT_MS, MCP_FIRST_COMMIT_RESERVE_MS, MCP_CALL_TIMEOUT_MS, MCP_DISCOVERY_TIMEOUT_MS, MCP_SESSION_IDLE_TIMEOUT_MS, MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS, MCP_CONNECT_TIMEOUT_MS, MCP_CLAIM_LEASE_SECONDS } from "./phase-budgets.js";
import {
  ServiceLifecycleDefaults,
  validServiceLifecycle,
} from "@tetral/gateway-protocol/src/service-lifecycle.js";
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

import { parseDatabasePoolConfig } from "@tetral/ts-dbconnect";
import { diagnosticEnvKeys, parseDiagnosticConfig, parseWorkloadResourceConfig, workloadResourceEnvKeys } from "@tetral/ts-observability";
import type { DiagnosticConfig } from "@tetral/ts-observability";

import type { DatabasePoolConfig } from "@tetral/ts-dbconnect";
export type { DatabasePoolConfig } from "@tetral/ts-dbconnect";

interface McpConnectorConfig {
  readonly deploymentEnvironment: string;
  readonly diagnostics: DiagnosticConfig;
  readonly serviceVersion: string;
  readonly grpcBindAddress: string;
  readonly httpBindAddress: string;
  readonly allowedRuntimePod: {
    readonly namespace: string;
    readonly serviceAccount: string;
  };
  readonly allowedDiscoveryCallers: readonly {
    readonly namespace: string;
    readonly serviceAccount: string;
  }[];
  readonly bridgeApiGrpcAddress: string;
  readonly bridgeTokenPath: string;
  readonly runtimeBindingTokenHMACKey: string;
  readonly databaseUrl: string;
  readonly databasePool: DatabasePoolConfig;
  readonly databaseTLS?: {
    readonly caPath: string;
    readonly serverName: string;
  };
  readonly drainTimeoutMs: number;
  readonly cancelJoinTimeoutMs: number;
  readonly bridgePolicies: McpBridgePolicies;
  readonly clientPolicies: McpClientPolicies;
  readonly vaultKeyHex: string;
  readonly kubernetesApiServerUrl: string;
  readonly kubernetesApiCaCertPath: string;
  readonly tokenReviewReviewerTokenPath: string;
}

/** Checked against the Runtime-owned descriptor projection by boundary tests. */
export const McpBridgePolicyDefaults = Object.freeze({
  mcpManifestChanged: 5000,
  claimMcpToolResult: 10000,
  commitMcpToolResult: 10000,
  relinquishMcpToolResult: 10000,
});
export const McpClientPolicyDefaults = Object.freeze({
  executionTimeoutMs: MCP_EXECUTION_TIMEOUT_MS,
  callTimeoutMs: MCP_CALL_TIMEOUT_MS,
  credentialTimeoutMs: MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS,
  connectTimeoutMs: MCP_CONNECT_TIMEOUT_MS,
  idleTimeoutMs: MCP_SESSION_IDLE_TIMEOUT_MS,
  discoveryTimeoutMs: MCP_DISCOVERY_TIMEOUT_MS,
});
export type McpBridgePolicies = Readonly<
  Record<keyof typeof McpBridgePolicyDefaults, number>
>;
export type McpClientPolicies = Readonly<
  Record<keyof typeof McpClientPolicyDefaults, number>
>;
const lifecyclePolicyKeys = {
  mcpManifestChanged: "TETRAL_BRIDGE_MCP_MANIFEST_CHANGED_TIMEOUT_MS",
  claimMcpToolResult: "TETRAL_BRIDGE_CLAIM_MCP_TOOL_RESULT_TIMEOUT_MS",
  commitMcpToolResult: "TETRAL_BRIDGE_COMMIT_MCP_TOOL_RESULT_TIMEOUT_MS",
  relinquishMcpToolResult:
    "TETRAL_BRIDGE_RELINQUISH_MCP_TOOL_RESULT_TIMEOUT_MS",
  executionTimeoutMs: "TETRAL_MCP_EXECUTION_TIMEOUT_MS",
  callTimeoutMs: "TETRAL_MCP_CALL_TIMEOUT_MS",
  credentialTimeoutMs: "TETRAL_MCP_CREDENTIAL_TIMEOUT_MS",
  connectTimeoutMs: "TETRAL_MCP_CONNECT_TIMEOUT_MS",
  idleTimeoutMs: "TETRAL_MCP_SESSION_IDLE_TIMEOUT_MS",
  discoveryTimeoutMs: "TETRAL_MCP_DISCOVERY_TIMEOUT_MS",
} as const;
function parseLifecyclePolicies(
  env: Readonly<Record<string, string | undefined>>,
):
  | {
      readonly bridgePolicies: McpBridgePolicies;
      readonly clientPolicies: McpClientPolicies;
    }
  | undefined {
  const parsed: Record<string, number> = {
    ...McpBridgePolicyDefaults,
    ...McpClientPolicyDefaults,
  };
  for (const [method, key] of Object.entries(lifecyclePolicyKeys)) {
    const raw = env[key];
    if (raw === undefined) continue;
    if (
      !/^[1-9][0-9]*$/.test(raw) ||
      !Number.isSafeInteger(Number(raw)) ||
      Number(raw) > 2147483647
    )
      return undefined;
    parsed[method] = Number(raw);
  }
  // Phase ceilings share one execution deadline; the first commit retains its lease reserve.
  if (parsed.executionTimeoutMs! > MCP_EXECUTION_TIMEOUT_MS || parsed.commitMcpToolResult! > MCP_FIRST_COMMIT_RESERVE_MS ||
      parsed.executionTimeoutMs! + parsed.commitMcpToolResult! > MCP_CLAIM_LEASE_SECONDS * 1000) return undefined;
  return {
    bridgePolicies: {
      mcpManifestChanged: parsed.mcpManifestChanged!,
      claimMcpToolResult: parsed.claimMcpToolResult!,
      commitMcpToolResult: parsed.commitMcpToolResult!,
      relinquishMcpToolResult: parsed.relinquishMcpToolResult!,
    },
    clientPolicies: {
      executionTimeoutMs: parsed.executionTimeoutMs!,
      callTimeoutMs: parsed.callTimeoutMs!,
      credentialTimeoutMs: parsed.credentialTimeoutMs!,
      connectTimeoutMs: parsed.connectTimeoutMs!,
      idleTimeoutMs: parsed.idleTimeoutMs!,
      discoveryTimeoutMs: parsed.discoveryTimeoutMs!,
    },
  };
}

/** Describes either a complete validated startup configuration or a safe failure. */
export type McpConnectorConfigResult =
  | { readonly ok: true; readonly config: McpConnectorConfig }
  | { readonly ok: false; readonly error: { readonly kind: "config_error"; readonly message: string } };

const ConfigKeys = [
  ...diagnosticEnvKeys,
  ...Object.values(lifecyclePolicyKeys),
  "TETRAL_MCP_CONNECTOR_GRPC_ADDR",
  "TETRAL_MCP_CONNECTOR_HTTP_ADDR",
  ...workloadResourceEnvKeys,
  "TETRAL_INTERNAL_GRPC_AUDIENCE",
  "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS",
  "TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS",
  "TETRAL_BRIDGE_API_GRPC_ADDR",
  "TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH",
  "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY",
  "TETRAL_DATABASE_URL",
  "TETRAL_DATABASE_TLS_CA_PATH",
  "TETRAL_DATABASE_TLS_SERVER_NAME",
  "TETRAL_DRAIN_TIMEOUT_MS",
  "TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS",
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
  const allowedDiscoveryCallers = parseDiscoveryServiceAccounts(env.TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS ?? "");
  const diagnostics = parseDiagnosticConfig(env);
  const resource = parseWorkloadResourceConfig(env, 4096);
  const caPath = env.TETRAL_DATABASE_TLS_CA_PATH,
    serverName = env.TETRAL_DATABASE_TLS_SERVER_NAME;
  const drainValue =
    env.TETRAL_DRAIN_TIMEOUT_MS ??
    String(ServiceLifecycleDefaults.drainTimeoutMs);
  const drainTimeoutMs = /^[1-9][0-9]*$/.test(drainValue)
    ? Number(drainValue)
    : undefined;
  const joinValue =
    env.TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS ??
    String(ServiceLifecycleDefaults.cancelJoinTimeoutMs);
  const cancelJoinTimeoutMs = /^[1-9][0-9]*$/.test(joinValue)
    ? Number(joinValue)
    : undefined;
  if (
    cancelJoinTimeoutMs === undefined ||
    drainTimeoutMs === undefined ||
    !validServiceLifecycle(drainTimeoutMs, cancelJoinTimeoutMs) ||
    (caPath === undefined) !== (serverName === undefined) ||
    (caPath !== undefined && (!nonEmpty(caPath) || !nonEmpty(serverName))) ||
    !Number.isSafeInteger(drainTimeoutMs)
  )
    return {
      ok: false,
      error: { kind: "config_error", message: "invalid mcp connector config" },
    };
  const policies = parseLifecyclePolicies(env);
  const databasePool = parseDatabasePoolConfig(env, { empty: "reject" });
  if (
    !nonEmpty(env.TETRAL_MCP_CONNECTOR_GRPC_ADDR) ||
    !nonEmpty(env.TETRAL_MCP_CONNECTOR_HTTP_ADDR) ||
    !nonEmpty(env.TETRAL_DEPLOYMENT_ENVIRONMENT) ||
    !nonEmpty(env.TETRAL_SERVICE_VERSION) ||
    env.TETRAL_INTERNAL_GRPC_AUDIENCE !== "tetral-internal-grpc" ||
    allowedRuntimePod === undefined ||
    allowedDiscoveryCallers === undefined ||
    databasePool === undefined ||
    policies === undefined ||
    diagnostics === undefined ||
    resource === undefined ||
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
      diagnostics,
      deploymentEnvironment: resource.deploymentEnvironment,
      serviceVersion: resource.serviceVersion,
      grpcBindAddress: env.TETRAL_MCP_CONNECTOR_GRPC_ADDR,
      httpBindAddress: env.TETRAL_MCP_CONNECTOR_HTTP_ADDR,
      allowedRuntimePod,
      allowedDiscoveryCallers,
      bridgeApiGrpcAddress: env.TETRAL_BRIDGE_API_GRPC_ADDR,
      bridgeTokenPath: env.TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH,
      runtimeBindingTokenHMACKey: env.TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY,
      databaseUrl: env.TETRAL_DATABASE_URL,
      databasePool,
      drainTimeoutMs,
      cancelJoinTimeoutMs,
      ...policies,
      ...(caPath !== undefined && serverName !== undefined
        ? { databaseTLS: { caPath, serverName } }
        : {}),
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
    !validNamespace(namespace) ||
    !validAccount(serviceAccount)
  ) {
    return undefined;
  }
  return { namespace, serviceAccount };
}

// The compatibility environment key names Bridge, but discovery also belongs to Job Runner.
// No discovery identity is admitted to Runtime-only tool execution.
function parseDiscoveryServiceAccounts(value: string): readonly { readonly namespace: string; readonly serviceAccount: string }[] | undefined {
  if (value.length > 4096) return undefined;
  const values = value.split(",");
  if (values.length > 16) return undefined;
  if (new Set(values).size !== values.length) return undefined;
  const accounts = values.map(parseSingleServiceAccount);
  if (accounts.some((account) => account === undefined)) return undefined;
  return accounts as readonly { readonly namespace: string; readonly serviceAccount: string }[];
}

function validNamespace(value: string): boolean { return value.length <= 63 && /^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/.test(value); }
function validAccount(value: string): boolean { return value.length <= 253 && value.split(".").every(validNamespace); }
