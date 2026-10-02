import {
  ServiceLifecycleDefaults,
  validServiceLifecycle,
} from "@tetral/gateway-protocol/src/service-lifecycle.js";
/**
 * @packageDocumentation
 *
 * Defines and validates the provider-gateway process configuration. The
 * process command reads the ambient environment through this module, while
 * tests and other package consumers use explicit environment maps. Validation
 * admits only the recognized keys, one exact Runtime service-account pair, the
 * fixed internal audience, bounded listener and identity fields, required SQL,
 * vault, Kubernetes, and Bridge settings, and a positive concurrent-turn cap.
 * Every rejected input becomes the same bounded configuration error and never
 * exposes an environment value.
 */

import { z } from "zod/v4";
import { diagnosticEnvKeys, parseDiagnosticConfig, parseWorkloadResourceConfig, workloadResourceEnvKeys } from "@tetral/ts-observability";
import type { DiagnosticConfig } from "@tetral/ts-observability";
import { parseDatabasePoolConfig } from "@tetral/ts-dbconnect";
import type { DatabasePoolConfig } from "@tetral/ts-dbconnect";
export type { DatabasePoolConfig } from "@tetral/ts-dbconnect";

/** Contains the complete validated configuration needed to compose one provider-gateway process. */
export interface ProviderGatewayConfig {
  readonly deploymentEnvironment: string;
  readonly diagnostics: DiagnosticConfig;
  readonly serviceVersion: string;
  readonly grpcBindAddress: string;
  readonly httpBindAddress: string;
  readonly allowedRuntimePod: {
    readonly namespace: string;
    readonly serviceAccount: string;
  };
  readonly runtimeBindingTokenHMACKey: string;
  readonly databaseUrl: string;
  readonly databasePool: DatabasePoolConfig;
  readonly databaseTLS?: {
    readonly caPath: string;
    readonly serverName: string;
  };
  readonly drainTimeoutMs: number;
  readonly cancelJoinTimeoutMs: number;
  readonly vaultKeyHex: string;
  readonly kubernetesApiServerUrl: string;
  readonly kubernetesApiCaCertPath: string;
  readonly tokenReviewReviewerTokenPath: string;
  readonly bridgeApiGrpcAddress: string;
  readonly bridgeTokenPath: string;
  readonly maxConcurrentTurns: number;
}

/** Describes a bounded configuration or startup failure suitable for structured startup logging. */
export interface ProviderGatewayStartupFailure {
  readonly kind: "config_error" | "startup_error";
  readonly message: string;
}

/** Reports either a complete validated configuration or a safe startup failure. */
export type ProviderGatewayConfigResult =
  | { readonly ok: true; readonly config: ProviderGatewayConfig }
  | { readonly ok: false; readonly error: ProviderGatewayStartupFailure };

const AddressSchema = z.string().min(1).max(512);
const IdentityFieldSchema = z.string().min(1).max(253);
const ServiceAccountSchema = z
  .string()
  .min(3)
  .max(511)
  .refine((value) => parseSingleServiceAccount(value) !== undefined);
const ConfigSchema = z.strictObject({
  TETRAL_LOG_LEVEL: z.string().optional(),
  TETRAL_LOG_MAX_RECORD_BYTES: z.string().optional(),
  TETRAL_LOG_SUMMARY_INTERVAL_MS: z.string().optional(),
  TETRAL_LOG_BURST: z.string().optional(),
  TETRAL_PROVIDER_GATEWAY_GRPC_ADDR: AddressSchema,
  TETRAL_PROVIDER_GATEWAY_HTTP_ADDR: AddressSchema,
  TETRAL_DEPLOYMENT_ENVIRONMENT: IdentityFieldSchema,
  TETRAL_SERVICE_VERSION: IdentityFieldSchema,
  TETRAL_INTERNAL_GRPC_AUDIENCE: z.literal("tetral-internal-grpc"),
  TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: ServiceAccountSchema,
  TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY: z.string().min(32).max(4096),
  TETRAL_DATABASE_TLS_CA_PATH: AddressSchema.optional(),
  TETRAL_DATABASE_TLS_SERVER_NAME: AddressSchema.optional(),
  TETRAL_SERVICE_DRAIN_TIMEOUT_MS: z.string().optional(),
  TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS: z.string().optional(),
  TETRAL_DATABASE_URL: z.string().min(1).max(4096),
  TETRAL_DATABASE_POOL_MAX: z.string().optional(),
  TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS: z.string().optional(),
  TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS: z.string().optional(),
  TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS: z.string().optional(),
  TETRAL_DATABASE_STATEMENT_TIMEOUT_MS: z.string().optional(),
  ENGINE_VAULT_KEY: z.string().regex(/^[0-9a-fA-F]{64}$/),
  KUBERNETES_API_SERVER_URL: AddressSchema,
  KUBERNETES_API_CA_CERT_PATH: AddressSchema,
  KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: AddressSchema,
  TETRAL_BRIDGE_API_GRPC_ADDR: AddressSchema,
  TETRAL_PROVIDER_GATEWAY_BRIDGE_TOKEN_PATH: AddressSchema,
  TETRAL_GATEWAY_MAX_CONCURRENT_TURNS: z.string().optional(),
});
const ProviderGatewayEnvKeys = [
  ...diagnosticEnvKeys,
  "TETRAL_PROVIDER_GATEWAY_GRPC_ADDR",
  "TETRAL_PROVIDER_GATEWAY_HTTP_ADDR",
  ...workloadResourceEnvKeys,
  "TETRAL_INTERNAL_GRPC_AUDIENCE",
  "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS",
  "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY",
  "TETRAL_DATABASE_URL",
  "TETRAL_DATABASE_TLS_CA_PATH",
  "TETRAL_DATABASE_TLS_SERVER_NAME",
  "TETRAL_SERVICE_DRAIN_TIMEOUT_MS",
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
  "TETRAL_BRIDGE_API_GRPC_ADDR",
  "TETRAL_PROVIDER_GATEWAY_BRIDGE_TOKEN_PATH",
  "TETRAL_GATEWAY_MAX_CONCURRENT_TURNS",
] as const;

/**
 * Validates an already projected provider-gateway environment map.
 *
 * The map must contain every required recognized key and no additional
 * properties. An omitted concurrent-turn value uses the process default of
 * eight; a supplied value must be a positive safe integer.
 */
export function loadProviderGatewayConfig(env: Record<string, string | undefined>): ProviderGatewayConfigResult {
  const resource = parseWorkloadResourceConfig(env, 253);
  const diagnostics = parseDiagnosticConfig(env);
  const parsed = ConfigSchema.safeParse(env);
  if (!parsed.success || diagnostics === undefined || resource === undefined) {
    return { ok: false, error: { kind: "config_error", message: "invalid gateway config" } };
  }
  const allowedRuntimePod = parseSingleServiceAccount(parsed.data.TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS);
  if (allowedRuntimePod === undefined) {
    return { ok: false, error: { kind: "config_error", message: "invalid gateway config" } };
  }
  const maxConcurrentTurns = parsePositiveInteger(
    parsed.data.TETRAL_GATEWAY_MAX_CONCURRENT_TURNS,
    8,
  );
  const caPath = parsed.data.TETRAL_DATABASE_TLS_CA_PATH,
    serverName = parsed.data.TETRAL_DATABASE_TLS_SERVER_NAME;
  const drainTimeoutMs = parsePositiveInteger(
    parsed.data.TETRAL_SERVICE_DRAIN_TIMEOUT_MS,
    ServiceLifecycleDefaults.drainTimeoutMs,
  );
  const cancelJoinTimeoutMs = parsePositiveInteger(
    parsed.data.TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS,
    ServiceLifecycleDefaults.cancelJoinTimeoutMs,
  );
  if (
    cancelJoinTimeoutMs === undefined ||
    drainTimeoutMs === undefined ||
    !validServiceLifecycle(drainTimeoutMs, cancelJoinTimeoutMs) ||
    (caPath === undefined) !== (serverName === undefined) ||
    drainTimeoutMs === undefined
  )
    return {
      ok: false,
      error: { kind: "config_error", message: "invalid gateway config" },
    };
  const databasePool = parseDatabasePoolConfig(parsed.data, {
    empty: "default",
  });
  if (maxConcurrentTurns === undefined || databasePool === undefined) {
    return { ok: false, error: { kind: "config_error", message: "invalid gateway config" } };
  }
  return {
    ok: true,
    config: {
      diagnostics,
      deploymentEnvironment: resource.deploymentEnvironment,
      serviceVersion: resource.serviceVersion,
      grpcBindAddress: parsed.data.TETRAL_PROVIDER_GATEWAY_GRPC_ADDR,
      httpBindAddress: parsed.data.TETRAL_PROVIDER_GATEWAY_HTTP_ADDR,
      allowedRuntimePod: {
        namespace: allowedRuntimePod.namespace,
        serviceAccount: allowedRuntimePod.serviceAccount,
      },
      runtimeBindingTokenHMACKey: parsed.data.TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY,
      databaseUrl: parsed.data.TETRAL_DATABASE_URL,
      databasePool,
      drainTimeoutMs,
      cancelJoinTimeoutMs,
      ...(caPath !== undefined && serverName !== undefined
        ? { databaseTLS: { caPath, serverName } }
        : {}),
      vaultKeyHex: parsed.data.ENGINE_VAULT_KEY,
      kubernetesApiServerUrl: parsed.data.KUBERNETES_API_SERVER_URL,
      kubernetesApiCaCertPath: parsed.data.KUBERNETES_API_CA_CERT_PATH,
      tokenReviewReviewerTokenPath: parsed.data.KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH,
      bridgeApiGrpcAddress: parsed.data.TETRAL_BRIDGE_API_GRPC_ADDR,
      bridgeTokenPath: parsed.data.TETRAL_PROVIDER_GATEWAY_BRIDGE_TOKEN_PATH,
      maxConcurrentTurns,
    },
  };
}

/** Loads provider-gateway startup configuration from the current process environment. */
export function loadProviderGatewayConfigFromProcessEnv(): ProviderGatewayConfigResult {
  return loadProviderGatewayConfigFromEnv(process.env);
}

/**
 * Projects the recognized keys from an explicit environment map and validates
 * them with the same rules used by process startup.
 */
export function loadProviderGatewayConfigFromEnv(env: Record<string, string | undefined>): ProviderGatewayConfigResult {
  const projected: Record<string, string | undefined> = {};
  for (const key of ProviderGatewayEnvKeys) {
    projected[key] = env[key];
  }
  return loadProviderGatewayConfig(projected);
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

function parsePositiveInteger(value: string | undefined, fallback: number): number | undefined {
  if (value === undefined || value === "") {
    return fallback;
  }
  if (!/^[1-9][0-9]*$/.test(value)) {
    return undefined;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}
