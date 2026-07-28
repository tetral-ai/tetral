/**
 * Parses Runtime Pod boot settings into one immutable configuration object. The process command
 * calls this module once, then passes the result into dependency construction and lifecycle startup.
 * Zod and Node IP validation guard pod identity, one exact Bridge service account, fixed internal
 * audience, bounded address strings, a valid Runtime gRPC port, positive bounded numeric settings, and
 * provider/model syntax. It centralizes this process's environment read and returns validated
 * configuration to the composition root.
 */
import { isIP } from "node:net";
import { z } from "zod/v4";

/** Identifies the local pod used to reject commands addressed to another Runtime Pod instance. */
export interface RuntimePodIdentity {
  readonly namespace: string;
  readonly name: string;
  readonly uid: string;
  readonly ip: string;
}

/** Selects a provider and model without carrying credentials or provider-specific payloads. */
export interface RuntimePodModelRef {
  readonly providerId: string;
  readonly modelId: string;
}

/**
 * Contains the complete validated boot configuration consumed by Runtime Pod composition.
 * All values are readonly and secret-bearing entries remain paths to mounted token material.
 */
export interface RuntimePodConfig {
  readonly ownPod: RuntimePodIdentity;
  readonly deploymentEnvironment: string;
  readonly serviceVersion: string;
  readonly bridge: {
    readonly namespace: string;
    readonly serviceAccount: string;
  };
  readonly grpcBindAddress: string;
  readonly httpBindAddress: string;
  readonly kubernetesApiServerUrl: string;
  readonly kubernetesApiCaCertPath: string;
  readonly tokenReviewReviewerTokenPath: string;
  readonly outboundInternalGrpcTokenPath: string;
  readonly bridgeApiGrpcAddress: string;
  readonly gatewayGrpcAddress: string;
  readonly mcpConnectorGrpcAddress: string;
  readonly webConnectorGrpcAddress: string;
  readonly providerStreamTimeoutMs: number;
  readonly platformModels: {
    readonly approvalReviewer: RuntimePodModelRef;
  };
  readonly skillGuidance: {
    readonly descriptionBudgetBytes: number;
  };
}

/** Describes a bounded startup failure suitable for structured startup logging. */
export interface RuntimePodStartupFailure {
  readonly kind: "config_error" | "startup_error";
  readonly message: string;
}

/** Carries either validated Runtime Pod configuration or a generic boot failure. */
export type RuntimePodConfigResult =
  | { readonly ok: true; readonly config: RuntimePodConfig }
  | { readonly ok: false; readonly error: RuntimePodStartupFailure };

const IdentityFieldSchema = z.string().min(1).max(253);
const AddressSchema = z.string().min(1).max(512);
const ModelRefSchema = z.string().min(3).max(256).refine((value) => parseModelRef(value) !== undefined);
const PositiveIntegerStringSchema = z
  .string()
  .min(1)
  .max(12)
  .refine((value) => positiveIntegerString(value));
const ProviderStreamTimeoutSchema = PositiveIntegerStringSchema.refine(
  (value) => Number(value) <= 2_147_483_647,
);
const SkillGuidanceDescriptionBudgetSchema = PositiveIntegerStringSchema.refine(
  (value) => Number(value) < 64 * 1_024,
);
const PortSchema = z
  .string()
  .min(1)
  .max(5)
  .refine((value) => {
    const port = Number(value);
    return Number.isInteger(port) && port > 0 && port <= 65535;
  });
const ServiceAccountSchema = z
  .string()
  .min(3)
  .max(511)
  .refine((value) => parseSingleServiceAccount(value) !== undefined);
const ConfigSchema = z.strictObject({
  TETRAL_RUNTIME_POD_NAMESPACE: IdentityFieldSchema,
  TETRAL_RUNTIME_POD_NAME: IdentityFieldSchema,
  TETRAL_RUNTIME_POD_UID: IdentityFieldSchema,
  TETRAL_RUNTIME_POD_IP: IdentityFieldSchema.refine((value) => isIP(value) !== 0),
  TETRAL_RUNTIME_POD_GRPC_PORT: PortSchema,
  TETRAL_RUNTIME_POD_HTTP_ADDR: AddressSchema,
  TETRAL_DEPLOYMENT_ENVIRONMENT: IdentityFieldSchema,
  TETRAL_SERVICE_VERSION: IdentityFieldSchema,
  TETRAL_RUNTIME_POD_GRPC_AUDIENCE: z.literal("tetral-internal-grpc"),
  TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: ServiceAccountSchema,
  KUBERNETES_API_SERVER_URL: AddressSchema,
  KUBERNETES_API_CA_CERT_PATH: AddressSchema,
  KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: AddressSchema,
  TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH: AddressSchema,
  TETRAL_BRIDGE_API_GRPC_ADDR: AddressSchema,
  TETRAL_GATEWAY_GRPC_ADDR: AddressSchema,
  TETRAL_MCP_CONNECTOR_GRPC_ADDR: AddressSchema,
  TETRAL_WEB_CONNECTOR_GRPC_ADDR: AddressSchema,
  TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS: ProviderStreamTimeoutSchema.default("1800000"),
  TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL: ModelRefSchema,
  TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES: SkillGuidanceDescriptionBudgetSchema,
});
const RuntimePodEnvKeys = [
  "TETRAL_RUNTIME_POD_NAMESPACE",
  "TETRAL_RUNTIME_POD_NAME",
  "TETRAL_RUNTIME_POD_UID",
  "TETRAL_RUNTIME_POD_IP",
  "TETRAL_RUNTIME_POD_GRPC_PORT",
  "TETRAL_RUNTIME_POD_HTTP_ADDR",
  "TETRAL_DEPLOYMENT_ENVIRONMENT",
  "TETRAL_SERVICE_VERSION",
  "TETRAL_RUNTIME_POD_GRPC_AUDIENCE",
  "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS",
  "KUBERNETES_API_SERVER_URL",
  "KUBERNETES_API_CA_CERT_PATH",
  "KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH",
  "TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH",
  "TETRAL_BRIDGE_API_GRPC_ADDR",
  "TETRAL_GATEWAY_GRPC_ADDR",
  "TETRAL_MCP_CONNECTOR_GRPC_ADDR",
  "TETRAL_WEB_CONNECTOR_GRPC_ADDR",
  "TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS",
  "TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL",
  "TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES",
] as const;

/**
 * Validates an already projected Runtime Pod environment object and normalizes its values.
 * Invalid fields return the same bounded configuration error rather than schema diagnostics.
 */
export function loadRuntimePodConfig(env: Record<string, string | undefined>): RuntimePodConfigResult {
  const parsed = ConfigSchema.safeParse(env);
  if (!parsed.success) {
    return {
      ok: false,
      error: {
        kind: "config_error",
        message: "invalid runtime pod identity",
      },
    };
  }
  const bridge = parseSingleServiceAccount(parsed.data.TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS);
  if (bridge === undefined) {
    return {
      ok: false,
      error: {
        kind: "config_error",
        message: "invalid runtime pod identity",
      },
    };
  }
  const approvalReviewerModel = parseModelRef(parsed.data.TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL);
  if (approvalReviewerModel === undefined) {
    return {
      ok: false,
      error: {
        kind: "config_error",
        message: "invalid runtime pod identity",
      },
    };
  }
  return {
    ok: true,
    config: {
      ownPod: {
        namespace: parsed.data.TETRAL_RUNTIME_POD_NAMESPACE,
        name: parsed.data.TETRAL_RUNTIME_POD_NAME,
        uid: parsed.data.TETRAL_RUNTIME_POD_UID,
        ip: parsed.data.TETRAL_RUNTIME_POD_IP,
      },
      deploymentEnvironment: parsed.data.TETRAL_DEPLOYMENT_ENVIRONMENT,
      serviceVersion: parsed.data.TETRAL_SERVICE_VERSION,
      bridge: {
        namespace: bridge.namespace,
        serviceAccount: bridge.serviceAccount,
      },
      grpcBindAddress: `0.0.0.0:${parsed.data.TETRAL_RUNTIME_POD_GRPC_PORT}`,
      httpBindAddress: parsed.data.TETRAL_RUNTIME_POD_HTTP_ADDR,
      kubernetesApiServerUrl: parsed.data.KUBERNETES_API_SERVER_URL,
      kubernetesApiCaCertPath: parsed.data.KUBERNETES_API_CA_CERT_PATH,
      tokenReviewReviewerTokenPath: parsed.data.KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH,
      outboundInternalGrpcTokenPath: parsed.data.TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH,
      bridgeApiGrpcAddress: parsed.data.TETRAL_BRIDGE_API_GRPC_ADDR,
      gatewayGrpcAddress: parsed.data.TETRAL_GATEWAY_GRPC_ADDR,
      mcpConnectorGrpcAddress: parsed.data.TETRAL_MCP_CONNECTOR_GRPC_ADDR,
      webConnectorGrpcAddress: parsed.data.TETRAL_WEB_CONNECTOR_GRPC_ADDR,
      providerStreamTimeoutMs: Number(parsed.data.TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS),
      platformModels: {
        approvalReviewer: approvalReviewerModel,
      },
      skillGuidance: {
        descriptionBudgetBytes: Number(parsed.data.TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES),
      },
    },
  };
}

/** Reads the process environment once and delegates to the allowlisted environment loader. */
export function loadRuntimePodConfigFromProcessEnv(): RuntimePodConfigResult {
  return loadRuntimePodConfigFromEnv(process.env);
}

/**
 * Projects only recognized Runtime Pod keys from an environment map before strict validation.
 * Unrelated process variables therefore cannot become configuration or trigger unknown-key errors.
 */
export function loadRuntimePodConfigFromEnv(env: Record<string, string | undefined>): RuntimePodConfigResult {
  const projected: Record<string, string | undefined> = {};
  for (const key of RuntimePodEnvKeys) {
    projected[key] = env[key];
  }
  return loadRuntimePodConfig(projected);
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

/** Parses one provider/model identifier without throwing on malformed configuration. */
export function parseModelRef(value: string): RuntimePodModelRef | undefined {
  const [providerId, modelId, extra] = value.split("/");
  if (
    providerId === undefined ||
    modelId === undefined ||
    extra !== undefined ||
    providerId.length === 0 ||
    modelId.length === 0
  ) {
    return undefined;
  }
  return { providerId, modelId };
}

function positiveIntegerString(value: string): boolean {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 && String(parsed) === value;
}
