/**
 * Owns service-account authentication for Runtime Pod internal gRPC boundaries. Outbound Bridge,
 * Gateway, and tool clients call this module to read a mounted token into request metadata, while
 * application command handling calls it to validate inbound Bridge callers through Kubernetes
 * TokenReview. It requires one bearer credential, the internal audience, a canonical Kubernetes
 * service-account username, an allowed method, and the exact configured Bridge identity; failures
 * expose only fixed messages and never token contents.
 */
import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";

const Audience = "tetral-internal-grpc";
const RuntimePodMethods = new Set([
  "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptInput",
  "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptAgentMail",
  "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptTaskNotification",
  "/tetral.agent_runtime.v1.AgentRuntimePodService/Interrupt",
  "/tetral.agent_runtime.v1.AgentRuntimePodService/ResolveToolConfirmation",
  "/tetral.agent_runtime.v1.AgentRuntimePodService/ApplyRuntimeConfig",
  "/tetral.agent_runtime.v1.AgentRuntimePodService/CleanupSession",
]);

/** Identifies the mounted service-account token used by an outbound internal gRPC client. */
export interface ServiceAccountTokenConfig {
  readonly tokenPath: string;
}

/** Names one Kubernetes service account after successful TokenReview authentication. */
export interface ServiceAccountIdentity {
  readonly namespace: string;
  readonly name: string;
}

/** Supplies the transport metadata, method, verifier, and authorization target for one command. */
export interface RuntimeCallerAuthInput {
  readonly metadata: Metadata;
  readonly method: string;
  readonly tokenReviewClient: RuntimeTokenReviewClient;
  readonly allowedBridge: ServiceAccountIdentity;
}

/** Abstracts Kubernetes TokenReview so command authentication can fail closed without SDK payloads. */
export interface RuntimeTokenReviewClient {
  createTokenReview(input: { readonly token: string; readonly audiences: readonly string[] }): Promise<{
    readonly authenticated: boolean;
    readonly audiences: readonly string[];
    readonly username: string;
  }>;
}

/** Configures the Kubernetes API endpoint, reviewer credentials, CA trust, and optional HTTP adapter. */
export interface KubernetesTokenReviewClientOptions {
  readonly apiServerUrl: string;
  readonly reviewerTokenPath: string;
  readonly apiServerCaCertPath: string;
  readonly fetchImpl?: (url: string, init: BunTokenReviewRequestInit) => Promise<Response>;
}

/** Adds Bun's per-request TLS CA option to a standard fetch request initializer. */
export type BunTokenReviewRequestInit = RequestInit & {
  readonly tls: {
    readonly ca: readonly Blob[];
  };
};

/** Reports either the verified service-account identity or a bounded authentication failure. */
export type RuntimeCallerAuthResult =
  | { readonly ok: true; readonly serviceAccount: ServiceAccountIdentity }
  | { readonly ok: false; readonly code: "Unauthenticated" | "PermissionDenied"; readonly message: string };

/**
 * Reads a mounted service-account token and returns lowercase bearer metadata for an internal RPC.
 *
 * @throws A generic error when the token file is unavailable, empty after trimming, or contains internal whitespace.
 */
export async function buildOutboundBearerMetadata(config: ServiceAccountTokenConfig): Promise<Metadata> {
  let token: string;
  try {
    token = (await readFile(config.tokenPath, "utf8")).trim();
  } catch {
    throw new Error("service account token unavailable");
  }
  if (token.length === 0 || /\s/.test(token)) {
    throw new Error("service account token unavailable");
  }
  const metadata = new Metadata();
  metadata.set("authorization", `bearer ${token}`);
  return metadata;
}

/**
 * Authenticates and authorizes one Runtime Pod command caller.
 *
 * The function asks the injected TokenReview client for the fixed internal audience, accepts only
 * canonical service-account usernames, and permits only the closed Runtime Pod method set for the
 * configured Bridge service account. Missing metadata and verifier failures return a bounded result.
 */
export async function authenticateRuntimeCaller(input: RuntimeCallerAuthInput): Promise<RuntimeCallerAuthResult> {
  const token = bearerToken(input.metadata);
  if (token === undefined) {
    return unauthenticated();
  }
  let response: Awaited<ReturnType<RuntimeTokenReviewClient["createTokenReview"]>>;
  try {
    response = await input.tokenReviewClient.createTokenReview({ token, audiences: [Audience] });
  } catch {
    return unauthenticated();
  }
  if (!response.authenticated || !response.audiences.includes(Audience)) {
    return unauthenticated();
  }
  const serviceAccount = serviceAccountFromUsername(response.username);
  if (serviceAccount === undefined) {
    return unauthenticated();
  }
  if (
    !RuntimePodMethods.has(input.method) ||
    input.allowedBridge.namespace !== serviceAccount.namespace ||
    input.allowedBridge.name !== serviceAccount.name
  ) {
    return { ok: false, code: "PermissionDenied", message: "permission denied" };
  }
  return { ok: true, serviceAccount };
}

/**
 * Implements Runtime TokenReview against the Kubernetes authentication API using mounted reviewer
 * credentials and explicit cluster CA trust. Non-successful HTTP responses remain private adapter
 * failures so the caller can map them to a generic unauthenticated result.
 */
export class KubernetesTokenReviewClient implements RuntimeTokenReviewClient {
  private readonly fetchImpl: (url: string, init: BunTokenReviewRequestInit) => Promise<Response>;

  constructor(private readonly options: KubernetesTokenReviewClientOptions) {
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  /** Submits a TokenReview and normalizes absent response fields to a denied identity. */
  async createTokenReview(input: { readonly token: string; readonly audiences: readonly string[] }): Promise<{
    readonly authenticated: boolean;
    readonly audiences: readonly string[];
    readonly username: string;
  }> {
    const reviewerToken = await readReviewerToken(this.options.reviewerTokenPath);
    const response = await this.fetchImpl(`${this.options.apiServerUrl.replace(/\/$/, "")}/apis/authentication.k8s.io/v1/tokenreviews`, {
      method: "POST",
      tls: {
        ca: [Bun.file(this.options.apiServerCaCertPath)],
      },
      headers: {
        authorization: `bearer ${reviewerToken}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        apiVersion: "authentication.k8s.io/v1",
        kind: "TokenReview",
        spec: {
          token: input.token,
          audiences: input.audiences,
        },
      }),
    });
    if (!response.ok) {
      throw new Error("token review unavailable");
    }
    const body = (await response.json()) as {
      readonly status?: {
        readonly authenticated?: boolean;
        readonly audiences?: readonly string[];
        readonly user?: { readonly username?: string };
      };
    };
    return {
      authenticated: body.status?.authenticated === true,
      audiences: body.status?.audiences ?? [],
      username: body.status?.user?.username ?? "",
    };
  }
}

/**
 * Verifies that the mounted reviewer token and Kubernetes API CA file are readable and non-empty.
 * The command dependency builder calls this during bootstrap before the pod becomes ready.
 */
export async function validateKubernetesTokenReviewReviewerMaterial(
  options: Pick<KubernetesTokenReviewClientOptions, "reviewerTokenPath" | "apiServerCaCertPath">,
): Promise<void> {
  await readReviewerToken(options.reviewerTokenPath);
  await readNonEmptyFile(options.apiServerCaCertPath, "kubernetes api ca material unavailable");
}

function bearerToken(metadata: Metadata): string | undefined {
  const values = metadata.get("authorization");
  if (values.length !== 1 || typeof values[0] !== "string") {
    return undefined;
  }
  const trimmed = values[0].trim();
  const parts = trimmed.split(" ");
  if (parts.length !== 2 || parts[0]?.toLowerCase() !== "bearer" || parts[1] === "") {
    return undefined;
  }
  return parts[1];
}

function serviceAccountFromUsername(username: string): ServiceAccountIdentity | undefined {
  const prefix = "system:serviceaccount:";
  if (!username.startsWith(prefix)) {
    return undefined;
  }
  const parts = username.slice(prefix.length).split(":");
  if (parts.length !== 2 || parts[0] === "" || parts[1] === "") {
    return undefined;
  }
  const namespace = parts[0];
  const name = parts[1];
  if (namespace === undefined || name === undefined) {
    return undefined;
  }
  return { namespace, name };
}

function unauthenticated(): RuntimeCallerAuthResult {
  return { ok: false, code: "Unauthenticated", message: "unauthenticated" };
}

async function readReviewerToken(path: string): Promise<string> {
  try {
    const token = (await readFile(path, "utf8")).trim();
    if (token.length === 0 || /\s/.test(token)) {
      throw new Error("empty token");
    }
    return token;
  } catch {
    throw new Error("service account token unavailable");
  }
}

async function readNonEmptyFile(path: string, message: string): Promise<void> {
  try {
    if ((await readFile(path, "utf8")).trim().length === 0) {
      throw new Error(message);
    }
  } catch {
    throw new Error(message);
  }
}
