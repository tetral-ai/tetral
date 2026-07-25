/**
 * @packageDocumentation
 *
 * Authenticates internal callers of the MCP connector and creates bearer metadata for its
 * outbound Bridge calls. The connector bootstrap and RPC service call this module, which reads
 * projected service-account material and calls the Kubernetes TokenReview API. It guards the
 * fixed internal audience, method-specific service-account allowlists, presence of a reviewed pod
 * UID, and bounded failure messages without exposing bearer tokens.
 */
import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";

const Audience = "tetral-internal-grpc";
const McpMethods = new Set([
  "/tetral.provider_gateway.v1.McpConnectorService/RunMcpTool",
  "/tetral.provider_gateway.v1.McpConnectorService/ListMcpTools",
]);

/** Identifies a Kubernetes service account and the pod that presents its bound token. */
export interface ServiceAccountIdentity {
  readonly namespace: string;
  readonly name: string;
  readonly podUid: string;
}

/** Locates the projected service-account token used for an outbound internal RPC. */
export interface ServiceAccountTokenConfig {
  readonly tokenPath: string;
}

/** Supplies the metadata, method, verifier, and allowed identities for one inbound MCP RPC. */
export interface McpCallerAuthInput {
  readonly metadata: Metadata;
  readonly method: string;
  readonly tokenReviewClient: McpTokenReviewClient;
  readonly allowedRuntimePod: Pick<ServiceAccountIdentity, "namespace" | "name">;
  readonly allowedBridge: Pick<ServiceAccountIdentity, "namespace" | "name">;
}

/** Abstracts Kubernetes TokenReview for fail-closed caller authentication. */
export interface McpTokenReviewClient {
  createTokenReview(input: { readonly token: string; readonly audiences: readonly string[] }): Promise<{
    readonly authenticated: boolean;
    readonly audiences: readonly string[];
    readonly username: string;
    readonly podUid: string;
  }>;
}

/** Configures Kubernetes TokenReview transport, reviewer credentials, CA trust, and test injection. */
export interface KubernetesTokenReviewClientOptions {
  readonly apiServerUrl: string;
  readonly reviewerTokenPath: string;
  readonly apiServerCaCertPath: string;
  readonly fetchImpl?: (url: string, init: BunTokenReviewRequestInit) => Promise<Response>;
}

/** Adds Bun's per-request TLS CA material to a standard fetch request initializer. */
export type BunTokenReviewRequestInit = RequestInit & {
  readonly tls: {
    readonly ca: readonly Blob[];
  };
};

/** Reports either the verified caller identity or a bounded gRPC authentication decision. */
export type McpCallerAuthResult =
  | { readonly ok: true; readonly serviceAccount: ServiceAccountIdentity }
  | { readonly ok: false; readonly code: "Unauthenticated" | "PermissionDenied"; readonly message: string };

/**
 * Reads a projected service-account token and returns bearer metadata for an internal RPC.
 *
 * @throws A generic error when the token file is unavailable, empty after trimming, or contains whitespace.
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
 * Authenticates and authorizes one MCP connector RPC caller.
 *
 * The function accepts exactly one bearer token, verifies the fixed internal audience, requires a
 * reviewed pod UID, parses a canonical service-account username, and applies the service-account
 * allowlist for the requested MCP method. Missing metadata and TokenReview failures return bounded
 * decisions.
 */
export async function authenticateMcpCaller(input: McpCallerAuthInput): Promise<McpCallerAuthResult> {
  const token = bearerToken(input.metadata);
  if (token === undefined) {
    return unauthenticated();
  }
  let response: Awaited<ReturnType<McpTokenReviewClient["createTokenReview"]>>;
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
  const allowedCaller = allowedCallerForMethod(input);
  if (
    allowedCaller === undefined ||
    allowedCaller.namespace !== serviceAccount.namespace ||
    allowedCaller.name !== serviceAccount.name
  ) {
    return { ok: false, code: "PermissionDenied", message: "permission denied" };
  }
  if (response.podUid === "") {
    return unauthenticated();
  }
  return { ok: true, serviceAccount: { ...serviceAccount, podUid: response.podUid } };
}

function allowedCallerForMethod(input: McpCallerAuthInput): Pick<ServiceAccountIdentity, "namespace" | "name"> | undefined {
  if (!McpMethods.has(input.method)) {
    return undefined;
  }
  if (input.method === "/tetral.provider_gateway.v1.McpConnectorService/RunMcpTool") {
    return input.allowedRuntimePod;
  }
  if (input.method === "/tetral.provider_gateway.v1.McpConnectorService/ListMcpTools") {
    return input.allowedBridge;
  }
  return undefined;
}

/** Calls the Kubernetes TokenReview API with projected reviewer credentials and pinned CA material. */
export class KubernetesTokenReviewClient implements McpTokenReviewClient {
  private readonly fetchImpl: (url: string, init: BunTokenReviewRequestInit) => Promise<Response>;

  constructor(private readonly options: KubernetesTokenReviewClientOptions) {
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  async createTokenReview(input: { readonly token: string; readonly audiences: readonly string[] }): Promise<{
    readonly authenticated: boolean;
    readonly audiences: readonly string[];
    readonly username: string;
    readonly podUid: string;
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
        readonly user?: {
          readonly username?: string;
          readonly extra?: Record<string, readonly string[]>;
        };
      };
    };
    return {
      authenticated: body.status?.authenticated === true,
      audiences: body.status?.audiences ?? [],
      username: body.status?.user?.username ?? "",
      podUid: body.status?.user?.extra?.["authentication.kubernetes.io/pod-uid"]?.[0] ?? "",
    };
  }
}

/**
 * Verifies that TokenReview reviewer credentials and Kubernetes CA material are readable and non-empty.
 *
 * Connector startup calls this before accepting RPC traffic.
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

function serviceAccountFromUsername(username: string): Pick<ServiceAccountIdentity, "namespace" | "name"> | undefined {
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

function unauthenticated(): McpCallerAuthResult {
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
