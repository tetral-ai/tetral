/**
 * @packageDocumentation
 *
 * Verifies Runtime binding tokens at Gateway request admission. A token is an
 * `rtbt_v1` envelope containing a base64url JSON payload and an HMAC-SHA256
 * signature over that encoded payload. Verification fails closed unless the
 * envelope has exactly three parts, the signature is valid, the version is
 * supported, the expiry is in the future, and every workspace, session,
 * thread, binding, generation, and Runtime pod claim exactly matches the
 * admitted request and authenticated caller.
 *
 * The provider-gateway and MCP connector service shells call the verifier
 * after workload authentication and request-shape validation and before
 * provider, credential, or MCP work. Their process composition roots create
 * it with the HMAC key shared with the Agent Runtime Bridge token signer and
 * pass the authenticated caller's pod UID separately from the bearer token.
 * This module delegates signing checks to Node crypto and performs no request
 * validation, credential resolution, durable state access, or outbound I/O.
 */

import { createHmac, timingSafeEqual } from "node:crypto";

/** Identifies the Runtime binding claims shared by provider and tool requests. */
export interface RuntimeBindingRequestIdentity {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
}

/**
 * Checks whether a bearer token is unexpired and bound to both the request
 * identity and authenticated Runtime pod; the calling service owns admission.
 */
export interface RuntimeBindingTokenVerifier {
  /** Returns `false` for every missing, malformed, expired, or mismatched token. */
  verify(input: {
    readonly request: RuntimeBindingRequestIdentity;
    readonly runtimeBindingToken: string;
    readonly runtimePodUid: string;
  }): boolean;
}

/** Configures Runtime binding token verification and its source of wall-clock time. */
export interface RuntimeBindingTokenVerifierOptions {
  /** HMAC secret shared with the Agent Runtime Bridge token signer. */
  readonly hmacKey: string;
  /** Supplies the current time; defaults to a newly created `Date`. */
  readonly now?: () => Date;
}

interface RuntimeBindingTokenPayload {
  readonly v: number;
  readonly workspace_id: string;
  readonly session_id: string;
  readonly session_thread_id: string;
  readonly binding_id: string;
  readonly binding_generation: number;
  readonly runtime_pod_uid: string;
  readonly exp: number;
}

/**
 * Creates a synchronous, fail-closed verifier for version-one Runtime binding
 * tokens.
 *
 * @throws An `Error` when the HMAC key is shorter than 32 characters.
 */
export function createRuntimeBindingTokenVerifier(options: RuntimeBindingTokenVerifierOptions): RuntimeBindingTokenVerifier {
  if (options.hmacKey.length < 32) {
    throw new Error("runtime binding token verifier is unavailable");
  }
  const now = options.now ?? (() => new Date());
  return {
    verify: ({ request, runtimeBindingToken, runtimePodUid }) =>
      verifyRuntimeBindingToken({
        request,
        runtimeBindingToken,
        runtimePodUid,
        hmacKey: options.hmacKey,
        now: now(),
      }),
  };
}

function verifyRuntimeBindingToken(input: {
  readonly request: RuntimeBindingRequestIdentity;
  readonly runtimeBindingToken: string;
  readonly runtimePodUid: string;
  readonly hmacKey: string;
  readonly now: Date;
}): boolean {
  if (input.runtimePodUid === "") {
    return false;
  }
  const [prefix, payloadPart, signaturePart, extra] = input.runtimeBindingToken.split(".");
  if (prefix !== "rtbt_v1" || payloadPart === undefined || signaturePart === undefined || extra !== undefined) {
    return false;
  }
  if (!signatureMatches(payloadPart, signaturePart, input.hmacKey)) {
    return false;
  }
  const payload = decodePayload(payloadPart);
  if (payload === undefined || payload.v !== 1 || payload.exp <= Math.floor(input.now.getTime() / 1000)) {
    return false;
  }
  return payload.workspace_id === input.request.workspaceId &&
    payload.session_id === input.request.sessionId &&
    payload.session_thread_id === input.request.sessionThreadId &&
    payload.binding_id === input.request.bindingId &&
    payload.binding_generation === input.request.bindingGeneration &&
    payload.runtime_pod_uid === input.runtimePodUid;
}

function signatureMatches(payloadPart: string, signaturePart: string, hmacKey: string): boolean {
  const expected = Buffer.from(createHmac("sha256", hmacKey).update(payloadPart).digest("base64url"), "utf8");
  const actual = Buffer.from(signaturePart, "utf8");
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

function decodePayload(payloadPart: string): RuntimeBindingTokenPayload | undefined {
  try {
    const parsed = JSON.parse(Buffer.from(payloadPart, "base64url").toString("utf8")) as Partial<RuntimeBindingTokenPayload>;
    if (
      parsed.v !== 1 ||
      typeof parsed.workspace_id !== "string" ||
      typeof parsed.session_id !== "string" ||
      typeof parsed.session_thread_id !== "string" ||
      typeof parsed.binding_id !== "string" ||
      typeof parsed.binding_generation !== "number" ||
      typeof parsed.runtime_pod_uid !== "string" ||
      typeof parsed.exp !== "number"
    ) {
      return undefined;
    }
    return parsed as RuntimeBindingTokenPayload;
  } catch {
    return undefined;
  }
}
