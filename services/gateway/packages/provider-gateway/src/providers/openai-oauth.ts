/**
 * @packageDocumentation
 * Adapts OpenAI Responses SDK requests to the ChatGPT Codex subscription OAuth
 * route and exports the fixed public endpoints and client identifiers shared by
 * request streaming and credential refresh.
 *
 * The adapter removes the SDK authorization header, injects the resolved bearer
 * token and any supplied account identifier, and rewrites only Responses or
 * chat-completions paths to the subscription endpoint. It does not add
 * client-fingerprint headers, choose models, refresh tokens, or mutate response
 * bodies.
 *
 * `clients.ts` wraps its provider fetch with this adapter, while the OAuth
 * refresh writer consumes the issuer and client constants. The adapter delegates
 * the final request to the caller-supplied fetch implementation, which retains
 * provider egress and timeout enforcement.
 */
import type { FetchFunction } from "@ai-sdk/provider-utils";

// Fixed public Codex OAuth client id. It is a flow constant, not credential data:
// the issuer rejects a refresh whose client_id differs from the one the user's
// external login flow used, so this value must equal that client id verbatim. It
// is never stored on the credential row and is never logged.
/** Public OAuth application identifier shared by login and token refresh. */
export const OpenAICodexOAuthClientId = "app_EMoamEEZ73f0CkXaXp7hrann";
/** OpenAI issuer origin used to derive OAuth endpoints. */
export const OpenAICodexOAuthIssuer = "https://auth.openai.com";
/** Token endpoint used for refresh-token rotation. */
export const OpenAICodexOAuthTokenEndpoint = `${OpenAICodexOAuthIssuer}/oauth/token`;
/** ChatGPT Codex subscription endpoint used for Responses requests. */
export const OpenAICodexResponsesEndpoint = "https://chatgpt.com/backend-api/codex/responses";
/** Non-secret placeholder required while constructing the OpenAI SDK client. */
export const OpenAIOAuthDummyAPIKey = "tetral-openai-oauth-dummy-key";

/** Inputs needed to bind one OAuth credential to an outbound provider fetch. */
export interface OpenAIOAuthFetchOptions {
  readonly fetchImpl: FetchFunction;
  readonly accessToken: string;
  readonly accountId?: string | undefined;
}

// Strips the SDK's authorization header, injects the subscription Bearer token
// (and ChatGPT-Account-Id when present), and rewrites /v1/responses and
// /chat/completions paths to the codex subscription endpoint. clients.ts also
// strips stateless input-item ids and reconstructs allowlisted manual redirects.
/**
 * Creates a fetch adapter that swaps SDK authentication for OAuth credentials
 * and routes supported OpenAI paths to the subscription endpoint.
 *
 * Other URLs retain their original destination while the authentication swap
 * still applies, and `preconnect` remains delegated to the supplied fetch
 * implementation.
 */
export function createOpenAIOAuthFetch(options: OpenAIOAuthFetchOptions): FetchFunction {
  const oauthFetch = async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
    const request = new Request(input, init);
    const headers = new Headers(request.headers);
    headers.delete("authorization");
    headers.delete("Authorization");
    headers.set("authorization", `Bearer ${options.accessToken}`);
    // A 2026-07-06 live probe confirmed that the subscription endpoint accepts
    // requests with originator, session-id, and custom User-Agent omitted.
    // Tetral's chosen OAuth header set is authorization plus, when present,
    // ChatGPT-Account-Id only.
    if (options.accountId !== undefined && options.accountId.length > 0) {
      headers.set("ChatGPT-Account-Id", options.accountId);
    }
    const url = rewrittenOpenAIURL(request.url);
    const body = request.method === "GET" || request.method === "HEAD" ? undefined : await request.text();
    return options.fetchImpl(new Request(url, {
      method: request.method,
      headers,
      ...(body !== undefined ? { body } : {}),
      signal: request.signal,
    }));
  };
  return Object.assign(oauthFetch, {
    preconnect: ((...args: Parameters<FetchFunction["preconnect"]>) => options.fetchImpl.preconnect(...args)) satisfies FetchFunction["preconnect"],
  }) satisfies FetchFunction;
}

function rewrittenOpenAIURL(url: string): string {
  const parsed = new URL(url);
  return parsed.pathname.includes("/v1/responses") || parsed.pathname.includes("/chat/completions")
    ? OpenAICodexResponsesEndpoint
    : url;
}
