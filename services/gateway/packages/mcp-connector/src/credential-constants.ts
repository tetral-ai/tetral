/**
 * @packageDocumentation
 *
 * Defines the proactive refresh lead time shared by MCP credential selection
 * and the locked OAuth update path. Keeping one value at both decision points
 * ensures near-expiry credentials enter refresh consistently while forced
 * refreshes bypass the window. The credential resolver and update-path modules
 * consume this constant; the module performs no I/O and handles no credential
 * material.
 */

// REFRESH_SKEW_SECONDS bounds the proactive OAuth-refresh window. In
// oauthRefreshAction (credential.ts and credential-update-path.ts) an MCP OAuth
// credential refreshes when expires_at <= now + REFRESH_SKEW_SECONDS, i.e. this
// many seconds of lead time before it actually expires; the value is that lead
// window in seconds.
//
// It gates proactive refresh only, and it is MCP-scoped: the reactive 401/403
// auth-failure path in client.ts (refreshCredential, force=true) forces a
// refresh regardless of expiry and never consults this skew, and the GitHub Git
// clone-token regime carries no equivalent skew (that token is not
// platform-refreshed).
//
// UPDATE-WITH: credential.ts, credential-update-path.ts, client.ts
/**
 * Number of seconds before OAuth expiry when ordinary MCP credential
 * resolution requests a refresh. Authentication-failure refreshes are forced
 * and do not depend on this lead time.
 */
export const REFRESH_SKEW_SECONDS = 60;
