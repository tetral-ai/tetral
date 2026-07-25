/**
 * Owns the wall-clock budget allocated between a durable MCP tool-result claim
 * and its commit. Client, credential, and Bridge adapters consume the
 * individual bounds defined here. These constants do not by themselves define
 * one end-to-end deadline across retries.
 *
 * @packageDocumentation
 */

/** Mirrors the Bridge-owned durable MCP claim lease. */
export const MCP_CLAIM_LEASE_SECONDS = 180;
/** Bounds credential selection, decryption, and any nested refresh work. */
export const MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS = 15_000;
/** Bounds the provider OAuth token-endpoint request within credential resolution. */
export const MCP_REFRESH_HTTP_TIMEOUT_MS = 10_000;
/** Bounds MCP transport connection and protocol initialization. */
export const MCP_CONNECT_TIMEOUT_MS = 10_000;
/** Bounds the durable Bridge Claim acknowledgement before external execution begins. */
export const MCP_CLAIM_RPC_TIMEOUT_MS = 10_000;
/** Bounds the durable Bridge commit after the external tool call returns. */
export const MCP_COMMIT_RPC_TIMEOUT_MS = 10_000;
