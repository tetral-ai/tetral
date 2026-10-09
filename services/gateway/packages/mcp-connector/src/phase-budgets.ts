/**
 * Owns the wall-clock budget allocated between a durable MCP tool-result claim
 * and its commit. Client, credential, and Bridge adapters consume the
 * individual bounds defined here. The shared execution budget clips each phase without renewing on retries.
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

/** Claim dispatch through prepared result, leaving the first commit reserve. */
export const MCP_EXECUTION_TIMEOUT_MS = 170_000;
export const MCP_FIRST_COMMIT_RESERVE_MS = 10_000;
export const MCP_CALL_TIMEOUT_MS = 120_000;
export const MCP_DISCOVERY_TIMEOUT_MS = 120_000;
export const MCP_SESSION_IDLE_TIMEOUT_MS = 1_800_000;
