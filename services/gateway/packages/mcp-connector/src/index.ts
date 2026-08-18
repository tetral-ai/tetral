/**
 * MCP connector: serves the ListMcpTools and RunMcpTool RPCs of
 * McpConnectorService against the curated MCP server catalog, holding decoded
 * media in memory and returning refs-only tool results.
 * Package consumers use this barrel to import the connector's public domain
 * surfaces; the barrel delegates behavior to the authentication, catalog,
 * client, credential, formatting, idempotency, and service modules below.
 *
 * OWNS:
 * - RPCs: ListMcpTools, RunMcpTool (service.ts).
 * - The per-(workspaceId, sessionId, mcpServerName, sha256(token)) cache of MCP
 *   client connections and their idle/eviction lifecycle (client.ts).
 * - The one durable write in this package: the row-locked OAuth
 *   credential-update path (credential-update-path.ts). Every other store
 *   access — credential resolution, session vault lookup — is read-only, and
 *   the connector never writes the attachment store.
 * - Curated catalog membership and request/response bounds (catalog.ts,
 *   bounds.ts).
 *
 * STATE MACHINE (tool-result durability). Every durable attachment and
 * idempotency write is performed by Bridge, not by this package; the connector
 * only holds an executed result in memory and forwards bytes to Bridge:
 * - absent -> in_flight: Connector creates one claimId and ClaimMcpToolResult
 *   reserves the durable toolUseEventId for that execution attempt. Writer:
 *   Bridge (bridge-client.ts).
 * - in_flight -> in_flight: replay of the same claim renews its lease; another
 *   active claim remains in flight; an expired lease is replaced by a new claimId.
 * - in_flight -> stored: after in-memory execution, CommitMcpToolResult ships
 *   inline media; Bridge creates attachment rows and stores the refs-only result.
 * - stored -> stored: a duplicate claim reads the stored refs-only response and
 *   returns a replay outcome without mutating the row.
 *
 * INVARIANTS:
 * - The RunMcpTool response returned to callers is refs-only; decoded media
 *   bytes leave this package only inside the CommitMcpToolResult request.
 * - Attachment rows exist only after Bridge's CommitMcpToolResult succeeds;
 *   the connector is read-only on the attachment store.
 * - The credential-update path is this package's sole durable writer.
 * - This package imports neither the @tetral/gateway-lowering nor the
 *   provider-gateway package.
 *
 * UPDATE-WITH: service.ts, client.ts, bridge-client.ts,
 * credential-update-path.ts, formatter.ts, catalog.ts, bounds.ts.
 *
 * @packageDocumentation
 */
export * from "./auth.js";
export * from "./bounds.js";
export * from "./bridge-client.js";
export * from "./catalog.js";
export * from "./client.js";
export * from "./config.js";
export * from "./credential.js";
export * from "./errors.js";
export * from "./formatter.js";
export * from "./idempotency.js";
export * from "./service.js";
