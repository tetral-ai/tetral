/**
 * @packageDocumentation
 *
 * Defines the closed catalog of MCP servers that the connector may contact and
 * the exact-match helpers used to enforce it. The catalog admits only its
 * declared name and endpoint; URL comparison removes at most one trailing slash
 * and preserves every other character so alternate authorities, credentials,
 * queries, fragments, casing, and repeated slashes remain outside the catalog.
 * Connector service, client, and credential paths call these helpers before
 * discovery or connection, and the helpers consult only the catalog below.
 */

/** Describes one server in the connector's closed MCP catalog. */
export interface McpCatalogEntry {
  readonly name: "github";
  readonly url: "https://api.githubcopilot.com/mcp/";
}

/** Lists the complete set of MCP server names and endpoints admitted by the connector. */
export const MCP_CATALOG = [
  { name: "github", url: "https://api.githubcopilot.com/mcp/" },
] as const satisfies readonly McpCatalogEntry[];

/** Returns the catalog entry whose name exactly matches the supplied server name. */
export function catalogEntryByName(name: string): McpCatalogEntry | undefined {
  return MCP_CATALOG.find((entry) => entry.name === name);
}

/**
 * Resolves an endpoint to its catalog entry after removing at most one trailing
 * slash.
 *
 * @throws {@link McpCatalogError} when the resulting endpoint is not cataloged.
 */
export function assertCatalogURL(rawURL: string): McpCatalogEntry {
  const normalized = normalizeCatalogURL(rawURL);
  const entry = MCP_CATALOG.find((candidate) => normalizeCatalogURL(candidate.url) === normalized);
  if (entry === undefined) {
    throw new McpCatalogError("unsupported_mcp_server", "MCP server URL is outside the curated catalog.");
  }
  return entry;
}

/** Removes one trailing slash, when present, without otherwise canonicalizing a URL. */
export function normalizeCatalogURL(rawURL: string): string {
  return trimAtMostOneTrailingSlash(rawURL);
}

function trimAtMostOneTrailingSlash(value: string): string {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

/** Identifies rejection of an endpoint that is not in the closed MCP catalog. */
export class McpCatalogError extends Error {
  constructor(
    readonly code: "unsupported_mcp_server",
    message: string,
  ) {
    super(message);
    this.name = "McpCatalogError";
  }
}
