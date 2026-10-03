import { githubAdapter } from './github.js';
import { slackAdapter } from './slack.js';
import type { McpServerAdapter } from './types.js';
const protectedHeaders = new Set(['authorization', 'accept', 'content-type', 'mcp-session-id', 'mcp-protocol-version', 'last-event-id']);
/** Check adapter policy at admission, including future registry additions. */
export function validateAdapter(adapter: McpServerAdapter): void {
  for (const header of Object.keys(adapter.requestHeaders)) {
    if (protectedHeaders.has(header.toLowerCase())) throw new Error('MCP adapter owns a protected header.');
  }
}
export const MCP_ADAPTERS: readonly McpServerAdapter[] = Object.freeze([githubAdapter, slackAdapter]);
for (const adapter of MCP_ADAPTERS) validateAdapter(adapter);
/** Remove at most one trailing slash. No other URL spelling is admitted. */
export function normalizeMcpEndpoint(endpoint: string): string { return endpoint.endsWith('/') ? endpoint.slice(0, -1) : endpoint; }
export function adapterById(id: string): McpServerAdapter | undefined { return MCP_ADAPTERS.find(adapter => adapter.id === id); }
export function adapterByEndpoint(endpoint: string): McpServerAdapter | undefined {
  return MCP_ADAPTERS.find(adapter => normalizeMcpEndpoint(adapter.endpoint) === normalizeMcpEndpoint(endpoint));
}
export function requireAdapter(endpoint: string): McpServerAdapter {
  const adapter = adapterByEndpoint(endpoint);
  if (adapter === undefined) throw new Error('Unsupported MCP server endpoint.');
  return adapter;
}
