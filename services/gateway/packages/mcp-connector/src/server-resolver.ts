/** Workspace-scoped installed configuration is the authority for configured MCP names. */
import { asSQLSource } from '@tetral/ts-dbconnect';
import type { SQLSource } from '@tetral/ts-dbconnect';
import type { McpCredentialSQL } from './credential.js';
import type { McpServerAdapter } from './adapters/types.js';
import { adapterByEndpoint } from './adapters/registry.js';
import { McpConnectorError } from './errors.js';
export interface McpServerIdentity { readonly workspaceId: string; readonly sessionId: string; readonly mcpServerName: string; }
export interface ResolvedMcpServer { readonly configuredName: string; readonly endpoint: string; readonly adapter: McpServerAdapter; }
export interface McpServerResolver { resolve(input: McpServerIdentity & { readonly signal?: AbortSignal }): Promise<ResolvedMcpServer>; }
export class SQLMcpServerResolver implements McpServerResolver {
  private readonly source: SQLSource<McpCredentialSQL>;
  constructor(sql: McpCredentialSQL | SQLSource<McpCredentialSQL>) { this.source = asSQLSource(sql); }
  async resolve(input: McpServerIdentity & { readonly signal?: AbortSignal }): Promise<ResolvedMcpServer> {
    input.signal?.throwIfAborted();
    const installed = await this.source.withSQL(async sql => {
      if (sql.begin === undefined) throw new McpConnectorError('mcp_invalid_input', 'MCP server configuration is unavailable.');
      return sql.begin(async tx => {
        await tx`SELECT set_config('tetral.workspace_id', ${input.workspaceId}, true)`;
        const rows = await tx<readonly { installed_tools_json: unknown }[]>`SELECT installed_tools_json FROM sessions WHERE workspace_id = ${input.workspaceId} AND id = ${input.sessionId}`;
        return rows[0]?.installed_tools_json;
      });
    });
    input.signal?.throwIfAborted();
    let config: unknown;
    try { config = typeof installed === 'string' ? JSON.parse(installed) : installed; }
    catch { throw new McpConnectorError('mcp_invalid_input', 'Configured MCP server is unavailable or unsupported.'); }
    const servers = (config as { mcp_servers?: unknown } | undefined)?.mcp_servers;
    const matches = Array.isArray(servers) ? servers.filter((server: unknown) => typeof server === 'object' && server !== null && (server as { name?: unknown }).name === input.mcpServerName) : [];
    const server = matches.length === 1 ? matches[0] as { type?: unknown; url?: unknown } : undefined;
    const adapter = server?.type === 'url' && typeof server.url === 'string' ? adapterByEndpoint(server.url) : undefined;
    if (adapter === undefined) throw new McpConnectorError('mcp_invalid_input', 'Configured MCP server is unavailable or unsupported.');
    return Object.freeze({ configuredName: input.mcpServerName, endpoint: adapter.endpoint, adapter });
  }
}
