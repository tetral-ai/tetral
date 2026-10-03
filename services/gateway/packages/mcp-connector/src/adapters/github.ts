import type { McpServerAdapter } from './types.js';
export const GITHUB_MCP_TOOLSETS_HEADER = "X-MCP-Toolsets";
export const githubAdapter = Object.freeze({
  id: 'github', endpoint: 'https://api.githubcopilot.com/mcp/',
  requestHeaders: Object.freeze({ [GITHUB_MCP_TOOLSETS_HEADER]: 'default,actions' }),
}) satisfies McpServerAdapter;
