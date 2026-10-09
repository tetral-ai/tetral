import type { McpServerAdapter } from './types.js';
export const slackAdapter = Object.freeze({
  id: 'slack', endpoint: 'https://mcp.slack.com/mcp', requestHeaders: Object.freeze({}),
}) satisfies McpServerAdapter;
