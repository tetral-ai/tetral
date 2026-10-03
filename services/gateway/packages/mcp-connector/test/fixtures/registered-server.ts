/** Static installed configuration for component owners that do not exercise SQL resolution. */
import { adapterById } from '../../src/adapters/registry.js';
import { McpConnectorError } from '../../src/errors.js';
import type { McpServerResolver } from '../../src/server-resolver.js';
export function registeredServer(id = 'github', configuredName = id) {
  const adapter = adapterById(id);
  if (adapter === undefined) throw new Error('Unregistered fixture adapter.');
  return { configuredName, endpoint: adapter.endpoint, adapter };
}
export const fixtureServerResolver: McpServerResolver = {
  async resolve(input) {
    input.signal?.throwIfAborted();
    if (adapterById(input.mcpServerName) === undefined) throw new McpConnectorError('mcp_invalid_input', 'Unsupported configured fixture server.');
    return registeredServer(input.mcpServerName);
  },
};
