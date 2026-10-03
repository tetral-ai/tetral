import { observeProtocol } from '../fixtures/protocol-observations.js';
import { expect, test as bunTest } from 'bun:test';
let caseName = '';
const test = (name: string, body: () => void | Promise<void>, timeout?: number) => bunTest(name, async () => { caseName=name; await body(); }, timeout);
function peerFor(adapter: 'github'|'slack') { const peer=new McpHTTPProtocolFixture(adapter); observeProtocol(peer,caseName); return peer; }
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { McpSDKClient, streamableHTTPTransportOptions } from '../../src/client.js';
import { MCP_ADAPTERS, adapterByEndpoint, validateAdapter } from '../../src/adapters/registry.js';
import { McpHTTPProtocolFixture } from '../fixtures/mcp-http-protocol.js';
import { registeredServer } from '../fixtures/registered-server.js';
import { createHash } from 'node:crypto';

for (const adapter of MCP_ADAPTERS) test(`${adapter.id} configured identity routes actual SDK headers and JSON-only peer`, async () => {
  const peer = peerFor(adapter.id as 'github' | 'slack');
  peer.notificationsEnabled = false;
  peer.credentials.set('Bearer fixture-secret', 'selected-vault');
  const client = new McpSDKClient({
    serverResolver: {async resolve(input) { return registeredServer(adapter.id, input.mcpServerName); }},
    credentialResolver: {async resolve() {return {ok: true, mode: 'bearer', token: 'fixture-secret', tokenHash: createHash('sha256').update('fixture-secret').digest('hex'), vaultId: 'v', credentialId: 'c'};}, async refresh() {throw new Error('Unexpected refresh');}},
    onToolsListChanged: async () => undefined,
    createTransport: input => {expect(input.url.href).toBe(adapter.endpoint); return new StreamableHTTPClientTransport(peer.url, streamableHTTPTransportOptions(input));},
  });
  try {
    const input = {workspaceId: 'w',sessionId: 's',mcpServerName: 'work-'+adapter.id,sessionThreadId: 't',toolName: 'read_echo',input: {nonce: 'adapter-control'}};
    await expect(client.callTool(input)).resolves.toMatchObject({structuredContent: {ok: true,nonce: 'adapter-control'}});
    await client.callTool(input);
    expect(peer.counts).toMatchObject({initialize: 1,list: 1,call: 2,effects: 2});
    expect(peer.requests.every(request => request.credentialLabel === 'selected-vault')).toBe(true);
    expect(peer.requests.every(request=>request.accept.includes('application/json')&&request.accept.includes('text/event-stream')&&request.contentType.includes('application/json'))).toBe(true);
    expect(peer.requests.filter(request=>request.method!=='initialize').every(request=>request.protocolVersion==='2025-11-25'&&request.session===peer.requests[0]!.session)).toBe(true);
    expect(peer.requests.map(request => request.toolset)).toEqual(peer.requests.map(() => adapter.id === 'github' ? 'default,actions' : undefined));
  } finally {await client.closeAll(); await peer.close();}
}, 10_000);

test('registry keeps exact endpoint spelling and protected header ownership', () => {
  for (const adapter of MCP_ADAPTERS) {
    expect(adapterByEndpoint(adapter.endpoint.replace(/\/$/, ''))).toBe(adapter);
    expect(adapterByEndpoint(adapter.endpoint.replace(/\/$/, '')+'/')).toBe(adapter);
    for (const endpoint of [adapter.endpoint+'/', adapter.endpoint+'?token=sentinel', adapter.endpoint+'#fragment', adapter.endpoint.replace('https://','https://user:pass@'), adapter.endpoint.replace('/mcp','/other'), adapter.endpoint.replace('https://','http://'), adapter.endpoint.replace('https://','https://lookalike.')]) {
      if (endpoint === 'https://mcp.slack.com/mcp/') continue;
      expect(adapterByEndpoint(endpoint)).toBeUndefined();
    }
    for (const header of ['Authorization','authorization','Mcp-Session-Id','MCP-Protocol-Version','Content-Type','Accept','Last-Event-ID']) expect(() => validateAdapter({...adapter,requestHeaders: {[header]: 'sentinel'}})).toThrow('protected header');
  }
});
