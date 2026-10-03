import { observeProtocol } from '../fixtures/protocol-observations.js';
import { expect, test as bunTest } from 'bun:test';
let caseName = '';
const test = (name: string, body: () => void | Promise<void>, timeout?: number) => bunTest(name, async () => { caseName=name; await body(); }, timeout);
function peerFor(adapter: 'github'|'slack') { const peer=new McpHTTPProtocolFixture(adapter); observeProtocol(peer,caseName); return peer; }
import { McpExecutionBudget, MCP_EXECUTION_TIMEOUT_MS, MCP_FIRST_COMMIT_RESERVE_MS } from '../../src/execution-budget.js';
import { DiscoverySDKClient } from '../../src/discovery.js';
import { McpSDKClient, streamableHTTPTransportOptions } from '../../src/client.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { McpHTTPProtocolFixture, until } from '../fixtures/mcp-http-protocol.js';
import { registeredServer } from '../fixtures/registered-server.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import type { RequestOptions } from '@modelcontextprotocol/sdk/shared/protocol.js';
const identity={workspaceId:'w',sessionId:'s',mcpServerName:'work-github',sessionThreadId:'t',toolName:'read_echo',input:{nonce:'budget-control'}};

test('one monotonic allowance clips a remaining phase and never follows wall-clock changes',()=>{
 let clock=0;const budget=new McpExecutionBudget(undefined,()=>clock);clock=166000;expect(budget.timeoutMs(120000)).toBe(4000);clock=170000;expect(()=>budget.timeoutMs()).toThrow('budget exhausted');
 expect(MCP_EXECUTION_TIMEOUT_MS+MCP_FIRST_COMMIT_RESERVE_MS).toBe(180000);
});

for(const row of [{name:'four seconds remain',total:170000,listedAt:166000,callTimeout:4000},{name:'expired before dispatch',total:170000,listedAt:170000},{name:'short caller deadline',total:10000,listedAt:9000,callTimeout:1000}])test(`SDK request deadline propagation: ${row.name}`,async()=>{
 let clock=0;let requestedTimeout:number|undefined;const peer=peerFor('github');peer.notificationsEnabled=false;
 class ObservedSDKClient extends DiscoverySDKClient{
  override async connect(transport:Transport,options?:RequestOptions){expect(options?.timeout).toBeLessThanOrEqual(10000);const result=await super.connect(transport,options);clock+=1000;return result;}
  override async listTools(params?:Parameters<DiscoverySDKClient['listTools']>[0],options?:RequestOptions){const result=await super.listTools(params,options);clock=row.listedAt;return result;}
  override async callTool(params:Parameters<DiscoverySDKClient['callTool']>[0],schema?:Parameters<DiscoverySDKClient['callTool']>[1],options?:RequestOptions){requestedTimeout=options?.timeout;return super.callTool(params,schema,options);}
 }
 const client=new McpSDKClient({monotonicNow:()=>clock,serverResolver:{async resolve(){return registeredServer('github','work-github');}},credentialResolver:{async resolve(){clock+=2000;return {ok:true,mode:'bearer',token:'fixture',tokenHash:'fixture',vaultId:'v',credentialId:'c'};},async refresh(){throw new Error('Unexpected refresh');}},onToolsListChanged:async()=>undefined,createClient:()=>new ObservedSDKClient({name:'budget-sdk',version:'1'}, {}),createTransport:input=>new StreamableHTTPClientTransport(peer.url,streamableHTTPTransportOptions(input))});
 try{const operation=client.callTool(identity,{timeoutMs:row.total});if(row.callTimeout===undefined){await expect(operation).rejects.toMatchObject({code:'mcp_timeout'});expect(peer.counts.call).toBe(0);}else{await operation;expect(requestedTimeout).toBe(row.callTimeout);expect(peer.counts.call).toBe(1);}}finally{await client.closeAll();await peer.close();}
},10_000);

test('authentication refresh crossing the original deadline cannot rebuild or dispatch a second call',async()=>{
 let clock=0,refreshes=0;const peer=peerFor('github');peer.notificationsEnabled=false;peer.faults.set('tools/call',[401]);
 const client=new McpSDKClient({monotonicNow:()=>clock,serverResolver:{async resolve(){return registeredServer('github','work-github');}},credentialResolver:{async resolve(){return {ok:true,mode:'bearer',token:'A',tokenHash:'A',vaultId:'v',credentialId:'c'};},async refresh(){refreshes++;clock=170001;return {ok:true,mode:'bearer',token:'B',tokenHash:'B',vaultId:'v',credentialId:'c'};}},onToolsListChanged:async()=>undefined,createTransport:input=>new StreamableHTTPClientTransport(peer.url,streamableHTTPTransportOptions(input))});
 try{await expect(client.callTool(identity)).rejects.toMatchObject({code:'mcp_timeout'});expect([refreshes,peer.counts.initialize,peer.counts.list,peer.counts.call]).toEqual([1,1,1,1]);}finally{await client.closeAll();await peer.close();}
},10_000);

test('real HTTP held tool response is cancelled when the execution budget expires',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;const held=peer.hold('tools/call');
 const client=new McpSDKClient({executionTimeoutMs:1000,serverResolver:{async resolve(){return registeredServer('github','work-github');}},credentialResolver:{async resolve(){return {ok:true,mode:'bearer',token:'fixture',tokenHash:'fixture',vaultId:'v',credentialId:'c'};},async refresh(){throw new Error('Unexpected refresh');}},onToolsListChanged:async()=>undefined,createTransport:input=>new StreamableHTTPClientTransport(peer.url,streamableHTTPTransportOptions(input))});
 const operation=client.callTool(identity);void operation.catch(()=>undefined);
 try{await held.entered;await expect(operation).rejects.toMatchObject({code:'mcp_timeout'});await until(()=>peer.counts.cancelledCalls===1);expect(peer.counts).toMatchObject({initialize:1,list:1,call:1,effects:1});expect(client.connectionCount()).toBe(0);}finally{held.release();await client.closeAll();await peer.close();}
},10_000);
