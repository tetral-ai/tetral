import { expect, test } from 'bun:test';
function peerFor(adapter: 'github'|'slack') { return new McpHTTPProtocolFixture(adapter); }
import { McpExecutionBudget, MCP_EXECUTION_TIMEOUT_MS, MCP_FIRST_COMMIT_RESERVE_MS } from '../../src/execution-budget.js';
import { DiscoverySDKClient } from '../../src/discovery.js';
import { McpSDKClient, streamableHTTPTransportOptions } from '../../src/client.js';
import type { McpSDKClientOptions } from '../../src/client.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { McpHTTPProtocolFixture, until } from '../fixtures/mcp-http-protocol.js';
import { registeredServer } from '../fixtures/registered-server.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import type { RequestOptions } from '@modelcontextprotocol/sdk/shared/protocol.js';
const identity={workspaceId:'w',sessionId:'s',mcpServerName:'work-github',sessionThreadId:'t',toolName:'read_echo',input:{nonce:'budget-control'}};
// Every request here carries the unregistered fixture bearer through the direct SDK client.
const sent=(method:string,session=1)=>`direct-sdk ${method} github-session-${session} unrecognized`;

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
 try{const operation=client.callTool(identity,{timeoutMs:row.total});if(row.callTimeout===undefined){await expect(operation).rejects.toMatchObject({code:'mcp_timeout'});expect(peer.counts.call).toBe(0);}else{await operation;expect(requestedTimeout).toBe(row.callTimeout);expect(peer.counts.call).toBe(1);}
  expect(peer.trace()).toEqual([sent('initialize'),sent('tools/list'),...(row.callTimeout===undefined?[]:[sent('tools/call')])]);}finally{await client.closeAll();await peer.close();}
},10_000);

test('authentication refresh crossing the original deadline cannot rebuild or dispatch a second call',async()=>{
 let clock=0,refreshes=0;const peer=peerFor('github');peer.notificationsEnabled=false;peer.faults.set('tools/call',[401]);
 const client=new McpSDKClient({monotonicNow:()=>clock,serverResolver:{async resolve(){return registeredServer('github','work-github');}},credentialResolver:{async resolve(){return {ok:true,mode:'bearer',token:'A',tokenHash:'A',vaultId:'v',credentialId:'c'};},async refresh(){refreshes++;clock=170001;return {ok:true,mode:'bearer',token:'B',tokenHash:'B',vaultId:'v',credentialId:'c'};}},onToolsListChanged:async()=>undefined,createTransport:input=>new StreamableHTTPClientTransport(peer.url,streamableHTTPTransportOptions(input))});
 try{await expect(client.callTool(identity)).rejects.toMatchObject({code:'mcp_timeout'});expect([refreshes,peer.counts.initialize,peer.counts.list,peer.counts.call]).toEqual([1,1,1,1]);expect(peer.trace()).toEqual([sent('initialize'),sent('tools/list'),sent('tools/call')]);}finally{await client.closeAll();await peer.close();}
},10_000);

test('real HTTP held tool response is cancelled when the execution budget expires',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;const held=peer.hold('tools/call');
 const client=new McpSDKClient({executionTimeoutMs:1000,serverResolver:{async resolve(){return registeredServer('github','work-github');}},credentialResolver:{async resolve(){return {ok:true,mode:'bearer',token:'fixture',tokenHash:'fixture',vaultId:'v',credentialId:'c'};},async refresh(){throw new Error('Unexpected refresh');}},onToolsListChanged:async()=>undefined,createTransport:input=>new StreamableHTTPClientTransport(peer.url,streamableHTTPTransportOptions(input))});
 const operation=client.callTool(identity);void operation.catch(()=>undefined);
 try{await held.entered;await expect(operation).rejects.toMatchObject({code:'mcp_timeout'});await until(()=>peer.counts.cancelledCalls===1);expect(peer.counts).toMatchObject({initialize:1,list:1,call:1,effects:1});expect(client.connectionCount()).toBe(0);expect(peer.trace()).toEqual([sent('initialize'),sent('tools/list'),sent('tools/call')]);}finally{held.release();await client.closeAll();await peer.close();}
},10_000);

function timeoutClient(peer: McpHTTPProtocolFixture, options: Partial<McpSDKClientOptions> = {}): McpSDKClient {
 return new McpSDKClient({
  serverResolver:{async resolve(){return registeredServer('github','work-github');}},
  credentialResolver:{async resolve(){return {ok:true,mode:'bearer',token:'fixture',tokenHash:'fixture',vaultId:'v',credentialId:'c'};},async refresh(){throw new Error('Unexpected refresh');}},
  onToolsListChanged:async()=>undefined,
  createTransport:input=>new StreamableHTTPClientTransport(peer.url,streamableHTTPTransportOptions(input)),
  ...options,
 });
}

test('native SDK call timeout retires sole HTTP execution before the total deadline',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;
 const client=timeoutClient(peer,{executionTimeoutMs:1000,callTimeoutMs:50});
 let held:ReturnType<McpHTTPProtocolFixture['hold']>|undefined;
 try{
  await client.listTools(identity);expect(peer.counts).toMatchObject({initialize:1,list:1,call:0,effects:0});peer.resetCounts();
  held=peer.hold('tools/call');const started=performance.now();
  const operation=client.callTool(identity);void operation.catch(()=>undefined);
  await held.entered;await expect(operation).rejects.toMatchObject({code:'mcp_timeout'});
  const rejectionElapsedMs=performance.now()-started;expect(rejectionElapsedMs).toBeLessThan(1000);
  await until(()=>peer.counts.cancelledCalls===1);
  expect(client.connectionCount()).toBe(0);expect(peer.counts).toMatchObject({initialize:0,list:0,call:1,effects:1,cancelledCalls:1});
  held.release();
  await expect(client.callTool({...identity,input:{nonce:'after-native-timeout'}})).resolves.toMatchObject({structuredContent:{nonce:'after-native-timeout'}});
  expect(peer.counts).toMatchObject({initialize:1,list:1,call:2,effects:2});expect(client.connectionCount()).toBe(1);
  expect(peer.trace()).toEqual([sent('tools/call'),sent('initialize',2),sent('tools/list',2),sent('tools/call',2)]);
 }finally{held?.release();await client.closeAll();await peer.close();}
},10_000);

test('native SDK call timeout preserves an unrelated execution owning the same transport',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;
 class NativeTimeoutSDK extends DiscoverySDKClient {
  override async callTool(params:Parameters<DiscoverySDKClient['callTool']>[0],schema?:Parameters<DiscoverySDKClient['callTool']>[1],options?:RequestOptions){
   return super.callTool(params,schema,params.arguments?.nonce==='native-timeout'?{...options,timeout:50}:options);
  }
 }
 const client=timeoutClient(peer,{createClient:()=>new NativeTimeoutSDK({name:'shared-native-timeout-sdk',version:'1'}, {})});
 let held:ReturnType<McpHTTPProtocolFixture['hold']>|undefined;let survivor:Promise<unknown>|undefined;
 try{
  await client.listTools(identity);peer.resetCounts();held=peer.hold('tools/call');
  survivor=client.callTool({...identity,input:{nonce:'survivor'}},{timeoutMs:5000});void survivor.catch(()=>undefined);
  await held.entered;let survivorSettled=false;void survivor.finally(()=>{survivorSettled=true;}).catch(()=>undefined);
  const operation=client.callTool({...identity,input:{nonce:'native-timeout'}},{timeoutMs:1000});void operation.catch(()=>undefined);
  await until(()=>peer.counts.call===2);await expect(operation).rejects.toMatchObject({code:'mcp_timeout'});
  expect(survivorSettled).toBe(false);expect(client.connectionCount()).toBe(1);expect(peer.counts).toMatchObject({call:2,effects:2,cancelledCalls:0});
  held.release();await expect(survivor).resolves.toMatchObject({structuredContent:{nonce:'survivor'}});
  expect(client.connectionCount()).toBe(1);expect(peer.counts).toMatchObject({initialize:0,list:0,call:2,effects:2,cancelledCalls:0});
  expect(peer.trace()).toEqual([sent('tools/call'),sent('tools/call')]);
 }finally{held?.release();await survivor?.catch(()=>undefined);await client.closeAll();await peer.close();}
},10_000);

test('native SDK warm discovery timeout preserves ready transport and concurrent execution',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;let warm=false;
 class NativeTimeoutSDK extends DiscoverySDKClient {
  override async listTools(params?:Parameters<DiscoverySDKClient['listTools']>[0],options?:RequestOptions){
   return super.listTools(params,warm?{...options,timeout:50}:options);
  }
 }
 const client=timeoutClient(peer,{createClient:()=>new NativeTimeoutSDK({name:'discovery-native-timeout-sdk',version:'1'}, {})});
 let heldCall:ReturnType<McpHTTPProtocolFixture['hold']>|undefined,heldList:ReturnType<McpHTTPProtocolFixture['hold']>|undefined,survivor:Promise<unknown>|undefined;
 try{
  await client.listTools(identity);peer.resetCounts();heldCall=peer.hold('tools/call');
  survivor=client.callTool({...identity,input:{nonce:'discovery-survivor'}},{timeoutMs:5000});void survivor.catch(()=>undefined);await heldCall.entered;
  warm=true;heldList=peer.hold('tools/list');const listing=client.listTools(identity,{timeoutMs:1000});void listing.catch(()=>undefined);
  await heldList.entered;await expect(listing).rejects.toMatchObject({code:'mcp_timeout'});
  expect(client.connectionCount()).toBe(1);expect(peer.counts).toMatchObject({initialize:0,list:1,call:1,effects:1,cancelledCalls:0});
  heldList.release();heldCall.release();await expect(survivor).resolves.toMatchObject({structuredContent:{nonce:'discovery-survivor'}});
  expect(client.connectionCount()).toBe(1);expect(peer.counts).toMatchObject({initialize:0,list:1,call:1,effects:1,cancelledCalls:0});
  expect(peer.trace()).toEqual([sent('tools/call'),sent('tools/list')]);
 }finally{heldList?.release();heldCall?.release();await survivor?.catch(()=>undefined);await client.closeAll();await peer.close();}
},10_000);
