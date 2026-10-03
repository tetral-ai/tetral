import { observeProtocol } from '../fixtures/protocol-observations.js';
import { expect, test as bunTest } from 'bun:test';
let caseName = '';
const test = (name: string, body: () => void | Promise<void>, timeout?: number) => bunTest(name, async () => { caseName=name; await body(); }, timeout);
function peerFor(adapter: 'github'|'slack') { const peer=new McpHTTPProtocolFixture(adapter); observeProtocol(peer,caseName); return peer; }
import { createHmac } from 'node:crypto';
import { Server, ServerCredentials, Metadata, credentials, status } from '@grpc/grpc-js';
import { AgentRuntimeBridgeServiceService } from '@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js';
import type { AgentRuntimeBridgeServiceServer, CommitMcpToolResultRequest } from '@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js';
import { McpConnectorServiceClient, McpErrorKind, RunMcpToolStatus } from '@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js';
import type { RunMcpToolRequest, RunMcpToolResponse } from '@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js';
import { createRuntimeBindingTokenVerifier } from '@tetral/gateway-protocol/src/binding-token.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { BridgeAPIMcpToolResultIdempotencyStore, BridgeAPIManifestChangeNotifier } from '../../src/bridge-client.js';
import { McpSDKClient, streamableHTTPTransportOptions } from '../../src/client.js';
import { McpConnectorServiceShell } from '../../src/service.js';
import { createJsonLogger } from '../../src/logger.js';
import { createMcpConnectorGrpcServer } from '../../src/server.js';
import { McpHTTPProtocolFixture, until } from '../fixtures/mcp-http-protocol.js';
import { DiscoverySDKClient } from '../../src/discovery.js';
import type { RequestOptions } from '@modelcontextprotocol/sdk/shared/protocol.js';
import { registeredServer } from '../fixtures/registered-server.js';
const key='test-runtime-binding-hmac-key-of-32bytes';
interface ClockCharges {
 now: () => number;
 claim: () => void;
 preparation: () => void;
}
async function composition(sinkFails=false, loseAck=false, clock?: ClockCharges){
 const lines:string[]=[];const logger=createJsonLogger({write:line=>{if(sinkFails)throw new Error("fixture-sink-secret");lines.push(line);}});
 const peer=peerFor('github');peer.notificationsEnabled=false;const commits:CommitMcpToolResultRequest[]=[];const stored=new Map<string,string>();let notifications=0;let verifierLists=0;
 let rpc:McpConnectorServiceClient|undefined;
 const bridge=new Server();bridge.addService(AgentRuntimeBridgeServiceService,{
  claimMcpToolResult:(call,callback)=>{const result=stored.get(call.request.toolUseEventId);clock?.claim();callback(null,result===undefined?{acquired:{mcpServerName:'work-github',toolName:peer.tools[0]!.name,inputJson:'{"nonce":"rpc-control"}'}}:{alreadyCompleted:{resultJson:result}});},
  commitMcpToolResult:(call,callback)=>{commits.push(call.request);stored.set(call.request.toolUseEventId,call.request.resultJson);if(loseAck&&commits.length===1){callback({code:status.UNAVAILABLE,message:'fixture-ack-sentinel'});return;}callback(null,loseAck?{duplicate:{attachmentRef:''}}:{committed:{attachmentRef:''}});},
  relinquishMcpToolResult:(_call,callback)=>callback(null,{relinquished:{}}),
  mcpManifestChanged:(call,callback)=>{notifications++;rpc!.listMcpTools({workspaceId:call.request.workspaceId,sessionId:call.request.sessionId,mcpServerName:call.request.mcpServerName},metadata(),(error,response)=>{verifierLists++;if(error)callback(error);else{expect(response.manifestEtag).toBe(call.request.manifestEtag);callback(null,{committed:{}});}});},
 }as AgentRuntimeBridgeServiceServer);
 const port=await new Promise<number>((resolve,reject)=>bridge.bindAsync('127.0.0.1:0',ServerCredentials.createInsecure(),(error,port)=>error?reject(error):resolve(port)));
 const store=new BridgeAPIMcpToolResultIdempotencyStore({address:'127.0.0.1:'+port,tokenPath:'unused',metadataFactory:async()=>metadata(),logger});
 const notifier=new BridgeAPIManifestChangeNotifier({address:'127.0.0.1:'+port,tokenPath:'unused',metadataFactory:async()=>metadata()});
 let service!:McpConnectorServiceShell;
 const clientTimeouts:number[]=[];const sdkTimeouts:number[]=[];
 class ObservedSDK extends DiscoverySDKClient {
  override async callTool(params:Parameters<DiscoverySDKClient['callTool']>[0],schema?:Parameters<DiscoverySDKClient['callTool']>[1],options?:RequestOptions){
   if(options?.timeout!==undefined)sdkTimeouts.push(options.timeout);
   return super.callTool(params,schema,options);
  }
 }
 class ObservedClient extends McpSDKClient {
  override async callTool(input:Parameters<McpSDKClient['callTool']>[0],options?:Parameters<McpSDKClient['callTool']>[1]){
   if(options?.timeoutMs!==undefined)clientTimeouts.push(options.timeoutMs);
   return super.callTool(input,options);
  }
 }
 const client=new ObservedClient({logger,monotonicNow:clock?.now,createClient:clock===undefined?undefined:()=>new ObservedSDK({name:'rpc-component-sdk',version:'1'},{}),serverResolver:{async resolve(){return registeredServer('github','work-github');}},credentialResolver:{async resolve(){return {ok:true,mode:'bearer',token:'fixture',tokenHash:'fixture',vaultId:'v',credentialId:'c'};},async refresh(){throw new Error('Unexpected refresh');}},onToolsListChanged:async input=>{await service.handleToolsListChangedNotification(input);},onConnectionReady:async(input,tools,options)=>{await service.handleConnectionReady(input,tools,options);clock?.preparation();},createTransport:input=>new StreamableHTTPClientTransport(peer.url,streamableHTTPTransportOptions(input))});
 service=new McpConnectorServiceShell({monotonicNow:clock?.now,client,idempotencyStore:store,manifestChangeNotifier:notifier,authenticator:{async authenticate({metadata}){return metadata.get('authorization')[0]==='Bearer component-caller'?{ok:true,serviceAccount:{namespace:'tetral-agent-runtime',name:'agent-runtime',podUid:'rpc-pod'}}:{ok:false,code:'Unauthenticated',message:'caller rejected'};}},runtimeBindingTokenVerifier:createRuntimeBindingTokenVerifier({hmacKey:key}),ready:()=>true,logger});
 const server=createMcpConnectorGrpcServer(service);const connectorPort=await server.bind('127.0.0.1:0');rpc=new McpConnectorServiceClient('127.0.0.1:'+connectorPort,credentials.createInsecure());
 const run=(event:string,timeoutMs=5000)=>new Promise<RunMcpToolResponse>((resolve,reject)=>rpc!.runMcpTool(request(event),metadata(),{deadline:new Date(Date.now()+timeoutMs)},(error,response)=>error?reject(error):resolve(response)));
 const runShell=(event:string,timeoutMs?:number)=>service.runMcpTool(request(event),metadata(),timeoutMs===undefined?undefined:{timeoutMs});
 return {peer,client,commits,run,runShell,clientTimeouts,sdkTimeouts,lines,logger,notifications:()=>notifications,verifierLists:()=>verifierLists,close:async()=>{rpc!.close();await service.shutdown(new Date(Date.now()+5000));await client.closeAll();await store.close();await notifier.close();await server.shutdown();bridge.forceShutdown();await peer.close();}};
}
function metadata(){const metadata=new Metadata();metadata.set('authorization','Bearer component-caller');return metadata;}
function request(toolUseEventId:string):RunMcpToolRequest{
 const identity={workspaceId:'wksp_rpc',sessionId:'sesn_rpc',sessionThreadId:'thrd_rpc',bindingId:'bind_rpc',bindingGeneration:1,runtimeProcessId:'rpc-process'};
 const payload=Buffer.from(JSON.stringify({v:1,workspace_id:identity.workspaceId,session_id:identity.sessionId,session_thread_id:identity.sessionThreadId,binding_id:identity.bindingId,binding_generation:identity.bindingGeneration,runtime_pod_uid:'rpc-pod',runtime_process_id:identity.runtimeProcessId,exp:Math.floor(Date.now()/1000)+60})).toString('base64url');
 return {...identity,toolUseEventId,runtimeBindingToken:`rtbt_v1.${payload}.${createHmac('sha256',key).update(payload).digest('base64url')}`};
}

test('execution-created readiness reports outside initializer and Bridge verification re-lists published SDK client',async()=>{
 const f=await composition();try{await expect(f.run('sevt_rpc_cold')).resolves.toMatchObject({status:RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED});expect(f.notifications()).toBe(1);expect(f.verifierLists()).toBe(1);expect(f.peer.counts).toMatchObject({initialize:1,list:2,call:1});await f.run('sevt_rpc_warm');expect(f.peer.counts).toMatchObject({initialize:1,list:2,call:2});expect(f.commits.map(commit=>commit.toolUseEventId)).toEqual(['sevt_rpc_cold','sevt_rpc_warm']);const records=f.lines.map(line=>JSON.parse(line));for(const phase of ['server_resolution','credential_resolution','connect','readiness','manifest_preparation','tool_call','claim','execution','first_commit'])expect(records.some(record=>record.phase===phase&&record['workspace.id']==='wksp_rpc'&&record['session.id']==='sesn_rpc'&&typeof record['duration.ms']==='number')).toBe(true);const callRecord=records.find(record=>record.phase==='tool_call');expect(callRecord).toMatchObject({'mcp.tool_use_event_id':'sevt_rpc_cold',attempt:1});expect(callRecord['timeout.elapsed_ms']).toBeGreaterThanOrEqual(0);expect(callRecord['timeout.remaining_ms']).toBeGreaterThan(0);const claim=records.find(record=>record.phase==='claim');expect(callRecord['request.id']).toBe(claim['request.id']);expect(records.find(record=>record.phase==='first_commit')).toMatchObject({'request.id':claim['request.id'],'mcp.tool_use_event_id':'sevt_rpc_cold',outcome:'ack_received',attempt:1});}finally{await f.close();}
},10000);

test('actual caller gRPC deadline cancels held HTTP and commits mcp_timeout on original Tool Use, then replay has no call',async()=>{
 const f=await composition();await f.client.listTools({workspaceId:'wksp_rpc',sessionId:'sesn_rpc',mcpServerName:'work-github'});const held=f.peer.hold('tools/call');
 const operation=f.run('sevt_rpc_deadline',1000);void operation.catch(()=>undefined);
 try{await held.entered;await expect(operation).rejects.toMatchObject({code:4});await until(()=>f.commits.length===1&&f.peer.counts.cancelledCalls===1);const result=JSON.parse(f.commits[0]!.resultJson);expect(result.response).toMatchObject({status:RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR,error_kind:McpErrorKind.MCP_ERROR_KIND_TIMEOUT});expect(f.peer.counts.call).toBe(1);expect(f.peer.counts.effects).toBe(1);await expect(f.run('sevt_rpc_deadline')).resolves.toMatchObject({errorKind:McpErrorKind.MCP_ERROR_KIND_TIMEOUT});expect(f.peer.counts.call).toBe(1);}finally{held.release();await f.close();}
},10000);


test('actual SDK dependency diagnostics stay out of owner logs and throwing sink preserves receipt',async()=>{
 const f=await composition();try{const sentinel='SENTINEL_RAW_HTTP_BODY';f.peer.faultBody=sentinel;f.peer.faults.set('tools/call',[500]);await expect(f.run('sevt_safe_failure')).resolves.toMatchObject({status:RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,errorKind:McpErrorKind.MCP_ERROR_KIND_INTERNAL});expect(f.commits).toHaveLength(1);expect(f.lines.join('')).not.toContain(sentinel);expect(f.lines.map(line=>JSON.parse(line)).find(record=>record.phase==='tool_call')).toMatchObject({outcome:'failed'});expect(f.peer.counts.effects).toBe(0);}finally{await f.close();}
 const broken=await composition(true);try{await expect(broken.run('sevt_sink_failure')).resolves.toMatchObject({status:RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED});expect(broken.commits).toHaveLength(1);expect(broken.peer.counts.effects).toBe(1);expect(broken.logger.stats().sinkFailures).toBeGreaterThan(0);}finally{await broken.close();}
},10000);


test('lost first Commit ACK converges original receipt without another external execution',async()=>{
 const f=await composition(false,true);try{await expect(f.run('sevt_receipt_recovery')).resolves.toMatchObject({status:RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED});expect(f.peer.counts).toMatchObject({call:1,effects:1});expect(f.commits).toHaveLength(2);expect(f.commits[1]).toEqual(f.commits[0]);const records=f.lines.map(line=>JSON.parse(line));expect(records.find(record=>record.phase==='first_commit')).toMatchObject({outcome:'failed',attempt:1});expect(records.find(record=>record.phase==='receipt_recovery')).toMatchObject({outcome:'converged',attempt:1,'request.id':f.commits[0]!.claimId,'mcp.tool_use_event_id':'sevt_receipt_recovery'});expect(records.filter(record=>record.phase==='tool_call')).toHaveLength(1);expect(f.lines.join('')).not.toContain('fixture-ack-sentinel');}finally{await f.close();}
},10000);


for(const row of [
 {name:'default total',callerMs:undefined,claimMs:166000,remainingMs:4000},
 {name:'shorter caller',callerMs:10000,claimMs:9000,remainingMs:1000},
])test(`ServiceShell claim consumes the original monotonic allowance: ${row.name}`,async()=>{
 let elapsed=0;const f=await composition(false,false,{now:()=>elapsed,claim:()=>{elapsed+=row.claimMs;},preparation:()=>undefined});
 const event='sevt_claim_budget';
 try{
  // Direct shell entry keeps its caller allowance in the injected clock domain;
  // claim, manifest verification and commit still cross actual Bridge RPCs.
  await expect(f.runShell(event,row.callerMs)).resolves.toMatchObject({status:RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED});
  expect(f.clientTimeouts).toEqual([row.remainingMs]);
  expect(f.sdkTimeouts).toEqual([row.remainingMs]);
  expect(f.peer.counts).toMatchObject({initialize:1,list:2,call:1,effects:1});
  expect(f.commits).toHaveLength(1);
  expect(f.commits[0]!.toolUseEventId).toBe(event);
  const claim=f.lines.map(line=>JSON.parse(line)).find(record=>record.phase==='claim');
  expect(claim).toMatchObject({'timeout.elapsed_ms':row.claimMs,'timeout.remaining_ms':row.remainingMs});
  expect(f.commits[0]!.claimId).toBe(claim['request.id']);
  console.info(JSON.stringify({kind:'mcp-service-budget-observation',case:caseName,claimElapsedMs:row.claimMs,clientTimeoutMs:f.clientTimeouts[0],sdkTimeoutMs:f.sdkTimeouts[0],commits:f.commits.length}));
 }finally{await f.close();}
},10000);

for(const row of [
 {name:'default total',callerMs:undefined,totalMs:170000,claimMs:1000},
 {name:'shorter caller',callerMs:10000,totalMs:10000,claimMs:2000},
])test(`synchronous manifest preparation exhausts the shared ServiceShell allowance before dispatch: ${row.name}`,async()=>{
 let elapsed=0;const preparationMs=row.totalMs-row.claimMs;
 const f=await composition(false,false,{now:()=>elapsed,claim:()=>{elapsed+=row.claimMs;},preparation:()=>{
  // Charge synchronous preparation before the ready callback returns: no
  // timer/sleep/abort is involved, so dispatch must inspect shared time.
  elapsed+=preparationMs;
 }});
 const event='sevt_preparation_budget';
 try{
  await expect(f.runShell(event,row.callerMs)).resolves.toMatchObject({status:RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR,errorKind:McpErrorKind.MCP_ERROR_KIND_TIMEOUT});
  expect(f.clientTimeouts).toEqual([preparationMs]);expect(f.sdkTimeouts).toEqual([]);
  expect(f.notifications()).toBe(1);expect(f.verifierLists()).toBe(1);
  expect(f.peer.counts).toMatchObject({initialize:1,list:2,call:0,effects:0});
  expect(f.commits).toHaveLength(1);expect(f.commits[0]!.toolUseEventId).toBe(event);
  expect(JSON.parse(f.commits[0]!.resultJson).response).toMatchObject({status:RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR,error_kind:McpErrorKind.MCP_ERROR_KIND_TIMEOUT});
  const records=f.lines.map(line=>JSON.parse(line));const claim=records.find(record=>record.phase==='claim');
  expect(f.commits[0]!.claimId).toBe(claim['request.id']);
  expect(records.find(record=>record.phase==='manifest_preparation')).toMatchObject({'timeout.elapsed_ms':row.totalMs,'timeout.remaining_ms':0});
  expect(records.filter(record=>record.phase==='tool_call')).toHaveLength(1);
  expect(records.find(record=>record.phase==='tool_call')).toMatchObject({outcome:'mcp_timeout','timeout.elapsed_ms':row.totalMs,'timeout.remaining_ms':0});
  console.info(JSON.stringify({kind:'mcp-service-budget-observation',case:caseName,claimElapsedMs:row.claimMs,preparationElapsedMs:preparationMs,totalElapsedMs:elapsed,clientTimeoutMs:f.clientTimeouts[0],externalCalls:f.peer.counts.call,commits:f.commits.length}));
 }finally{await f.close();}
},10000);
