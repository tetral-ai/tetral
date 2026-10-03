import { observeProtocol } from '../fixtures/protocol-observations.js';
import { expect, test as bunTest } from 'bun:test';
let caseName = '';
const test = (name: string, body: () => void | Promise<void>, timeout?: number) => bunTest(name, async () => { caseName=name; await body(); }, timeout);
function peerFor(adapter: 'github'|'slack') { const peer=new McpHTTPProtocolFixture(adapter); observeProtocol(peer,caseName); return peer; }
import { createHash } from 'node:crypto';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { DiscoverySDKClient } from '../../src/discovery.js';
import { createJsonLogger } from '../../src/logger.js';
import { McpSDKClient, streamableHTTPTransportOptions } from '../../src/client.js';
import type { McpSDKClientOptions } from '../../src/client.js';
import { McpHTTPProtocolFixture, fixtureTools, barrier, until } from '../fixtures/mcp-http-protocol.js';
import { registeredServer } from '../fixtures/registered-server.js';

const identity = {workspaceId:'w',sessionId:'s',mcpServerName:'work-github'};
const call = {...identity, sessionThreadId:'t',toolName:'read_echo',input:{nonce:'readiness-control'}};
function component(peer: McpHTTPProtocolFixture, options: Partial<McpSDKClientOptions> = {}, onRefresh?:()=>void) {
  let refreshes=0;
  let token='token-0';
  for(let generation=0;generation<4;generation++)peer.credentials.set('Bearer token-'+generation,'generation-'+generation);
  const material=()=>({ok:true as const,mode:'bearer' as const,token,tokenHash:createHash('sha256').update(token).digest('hex'),vaultId:'v',credentialId:'c'});
  const client=new McpSDKClient({
    serverResolver:{async resolve(input){return registeredServer(peer.adapter,input.mcpServerName);}},
    credentialResolver:{async resolve(){return material();},async refresh(){refreshes++; token='token-'+refreshes;onRefresh?.();return material();}},
    onToolsListChanged:async()=>undefined,
    createTransport:input=>new StreamableHTTPClientTransport(peer.url,streamableHTTPTransportOptions(input)),
    ...options,
  });
  return {client,refreshes:()=>refreshes};
}

for (const row of [
  {name:'cold discovery init401 then list401', listing:true, init:[401,0,0],list:[401,0], calls:[],expected:[2,3,2,0],success:true},
  {name:'cold call init401 then call401',init:[401,0,0],list:[],calls:[401,0],expected:[2,3,2,2],success:true},
  {name:'cold call list401 then call401',init:[],list:[401,0],calls:[401],expected:[1,2,2,1]},
  {name:'cold call init401 list401 call401',init:[401,0,0],list:[401,0],calls:[401],expected:[2,3,2,1]},
  {name:'call401 rebuild init401',init:[0,401],list:[],calls:[401],expected:[1,2,1,1]},
  {name:'call401 rebuild list401',init:[],list:[0,401],calls:[401],expected:[1,2,2,1]},
  {name:'init401 retry init401',init:[401,401],list:[],calls:[],expected:[1,2,0,0]},
  {name:'warm call401 retry success',warm:true,init:[],list:[],calls:[401,0],expected:[1,1,1,2],success:true},
  {name:'warm discovery list401 retry success',warm:true,listing:true,init:[],list:[401,0],calls:[],expected:[1,1,2,0],success:true},
]) test(`actual SDK refresh allowances: ${row.name}`, async()=>{
  const peer=peerFor('github');peer.notificationsEnabled=false;
  const f=component(peer);
  try {
    if(row.warm){await f.client.listTools(identity);peer.resetCounts();}
    peer.faults.set('initialize',[...row.init]);peer.faults.set('tools/list',[...row.list]);peer.faults.set('tools/call',[...row.calls]);
    const operation=row.listing?f.client.listTools(identity):f.client.callTool(call);
    if(row.success)await operation;else await expect(operation).rejects.toMatchObject({code:'mcp_authentication_failed'});
    expect([f.refreshes(),peer.counts.initialize,peer.counts.list,peer.counts.call]).toEqual(row.expected);
    expect(peer.counts.effects).toBe(row.success&&!row.listing?1:0);
    expect(peer.requests.at(-1)?.credentialLabel).toBe('generation-'+f.refreshes());
    expect(peer.requests.every(request=>request.toolset==='default,actions'&&request.accept.includes('application/json')&&request.accept.includes('text/event-stream'))).toBe(true);
    expect(peer.requests.filter(request=>request.method!=='initialize').every(request=>request.protocolVersion==='2025-11-25'&&request.session!=='missing-session')).toBe(true);
  } finally {await f.client.closeAll();await peer.close();}
},10_000);

for(const adapter of ['github','slack'] as const)for(const warm of [false,true])for(const result of ['valid','wrong-type','missing','tool-error','error-malformed','jsonrpc-401','jsonrpc-403','no-output'] as const)test(`actual SDK output validation ${adapter} ${warm?'warm':'cold'} ${result}`,async()=>{
 const peer=peerFor(adapter);peer.notificationsEnabled=false;
 if(result==='no-output'){peer.tools=fixtureTools('no-output');peer.result='missing';}else peer.result=result;
 const f=component(peer);
 try{
  if(warm){await f.client.listTools(identity);peer.resetCounts();}
  const operation=f.client.callTool({...call,toolName:peer.tools[0]!.name});
  if(['wrong-type','error-malformed'].includes(result))await expect(operation).rejects.toMatchObject({code:'mcp_invalid_input'});
  else if(result==='missing')await expect(operation).rejects.toMatchObject({code:-32600});
  else if(result==='jsonrpc-401'||result==='jsonrpc-403')await expect(operation).rejects.toMatchObject({code:result==='jsonrpc-401'?401:403});
  else if(result==='no-output')await expect(operation).resolves.toEqual({content:[{type:'text',text:'missing structured result'}],isError:false,refreshTriggered:false});
  else if(result==='tool-error')await expect(operation).resolves.toEqual({isError:true,content:[{type:'text',text:'controlled tool error'}],refreshTriggered:false});
  else await expect(operation).resolves.toEqual({content:[{type:'text',text:JSON.stringify({ok:true,source:adapter+'-fixture',nonce:'readiness-control'})}],structuredContent:{ok:true,source:adapter+'-fixture',nonce:'readiness-control'},refreshTriggered:false});
  expect(peer.counts.effects).toBe(1);
  expect(f.refreshes()).toBe(0);expect(peer.counts.call).toBe(1);expect(peer.counts.initialize).toBe(warm?0:1);expect(peer.counts.list).toBe(warm?0:1);
 }finally{await f.client.closeAll();await peer.close();}
},10_000);

test('held readiness prevents cache/call; one cancelled waiter leaves peer initialization alive',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;
 const held=peer.hold('tools/list');const f=component(peer);const cancelled=new AbortController();
 const a=f.client.callTool(call,{signal:cancelled.signal});void a.catch(()=>undefined);
 const b=f.client.callTool({...call,input:{nonce:'remaining-waiter'}});
 try{
  await held.entered;expect(peer.counts.call).toBe(0);expect(f.client.connectionCount()).toBe(0);
  cancelled.abort(new Error('One waiter left'));await expect(a).rejects.toBeDefined();expect(peer.counts.initialize).toBe(1);
  held.release();await b;expect(peer.counts).toMatchObject({initialize:1,list:1,call:1});expect(f.client.connectionCount()).toBe(1);
 }finally{held.release();await f.client.closeAll();await peer.close();}
},10_000);

test('pending readiness refresh costs are inherited by two calls; subsequent warm call has new allowance',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;peer.faults.set('tools/list',[401,0]);peer.faults.set('tools/call',[401,401]);
 const held=peer.hold('tools/list');const f=component(peer);
 const a=f.client.callTool(call),b=f.client.callTool(call);void a.catch(()=>undefined);void b.catch(()=>undefined);
 try{
  await held.entered;await until(()=>f.client.pendingWaiterCount()===2);held.release();
  const results=await Promise.allSettled([a,b]);expect(results.every(result=>result.status==='rejected'&&(result.reason as {code?:unknown}).code==='mcp_authentication_failed')).toBe(true);
  expect([f.refreshes(),peer.counts.initialize,peer.counts.list,peer.counts.call]).toEqual([1,2,2,2]);
  peer.faults.set('tools/call',[401,0]);await f.client.callTool(call);expect(f.refreshes()).toBe(2);
 }finally{held.release();await f.client.closeAll();await peer.close();}
},10_000);

test('late waiter joins after rotation and inherits pending replacement listing refresh cost',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;peer.faults.set('tools/list',[401,0]);peer.faults.set('tools/call',[401,401,401]);
 const held=peer.hold('tools/list');const f=component(peer);
 const a=f.client.callTool(call),b=f.client.callTool(call);void a.catch(()=>undefined);void b.catch(()=>undefined);
 try{
  await held.entered;expect(f.refreshes()).toBe(1);expect(peer.counts.initialize).toBe(2);
  const late=f.client.callTool(call);void late.catch(()=>undefined);await until(()=>f.client.pendingWaiterCount()===3);held.release();
  const results=await Promise.allSettled([a,b,late]);expect(results.every(result=>result.status==='rejected'&&(result.reason as {code?:unknown}).code==='mcp_authentication_failed')).toBe(true);
  expect([f.refreshes(),peer.counts.initialize,peer.counts.list,peer.counts.call]).toEqual([1,2,2,3]);
 }finally{held.release();await f.client.closeAll();await peer.close();}
},10_000);

test('already retrying waiter cannot borrow an eligible pending opening refresh',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;
 const released=barrier(),refreshEntered=barrier();let token='A',refreshes=0;
 const material=()=>({ok:true as const,mode:'bearer' as const,token,tokenHash:token,vaultId:'v',credentialId:'c'});
 const f=component(peer,{credentialResolver:{async resolve(){return material();},async refresh(){refreshes++;token=refreshes===1?'B':'C';if(refreshes===1){refreshEntered.resolve();await released.promise;}return material();}}});
 await f.client.listTools(identity);peer.resetCounts();peer.faults.set('tools/call',[401,0]);peer.faults.set('tools/list',[401,0]);
 const retrying=f.client.callTool(call);void retrying.catch(()=>undefined);
 const held=peer.hold('initialize');let independent:Promise<unknown>|undefined;
 try{
  await refreshEntered.promise;independent=f.client.callTool({...call,input:{nonce:'independent'}});await held.entered;
  released.resolve();await until(()=>f.client.pendingWaiterCount()===2);held.release();
  await expect(retrying).rejects.toMatchObject({code:'mcp_authentication_failed'});await independent;
  expect([refreshes,peer.counts.initialize,peer.counts.list,peer.counts.call]).toEqual([2,2,2,2]);expect(peer.counts.effects).toBe(1);
 }finally{released.resolve();held.release();await independent?.catch(()=>undefined);await f.client.closeAll();await peer.close();}
},10_000);

for(const name of ['read-401','read-403'])for(const warm of [false,true])test(`missing SDK output ${name} ${warm?'warm':'cold'} has no authentication provenance`,async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;peer.result='missing';peer.tools=peer.tools.map(tool=>({...tool,name}));const f=component(peer);
 try{if(warm){await f.client.listTools(identity);peer.resetCounts();}await expect(f.client.callTool({...call,toolName:name})).rejects.toMatchObject({code:-32600});expect(peer.counts).toMatchObject({call:1,effects:1,list:warm?0:1,initialize:warm?0:1});expect(f.refreshes()).toBe(0);}finally{await f.client.closeAll();await peer.close();}
},10_000);

for(const successful of [false,true])test(`second page HTTP401 ${successful?'successful replacement excludes old partial tools':'repeat rejection exhausts one listing allowance'}`,async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;const old={...fixtureTools()[0]!,name:'old_partial'};const fresh={...fixtureTools()[0]!,name:'new_session'};
 peer.pages=[{tools:[old],nextCursor:'page-two'},{tools:[]}];peer.faults.set('tools/list',successful?[0,401,0]:[0,401,0,401]);
 const f=component(peer,{},successful?()=>{peer.pages=[{tools:[fresh]}];}:undefined);
 try{if(successful){const listed=await f.client.listTools(identity);expect(listed.map(tool=>tool.name)).toEqual(['new_session']);expect(listed.some(tool=>tool.name==='old_partial')).toBe(false);expect([f.refreshes(),peer.counts.initialize,peer.counts.list,peer.counts.call]).toEqual([1,2,3,0]);expect(peer.requests.filter(request=>request.method==='tools/list').map(request=>request.cursor)).toEqual([undefined,'page-two',undefined]);await expect(f.client.callTool({...call,toolName:'new_session',input:{nonce:'restart-control'}})).resolves.toMatchObject({structuredContent:{ok:true,source:'github-fixture',nonce:'restart-control'}});expect(peer.counts.call).toBe(1);expect(peer.counts.effects).toBe(1);}else{await expect(f.client.listTools(identity)).rejects.toMatchObject({code:'mcp_authentication_failed'});expect([f.refreshes(),peer.counts.initialize,peer.counts.list,peer.counts.call]).toEqual([1,2,4,0]);expect(peer.requests.filter(request=>request.method==='tools/list').map(request=>request.cursor)).toEqual([undefined,'page-two',undefined,'page-two']);expect(f.client.connectionCount()).toBe(0);}const sessions=peer.requests.filter(request=>request.method==='initialize').map(request=>request.session);expect(sessions).toHaveLength(2);expect(sessions[0]).not.toBe(sessions[1]);}finally{await f.client.closeAll();await peer.close();}
},10000);

test('all cancelled readiness owners join and close; later independent request can recover',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;const held=peer.hold('tools/list');const f=component(peer);const one=new AbortController(),two=new AbortController();
 const a=f.client.callTool(call,{signal:one.signal}),b=f.client.callTool(call,{signal:two.signal});void a.catch(()=>undefined);void b.catch(()=>undefined);
 try{await held.entered;await until(()=>f.client.pendingWaiterCount()===2);one.abort(new Error('one'));two.abort(new Error('two'));await Promise.allSettled([a,b]);await until(()=>f.client.pendingWaiterCount()===0);expect(f.client.connectionCount()).toBe(0);expect(peer.counts.call).toBe(0);held.release();await f.client.callTool(call);expect(peer.counts.initialize).toBe(2);expect(peer.counts.call).toBe(1);}finally{held.release();await f.client.closeAll();await peer.close();}
},10_000);

test('held second page publishes no ready client and first-page SDK output metadata survives aggregation',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;peer.pages=[{tools:fixtureTools(),nextCursor:'page-two'},{tools:[{...fixtureTools()[0]!,name:'read_extra'}]}];peer.result='wrong-type';const held=peer.hold('tools/list','page-two');const f=component(peer);
 const operation=f.client.callTool(call);void operation.catch(()=>undefined);
 try{await held.entered;expect(peer.counts.call).toBe(0);expect(f.client.connectionCount()).toBe(0);held.release();const failure=await operation.catch(error=>error);expect(failure).toMatchObject({code:'mcp_invalid_input'});expect(peer.counts).toMatchObject({initialize:1,list:2,call:1,effects:1});expect(f.refreshes()).toBe(0);expect(f.client.connectionCount()).toBe(1);}finally{held.release();await f.client.closeAll();await peer.close();}
},10000);

for(const sameKey of [true,false])test(`concurrent actual SDK opening ${sameKey?'coalesces one key':'separates Session keys'} and retains distinct arguments`,async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;const held=peer.hold('tools/list');const f=component(peer);
 const a=f.client.callTool({...call,input:{nonce:'first'}}),b=f.client.callTool({...call,sessionId:sameKey?call.sessionId:'second-session',input:{nonce:'second'}});
 try{await held.entered;await until(()=>f.client.pendingWaiterCount()===2);expect(peer.counts.initialize).toBe(sameKey?1:2);expect(peer.counts.call).toBe(0);held.release();const results=await Promise.all([a,b]);expect(results.map(result=>result.structuredContent)).toEqual([{ok:true,source:'github-fixture',nonce:'first'},{ok:true,source:'github-fixture',nonce:'second'}]);expect(peer.counts.list).toBe(sameKey?1:2);await f.client.callTool({...call,input:{nonce:'reused-client-call'}});expect(peer.counts.initialize).toBe(sameKey?1:2);expect(peer.counts.call).toBe(3);}finally{held.release();await f.client.closeAll();await peer.close();}
},10000);

for(const failure of ['initialize','second-page','repeated-cursor','timeout']as const)test(`actual HTTP readiness ${failure} failure closes and permits a later independent recovery`,async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;let closures=0;
 if(failure==='initialize')peer.faults.set('initialize',[500]);
 if(failure==='second-page'){peer.faultBody='upstream diagnostic mentions 401';peer.pages=[{tools:fixtureTools(),nextCursor:'page-two'},{tools:[]}];peer.faults.set('tools/list',[0,500]);}
 if(failure==='repeated-cursor')peer.pages=[{tools:fixtureTools(),nextCursor:'same'},{tools:[],nextCursor:'same'}];
 const held=failure==='timeout'?peer.hold('tools/list'):undefined;
 const f=component(peer,{discoveryTimeoutMs:1000,createTransport:input=>{const transport=new StreamableHTTPClientTransport(peer.url,streamableHTTPTransportOptions(input));const close=transport.close.bind(transport);transport.close=async()=>{closures++;await close();};return transport;}});
 const operation=f.client.callTool(call);void operation.catch(()=>undefined);
 try{await held?.entered;if(failure==='timeout')await expect(operation).rejects.toMatchObject({code:'mcp_timeout'});else if(failure==='repeated-cursor')await expect(operation).rejects.toMatchObject({reason:'repeated_cursor'});else await expect(operation).rejects.toMatchObject({code:500});expect(peer.counts.call).toBe(0);expect(f.refreshes()).toBe(0);expect(f.client.connectionCount()).toBe(0);expect(closures).toBe(1);held?.release();peer.pages=undefined;await f.client.callTool(call);expect(peer.counts.call).toBe(1);expect(f.client.connectionCount()).toBe(1);}finally{held?.release();await f.client.closeAll();await peer.close();}
},10000);

test('real HTTP500 body mentioning401 is not authentication provenance',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;peer.faultBody='upstream diagnostic mentions 401';peer.faults.set('tools/call',[500]);const f=component(peer);
 try{await expect(f.client.callTool(call)).rejects.toMatchObject({code:500});expect(f.refreshes()).toBe(0);expect(peer.counts).toMatchObject({initialize:1,list:1,call:1,effects:0});}finally{await f.client.closeAll();await peer.close();}
},10000);


test('shared actual SDK initializer records both execution participants with default JSON logger',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;const held=peer.hold('tools/list');const lines:string[]=[];const f=component(peer,{logger:createJsonLogger({write:line=>lines.push(line)})});
 const observation=(id:string)=>()=>({claimId:'claim-'+id,toolUseEventId:'tool-'+id,elapsedMs:42,remainingMs:1000});
 const a=f.client.callTool(call,{executionObservation:observation('a')}),b=f.client.callTool(call,{executionObservation:observation('b')});
 try{await held.entered;await until(()=>f.client.pendingWaiterCount()===2);held.release();await Promise.all([a,b]);const records=lines.map(line=>JSON.parse(line));expect(records.filter(record=>record.phase==='readiness').map(record=>record['mcp.tool_use_event_id']).sort()).toEqual(['tool-a','tool-b']);for(const record of records.filter(record=>record.phase==='readiness'))expect(record).toMatchObject({'timeout.elapsed_ms':42,'timeout.remaining_ms':1000,attempt:1});expect(f.client.pendingWaiterCount()).toBe(0);}finally{held.release();await f.client.closeAll();await peer.close();}
},10000);

test('held real credential refresh exhausts caller budget without replacement SDK dispatch',async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;let refreshes=0;const entered=barrier();const material={ok:true as const,mode:'bearer' as const,token:'refresh-control',tokenHash:'refresh-control',vaultId:'v',credentialId:'c'};
 const f=component(peer,{credentialResolver:{async resolve(){return material;},async refresh(input){refreshes++;entered.resolve();await new Promise<void>((_resolve,reject)=>{if(input.signal?.aborted)reject(input.signal.reason);else input.signal?.addEventListener('abort',()=>reject(input.signal!.reason),{once:true});});return material;}}});
 await f.client.listTools(identity);peer.resetCounts();peer.faults.set('tools/call',[401]);const operation=f.client.callTool(call,{timeoutMs:1000});void operation.catch(()=>undefined);
 try{await entered.promise;await expect(operation).rejects.toMatchObject({code:'mcp_timeout'});expect([refreshes,peer.counts.initialize,peer.counts.list,peer.counts.call]).toEqual([1,0,0,1]);expect(peer.counts.effects).toBe(0);}finally{await f.client.closeAll();await peer.close();}
},10000);


for(const phase of ['initialize','tools/list'] as const)test(`unequal caller deadlines preserve a later readiness owner during held ${phase}`,async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;const held=peer.hold(phase);const f=component(peer);
 const first=f.client.callTool({...call,input:{nonce:'short-owner'}},{timeoutMs:1500});void first.catch(()=>undefined);
 let survivor:Promise<unknown>|undefined;
 try{
  await held.entered;expect(f.client.pendingWaiterCount()).toBe(1);
  survivor=f.client.callTool({...call,input:{nonce:'surviving-owner'}},{timeoutMs:5000});void survivor.catch(()=>undefined);
  await until(()=>f.client.pendingWaiterCount()===2);
  expect(peer.counts.call).toBe(0);expect(f.client.connectionCount()).toBe(0);
  await expect(first).rejects.toMatchObject({code:'mcp_timeout'});
  await until(()=>f.client.pendingWaiterCount()===1);
  held.release();
  await expect(survivor).resolves.toEqual({content:[{type:'text',text:JSON.stringify({ok:true,source:'github-fixture',nonce:'surviving-owner'})}],structuredContent:{ok:true,source:'github-fixture',nonce:'surviving-owner'},refreshTriggered:false});
  expect(f.refreshes()).toBe(0);expect(peer.counts).toMatchObject({initialize:1,list:1,call:1,effects:1});
  expect(f.client.pendingWaiterCount()).toBe(0);expect(f.client.connectionCount()).toBe(1);
 }finally{held.release();await survivor?.catch(()=>undefined);await f.client.closeAll();await peer.close();}
},10000);


for(const departure of ['cancelled','expired'] as const)test(`stale token close held until all readiness owners ${departure} cannot dispatch initialization`,async()=>{
 const peer=peerFor('github');peer.notificationsEnabled=false;const closing=barrier(),release=barrier(),retired=barrier();let token='token-0',transportStarts=0,clientsCreated=0;
 const f=component(peer,{
  createClient:()=>{
   const sdk=new DiscoverySDKClient({name:'retirement-component',version:'1'},{});
   if(++clientsCreated===2){const close=sdk.close.bind(sdk);sdk.close=async()=>{await close();retired.resolve();};}
   return sdk;
  },
  credentialResolver:{async resolve(){return {ok:true,mode:'bearer',token,tokenHash:createHash('sha256').update(token).digest('hex'),vaultId:'v',credentialId:'c'};},async refresh(){throw new Error('Unexpected refresh');}},
  createTransport:input=>{
   const transport=new StreamableHTTPClientTransport(peer.url,streamableHTTPTransportOptions(input));
   const start=transport.start.bind(transport);transport.start=async()=>{transportStarts++;await start();};
   if(input.token==='token-0'){const close=transport.close.bind(transport);transport.close=async()=>{closing.resolve();await release.promise;await close();};}
   return transport;
  },
 });
 const abort=new AbortController();let replacement:Promise<unknown>|undefined;
 try{
  await f.client.listTools(identity);token='token-1';
  replacement=f.client.callTool(call,departure==='cancelled'?{signal:abort.signal}:{timeoutMs:1000});void replacement.catch(()=>undefined);
  await closing.promise;expect(f.client.pendingWaiterCount()).toBe(1);expect(f.client.connectionCount()).toBe(0);
  if(departure==='cancelled'){abort.abort(new Error('fixture-stale-token-owner-left'));await expect(replacement).rejects.toThrow('fixture-stale-token-owner-left');}
  else await expect(replacement).rejects.toMatchObject({code:'mcp_timeout'});
  await until(()=>f.client.pendingWaiterCount()===0);
  release.resolve();
  // Join the actual replacement SDK retirement before shutdown can supply
  // any global lifetime abort; connect/list/call remain the inherited SDK.
  await retired.promise;
  expect(transportStarts).toBe(1);expect(peer.counts).toMatchObject({initialize:1,list:1,call:0,effects:0});
  expect(f.client.connectionCount()).toBe(0);expect(f.refreshes()).toBe(0);
  console.info(JSON.stringify({kind:'mcp-stale-token-retirement-observation',case:caseName,remainingOwners:f.client.pendingWaiterCount(),transportStarts,externalInitialize:peer.counts.initialize,externalCalls:peer.counts.call}));
 }finally{release.resolve();await replacement?.catch(()=>undefined);await f.client.closeAll();await peer.close();}
},10000);
