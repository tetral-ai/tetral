/** Explicit deployed-chain runner. Importing this module never invokes an environment adapter. */
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
export const DRIVER_CONTRACT = 'tetral-mcp-interoperability-v1';
export const LIVE_LIMITS = Object.freeze({durationMs:900000,cleanupReserveMs:30000,externalCalls:6,tokenRequests:2,discoveryPasses:6});
type Adapter = 'github'|'slack';
type JSONValue = null|boolean|number|string|JSONValue[]|{[key:string]:JSONValue};
export interface Counters { externalCalls:number; successfulToolResponses:number; tokenRequests:number; githubDiscoveryPasses:number; slackDiscoveryPasses:number; githubInitializations:number; slackInitializations:number }
export interface ProviderContract { revision:string; sha256:string }
export interface ProtocolFacts { transportMode:'json'|'sse'; protocolVersion:string; serverCapabilities:{[key:string]:JSONValue} }
interface ToolBinding {serverName:string;name:string;arguments:Record<string,JSONValue>;definitionHash:string;oracles:{providerPath:string[];runtimePath:string[];committedPath:string[];expected:string|number}[]}
export interface LiveFixture {
 contract:typeof DRIVER_CONTRACT;engineRevision:string;sdkRevision:string;environmentId:string;workspaceId:string;sessionId:string;vaultRefs:string[];
 github:ToolBinding;slack:ToolBinding;slackCredentialRef:string;slackUserId:string;slackTeamId:string;slackScope:'users:read';slackRefreshEndpoint:'https://slack.com/api/oauth.v2.user.access';slackConfidentialClient:true;
 providerContracts:Record<Adapter,ProviderContract>;
 driver:{entrypoint:string;argv:string[];sha256:string;revision:string;sourceSha256:string;counterProvenance:string};
}
export interface DriverRequest {contract:string;operation:'preflight'|'read'|'expire-slack'|'cleanup';runId:string;fixture:LiveFixture;adapter?:Adapter;toolUseEventId?:string;call?:{serverName:string;toolName:string;arguments:Record<string,JSONValue>};phase?:'cold'|'warm'|'refresh';remainingMs:number;remainingBudget:{externalCalls:number;tokenRequests:number;githubDiscoveryPasses:number;slackDiscoveryPasses:number}}
export interface DriverObservation {
 contract:string;operation:DriverRequest['operation'];runId:string;environmentId:string;engineRevision:string;sdkRevision:string;counterProvenance:string;counters:Counters;
 providerContracts?:Record<Adapter,ProviderContract>;protocolFacts?:Record<Adapter,ProtocolFacts>;protocol?:ProtocolFacts;
 phase?:'cold'|'warm'|'refresh';clientIdentity?:string;coldResetConfirmed?:boolean;
 catalogs?:{github:{name:string;definition:JSONValue}[];slack:{name:string;definition:JSONValue}[]};
 installedServers?:{name:string;endpoint:string}[];workspaceId?:string;sessionId?:string;vaultRefs?:string[];
 rejectedToolCalls?:{provenance:'http-status';status:401|403}[];
 toolUseEventId?:string;claimId?:string;serverName?:string;toolName?:string;arguments?:Record<string,JSONValue>;providerResult?:JSONValue;runtimeResult?:JSONValue;committedResult?:JSONValue;durableStatus?:'completed';backendPods?:string[];
 expiryDue?:boolean;refreshMaterialPreserved?:boolean;dedicatedCredential?:boolean;credentialRef?:string;tokenEndpointSucceeded?:boolean;encryptedWriteCommitted?:boolean;replacementCredentialUsed?:boolean;cleanupConfirmed?:boolean;
}
export type EnvironmentAdapter = (request:DriverRequest)=>Promise<DriverObservation>;
export interface RunEvidence {kind:'live-external-chain'|'synthetic-offline';runId:string;fixtureSha256:string;driverSha256:string;engineRevision:string;sdkRevision:string;status:'PASS'|'FAIL'|'INCONCLUSIVE';firstFailure?:string;cleanup:'confirmed'|'unconfirmed';steps:{operation:string;adapter?:Adapter;toolUseEventId?:string;counters:Counters;claimSha256?:string;clientSha256?:string;backendPodSha256?:string[];rejectedAuthStatuses?:(401|403)[];protocol?:SafeProtocolFacts}[];providerContracts?:Record<Adapter,{revisionSha256:string;sha256:string}>;protocolFacts?:Record<Adapter,SafeProtocolFacts>;driverSourceSha256:string;driverRevisionSha256:string}
export interface SafeProtocolFacts {transportMode:'json'|'sse';protocolVersion:string;capabilitiesSha256:string;tools:boolean;toolsListChanged:boolean;resources:boolean;resourcesSubscribe:boolean;resourcesListChanged:boolean;prompts:boolean;logging:boolean;completions:boolean;tasks:boolean}
const sha=(value:string|Uint8Array)=>createHash('sha256').update(value).digest('hex');
class RunnerFailure extends Error {}
class ContractFailure extends RunnerFailure {}
function requireContract(fact:unknown,message:string):asserts fact {if(!fact)throw new ContractFailure(message);}
function requireFact(fact:unknown,message:string):asserts fact {if(!fact)throw new RunnerFailure(message);}
function canonical(value:unknown):string {return JSON.stringify(value,(_key,item)=>item&&typeof item==='object'&&!Array.isArray(item)?Object.fromEntries(Object.entries(item).sort(([a],[b])=>a.localeCompare(b))):item);}
function atPath(value:JSONValue|undefined,path:string[]):unknown {let current:unknown=value;for(const key of path)current=current&&typeof current==='object'?(current as Record<string,unknown>)[key]:undefined;return current;}
/** Fixtures contain references and independent identifiers, never credential material. */
export function validateFixture(value:unknown):asserts value is LiveFixture {
 const fixture=value as LiveFixture;
 requireFact(fixture?.contract===DRIVER_CONTRACT,'Fixture contract is missing or unsupported.');
 for(const field of ['engineRevision','sdkRevision'] as const)requireFact(/^[a-f0-9]{40}$/.test(fixture[field]),'Fixture source identity is missing.');
 for(const field of ['environmentId','workspaceId','sessionId','slackCredentialRef','slackUserId','slackTeamId'] as const)requireFact(typeof fixture[field]==='string'&&fixture[field].length>0,'Fixture identity binding is missing.');
 requireFact(Array.isArray(fixture.vaultRefs)&&fixture.vaultRefs.length>0&&new Set(fixture.vaultRefs).size===fixture.vaultRefs.length,'Fixture Vault binding is missing or duplicate.');
 requireFact(fixture.github?.serverName==='work-github'&&fixture.slack?.serverName==='work-slack','Fixture configured Server identity is invalid.');
 for(const adapter of ['github','slack']as const){const pin=fixture.providerContracts?.[adapter];requireFact(typeof pin?.revision==='string'&&pin.revision.length>0&&pin.revision.length<=256&&/^[a-f0-9]{64}$/.test(pin.sha256),'Provider contract/catalog binding is missing.');}
 for(const binding of [fixture.github,fixture.slack]){
  requireFact(typeof binding.name==='string'&&binding.name.length>0&&/^[a-f0-9]{64}$/.test(binding.definitionHash),'Fixture tool definition binding is missing.');
  requireFact(binding.arguments&&typeof binding.arguments==='object'&&!Array.isArray(binding.arguments),'Fixture call JSON is missing.');
  requireFact(Array.isArray(binding.oracles)&&binding.oracles.length>=2&&binding.oracles.every(oracle=>[oracle.providerPath,oracle.runtimePath,oracle.committedPath].every(path=>Array.isArray(path)&&path.length>0&&path.every(key=>typeof key==='string'))&&(typeof oracle.expected==='string'||typeof oracle.expected==='number')),'Independent result oracle is missing.');
 }
 requireFact(fixture.github.name==='get_me'&&canonical(fixture.github.arguments)==='{}','GitHub read binding must be get_me with empty arguments.');
 requireFact(fixture.github.oracles.some(oracle=>typeof oracle.expected==='number')&&fixture.github.oracles.some(oracle=>typeof oracle.expected==='string'),'GitHub account login and numeric ID are required.');
 requireFact(fixture.slack.oracles.some(oracle=>oracle.expected===fixture.slackUserId)&&fixture.slack.oracles.some(oracle=>oracle.expected===fixture.slackTeamId),'Slack independent user/team oracle is missing.');
 requireFact(fixture.slackScope==='users:read'&&fixture.slackRefreshEndpoint==='https://slack.com/api/oauth.v2.user.access'&&fixture.slackConfidentialClient===true,'Slack refresh credential binding is incomplete.');
 requireFact(fixture.driver&&typeof fixture.driver.entrypoint==='string'&&fixture.driver.entrypoint.length>0&&Array.isArray(fixture.driver.argv)&&fixture.driver.argv.every(arg=>typeof arg==='string')&&/^[a-f0-9]{64}$/.test(fixture.driver.sha256)&&/^[a-f0-9]{64}$/.test(fixture.driver.sourceSha256)&&typeof fixture.driver.revision==='string'&&fixture.driver.revision.length>0&&typeof fixture.driver.counterProvenance==='string'&&fixture.driver.counterProvenance.length>0,'Environment adapter binding is missing.');
 const inspect=(value:unknown):void=>{if(value&&typeof value==='object')for(const [key,item]of Object.entries(value)){requireFact(!/^(authorization|access_token|refresh_token|client_secret|token|secret|password|apiKey)$/i.test(key),'Fixture contains credential material.');inspect(item);}};inspect(fixture);
}
function protocolFacts(value:ProtocolFacts|undefined):SafeProtocolFacts {
 requireFact(value&&(value.transportMode==='json'||value.transportMode==='sse')&&/^\d{4}-\d{2}-\d{2}$/.test(value.protocolVersion),'Transport observation is missing or unsupported.');
 const caps=value.serverCapabilities;requireFact(caps&&typeof caps==='object'&&!Array.isArray(caps)&&caps.tools!==null&&typeof caps.tools==='object'&&!Array.isArray(caps.tools),'Advertised tool capability observation is missing.');
 for(const group of ['resources','prompts','logging','completions','tasks'])requireFact(caps[group]===undefined||caps[group]!==null&&typeof caps[group]==='object'&&!Array.isArray(caps[group]),'Advertised capability group is malformed.');
 const flag=(group:string,key:string):boolean=>{const capability=caps[group];const raw=capability&&typeof capability==='object'&&!Array.isArray(capability)?capability[key]:undefined;requireFact(raw===undefined||typeof raw==='boolean','Advertised capability flag is malformed.');return raw===true;};
 return {transportMode:value.transportMode,protocolVersion:value.protocolVersion,capabilitiesSha256:sha(canonical(caps)),tools:true,toolsListChanged:flag('tools','listChanged'),resources:caps.resources!==undefined,resourcesSubscribe:flag('resources','subscribe'),resourcesListChanged:flag('resources','listChanged'),prompts:caps.prompts!==undefined,logging:caps.logging!==undefined,completions:caps.completions!==undefined,tasks:caps.tasks!==undefined};
}
function boundedIdentity(value:unknown):value is string {return typeof value==='string'&&value.length>0&&value.length<=256;}
/** Compare observations against independent fixture facts; driver verdicts have no authority. */
export async function runInteroperability(fixture:LiveFixture,invoke:EnvironmentAdapter,options:{synthetic?:boolean;now?:()=>number;runId?:string}={}):Promise<RunEvidence>{
 validateFixture(fixture);const now=options.now??(()=>performance.now());const started=now();const runId=options.runId??randomUUID();
 const evidence:RunEvidence={kind:options.synthetic?'synthetic-offline':'live-external-chain',runId,fixtureSha256:sha(canonical(fixture)),driverSha256:fixture.driver.sha256,engineRevision:fixture.engineRevision,sdkRevision:fixture.sdkRevision,status:'INCONCLUSIVE',cleanup:'unconfirmed',steps:[],driverSourceSha256:fixture.driver.sourceSha256,driverRevisionSha256:sha(fixture.driver.revision)};
 let counters:Counters={externalCalls:0,successfulToolResponses:0,tokenRequests:0,githubDiscoveryPasses:0,slackDiscoveryPasses:0,githubInitializations:0,slackInitializations:0};const clients=new Map<Adapter,string>();const used=new Set<string>();const claims=new Set<string>();
 const observe=async(operation:DriverRequest['operation'],adapter?:Adapter,phase?:'cold'|'warm'|'refresh'):Promise<DriverObservation>=>{
  const left=LIVE_LIMITS.durationMs-(now()-started)-(operation==='cleanup'?0:LIVE_LIMITS.cleanupReserveMs);requireFact(left>0,'Live duration bound exhausted.');const remaining=left;
  if(operation==='read')requireFact(counters.externalCalls<LIVE_LIMITS.externalCalls,'External call bound exhausted.');
  if(operation==='expire-slack')requireFact(counters.tokenRequests<LIVE_LIMITS.tokenRequests,'Token request bound exhausted.');
  const binding=adapter===undefined?undefined:fixture[adapter];const toolUseEventId=operation==='read'?randomUUID():undefined;
  const request:DriverRequest={contract:DRIVER_CONTRACT,operation,runId,fixture,remainingMs:remaining,remainingBudget:{externalCalls:LIVE_LIMITS.externalCalls-counters.externalCalls,tokenRequests:LIVE_LIMITS.tokenRequests-counters.tokenRequests,githubDiscoveryPasses:LIVE_LIMITS.discoveryPasses-counters.githubDiscoveryPasses,slackDiscoveryPasses:LIVE_LIMITS.discoveryPasses-counters.slackDiscoveryPasses},...(phase===undefined?{}:{phase}),...(adapter===undefined?{}:{adapter}),...(toolUseEventId===undefined?{}:{toolUseEventId}),...(binding===undefined?{}:{call:{serverName:binding.serverName,toolName:binding.name,arguments:binding.arguments}})};
  const observed=await invoke(request);requireFact(now()-started<=LIVE_LIMITS.durationMs,'Live duration bound exhausted.');
  requireFact(observed?.contract===DRIVER_CONTRACT&&observed.operation===operation&&observed.runId===runId,'Missing or contradictory operation observation.');
  requireFact(observed.environmentId===fixture.environmentId&&observed.engineRevision===fixture.engineRevision&&observed.sdkRevision===fixture.sdkRevision&&observed.counterProvenance===fixture.driver.counterProvenance,'Observed source/environment identity changed.');
  for(const key of Object.keys(counters)as(keyof Counters)[]){const count=observed.counters?.[key];requireFact(Number.isSafeInteger(count)&&count>=counters[key],'Missing or regressed cumulative counter.');requireFact(count<=(key==='externalCalls'?LIVE_LIMITS.externalCalls:key==='tokenRequests'?LIVE_LIMITS.tokenRequests:key.endsWith('Initializations')?Infinity:LIVE_LIMITS.discoveryPasses),'Live counter bound exhausted.');}
  if(operation==='read'){
   const attempts=observed.counters.externalCalls-counters.externalCalls;const successes=observed.counters.successfulToolResponses-counters.successfulToolResponses;
   const rejected=observed.rejectedToolCalls??[];
   requireContract(successes===1,'Successful tool response count is missing or duplicated.');
   requireContract((attempts===1||attempts===2)&&Array.isArray(rejected)&&rejected.length===attempts-1&&rejected.every(call=>call.provenance==='http-status'&&(call.status===401||call.status===403)),'Extra external attempt lacks genuine HTTP authentication rejection.');
  }
  requireFact(observed.counters.successfulToolResponses<=observed.counters.externalCalls,'Successful response counter exceeds raw attempts.');
  const projected=Object.fromEntries(Object.keys(counters).map(key=>[key,observed.counters[key as keyof Counters]]))as unknown as Counters;
  const previous={...counters};
  const step:RunEvidence['steps'][number]={operation,...(adapter===undefined?{}:{adapter}),...(toolUseEventId===undefined?{}:{toolUseEventId}),counters:projected};evidence.steps.push(step);counters=projected;
  if(operation==='read'){
   requireFact(observed.toolUseEventId===toolUseEventId&&!used.has(observed.toolUseEventId!)&&boundedIdentity(observed.claimId)&&!claims.has(observed.claimId),'Tool Use/claim observation is missing or duplicated.');used.add(observed.toolUseEventId!);claims.add(observed.claimId!);
   requireFact(observed.serverName===binding!.serverName&&observed.toolName===binding!.name&&canonical(observed.arguments)===canonical(binding!.arguments),'Observed invocation differs from complete bound call JSON.');
   requireContract(observed.durableStatus==='completed'&&observed.runtimeResult!==undefined&&canonical(observed.runtimeResult)===canonical(observed.committedResult),'Runtime result differs from successful durable settlement.');
   requireFact(Array.isArray(observed.backendPods)&&observed.backendPods.length>0&&observed.backendPods.length<=16&&observed.backendPods.every(boundedIdentity)&&new Set(observed.backendPods).size===observed.backendPods.length,'Backend identity evidence is missing.');
   requireFact(observed.phase===phase&&boundedIdentity(observed.clientIdentity),'SDK phase/client identity observation is missing.');
   if(observed.rejectedToolCalls?.length)step.rejectedAuthStatuses=observed.rejectedToolCalls.map(call=>call.status);
   step.claimSha256=sha(runId+':claim:'+observed.claimId);step.clientSha256=sha(runId+':client:'+observed.clientIdentity);step.backendPodSha256=observed.backendPods!.map(pod=>sha(runId+':backend:'+pod));step.protocol=protocolFacts(observed.protocol);
   const initKey=adapter==='github'?'githubInitializations':'slackInitializations';const listKey=adapter==='github'?'githubDiscoveryPasses':'slackDiscoveryPasses';
   if(phase==='cold'){requireContract(counters[initKey]===previous[initKey]+1&&counters[listKey]>=previous[listKey]+1,'Cold read did not establish SDK readiness.');clients.set(adapter!,observed.clientIdentity);}
   if(phase==='warm')requireContract(observed.clientIdentity===clients.get(adapter!)&&counters[initKey]===previous[initKey]&&counters[listKey]===previous[listKey],'Warm read did not reuse the ready SDK client.');
   for(const oracle of binding!.oracles)requireContract(atPath(observed.providerResult,oracle.providerPath)===oracle.expected&&atPath(observed.runtimeResult,oracle.runtimePath)===oracle.expected&&atPath(observed.committedResult,oracle.committedPath)===oracle.expected,'Independent account identity oracle mismatch.');
  }
  return observed;
 };
 try{
  const preflight=await observe('preflight');requireFact(preflight.coldResetConfirmed===true,'Cold client reset/readback is missing.');requireFact(preflight.workspaceId===fixture.workspaceId&&preflight.sessionId===fixture.sessionId&&canonical(preflight.vaultRefs)===canonical(fixture.vaultRefs),'Observed Session/Vault scope differs from fixture.');
  requireFact(canonical(preflight.installedServers)==canonical([{name:'work-github',endpoint:'https://api.githubcopilot.com/mcp/'},{name:'work-slack',endpoint:'https://mcp.slack.com/mcp'}]),'Installed Server binding differs from fixture.');
  for(const adapter of ['github','slack']as const){requireFact(canonical(preflight.providerContracts?.[adapter])===canonical(fixture.providerContracts[adapter]),'Retrieved provider contract/catalog identity differs from fixture.');const binding=fixture[adapter];const definitions=preflight.catalogs?.[adapter]?.filter(tool=>tool.name===binding.name);requireFact(definitions?.length===1&&sha(canonical(definitions[0]!.definition))===binding.definitionHash,'Discovered tool definition is missing, duplicate or changed.');}
  evidence.providerContracts={github:{revisionSha256:sha(fixture.providerContracts.github.revision),sha256:fixture.providerContracts.github.sha256},slack:{revisionSha256:sha(fixture.providerContracts.slack.revision),sha256:fixture.providerContracts.slack.sha256}};
  evidence.protocolFacts={github:protocolFacts(preflight.protocolFacts?.github),slack:protocolFacts(preflight.protocolFacts?.slack)};
  for(const adapter of ['github','slack']as const){await observe('read',adapter,'cold');await observe('read',adapter,'warm');}
  const beforeRefresh=counters.tokenRequests;const expiry=await observe('expire-slack');requireFact(expiry.expiryDue&&expiry.refreshMaterialPreserved&&expiry.dedicatedCredential&&expiry.credentialRef===fixture.slackCredentialRef,'Dedicated expiry lever observation is incomplete.');
  const refreshed=await observe('read','slack','refresh');requireFact(counters.tokenRequests>beforeRefresh&&refreshed.tokenEndpointSucceeded&&refreshed.encryptedWriteCommitted&&refreshed.replacementCredentialUsed,'Actual Slack refresh/write-back/replacement observation is missing.');
  evidence.status='PASS';
 }catch(error){evidence.status=error instanceof ContractFailure?'FAIL':'INCONCLUSIVE';evidence.firstFailure=error instanceof RunnerFailure?error.message:'Environment adapter observation failed.';}
 finally{try{const cleanup=await observe('cleanup');if(cleanup.cleanupConfirmed)evidence.cleanup='confirmed';else throw new Error('Cleanup readback is missing.');}catch{if(evidence.firstFailure===undefined)evidence.firstFailure='Cleanup readback is missing.';if(evidence.status!=='FAIL')evidence.status='INCONCLUSIVE';}}
 return evidence;
}

/** Each invocation receives structured stdin; stdout/stderr are never persisted verbatim. */
async function readBounded(stream:ReadableStream<Uint8Array>,limit:number,retain:boolean):Promise<string>{
 const reader=stream.getReader();let size=0;const chunks:Uint8Array[]=[];
 try{for(;;){const chunk=await reader.read();if(chunk.done)break;size+=chunk.value.byteLength;if(size>limit){await reader.cancel();throw new RunnerFailure('Environment adapter output is oversized.');}if(retain)chunks.push(chunk.value);}return retain?Buffer.concat(chunks).toString('utf8'):'';}finally{reader.releaseLock();}
}
export function commandAdapter(fixture:LiveFixture):EnvironmentAdapter{return async request=>{
 const child=Bun.spawn({cmd:[fixture.driver.entrypoint,...fixture.driver.argv],stdin:'pipe',stdout:'pipe',stderr:'pipe'});child.stdin.write(JSON.stringify(request));child.stdin.end();
 const terminate=setTimeout(()=>child.kill('SIGTERM'),Math.max(1,request.remainingMs-500));const kill=setTimeout(()=>child.kill('SIGKILL'),Math.max(1,request.remainingMs-100));
 const stdout=readBounded(child.stdout,1024*1024,true),stderr=readBounded(child.stderr,64*1024,false);
 try{const [code,output]=await Promise.all([child.exited,stdout,stderr]);requireFact(code===0,'Environment adapter did not complete.');try{return JSON.parse(output)as DriverObservation;}catch{throw new RunnerFailure('Environment adapter observation is malformed.');}}
 catch(error){child.kill('SIGKILL');throw error;}
 finally{await Promise.allSettled([child.exited,stdout,stderr]);clearTimeout(terminate);clearTimeout(kill);}
};}

if(import.meta.main){
 try{
  const args=process.argv.slice(2);const fixturePath=args[args.indexOf('--fixture')+1];const output=args[args.indexOf('--output')+1];
  requireFact(args.includes('--fixture')&&args.includes('--output')&&fixturePath&&output,'A bound fixture and output directory are required.');
  const bytes=await readFile(fixturePath);const fixture:unknown=JSON.parse(bytes.toString('utf8'));validateFixture(fixture);
  requireFact(sha(await readFile(fixture.driver.entrypoint))===fixture.driver.sha256,'Environment adapter source hash differs from fixture.');
  if(args.includes('--offline-preflight')){process.stdout.write(JSON.stringify({kind:'offline-preflight',fixtureSha256:sha(bytes),contract:DRIVER_CONTRACT})+'\n');}
  else{const evidence=await runInteroperability(fixture,commandAdapter(fixture));await mkdir(output,{recursive:true});await writeFile(resolve(output,'mcp-interoperability.json'),JSON.stringify(evidence,null,2)+'\n',{flag:'wx',mode:0o600});process.exitCode=evidence.status==='PASS'?0:2;}
 }catch{process.stderr.write('MCP interoperability preflight failed; no acceptance result.\n');process.exitCode=2;}
}
