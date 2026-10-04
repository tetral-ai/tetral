/** Actual app, gRPC and pinned SDK fixture. Named provider SSE scripts are test-owned. */
import { createInterface } from "node:readline";
import { AsyncLocalStorage } from "node:async_hooks";
import type { FetchFunction } from "@ai-sdk/provider-utils";
import { createHmac, createHash } from "node:crypto";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { publicStreamingScript, type PublicStreamingScenario } from "../../../../../../integration/testdata/public-streaming-provider.js";
import { createNatsPreviewPublisher } from "../../src/providers/preview-nats.js";
import type { PreviewNatsConfig } from "../../src/providers/preview-config.js";
import { createPublicStreamingPublisherControls, type PublicPublisherControl } from "../../../../../../integration/testdata/public-streaming-publisher.js";
import { BridgeAPIAttachmentResolver } from "../../src/attachments.js";
import { Metadata, Server, status } from "@grpc/grpc-js";
import { createProviderGatewayApp } from "../../src/app.js";
import { createProviderClientRegistry } from "../../src/providers/clients.js";
import { ProviderCredentialResolver, CachedPlatformCredentialPool, SQLGatewayCredentialStore } from "../../src/providers/credentials.js";
import { openPostgresSQLOwner } from "@tetral/ts-dbconnect";
import { verifyPostgreSQLReadiness } from "../../../schema/src/verify.js";
import { encryptAES256GCM } from "../../src/providers/crypto.js";
import type { ProviderAssemblyBounds, ProviderAssemblyResources } from "../../src/providers/block-assembler.js";
import { DefaultProviderAssemblyBounds } from "../../src/providers/resource-policy.js";
import type { ProviderModelStreamResources } from "../../src/providers/model-stream.js";
import type { GatewayCredentialStore } from "../../src/providers/credentials.js";
import type { ProviderGatewayConfig } from "../../src/config.js";
import type { ProviderRequestStreamer } from "../../src/service.js";
import { ProviderRequest as ProviderRequestMessage, ProviderStreamEvent as ProviderStreamEventMessage, ProviderRequestKind, ProviderThreadRole, ProviderThreadVisibility, ProviderContextRole, SystemSegmentKind, SystemCacheHint } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";

export const ContentLifecycleBindingKey="content-lifecycle-test-binding-key-32";
export const ContentLifecyclePodUid="pod_content_lifecycle";
const MasterKey="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
export type ContentLifecycleScenario=PublicStreamingScenario|"interleaved"|"text"|"text-large"|"truncated"|"error-after-partial"|"empty-text"|"empty-then-valid"|"metadata-only"|"done"|"error-after-complete-prefix"|"multiple-large"|"empty-storm"|"durable-interleaved"|"empty-blocks-storm"|"durable-write"|"durable-bash"|"durable-write-queued"|"durable-read-three"|"tool-whitespace"|"reasoning-only"|"tool-before-text"|"unicode-text"|"compaction-primer"|"compaction-summary"|"resource-after-tool"|"resource-after-write";
export interface ContentLifecycleGatewayFixtureOptions {
 /** Full-topology cases use the installed Gateway role and real credential store. */
 readonly databaseUrl?:string;
 /** Resource cycles arm fixed scripts by actual Session identity over fixture IPC. */
 readonly sessionScenarioPlans?:boolean;
 readonly memorySampleIntervalMs?:number;
 readonly scenario?:ContentLifecycleScenario;
 readonly followupScenario?:ContentLifecycleScenario;
 readonly bridgeAddress?:string;
 readonly bridgeToken?:string;
 readonly blockCount?:number;
 readonly emptyRecords?:number;
 readonly textCodeUnits?:number;
 readonly textCodeUnitsCycle?:readonly number[];
 readonly fragmentCodeUnits?:number;
 readonly deltaDelayMs?:number;
 readonly sourceYieldEveryRecords?:number;
 /** Enqueue this many consecutive SSE records per source chunk; each record still reaches the SDK as its own event. */
 readonly sourceRecordsPerChunk?:number;
 readonly sourcePackaging?:"single-chunk";
 readonly assemblyBounds?:ProviderAssemblyBounds;
 readonly bindingKey?:string;
 readonly runtimePodUid?:string;
 readonly measureResources?:boolean;
 /** Hold the first application write callback after the native transport completed. */
 readonly holdFirstWriteCallback?:boolean;
 readonly recordContext?:boolean;
 readonly concurrentProviderBarrier?:{readonly count:number;readonly retainedBytesPerRequest:number};
 readonly previewNats?:PreviewNatsConfig;
 readonly previewPublisherControls?:{readonly holdInitialConnect?:boolean;readonly holdAfterNativeConnect?:boolean};
 readonly holdPreviewFragments?:boolean;
 readonly holdEveryRequest?:boolean;
 readonly holdFinish?:boolean;
 readonly holdAfterFirstComplete?:boolean;
}
export function contentLifecycleRequest(overrides:Partial<ProviderRequest>={},identity:{readonly bindingKey?:string;readonly runtimePodUid?:string}={}):ProviderRequest {
 const request=ProviderRequestMessage.fromPartial({requestId:"req_content_lifecycle",modelRequestId:"mreq_content_lifecycle",workspaceId:"wksp_content_lifecycle",sessionId:"sesn_content_lifecycle",sessionThreadId:"thrd_content_lifecycle",bindingId:"bind_content_lifecycle",bindingGeneration:1,runtimeProcessId:"runtime_content_lifecycle",requestKind:ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,outputContractVersion:2,modelRequestStartEventId:"evt_1000000000000000",threadRole:ProviderThreadRole.PROVIDER_THREAD_ROLE_MAIN,threadVisibility:ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_PUBLIC,model:{providerId:"anthropic",modelId:"claude-opus-4-8",variant:""},system:[{kind:SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,cacheHint:SystemCacheHint.SYSTEM_CACHE_HINT_NONE,text:"Fixture only."}],context:[{role:ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,content:[{text:{text:"fixture"}}]}],tools:[{name:"Read",description:"Fixture read",function:{inputSchemaJson:'{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}'}}],limits:{maxOutputTokens:0,timeoutMs:30000},...overrides});
 const payload=Buffer.from(JSON.stringify({v:1,workspace_id:request.workspaceId,session_id:request.sessionId,session_thread_id:request.sessionThreadId,binding_id:request.bindingId,binding_generation:request.bindingGeneration,runtime_pod_uid:identity.runtimePodUid??ContentLifecyclePodUid,runtime_process_id:request.runtimeProcessId,exp:Math.floor(Date.now()/1000)+3600})).toString("base64url");
 return {...request,runtimeBindingToken:`rtbt_v1.${payload}.${createHmac("sha256",identity.bindingKey??ContentLifecycleBindingKey).update(payload).digest("base64url")}`};
}
export function contentLifecycleMetadata():Metadata {const metadata=new Metadata();metadata.set("authorization","bearer fixture-runtime-token");return metadata;}
export async function createContentLifecycleGatewayFixture(options:ContentLifecycleGatewayFixtureOptions={}) {
 if((options.bridgeAddress===undefined)!==(options.bridgeToken===undefined)||options.bridgeToken==="")throw new Error("fixture Bridge address/token required together");
 if(options.holdFirstWriteCallback&&!options.measureResources)throw new Error("held write observation requires resource observation");
 const scenario=options.scenario??"interleaved", fragmentCodeUnits=options.fragmentCodeUnits??8192, textCodeUnits=options.textCodeUnits??8*1024*1024;
 if(!Number.isSafeInteger(fragmentCodeUnits)||fragmentCodeUnits<1||fragmentCodeUnits>65536||!Number.isSafeInteger(textCodeUnits)||textCodeUnits<0||textCodeUnits>16*1024*1024)throw new Error("invalid fixture controls");
 if(options.textCodeUnitsCycle!==undefined&&(options.textCodeUnitsCycle.length!==2||options.textCodeUnitsCycle.some(value=>!Number.isSafeInteger(value)||value<1||value>16*1024*1024-2)))throw new Error("invalid fixture size cycle");
 if(options.sourceYieldEveryRecords!==undefined&&(!Number.isSafeInteger(options.sourceYieldEveryRecords)||options.sourceYieldEveryRecords<1||options.sourceYieldEveryRecords>8192))throw new Error("invalid fixture source batching");
 // Chunking skips the per-record gates, so it excludes every option that holds or paces individual records.
 if(options.sourceRecordsPerChunk!==undefined&&(!Number.isSafeInteger(options.sourceRecordsPerChunk)||options.sourceRecordsPerChunk<1||options.sourceRecordsPerChunk>8192||options.holdFinish||options.holdAfterFirstComplete||options.concurrentProviderBarrier!==undefined||options.deltaDelayMs!==undefined||options.sourceYieldEveryRecords!==undefined||options.sourcePackaging!==undefined||options.holdPreviewFragments))throw new Error("invalid fixture source chunking");
 if(options.sourcePackaging!==undefined&&(options.sourcePackaging!=="single-chunk"||options.holdFinish||options.holdAfterFirstComplete||options.concurrentProviderBarrier!==undefined||options.textCodeUnitsCycle!==undefined))throw new Error("invalid fixture source packaging");
 const preparedSource=options.sourcePackaging==="single-chunk"?(()=>{const records=Array.from(sseScript(scenario,textCodeUnits,fragmentCodeUnits,options.blockCount??3,options.emptyRecords??10000));return {bytes:new TextEncoder().encode(records.join("")),recordCount:records.length};})():undefined;
 let fragmentPermits=0,fragmentWaiters:Array<()=>void>=[],fragmentWaiting=0,fragmentReleased=0,fragmentClosing=false;
 const releaseFragment=():void=>{fragmentReleased++;const waiter=fragmentWaiters.shift();if(waiter)waiter();else fragmentPermits++;};
 const awaitFragment=async():Promise<void>=>{if(!options.holdPreviewFragments||fragmentClosing)return;if(fragmentPermits>0){fragmentPermits--;return;}fragmentWaiting++;try{await new Promise<void>(resolve=>fragmentWaiters.push(resolve));}finally{fragmentWaiting--;}};
 const closeFragments=():void=>{fragmentClosing=true;for(const waiter of fragmentWaiters.splice(0))waiter();};
 let finishGate=gate(options.holdFinish??false);const prefixGate=gate(options.holdAfterFirstComplete??false);
 const barrierOptions=options.concurrentProviderBarrier;
 if(barrierOptions!==undefined&&(!Number.isSafeInteger(barrierOptions.count)||barrierOptions.count<1||barrierOptions.count>8||!Number.isSafeInteger(barrierOptions.retainedBytesPerRequest)||barrierOptions.retainedBytesPerRequest<1||barrierOptions.retainedBytesPerRequest>=textCodeUnits||scenario!=="text-large"))throw new Error("invalid provider barrier controls");
 const providerBarrierGate=gate(barrierOptions!==undefined),providerBarrier={sourceArrivals:0,assemblyArrivals:0,ready:false,released:barrierOptions===undefined};
 const releaseProviderBarrier=():void=>{if(!providerBarrier.ready)throw new Error("provider barrier not ready");providerBarrier.released=true;providerBarrierGate.release();};
 const nativeAttachments:Array<{requestOrdinal:number;kind:string;mediaType:string;sizeBytes:number;sha256:string}>= [];
 const nativeContexts:Array<{requestOrdinal:number;messages:unknown[]}>= [];
 const previewOutcomes:Array<{event:string;outcome:string}>=[];
 const capturePreview=(record:Record<string,unknown>):void=>{const kind=record["event.kind"],outcome=record["transport.outcome"];if(typeof kind==="string"&&kind.startsWith("preview.")&&typeof outcome==="string"&&previewOutcomes.length<64)previewOutcomes.push({event:kind,outcome});};
 const admissionFailures:Array<{requestId?:string;validationMember?:string;errorClass?:string;errorCode?:string}> = [];
 const captureAdmission=(record:Record<string,unknown>):void=>{if(record.event!=="provider_request_streamed"||record["request.outcome"]!=="failed"||admissionFailures.length>=16)return;admissionFailures.push({...(typeof record["request.id"]==="string"?{requestId:record["request.id"]}:{}),...(typeof record["validation.member"]==="string"?{validationMember:record["validation.member"]}:{}),...(typeof record["error.class"]==="string"?{errorClass:record["error.class"]}:{}),...(typeof record["error.code"]==="string"?{errorCode:record["error.code"]}:{})});};
 let providerCalls=0,sourceRecords=0,activeProviderSources=0,sourceCancellations=0,joinedSources=0;
 let sdk:ProviderModelStreamResources|undefined,assembly:ProviderAssemblyResources|undefined;
 const sdkSnapshots=new Map<number,ProviderModelStreamResources>(),assemblySnapshots=new Map<string,ProviderAssemblyResources>();
 if(options.measureResources)Bun.gc(true);
 const baselineMemory=process.memoryUsage();
 const baselineCpu=process.cpuUsage(),measurementStartedAt=performance.now();
 let lastSampleAt=measurementStartedAt,loopLagMaxMs=0,loopLagSumMs=0,loopLagSamples=0;
 let finalMemory:ReturnType<typeof process.memoryUsage>|undefined;
 let latestMemory=baselineMemory,lastMemorySampleAt=measurementStartedAt;
 let peakHeap=baselineMemory.heapUsed,peakRss=baselineMemory.rss,peakActiveProviderSources=0;
 const peakAssembly={retainedBytes:0,cumulativeContentBytes:0,segments:0,openBlocks:0,identities:0};
 const sample=():void=>{if(!options.measureResources)return;const now=performance.now();if(now-lastMemorySampleAt<(options.memorySampleIntervalMs??0))return;lastMemorySampleAt=now;const memory=process.memoryUsage();latestMemory=memory;peakHeap=Math.max(peakHeap,memory.heapUsed);peakRss=Math.max(peakRss,memory.rss);};
 const sampler=options.measureResources?setInterval(()=>{const now=performance.now(),lag=Math.max(0,now-lastSampleAt-5);lastSampleAt=now;loopLagMaxMs=Math.max(loopLagMaxMs,lag);loopLagSumMs+=lag;loopLagSamples++;sample();},5):undefined;
 const writerCalls:Record<string,{requestId:string;startedMs:number;events:Array<{event:string;atMs:number;code?:string;message?:string;rstCode?:number}>}>= {};
 const writer={heldApplicationCallbacks:0,writeCalls:0,callbacks:0,drains:0,pendingCallbacks:0,pendingBytes:0,peakPendingBytes:0,peakPendingCallbacks:0,errors:0,cancellations:0,closed:0};
 const scenariosBySession=new Map<string,ContentLifecycleScenario[]>();
 const requestScenario=new AsyncLocalStorage<{sessionId:string;requestId:string;scenario:ContentLifecycleScenario}>();
 const configureScenarios=(sessionId:string,scenarios:readonly ContentLifecycleScenario[]):void=>{
  const allowed=new Set<ContentLifecycleScenario>(["durable-interleaved","durable-write","done","text","compaction-primer","compaction-summary","public-cycle","public-load"]);
  if(!options.sessionScenarioPlans||sessionId.length===0||scenarios.length===0||scenarios.length>3||scenarios.some(value=>!allowed.has(value))||scenariosBySession.has(sessionId))throw new Error("fixture scenario plan invalid");
  scenariosBySession.set(sessionId,[...scenarios]);
 };
 const activeCalls=new Set<{emit(event:"error",error:Error):void}>();
 const cutProviderRpc=():void=>{for(const call of activeCalls)call.emit("error",Object.assign(new Error("fixture provider RPC interrupted"),{code:status.UNAVAILABLE,details:"fixture provider RPC interrupted"}));};
 const fetchImpl=async (_url:unknown,init?:RequestInit):Promise<Response>=>{
  providerCalls++;
  if(options.holdEveryRequest&&providerCalls>1){if(activeProviderSources!==0)throw new Error("per-request fixture gates require sequential provider requests");finishGate=gate(options.holdFinish??false);fragmentClosing=false;fragmentPermits=0;}
  const nativeRequest=_url instanceof Request?_url:new Request(_url as string,init);
  const nativeBody=await nativeRequest.clone().text();
  if(nativeBody!=="") {
   const payload=JSON.parse(nativeBody) as {messages?:Array<{role?:string;content?:unknown}>};
   if(options.recordContext){
    if(nativeContexts.length>=16||(payload.messages?.length??0)>32)throw new Error("fixture native context count exceeded");
    for(const message of payload.messages??[])if(Array.isArray(message.content)&&message.content.length>64)throw new Error("fixture native content parts exceeded");
    const messages=(payload.messages??[]).map(message=>({role:message.role,content:typeof message.content==="string"?message.content:(Array.isArray(message.content)?message.content.map(part=>semanticNativePart(part)):[])}));
    if(new TextEncoder().encode(JSON.stringify(messages)).byteLength>65536)throw new Error("fixture native context bytes exceeded");nativeContexts.push({requestOrdinal:providerCalls,messages});
   }
   for(const message of payload.messages??[])for(const item of Array.isArray(message.content)?message.content:[]) {
    if(item?.type!=="image"&&item?.type!=="document")continue;
    const source=item.source;if(source?.type!=="base64"&&source?.type!=="text")continue;
    const bytes=source.type==="base64"?Buffer.from(source.data,"base64"):Buffer.from(source.data,"utf8");
    nativeAttachments.push({requestOrdinal:providerCalls,kind:item.type,mediaType:source.media_type??"",sizeBytes:bytes.byteLength,sha256:createHash("sha256").update(bytes).digest("hex")});
   }
  }
  activeProviderSources++;peakActiveProviderSources=Math.max(peakActiveProviderSources,activeProviderSources);
  const selectedScenario=requestScenario.getStore()?.scenario??(providerCalls>1&&options.followupScenario!==undefined?options.followupScenario:scenario);
  const script=sseScript(selectedScenario,options.textCodeUnitsCycle?.[(providerCalls-1)%2]??textCodeUnits,fragmentCodeUnits,options.blockCount??3,options.emptyRecords??10000,options.sessionScenarioPlans?requestScenario.getStore()?.requestId:undefined);
  let closed=false,sourceTextCodeUnits=0,barrierArrived=false,localSourceRecords=0,preparedRead=false;
  const close=():void=>{if(closed)return;closed=true;activeProviderSources--;joinedSources++;nativeRequest.signal?.removeEventListener("abort",abort);finishGate.release();prefixGate.release();closeFragments();};
  let bodyController:ReadableStreamDefaultController<Uint8Array>|undefined;
  const abort=():void=>{sourceCancellations++;bodyController?.error(new DOMException("Fixture provider aborted.","AbortError"));close();};
  const body=new ReadableStream<Uint8Array>({
   start(controller){bodyController=controller;nativeRequest.signal?.addEventListener("abort",abort,{once:true});if(nativeRequest.signal?.aborted)abort();},
   async pull(controller){if(closed)return;try{
    if(preparedSource!==undefined){if(preparedRead){controller.close();close();return;}preparedRead=true;sourceRecords+=preparedSource.recordCount;controller.enqueue(preparedSource.bytes);return;}
    if(barrierOptions!==undefined&&!barrierArrived&&sourceTextCodeUnits>=barrierOptions.retainedBytesPerRequest){barrierArrived=true;providerBarrier.sourceArrivals++;providerBarrier.ready=providerBarrier.sourceArrivals>=barrierOptions.count&&providerBarrier.assemblyArrivals>=barrierOptions.count;await providerBarrierGate.wait;if(closed)return;}
    let next=script.next();if(next.done){controller.close();close();return;}if((options.deltaDelayMs??0)>0)await new Promise(resolve=>setTimeout(resolve,options.deltaDelayMs));if(closed)return;if(next.value.startsWith("event: message_delta")||next.value.startsWith("event: fixture_resource_gate"))await finishGate.wait;
    if(next.value.startsWith("event: fixture_resource_gate")){next=script.next();if(next.done){controller.close();close();return;}}
     if(next.value.includes('"index":2')&&next.value.startsWith("event: content_block_start"))await prefixGate.wait;
     if(closed)return;
     if(next.value.startsWith("event: content_block_delta")&&next.value.includes('"type":"text_delta"'))await awaitFragment();
     if(closed)return;
     if(next.value.startsWith("event: content_block_delta")){const data=JSON.parse(next.value.split("\n")[1]!.slice(6));if(data.delta?.type==="text_delta")sourceTextCodeUnits+=data.delta.text.length;}
     if(options.sourceYieldEveryRecords!==undefined&&localSourceRecords>0&&localSourceRecords%options.sourceYieldEveryRecords===0)await new Promise(resolve=>setTimeout(resolve,0));
     if(closed)return;localSourceRecords++;sourceRecords++;let chunk=next.value;
     for(let chunked=1;chunked<(options.sourceRecordsPerChunk??1);chunked++){const more=script.next();if(more.done)break;chunk+=more.value;localSourceRecords++;sourceRecords++;}
     controller.enqueue(new TextEncoder().encode(chunk));}catch(error){controller.error(error);close();}},
   cancel(){sourceCancellations++;close();},
  });
  return new Response(body,{headers:{"content-type":"text/event-stream"}});
 };
 const encryptedAuth=await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({type:"provider_api_key",provider_id:"anthropic",access_mode:"user_api_key",token:"fixture-key"})),MasterKey);
 const encryptedPlatformKey=await encryptAES256GCM(new TextEncoder().encode("fixture-platform-key"),MasterKey);
 let sessionCredentialReads=0,platformSelections=0;
 const sqlOwner=options.databaseUrl===undefined?undefined:await openPostgresSQLOwner({url:options.databaseUrl,pool:fixtureConfig().databasePool,verify:verifyPostgreSQLReadiness});
 let credentialStoreClosed=false;
 const backingStore:GatewayCredentialStore=sqlOwner===undefined?{loadActiveSessionProviderAuth:async()=>[{providerId:"anthropic",vaultId:"vlt_fixture",credentialId:"cred_fixture",accessMode:"user_api_key",credentialAuthType:"provider_api_key",credentialProviderId:"anthropic",credentialAccessMode:"user_api_key",encryptedAuth,archived:false,revoked:false}],loadPlatformProviderKeyRows:async()=>[{keyId:"pfk_fixture",providerId:"anthropic",encryptedKey:encryptedPlatformKey,weight:1,priority:0,cacheScope:"fixture",status:"active",updatedAt:"2026-10-03T00:00:00Z"}]}:new SQLGatewayCredentialStore(sqlOwner);
 const store:GatewayCredentialStore={loadActiveSessionProviderAuth:async input=>{sessionCredentialReads++;return await backingStore.loadActiveSessionProviderAuth(input);},loadPlatformProviderKeyRows:async()=>await backingStore.loadPlatformProviderKeyRows()};
 const realPool=new CachedPlatformCredentialPool({store,masterKeyHex:MasterKey,poolOptions:{random:()=>0}});
 const credentialResolver=new ProviderCredentialResolver({masterKeyHex:MasterKey,store,platformPool:{select:async(...args)=>{platformSelections++;return realPool.select(...args);},recordFailure:(...args)=>realPool.recordFailure(...args)}});
 const nativeProviderStreamer=createProviderClientRegistry({fetch:fetchImpl as FetchFunction, observeModelStream:resources=>{sdk=resources;sdkSnapshots.set(resources.operationId,resources);if(resources.sourceRecords%1024===0||!resources.active)sample();}});
 const nativeIterators={started:0,active:0,joined:0,cancelledJoins:0,cancelToJoinMs:[] as number[]};
 const providerStreamer:ProviderRequestStreamer={joinIteratorReturn:true,async *stream(input){
  nativeIterators.started++;nativeIterators.active++;let abortedAt:number|undefined;
  const onAbort=():void=>{abortedAt??=performance.now();};
  input.abortSignal?.addEventListener("abort",onAbort,{once:true});if(input.abortSignal?.aborted)onAbort();
  try{
   if(!options.sessionScenarioPlans){yield* nativeProviderStreamer.stream(input);}
   else {
    const sessionId=input.request.sessionId,plan=scenariosBySession.get(sessionId),selected=plan?.shift();
    if(selected===undefined)throw new Error("fixture has no script for actual Session request");
    if((selected==="compaction-summary")!==(input.request.requestKind===ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY))throw new Error("fixture compaction script/request kind mismatch");
    if(plan!.length===0)scenariosBySession.delete(sessionId);
    const scope={sessionId,requestId:input.request.requestId,scenario:selected};
    const iterator=nativeProviderStreamer.stream(input)[Symbol.asyncIterator]();
    try{for(;;){const next=await requestScenario.run(scope,()=>iterator.next());if(next.done)break;yield next.value;}}
    finally{await requestScenario.run(scope,async()=>{await iterator.return?.(undefined);});}
   }
  }finally{
   // Delegated native iterator finally has completed before this join observation.
   input.abortSignal?.removeEventListener("abort",onAbort);nativeIterators.active--;nativeIterators.joined++;
   if(abortedAt!==undefined){nativeIterators.cancelledJoins++;nativeIterators.cancelToJoinMs.push(performance.now()-abortedAt);}
  }
 }};
 const originalAddService=Server.prototype.addService;
 {
  Server.prototype.addService=function(definition,implementation){
   const handler=implementation.streamProviderRequest;
   if(typeof handler==="function")implementation={...implementation,streamProviderRequest:(call:any)=>{
    activeCalls.add(call);call.on("close",()=>activeCalls.delete(call));
    const callId=String(Object.keys(writerCalls).length+1);
    const trace={requestId:String(call.request.requestId),startedMs:performance.now()-measurementStartedAt,events:[] as Array<{event:string;atMs:number;code?:string;message?:string;rstCode?:number}>};writerCalls[callId]=trace;
    const record=(event:string,failure?:unknown):void=>{const error=failure as {code?:unknown;message?:unknown}|undefined;trace.events.push({event,atMs:performance.now()-measurementStartedAt,...(error?.code===undefined?{}:{code:String(error.code)}),...(typeof error?.message==="string"?{message:error.message.slice(0,256)}:{}),...(typeof call.call?.stream?.rstCode==="number"?{rstCode:call.call.stream.rstCode}:{})});};
    const originalWrite=call.write.bind(call),pending=new Map<object,number>();
    let heldCallback:((error?:unknown)=>void)|undefined;let observedWrites=0;
    const release=(token:object):void=>{const bytes=pending.get(token);if(bytes===undefined)return;pending.delete(token);writer.pendingCallbacks--;writer.pendingBytes-=bytes;};
    call.write=(message:any,callback:any)=>{
     record("write");observedWrites++;const hold=options.holdFirstWriteCallback&&observedWrites===1;
     const bytes=ProviderStreamEventMessage.encode(message).finish().byteLength,token={};pending.set(token,bytes);writer.writeCalls++;writer.pendingCallbacks++;writer.pendingBytes+=bytes;writer.peakPendingCallbacks=Math.max(writer.peakPendingCallbacks,writer.pendingCallbacks);writer.peakPendingBytes=Math.max(writer.peakPendingBytes,writer.pendingBytes);sample();
     return originalWrite(message,(error:any)=>{writer.callbacks++;record("callback",error);release(token);if(hold){heldCallback=callback;writer.heldApplicationCallbacks++;record("application-callback-held");}else callback(error);});
    };
    call.on("drain",()=>{writer.drains++;record("drain");});
    const clear=():void=>{for(const token of pending.keys())release(token);};
    call.on("cancelled",()=>{writer.cancellations++;record("cancelled");});call.on("error",(error:any)=>{writer.errors++;record("error",error);});call.on("close",()=>{writer.closed++;record("close");clear();if(heldCallback!==undefined){heldCallback=undefined;writer.heldApplicationCallbacks--;}});
    (handler as (call:any)=>void)(call);
   }};
   return originalAddService.call(this,definition,implementation);
  };
 }
 let tokenDirectory:string|undefined,attachmentResolver:BridgeAPIAttachmentResolver|undefined;
 if(options.bridgeAddress!==undefined) {
  if(options.bridgeToken===undefined||options.bridgeToken.length===0)throw new Error("fixture Bridge token required");
  tokenDirectory=await mkdtemp(join(tmpdir(),"tetral-content-gateway-"));const tokenPath=join(tokenDirectory,"bridge-token");
  await writeFile(tokenPath,options.bridgeToken,{mode:0o600});attachmentResolver=new BridgeAPIAttachmentResolver({address:options.bridgeAddress,tokenPath});
 }
 if(options.previewPublisherControls!==undefined&&options.previewNats===undefined)throw new Error("publisher controls require actual NATS configuration");
 const publisherControls=options.previewPublisherControls===undefined?undefined:createPublicStreamingPublisherControls(options.previewPublisherControls.holdInitialConnect,options.previewPublisherControls.holdAfterNativeConnect);
 const previewPublisher=options.previewNats===undefined?undefined:await createNatsPreviewPublisher(options.previewNats,{info:capturePreview,error:capturePreview},publisherControls===undefined?{}:{connectionFactory:publisherControls.connectionFactory});
 const injectedPreviewPublisher=previewPublisher===undefined?undefined:{...previewPublisher,close:()=>{publisherControls?.markShutdown();return previewPublisher.close();}};
 const app=createProviderGatewayApp({...(injectedPreviewPublisher===undefined?{}:{previewPublisher:injectedPreviewPublisher}),config:{...fixtureConfig(),runtimeBindingTokenHMACKey:options.bindingKey??ContentLifecycleBindingKey},logger:{info:captureAdmission,error:captureAdmission},tokenReviewClient:{createTokenReview:async()=>({authenticated:true,audiences:["tetral-internal-grpc"],username:"system:serviceaccount:tetral-agent-runtime:agent-runtime",podUid:options.runtimePodUid??ContentLifecyclePodUid})},credentialResolver,providerStreamer,attachmentResolver,assemblyBounds:options.assemblyBounds??DefaultProviderAssemblyBounds,observeAssemblyResources:(resources,requestId)=>{assembly=resources;assemblySnapshots.set(requestId,resources);
  if(barrierOptions!==undefined){providerBarrier.assemblyArrivals=[...assemblySnapshots.values()].filter(value=>value.retainedBytes>=barrierOptions.retainedBytesPerRequest).length;providerBarrier.ready=providerBarrier.sourceArrivals>=barrierOptions.count&&providerBarrier.assemblyArrivals>=barrierOptions.count;}
  for(const key of Object.keys(peakAssembly) as (keyof ProviderAssemblyResources)[]){let total=0;for(const value of assemblySnapshots.values())total+=value[key];peakAssembly[key]=Math.max(peakAssembly[key],total);}if(resources.openBlocks===0)sample();}});
 const closeCredentialStore=async():Promise<void>=>{await sqlOwner?.close({deadline:new Date(Date.now()+5000)});credentialStoreClosed=true;};
 let started:Awaited<ReturnType<typeof app.start>>;
 try{started=await app.start();}catch(error){await closeCredentialStore();await attachmentResolver?.close();if(tokenDirectory!==undefined)await rm(tokenDirectory,{recursive:true,force:true});if(sampler!==undefined)clearInterval(sampler);throw error;}finally{Server.prototype.addService=originalAddService;}
 return {app,settleResources:async()=>{
  const start=performance.now();await new Promise(resolve=>setTimeout(resolve,100));const naturalMemory=process.memoryUsage();
  // Test-only reachability observation; normal production request cleanup does not force GC.
  Bun.gc(true);await new Promise(resolve=>setTimeout(resolve,0));Bun.gc(true);
  return {naturalMemory,forcedMemory:process.memoryUsage(),settleElapsedMs:performance.now()-start};
 },publisherControl:(operation:PublicPublisherControl)=>{if(publisherControls===undefined)throw new Error("publisher controls disabled");return publisherControls.control(operation);},releasePublisherControls:()=>publisherControls?.release(),releaseFragment,cutProviderRpc,configureScenarios,releaseProviderBarrier,releaseFinish:()=>finishGate.release(),releasePrefix:prefixGate.release,address:`127.0.0.1:${started.grpcPort}`,request:(overrides:Partial<ProviderRequest>={})=>contentLifecycleRequest(overrides,options),metadata:contentLifecycleMetadata,observations:()=>({fragmentWaiting,fragmentReleased,previewMetrics:previewPublisher?.metrics.render(),previewOutcomes,publisherControls:publisherControls?.observation(),pendingScenarioPlans:scenariosBySession.size,memory:latestMemory,memorySampleElapsedMs:lastMemorySampleAt-measurementStartedAt,assemblyTotals:Object.fromEntries(Object.keys(peakAssembly).map(key=>[key,[...assemblySnapshots.values()].reduce((sum,value)=>sum+value[key as keyof ProviderAssemblyResources],0)])),credentialStore:sqlOwner===undefined?"fixture":"sql",credentialStoreClosed,providerCalls,nativeContexts,providerBarrier,nativeIterators,admissionFailures,nativeAttachments,sessionCredentialReads,platformSelections,sourceRecords,activeProviderSources,sourceCancellations,joinedSources,sdk,assembly,peakAssembly,peakActiveProviderSources,writer,writerCalls,cpuMicros:process.cpuUsage(baselineCpu),measurementElapsedMs:performance.now()-measurementStartedAt,eventLoop:{lagMaxMs:loopLagMaxMs,lagSumMs:loopLagSumMs,samples:loopLagSamples},baselineMemory,finalMemory,peakHeap,peakRss,sdkTotals:{sourceRecords:[...sdkSnapshots.values()].reduce((sum,value)=>sum+value.sourceRecords,0),active:[...sdkSnapshots.values()].filter(value=>value.active).length},bounds:{assembly:options.assemblyBounds??DefaultProviderAssemblyBounds}}),shutdown:async()=>{closeFragments();providerBarrierGate.release();finishGate.release();prefixGate.release();try{await app.shutdown();}finally{await closeCredentialStore();await attachmentResolver?.close();if(tokenDirectory!==undefined)await rm(tokenDirectory,{recursive:true,force:true});if(sampler!==undefined)clearInterval(sampler);sample();if(options.measureResources)Bun.gc(true);finalMemory=process.memoryUsage();}}};
}
function semanticNativePart(part:unknown):unknown {
 if(typeof part!=="object"||part===null)return part;
 const object=part as Record<string,unknown>,result:Record<string,unknown>={};
 for(const key of ["type","text","thinking","signature","id","name","input","tool_use_id","is_error"])if(object[key]!==undefined)result[key]=object[key];
 if(object.content!==undefined){if(Array.isArray(object.content)){if(object.content.length>64)throw new Error("fixture native content parts exceeded");result.content=object.content.map(semanticNativePart);}else if(typeof object.content==="string")result.content=object.content;}
 return result;
}
function event(value:Record<string,unknown>):string {return `event: ${value.type}\ndata: ${JSON.stringify(value)}\n\n`;}
function* sseScript(scenario:ContentLifecycleScenario,textCodeUnits:number,fragmentCodeUnits:number,blockCount:number,emptyRecords:number,requestId?:string):Generator<string> {
 if(scenario.startsWith("public-")){yield* publicStreamingScript(scenario as PublicStreamingScenario);return;}
 const durable=scenario==="resource-after-tool"||scenario==="resource-after-write"||scenario==="durable-interleaved"||scenario==="durable-write"||scenario==="durable-bash"||scenario==="durable-write-queued"||scenario==="durable-read-three";
 const signedReasoning=durable||scenario==="reasoning-only";
 yield event({type:"message_start",message:{id:"msg_fixture",type:"message",role:"assistant",model:"claude-opus-4-8",content:[],stop_reason:null,stop_sequence:null,usage:{input_tokens:scenario==="compaction-primer"?968000:1,output_tokens:1}}});
 if(scenario==="tool-whitespace"){
  yield event({type:"content_block_start",index:0,content_block:{type:"tool_use",id:"call-whitespace",name:"Read",input:{}}});
  for(let offset=0;offset<textCodeUnits;offset+=fragmentCodeUnits)yield event({type:"content_block_delta",index:0,delta:{type:"input_json_delta",partial_json:" ".repeat(Math.min(fragmentCodeUnits,textCodeUnits-offset))}});
  yield event({type:"content_block_delta",index:0,delta:{type:"input_json_delta",partial_json:"{}"}});yield event({type:"content_block_stop",index:0});
  yield event({type:"message_delta",delta:{stop_reason:"tool_use",stop_sequence:null},usage:{output_tokens:1}});yield event({type:"message_stop"});return;
 }
 if(scenario==="tool-before-text"){
  yield event({type:"content_block_start",index:0,content_block:{type:"tool_use",id:"call-read-note",name:"Read",input:{}}});
  yield event({type:"content_block_delta",index:0,delta:{type:"input_json_delta",partial_json:JSON.stringify({file_path:"/workspace/note.txt"})}});yield event({type:"content_block_stop",index:0});
 }
 if(scenario==="interleaved"||signedReasoning||scenario==="metadata-only"||scenario==="error-after-complete-prefix"){
  yield event({type:"content_block_start",index:0,content_block:{type:"thinking",thinking:"",signature:""}});
  if(scenario!=="metadata-only")yield event({type:"content_block_delta",index:0,delta:{type:"thinking_delta",thinking:signedReasoning?"reason-before-text":"private R1"}});
  yield event({type:"content_block_delta",index:0,delta:{type:"signature_delta",signature:signedReasoning?"fixture-signature-text":"fixture_signature"}});yield event({type:"content_block_stop",index:0});
 }
 if(scenario==="reasoning-only"){
  yield event({type:"message_delta",delta:{stop_reason:"end_turn",stop_sequence:null},usage:{output_tokens:1}});yield event({type:"message_stop"});return;
 }
 if(scenario==="empty-blocks-storm") {
  for(let index=0;index<emptyRecords;index++) {
   yield event({type:"content_block_start",index,content_block:{type:"text",text:""}});
   yield event({type:"content_block_stop",index});
  }
  yield event({type:"message_delta",delta:{stop_reason:"end_turn",stop_sequence:null},usage:{output_tokens:0}});yield event({type:"message_stop"});return;
 }
 const texts=scenario==="compaction-primer"?["primer"]:scenario==="compaction-summary"?["compact-prior"]:scenario==="interleaved"||durable||scenario==="error-after-complete-prefix"?["alpha","beta"]:scenario==="done"?["done"]:scenario==="unicode-text"?['☃🙂\n"\\'.repeat(4096)]:scenario==="multiple-large"?Array.from({length:blockCount},()=>"x".repeat(textCodeUnits)):scenario==="empty-then-valid"?["","alpha"]:scenario==="empty-text"?[""]:scenario==="text-large"?["x".repeat(textCodeUnits)]:["alpha"];
 for(let block=0;block<texts.length;block++){
  const index=block+1,text=texts[block]!;yield event({type:"content_block_start",index,content_block:{type:"text",text:""}});
  if(scenario==="empty-storm")for(let i=0;i<emptyRecords;i++)yield event({type:"content_block_delta",index,delta:{type:"text_delta",text:""}});
  for(let offset=0;offset<text.length;offset+=fragmentCodeUnits)yield event({type:"content_block_delta",index,delta:{type:"text_delta",text:text.slice(offset,offset+fragmentCodeUnits)}});
  if(scenario==="truncated")return;
  if(scenario==="error-after-partial"||scenario==="error-after-complete-prefix"&&block===1)throw new TypeError("fixture provider stream failure");
  yield event({type:"content_block_stop",index});
 }
 if(durable){
  yield event({type:"content_block_start",index:3,content_block:{type:"thinking",thinking:"",signature:""}});
  yield event({type:"content_block_delta",index:3,delta:{type:"thinking_delta",thinking:"reason-before-tool"}});
  yield event({type:"content_block_delta",index:3,delta:{type:"signature_delta",signature:"fixture-signature-tool"}});
  yield event({type:"content_block_stop",index:3});
 }
 if(scenario==="interleaved"||durable){
  const index=durable?4:3;
  const tool=(scenario==="resource-after-write"||scenario==="durable-write"||scenario==="durable-write-queued")?{id:"call-write-note",name:"Write",input:{file_path:"/workspace/note.txt",content:"first\n"}}:scenario==="durable-bash"?{id:"call-bash-fixture",name:"Bash",input:{command:"printf fixture-note"}}:{id:durable?"call-read-note":"call_fixture_1",name:"Read",input:{file_path:scenario==="durable-read-three"?"/workspace/one.txt":"/workspace/note.txt"}};
  yield event({type:"content_block_start",index,content_block:{type:"tool_use",id:requestId===undefined?tool.id:`${tool.id}-${requestId}`,name:tool.name,input:{}}});yield event({type:"content_block_delta",index,delta:{type:"input_json_delta",partial_json:JSON.stringify(tool.input)}});yield event({type:"content_block_stop",index});
  if(scenario==="durable-read-three"){
   for(const [offset,path] of ["two","three"].entries()){
    const siblingIndex=5+offset;
    yield event({type:"content_block_start",index:siblingIndex,content_block:{type:"tool_use",id:`call-read-${path}`,name:"Read",input:{}}});
    yield event({type:"content_block_delta",index:siblingIndex,delta:{type:"input_json_delta",partial_json:JSON.stringify({file_path:`/workspace/${path}.txt`})}});
    yield event({type:"content_block_stop",index:siblingIndex});
   }
  }
  if(scenario==="durable-write-queued"){
   yield event({type:"content_block_start",index:5,content_block:{type:"tool_use",id:"call-write-note-2",name:"Write",input:{}}});
   yield event({type:"content_block_delta",index:5,delta:{type:"input_json_delta",partial_json:JSON.stringify({file_path:"/workspace/note.txt",content:"second\n"})}});yield event({type:"content_block_stop",index:5});
  }
 }
 if(scenario==="resource-after-tool"||scenario==="resource-after-write"){
  yield "event: fixture_resource_gate\ndata: {}\n\n";
  yield event({type:"content_block_start",index:5,content_block:{type:"text",text:""}});
  for(let bytes=0;bytes<16*1024*1024;bytes+=8192)yield event({type:"content_block_delta",index:5,delta:{type:"text_delta",text:"x".repeat(8192)}});
  yield event({type:"content_block_stop",index:5});
 }
 yield event({type:"message_delta",delta:{stop_reason:scenario==="interleaved"||durable||scenario==="tool-before-text"?"tool_use":"end_turn",stop_sequence:null},usage:{output_tokens:4}});yield event({type:"message_stop"});
}
function fixtureConfig():ProviderGatewayConfig {return {deploymentEnvironment:"test",diagnostics:{level:"info",maxRecordBytes:16384,summaryIntervalMs:30000,burst:1},serviceVersion:"test",grpcBindAddress:"127.0.0.1:0",httpBindAddress:"127.0.0.1:0",allowedRuntimePod:{namespace:"tetral-agent-runtime",serviceAccount:"agent-runtime"},runtimeBindingTokenHMACKey:ContentLifecycleBindingKey,databaseUrl:"postgres://fixture.invalid/tetral",databasePool:{max:10,idleTimeout:30,maxLifetime:1800,connectionTimeout:30,statementTimeoutMs:30000},drainTimeoutMs:30000,cancelJoinTimeoutMs:5000,vaultKeyHex:MasterKey,kubernetesApiServerUrl:"https://kubernetes.invalid",kubernetesApiCaCertPath:"/fixture/ca",tokenReviewReviewerTokenPath:"/fixture/token",bridgeApiGrpcAddress:"127.0.0.1:1",bridgeTokenPath:"/fixture/bridge",maxConcurrentTurns:8};}

function gate(held:boolean):{readonly wait:Promise<void>;readonly release:()=>void}{let release=()=>{};const wait=held?new Promise<void>(resolve=>{release=resolve;}):Promise.resolve();return {wait,release};}

// Newline JSON controls work identically through Go exec.Cmd and Bun.spawn.
if(import.meta.main){
 let fixture:Awaited<ReturnType<typeof createContentLifecycleGatewayFixture>>|undefined;
 const send=(value:unknown):void=>{process.stdout.write(JSON.stringify(value)+"\n");};
 const input=createInterface({input:process.stdin});
 input.on("line",async line=>{
  try {
   const message=JSON.parse(line) as {kind:string;options?:ContentLifecycleGatewayFixtureOptions;sessionId?:string;scenarios?:ContentLifecycleScenario[];operation?:PublicPublisherControl};
   if(message.kind==="start"){fixture=await createContentLifecycleGatewayFixture(message.options);send({kind:"ready",address:fixture.address,protocolVersion:2,request:fixture.request(),observation:fixture.observations()});}
   else if(message.kind==="configure_scenarios"){fixture?.configureScenarios(message.sessionId??"",message.scenarios??[]);send({kind:"configured_scenarios"});}
   else if(message.kind==="observe_settled"){const settled=await fixture?.settleResources();send({kind:"settled",...settled,...fixture?.observations()});}
   else if(message.kind==="observe")send({kind:"observation",...fixture?.observations()});
   else if(message.kind==="release_provider_barrier"){fixture?.releaseProviderBarrier();process.stdout.write(JSON.stringify({kind:"provider_barrier_released"})+"\n");}
   else if(message.kind==="cut_provider_rpc"){fixture?.cutProviderRpc();send({kind:"provider_rpc_cut"});}
   else if(message.kind==="publisher_control"){if(message.operation===undefined)throw new Error("publisher control operation required");send({kind:"publisher_controlled",publisherControls:fixture?.publisherControl(message.operation)});}
   else if(message.kind==="release_fragment"){fixture?.releaseFragment();send({kind:"released_fragment"});}
   else if(message.kind==="release_finish"){fixture?.releaseFinish();send({kind:"released_finish"});}
   else if(message.kind==="release_prefix"){fixture?.releasePrefix();send({kind:"released_prefix"});}
   else if(message.kind==="stop"){await fixture?.shutdown();send({kind:"stopped",...fixture?.observations()});process.exit(0);}
  }catch{send({kind:"fixture_error"});process.exitCode=1;}
 });
 input.on("close",async()=>{fixture?.releasePublisherControls();await fixture?.shutdown();process.exit(0);});
}
