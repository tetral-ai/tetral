import assert from "node:assert/strict";
import {createHmac} from "node:crypto";
import {Metadata} from "@grpc/grpc-js";
import {Effect,Stream} from "effect";
import {RuntimePodGatewayClient} from "../../src/gateway-client.js";
import {createProviderGatewayApp} from "../../../../../gateway/packages/provider-gateway/src/app.js";
import {ProviderClientRegistry} from "../../../../../gateway/packages/provider-gateway/src/providers/clients.js";
import {validProviderRequest} from "../../../../../gateway/packages/provider-gateway/test/unit/fixtures.js";
import type {ProviderGatewayConfig} from "../../../../../gateway/packages/provider-gateway/src/config.js";
import type {ProviderStreamEvent} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
const completionSamples: Array<{cohort:string;method:string;receiver:string;outcome:string;start_boundary:string;end_boundary:string;clock_id:string;start_ns:number;end_ns:number;duration_ns:number}>=[];
const [backend,variant]=process.argv.slice(2);assert(backend&&variant);
const signingKey="replica-provider-binding-key-at-least-32-bytes",podUID="pod_replica_provider",runtimeProcessId="process_replica_provider";
const config:ProviderGatewayConfig={deploymentEnvironment:"test",diagnostics:{level:"info",maxRecordBytes:16384,summaryIntervalMs:30000,burst:1},serviceVersion:"replica-test",grpcBindAddress:"127.0.0.1:0",httpBindAddress:"127.0.0.1:0",allowedRuntimePod:{namespace:"tetral-agent-runtime",serviceAccount:"agent-runtime"},runtimeBindingTokenHMACKey:signingKey,databaseUrl:"postgres://not-opened/fixture",databasePool:{max:2,idleTimeout:30,maxLifetime:1800,connectionTimeout:30,statementTimeoutMs:30000},drainTimeoutMs:500,cancelJoinTimeoutMs:1000,vaultKeyHex:"00".repeat(32),kubernetesApiServerUrl:"https://kubernetes.default.svc",kubernetesApiCaCertPath:"unused",tokenReviewReviewerTokenPath:"unused",bridgeApiGrpcAddress:"unused:9090",bridgeTokenPath:"unused",maxConcurrentTurns:2};
const fetchProvider=Object.assign(async(input:RequestInfo|URL,init?:RequestInit)=>{const original=new Request(input,init);return await fetch(new Request(`${backend}/provider`,original));},{preconnect:()=>{}});
const registry=new ProviderClientRegistry({fetch:fetchProvider});
const logs:unknown[]=[];
const app=createProviderGatewayApp({config,logger:{info:r=>logs.push(r),error:r=>logs.push(r)},tokenReviewClient:{createTokenReview:async()=>({authenticated:true,audiences:["tetral-internal-grpc"],username:"system:serviceaccount:tetral-agent-runtime:agent-runtime",podUid:podUID})},providerStreamer:{stream:input=>registry.stream({...input,credential:{source:"session",authType:"provider_api_key",providerId:"anthropic",supplyMode:"anthropic-api-key",vaultId:"fixture",credentialId:"fixture",accessMode:"api_key",apiKey:"fixture-provider-key"}})}});
const started=await app.start();
const client=new RuntimePodGatewayClient({address:`127.0.0.1:${started.grpcPort}`,tokenPath:"unused",metadataFactory:async()=>{const md=new Metadata();md.set("authorization","Bearer fixture-runtime");return md;}});
const state=async()=>await(await fetch(`${backend}/state`)).json() as {starts:number;cancelled:number;completed:number;active:number;markers:string[]|null};
async function until(check:()=>Promise<boolean>){const deadline=Date.now()+10000;while(!await check()){assert(Date.now()<deadline,"observable condition timed out");await new Promise(resolve=>setTimeout(resolve,10));}}
async function control(action:string,marker:string){assert.equal((await fetch(`${backend}/${action}?marker=${marker}`)).status,200);}
async function open(marker:string){
 const request=validProviderRequest({workspaceId:"default",sessionId:"sesn_replica_provider",sessionThreadId:"sthr_replica_provider",bindingId:"bind_replica_provider",bindingGeneration:1,runtimeProcessId,requestId:`req_${marker}`,modelRequestId:`mreq_${marker}`,model:{providerId:"anthropic",modelId:"claude-opus-4-8",variant:""},tools:[],attachments:[],limits:{maxOutputTokens:128,timeoutMs:20000}});
 request.context[0]!.content=[{text:{text:marker}}];
 const payload=Buffer.from(JSON.stringify({v:1,workspace_id:request.workspaceId,session_id:request.sessionId,session_thread_id:request.sessionThreadId,binding_id:request.bindingId,binding_generation:request.bindingGeneration,runtime_pod_uid:podUID,runtime_process_id:runtimeProcessId,exp:Math.floor(Date.now()/1000)+300})).toString("base64url");request.runtimeBindingToken=`rtbt_v1.${payload}.${createHmac("sha256",signingKey).update(payload).digest("base64url")}`;
 const startedAt=performance.now();
 const record=(outcome:string)=>{const startNS=Math.round(startedAt*1e6),endNS=Math.round(performance.now()*1e6);completionSamples.push({cohort:"model_stream",method:"StreamProviderRequest",receiver:`gateway:${started.grpcPort}`,outcome,start_boundary:"stream_call_started",end_boundary:"stream_completed",clock_id:`bun:${process.pid}`,start_ns:startNS,end_ns:endNS,duration_ns:endNS-startNS});};
 const handle=await client.streamProviderRequest(request);const events:ProviderStreamEvent[]=[];
 const consumed=Effect.runPromise(Stream.runForEach(handle.events,event=>Effect.sync(()=>{events.push(event);})));void consumed.catch(()=>{});
 const completion=consumed.then(async()=>{const result=await handle.completion;const providerError=events.find(event=>event.providerError)?.providerError?.error;record(providerError?.code??(result.outcome==="eof"?(events.some(event=>event.finish)?"success":"incomplete_stream"):result.outcome));return result;},error=>{record("transport_error");throw error;});void completion.catch(()=>{});
 return{handle,events,join:async()=>await completion,marker};
}
async function partial(stream:Awaited<ReturnType<typeof open>>){await until(async()=>{const snapshot=await state();return snapshot.markers!==null&&snapshot.markers.includes(stream.marker);});assert(!stream.events.some(event=>event.textComplete),"partial SDK text must remain private before completion");}
function assertOwnFrames(stream:Awaited<ReturnType<typeof open>>){for(const e of stream.events)if(e.textComplete?.text)assert.equal(e.textComplete.text,`${stream.marker} partial`);}
try{
 if(variant==="admission_cancel"){
  const a=await open("replica_capacity_a"),b=await open("replica_capacity_b");await partial(a);await partial(b);
  assert.equal((await state()).starts,2);
  const rejected=await open("replica_capacity_rejected");await rejected.join();assert.equal(rejected.events.find(e=>e.providerError)?.providerError?.error?.code,"provider_unavailable");assert.equal((await state()).starts,2);
  a.handle.cancel("caller");assert.equal((await a.join()).outcome,"cancelled");await until(async()=>(await state()).cancelled===1);
  const replacement=await open("replica_capacity_replacement");await partial(replacement);assert.equal((await state()).starts,3);
  await control("finish",b.marker);await control("finish",replacement.marker);await b.join();await replacement.join();for(const s of [a,b,replacement])assertOwnFrames(s);
 }else if(variant==="drop"){
  const broken=await open("replica_drop_first");await partial(broken);await control("drop",broken.marker);await broken.join();assertOwnFrames(broken);assert(broken.events.some(e=>e.providerError),"partial stream drop must remain an error");assert.equal((await state()).starts,1);
  const subsequent=await open("replica_drop_business_attempt");await partial(subsequent);await control("finish",subsequent.marker);await subsequent.join();assertOwnFrames(subsequent);assert.equal((await state()).starts,2);
 }else{
  const stream=await open(`replica_${variant}`);await partial(stream);
  const draining=app.shutdown(new Date(Date.now()+(variant==="drain_complete"?2000:100)));assert.equal(app.ready().ready,false);
  if(variant==="drain_complete")await control("finish",stream.marker);
  await draining;await stream.join();assertOwnFrames(stream);
  await until(async()=>(await state()).active===0);
  const final=await state();assert.equal(final.starts,1);assert.equal(variant==="drain_complete"?final.completed:final.cancelled,1);
 }
 await until(async()=>(await state()).active===0);
 assert(app.service.metricsText().includes("providergateway_provider_streams_active 0"));
}finally{await client.close();await app.shutdown(new Date(Date.now()+500));}
console.log(JSON.stringify({ok:true,completionSamples,...await state()}));
