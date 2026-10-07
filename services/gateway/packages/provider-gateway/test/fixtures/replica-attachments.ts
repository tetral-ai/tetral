import { NormalizedProviderEventType } from "@tetral/gateway-lowering/src/normalized-stream.js";
import assert from "node:assert/strict";
import { createHash, createHmac } from "node:crypto";
import { credentials, Metadata } from "@grpc/grpc-js";
import { AgentRuntimeBridgeServiceClient } from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { ProviderGatewayServiceClient, ProviderFinishReason } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { BridgeAPIAttachmentResolver, bridgeAttachmentGrpcChannelOptions } from "../../src/attachments.js";
import { createProviderGatewayApp } from "../../src/app.js";
import { validProviderRequest } from "../unit/fixtures.js";
import type { ProviderGatewayConfig } from "../../src/config.js";

const completionSamples: Array<{cohort:string;method:string;receiver:string;outcome:string;start_boundary:string;end_boundary:string;clock_id:string;start_ns:number;end_ns:number;duration_ns:number}>=[];
const [addressA,addressB,variant,controlURL,expectedHash] = process.argv.slice(2);
assert(addressA && addressB && variant && controlURL && expectedHash);
const signingKey="replica-provider-binding-key-at-least-32-bytes";
const podUID="pod_replica_attachment";
const runtimeProcessId=`process_${podUID}`;
const a=new AgentRuntimeBridgeServiceClient(addressA,credentials.createInsecure(),bridgeAttachmentGrpcChannelOptions());
const b=new AgentRuntimeBridgeServiceClient(addressB,credentials.createInsecure(),bridgeAttachmentGrpcChannelOptions());
// Route a real generated metadata call to A and byte calls to B, preserving
// their handles, options, serialization and callbacks. No synthetic response.
b.resolveFileAttachmentMetadata=a.resolveFileAttachmentMetadata.bind(a);
let clientCloses=0;const closeB=b.close.bind(b);
b.close=()=>{clientCloses++;a.close();closeB();};
const resolver=new BridgeAPIAttachmentResolver({address:addressB,tokenPath:"unused",client:b,metadataFactory:async()=>{const m=new Metadata();m.set("authorization","Bearer replica-provider");return m;}});
const actualResolve=resolver.resolve.bind(resolver);
resolver.resolve=async(input)=>{
 const startedAt=performance.now();let outcome="transport_error";
 try{const result=await actualResolve(input);outcome=result.ok?"success":(result.error.code??"attachment_error");return result;}
 finally{const startNS=Math.round(startedAt*1e6),endNS=Math.round(performance.now()*1e6);completionSamples.push({cohort:"attachment_preparation",method:"resolve",receiver:`bridge-metadata:${addressA};bridge-bytes:${addressB}`,outcome,start_boundary:"resolve_started",end_boundary:"resolve_completed",clock_id:`bun:${process.pid}`,start_ns:startNS,end_ns:endNS,duration_ns:endNS-startNS});}
};
const config:ProviderGatewayConfig={
 deploymentEnvironment:"test",diagnostics:{level:"info",maxRecordBytes:16384,summaryIntervalMs:30000,burst:1},serviceVersion:"replica-test",
 grpcBindAddress:"127.0.0.1:0",httpBindAddress:"127.0.0.1:0",allowedRuntimePod:{namespace:"tetral-agent-runtime",serviceAccount:"agent-runtime"},
 runtimeBindingTokenHMACKey:signingKey,databaseUrl:"postgres://not-opened-by-this-app/fixture",databasePool:{max:2,idleTimeout:30,maxLifetime:1800,connectionTimeout:30,statementTimeoutMs:30000},drainTimeoutMs:100,cancelJoinTimeoutMs:1000,
 vaultKeyHex:"00".repeat(32),kubernetesApiServerUrl:"https://kubernetes.default.svc",kubernetesApiCaCertPath:"unused",tokenReviewReviewerTokenPath:"unused",
 bridgeApiGrpcAddress:addressB,bridgeTokenPath:"unused",maxConcurrentTurns:2,
};
let providerCalls=0,hash="";
const logs:unknown[]=[];
const app=createProviderGatewayApp({config,logger:{info:r=>logs.push(r),error:r=>logs.push(r)},
 tokenReviewClient:{createTokenReview:async()=>{await new Promise(resolve=>setTimeout(resolve,30));return{authenticated:true,audiences:["tetral-internal-grpc"],username:"system:serviceaccount:tetral-agent-runtime:agent-runtime",podUid:podUID};}},
 attachmentResolver:resolver,
 providerStreamer:{stream:async function*(input){
  providerCalls++;
  if(variant==="deleted")assert.equal(input.resolvedAttachments!.length,0);
  else if(variant==="transient"){assert.equal(input.resolvedAttachments!.length,1);assert.equal(Buffer.from(input.resolvedAttachments![0]!.data).toString(),"independent-transient-bytes");}
  else{assert.equal(input.resolvedAttachments!.length,1);assert.equal(input.resolvedAttachments![0]!.data.length,8*1024*1024+17);hash=createHash("sha256").update(input.resolvedAttachments![0]!.data).digest("hex");assert.equal(hash,expectedHash);}
  yield{type:NormalizedProviderEventType.Finish,finish:{reason:ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,usage:{inputTotalTokens:1,inputUncachedTokens:1,outputTotalTokens:0,totalTokens:1,providerUsageJson:"{}"},metadataJson:"{}"}};
 }},
});
const started=await app.start();
const client=new ProviderGatewayServiceClient(`127.0.0.1:${started.grpcPort}`,credentials.createInsecure());
try{
 const request=validProviderRequest({workspaceId:"default",sessionId:"sesn_replica_attachment",sessionThreadId:"sthr_replica_attachment",bindingId:"bind_replica_attachment",bindingGeneration:1,runtimeProcessId,
  model:{providerId:"anthropic",modelId:"claude-opus-4-8",variant:""},tools:[],limits:{maxOutputTokens:1024,timeoutMs:variant==="deadline"?500:10000},
  attachments:variant==="transient"?[{transient:{attachmentRef:"att_fixture_replica_transient",sourcePath:"sandbox:replica_transient.png",pageRange:"",detail:"auto"},mime:"image/png",filename:"replica_transient.png"}]:[{fileBacked:{sourceEventId:variant==="scope"?"sevt_wrong_scope":"sevt_replica_attachment",fileId:"file_replica_attachment"},mime:"image/png",filename:"replica.png"}],
 });
 const payload=Buffer.from(JSON.stringify({v:1,workspace_id:request.workspaceId,session_id:request.sessionId,session_thread_id:request.sessionThreadId,binding_id:request.bindingId,binding_generation:request.bindingGeneration,runtime_pod_uid:podUID,runtime_process_id:runtimeProcessId,exp:Math.floor(Date.now()/1000)+300})).toString("base64url");
 request.runtimeBindingToken=`rtbt_v1.${payload}.${createHmac("sha256",signingKey).update(payload).digest("base64url")}`;
 const md=new Metadata();md.set("authorization","Bearer runtime-fixture");
 const call=client.streamProviderRequest(request,md,{deadline:Date.now()+15000});
 const events: any[]=[];
 const ended=new Promise<void>(resolve=>{call.on("data",event=>events.push(event));call.on("error",()=>resolve());call.on("end",()=>resolve());});
 if(["abort","deadline","shutdown"].includes(variant)){
  const response=await fetch(controlURL,{signal:AbortSignal.timeout(5000)});assert.equal(response.status,200);
  if(variant==="abort")call.cancel();
  if(variant==="shutdown"){const draining=app.shutdown(new Date(Date.now()+100));assert.equal(app.ready().ready,false);await draining;}
 }
 await ended;
 if(variant==="assembled"||variant==="deleted"||variant==="transient")assert.equal(providerCalls,1);
 else{
  assert.equal(providerCalls,0);
  if(variant==="scope"||variant==="malformed")assert.equal(events.find(e=>e.providerError)?.providerError.error?.code,"provider_request_invalid");
  if(variant==="short")assert.equal(events.find(e=>e.providerError)?.providerError.error?.code,"attachment_unavailable");
 }
}finally{
 client.close();await app.shutdown(new Date(Date.now()+500));await resolver.close();await resolver.close();
}
assert.equal(clientCloses,1);
assert(app.service.metricsText().includes("providergateway_provider_streams_active 0"));
console.log(JSON.stringify({ok:true,providerCalls,clientCloses,hash,completionSamples}));
