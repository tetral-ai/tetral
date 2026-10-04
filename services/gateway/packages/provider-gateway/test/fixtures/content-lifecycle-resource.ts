/** Sequentially launched calibration driver; Gateway memory is measured in its own child. */
import { createHash } from "node:crypto";
import { credentials } from "@grpc/grpc-js";
import { ProviderGatewayServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { contentLifecycleMetadata } from "./content-lifecycle-gateway.js";
import { ProviderAssemblyCalibrationCandidate } from "../../src/providers/resource-policy.js";
import type { ContentLifecycleGatewayFixtureOptions } from "./content-lifecycle-gateway.js";
const cases:Record<string,{options:ContentLifecycleGatewayFixtureOptions;concurrency?:number;slowReader?:boolean;abort?:boolean}>= {
 "four8-barrier":{options:{scenario:"text-large",textCodeUnits:8*1024*1024,concurrentProviderBarrier:{count:4,retainedBytesPerRequest:4*1024*1024}},concurrency:4},
 "eight2-barrier":{options:{scenario:"text-large",textCodeUnits:2*1024*1024,concurrentProviderBarrier:{count:8,retainedBytesPerRequest:1024*1024}},concurrency:8},
 "four-small":{options:{scenario:"text-large",textCodeUnits:1024},concurrency:4},
 "plain8":{options:{scenario:"text-large",textCodeUnits:8*1024*1024}},
 "plain16":{options:{scenario:"text-large",textCodeUnits:16*1024*1024-2}},
 "tiny8":{options:{scenario:"text-large",textCodeUnits:8*1024*1024,fragmentCodeUnits:1}},
 "empty-storm":{options:{scenario:"empty-storm",emptyRecords:1000,sdkBounds:{maxRecords:8,maxSerializedPayloadBytes:64*1024*1024}}},
 "three-max64":{options:{scenario:"multiple-large",textCodeUnits:16*1024*1024-2,blockCount:3,assemblyBounds:{...ProviderAssemblyCalibrationCandidate,maxCumulativeContentBytes:64*1024*1024}}},
 "three-max":{options:{scenario:"multiple-large",textCodeUnits:16*1024*1024-2,blockCount:3}},
 "slow8":{options:{scenario:"text-large",textCodeUnits:8*1024*1024},slowReader:true},
 "abort8":{options:{scenario:"text-large",textCodeUnits:8*1024*1024,holdFinish:true},abort:true},
 "four8":{options:{scenario:"text-large",textCodeUnits:8*1024*1024},concurrency:4},
 "eight2":{options:{scenario:"text-large",textCodeUnits:2*1024*1024},concurrency:8},
};
const name=process.argv[2]??"plain8",selected=cases[name];if(selected===undefined)throw new Error("unknown calibration case");
const child=Bun.spawn([process.execPath,new URL("content-lifecycle-gateway.ts",import.meta.url).pathname],{stdin:"pipe",stdout:"pipe",stderr:"pipe"});
const reader=child.stdout.getReader();let buffered="";
const receive=async():Promise<any>=>{while(true){const newline=buffered.indexOf("\n");if(newline>=0){const line=buffered.slice(0,newline);buffered=buffered.slice(newline+1);return JSON.parse(line);}const next=await reader.read();if(next.done)throw new Error("fixture exited before response");buffered+=new TextDecoder().decode(next.value);if(buffered.length>65536)throw new Error("fixture output bound exceeded");}};
const send=(value:unknown):void=>{child.stdin.write(JSON.stringify(value)+"\n");child.stdin.flush();};
let forcedKill=false,watchdogExpired=false,failed=false;
const watchdog=setTimeout(()=>{watchdogExpired=true;forcedKill=true;child.kill("SIGKILL");},name==="four-small"?4000:60000);
let client:ProviderGatewayServiceClient|undefined;
const started=performance.now();
const clientTimeline:Array<{requestId:string;event:string;atMs:number;code?:number;details?:string}> = [];
let requests:unknown[]=[];let stopped:unknown,observation:unknown,interimObservation:unknown;
try{
 send({kind:"start",options:{...selected.options,measureResources:true}});const ready=await receive();if(ready.kind!=="ready")throw new Error("fixture not ready");
 client=new ProviderGatewayServiceClient(ready.address,credentials.createInsecure(),{"grpc.max_receive_message_length":32*1024*1024});
 const run=async(index:number)=>{
  const request={...ready.request,requestId:`req_resource_${index}`,modelRequestId:`mreq_resource_${index}`,limits:{...ready.request.limits,timeoutMs:120000}} as ProviderRequest;
  clientTimeline.push({requestId:request.requestId,event:"dispatch",atMs:performance.now()-started});
  const call=client!.streamProviderRequest(request,contentLifecycleMetadata());
  call.on("status",status=>{clientTimeline.push({requestId:request.requestId,event:"status",atMs:performance.now()-started,code:status.code,details:status.details.slice(0,512)});});
  const digest=createHash("sha256");let frames=0,textBytes=0,terminal="none",errorCode:string|undefined;
  if(selected.slowReader){call.pause();await new Promise(resolve=>setTimeout(resolve,300));call.resume();}
  if(selected.abort){call.pause();await new Promise(resolve=>setTimeout(resolve,100));clientTimeline.push({requestId:request.requestId,event:"cancel_trigger",atMs:performance.now()-started});call.cancel();}
  try{for await(const frame of call){frames++;if(frame.textComplete!==undefined){const bytes=new TextEncoder().encode(frame.textComplete.text);textBytes+=bytes.byteLength;digest.update(bytes);}if(frame.finish!==undefined)terminal="finish";if(frame.providerError!==undefined){terminal="provider-error";errorCode=frame.providerError.error?.code;}}}catch(error){terminal="grpc-error";errorCode=String((error as {code?:number}).code);clientTimeline.push({requestId:request.requestId,event:"error",atMs:performance.now()-started,code:(error as {code:number}).code,details:String((error as {details?:string}).details??"").slice(0,512)});}
  const expected=createHash("sha256"),units=(selected.options.textCodeUnits??0)*(selected.options.scenario==="multiple-large"?(selected.options.blockCount??3):1);
  for(let offset=0;offset<units;offset+=4096)expected.update("x".repeat(Math.min(4096,units-offset)));
  const sha256=digest.digest("hex"),expectedSha256=expected.digest("hex");
  if(!selected.abort&&(selected.options.scenario==="text-large"||name==="three-max64")&&(terminal!=="finish"||textBytes!==units||sha256!==expectedSha256))failed=true;
  return {frames,textBytes,terminal,errorCode,sha256,expectedSha256,digestMatches:sha256===expectedSha256};
 };
 const pending=Promise.all(Array.from({length:selected.concurrency??1},(_,index)=>run(index)));
 if(selected.options.concurrentProviderBarrier!==undefined){
  let barrierReady=false;for(let poll=0;poll<200;poll++){send({kind:"observe"});const observed=await receive();interimObservation=observed;if(observed.providerBarrier.ready){barrierReady=true;break;}await new Promise(resolve=>setTimeout(resolve,10));}
  if(!barrierReady)throw new Error("provider barrier timed out");
  send({kind:"release_provider_barrier"});const released=await receive();if(released.kind!=="provider_barrier_released")throw new Error("provider barrier release failed");
 }else{await Promise.race([pending,new Promise(resolve=>setTimeout(resolve,250))]);send({kind:"observe"});interimObservation=await receive();}
 requests=await pending;
 send({kind:"observe"});observation=await receive();
} catch {failed=true;}
finally{
 client?.close();
 if(!watchdogExpired){try{send({kind:"stop"});stopped=await receive();}catch{failed=true;}}
 clearTimeout(watchdog);
 let joinTimer:ReturnType<typeof setTimeout>|undefined;
 const exit=await Promise.race([child.exited,new Promise<number>(resolve=>{joinTimer=setTimeout(()=>{forcedKill=true;child.kill("SIGKILL");resolve(-1);},5000);})]);
 if(joinTimer!==undefined)clearTimeout(joinTimer);
 const stderr=await new Response(child.stderr).text();
 process.stdout.write(JSON.stringify({name,scope:"actual Gateway child; actual pinned ai6.0.168 SDK; parent gRPC consumer; local Bun only",controls:selected,elapsedMs:performance.now()-started,requests,clientTimeline,interimObservation,observation,stopped,failed,forcedKill,watchdogExpired,exit,stderrBytes:stderr.length})+"\n");
 if(failed||forcedKill||exit!==0)process.exitCode=1;
}
