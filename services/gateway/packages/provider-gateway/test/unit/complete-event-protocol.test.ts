import { expect,test } from "bun:test";
import { credentials } from "@grpc/grpc-js";
import { ProviderGatewayServiceClient,ProviderStreamEventType } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createContentLifecycleGatewayFixture } from "../fixtures/content-lifecycle-gateway.js";
test("actual pinned SDK and app deliver complete frames through real gRPC",async()=>{
 const fixture=await createContentLifecycleGatewayFixture(); const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure(),{"grpc.max_receive_message_length":32*1024*1024});
 try {
  const frames:ProviderStreamEvent[]=[];for await(const frame of client.streamProviderRequest(fixture.request(),fixture.metadata()))frames.push(frame);
  expect(frames.map(frame=>frame.type)).toEqual([ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_THINKING_STARTED,ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_COMPLETE,ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE,ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE,ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL_COMPLETE,ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH]);
  expect(frames.map(frame=>frame.frameSequence)).toEqual([1,2,3,4,5,6]); expect(frames[2]?.textComplete?.text).toBe("alpha");expect(frames[3]?.textComplete?.text).toBe("beta");expect(frames[4]?.toolCallComplete?.modelToolCallId).toBe("call_fixture_1");
  expect(JSON.stringify(frames[0])).not.toContain("private R1");expect(frames[1]?.reasoningComplete?.providerMetadataJson).toContain("fixture_signature");
  expect(fixture.observations()).toMatchObject({providerCalls:1,activeProviderSources:0,sdk:{active:false},assembly:{retainedBytes:0,segments:0,openBlocks:0,identities:0}});
 }finally{client.close();await fixture.shutdown();}
});
for(const version of [0,1])test(`version ${version} rejects before provider SDK invocation`,async()=>{
 const fixture=await createContentLifecycleGatewayFixture(); const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{let failure:unknown;try{for await(const _ of client.streamProviderRequest(fixture.request({outputContractVersion:version}),fixture.metadata())){}}catch(error){failure=error;}expect(failure).toMatchObject({code:3});expect(fixture.observations().providerCalls).toBe(0);}finally{client.close();await fixture.shutdown();}
});
for(const scenario of ["truncated","error-after-partial"] as const)test(`actual SDK ${scenario} has one provider attempt and no complete text`,async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario});const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{const frames:ProviderStreamEvent[]=[];for await(const frame of client.streamProviderRequest(fixture.request(),fixture.metadata()))frames.push(frame);expect(frames.some(frame=>frame.textComplete!==undefined)).toBe(false);expect(frames.at(-1)?.providerError).toBeDefined();expect(fixture.observations().providerCalls).toBe(1);expect(fixture.observations().activeProviderSources).toBe(0);}finally{client.close();await fixture.shutdown();}
});
for(const vector of [
 {scenario:"durable-write",name:"Write",id:"call-write-note",input:{file_path:"/workspace/note.txt",content:"first\n"}},
 {scenario:"durable-bash",name:"Bash",id:"call-bash-fixture",input:{command:"printf fixture-note"}},
] as const)test(`actual SDK ${vector.scenario} preserves durable signed prefix and tool input`,async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario:vector.scenario,followupScenario:"done"});const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{
  const request=fixture.request({tools:[{name:vector.name,description:"Fixture tool",function:{inputSchemaJson:'{"type":"object","additionalProperties":true}'}}]});
  const frames:ProviderStreamEvent[]=[];for await(const frame of client.streamProviderRequest(request,fixture.metadata()))frames.push(frame);
  expect(frames.filter(frame=>frame.textComplete!==undefined).map(frame=>frame.textComplete!.text)).toEqual(["alpha","beta"]);
  expect(frames.filter(frame=>frame.reasoningComplete!==undefined).map(frame=>({text:frame.reasoningComplete!.text,metadata:JSON.parse(frame.reasoningComplete!.providerMetadataJson)}))).toEqual([
   {text:"reason-before-text",metadata:{anthropic:{signature:"fixture-signature-text"}}},
   {text:"reason-before-tool",metadata:{anthropic:{signature:"fixture-signature-tool"}}},
  ]);
  const tool=frames.find(frame=>frame.toolCallComplete!==undefined)?.toolCallComplete;expect(tool?.name).toBe(vector.name);expect(tool?.modelToolCallId).toBe(vector.id);expect(JSON.parse(tool!.inputJson)).toEqual(vector.input);expect(frames.at(-1)?.finish).toBeDefined();
  const followup:ProviderStreamEvent[]=[];for await(const frame of client.streamProviderRequest(request,fixture.metadata()))followup.push(frame);expect(followup[0]?.textComplete?.text).toBe("done");expect(followup.at(-1)?.finish).toBeDefined();
  expect(fixture.observations().nativeIterators).toMatchObject({started:2,active:0,joined:2});
 }finally{client.close();await fixture.shutdown();}
});
test("actual SDK provider barrier retains two half blocks before explicit release",async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario:"text-large",textCodeUnits:1024,fragmentCodeUnits:256,concurrentProviderBarrier:{count:2,retainedBytesPerRequest:512}});const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{
  const requests=Promise.all([0,1].map(async index=>{const frames:ProviderStreamEvent[]=[];for await(const frame of client.streamProviderRequest(fixture.request({requestId:`req_barrier_${index}`,modelRequestId:`mreq_barrier_${index}`}),fixture.metadata()))frames.push(frame);return frames;}));
  for(let poll=0;poll<100&&!fixture.observations().providerBarrier.ready;poll++)await new Promise(resolve=>setTimeout(resolve,5));
  expect(fixture.observations()).toMatchObject({providerBarrier:{ready:true,sourceArrivals:2,assemblyArrivals:2,released:false},activeProviderSources:2,nativeIterators:{active:2},peakAssembly:{retainedBytes:1024},sdkTotals:{active:2}});
  fixture.releaseProviderBarrier();const frames=await requests;expect(frames.map(value=>value[0]?.textComplete?.text)).toEqual(["x".repeat(1024),"x".repeat(1024)]);expect(frames.every(value=>value.at(-1)?.finish!==undefined)).toBe(true);expect(fixture.observations().nativeIterators).toMatchObject({active:0,joined:2});
 }finally{client.close();await fixture.shutdown();}
});
test("actual SDK queued Writes and opt-in native context preserve independent literals",async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario:"durable-write-queued",recordContext:true});const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{
  const request=fixture.request({tools:[{name:"Write",description:"Fixture tool",function:{inputSchemaJson:'{"type":"object","additionalProperties":true}'}}]});
  const frames:ProviderStreamEvent[]=[];for await(const frame of client.streamProviderRequest(request,fixture.metadata()))frames.push(frame);
  expect(frames.filter(frame=>frame.toolCallComplete!==undefined).map(frame=>({id:frame.toolCallComplete!.modelToolCallId,name:frame.toolCallComplete!.name,input:JSON.parse(frame.toolCallComplete!.inputJson)}))).toEqual([
   {id:"call-write-note",name:"Write",input:{file_path:"/workspace/note.txt",content:"first\n"}},
   {id:"call-write-note-2",name:"Write",input:{file_path:"/workspace/note.txt",content:"second\n"}},
  ]);
  expect(fixture.observations().nativeContexts).toEqual([{requestOrdinal:1,messages:[{role:"user",content:[{type:"text",text:"fixture"}]}]}]);
  expect(frames.at(-1)?.finish).toBeDefined();
 }finally{client.close();await fixture.shutdown();}
});
test("actual SDK reasoning-only finishes with exact signed reasoning and no semantic member",async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario:"reasoning-only"});const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{
  const frames:ProviderStreamEvent[]=[];for await(const frame of client.streamProviderRequest(fixture.request(),fixture.metadata()))frames.push(frame);
  expect(frames.map(frame=>frame.type)).toEqual([ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_THINKING_STARTED,ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_COMPLETE,ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH]);
  expect(frames[1]?.reasoningComplete?.text).toBe("reason-before-text");expect(JSON.parse(frames[1]!.reasoningComplete!.providerMetadataJson)).toEqual({anthropic:{signature:"fixture-signature-text"}});
  expect(fixture.observations()).toMatchObject({activeProviderSources:0,joinedSources:1,nativeIterators:{active:0,joined:1},sdkTotals:{active:0}});
 }finally{client.close();await fixture.shutdown();}
});
test("actual SDK tool-before-text preserves Read then alpha and done followup",async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario:"tool-before-text",followupScenario:"done"});const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{
  const frames:ProviderStreamEvent[]=[];for await(const frame of client.streamProviderRequest(fixture.request(),fixture.metadata()))frames.push(frame);
  expect(frames.map(frame=>frame.type)).toEqual([ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL_COMPLETE,ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE,ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH]);
  expect(frames[0]?.toolCallComplete?.modelToolCallId).toBe("call-read-note");expect(frames[0]?.toolCallComplete?.name).toBe("Read");expect(JSON.parse(frames[0]!.toolCallComplete!.inputJson)).toEqual({file_path:"/workspace/note.txt"});expect(frames[1]?.textComplete?.text).toBe("alpha");
  const followup:ProviderStreamEvent[]=[];for await(const frame of client.streamProviderRequest(fixture.request(),fixture.metadata()))followup.push(frame);expect(followup[0]?.textComplete?.text).toBe("done");expect(followup.at(-1)?.finish).toBeDefined();
 }finally{client.close();await fixture.shutdown();}
});
test("actual SDK preserves Unicode and escapes across segmented single HTTP input",async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario:"unicode-text",fragmentCodeUnits:65536,sourcePackaging:"single-chunk"});const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{
  const frames:ProviderStreamEvent[]=[];for await(const frame of client.streamProviderRequest(fixture.request(),fixture.metadata()))frames.push(frame);
  expect(frames.map(frame=>frame.type)).toEqual([ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE,ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH]);
  expect(frames[0]?.textComplete?.text).toBe(Array.from({length:4096},()=>"☃🙂\n\"\\").join(""));expect(fixture.observations().nativeIterators).toMatchObject({active:0,joined:1});
 }finally{client.close();await fixture.shutdown();}
});
test("actual SDK single HTTP chunk of suppressed empty deltas remains cancellable",async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario:"empty-storm",emptyRecords:100000,sourcePackaging:"single-chunk"});const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{
  const call=client.streamProviderRequest(fixture.request(),fixture.metadata());const frames:ProviderStreamEvent[]=[];call.on("data",frame=>frames.push(frame));
  const terminal=new Promise<number>(resolve=>call.once("status",status=>resolve(status.code)));call.on("error",()=>{});
  for(let poll=0;poll<100&&fixture.observations().nativeIterators.active===0;poll++)await new Promise(resolve=>setTimeout(resolve,5));
  expect(fixture.observations().nativeIterators.active).toBe(1);call.cancel();expect(await terminal).toBe(1);
  for(let poll=0;poll<100&&fixture.observations().nativeIterators.active!==0;poll++)await new Promise(resolve=>setTimeout(resolve,5));
  expect(frames.some(frame=>frame.textComplete!==undefined)).toBe(false);expect(fixture.observations()).toMatchObject({activeProviderSources:0,joinedSources:1,nativeIterators:{active:0,joined:1},sdkTotals:{active:0},assembly:{retainedBytes:0}});
 }finally{client.close();await fixture.shutdown();}
});
