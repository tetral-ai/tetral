import { describe, expect, test } from "bun:test";
import type { TextStreamPart, ToolSet } from "ai";
import { createProviderSdkRetentionGuard, ProviderSdkRetentionLimitError } from "../../src/providers/sdk-retention-guard.js";
import { createContentLifecycleGatewayFixture } from "../fixtures/content-lifecycle-gateway.js";
import { credentials } from "@grpc/grpc-js";
import { ProviderGatewayServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderAssemblyCalibrationCandidate } from "../../src/providers/resource-policy.js";
describe("SDK pre-retention guard",()=>{
  test("charges empty and metadata/control records before forwarding, with an independent count oracle",async()=>{
    const records:TextStreamPart<ToolSet>[]=[{type:"text-start",id:"t"},{type:"text-delta",id:"t",text:""},{type:"text-end",id:"t",providerMetadata:{anthropic:{signature:"sig"}}}];
    let aborted=false,stopped=false;const guard=createProviderSdkRetentionGuard({bounds:{maxRecords:2,maxSerializedPayloadBytes:10000},abort:()=>{aborted=true;}});
    const received:TextStreamPart<ToolSet>[]=[];
    try{for await(const record of recordStream(records).pipeThrough(guard.transform({tools:{},stopStream:()=>{stopped=true;}})))received.push(record);}catch(error){expect(error).toBeInstanceOf(ProviderSdkRetentionLimitError);}
    expect(received).toHaveLength(2);expect(guard.resources()).toMatchObject({sourceRecords:3,forwardedRecords:2,serializedPayloadBytes:Buffer.byteLength(JSON.stringify(records[0]))+Buffer.byteLength(JSON.stringify(records[1]))});expect(aborted).toBe(true);expect(stopped).toBe(true);guard.release();expect(guard.resources().active).toBe(false);
  });
  test("serialized byte cap includes escaped metadata and rejects before forwarding",async()=>{
    const record={type:"text-start",id:"t",providerMetadata:{anthropic:{signature:'"\\\n'}}} as const;const exact=Buffer.byteLength(JSON.stringify(record));
    for(const maximum of [exact-1,exact]){
      let aborted=false;const guard=createProviderSdkRetentionGuard({bounds:{maxRecords:4,maxSerializedPayloadBytes:maximum},abort:()=>{aborted=true;},observe:()=>{throw Error("sink");}});let accepted=0;
      try{for await(const _record of recordStream([record]).pipeThrough(guard.transform({tools:{},stopStream:()=>{}})))accepted++;}catch(error){expect(error).toBeInstanceOf(ProviderSdkRetentionLimitError);}
      expect(accepted).toBe(maximum===exact?1:0);expect(aborted).toBe(maximum<exact);expect(guard.resources().serializedPayloadBytes).toBe(maximum===exact?exact:0);
    }
  });
  test("actual SDK empty block controls reach the guard and cancel/join the source",async()=>{
    const fixture=await createContentLifecycleGatewayFixture({scenario:"empty-blocks-storm",emptyRecords:20,sdkBounds:{maxRecords:8,maxSerializedPayloadBytes:10000}});
    const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
    try{
      const events=[];for await(const event of client.streamProviderRequest(fixture.request(),fixture.metadata()))events.push(event);
      expect(events).toHaveLength(1);expect(events[0]?.providerError?.error).toMatchObject({code:"provider_stream_limit_exceeded",retryable:false,fatal:true});
      await fixture.shutdown();const resources=fixture.observations();expect(resources.sdkTotals).toMatchObject({sourceRecords:9,forwardedRecords:8,active:0});expect(resources.activeProviderSources).toBe(0);expect(resources.joinedSources).toBe(1);expect(resources.sourceCancellations).toBeGreaterThan(0);expect(resources.assembly?.retainedBytes).toBe(0);
    }finally{client.close();await fixture.shutdown();}
  });
});

function recordStream<T>(records:readonly T[]):ReadableStream<T>{let index=0;return new ReadableStream<T>({pull(controller){if(index===records.length)controller.close();else controller.enqueue(records[index++]!);}});}

for(const whitespace of [26,33]) test(`actual SDK streamed tool argument budget whitespace=${whitespace}`,async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario:"tool-whitespace",textCodeUnits:whitespace,fragmentCodeUnits:4,assemblyBounds:{...ProviderAssemblyCalibrationCandidate,maxCumulativeContentBytes:32}});
 const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{
  const events=[];for await(const event of client.streamProviderRequest(fixture.request(),fixture.metadata()))events.push(event);
  if(whitespace===26){expect(events).toHaveLength(2);expect(events[0]?.toolCallComplete?.inputJson).toBe("{}");expect(events[1]?.finish).toBeDefined();}
  else{expect(events).toHaveLength(1);expect(events[0]?.providerError?.error).toMatchObject({code:"provider_stream_limit_exceeded",retryable:false,fatal:true});}
  await fixture.shutdown();const state=fixture.observations();expect(state.nativeIterators).toMatchObject({active:0,started:1,joined:1});expect(state.sdkTotals.active).toBe(0);expect(state.activeProviderSources).toBe(0);expect(state.assemblyTotals.cumulativeContentBytes).toBe(0);
 }finally{client.close();await fixture.shutdown();}
});
