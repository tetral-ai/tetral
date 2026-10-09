import { expect,test } from "bun:test";
import { createHash } from "node:crypto";
import { credentials,status } from "@grpc/grpc-js";
import { ProviderGatewayServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest,ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createContentLifecycleGatewayFixture,type ContentLifecycleGatewayFixtureOptions } from "../fixtures/content-lifecycle-gateway.js";

// Each variant sends the original large payloads through the pinned SDK adapter,
// assembler and gRPC writer over real gRPC. The 300 s harness budget bounds the
// case and its request deadline; production watchdogs and payload bounds are
// unchanged. Expected digests come from an independent byte generator.
const caseBudgetMs=300_000;
const eightMiB=8*1024*1024,nearSixteenMiB=16*1024*1024-2;
// Mirrors the Gateway's 32 MiB encoded response-frame bound so a near-16 MiB complete text frame can be received.
const responseFrameBytes=32*1024*1024;
type Fixture=Awaited<ReturnType<typeof createContentLifecycleGatewayFixture>>;
type Observation=ReturnType<Fixture["observations"]>;

const digest=(bytes:Uint8Array|string):string=>createHash("sha256").update(bytes).digest("hex");

async function withRetentionGateway(options:ContentLifecycleGatewayFixtureOptions,run:(fixture:Fixture,client:ProviderGatewayServiceClient,request:ProviderRequest)=>Promise<void>):Promise<void> {
 const fixture=await createContentLifecycleGatewayFixture({...options,measureResources:true});
 const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure(),{"grpc.max_receive_message_length":responseFrameBytes});
 try{await run(fixture,client,fixture.request({limits:{...fixture.request().limits!,timeoutMs:caseBudgetMs}}));}finally{client.close();await fixture.shutdown();}
}

async function receiveBlocks(frames:AsyncIterable<ProviderStreamEvent>):Promise<string[]> {
 const blocks:string[]=[];let finished=false;
 for await(const frame of frames){
  expect(frame.providerError).toBeUndefined();
  expect(finished).toBe(false);
  if(frame.textComplete!==undefined)blocks.push(frame.textComplete.text);
  finished=frame.finish!==undefined;
 }
 expect(finished).toBe(true);
 return blocks;
}

function expectBlocks(blocks:readonly string[],count:number,size:number):void {
 const expected=digest(new Uint8Array(size).fill(0x78));
 expect(blocks.map(block=>({bytes:Buffer.byteLength(block),digest:digest(block)}))).toEqual(Array.from({length:count},()=>({bytes:size,digest:expected})));
}

async function waitObservation(fixture:Fixture,label:string,reached:(observed:Observation)=>boolean,timeoutMs=caseBudgetMs):Promise<Observation> {
 const deadline=performance.now()+timeoutMs;
 for(;;){
  const observed=fixture.observations();
  if(reached(observed))return observed;
  if(performance.now()>deadline)throw new Error(`${label} was not observed: ${JSON.stringify({assemblyTotals:observed.assemblyTotals,writer:observed.writer,nativeIterators:observed.nativeIterators,activeProviderSources:observed.activeProviderSources})}`);
  await new Promise(resolve=>setTimeout(resolve,10));
 }
}

// Waits for zero assembler state, SDK readers, writer custody and open provider
// sources, then requires that the variant's single provider call was joined.
async function expectReleased(fixture:Fixture,cancelled:boolean):Promise<void> {
 const observed=await waitObservation(fixture,"released Gateway owners",value=>Object.keys(value.assemblyTotals).length===5&&Object.values(value.assemblyTotals).every(total=>total===0)&&value.activeProviderSources===0&&value.nativeIterators.active===0&&value.sdkTotals.active===0&&value.writer.pendingCallbacks===0&&value.writer.pendingBytes===0&&value.writer.heldApplicationCallbacks===0,30_000);
 expect({providerCalls:observed.providerCalls,joinedSources:observed.joinedSources,started:observed.nativeIterators.started,joined:observed.nativeIterators.joined}).toEqual({providerCalls:1,joinedSources:1,started:1,joined:1});
 expect(observed.writer.writeCalls).toBeGreaterThanOrEqual(1);
 if(cancelled){expect(observed.nativeIterators.cancelledJoins).toBe(1);expect(observed.sourceCancellations).toBeGreaterThanOrEqual(1);}
 else expect(observed.writer.callbacks).toBe(observed.writer.writeCalls);
}

test("8 MiB one-character fragments complete byte-exact and release owners",async()=>{
 // Chunking only groups SSE records on the wire; every one-character text delta still reaches the SDK as its own record.
 await withRetentionGateway({scenario:"text-large",textCodeUnits:eightMiB,fragmentCodeUnits:1,sourceRecordsPerChunk:4096},async(fixture,client,request)=>{
  expectBlocks(await receiveBlocks(client.streamProviderRequest(request,fixture.metadata())),1,eightMiB);
  expect(fixture.observations().sourceRecords).toBe(eightMiB+5);
  expect(fixture.observations().sdk?.sourceRecords).toBeGreaterThanOrEqual(eightMiB);
  await expectReleased(fixture,false);
 });
},caseBudgetMs);

test("three near-16 MiB blocks complete byte-exact and release owners",async()=>{
 await withRetentionGateway({scenario:"multiple-large",textCodeUnits:nearSixteenMiB,blockCount:3},async(fixture,client,request)=>{
  expectBlocks(await receiveBlocks(client.streamProviderRequest(request,fixture.metadata())),3,nearSixteenMiB);
  await expectReleased(fixture,false);
 });
},caseBudgetMs);

test("cancel with an 8 MiB block held in writer custody joins the provider source",async()=>{
 await withRetentionGateway({scenario:"text-large",textCodeUnits:eightMiB,holdFinish:true,holdFirstWriteCallback:true},async(fixture,client,request)=>{
  const call=client.streamProviderRequest(request,fixture.metadata());
  const frames:ProviderStreamEvent[]=[];
  const completion=(async()=>{try{for await(const frame of call)frames.push(frame);return undefined;}catch(error){return error;}})();
  try{
   await waitObservation(fixture,"8 MiB block held in writer custody with its provider source open",value=>value.writer.heldApplicationCallbacks===1&&value.activeProviderSources===1);
   call.cancel();
   expect(await completion).toMatchObject({code:status.CANCELLED});
   expect(frames.some(frame=>frame.finish!==undefined)).toBe(false);
   await expectReleased(fixture,true);
  }finally{call.cancel();}
 });
},caseBudgetMs);

test("a reader that starts after the 8 MiB block reached the writer receives exact bytes",async()=>{
 await withRetentionGateway({scenario:"text-large",textCodeUnits:eightMiB},async(fixture,client,request)=>{
  const call=client.streamProviderRequest(request,fixture.metadata());
  try{
   await waitObservation(fixture,"8 MiB block submitted before the reader reads",value=>value.writer.writeCalls>=1);
   expectBlocks(await receiveBlocks(call),1,eightMiB);
   await expectReleased(fixture,false);
  }finally{call.cancel();}
 });
},caseBudgetMs);
