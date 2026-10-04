import { describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { credentials, status } from "@grpc/grpc-js";
import { ProviderGatewayServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderStreamEventType } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { writeProviderStreamEvents } from "../../src/grpc-server.js";
import type { ProviderWriteClock } from "../../src/grpc-server.js";
import { createContentLifecycleGatewayFixture } from "../fixtures/content-lifecycle-gateway.js";
const frame=(sequence:number):ProviderStreamEvent=>({frameSequence:sequence,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE,textComplete:{providerPartId:`text-${sequence}`,eventId:`evt_${sequence.toString().padStart(32,"0")}`,text:"complete"}});
class Writer extends EventEmitter {
 cancelled=false;callbacks:Array<(error?:Error|null)=>void>=[];writes=0;
 write(_frame:ProviderStreamEvent,callback:(error?:Error|null)=>void):boolean{this.callbacks.push(callback);this.writes++;return false;}
}
function clock(){let now=0;let timer:(()=>void)|undefined;const delays:number[]=[];let active=false;return {delays,active:()=>active,setNow:(value:number)=>{now=value;},expire:()=>timer!(),value:{now:()=>now,setTimeout:(callback,delay)=>{timer=callback;delays.push(delay);active=true;return 1 as unknown as ReturnType<typeof setTimeout>;},clearTimeout:()=>{active=false;}} satisfies ProviderWriteClock};}
async function* frames(){yield frame(1);yield frame(2);}
async function wait(predicate:()=>boolean){for(let i=0;i<100&&!predicate();i++)await Promise.resolve();expect(predicate()).toBe(true);}
describe("Absolute complete-frame writer deadline",()=>{
 for(const phase of ["callback","drain"] as const)test(`stalled ${phase} keeps the absolute deadline and releases listeners`,async()=>{
  const call=new Writer(),time=clock();let expired=0,settled:string|undefined;
  const result=writeProviderStreamEvents(call,frames(),{deadline:()=>10,clock:time.value,onDeadline:()=>{expired++;},onWriteSettled:(_frame,outcome)=>{settled=outcome;}}).catch(error=>error);
  await wait(()=>call.writes===1);if(phase==="drain")call.callbacks[0]!();time.setNow(10);time.expire();
  expect(await result).toMatchObject({code:status.DEADLINE_EXCEEDED});expect(expired).toBe(1);expect(settled).toBe(phase==="callback"?undefined:"error");call.emit("close");expect(settled).toBe("error");expect(time.active()).toBe(false);expect(call.listenerCount("drain")).toBe(0);expect(call.listenerCount("close")).toBe(0);
 });
 test("next frame receives remaining allowance rather than a restarted budget",async()=>{
  const call=new Writer(),time=clock();const result=writeProviderStreamEvents(call,frames(),{deadline:()=>10,clock:time.value});
  await wait(()=>call.writes===1);time.setNow(5);call.callbacks[0]!();call.emit("drain");await wait(()=>call.writes===2);
  expect(time.delays).toEqual([10,5]);time.setNow(9);call.callbacks[1]!();call.emit("drain");await result;expect(time.active()).toBe(false);
 });
 test("caller cancellation clears its timer without deadline failure",async()=>{
  const call=new Writer(),time=clock();let expired=false;const result=writeProviderStreamEvents(call,frames(),{deadline:()=>10,clock:time.value,onDeadline:()=>{expired=true;}});
  await wait(()=>call.writes===1);call.cancelled=true;call.emit("cancelled");await result;expect(expired).toBe(false);expect(time.active()).toBe(false);call.emit("close");
 });
 test("deadline abort joins the actual SDK source and clears content ownership",async()=>{
  const fixture=await createContentLifecycleGatewayFixture({scenario:"text",holdFinish:true});const abort=new AbortController(),call=new Writer();
  try{
   const result=writeProviderStreamEvents(call,fixture.app.service.streamProviderRequest(fixture.request({limits:{timeoutMs:80,maxOutputTokens:0}}),fixture.metadata(),{abortSignal:abort.signal}),{deadline:event=>fixture.app.service.providerFrameDeadline(event),onDeadline:()=>abort.abort(),onWriteStarted:event=>fixture.app.service.recordCompleteFrameWriteStarted(event),onWriteSettled:(event,outcome)=>fixture.app.service.recordCompleteFrameWrite(event,outcome)}).catch(error=>error);
   expect(await result).toMatchObject({code:status.DEADLINE_EXCEEDED});expect(call.writes).toBe(1);call.emit("close");
   await fixture.shutdown();const observed=fixture.observations();expect(observed.activeProviderSources).toBe(0);expect(observed.joinedSources).toBe(1);expect(observed.sdkTotals.active).toBe(0);expect(observed.assembly?.retainedBytes).toBe(0);
  }finally{await fixture.shutdown();}
 });
});


test("real grpc deadline terminates its client and joins actual SDK/writer owners",async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario:"text",holdFinish:true,measureResources:true});
 const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try{
  const events:ProviderStreamEvent[]=[];
  let transportError:unknown;
  try{for await(const event of client.streamProviderRequest(fixture.request({limits:{timeoutMs:80,maxOutputTokens:0}}),fixture.metadata()))events.push(event);}catch(error){transportError=error;}
  expect(events.some(event=>event.textComplete?.text==="alpha")).toBe(true);
  expect(events.some(event=>event.finish!==undefined)).toBe(false);
  expect(transportError!==undefined||events.some(event=>event.providerError!==undefined)).toBe(true);
  await fixture.shutdown();const observed=fixture.observations();
  expect(observed.nativeIterators).toMatchObject({active:0,started:1,joined:1});expect(observed.sdkTotals.active).toBe(0);expect(observed.activeProviderSources).toBe(0);
  expect(observed.writer).toMatchObject({pendingBytes:0,pendingCallbacks:0});expect(observed.writer.closed).toBe(1);
 }finally{client.close();await fixture.shutdown();}
},5000);


for(const outcome of ["cancelled","deadline"] as const)test(`real grpc ${outcome} closes custody with a held application callback`,async()=>{
 const fixture=await createContentLifecycleGatewayFixture({scenario:"text",holdFinish:true,measureResources:true,holdFirstWriteCallback:true});
 const client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 const call=client.streamProviderRequest(fixture.request({limits:{timeoutMs:outcome==="deadline"?150:30000,maxOutputTokens:0}}),fixture.metadata());
 const events:ProviderStreamEvent[]=[];
 const completion=(async()=>{try{for await(const event of call)events.push(event);return undefined;}catch(error){return error;}})();
 try{
  for(let i=0;i<100&&fixture.observations().writer.heldApplicationCallbacks!==1;i++)await new Promise(resolve=>setTimeout(resolve,1));
  expect(fixture.observations().writer).toMatchObject({heldApplicationCallbacks:1,writeCalls:1,callbacks:1,pendingCallbacks:0});
  expect(fixture.app.service.metricsText()).not.toContain("providergateway_complete_frame_pending_bytes 0\n");
  if(outcome==="cancelled")call.cancel();
  const failure=await completion;expect(failure).toMatchObject({code:outcome==="cancelled"?status.CANCELLED:status.DEADLINE_EXCEEDED});
  await fixture.shutdown();
  expect(events.some(event=>event.finish!==undefined)).toBe(false);
  expect(fixture.app.service.metricsText()).toContain("providergateway_complete_frame_pending_bytes 0\n");
  const observed=fixture.observations();expect(observed.writer).toMatchObject({heldApplicationCallbacks:0,pendingCallbacks:0,pendingBytes:0,closed:1});
  expect(observed.nativeIterators).toMatchObject({active:0,started:1,joined:1});expect(observed.sdkTotals.active).toBe(0);expect(observed.activeProviderSources).toBe(0);
 }finally{call.cancel();client.close();await fixture.shutdown();}
},5000);
