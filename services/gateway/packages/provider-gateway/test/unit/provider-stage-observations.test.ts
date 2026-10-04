import { describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { Metadata } from "@grpc/grpc-js";
import { NormalizedProviderEventType as FragmentType } from "@tetral/gateway-lowering/src/normalized-stream.js";
import { ProviderFinishReason } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderGatewayServiceShell } from "../../src/service.js";
import { ProviderGatewayMetricsRegistry } from "../../src/metrics.js";
import { writeProviderStreamEvents } from "../../src/grpc-server.js";
import { validProviderRequest } from "./fixtures.js";
const request = () => validProviderRequest({model:{providerId:"anthropic",modelId:"claude-opus-4-8",variant:""}});
const finish = () => ({type:FragmentType.PROVIDER_STREAM_EVENT_TYPE_FINISH,finish:{reason:ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,metadataJson:"{}",usage:undefined}} as const);
const auth = {authenticate:async()=>({ok:true as const,serviceAccount:{namespace:"ns",name:"runtime",podUid:"pod"}})};
class ControlledWriter extends EventEmitter {
  cancelled = false;
  callback: ((error?: Error | null)=>void) | undefined;
  writes = 0;
  write(_event: ProviderStreamEvent, callback: (error?: Error | null)=>void): boolean {
    this.writes++; if (this.writes === 1) {this.callback=callback;return false;} callback();return true;
  }
}
describe("Provider stage completion samples", () => {
  for (const sink of ["recording","throwing","disabled"] as const) test(`independent clock and callback/drain ownership with ${sink} sink`,async()=>{
    let now=0; const samples: Record<string,unknown>[]=[]; const metrics=new ProviderGatewayMetricsRegistry();
    const service=new ProviderGatewayServiceShell({authenticator:auth,runtimeBindingTokenVerifier:{verify:()=>true},ready:()=>true,metrics,observationClock:()=>now,
      logger:{info:record=>{if(sink==="throwing")throw Error("sink unavailable");if(sink==="recording")samples.push(record);},error:()=>{}},
      providerStreamer:{stream:async function*(){now=5;yield {type:FragmentType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,text:{id:"text",text:"",metadataJson:"{}"}};yield {type:FragmentType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,text:{id:"text",text:"alpha",metadataJson:"{}"}};now=20;yield {type:FragmentType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,text:{id:"text",text:"",metadataJson:"{}"}};yield finish();}}
    });
    const writer=new ControlledWriter();
    const operation=writeProviderStreamEvents(writer,service.streamProviderRequest(request(),new Metadata()),{onWriteStarted:frame=>service.recordCompleteFrameWriteStarted(frame),onWriteCallback:frame=>service.recordCompleteFrameWriteCallback(frame),onWriteSettled:(frame,outcome)=>service.recordCompleteFrameWrite(frame,outcome)});
    for(let i=0;i<100 && writer.writes===0;i++)await Promise.resolve();
    expect(writer.writes).toBe(1);expect(metrics.render({ready:true})).not.toContain("providergateway_complete_frame_pending_bytes 0\n");
    now=27;writer.callback!();await Promise.resolve();expect(writer.writes).toBe(1);expect(metrics.render({ready:true})).not.toContain("providergateway_complete_frame_pending_bytes 0\n");
    now=40;writer.emit("drain");await operation;
    expect(metrics.render({ready:true})).toContain("providergateway_complete_frame_pending_bytes 0\n");
    expect(metrics.render({ready:true})).toContain("providergateway_provider_first_fragment_ms_sum 5\n");
    expect(metrics.render({ready:true})).toContain("providergateway_provider_first_complete_ms_sum 15\n");
    expect(metrics.render({ready:true})).toContain("providergateway_complete_frame_write_ms_sum 7\n");
    if(sink==="recording"){
      const stageSamples=samples.filter(record=>record.event==="provider.stage_completed");
      expect(stageSamples.map(record=>[record.stage,record.duration_ms])).toEqual([["complete_frame_write",7],["provider_first_fragment",5],["provider_first_complete",15]]);
      for(const sample of stageSamples)expect(sample).toMatchObject({outcome:"success",kind:"text",bytes_in:7,bytes_out:expect.any(Number)});
      expect(JSON.stringify(stageSamples)).not.toContain("alpha");
    }
    expect(writer.listenerCount("cancelled")).toBe(0);expect(writer.listenerCount("drain")).toBe(0);
  });
  test("Finish controls do not count as fragments and missing stages retain error denominator",async()=>{
    let now=0;const samples:Record<string,unknown>[]=[];
    const service=new ProviderGatewayServiceShell({authenticator:auth,runtimeBindingTokenVerifier:{verify:()=>true},ready:()=>true,observationClock:()=>now,logger:{info:record=>samples.push(record),error:()=>{}},providerStreamer:{stream:async function*(){now=5;yield finish();}}});
    for await(const _frame of service.streamProviderRequest(request(),new Metadata())){}
    expect(samples.filter(record=>record.event==="provider.stage_completed")).toEqual([expect.objectContaining({stage:"provider_first_fragment",outcome:"success",kind:"none",duration_ms:5,bytes_in:0,bytes_out:0}),expect.objectContaining({stage:"provider_first_complete",outcome:"success",kind:"none",duration_ms:5})]);
    samples.length=0;now=0;
    const failed=new ProviderGatewayServiceShell({authenticator:auth,runtimeBindingTokenVerifier:{verify:()=>true},ready:()=>true,observationClock:()=>now,logger:{info:record=>samples.push(record),error:()=>{}},providerStreamer:{stream:async function*(){now=5;throw Error("private provider body");}}});
    for await(const _frame of failed.streamProviderRequest(request(),new Metadata())){}
    expect(samples.filter(record=>record.event==="provider.stage_completed").map(record=>record.outcome)).toEqual(["error","error"]);
    expect(JSON.stringify(samples)).not.toContain("private provider body");
  });
  test("writer cancellation emits closed outcome and clears frame ownership",async()=>{
    let now=0;const samples:Record<string,unknown>[]=[];const metrics=new ProviderGatewayMetricsRegistry();
    const service=new ProviderGatewayServiceShell({authenticator:auth,runtimeBindingTokenVerifier:{verify:()=>true},ready:()=>true,metrics,observationClock:()=>now,logger:{info:record=>samples.push(record),error:()=>{}},providerStreamer:{stream:async function*(){now=5;yield {type:FragmentType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,text:{id:"t",text:"",metadataJson:"{}"}};yield {type:FragmentType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,text:{id:"t",text:"x",metadataJson:"{}"}};now=20;yield {type:FragmentType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,text:{id:"t",text:"",metadataJson:"{}"}};yield finish();}}});
    let joined=false;let custody:Promise<void>|undefined;
    const writer=new ControlledWriter();const operation=writeProviderStreamEvents(writer,service.streamProviderRequest(request(),new Metadata()),{registerWriteCustody:pending=>{custody=pending;void pending.then(()=>{joined=true;});},onWriteStarted:frame=>service.recordCompleteFrameWriteStarted(frame),onWriteSettled:(frame,outcome)=>service.recordCompleteFrameWrite(frame,outcome)});
    for(let i=0;i<100 && writer.writes===0;i++)await Promise.resolve();now=27;writer.cancelled=true;writer.emit("cancelled");await operation;
    // The submitted callback remains held; an outcome alone is not release.
    expect(metrics.render({ready:true})).not.toContain("providergateway_complete_frame_pending_bytes 0\n");
    expect(writer.listenerCount("close")).toBe(1);expect(joined).toBe(false);
    writer.emit("close");await custody;expect(joined).toBe(true);
    const recorded=samples.length;
    writer.callback!(); // Late callback after close cannot re-observe released payload.
    expect(samples.length).toBe(recorded);
    expect(writer.listenerCount("close")).toBe(0);
    expect(samples.filter(record=>record.event==="provider.stage_completed").map(record=>record.outcome)).toEqual(["cancelled","cancelled","cancelled"]);
    expect(metrics.render({ready:true})).toContain("providergateway_complete_frame_pending_bytes 0\n");
  });
});
