import { describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { Metadata } from "@grpc/grpc-js";
import { diagnosticDefaults } from "@tetral/ts-observability";
import { NormalizedProviderEventType as FragmentType } from "@tetral/gateway-lowering/src/normalized-stream.js";
import type { NormalizedProviderEvent, NormalizedTextEventType } from "@tetral/gateway-lowering/src/normalized-stream.js";
import { ProviderFinishReason } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderStreamEvent, RequestUsage } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderGatewayServiceShell } from "../../src/service.js";
import type { ProviderRequestStreamer } from "../../src/service.js";
import { ProviderGatewayMetricsRegistry } from "../../src/metrics.js";
import { createJsonLogger } from "../../src/logger.js";
import type { GatewayLogger } from "../../src/logger.js";
import { writeProviderStreamEvents } from "../../src/grpc-server.js";
import { validProviderRequest } from "./fixtures.js";
const request = () => validProviderRequest({model:{providerId:"anthropic",modelId:"claude-opus-4-8",variant:""}});
const finish = (usage?: RequestUsage) => ({type:FragmentType.Finish,finish:{reason:ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,metadataJson:"{}",usage}} as const);
const text = (type: NormalizedTextEventType, value = ""): NormalizedProviderEvent => ({type,text:{id:"text",text:value,metadataJson:"{}"}});
const auth = {authenticate:async()=>({ok:true as const,serviceAccount:{namespace:"ns",name:"runtime",podUid:"pod"}})};
const series = 'provider="anthropic",model="claude-opus-4-8"';
const stageSeries = (stage: string, outcome: string) => `service="provider-gateway",operation="${stage}",outcome="${outcome}"`;
function shell(options: { readonly metrics: ProviderGatewayMetricsRegistry; readonly clock: () => number; readonly providerStreamer: ProviderRequestStreamer; readonly logger?: GatewayLogger }): ProviderGatewayServiceShell {
  return new ProviderGatewayServiceShell({authenticator:auth,runtimeBindingTokenVerifier:{verify:()=>true},ready:()=>true,metrics:options.metrics,observationClock:options.clock,
    logger:options.logger ?? {info:()=>undefined,error:()=>undefined},providerStreamer:options.providerStreamer});
}
async function drain(stream: AsyncIterable<ProviderStreamEvent>): Promise<void> { for await (const _frame of stream) { /* drained */ } }
class ControlledWriter extends EventEmitter {
  cancelled = false;
  callback: ((error?: Error | null)=>void) | undefined;
  writes = 0;
  write(_event: ProviderStreamEvent, callback: (error?: Error | null)=>void): boolean {
    this.writes++; if (this.writes === 1) {this.callback=callback;return false;} callback();return true;
  }
}
describe("Provider stage durations", () => {
  for (const sink of ["recording","throwing","disabled"] as const) test(`independent clock and callback/drain ownership with ${sink} sink`,async()=>{
    let now=0; const records: Record<string,unknown>[]=[]; const metrics=new ProviderGatewayMetricsRegistry();
    const service=shell({metrics,clock:()=>now,
      logger:{info:record=>{if(sink==="throwing")throw Error("sink unavailable");if(sink==="recording")records.push(record);},error:()=>{}},
      providerStreamer:{stream:async function*(){now=5;yield text(FragmentType.TextStart);yield text(FragmentType.TextDelta,"alpha");now=20;yield text(FragmentType.TextEnd);yield finish();}}
    });
    const writer=new ControlledWriter();
    const operation=writeProviderStreamEvents(writer,service.streamProviderRequest(request(),new Metadata()),{onWriteStarted:frame=>service.recordCompleteFrameWriteStarted(frame),onWriteCallback:frame=>service.recordCompleteFrameWriteCallback(frame),onWriteSettled:(frame,outcome)=>service.recordCompleteFrameWrite(frame,outcome)});
    for(let i=0;i<100 && writer.writes===0;i++)await Promise.resolve();
    expect(writer.writes).toBe(1);expect(metrics.render({ready:true})).not.toContain("providergateway_complete_frame_pending_bytes 0\n");
    now=27;writer.callback!();await Promise.resolve();expect(writer.writes).toBe(1);expect(metrics.render({ready:true})).not.toContain("providergateway_complete_frame_pending_bytes 0\n");
    now=40;writer.emit("drain");await operation;
    expect(metrics.render({ready:true})).toContain("providergateway_complete_frame_pending_bytes 0\n");
    // The 5/15/7 ms stage durations come from independent clocks, so each
    // stage's histogram sum holds its own duration.
    const histogram=metrics.render({ready:true});
    for(const [family,stage,seconds] of [["tetral_long_operation_duration_seconds","provider_first_fragment","0.005"],["tetral_long_operation_duration_seconds","provider_first_complete","0.015"],["tetral_operation_duration_seconds","complete_frame_write","0.007"]] as const){
      expect(histogram).toContain(`${family}_count{${stageSeries(stage,"ok")}} 1\n`);
      expect(histogram).toContain(`${family}_sum{${stageSeries(stage,"ok")}} ${seconds}\n`);
    }
    // A stage writes no record of its own: the turn's summary is the only one.
    if(sink==="recording")expect(records.map(record=>record.event)).toEqual(["provider_request_streamed"]);
    expect(writer.listenerCount("cancelled")).toBe(0);expect(writer.listenerCount("drain")).toBe(0);
  });
  test("Finish controls do not count as fragments and missing stages retain error denominator",async()=>{
    let now=0;const metrics=new ProviderGatewayMetricsRegistry();
    await drain(shell({metrics,clock:()=>now,providerStreamer:{stream:async function*(){now=5;yield finish();}}}).streamProviderRequest(request(),new Metadata()));
    const finished=metrics.render({ready:true});
    expect(finished).toContain(`tetral_long_operation_duration_seconds_sum{${stageSeries("provider_first_fragment","ok")}} 0.005\n`);
    expect(finished).toContain(`tetral_long_operation_duration_seconds_sum{${stageSeries("provider_first_complete","ok")}} 0.005\n`);
    expect(finished).not.toContain("providergateway_time_to_first_token_seconds_count");
    now=0;const failedMetrics=new ProviderGatewayMetricsRegistry();
    await drain(shell({metrics:failedMetrics,clock:()=>now,providerStreamer:{stream:async function*(){now=5;throw Error("private provider body");}}}).streamProviderRequest(request(),new Metadata()));
    const failed=failedMetrics.render({ready:true});
    for(const stage of ["provider_first_fragment","provider_first_complete"])expect(failed).toContain(`tetral_long_operation_duration_seconds_count{${stageSeries(stage,"error")}} 1\n`);
    expect(failed).toContain(`providergateway_stream_duration_seconds_count{${series},outcome="error"} 1\n`);
    expect(failed).not.toContain("private provider body");
  });
  test("writer cancellation emits closed outcome and clears frame ownership",async()=>{
    let now=0;const metrics=new ProviderGatewayMetricsRegistry();
    const service=shell({metrics,clock:()=>now,providerStreamer:{stream:async function*(){now=5;yield text(FragmentType.TextStart);yield text(FragmentType.TextDelta,"x");now=20;yield text(FragmentType.TextEnd);yield finish();}}});
    let joined=false;let custody:Promise<void>|undefined;
    const writer=new ControlledWriter();const operation=writeProviderStreamEvents(writer,service.streamProviderRequest(request(),new Metadata()),{registerWriteCustody:pending=>{custody=pending;void pending.then(()=>{joined=true;});},onWriteStarted:frame=>service.recordCompleteFrameWriteStarted(frame),onWriteSettled:(frame,outcome)=>service.recordCompleteFrameWrite(frame,outcome)});
    for(let i=0;i<100 && writer.writes===0;i++)await Promise.resolve();now=27;writer.cancelled=true;writer.emit("cancelled");await operation;
    // The submitted callback remains held; an outcome alone is not release.
    expect(metrics.render({ready:true})).not.toContain("providergateway_complete_frame_pending_bytes 0\n");
    expect(writer.listenerCount("close")).toBe(1);expect(joined).toBe(false);
    writer.emit("close");await custody;expect(joined).toBe(true);
    writer.callback!(); // Late callback after close cannot re-observe released payload.
    expect(writer.listenerCount("close")).toBe(0);
    const histogram=metrics.render({ready:true});
    for(const [family,stage] of [["tetral_long_operation_duration_seconds","provider_first_fragment"],["tetral_long_operation_duration_seconds","provider_first_complete"],["tetral_operation_duration_seconds","complete_frame_write"]] as const)
      expect(histogram).toContain(`${family}_count{${stageSeries(stage,"cancelled")}} 1\n`);
    expect(histogram).not.toContain(`operation="complete_frame_write",outcome="ok"`);
    expect(histogram).toContain("providergateway_complete_frame_pending_bytes 0\n");
  });
});

describe("Provider model speed", () => {
  const usage = (outputTotalTokens: number): RequestUsage => ({inputTotalTokens:40,inputUncachedTokens:30,inputCacheReadTokens:10,outputTotalTokens,providerUsageJson:"{}"});
  for (const [name, reported] of [["reported usage observes its output rate and tokens", usage(100)], ["a stream without reported usage observes no rate", undefined]] as const) test(name, async () => {
    let now=0;const metrics=new ProviderGatewayMetricsRegistry();
    await drain(shell({metrics,clock:()=>now,providerStreamer:{stream:async function*(){
      now=800;yield text(FragmentType.TextStart);yield text(FragmentType.TextDelta,"first");
      now=2800;yield text(FragmentType.TextDelta,"last");yield text(FragmentType.TextEnd);
      now=3000;yield finish(reported);
    }}}).streamProviderRequest(request(),new Metadata()));
    const body=metrics.render({ready:true});
    expect(body).toContain(`providergateway_time_to_first_token_seconds_count{${series},first_kind="text"} 1\n`);
    expect(body).toContain(`providergateway_time_to_first_token_seconds_sum{${series},first_kind="text"} 0.8\n`);
    expect(body).toContain(`providergateway_stream_duration_seconds_count{${series},outcome="ok"} 1\n`);
    expect(body).toContain(`providergateway_stream_duration_seconds_sum{${series},outcome="ok"} 3\n`);
    if (reported === undefined) {
      expect(body).not.toContain("providergateway_output_tokens_per_second_count");
      expect(body).not.toContain("providergateway_tokens_total{");
      return;
    }
    expect(body).toContain(`providergateway_output_tokens_per_second_count{${series}} 1\n`);
    expect(body).toContain(`providergateway_output_tokens_per_second_sum{${series}} 50\n`);
    expect(body).toContain(`providergateway_output_tokens_per_second_bucket{${series},le="40"} 0\n`);
    expect(body).toContain(`providergateway_output_tokens_per_second_bucket{${series},le="80"} 1\n`);
    expect(body).toContain(`providergateway_tokens_total{${series},type="output"} 100\n`);
    expect(body).toContain(`providergateway_tokens_total{${series},type="input"} 40\n`);
    expect(body).toContain(`providergateway_tokens_total{${series},type="cached"} 10\n`);
  });
  test("1,000 fragments add no record", async () => {
    const lines: string[] = [];
    const logger = createJsonLogger({write:line=>lines.push(line),diagnostics:{...diagnosticDefaults,level:"debug"}});
    try {
      await drain(shell({metrics:new ProviderGatewayMetricsRegistry(),clock:()=>performance.now(),logger,providerStreamer:{stream:async function*(){
        yield text(FragmentType.TextStart);
        for (let fragment = 0; fragment < 1_000; fragment++) yield text(FragmentType.TextDelta, "x");
        yield text(FragmentType.TextEnd);yield finish(usage(1_000));
      }}}).streamProviderRequest(request(),new Metadata()));
      expect(lines.map(line=>JSON.parse(line).event)).toEqual(["provider_request_streamed"]);
    } finally { logger.close(); }
  });
  test("a provider's 429 counts rate_limited and its 503 or 529 overloaded", async () => {
    const metrics=new ProviderGatewayMetricsRegistry();
    for (const statusCode of [429, 503, 529, 500]) {
      await drain(shell({metrics,clock:()=>performance.now(),providerStreamer:{stream:async function*(){
        throw Object.assign(new Error("provider refused the request"), {statusCode});
      }}}).streamProviderRequest(request(),new Metadata()));
    }
    const body=metrics.render({ready:true});
    expect(body).toContain(`providergateway_provider_rejected_total{provider="anthropic",cause="rate_limited"} 1\n`);
    expect(body).toContain(`providergateway_provider_rejected_total{provider="anthropic",cause="overloaded"} 2\n`);
    expect(body.split("\n").filter(line=>line.startsWith("providergateway_provider_rejected_total{"))).toHaveLength(2);
  });
});
