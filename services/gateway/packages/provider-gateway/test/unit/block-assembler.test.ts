import { NormalizedProviderEventType as FragmentType } from "@tetral/gateway-lowering/src/normalized-stream.js";
import type { NormalizedProviderEvent } from "@tetral/gateway-lowering/src/normalized-stream.js";
import { describe, expect, test } from "bun:test";
import { ProviderBlockAssembler, ProviderAssemblyLimitError, ProviderIncompleteStreamError } from "../../src/providers/block-assembler.js";
import type { ProviderAssemblyBounds, ProviderPreviewOffer } from "../../src/providers/block-assembler.js";
import { DefaultProviderAssemblyBounds } from "../../src/providers/resource-policy.js";
import { ProviderStreamRaiser } from "@tetral/gateway-lowering/src/stream.js";
import type { GatewayStreamPart } from "@tetral/gateway-lowering/src/stream.js";
import { ProviderRequestKind, ProviderThreadRole, ProviderThreadVisibility, ProviderStreamEventType } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
const limits: ProviderAssemblyBounds = { maxRetainedBytes:64*1024*1024,maxOpenBlocks:64,maxIdentities:4096,maxSegments:8192,coalesceCodeUnits:8192 };
function setup(options: { bounds?:Partial<ProviderAssemblyBounds>; previews?: Parameters<ProviderPreviewOffer>[0][]; role?:ProviderThreadRole; visibility?:ProviderThreadVisibility; kind?:ProviderRequestKind } = {}) {
 let ids = 0;
 const assembler = new ProviderBlockAssembler({ bounds:{...limits,...options.bounds},request:{requestKind:options.kind??ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,threadRole:options.role??ProviderThreadRole.PROVIDER_THREAD_ROLE_MAIN,threadVisibility:options.visibility??ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_PUBLIC},allocateEventId:()=>`evt_${(++ids).toString(16).padStart(32,"0")}`,offerPreview:preview=>options.previews?.push(preview) });
 const raiser = new ProviderStreamRaiser({usageWireFamily:"openai-wire",modelLimits:{contextWindowTokens:200000,outputTokenLimit:10000}});
 const send = (...parts:GatewayStreamPart[]) => parts.flatMap(part=>raiser.map(part).flatMap(event=>assembler.accept(event)));
 return {assembler,send,ids:()=>ids};
}
describe("Gateway complete block ownership",()=>{
 test("production assembly defaults keep their documented values",()=>{
  expect(DefaultProviderAssemblyBounds).toEqual({maxRetainedBytes:33554432,maxOpenBlocks:64,maxIdentities:4096,maxSegments:8192,coalesceCodeUnits:8192});
 });
 test("completed blocks do not consume the next block's live-content budget",()=>{
  // This byte-boundary fixture checks the assembly contract, not model capacity.
  const {assembler,send}=setup({bounds:DefaultProviderAssemblyBounds});
  const textBytes=16*1024*1024-2;
  for(let index=0;index<3;index++) {
   const id=`part-${index}`,text=String.fromCharCode(65+index).repeat(textBytes);
   send({type:"text-start",id},{type:"text-delta",id,delta:text});
   const frames=send({type:"text-end",id});
   expect(frames).toHaveLength(1);
   expect(frames[0]?.frameSequence).toBe(index+1);
   expect(frames[0]?.textComplete?.providerPartId).toBe(id);
   expect(frames[0]?.textComplete?.text).toBe(text);
   expect(assembler.resources).toMatchObject({retainedBytes:0,segments:0,openBlocks:0});
  }
  expect(send({type:"finish",finishReason:"stop"})).toHaveLength(1);
  assembler.assertComplete();
  expect(assembler.resources.cumulativeContentBytes).toBe(3*textBytes);
 });
 test("interleaves blocks, assigns IDs on nonempty text, suppresses empty text",()=>{
  const {assembler,send,ids}=setup(); expect(send({type:"text-start",id:"empty"},{type:"text-end",id:"empty"})).toEqual([]); expect(ids()).toBe(0);
  send({type:"text-start",id:"a"},{type:"text-start",id:"b"},{type:"text-delta",id:"a",delta:"A"},{type:"text-delta",id:"b",delta:"B"});
  const frames=send({type:"text-end",id:"b"},{type:"text-end",id:"a"},{type:"finish",finishReason:"stop"}); expect(frames.map(f=>f.frameSequence)).toEqual([1,2,3]);
  expect(frames[0]?.textComplete).toEqual({providerPartId:"b",eventId:"evt_00000000000000000000000000000002",text:"B"}); expect(frames[1]?.textComplete?.text).toBe("A"); assembler.assertComplete(); expect(assembler.resources.retainedBytes).toBe(0);
 });
 test("thinking is bodyfree; final signature merges two metadata levels",()=>{
  const previews:Parameters<ProviderPreviewOffer>[0][]=[]; const {send}=setup({previews}); const start=send({type:"reasoning-start",id:"r",metadata:{anthropic:{other:"kept"}}});
  expect(start[0]).toEqual({frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_THINKING_STARTED,thinkingStarted:{providerPartId:"r",eventId:"evt_00000000000000000000000000000001"}});
  const frames=send({type:"reasoning-delta",id:"r",delta:"private body",metadata:{anthropic:{tag:"new"}}},{type:"reasoning-end",id:"r",metadata:{anthropic:{signature:"signed"}}});
  expect(JSON.parse(frames[0]!.reasoningComplete!.providerMetadataJson)).toEqual({anthropic:{other:"kept",tag:"new",signature:"signed"}}); expect(JSON.stringify(previews)).not.toContain("private body"); expect(JSON.stringify(previews)).not.toContain("signed");
 });
 test("split surrogate and escaped text use canonical JSON accounting",()=>{
  const {send}=setup(); send({type:"text-start",id:"t"},{type:"text-delta",id:"t",delta:"\ud83d"},{type:"text-delta",id:"t",delta:'\ude00\n"\\\u0001'}); expect(send({type:"text-end",id:"t"})[0]?.textComplete?.text).toBe('😀\n"\\\u0001');
  const bad=setup(); bad.send({type:"text-start",id:"t"},{type:"text-delta",id:"t",delta:"\ud83d"}); expect(()=>bad.send({type:"text-end",id:"t"})).toThrow("Unicode scalar");
 });
 test("coalesces short fragments and releases live segments",()=>{
  const {assembler,send}=setup({bounds:{coalesceCodeUnits:8,maxSegments:2}}); send({type:"text-start",id:"t"}); for(let i=0;i<16;i++) send({type:"text-delta",id:"t",delta:"a"}); expect(assembler.resources.segments).toBe(2); expect(send({type:"text-end",id:"t"})[0]?.textComplete?.text).toBe("a".repeat(16)); expect(assembler.resources.segments).toBe(0);
  send({type:"text-start",id:"u"}); for(let i=0;i<16;i++) send({type:"text-delta",id:"u",delta:"b"}); expect(()=>send({type:"text-delta",id:"u",delta:"c"})).toThrow(ProviderAssemblyLimitError);
 });
 test("streamed tool input stays charged once until its complete call and completion needs closed lifecycle",()=>{
  // 100000 streamed argument bytes plus the 2-byte empty metadata object.
  const {assembler,send}=setup(); send({type:"tool-input-start",id:"call",name:"Search"},{type:"tool-input-delta",id:"call",delta:"x".repeat(100000)}); expect(assembler.resources.retainedBytes).toBe(100002); expect(()=>send({type:"tool-call",id:"call",name:"Search",input:{q:"x"}})).toThrow(ProviderIncompleteStreamError);
  send({type:"tool-input-end",id:"call"}); expect(send({type:"tool-call",id:"call",name:"Search",input:{q:"x"}})[0]?.toolCallComplete).toEqual({modelToolCallId:"call",name:"Search",inputJson:'{"q":"x"}',providerMetadataJson:"{}"}); expect(assembler.resources.retainedBytes).toBe(0); expect(()=>send({type:"tool-call",id:"call",name:"Search",input:{q:"x"}})).toThrow(ProviderIncompleteStreamError);
 });
 test("closed identities stay unique and finish cannot hide open content",()=>{
  const {send}=setup(); send({type:"text-start",id:"t"},{type:"text-end",id:"t"}); expect(()=>send({type:"text-start",id:"t"})).toThrow(ProviderIncompleteStreamError); const other=setup(); other.send({type:"text-start",id:"open"}); expect(()=>other.send({type:"finish",finishReason:"stop"})).toThrow(ProviderIncompleteStreamError);
 });
 test("reasoning count stays charged after handoff",()=>{
  const {assembler,send}=setup(); for(let i=0;i<16;i++) send({type:"reasoning-start",id:`r${i}`},{type:"reasoning-end",id:`r${i}`}); expect(assembler.resources.retainedBytes).toBe(0); expect(()=>send({type:"reasoning-start",id:"extra"})).toThrow(ProviderAssemblyLimitError);
 });
 test("reasoning byte budget stays charged after handoff",()=>{
  const {send}=setup(); send({type:"reasoning-start",id:"r"},{type:"reasoning-delta",id:"r",delta:"a".repeat(2*1024*1024-2)},{type:"reasoning-end",id:"r"}); expect(()=>send({type:"reasoning-start",id:"extra"})).toThrow(ProviderAssemblyLimitError);
 });
 test("merged metadata stays bounded",()=>{
  const {send}=setup(); send({type:"reasoning-start",id:"r",metadata:{anthropic:{a:"a".repeat(10000)}}}); expect(()=>send({type:"reasoning-end",id:"r",metadata:{anthropic:{signature:"s".repeat(10000)}}})).toThrow(ProviderAssemblyLimitError);
 });
 for(const bounds of [{maxRetainedBytes:2},{maxOpenBlocks:1},{maxIdentities:1}]) test(`explicit cap ${Object.keys(bounds)[0]}`,()=>{
  const {send}=setup({bounds}); send({type:"text-start",id:"t"}); expect(()=>Object.keys(bounds)[0]!.includes("Bytes") ? send({type:"text-delta",id:"t",delta:"abc"}) : send({type:"text-start",id:"u"})).toThrow(ProviderAssemblyLimitError);
 });
 for(const options of [{role:ProviderThreadRole.PROVIDER_THREAD_ROLE_SUBAGENT},{role:ProviderThreadRole.PROVIDER_THREAD_ROLE_APPROVAL_REVIEWER},{role:ProviderThreadRole.PROVIDER_THREAD_ROLE_UNSPECIFIED},{visibility:ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_INTERNAL},{visibility:ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_UNSPECIFIED},{kind:ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY}]) test(`preview admission ${JSON.stringify(options)}`,()=>{
  const previews:Parameters<ProviderPreviewOffer>[0][]=[]; const {send}=setup({...options,previews}); send({type:"text-start",id:"t"},{type:"text-delta",id:"t",delta:"body"},{type:"text-end",id:"t"},{type:"reasoning-start",id:"r"},{type:"reasoning-end",id:"r"}); expect(previews).toEqual([]);
 });
 test("EOF requires terminal and cleanup clears resources",()=>{
  const {assembler,send}=setup(); send({type:"text-start",id:"t"},{type:"text-delta",id:"t",delta:"retained"}); expect(()=>assembler.assertComplete()).toThrow(ProviderIncompleteStreamError); assembler.release(); expect(assembler.resources).toEqual({retainedBytes:0,cumulativeContentBytes:0,segments:0,openBlocks:0,identities:0});
 });
});

describe("Gateway private fragment lifecycle rejection", () => {
  function raw(kind: "text" | "reasoning" | "toolInput", phase: "START" | "DELTA" | "END", name = "Read"): NormalizedProviderEvent {
    const prefix = kind === "toolInput" ? "ToolInput" : kind === "text" ? "Text" : "Reasoning";
    const type = FragmentType[`${prefix}${phase[0]}${phase.slice(1).toLowerCase()}` as keyof typeof FragmentType];
    return {type,[kind]:{id:"part",text:phase === "DELTA" ? "x" : "",metadataJson:"{}",...(kind === "toolInput" ? {name} : {})}} as NormalizedProviderEvent;
  }
  for (const kind of ["text","reasoning","toolInput"] as const) {
    test(`rejects duplicate ${kind} starts before and after end`, () => {
      const open = setup().assembler; open.accept(raw(kind,"START")); expect(()=>open.accept(raw(kind,"START"))).toThrow(ProviderIncompleteStreamError);
      const ended = setup().assembler; ended.accept(raw(kind,"START")); ended.accept(raw(kind,"END")); expect(()=>ended.accept(raw(kind,"START"))).toThrow(ProviderIncompleteStreamError);
    });
    for (const phase of ["DELTA","END"] as const) test(`rejects ${kind} ${phase.toLowerCase()} after end`, () => {
      const assembler = setup().assembler; assembler.accept(raw(kind,"START")); assembler.accept(raw(kind,"END")); expect(()=>assembler.accept(raw(kind,phase))).toThrow(ProviderIncompleteStreamError);
    });
    test(`Finish rejects open ${kind}`, () => {
      const {assembler,send} = setup(); assembler.accept(raw(kind,"START")); expect(()=>send({type:"finish",finishReason:"stop"})).toThrow(ProviderIncompleteStreamError);
    });
  }
  for (const phase of ["DELTA","END"] as const) test(`tool name mismatch on ${phase.toLowerCase()} is rejected`, () => {
    const assembler = setup().assembler; assembler.accept(raw("toolInput","START")); expect(()=>assembler.accept(raw("toolInput",phase,"Write"))).toThrow(ProviderIncompleteStreamError);
  });
  test("tool name mismatch on complete call is rejected", () => {
    const assembler = setup().assembler; assembler.accept(raw("toolInput","START")); assembler.accept(raw("toolInput","END"));
    expect(()=>assembler.accept({type:FragmentType.ToolCall,toolCall:{id:"part",name:"Write",inputJson:"{}",metadataJson:"{}"}})).toThrow(ProviderIncompleteStreamError);
  });
  test("Finish rejects ended tool input without a complete call", () => {
    const {assembler,send}=setup(); assembler.accept(raw("toolInput","START")); assembler.accept(raw("toolInput","END")); expect(()=>send({type:"finish",finishReason:"stop"})).toThrow(ProviderIncompleteStreamError);
  });
});


describe("streamed tool argument accounting",()=>{
 test("charges streamed arguments to the live budget until completion without retaining a second payload",()=>{
  const {assembler,send}=setup();
  send({type:"tool-input-start",id:"call",name:"Read"},{type:"tool-input-delta",id:"call",delta:"    {}"});
  // Six streamed argument bytes plus the 2-byte empty metadata object.
  expect(assembler.resources).toMatchObject({cumulativeContentBytes:6,retainedBytes:8,segments:0});
  send({type:"tool-input-end",id:"call"});
  expect(send({type:"tool-call",id:"call",name:"Read",input:{}})[0]?.toolCallComplete?.inputJson).toBe("{}");
  expect(assembler.resources.retainedBytes).toBe(0);
  assembler.release();expect(assembler.resources.cumulativeContentBytes).toBe(0);
 });
 test("streamed Tool arguments exactly at the live budget complete and release their charge",()=>{
  const {assembler,send}=setup({bounds:{maxRetainedBytes:64}});
  send({type:"tool-input-start",id:"call",name:"Read"},{type:"tool-input-delta",id:"call",delta:" ".repeat(60)+"{}"});
  expect(assembler.resources).toMatchObject({cumulativeContentBytes:62,retainedBytes:64});
  send({type:"tool-input-end",id:"call"});
  expect(send({type:"tool-call",id:"call",name:"Read",input:{}})[0]?.toolCallComplete?.inputJson).toBe("{}");
  expect(assembler.resources.retainedBytes).toBe(0);
 });
 test("streamed Tool arguments one byte over the live budget fail as retained bytes",()=>{
  const {send}=setup({bounds:{maxRetainedBytes:64}});
  send({type:"tool-input-start",id:"call",name:"Read"});
  let failure:unknown;
  try { send({type:"tool-input-delta",id:"call",delta:" ".repeat(61)+"{}"}); } catch (error) { failure=error; }
  expect(failure).toBeInstanceOf(ProviderAssemblyLimitError);
  expect((failure as ProviderAssemblyLimitError).reason).toBe("retained_bytes");
 });
 test("split Unicode arguments count joined UTF8 and reject incomplete scalars",()=>{
  const {assembler,send}=setup();send({type:"tool-input-start",id:"call",name:"Read"},{type:"tool-input-delta",id:"call",delta:'{"q":"\ud83d'},{type:"tool-input-delta",id:"call",delta:'\ude00"}'},{type:"tool-input-end",id:"call"});
  expect(assembler.resources.cumulativeContentBytes).toBe(Buffer.byteLength('{"q":"😀"}'));
  const bad=setup();bad.send({type:"tool-input-start",id:"call",name:"Read"},{type:"tool-input-delta",id:"call",delta:"\ud83d"});expect(()=>bad.send({type:"tool-input-end",id:"call"})).toThrow("Unicode scalar");
 });
});
