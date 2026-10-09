import {expect,test} from "bun:test";
import {RequestContentProcessor,RequestAssistantMemberSequencer} from "../../src/runtime/accumulator.js";
import type {RequestContentProcessorOptions} from "../../src/runtime/accumulator.js";
import {ThreadState} from "../../src/thread-loop/thread-state.js";
import {createToolCatalog,lookupToolEntry} from "../../src/tools/tool-catalog.js";
import {normalizeRuntimeFailure} from "../../src/contracts/runtime.js";
import type {LLMEvent} from "../../src/llm/llm-event.js";
import {RuntimePodMetricsRegistry} from "../../../runtime-pod/src/metrics.js";
import {deferred} from "./thread-loop/thread-loop-test-support.js";
const source={providerId:"openai",modelId:"model"} as const;
const id=(n:number)=>`evt_${n.toString(16).padStart(32,"0")}`;
function setup(overrides:Partial<RequestContentProcessorOptions>={}){
 const state=new ThreadState("session");
 state.contextManager.replaceMessages([{messageSequence:1,contextKind:"user",parts:[{type:"text",text:"run"}]}]);
 state.installThreadTurn({executionRunId:"run",pendingInputContextSequences:[],request:{modelRequestId:"request",requestStartEventId:"start",requestKind:"agent_provider_request",contextThroughMessageSequence:1,toolMembers:[]}},undefined);
 let invalidations=0,number=0;const writes:unknown[]=[];const repairs:unknown[]=[];
 const processor=new RequestContentProcessor({modelRequestId:"request",requestId:"provider-request",workspaceId:"workspace",sessionId:"session",sessionThreadId:"thread",bindingId:"binding",bindingGeneration:1,targetPodUid:"pod",runtimeProcessId:"process",contextOwner:state.contextManager,activeToolReferences:()=>state.activeTools(),onAssistantMessageCommitted:ref=>state.associateCurrentRequestMessage(ref),onCommittedApplicationFailure:()=>{invalidations++;state.invalidateResidentState();},writer:{appendEvent:async(event,_source,declaration,_request,eventId)=>{writes.push({event,declaration,eventId});number++;return {ok:true,type:"committed",eventId:eventId??`tool-${number}`,...(declaration===undefined?{}:{assistant:{messageSequence:2,createdToolUseEventIds:declaration.assistantContextAppend.parts.filter(part=>part.type==="tool").map(()=>`tool-${number}`)}})};},settleToolResult:async()=>({ok:true,result:{type:"committed"}}),commitInternalToolRepair:async repair=>{repairs.push(repair);return {ok:true,type:"committed",repairEventId:"repair",assignedMessageSequence:2};}},...overrides});
 const process=(event:LLMEvent,signal?:AbortSignal)=>processor.process({...source,event},signal);
 async function declare(call="call"){
  await process({type:"tool-call-complete",id:call,toolName:"Read",input:{file_path:"a"},inputPreview:{preview:"{}",truncated:false}});
  expect(await processor.reservePublicToolUse(source,call,{kind:"tool"},undefined,"sandbox_execute")).toBe(true);
  const result=await processor.commitPublicToolUse(source,call,{file_path:"a"},"allow");
  if(!result.ok)throw new Error("declaration failed");
  const reference=state.currentRequestMessage()!;
  state.registerActiveTool({toolUseEventId:result.toolUseEventId,modelRequestId:"request",modelToolCallId:call,assistantMessageSequence:reference.assistantMessageSequence,disposition:"hot_execution"});
  state.applyThreadTurnFact({fact:"tool_use_committed",eventId:result.toolUseEventId,modelRequestId:"request",modelToolCallId:call,toolName:"Read"});
  return result.toolUseEventId;
 }
 return {state,processor,process,declare,writes,repairs,invalidations:()=>invalidations};
}
test("reasoning prefix is committed once with the next full text; content remains addressable after End",async()=>{
 const f=setup();await f.process({type:"thinking-started",providerPartId:"r",eventId:id(1)});
 await f.process({type:"reasoning-complete",providerPartId:"r",thinkingEventId:id(1),text:"reason",providerMetadata:{anthropic:{signature:"signed"}}});
 expect(f.state.contextManager.messages()).toHaveLength(1);
 await f.process({type:"text-complete",providerPartId:"t",eventId:id(2),text:"answer"});
 expect(f.state.contextManager.currentAssistantMessage()?.parts).toEqual([{type:"reasoning",text:"reason",providerMetadata:{anthropic:{signature:"signed"}}},{type:"text",text:"answer"}]);
 expect(f.state.contextManager.historyMessages()).toHaveLength(1);
 await f.process({type:"finish",finishReason:"stop"});
 f.state.applyThreadTurnFact({fact:"request_ended",eventId:"end",modelRequestId:"request",isError:false,providerContextRetention:{disposition:"completed",assistantMessageSequence:2,toolUseEventIds:[],repairEventIds:[]}});
 expect(f.state.contextManager.historyMessages()).toHaveLength(2);
 expect(f.state.contextManager.currentAssistantMessage()?.messageSequence).toBe(2);
 expect(f.writes).toHaveLength(2);
});
test("empty text neither takes the prefix nor creates a public message",async()=>{
 const f=setup();await f.process({type:"thinking-started",providerPartId:"r",eventId:id(1)});await f.process({type:"reasoning-complete",providerPartId:"r",thinkingEventId:id(1),text:"reason"});
 await f.process({type:"text-complete",providerPartId:"empty",eventId:id(2),text:""});
 expect(f.writes).toHaveLength(1);expect(f.state.contextManager.messages()).toHaveLength(1);
 await f.process({type:"text-complete",providerPartId:"answer",eventId:id(3),text:"answer"});
 expect(f.state.contextManager.currentAssistantMessage()?.parts.map(part=>part.type)).toEqual(["reasoning","text"]);
});
test("empty-only and no-content Finish fail without phantom Assistant",async()=>{
 for(const empty of [false,true]){const f=setup();if(empty)await f.process({type:"text-complete",providerPartId:"empty",eventId:id(1),text:""});const result=await f.process({type:"finish"});expect(result.ok).toBe(false);expect(f.writes).toHaveLength(0);expect(f.state.contextManager.messages()).toHaveLength(1);}
});
for(const type of ["committed","duplicate"] as const)test(`${type} supplied-ID mismatch invalidates the owner before another member can write`,async()=>{
 let writes=0;const f=setup({writer:{appendEvent:async()=>{writes++;return {ok:true,type,eventId:id(99),assistant:{messageSequence:2,createdToolUseEventIds:[]}};},settleToolResult:async()=>({ok:true,result:{type:"committed"}}),commitInternalToolRepair:async()=>({ok:true,type:"committed",repairEventId:"repair",assignedMessageSequence:2})}});
 expect((await f.process({type:"text-complete",providerPartId:"a",eventId:id(1),text:"first"})).ok).toBe(false);
 expect((await f.process({type:"text-complete",providerPartId:"b",eventId:id(2),text:"second"})).ok).toBe(false);
 expect(writes).toBe(1);expect(f.invalidations()).toBe(1);expect(f.state.currentRequestMessage()).toBeUndefined();expect(f.state.contextManager.messages()).toHaveLength(1);
});
test("an ACK followed by failed local association invalidates residency and fences dependent members",async()=>{
 const f=setup({onAssistantMessageCommitted:()=>{throw new Error("association changed");}});
 expect((await f.process({type:"text-complete",providerPartId:"a",eventId:id(1),text:"first"})).ok).toBe(false);
 expect((await f.process({type:"text-complete",providerPartId:"b",eventId:id(2),text:"second"})).ok).toBe(false);
 expect(f.invalidations()).toBeGreaterThan(0);expect(f.writes).toHaveLength(1);expect(f.state.persistentContextLoaded()).toBe(false);
});
test("late tool settlement appends to its committed message and releases registration once",async()=>{
 const f=setup();const tool=await f.declare();await f.process({type:"finish",finishReason:"tool-calls"});
 f.state.applyThreadTurnFact({fact:"request_ended",eventId:"end",modelRequestId:"request",isError:false,providerContextRetention:{disposition:"completed",assistantMessageSequence:2,toolUseEventIds:[tool],repairEventIds:[]}});
 f.processor.discardUncommittedMembers();
 expect(f.processor.activeToolPart("call")?.state.status).toBe("running");
 expect(await f.processor.commitToolSettlement(source,"call",{type:"completed",output:{text:"done",truncated:false}})).toEqual({type:"settled"});
 expect(f.state.contextManager.entry(2)?.parts.map(part=>part.type)).toEqual(["tool_call","tool_result"]);
 f.state.clearThreadToolRoute(tool);
 expect(await f.processor.commitToolSettlement(source,"call",{type:"completed",output:{text:"duplicate",truncated:false}})).toMatchObject({type:"failed"});
 expect(f.state.activeTools()).toHaveLength(0);expect(f.state.contextManager.entry(2)?.parts).toHaveLength(2);
});
test("failure cleanup retains the committed owner for closeout and prohibits new execution",async()=>{
 const f=setup();const tool=await f.declare();const reference=f.state.activeTools()[0]!;const entry=lookupToolEntry(createToolCatalog({family:"claude",configs:[{name:"Read",enabled:true,permissionPolicy:"always_allow"}]}),"Read")!;
 expect(f.state.resolveActiveTool(tool,reference,entry)?.input).toEqual({file_path:"a"});
 f.state.clear();
 expect(f.state.contextManager.entry(reference.assistantMessageSequence)?.parts[0]?.type).toBe("tool_call");
 expect(f.state.resolveActiveTool(tool,reference,entry)).toBeUndefined();
 expect(await f.processor.commitToolSettlement(source,"call",{type:"cancelled"})).toEqual({type:"settled"});
 f.state.clearAfterCustodyHandoff();expect(f.state.activeTools()).toHaveLength(0);expect(f.state.contextManager.messages()).toHaveLength(0);
});
test("pending submissions apply count/byte backpressure with cancellation",async()=>{
 const held=deferred<void>();const sequencer=new RequestAssistantMemberSequencer(async()=>({ok:true,events:[]}),{maxPendingEntries:1,maxPendingBytes:10});
 const first=sequencer.enqueueOperation(async()=>{await held.promise;return {ok:true,events:[]};},10).committed;
 let admitted=false;const waiting=sequencer.awaitCapacity(1).then(()=>{admitted=true;});await Promise.resolve();expect(admitted).toBe(false);
 const controller=new AbortController();const aborted=sequencer.awaitCapacity(1,controller.signal);controller.abort(new Error("cancelled"));await expect(aborted).rejects.toThrow("cancelled");
 held.resolve();await Promise.all([first,waiting]);expect(admitted).toBe(true);await expect(sequencer.awaitCapacity(11)).rejects.toThrow("submission budget");
});

test("invalid-tool repair freezes reasoning before waiting and atomically installs prefix/call/result",async()=>{
 const f=setup();
 await f.process({type:"thinking-started",providerPartId:"r",eventId:id(1)});
 await f.process({type:"reasoning-complete",providerPartId:"r",thinkingEventId:id(1),text:"signed reason",providerMetadata:{anthropic:{signature:"sig"}}});
 await f.process({type:"tool-call-complete",id:"invalid",toolName:"Read",input:{wrong:"field"},inputPreview:{preview:"{}",truncated:false}});
 const repaired=await f.processor.commitInternalToolRepair(source,"invalid","request","repair-key",normalizeRuntimeFailure({type:"runtime",code:"runtime_invalid_sequence",retryable:false,fatal:true}));
 expect(repaired.ok).toBe(true);
 expect(f.repairs).toHaveLength(1);
 expect(f.repairs[0]).toMatchObject({reasoningPrefixContextDelta:{parts:[{type:"reasoning",text:"signed reason",providerMetadata:{anthropic:{signature:"sig"}}}]}});
 expect(f.state.contextManager.entry(2)?.parts.map(part=>part.type)).toEqual(["reasoning","tool_call","tool_result"]);

 await f.process({type:"text-complete",providerPartId:"text",eventId:id(2),text:"next"});
 expect(f.state.contextManager.entry(2)?.parts.map(part=>part.type)).toEqual(["reasoning","tool_call","tool_result","text"]);
});


test("held content ACK exposes owner gauges and releases them after local application",async()=>{
 const entered=deferred<void>(),release=deferred<void>();
 const metrics=new RuntimePodMetricsRegistry();
 let clock=10;
 const observations:unknown[]=[];
 const f=setup({monotonicMs:()=>clock,onSubmissionDelta:(entries,bytes)=>metrics.recordContentSubmissionDelta(entries,bytes),onContentCommit:observation=>{observations.push(observation);metrics.observeContentCommitLatency(observation.kind,observation.phase,observation.durationMs,observation.outcome);},writer:{appendEvent:async(_event,_source,_declaration,_request,eventId)=>{entered.resolve();await release.promise;clock=35;return {ok:true,type:"committed",eventId:eventId!,assistant:{messageSequence:2,createdToolUseEventIds:[]}};},settleToolResult:async()=>({ok:true,result:{type:"committed"}}),commitInternalToolRepair:async()=>({ok:true,type:"committed",repairEventId:"repair",assignedMessageSequence:2})}});
 const pending=f.process({type:"text-complete",providerPartId:"text",eventId:id(1),text:"answer"});
 try{
  await entered.promise;
  expect(metrics.snapshot().pendingContentEntries).toBe(1);
  expect(metrics.snapshot().pendingContentBytes).toBeGreaterThan(0);
  expect(f.state.contextManager.messages()).toHaveLength(1);
  expect(observations).toEqual([]);
 }finally{release.resolve();await pending;}
 expect(metrics.snapshot().pendingContentEntries).toBe(0);
 expect(metrics.snapshot().pendingContentBytes).toBe(0);
 expect(observations).toEqual([{kind:"text",phase:"content_commit",durationMs:25,outcome:"committed",requestKind:"agent_provider_request",canonicalJsonBytes:Buffer.byteLength(JSON.stringify({parts:[{type:"text",text:"answer",truncated:false}]}),"utf8")},{kind:"text",phase:"content_apply",durationMs:0,outcome:"committed",requestKind:"agent_provider_request",canonicalJsonBytes:Buffer.byteLength(JSON.stringify({parts:[{type:"text",text:"answer",truncated:false}]}),"utf8")}]);
 expect(f.state.contextManager.entry(2)?.parts).toEqual([{type:"text",text:"answer"}]);
});

test("throwing content observers cannot change a committed member or leak pending ownership",async()=>{
 const f=setup({onSubmissionDelta:()=>{throw new Error("metrics unavailable");},onContentCommit:()=>{throw new Error("logger unavailable");}});
 expect((await f.process({type:"text-complete",providerPartId:"text",eventId:id(1),text:"answer"})).ok).toBe(true);
 expect(f.state.contextManager.entry(2)?.parts).toEqual([{type:"text",text:"answer"}]);
 expect(f.invalidations()).toBe(0);
});

for (const kind of ["text", "tool"] as const) {
 test(`a thrown ${kind} write records one failed commit and releases submission ownership`, async () => {
  const metrics = new RuntimePodMetricsRegistry();
  const observations: unknown[] = [];
  let clock = 10;
  let attempts = 0;
  let submittedBytes = 0;
  const f = setup({
   monotonicMs: () => clock,
   onSubmissionDelta: (entries, bytes) => metrics.recordContentSubmissionDelta(entries, bytes),
   onContentCommit: observation => observations.push(observation),
   writer: {
    appendEvent: async (_event, _source, declaration) => { attempts++; submittedBytes = Buffer.byteLength(JSON.stringify(declaration!.assistantContextAppend), "utf8"); clock = 17; throw new Error("transport failed before ACK"); },
    settleToolResult: async () => ({ ok: true, result: { type: "committed" } }),
    commitInternalToolRepair: async () => ({ ok: true, type: "committed", repairEventId: "repair", assignedMessageSequence: 2 }),
   },
  });
  if (kind === "text") {
   expect((await f.process({ type: "text-complete", providerPartId: "text", eventId: id(1), text: "answer" })).ok).toBe(false);
  } else {
   await f.process({ type: "tool-call-complete", id: "call", toolName: "Read", input: { file_path: "a" }, inputPreview: { preview: "{}", truncated: false } });
   expect(await f.processor.reservePublicToolUse(source, "call", { kind: "tool" }, undefined, "sandbox_execute")).toBe(true);
   expect((await f.processor.commitPublicToolUse(source, "call", { file_path: "a" }, "allow")).ok).toBe(false);
  }
  expect(attempts).toBe(1);
  expect(observations).toEqual([{ kind, phase: "content_commit", durationMs: 7, outcome: "failed", requestKind: "agent_provider_request", canonicalJsonBytes: submittedBytes }]);
  expect(f.invalidations()).toBe(1);
  expect(f.state.currentRequestMessage()).toBeUndefined();
  expect(f.state.contextManager.messages()).toHaveLength(1);
  expect(metrics.snapshot().pendingContentEntries).toBe(0);
  expect(metrics.snapshot().pendingContentBytes).toBe(0);
 });
}

test("submission limits reject invalid operational policy before observing work",()=>{
 for(const limits of [{maxPendingEntries:0,maxPendingBytes:1},{maxPendingEntries:1,maxPendingBytes:-1},{maxPendingEntries:1.5,maxPendingBytes:1}])expect(()=>new RequestAssistantMemberSequencer(async()=>({ok:true,events:[]}),limits)).toThrow("positive safe integers");
});


test("settlement ACK followed by failed reducer application invalidates the committed owner",async()=>{
 const f=setup({onToolResultCommitted:()=>{throw new Error("checkpoint mismatch");}});
 await f.declare();
 expect(await f.processor.commitToolSettlement(source,"call",{type:"completed",output:{text:"done",truncated:false}})).toMatchObject({type:"failed"});
 expect(f.invalidations()).toBe(1);
 expect(f.state.persistentContextLoaded()).toBe(false);
});

test("reasoning part budget remains cumulative after each prefix is consumed",async()=>{
 const f=setup();
 for(let n=0;n<16;n++){
  await f.process({type:"thinking-started",providerPartId:`r${n}`,eventId:id(n*2+1)});
  expect((await f.process({type:"reasoning-complete",providerPartId:`r${n}`,thinkingEventId:id(n*2+1),text:"r"})).ok).toBe(true);
  await f.process({type:"text-complete",providerPartId:`t${n}`,eventId:id(n*2+2),text:"t"});
 }
 await f.process({type:"thinking-started",providerPartId:"overflow",eventId:id(33)});
 expect((await f.process({type:"reasoning-complete",providerPartId:"overflow",thinkingEventId:id(33),text:"r"})).ok).toBe(false);
 expect(f.state.contextManager.entry(2)?.parts.filter(part=>part.type==="reasoning")).toHaveLength(16);
});

test("a reserved tool position holds its reasoning prefix before a later completed text",async()=>{
 const f=setup();
 await f.process({type:"thinking-started",providerPartId:"r",eventId:id(1)});
 await f.process({type:"reasoning-complete",providerPartId:"r",thinkingEventId:id(1),text:"reason"});
 await f.process({type:"tool-call-complete",id:"call",toolName:"Read",input:{file_path:"a"},inputPreview:{preview:"{}",truncated:false}});
 expect(await f.processor.reservePublicToolUse(source,"call",{kind:"tool"},undefined,"sandbox_execute")).toBe(true);
 let settled=false;
 const text=f.process({type:"text-complete",providerPartId:"t",eventId:id(2),text:"after"}).then(result=>{settled=true;return result;});
 await Promise.resolve();expect(settled).toBe(false);expect(f.writes).toHaveLength(1);
 try{expect((await f.processor.commitPublicToolUse(source,"call",{file_path:"a"},"allow")).ok).toBe(true);}finally{f.processor.cancelUndeclaredToolUses();await text;}
 expect(f.state.contextManager.entry(2)?.parts.map(part=>part.type)).toEqual(["reasoning","tool_call","text"]);
});


test("reasoning byte budget remains cumulative after its prefix commits",async()=>{
 const f=setup();
 await f.process({type:"thinking-started",providerPartId:"r1",eventId:id(1)});
 expect((await f.process({type:"reasoning-complete",providerPartId:"r1",thinkingEventId:id(1),text:"x".repeat(2*1024*1024-2)})).ok).toBe(true);
 await f.process({type:"text-complete",providerPartId:"t",eventId:id(2),text:"answer"});
 await f.process({type:"thinking-started",providerPartId:"r2",eventId:id(3)});
 expect((await f.process({type:"reasoning-complete",providerPartId:"r2",thinkingEventId:id(3),text:"x"})).ok).toBe(false);
 expect(f.state.contextManager.entry(2)?.parts.filter(part=>part.type==="reasoning")).toHaveLength(1);
});


test("held repair accounting includes the frozen canonical input, error and reasoning prefix",async()=>{
 const entered=deferred<void>(),release=deferred<void>(),metrics=new RuntimePodMetricsRegistry();
 let payloadBytes=0;
 const f=setup({onSubmissionDelta:(entries,bytes)=>metrics.recordContentSubmissionDelta(entries,bytes),writer:{appendEvent:async(_event,_source,_declaration,_request,eventId)=>({ok:true,type:"committed",eventId:eventId!}),settleToolResult:async()=>({ok:true,result:{type:"committed"}}),commitInternalToolRepair:async payload=>{payloadBytes=Buffer.byteLength(JSON.stringify(payload),"utf8");entered.resolve();await release.promise;return {ok:true,type:"committed",repairEventId:"repair",assignedMessageSequence:2};}}});
 await f.process({type:"thinking-started",providerPartId:"r",eventId:id(1)});
 await f.process({type:"reasoning-complete",providerPartId:"r",thinkingEventId:id(1),text:"prefix"});
 await f.process({type:"tool-call-complete",id:"invalid",toolName:"Read",input:{invalid:"x".repeat(2048)},inputPreview:{preview:"{}",truncated:false}});
 const pending=f.processor.commitInternalToolRepair(source,"invalid","request","repair-key",normalizeRuntimeFailure({type:"runtime",code:"runtime_invalid_sequence",retryable:false,fatal:true}));
 try{await entered.promise;expect(metrics.snapshot().pendingContentEntries).toBe(1);expect(metrics.snapshot().pendingContentBytes).toBe(payloadBytes);expect(payloadBytes).toBeGreaterThan(2048);}finally{release.resolve();await pending;}
 expect(metrics.snapshot().pendingContentEntries).toBe(0);expect(metrics.snapshot().pendingContentBytes).toBe(0);
 expect(f.state.contextManager.entry(2)?.parts.map(part=>part.type)).toEqual(["reasoning","tool_call","tool_result"]);
});

for (const contextKind of ["user", "assistant"] as const) {
 test(`a first Assistant ACK cannot overwrite an unrelated committed ${contextKind}`, async () => {
  const f = setup({ writer: {
   appendEvent: async (_event, _source, _declaration, _request, eventId) => ({ ok: true, type: "committed", eventId: eventId!, assistant: { messageSequence: 1, createdToolUseEventIds: [] } }),
   settleToolResult: async () => ({ ok: true, result: { type: "committed" } }),
   commitInternalToolRepair: async () => ({ ok: true, type: "committed", repairEventId: "repair", assignedMessageSequence: 1 }),
  } });
  const original = { messageSequence: 1, contextKind, parts: [{ type: "text" as const, text: "historical owner" }] };
  f.state.contextManager.replaceMessages([original]);
  const result = await f.process({ type: "text-complete", providerPartId: "new", eventId: id(1), text: "new content" });
  expect(result.ok).toBe(false);
  expect(f.state.contextManager.entry(1)).toEqual(original);
  expect(f.state.currentRequestMessage()).toBeUndefined();
  expect(f.state.persistentContextLoaded()).toBe(false);
  expect(f.invalidations()).toBe(1);
 });
}

for(const over of [false,true]){
 test(`text admission uses exact frozen append ${over?"over":"at"} its byte limit`,async()=>{
  const released=deferred<void>(),entered=deferred<void>(),metrics=new RuntimePodMetricsRegistry();
  const exact=Buffer.byteLength(JSON.stringify({parts:[{type:"text",text:"answer",truncated:false}]}),"utf8");
  let attempts=0;
  const f=setup({pendingSubmissionLimits:{maxPendingEntries:1,maxPendingBytes:exact},
   onSubmissionDelta:(entries,bytes)=>metrics.recordContentSubmissionDelta(entries,bytes),writer:{
    appendEvent:async(_event,_source,_append,_request,eventId)=>{attempts++;entered.resolve();await released.promise;return {ok:true,type:"committed",eventId:eventId!,assistant:{messageSequence:2,createdToolUseEventIds:[]}};},
    settleToolResult:async()=>({ok:true,result:{type:"committed"}}),
    commitInternalToolRepair:async()=>({ok:true,type:"committed",repairEventId:"repair",assignedMessageSequence:2}),
   }});
  const pending=f.process({type:"text-complete",providerPartId:"text",eventId:id(1),text:over?"answer!":"answer"});
  try{
   if(over){expect((await pending).ok).toBe(false);expect(attempts).toBe(0);expect(f.state.currentRequestMessage()).toBeUndefined();}
   else{await entered.promise;expect(metrics.snapshot().pendingContentEntries).toBe(1);expect(metrics.snapshot().pendingContentBytes).toBe(exact);released.resolve();expect((await pending).ok).toBe(true);expect(f.state.contextManager.entry(2)?.parts).toEqual([{type:"text",text:"answer"}]);}
  }finally{released.resolve();await pending;}
  expect(metrics.snapshot().pendingContentEntries).toBe(0);expect(metrics.snapshot().pendingContentBytes).toBe(0);
 });
}
for(const cancel of [false,true]){
 test(`repair admission ${cancel?"cancels":"waits"} behind a held exact-byte submission`,async()=>{
  const firstEntered=deferred<void>(),releaseFirst=deferred<void>(),repairEntered=deferred<void>(),releaseRepair=deferred<void>();
  const metrics=new RuntimePodMetricsRegistry(),abort=new AbortController();let repairCalls=0,repairBytes=0;
  const firstBytes=Buffer.byteLength(JSON.stringify({parts:[{type:"text",text:"first",truncated:false}]}),"utf8");
  const f=setup({pendingSubmissionLimits:{maxPendingEntries:1,maxPendingBytes:8192},onSubmissionDelta:(entries,bytes)=>metrics.recordContentSubmissionDelta(entries,bytes),writer:{
   appendEvent:async(_event,_source,_append,_request,eventId)=>{if(_event.type==="agent.thinking")return {ok:true,type:"committed",eventId:eventId!};firstEntered.resolve();await releaseFirst.promise;return {ok:true,type:"committed",eventId:eventId!,assistant:{messageSequence:2,createdToolUseEventIds:[]}};},
   settleToolResult:async()=>({ok:true,result:{type:"committed"}}),
   commitInternalToolRepair:async payload=>{repairCalls++;repairBytes=Buffer.byteLength(JSON.stringify(payload),"utf8");repairEntered.resolve();await releaseRepair.promise;return {ok:true,type:"committed",repairEventId:"repair",assignedMessageSequence:2};},
  }});
  const first=f.process({type:"text-complete",providerPartId:"text",eventId:id(1),text:"first"});
  await firstEntered.promise;
  await f.process({type:"thinking-started",providerPartId:"reason",eventId:id(2)});
  await f.process({type:"reasoning-complete",providerPartId:"reason",thinkingEventId:id(2),text:"repair-prefix",providerMetadata:{anthropic:{signature:"repair-signature"}}});
  await f.process({type:"tool-call-complete",id:"invalid",toolName:"Read",input:{invalid:"x".repeat(2048)},inputPreview:{preview:"{}",truncated:false}});
  const repair=f.processor.commitInternalToolRepair(source,"invalid","request","repair",normalizeRuntimeFailure({type:"runtime",code:"runtime_invalid_sequence",retryable:false,fatal:true}),abort.signal);
  try{
   expect((await f.process({type:"finish",finishReason:"stop"})).ok).toBe(true);
   expect(f.processor.requestEndAppend()).toEqual({parts:[{type:"reasoning",providerPartId:"reason",text:"repair-prefix",truncated:false,providerMetadata:{anthropic:{signature:"repair-signature"}}}]});
   await Promise.resolve();expect(repairCalls).toBe(0);expect(metrics.snapshot().pendingContentEntries).toBe(1);expect(metrics.snapshot().pendingContentBytes).toBe(firstBytes);
   if(cancel){abort.abort(new Error("cancel admission"));await expect(repair).rejects.toThrow("cancel admission");f.processor.discardProducerWorkingState();}
   else{releaseFirst.resolve();expect((await first).ok).toBe(true);await repairEntered.promise;expect(metrics.snapshot().pendingContentEntries).toBe(1);expect(metrics.snapshot().pendingContentBytes).toBe(repairBytes);expect(repairBytes).toBeGreaterThan(2048);releaseRepair.resolve();expect((await repair).ok).toBe(true);}
  }finally{abort.abort();releaseFirst.resolve();releaseRepair.resolve();await Promise.allSettled([first,repair]);}
  expect(metrics.snapshot().pendingContentEntries).toBe(0);expect(metrics.snapshot().pendingContentBytes).toBe(0);expect(repairCalls).toBe(cancel?0:1);
  expect(f.processor.requestEndAppend()).toBeUndefined();
  expect(f.state.contextManager.entry(2)?.parts.filter(part=>part.type==="reasoning")).toEqual(cancel?[]:[{type:"reasoning",text:"repair-prefix",providerMetadata:{anthropic:{signature:"repair-signature"}}}]);
 });
}

for (const receipt of ["committed", "duplicate"] as const) {
 for (const failure of ["wrong-id", "member-partial", "repair-partial", "settlement-partial"] as const) {
  test(`${receipt} ${failure} records a failed application without a successful local sample`, async () => {
   const metrics = new RuntimePodMetricsRegistry();
   const observations: Parameters<NonNullable<RequestContentProcessorOptions["onContentCommit"]>>[0][] = [];
   let clock = 0;
   let applications = 0;
   const f = setup({
    monotonicMs: () => clock,
    onSubmissionDelta: (entries, bytes) => metrics.recordContentSubmissionDelta(entries, bytes),
    onContentCommit: observation => {
     observations.push(observation);
     metrics.observeContentCommitLatency(observation.kind, observation.phase, observation.durationMs, observation.outcome, observation.requestKind);
    },
    onAssistantMessageCommitted: reference => {
     f.state.associateCurrentRequestMessage(reference);
     if (failure === "member-partial") { applications++; clock += 7; throw new Error("association application failed after install"); }
    },
    onInternalToolRepairCommitted: () => { applications++; clock += 7; throw new Error("repair reducer failed after install"); },
    onToolResultCommitted: () => { applications++; clock += 7; throw new Error("settlement reducer failed after append"); },
    writer: {
     appendEvent: async (_event, _source, declaration, _request, eventId) => {
      clock += 11;
      return { ok: true, type: receipt, eventId: failure === "wrong-id" ? id(99) : eventId ?? "tool",
       ...(declaration === undefined ? {} : { assistant: { messageSequence: 2, createdToolUseEventIds: declaration.assistantContextAppend.parts.filter(part => part.type === "tool").map(() => "tool") } }) };
     },
     settleToolResult: async () => { clock += 11; return { ok: true, result: { type: receipt } }; },
     commitInternalToolRepair: async () => { clock += 11; return { ok: true, type: receipt, repairEventId: "repair", assignedMessageSequence: 2 }; },
    },
   });
   let kind: "text" | "internal_tool_repair" | "tool_result" = "text";
   if (failure === "repair-partial") {
    kind = "internal_tool_repair";
    await f.process({ type: "thinking-started", providerPartId: "r", eventId: id(1) });
    await f.process({ type: "reasoning-complete", providerPartId: "r", thinkingEventId: id(1), text: "signed reason", providerMetadata: { anthropic: { signature: "sig" } } });
    await f.process({ type: "tool-call-complete", id: "invalid", toolName: "Read", input: { wrong: "field" }, inputPreview: { preview: "{}", truncated: false } });
    expect((await f.processor.commitInternalToolRepair(source, "invalid", "request", "repair-key", normalizeRuntimeFailure({ type: "runtime", code: "runtime_invalid_sequence", retryable: false, fatal: true }))).ok).toBe(false);
    expect(f.state.contextManager.entry(2)?.parts.map(part => part.type)).toEqual(["reasoning", "tool_call", "tool_result"]);
   } else if (failure === "settlement-partial") {
    kind = "tool_result";
    await f.declare();
    expect(await f.processor.commitToolSettlement(source, "call", { type: "completed", output: { text: "done", truncated: false } })).toMatchObject({ type: "failed" });
    expect(f.state.contextManager.entry(2)?.parts.filter(part => part.type === "tool_result")).toHaveLength(1);
   } else {
    expect((await f.process({ type: "text-complete", providerPartId: "t", eventId: id(1), text: "answer" })).ok).toBe(false);
    expect((await f.process({ type: "text-complete", providerPartId: "later", eventId: id(2), text: "must not write" })).ok).toBe(false);
   }
   const samples = observations.filter(sample => sample.kind === kind);
   expect(samples.map(({ phase, durationMs, outcome }) => ({ phase, durationMs, outcome }))).toEqual([
    { phase: "content_commit", durationMs: 11, outcome: receipt },
    { phase: "content_apply", durationMs: failure === "wrong-id" ? 0 : 7, outcome: "failed" },
   ]);
   expect(samples.map(sample => sample.canonicalJsonBytes)).toEqual([expect.any(Number), samples[0]!.canonicalJsonBytes]);
   const applicationsForKind = [...metrics.snapshot().contentCommitLatencyMs!.entries()].filter(([key]) => key.includes(`kind="${kind}"`) && key.includes('phase="content_apply"'));
   expect(applicationsForKind).toEqual([[expect.stringContaining('outcome="failed"'), { count: 1, sum: failure === "wrong-id" ? 0 : 7 }]]);
   expect(applications).toBe(failure === "wrong-id" ? 0 : 1);
   expect(f.invalidations()).toBe(1);
   expect(f.state.persistentContextLoaded()).toBe(false);
   expect(metrics.snapshot().pendingContentEntries).toBe(0);
   expect(metrics.snapshot().pendingContentBytes).toBe(0);
  });
 }
 test(`${receipt} first local member receipt installs and associates content once`, async () => {
  let applications = 0;
  const metrics = new RuntimePodMetricsRegistry();
  const f = setup({
   onContentCommit: observation => metrics.observeContentCommitLatency(observation.kind, observation.phase, observation.durationMs, observation.outcome, observation.requestKind),
   onAssistantMessageCommitted: reference => { applications++; f.state.associateCurrentRequestMessage(reference); }, writer: {
   appendEvent: async (_event, _source, _append, _request, eventId) => ({ ok: true, type: receipt, eventId: eventId!, assistant: { messageSequence: 2, createdToolUseEventIds: [] } }),
   settleToolResult: async () => ({ ok: true, result: { type: "committed" } }),
   commitInternalToolRepair: async () => ({ ok: true, type: "committed", repairEventId: "repair", assignedMessageSequence: 2 }),
  } });
  expect((await f.process({ type: "text-complete", providerPartId: "t", eventId: id(1), text: "answer" })).ok).toBe(true);
  expect(applications).toBe(1);
  expect([...metrics.snapshot().contentCommitLatencyMs!.entries()].filter(([key]) => key.includes('phase="content_apply"')))
   .toEqual([[expect.stringContaining(`outcome="${receipt}"`), { count: 1, sum: expect.any(Number) }]]);
  expect(f.state.contextManager.entry(2)?.parts).toEqual([{ type: "text", text: "answer" }]);
  expect(f.state.currentRequestMessage()).toEqual({ modelRequestId: "request", assistantMessageSequence: 2 });
  expect(f.invalidations()).toBe(0);
 });
}
