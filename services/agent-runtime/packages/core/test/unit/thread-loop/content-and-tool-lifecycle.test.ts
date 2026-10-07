import { test, expect } from "bun:test";
import { Effect, Stream } from "effect";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import { createToolCatalog } from "../../../src/tools/tool-catalog.js";
import {normalizeContextLoaderError,normalizeRuntimeFailure} from "../../../src/contracts/runtime.js";
import {RuntimeFailureSchema} from "../../../src/llm/llm-event.js";
import type { LLMEvent } from "../../../src/llm/llm-event.js";
import { RecordingContextLoader, runtimeThreadLoopLayer, testRunCustody, userMessage, writerFrom, deferred } from "./thread-loop-test-support.js";

type FixtureTool = "Read" | "Write" | "Bash";
type CustodyCorruption = "lost-registration" | "lost-content" | "stale-custody";

// Observation facts for one declared Sandbox Tool: provider requests, its
// declaration and request End ACKs, refresh hold/release, local End seal,
// acceptance, execution and result settlement.
const lifecycleFacts = new Set([
 "provider-request-1", "provider-request-2", "ACK:agent.tool_use", "execution-refresh-entered", "execution-refresh-released",
 "provider-finish", "ACK:span.model_request_end", "local-seal", "sandbox-accepted", "sandbox-executed", "result-settled",
]);

/**
 * Runs one declared Sandbox Tool through the actual ThreadLoop and Sandbox route.
 * The execution refresh is held after the declaration ACK. With delayed, End is
 * applied and the Assistant sealed before the refresh returns; otherwise the
 * refresh returns before provider Finish. A corruption is applied after End and
 * the local seal, before the held refresh returns.
 */
async function runRefreshAfterEndScenario(options: { readonly tool: FixtureTool; readonly delayed: boolean; readonly corruption?: CustodyCorruption }) {
 const {tool: name, delayed, corruption} = options;
 const session = new ThreadRuntime("sesn_1");
 const refreshEntered = deferred<void>();
 const releaseRefresh = deferred<void>();
 const accepted = deferred<void>();
 const sealed = deferred<void>();
 const order: string[] = [];
 const acceptances: { readonly toolUseEventId: string; readonly modelToolCallId: string }[] = [];
 const settledToolUseEventIds: string[] = [];
 const sessionErrors: unknown[] = [];
 let declaredToolUseEventId = "";
 let refreshes = 0, executions = 0, requests = 0;
 const originalFact=session.state.applyThreadTurnFact.bind(session.state);
 session.state.applyThreadTurnFact=fact=>{const result=originalFact(fact);if(fact.fact==="request_ended"){order.push("local-seal");sealed.resolve();}return result;};
 const writer = writerFrom(envelope => {
  order.push(`ACK:${envelope.event.type}`);
  if (envelope.event.type === "agent.tool_use") declaredToolUseEventId = `bridge-${envelope.writeId}`;
  if (envelope.event.type === "session.error") sessionErrors.push(envelope.event.error);
  return {ok:true,type:"committed",eventId:`bridge-${envelope.writeId}`,eventSequence:1};
 }, undefined, [], undefined, async envelope => {
  order.push("result-settled");
  settledToolUseEventIds.push(envelope.settlement.toolUseEventId);
  return {ok:true as const,result:{type:"committed" as const}};
 });
 const catalog = createToolCatalog({family:"claude",configs:[{name,enabled:true,permissionPolicy:"always_allow"}]});
 const input = name === "Read" ? {file_path:"fixture.txt"} : name === "Write" ? {file_path:"fixture.txt",content:"fixture"} : {command:"printf fixture"};
 const loader = new RecordingContextLoader([], {type:"context",entries:[userMessage("user-1",0,"run fixture tool")]});
 const run = Effect.runPromise(Effect.gen(function* () {
  const loop = yield* ThreadLoop.Service;
  return yield* loop.run(session,testRunCustody());
 }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
  writer,
  providerCallRuntime:{systemInstructions:"baseline",toolCatalog:catalog},
  llmService:{stream() {
   requests++;
   order.push(`provider-request-${requests}`);
   return Stream.fromAsyncIterable((async function* (): AsyncGenerator<LLMEvent> {
    if (requests > 1) {
     yield {type:"text-complete" as const,providerPartId:"final",eventId:"evt_33333333333333333333333333333333",text:"done"};
     yield {type:"finish",finishReason:"stop"};
     return;
    }
    yield {type:"tool-call-complete",id:"call-1",toolName:name,input,inputPreview:{preview:JSON.stringify(input),truncated:false}};
    await refreshEntered.promise;
    if (!delayed) await accepted.promise;
    order.push("provider-finish");
    yield {type:"finish",finishReason:"tool-calls"};
   })(), error => ({type:"llm-service",error:RuntimeFailureSchema.parse(normalizeRuntimeFailure({type:"runtime",code:"runtime_invalid_sequence",retryable:false,fatal:true,rawError:error}))}));
  }},
  refreshRuntimeBindingToken:async identity => {
   refreshes++;
   if (refreshes === 2) {
    order.push("execution-refresh-entered");
    refreshEntered.resolve();
    await releaseRefresh.promise;
    order.push("execution-refresh-released");
    if (corruption === "stale-custody") throw normalizeContextLoaderError({code:"superseded",sessionId:session.sessionId});
   }
   return identity.runtimeBindingToken;
  },
  acceptSandboxExecution:request => {acceptances.push({toolUseEventId:request.toolUseEventId,modelToolCallId:request.modelToolCallId});order.push("sandbox-accepted");accepted.resolve();return {type:"accepted"};},
  awaitSandboxExecution:() => {executions++;order.push("sandbox-executed");return {type:"completed",output:{text:"fixture-output",truncated:false}};},
 }))));
 try {
  await refreshEntered.promise;
  if (delayed) {
   await sealed.promise;
   expect(order).toContain("ACK:agent.tool_use");
   expect(order).toContain("ACK:span.model_request_end");
   expect(session.state.contextManager.currentAssistantMessage()?.contextKind).toBe("assistant");
   expect(session.state.contextManager.messages().some(message=>message.parts.some(part=>part.type === "tool_call"))).toBe(true);
  }
  if (corruption === "lost-registration") session.state.clearThreadToolRoute(declaredToolUseEventId);
  if (corruption === "lost-content") session.state.contextManager.replaceMessages(session.state.contextManager.messages().filter(message=>message.messageSequence!==session.state.currentRequestMessage()?.assistantMessageSequence));
  releaseRefresh.resolve();
  const result = await run;
  return {result, facts: order.filter(entry=>lifecycleFacts.has(entry)), declaredToolUseEventId, acceptances, executions, requests, settledToolUseEventIds, sessionErrors};
 } finally {releaseRefresh.resolve();accepted.resolve();await run.catch(() => undefined);}
}

for (const name of ["Read", "Write", "Bash"] as const) {
 test(`${name}: refresh after End`, async () => {
  const observed = await runRefreshAfterEndScenario({tool:name, delayed:true});
  expect(observed.result.type).toBe("completed");
  expect(observed.facts).toEqual([
   "provider-request-1", "ACK:agent.tool_use", "execution-refresh-entered", "provider-finish",
   "ACK:span.model_request_end", "local-seal", "execution-refresh-released", "sandbox-accepted",
   "sandbox-executed", "result-settled", "provider-request-2", "ACK:span.model_request_end", "local-seal",
  ]);
  expect(observed.acceptances).toEqual([{toolUseEventId:observed.declaredToolUseEventId,modelToolCallId:"call-1"}]);
  expect(observed.executions).toBe(1);
  expect(observed.settledToolUseEventIds).toEqual([observed.declaredToolUseEventId]);
 },15000);

 test(`${name}: refresh before End`, async () => {
  const observed = await runRefreshAfterEndScenario({tool:name, delayed:false});
  expect(observed.result.type).toBe("completed");
  expect(observed.facts).toHaveLength(13);
  // Provider Finish waits for acceptance; End and result settlement then race legally.
  expect(observed.facts.slice(0,5)).toEqual(["provider-request-1", "ACK:agent.tool_use", "execution-refresh-entered", "execution-refresh-released", "sandbox-accepted"]);
  const firstRequestTail = observed.facts.slice(5,10);
  expect(firstRequestTail.filter(entry=>entry==="provider-finish"||entry==="ACK:span.model_request_end"||entry==="local-seal")).toEqual(["provider-finish", "ACK:span.model_request_end", "local-seal"]);
  expect(firstRequestTail.filter(entry=>entry==="sandbox-executed"||entry==="result-settled")).toEqual(["sandbox-executed", "result-settled"]);
  expect(observed.facts.slice(10)).toEqual(["provider-request-2", "ACK:span.model_request_end", "local-seal"]);
  expect(observed.acceptances).toEqual([{toolUseEventId:observed.declaredToolUseEventId,modelToolCallId:"call-1"}]);
  expect(observed.executions).toBe(1);
  expect(observed.settledToolUseEventIds).toEqual([observed.declaredToolUseEventId]);
 },15000);
}

// Execution re-resolves its registered Tool and committed content after the
// refresh, so a corrupted owner fails before Sandbox acceptance and the run
// issues no next provider request.
const heldRefreshFacts = ["provider-request-1", "ACK:agent.tool_use", "execution-refresh-entered", "provider-finish", "ACK:span.model_request_end", "local-seal", "execution-refresh-released"];
for (const [corruption, title] of [
 ["lost-registration", "Read: lost Tool registration after End fails before Sandbox acceptance"],
 ["lost-content", "Read: lost committed Assistant content after End fails before Sandbox acceptance"],
] as const) {
 test(title, async () => {
  const observed = await runRefreshAfterEndScenario({tool:"Read", delayed:true, corruption});
  expect(observed.result.type).toBe("failed");
  expect(observed.facts).toEqual(heldRefreshFacts);
  // The durable session.error is the boundary that exposes the classified failure.
  expect(observed.sessionErrors).toHaveLength(1);
  expect(observed.sessionErrors[0]).toMatchObject({type:"runtime",code:"runtime_invalid_sequence",reason:"runtime_contract_validation"});
  expect(observed.acceptances).toEqual([]);
  expect(observed.executions).toBe(0);
  expect(observed.requests).toBe(1);
  expect(observed.settledToolUseEventIds).toEqual([]);
 },15000);
}

test("Read: superseded custody during refresh after End discards hot state without acceptance", async () => {
 const observed = await runRefreshAfterEndScenario({tool:"Read", delayed:true, corruption:"stale-custody"});
 expect(observed.result).toEqual({type:"interrupted",discardHotState:true});
 expect(observed.facts).toEqual(heldRefreshFacts);
 expect(observed.sessionErrors).toEqual([]);
 expect(observed.acceptances).toEqual([]);
 expect(observed.executions).toBe(0);
 expect(observed.requests).toBe(1);
 expect(observed.settledToolUseEventIds).toEqual([]);
},15000);
