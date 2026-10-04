import { test, expect } from "bun:test";
import { Effect, Stream } from "effect";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import { createToolCatalog } from "../../../src/tools/tool-catalog.js";
import {normalizeContextLoaderError,normalizeRuntimeFailure} from "../../../src/contracts/runtime.js";
import {RuntimeFailureSchema} from "../../../src/llm/llm-event.js";
import type { LLMEvent } from "../../../src/llm/llm-event.js";
import { RecordingContextLoader, runtimeThreadLoopLayer, testRunCustody, userMessage, writerFrom, deferred } from "./thread-loop-test-support.js";



for (const mode of ["lost-registration","lost-content","stale-custody"] as const) {
 const name="Read";
 for (const delayed of [true]) {
  test(`Read: ${mode} while refresh waits after End`, async () => {
   const session = new ThreadRuntime("sesn_1");
   const refreshEntered = deferred<void>();
   const releaseRefresh = deferred<void>();
   const accepted = deferred<void>();
   const sealed = deferred<void>();
   const order: string[] = [];
   let toolUseId="";
 let refreshes = 0, acceptances = 0, executions = 0, requests = 0;
   const originalFact=session.state.applyThreadTurnFact.bind(session.state);
   session.state.applyThreadTurnFact=fact=>{const result=originalFact(fact);if(fact.fact==="request_ended"){order.push("local-seal");sealed.resolve();}return result;};
   const writer = writerFrom(envelope => {
    order.push(`ACK:${envelope.event.type}`);
 if(envelope.event.type==="agent.tool_use")toolUseId=`bridge-${envelope.writeId}`;
    return {ok:true,type:"committed",eventId:`bridge-${envelope.writeId}`,eventSequence:1};
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
 if(mode==="stale-custody")throw normalizeContextLoaderError({code:"superseded",sessionId:session.sessionId});
     }
     return identity.runtimeBindingToken;
    },
    acceptSandboxExecution:() => {acceptances++;order.push("sandbox-accepted");accepted.resolve();return {type:"accepted"};},
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
   if(mode==="lost-registration")session.state.clearThreadToolRoute(toolUseId);
 if(mode==="lost-content")session.state.contextManager.replaceMessages(session.state.contextManager.messages().filter(message=>message.messageSequence!==session.state.currentRequestMessage()?.assistantMessageSequence));
 releaseRefresh.resolve();
   const result = await run;
   const record={name,delayed,order,acceptances,executions,result};

   console.log(JSON.stringify(record));
   expect(order.indexOf("ACK:agent.tool_use")).toBeLessThan(order.indexOf("execution-refresh-entered"));
   if (delayed) {
    expect(order.indexOf("local-seal")).toBeLessThan(order.indexOf("execution-refresh-released"));
    expect(result.type).not.toBe("completed");
    expect(acceptances).toBe(0);
    expect(executions).toBe(0);
   } else {
    expect(order.indexOf("execution-refresh-released")).toBeLessThan(order.indexOf("provider-finish"));
    expect(result.type).not.toBe("completed");
    expect(acceptances).toBe(0);
    expect(executions).toBe(0);
   }
   } finally {releaseRefresh.resolve();accepted.resolve();await run;}

  },15000);
 }
}
