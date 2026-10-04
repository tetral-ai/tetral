import { expect, test } from "bun:test";
import { Effect, Exit, Fiber, Stream } from "effect";
import { normalizeRuntimeFailure } from "../../src/contracts/runtime.js";
import type { LLMServiceError } from "../../src/llm/llm-service.js";
import { RuntimeFailureSchema } from "../../src/llm/llm-event.js";
import type { LLMEvent } from "../../src/llm/llm-event.js";
import type { RuntimeMetricsSink, RuntimeOperationObservation } from "../../src/runtime/metrics.js";
import { NoopRuntimeMetricsSink } from "../../src/runtime/metrics.js";
import { RequestContentProcessor } from "../../src/runtime/accumulator.js";
import { AutoApprovalReviewerManager } from "../../src/session/approval-reviewer-manager.js";
import * as ThreadLoop from "../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../src/thread-loop/thread-runtime.js";
import { RuntimePodMetricsRegistry } from "../../../runtime-pod/src/metrics.js";
import {
 deferred,
 RecordingContextLoader,
 runtimeThreadLoopLayer,
 testRunCustody,
 threadLoopRuntime,
 userMessage,
 waitForCondition,
 writerFrom,
} from "./thread-loop/thread-loop-test-support.js";

// The controlled clock advances at the real adapter boundaries. Provider Finish
// waits for the first actual settlement so ACK/application spans cannot absorb
// concurrent execution time. These are local stage durations, not service latency.
for (const observer of ["recording", "throwing", "disabled"] as const) {
 test(`actual Tool continuation and End stages keep ${observer} observers outside custody`, async () => {
  let clock = 0;
  let requests = 0;
  let acceptances = 0;
  let executions = 0;
  let settlements = 0;
  let settled = false;
  const operations: RuntimeOperationObservation[] = [];
  const content: Parameters<NonNullable<ThreadLoop.ThreadLoopRuntimeOptions["recordContentCommit"]>>[0][] = [];
  const registry = new RuntimePodMetricsRegistry();
  const throwObserver = () => { throw new Error("metrics observer failure"); };
  const throwingSink: RuntimeMetricsSink = {
   recordHotState: throwObserver, addActiveToolFibers: throwObserver,
   addPendingApprovals: throwObserver, observeProviderStreamDuration: throwObserver,
   observeEventWriteLatency: throwObserver, observeContextLoadLatency: throwObserver,
   recordCleanupCommandOutcome: throwObserver, recordContentSubmissionDelta: throwObserver,
   observeContentCommitLatency: throwObserver, observeContinuationLatency: throwObserver,
  };
  const session = new ThreadRuntime("sesn_content_metrics");
  const applyFact = session.state.applyThreadTurnFact.bind(session.state);
  session.state.applyThreadTurnFact = fact => {
   const result = applyFact(fact);
   if (fact.fact === "request_ended") clock += 5;
   return result;
  };
  const base = writerFrom(
   envelope => ({ ok: true, type: "committed", eventId: envelope.preallocatedEventId ?? `bridge-${envelope.writeId}` }),
   undefined, [], undefined,
   async () => { clock += 2; settlements += 1; settled = true; return { ok: true, result: { type: "committed" } }; },
  );
  const writer = { ...base, writeRequestEnd: async (envelope: Parameters<typeof base.writeRequestEnd>[0]) => {
   const result = await base.writeRequestEnd(envelope);
   clock += 13;
   return result;
  } };
  const loader = new RecordingContextLoader([], { type: "context", entries: [userMessage("user", 0, "read the file")] });
  const result = await Effect.runPromise(Effect.gen(function* () {
   return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
  }).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
   writer,
   runtime: { ...threadLoopRuntime(), monotonicMs: () => clock },
   metrics: observer === "recording" ? registry : observer === "throwing" ? throwingSink : NoopRuntimeMetricsSink,
   recordOperation: observation => { if (observer === "throwing") throwObserver(); operations.push(observation); },
   recordContentCommit: observation => { if (observer === "throwing") throwObserver(); content.push(observation); },
   refreshRuntimeBindingToken: async () => { clock += 3; return "binding-token"; },
   acceptSandboxExecution: async () => { acceptances += 1; clock += 5; return { type: "accepted" }; },
   awaitSandboxExecution: async () => { executions += 1; clock += 11; return { type: "completed", output: { text: "file bytes", truncated: false } }; },
   llmService: { stream() {
    requests += 1;
    const first = requests === 1;
    return Stream.fromAsyncIterable((async function* (): AsyncGenerator<LLMEvent> {
     if (first) {
      yield { type: "tool-call-complete", id: "read", toolName: "Read", input: { file_path: "/workspace/note.txt" }, inputPreview: { preview: "{}", truncated: false } };
      await waitForCondition(() => settled, "actual first Tool settlement");
      yield { type: "finish", finishReason: "tool-calls" };
     } else {
      yield { type: "text-complete", providerPartId: "done", eventId: "evt_00000000000000000000000000000001", text: "done" };
      yield { type: "finish", finishReason: "stop" };
     }
    })(), (error): LLMServiceError => ({ type: "llm-service", error: RuntimeFailureSchema.parse(normalizeRuntimeFailure({ type: "runtime", code: "runtime_invalid_sequence", retryable: false, fatal: true, rawError: error })) }));
   } },
  }))));
  expect(result).toMatchObject({ type: "completed" });
  expect({ requests, acceptances, executions, settlements }).toEqual({ requests: 2, acceptances: 1, executions: 1, settlements: 1 });
  expect(session.state.activeTools()).toHaveLength(0);
  if (observer !== "throwing") {
   expect(operations.map(({ operation, durationMs, outcome, requestKind }) => ({ operation, durationMs, outcome, requestKind }))).toEqual([
    { operation: "binding_refresh", durationMs: 3, outcome: "success", requestKind: "agent_provider_request" },
    { operation: "permit_wait", durationMs: 0, outcome: "success", requestKind: "agent_provider_request" },
    { operation: "binding_refresh", durationMs: 3, outcome: "success", requestKind: "agent_provider_request" },
    { operation: "tool_accept", durationMs: 5, outcome: "success", requestKind: "agent_provider_request" },
    { operation: "tool_settle", durationMs: 2, outcome: "success", requestKind: "agent_provider_request" },
    { operation: "member_barrier_wait", durationMs: 0, outcome: "success", requestKind: "agent_provider_request" },
    { operation: "binding_refresh", durationMs: 3, outcome: "success", requestKind: "agent_provider_request" },
    { operation: "member_barrier_wait", durationMs: 0, outcome: "success", requestKind: "agent_provider_request" },
   ]);
   const ends = content.filter(sample => sample.kind === "request_end");
   expect(ends.map(({ phase, durationMs, outcome }) => ({ phase, durationMs, outcome }))).toEqual([
    { phase: "request_end_commit", durationMs: 13, outcome: "committed" },
    { phase: "request_end_apply", durationMs: 5, outcome: "committed" },
    { phase: "request_end_commit", durationMs: 13, outcome: "committed" },
    { phase: "request_end_apply", durationMs: 5, outcome: "committed" },
   ]);
  }
  if (observer === "recording") {
   expect(registry.snapshot().pendingContentEntries).toBe(0);
   expect(registry.snapshot().pendingContentBytes).toBe(0);
   expect(registry.snapshot().activeToolFibers).toBe(0);
   expect(registry.snapshot().continuationLatencyMs?.size).toBe(5);
  }
 });
}

for (const fail of [false, true]) {
 test(`actual pre-End member barrier ${fail ? "drain failure" : "release"} is separate from submission`, async () => {
  const reviewerEntered = deferred<void>();
  const releaseReviewer = deferred<void>();
  const barrierEntered = deferred<void>();
  let reviewerOwner: Promise<void> | undefined;
  let clock = 100;
  let finishConsumed = false;
  let postFinishClockReads = 0;
  let ends = 0;
  const samples: RuntimeOperationObservation[] = [];
  const registry = new RuntimePodMetricsRegistry();
  const session = new ThreadRuntime(`sesn_member_wait_${fail}`, new AutoApprovalReviewerManager());
  const base = writerFrom(envelope => ({ ok: true, type: "committed", eventId: envelope.preallocatedEventId ?? `bridge-${envelope.writeId}` }));
  const fiber = Effect.runFork(Effect.gen(function* () {
   return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
  }).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader([], {
   type: "context", entries: [userMessage("user", 0, "write the file")],
  }), {
   approvalMode: "approve_for_me", metrics: registry,
   writer: { ...base, writeRequestEnd: envelope => { ends++; return base.writeRequestEnd(envelope); } },
   runtime: { ...threadLoopRuntime(), monotonicMs: () => {
    // After actual Finish consumption: provider-duration observation, then the
    // owning member barrier's start. The clock stays fixed through both reads.
    if (finishConsumed && ++postFinishClockReads === 2) barrierEntered.resolve();
    return clock;
   } },
   recordOperation: sample => samples.push(sample),
   createProcessor: options => {
    const processor = new RequestContentProcessor(options);
    const drain = processor.awaitAssistantMembersDrained.bind(processor);
    processor.awaitAssistantMembersDrained = async () => {
     await drain();
     if (fail) throw new Error("member drain observation boundary failed");
    };
    const process = processor.process.bind(processor);
    processor.process = async (envelope, signal) => {
     const result = await process(envelope, signal);
     if (envelope.event.type === "finish") finishConsumed = true;
     return result;
    };
    return processor;
   },
   events: [
    { type: "tool-call-complete", id: "write", toolName: "Write", input: { file_path: "/workspace/note.txt", content: "first\n" }, inputPreview: { preview: "{}", truncated: false } },
    { type: "finish", finishReason: "tool-calls" },
   ],
   reviewApproval: () => Effect.promise(async () => {
    reviewerEntered.resolve();
    reviewerOwner = releaseReviewer.promise;
    await reviewerOwner;
    return { type: "decision" as const, riskLevel: "low" as const, userAuthorization: "high" as const, outcome: "allow" as const };
   }),
  }))));
  let watchdog: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_resolve, reject) => {
   watchdog = setTimeout(() => reject(new Error("held member barrier was not reached")), 2000);
  });
  try {
   await Promise.race([Promise.all([reviewerEntered.promise, barrierEntered.promise]), deadline, Effect.runPromise(Fiber.await(fiber)).then(() => { throw new Error("run completed before held member barrier"); })]);
   expect(ends).toBe(0);
   expect(registry.snapshot().activeToolFibers).toBe(1);
   expect(samples.filter(sample => sample.operation === "member_barrier_wait")).toEqual([]);
   clock += 37;
   releaseReviewer.resolve();
   if (fail) expect(Exit.isFailure(await Effect.runPromise(Fiber.await(fiber)))).toBe(true);
   else expect(await Effect.runPromise(Fiber.join(fiber))).toMatchObject({ type: "completed" });
   const waits = samples.filter(sample => sample.operation === "member_barrier_wait");
   expect(waits[0]).toMatchObject({ operation: "member_barrier_wait", durationMs: 37, outcome: fail ? "error" : "success" });
   expect(waits.filter(sample => sample.durationMs === 37)).toHaveLength(1);
   expect(registry.snapshot().activeToolFibers).toBe(0);
  } finally {
   clearTimeout(watchdog);
   releaseReviewer.resolve();
   await reviewerOwner;
   await Effect.runPromise(Fiber.interrupt(fiber));
  }
 }, 10000);
}
