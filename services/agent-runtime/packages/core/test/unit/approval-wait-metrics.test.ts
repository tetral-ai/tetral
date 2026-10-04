import { expect, test } from "bun:test";
import { Context, Effect, Exit, Layer, Scope } from "effect";
import type { RuntimeMetricsSink, RuntimeOperationObservation } from "../../src/runtime/metrics.js";
import { NoopRuntimeMetricsSink } from "../../src/runtime/metrics.js";
import * as SessionManager from "../../src/session/session-manager.js";
import * as ThreadLoop from "../../src/thread-loop/thread-loop.js";
import { AutoApprovalReviewerManager } from "../../src/session/approval-reviewer-manager.js";
import { ThreadRuntime } from "../../src/thread-loop/thread-runtime.js";
import { RuntimePodMetricsRegistry } from "../../../runtime-pod/src/metrics.js";
import {
	acceptedInput,
	RecordingContextLoader,
	runtimeThreadLoopLayer,
	testRunCustody,
	threadLoopRuntime,
	userMessage,
} from "./thread-loop/thread-loop-test-support.js";

for (const outcome of ["allow", "deny", "evict"] as const) {
	test(`actual human approval ${outcome} closes one hot observation without changing durable ownership`, async () => {
		let clock = 100;
		const registry = new RuntimePodMetricsRegistry();
		const observations: RuntimeOperationObservation[] = [];
		const session = new ThreadRuntime(`sesn_approval_${outcome}`);
		const loader = new RecordingContextLoader([], {
			type: "context", entries: [userMessage("user", 0, "write the file")],
		});
		const layer = runtimeThreadLoopLayer(loader, {
			approvalMode: "ask_for_approval", metrics: registry,
			runtime: { ...threadLoopRuntime(), monotonicMs: () => clock },
			recordOperation: sample => observations.push(sample),
			events: [
				{ type: "tool-call-complete", id: "write", toolName: "Write",
					input: { file_path: "/workspace/note.txt", content: "first\n" },
					inputPreview: { preview: "{}", truncated: false } },
				{ type: "finish", finishReason: "tool-calls" },
			],
		});
		const run = await Effect.runPromise(Effect.gen(function* () {
			return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
		}).pipe(Effect.provide(layer)));
		expect(run.type).toBe("completed");
		expect(session.state.threadTurnTransition().checkpoint.idleCloseout?.stopReason).toBe("requires_action");
		const pending = session.state.pendingApprovalToolJobs()[0]!;
		expect(pending.job.approvalSource).toBe("user");
		expect(observations.filter(sample => sample.operation === "approval_wait")).toEqual([]);
		expect([...registry.snapshot().approvalWaitStarted!.values()]).toEqual([1]);
		expect([...registry.snapshot().approvalWaitOutstanding!.values()]).toEqual([1]);
		const coldBaseline = {
			messages: session.state.contextManager.messages(),
			currentRequestMessage: session.state.currentRequestMessage()!,
			turnCheckpoint: session.state.threadTurnTransition().checkpoint,
			turnToolRouteView: { routes: session.state.activeTools().map(tool => ({
				toolUseEventId: tool.toolUseEventId, disposition: tool.disposition,
			})) },
		};
		clock += 37;
		if (outcome === "evict") {
			// This is observation disposal, not a durable approval cancellation.
			session.state.clearAfterCustodyHandoff();
		} else {
			const confirmation = { ...acceptedInput(`confirm_${outcome}`, session.sessionId),
				toolUseEventId: pending.toolUseEventId, decision: outcome };
			const commit = async () => ({ ok: true as const, type: "committed" as const,
				assignedContextSequences: [3], pendingAttachments: [], interruptToolResults: [] });
			expect(await Effect.runPromise(ThreadLoop.settleToolConfirmation(session, confirmation, commit))).toMatchObject({ type: "applied" });
			expect(await Effect.runPromise(ThreadLoop.settleToolConfirmation(session, confirmation, commit))).toMatchObject({ type: "duplicate" });
			session.state.clearAfterCustodyHandoff();
		}
		const waits = observations.filter(sample => sample.operation === "approval_wait");
		expect(waits).toEqual([expect.objectContaining({ operation: "approval_wait", approvalSource: "user",
			durationMs: 37, outcome: outcome === "allow" ? "success" : outcome === "deny" ? "rejected" : "cancelled" })]);
		expect([...registry.snapshot().approvalWaitOutstanding!.values()]).toEqual([0]);
		expect([...registry.snapshot().continuationLatencyMs!.entries()].filter(([key]) => key.includes("approval_wait")))
			.toEqual([[expect.stringContaining('approval_source="user"'), { count: 1, sum: 37 }]]);

		if (outcome !== "evict") return;
		const coldRegistry = new RuntimePodMetricsRegistry();
		const coldObservations: RuntimeOperationObservation[] = [];
		const managerLayer = SessionManager.layer({
			maxLocalSessions: 1, now: () => "2026-10-03T00:00:00.000Z",
			metrics: coldRegistry, closeoutMonotonicMs: () => 999,
			recordOperation: sample => coldObservations.push(sample),
		}).pipe(Layer.provide(runtimeThreadLoopLayer(loader, { installLoaderState: false })));
		const scope = await Effect.runPromise(Scope.make());
		try {
			const context = await Effect.runPromise(Layer.buildWithScope(managerLayer, scope));
			const manager = Context.get(context, SessionManager.Service);
			const loaded = await Effect.runPromise(manager.preloadThread({
				...session.identity, ...coldBaseline, runtimeBindingToken: "binding-token",
				thread: { role: "main", visibility: "public", status: "idle", agentType: "general" },
				pendingToolUses: [{ toolUseEventId: pending.toolUseEventId, modelRequestId: pending.modelRequestId,
					modelToolCallId: pending.job.modelToolCallId, toolName: pending.job.name,
					input: pending.job.input, status: "pending" }],
			}));
			expect(loaded).toMatchObject({ ok: true, applied: true });
			const coldWaits = coldObservations.filter(sample => sample.operation === "approval_wait");
			expect(coldWaits).toEqual([expect.objectContaining({ operation: "approval_wait", approvalSource: "user",
				outcome: "unavailable", timingUnavailable: true })]);
			expect(coldWaits[0]).not.toHaveProperty("durationMs");
			expect(coldRegistry.snapshot().approvalWaitStarted!.size).toBe(0);
			expect(coldRegistry.snapshot().approvalWaitOutstanding!.size).toBe(0);
			expect([...coldRegistry.snapshot().approvalWaitUnavailable!.values()]).toEqual([1]);
			expect([...coldRegistry.snapshot().continuationLatencyMs!.keys()].filter(key => key.includes("approval_wait"))).toEqual([]);
		} finally {
			await Effect.runPromise(Scope.close(scope, Exit.void));
		}
	});
}

for (const observer of ["disabled", "throwing"] as const) {
	test(`actual human wait keeps ${observer} observers outside decision and disposal custody`, async () => {
		const fail = () => { throw new Error("approval observer rejected"); };
		const throwing: RuntimeMetricsSink = { ...NoopRuntimeMetricsSink,
			recordApprovalWaitDelta: fail, observeContinuationLatency: fail,
			recordApprovalWaitUnavailable: fail };
		const session = new ThreadRuntime(`sesn_approval_${observer}`);
		const result = await Effect.runPromise(Effect.gen(function* () {
			return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
		}).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader([], {
			type: "context", entries: [userMessage("user", 0, "write")],
		}), {
			approvalMode: "ask_for_approval", metrics: observer === "throwing" ? throwing : NoopRuntimeMetricsSink,
			...(observer === "throwing" ? { recordOperation: fail } : {}),
			events: [{ type: "tool-call-complete", id: "write", toolName: "Write",
				input: { file_path: "/workspace/note.txt", content: "first\n" }, inputPreview: { preview: "{}", truncated: false } },
				{ type: "finish", finishReason: "tool-calls" }],
		}))));
		expect(result.type).toBe("completed");
		expect(session.state.pendingApprovalToolJobs()).toHaveLength(1);
		const toolUseEventId = session.state.pendingApprovalToolJobs()[0]!.toolUseEventId;
		const confirmation = { ...acceptedInput(`confirm_${observer}`, session.sessionId), toolUseEventId, decision: "allow" as const };
		expect(await Effect.runPromise(ThreadLoop.settleToolConfirmation(session, confirmation, async () => ({
			ok: true, type: "committed", assignedContextSequences: [3], pendingAttachments: [], interruptToolResults: [],
		})))).toMatchObject({ type: "applied", wakeThread: true });
		session.state.clearAfterCustodyHandoff();
		expect(session.state.activeTools()).toEqual([]);
	});
}

for (const outcome of ["allow", "deny"] as const) {
 test(`actual reviewer ${outcome} classifies its hot approval span`, async () => {
  let clock = 100;
  let executions = 0;
  const registry = new RuntimePodMetricsRegistry();
  const observations: RuntimeOperationObservation[] = [];
  const session = new ThreadRuntime(`sesn_reviewer_${outcome}`, new AutoApprovalReviewerManager());
  const result = await Effect.runPromise(Effect.gen(function* () {
   return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
  }).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader([], {
   type: "context", entries: [userMessage("user", 0, "write the file")],
  }), {
   approvalMode: "approve_for_me", metrics: registry,
   runtime: { ...threadLoopRuntime(), monotonicMs: () => clock },
   recordOperation: sample => observations.push(sample),
   events: [{ type: "tool-call-complete", id: "write", toolName: "Write",
    input: { file_path: "/workspace/note.txt", content: "first\n" }, inputPreview: { preview: "{}", truncated: false } },
    { type: "finish", finishReason: "tool-calls" }],
   reviewApproval: () => Effect.sync(() => {
    expect([...registry.snapshot().approvalWaitOutstanding!.values()]).toEqual([1]);
    clock += 37;
    return { type: "decision" as const, riskLevel: "low" as const, userAuthorization: "high" as const, outcome };
   }),
   runTool: () => {
    executions += 1;
    return { type: "completed", output: { text: "done", truncated: false } };
   },
  }))));
  expect(result.type).toBe("completed");
  expect(executions).toBe(outcome === "allow" ? 1 : 0);
  expect(observations.filter(sample => sample.operation === "approval_wait")).toEqual([
   expect.objectContaining({ approvalSource: "auto_reviewer", durationMs: 37,
    outcome: outcome === "allow" ? "success" : "rejected" }),
  ]);
  expect([...registry.snapshot().approvalWaitStarted!.values()]).toEqual([1]);
  expect([...registry.snapshot().approvalWaitOutstanding!.values()]).toEqual([0]);
  expect(session.state.activeTools()).toEqual([]);
 });
}
