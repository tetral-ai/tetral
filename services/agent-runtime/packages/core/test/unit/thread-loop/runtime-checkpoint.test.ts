import { expect, test } from "bun:test";
import { Effect } from "effect";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import {
	RecordingContextLoader,
	runtimeThreadLoopLayer,
	testRunCustody,
	userMessage,
} from "./thread-loop-test-support.js";

test("quiesce preserves the checkpoint signal already captured by an admitted reviewer dependency", () => {
	const session = new ThreadRuntime("sesn_reviewer_read_checkpoint");
	const captured = session.state.checkpointSignal()!;
	expect(captured.aborted).toBe(false);
	session.state.beginRuntimeQuiesce(() => true);
	expect(captured.aborted).toBe(false);
	expect(session.state.runtimeCheckpointYieldRequested()).toBe(false);
	expect(session.state.checkpointSignal()).toBeUndefined();
	session.state.beginRuntimeQuiesce(() => false);
	expect(captured.aborted).toBe(true);
	expect(session.state.runtimeCheckpointYieldRequested()).toBe(true);
});

test("quiesce completes the admitted response and yields its durable checkpoint without idle closeout", async () => {
	const session = new ThreadRuntime("sesn_checkpoint");
	let requests = 0;
	const loader = new RecordingContextLoader([], {
		type: "context", entries: [userMessage("msg_checkpoint", 0, "run")],
	});
	const result = await Effect.runPromise(Effect.gen(function* () {
		const loop = yield* ThreadLoop.Service;
		return yield* loop.run(session, testRunCustody());
	}).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
		onStream: () => {
			requests += 1;
			session.state.beginRuntimeQuiesce();
		},
	}))));
	expect(result).toEqual({ type: "checkpoint_yield" });
	expect(requests).toBe(1);
	const checkpoint = session.state.threadTurnTransition().checkpoint;
	expect(checkpoint.request?.requestEnd).toBeDefined();
	expect(checkpoint.executionRunId).toBeDefined();
});

test("quiesce before admission preserves pending input and performs no provider dispatch", async () => {
	const session = new ThreadRuntime("sesn_checkpoint");
	session.state.beginRuntimeQuiesce();
	let requests = 0;
	const loader = new RecordingContextLoader([], {
		type: "context", entries: [userMessage("msg_checkpoint", 0, "run")],
	});
	const result = await Effect.runPromise(Effect.gen(function* () {
		const loop = yield* ThreadLoop.Service;
		return yield* loop.run(session, testRunCustody());
	}).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
		onStream: () => { requests += 1; },
	}))));
	expect(result).toEqual({ type: "checkpoint_yield" });
	expect(requests).toBe(0);
	expect(session.state.threadTurnTransition().checkpoint.request).toBeUndefined();
});
