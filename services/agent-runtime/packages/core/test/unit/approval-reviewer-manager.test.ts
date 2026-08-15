import { describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { RuntimeContextEntrySchema } from "../../src/contracts/runtime.js";
import { AutoApprovalReviewerManager } from "../../src/session/approval-reviewer-manager.js";
import { ContextManager } from "../../src/session/context-manager.js";

describe("AutoApprovalReviewerManager", () => {
	test("mints one reviewer trunk ensure operation per manager lifetime", () => {
		let nextId = 0;
		const createId = (prefix: string): string => `${prefix}_${++nextId}`;
		const first = new AutoApprovalReviewerManager();
		const second = new AutoApprovalReviewerManager();

		expect(first.trunkEnsureOperationId(createId)).toBe("aprv_ensure_1");
		expect(first.trunkEnsureOperationId(createId)).toBe("aprv_ensure_1");
		expect(second.trunkEnsureOperationId(createId)).toBe("aprv_ensure_2");
		expect(nextId).toBe(2);
	});

	test("prefers the trunk and routes overlapping reviews to sidecars", () => {
		const manager = new AutoApprovalReviewerManager();

		const trunk = manager.beginReview("review_1");
		const sidecar = manager.beginReview("review_2");

		expect(trunk.kind).toBe("trunk");
		expect(sidecar.kind).toBe("sidecar");
		expect(manager.ephemeralReviewIds()).toEqual(["review_2"]);
		sidecar.release();
		trunk.release();
		expect(manager.beginReview("review_3").kind).toBe("trunk");
	});

	test("linearizes cancellation and outcome once for the exact execution token", () => {
		const manager = new AutoApprovalReviewerManager();
		const cancelled = manager.beginReview("review_cancelled");
		const cancelledToken = {
			reviewId: "review_cancelled",
			reviewerThreadId: "thrd_reviewer_cancelled",
			runId: 41,
		};

		expect(cancelled.installExecutionToken(cancelledToken)).toBe(true);
		expect(cancelled.cancel()).toBe(true);
		expect(cancelled.claimOutcome()).toBe(false);
		expect(cancelled.raceState()).toBe("cancellation_won");
		expect(cancelled.executionToken()).toEqual(cancelledToken);
		expect(manager.executionState("review_cancelled")).toMatchObject({
			kind: "trunk",
			raceState: "cancellation_won",
			token: cancelledToken,
		});
		cancelled.release();
		cancelled.release();
		expect(manager.executionState("review_cancelled")).toBeUndefined();

		const outcome = manager.beginReview("review_outcome");
		expect(outcome.claimOutcome()).toBe(true);
		expect(outcome.cancel()).toBe(false);
		expect(outcome.raceState()).toBe("outcome_won");
		outcome.release();
		const afterSettlement = manager.beginReview("review_after_settlement");
		expect(afterSettlement.kind).toBe("trunk");
		afterSettlement.release();
	});

	test("feeds full transcript first and only the cursor delta after trunk completion", () => {
		const manager = new AutoApprovalReviewerManager();
		const first = userEntry("msg_1", 1, "first");
		const second = userEntry("msg_2", 2, "second");

		expect(
			manager.parentTranscriptFeed({ generation: 1, entries: [first] }),
		).toEqual({
			reanchored: true,
			entries: [first],
		});
		manager.completeTrunkReview({ generation: 1, entries: [first] });
		expect(
			manager.parentTranscriptFeed({ generation: 1, entries: [first, second] }),
		).toEqual({
			reanchored: false,
			entries: [second],
		});
	});

	test("repeats the same delta after a failed trunk review and fully reanchors after a rewrite", () => {
		const manager = new AutoApprovalReviewerManager();
		const first = userEntry("msg_1", 1, "first");
		const second = userEntry("msg_2", 2, "second");
		manager.completeTrunkReview({ generation: 4, entries: [first] });

		const delta = manager.parentTranscriptFeed({
			generation: 4,
			entries: [first, second],
		});
		expect(delta.entries).toEqual([second]);
		expect(
			manager.parentTranscriptFeed({ generation: 4, entries: [first, second] }),
		).toEqual(delta);
		expect(
			manager.parentTranscriptFeed({ generation: 5, entries: [second] }),
		).toEqual({
			reanchored: true,
			entries: [second],
		});
	});

	test("owns target-specific memo until disposal", async () => {
		const manager = new AutoApprovalReviewerManager();
		const decision = {
			type: "decision" as const,
			riskLevel: "low" as const,
			userAuthorization: "high" as const,
			outcome: "allow" as const,
		};

		manager.completeTrunkReview({ generation: 1, entries: [] });
		manager.rememberDecision("target-specific-key", decision);
		expect(manager.decisionFor("target-specific-key")).toEqual(decision);

		await Effect.runPromise(manager.dispose());
		expect(manager.isDisposed()).toBe(true);
		expect(manager.decisionFor("target-specific-key")).toBeUndefined();
	});

	test("disposal awaits asynchronous finalizers for owned reviewer work", async () => {
		const manager = new AutoApprovalReviewerManager();
		let started = false;
		let finalized = false;
		let releaseFinalizer = (): void => undefined;
		const finalizerGate = new Promise<void>((resolve) => {
			releaseFinalizer = resolve;
		});
		await Effect.runPromise(
			manager.fork(
				Effect.sync(() => {
					started = true;
				}).pipe(
					Effect.andThen(Effect.never),
					Effect.ensuring(
						Effect.promise(async () => {
							await finalizerGate;
							finalized = true;
						}),
					),
				),
			),
		);

		for (let attempt = 0; attempt < 100 && !started; attempt += 1) {
			await new Promise((resolve) => setTimeout(resolve, 1));
		}
		expect(started).toBe(true);

		let disposalFinished = false;
		const disposal = Effect.runPromise(manager.dispose()).then(() => {
			disposalFinished = true;
		});
		await Bun.sleep(0);
		expect(disposalFinished).toBe(false);
		releaseFinalizer();
		await disposal;
		expect(finalized).toBe(true);
	});
});

describe("ContextManager parent-list generation", () => {
	test("keeps generation across appends and invalidates it on list rewrites", () => {
		const manager = new ContextManager("sesn_1", [
			userEntry("msg_1", 1, "first"),
		]);
		const initial = manager.entryListSnapshot();

		manager.appendEntry(userEntry("msg_2", 2, "second"));
		expect(manager.entryListSnapshot().generation).toBe(initial.generation);

		manager.replaceEntries([
			userEntry("msg_1", 1, "first"),
			userEntry("msg_2", 2, "second"),
			userEntry("msg_3", 3, "third"),
		]);
		expect(manager.entryListSnapshot().generation).toBe(initial.generation);

		manager.updateEntry(userEntry("msg_2", 2, "second revised"));
		expect(manager.entryListSnapshot().generation).toBe(initial.generation + 1);

		manager.replaceEntriesThroughSequence(1, [
			userEntry("checkpoint", 1, "summary"),
		]);
		expect(manager.entryListSnapshot().generation).toBe(initial.generation + 2);

		manager.replaceEntries([userEntry("reload", 1, "reload")]);
		expect(manager.entryListSnapshot().generation).toBe(initial.generation + 3);
	});
});

function userEntry(_id: string, sequence: number, text: string) {
	return RuntimeContextEntrySchema.parse({
		messageSequence: sequence,
		contextKind: "user",
		parts: [{ type: "text", text }],
	});
}
