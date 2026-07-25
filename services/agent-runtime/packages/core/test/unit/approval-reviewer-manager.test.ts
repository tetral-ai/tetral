import { describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { RuntimeMessageSchema } from "../../src/contracts/runtime.js";
import { AutoApprovalReviewerManager } from "../../src/session/approval-reviewer-manager.js";
import { ContextManager } from "../../src/session/context-manager.js";

const createdAt = "2026-07-10T00:00:00.000Z";

describe("AutoApprovalReviewerManager", () => {
  test("mints one reviewer trunk id per manager lifetime", () => {
    let nextId = 0;
    const createId = (prefix: string): string => `${prefix}_${++nextId}`;
    const first = new AutoApprovalReviewerManager();
    const second = new AutoApprovalReviewerManager();

    expect(first.trunkThreadId(createId)).toBe("thrd_aprv_1");
    expect(first.trunkThreadId(createId)).toBe("thrd_aprv_1");
    expect(second.trunkThreadId(createId)).toBe("thrd_aprv_2");
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
    const first = userMessage("msg_1", 1, "first");
    const second = userMessage("msg_2", 2, "second");

    expect(manager.parentTranscriptFeed({ generation: 1, messages: [first] })).toEqual({
      reanchored: true,
      messages: [first],
    });
    manager.completeTrunkReview(
      { generation: 1, messages: [first] },
      [userMessage("reviewer_snapshot", 1, "snapshot")],
    );
    expect(manager.parentTranscriptFeed({ generation: 1, messages: [first, second] })).toEqual({
      reanchored: false,
      messages: [second],
    });
  });

  test("repeats the same delta after a failed trunk review and fully reanchors after a rewrite", () => {
    const manager = new AutoApprovalReviewerManager();
    const first = userMessage("msg_1", 1, "first");
    const second = userMessage("msg_2", 2, "second");
    manager.completeTrunkReview({ generation: 4, messages: [first] }, []);

    const delta = manager.parentTranscriptFeed({ generation: 4, messages: [first, second] });
    expect(delta.messages).toEqual([second]);
    expect(manager.parentTranscriptFeed({ generation: 4, messages: [first, second] })).toEqual(delta);
    expect(manager.parentTranscriptFeed({ generation: 5, messages: [second] })).toEqual({
      reanchored: true,
      messages: [second],
    });
  });

  test("owns the trunk snapshot and target-specific memo until disposal", () => {
    const manager = new AutoApprovalReviewerManager();
    const snapshot = [userMessage("reviewer_snapshot", 1, "snapshot")];
    const decision = {
      type: "decision" as const,
      riskLevel: "low" as const,
      userAuthorization: "high" as const,
      outcome: "allow" as const,
    };

    manager.completeTrunkReview({ generation: 1, messages: [] }, snapshot);
    manager.rememberDecision("target-specific-key", decision);
    expect(manager.trunkSnapshot()).toEqual(snapshot);
    expect(manager.decisionFor("target-specific-key")).toEqual(decision);

    manager.dispose();
    expect(manager.isDisposed()).toBe(true);
    expect(manager.trunkSnapshot()).toBeUndefined();
    expect(manager.decisionFor("target-specific-key")).toBeUndefined();
  });

  test("disposal closes the manager scope and finalizes owned reviewer work", async () => {
    const manager = new AutoApprovalReviewerManager();
    let started = false;
    let finalized = false;
    await Effect.runPromise(manager.fork(Effect.sync(() => {
      started = true;
    }).pipe(Effect.andThen(Effect.never),
      Effect.ensuring(Effect.sync(() => {
        finalized = true;
      })),
    )));

    for (let attempt = 0; attempt < 100 && !started; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    expect(started).toBe(true);

    manager.dispose();
    for (let attempt = 0; attempt < 100 && !finalized; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    expect(finalized).toBe(true);
  });
});

describe("ContextManager parent-list generation", () => {
  test("keeps generation across appends and invalidates it on list rewrites", () => {
    const manager = new ContextManager("sesn_1", [userMessage("msg_1", 1, "first")]);
    const initial = manager.messageListSnapshot();

    manager.appendMessage(userMessage("msg_2", 2, "second"));
    expect(manager.messageListSnapshot().generation).toBe(initial.generation);

    manager.replaceMessages([
      userMessage("msg_1", 1, "first"),
      userMessage("msg_2", 2, "second"),
      userMessage("msg_3", 3, "third"),
    ]);
    expect(manager.messageListSnapshot().generation).toBe(initial.generation);

    manager.updateMessage(userMessage("msg_2", 2, "second revised"));
    expect(manager.messageListSnapshot().generation).toBe(initial.generation + 1);

    manager.replaceMessagesThroughSequence(1, [userMessage("checkpoint", 1, "summary")]);
    expect(manager.messageListSnapshot().generation).toBe(initial.generation + 2);

    manager.replaceMessages([userMessage("reload", 1, "reload")]);
    expect(manager.messageListSnapshot().generation).toBe(initial.generation + 3);
  });
});

function userMessage(id: string, sequence: number, text: string) {
  return RuntimeMessageSchema.parse({
    id,
    sessionId: "sesn_1",
    role: "user",
    origin: "user",
    sequence,
    status: "completed",
    createdAt,
    parts: [{
      id: `part_${id}`,
      sessionId: "sesn_1",
      messageId: id,
      sequence: 0,
      type: "text",
      text,
      truncated: false,
      status: "completed",
      createdAt,
      completedAt: createdAt,
    }],
  });
}
