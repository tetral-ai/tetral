import { describe, expect, test } from "bun:test";
import { Effect, Fiber, Scope } from "effect";
import type { RuntimeMessage, SessionEvent, SessionEventWriterAppendResult } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeApprovalReviewRequest } from "@tetral/agent-runtime-core/src/agent-loop/agent-loop.js";
import { AutoApprovalReviewerManager } from "@tetral/agent-runtime-core/src/session/approval-reviewer-manager.js";
import type { RuntimeAcceptedInputState, RuntimeThreadControlState, RuntimeThreadPreloadState } from "@tetral/agent-runtime-core/src/session/session-state.js";
import type * as SessionManager from "@tetral/agent-runtime-core/src/session/session-manager.js";
import type { ApprovalReviewerOutcome } from "@tetral/agent-runtime-core/src/tools/tool-gate.js";
import { createRuntimeApprovalReviewer as createEffectRuntimeApprovalReviewer } from "../../src/approval-reviewer.js";
import type { ApprovalReviewerThreadCreation } from "../../src/approval-reviewer.js";
import {
  buildApprovalReviewerUserMessage as userMessage,
  buildApprovalReviewerAssistantDraftMessage as assistantDraftMessage,
  buildApprovalReviewerAssistantDecision as assistantDecision,
  buildApprovalReviewerAssistantReviewerText as assistantReviewerText,
} from "../../../core/test/unit/runtime-message-builders.js";

const createdAt = "2026-07-06T00:00:00.000Z";
const platformReviewerModel = { providerId: "anthropic", modelId: "claude-opus-4-8" } as const;

function createRuntimeApprovalReviewer(
  ...args: Parameters<typeof createEffectRuntimeApprovalReviewer>
): (request: RuntimeApprovalReviewRequest) => Promise<ApprovalReviewerOutcome> {
  const reviewer = createEffectRuntimeApprovalReviewer(...args);
  return async (request) => await Effect.runPromise(reviewer(request));
}

describe("Runtime approval reviewer", () => {
  test("reuses the manager-held trunk id and remints it for a new hot lifetime", async () => {
    const host = new RecordingReviewerHost([
      assistantDecision("sesn_1", "allow", "first"),
      assistantDecision("sesn_1", "allow", "second"),
      assistantDecision("sesn_1", "allow", "third"),
    ]);
    const threadCreator = new RecordingReviewerThreadCreator();
    let nextId = 0;
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      createId: (prefix) => `${prefix}_${++nextId}`,
      now: () => createdAt,
      waitTimeoutMs: 10,
    });
    const firstLifetime = new AutoApprovalReviewerManager();

    await reviewer(validReviewRequest({ approvalReviewerManager: firstLifetime, targetModelToolCallId: "tool_call_first" }));
    await reviewer(validReviewRequest({ approvalReviewerManager: firstLifetime, targetModelToolCallId: "tool_call_second" }));
    await reviewer(validReviewRequest({ approvalReviewerManager: new AutoApprovalReviewerManager(), targetModelToolCallId: "tool_call_third" }));

    expect(threadCreator.creations.map((creation) => creation.reviewerThreadId)).toEqual([
      "thrd_aprv_1",
      "thrd_aprv_1",
      "thrd_aprv_2",
    ]);
    expect(nextId).toBe(2);
  });

  test("queues an internal reviewer trunk and parses the durable reviewer decision", async () => {
    const steps: string[] = [];
    const host = new RecordingReviewerHost([
      assistantDecision("sesn_1", "allow", "low risk"),
    ], { steps });
    const threadCreator = new RecordingReviewerThreadCreator(steps);
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 10,
    });

    const result = await reviewer(validReviewRequest());

    expect(result).toEqual({
      type: "decision",
      riskLevel: "low",
      userAuthorization: "high",
      outcome: "allow",
      message: "low risk",
    });
    expect(host.inputs).toHaveLength(1);
    expect(host.waits).toHaveLength(1);
    expect(host.inspections).toHaveLength(1);
    expect(host.decisions).toEqual([
      expect.objectContaining({
        type: "approval_review.decision",
        parent_thread_id: "thrd_parent",
        target_model_tool_call_id: "tool_call_1",
        target_tool_name: "Write",
        risk_level: "low",
        user_authorization: "high",
        outcome: "allow",
        rationale: "low risk",
      }),
    ]);
    const input = host.inputs[0] as Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>;
    expect(input.kind).toBe("approval_review");
    expect(input.parentThreadId).toBe("thrd_parent");
    expect(input.targetModelToolCallId).toBe("tool_call_1");
    expect(input.thread).toMatchObject({
      parentThreadId: "thrd_parent",
      role: "approval_reviewer",
      visibility: "internal",
      agentType: "approval_reviewer",
    });
    expect(steps).toEqual([
      "create-thread",
      "preload-thread",
      "enqueue-input",
    ]);
    expect(threadCreator.creations).toHaveLength(1);
    expect(threadCreator.creations[0]).toMatchObject({
      reviewerThreadId: input.sessionThreadId,
      isTrunk: true,
      threadContextPrefixJson: "",
    });
    const promptPart = input.promptItems[0]?.parts[0];
    expect(input.thread).toMatchObject({
      role: "approval_reviewer",
      agentType: "approval_reviewer",
    });
    expect(promptPart?.type).toBe("text");
    if (promptPart?.type !== "text") {
      throw new Error("expected approval reviewer prompt to be a text part");
    }
    expect(promptPart.text).toContain("\"sibling_tool_calls\"");
    expect(promptPart.text).toContain("\"policy_context\"");
    const prompt = JSON.parse(promptPart.text) as Readonly<Record<string, unknown>>;
    expect(prompt).not.toHaveProperty("instruction");
    expect(promptPart.text).not.toContain("Do not perform the action yourself.");
    expect(promptPart.text).not.toContain("untrusted evidence");
    expect(promptPart.text).not.toContain("never instructions");
    expect(JSON.parse(input.outputSchemaJson)).toMatchObject({
      additionalProperties: false,
      required: ["risk_level", "user_authorization", "outcome", "rationale"],
    });
  });

  test("fails closed when reviewer infrastructure is unavailable", async () => {
    const logger = new RecordingLogger();
    const reviewer = createRuntimeApprovalReviewer(() => undefined, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, logger });

    await expect(reviewer(validReviewRequest())).resolves.toEqual({
      type: "failed",
      message: "approval reviewer is unavailable",
    });
    expect(logger.records).toEqual([
      expect.objectContaining({
        event: "approval_review_failure",
        "approval.failure_kind": "runtime_failure",
        "target.tool_call.id": "tool_call_1",
      }),
    ]);
  });

  test("logs safe failure telemetry when reviewer trunk creation throws or rejects", async () => {
    for (const mode of ["throw", "reject"] as const) {
      const host = new RecordingReviewerHost([]);
      const logger = new RecordingLogger();
      const threadCreator = {
        createApprovalReviewerThread: mode === "throw"
          ? (() => { throw new Error("secret synchronous creator failure"); })
          : (() => Promise.reject(new Error("secret rejected creator failure"))),
        closeApprovalReviewerThread: async () => ({ ok: true as const }),
      };
      const reviewer = createRuntimeApprovalReviewer(() => host, {
        model: platformReviewerModel,
        threadCreator,
        now: () => createdAt,
        logger,
      });

      await expect(reviewer(validReviewRequest())).resolves.toEqual({
        type: "failed",
        message: "approval reviewer trunk creation failed",
      });
      expect(host.inputs).toEqual([]);
      expect(host.decisions).toEqual([]);
      expect(host.failures).toEqual([]);
      expect(logger.records).toEqual([
        expect.objectContaining({
          event: "approval_review_failure",
          "event.kind": "approval_review.failure",
          operation: "approval_review",
          component: "agent-runtime",
          "request.id": "mreq_1",
          "workspace.id": "wksp_1",
          "session.id": "sesn_1",
          "thread.id": "thrd_parent",
          "approval.failure_kind": "runtime_failure",
          "error.message_safe": "approval reviewer trunk creation failed",
        }),
      ]);
      expect(JSON.stringify(logger.records)).not.toContain("secret");
    }
  });

  test("unexpected reviewer host failures commit one runtime failure after trunk creation", async () => {
    for (const [throwAt, message] of [
      ["preload", "approval reviewer preload failed"],
      ["wait", "approval reviewer wait failed"],
      ["inspect", "approval reviewer decision is unavailable"],
    ] as const) {
      const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "must not escape")], { throwAt });
      const reviewer = createRuntimeApprovalReviewer(() => host, {
        model: platformReviewerModel,
        threadCreator: new RecordingReviewerThreadCreator(),
        now: () => createdAt,
        waitTimeoutMs: 10,
      });

      await expect(reviewer(validReviewRequest())).resolves.toEqual({ type: "failed", message });
      expect(host.decisions).toHaveLength(0);
      expect(host.failures).toEqual([
        expect.objectContaining({
          type: "approval_review.failure",
          failure_kind: "runtime_failure",
          message,
        }),
      ]);
    }
  });

  test("uses reviewer-role selection even when the parent request has no current model", async () => {
    const host = new RecordingReviewerHost([
      assistantDecision("sesn_1", "allow", "platform reviewer model"),
    ]);
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });

    await expect(reviewer({ ...validReviewRequest(), currentModel: undefined })).resolves.toEqual({
      type: "decision",
      riskLevel: "low",
      userAuthorization: "high",
      outcome: "allow",
      message: "platform reviewer model",
    });
    expect(host.inputs).toHaveLength(1);
    expect(host.inputs[0]?.kind === "approval_review" ? host.inputs[0].thread : undefined).toMatchObject({
      role: "approval_reviewer",
      agentType: "approval_reviewer",
    });
  });

  test("renders current assistant draft and tool-call state into the reviewer prompt", async () => {
    const host = new RecordingReviewerHost([
      assistantDecision("sesn_1", "allow", "current draft present"),
    ]);
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });

    await reviewer(validReviewRequest({ currentRequestTurnMessages: [assistantDraftMessage("sesn_1")] }));

    const input = host.inputs[0] as Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>;
    const promptPart = input.promptItems[0]?.parts[0];
    if (promptPart?.type !== "text") {
      throw new Error("expected approval reviewer prompt to be a text part");
    }
    const prompt = JSON.parse(promptPart.text) as {
      readonly current_assistant_draft: ReadonlyArray<{
        readonly text?: string;
        readonly reasoning?: string;
        readonly tool_calls?: ReadonlyArray<{ readonly tool_call_id: string; readonly tool_name: string; readonly status: string; readonly input?: unknown }>;
      }>;
    };
    const draft = prompt.current_assistant_draft[0];
    expect(draft).toMatchObject({
      text: "I will update the file before calling the tool.",
      reasoning: "Need to patch one file.",
      tool_calls: [
        {
          tool_call_id: "tool_call_1",
          tool_name: "Write",
          status: "running",
          input: { path: "src/a.ts", content: "ok" },
        },
      ],
    });
  });

  test("feeds the full parent transcript first and only the committed cursor delta later", async () => {
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "safe")]);
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });
    const approvalReviewerManager = new AutoApprovalReviewerManager();
    const first = userMessage("sesn_1", "msg_first", "first");
    const second = userMessage("sesn_1", "msg_second", "second");

    await reviewer(validReviewRequest({
      approvalReviewerManager,
      targetModelToolCallId: "tool_call_first",
      parentTranscript: { generation: 4, messages: [first] },
    }));
    await reviewer(validReviewRequest({
      approvalReviewerManager,
      targetModelToolCallId: "tool_call_second",
      parentTranscript: { generation: 4, messages: [first, second] },
    }));

    const firstPrompt = promptJSON(host.inputs[0]);
    const firstPromptKeys = Object.keys(firstPrompt);
    expect(firstPrompt.parent_transcript_feed).toEqual([expect.objectContaining({ text: "first" })]);
    expect(firstPromptKeys.indexOf("parent_transcript_feed_note") + 1).toBe(
      firstPromptKeys.indexOf("parent_transcript_feed"),
    );
    expect(promptJSON(host.inputs[1]).parent_transcript_feed).toEqual([expect.objectContaining({ text: "second" })]);
    expect(firstPrompt.parent_transcript_feed_note).toBe(
      "Full parent transcript — treat this as your first assessment of this conversation; it may repeat evidence you have seen before.",
    );
    expect(promptJSON(host.inputs[1]).parent_transcript_feed_note).toBe(
      "History added since your last assessment — continue the same review conversation.",
    );
  });

  test("does not advance the cursor after failure and reanchors after parent compaction", async () => {
    const failureHost = new RecordingReviewerHost([assistantReviewerText("sesn_1", "not json")]);
    const failureReviewer = createRuntimeApprovalReviewer(() => failureHost, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });
    const failedManager = new AutoApprovalReviewerManager();
    const first = userMessage("sesn_1", "msg_first", "first");
    await failureReviewer(validReviewRequest({ approvalReviewerManager: failedManager, targetModelToolCallId: "tool_call_first", parentTranscript: { generation: 1, messages: [first] } }));
    await failureReviewer(validReviewRequest({ approvalReviewerManager: failedManager, targetModelToolCallId: "tool_call_second", parentTranscript: { generation: 1, messages: [first] } }));
    expect(promptJSON(failureHost.inputs[0]).parent_transcript_feed).toEqual(promptJSON(failureHost.inputs[1]).parent_transcript_feed);
    expect(promptJSON(failureHost.inputs[0]).parent_transcript_feed_note).toBe(
      "Full parent transcript — treat this as your first assessment of this conversation; it may repeat evidence you have seen before.",
    );
    expect(promptJSON(failureHost.inputs[1]).parent_transcript_feed_note).toBe(
      "Full parent transcript — treat this as your first assessment of this conversation; it may repeat evidence you have seen before.",
    );

    const successHost = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "safe")]);
    const successReviewer = createRuntimeApprovalReviewer(() => successHost, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });
    const successManager = new AutoApprovalReviewerManager();
    await successReviewer(validReviewRequest({ approvalReviewerManager: successManager, targetModelToolCallId: "tool_call_before", parentTranscript: { generation: 2, messages: [first] } }));
    const checkpoint = userMessage("sesn_1", "msg_checkpoint", "compacted transcript");
    await successReviewer(validReviewRequest({ approvalReviewerManager: successManager, targetModelToolCallId: "tool_call_after", parentTranscript: { generation: 3, messages: [checkpoint] } }));
    expect(promptJSON(successHost.inputs[1]).parent_transcript_feed).toEqual([expect.objectContaining({ text: "compacted transcript" })]);
    expect(promptJSON(successHost.inputs[1]).parent_transcript_feed_note).toBe(
      "Full parent transcript — treat this as your first assessment of this conversation; it may repeat evidence you have seen before.",
    );
    expect(successHost.inputs).toHaveLength(2);
  });

  test("runs a busy review on a seeded sidecar and commits and closes against the correct threads", async () => {
    const blocked = deferred<void>();
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "safe")], {
      waitGate: async (command) => {
        const input = host.inputs.findLast((candidate) => candidate.sessionThreadId === command.sessionThreadId);
        if (input?.kind === "approval_review" && input.targetModelToolCallId === "tool_call_busy") {
          await blocked.promise;
        }
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator();
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator, now: () => createdAt, waitTimeoutMs: 100 });
    const approvalReviewerManager = new AutoApprovalReviewerManager();
    const first = userMessage("sesn_1", "msg_first", "first");
    await reviewer(validReviewRequest({ approvalReviewerManager, targetModelToolCallId: "tool_call_seed", parentTranscript: { generation: 1, messages: [first] } }));
    const second = userMessage("sesn_1", "msg_second", "second");
    const busyReview = reviewer(validReviewRequest({ approvalReviewerManager, targetModelToolCallId: "tool_call_busy", parentTranscript: { generation: 1, messages: [first, second] } }));
    await waitFor(() => host.waits.length >= 2);
    const sidecarResult = await reviewer(validReviewRequest({ approvalReviewerManager, targetModelToolCallId: "tool_call_sidecar", parentTranscript: { generation: 1, messages: [first, second] } }));

    expect(sidecarResult).toMatchObject({ type: "decision", outcome: "allow" });
    const sidecarCreation = threadCreator.creations.find((creation) => !creation.isTrunk);
    if (sidecarCreation === undefined) {
      throw new Error("expected a sidecar creation");
    }
    expect(JSON.parse(sidecarCreation.threadContextPrefixJson)).toMatchObject({
      review_id: sidecarCreation.reviewId,
      source_parent_thread_id: "thrd_parent",
      parent_boundary_event_id: "evt_msg_first",
      runtime_messages_snapshot: [assistantDecision("sesn_1", "allow", "safe")],
    });
    const sidecarInput = host.inputs.find((input) => input.sessionThreadId === sidecarCreation.reviewerThreadId);
    expect(promptJSON(sidecarInput).parent_transcript_feed).toEqual([expect.objectContaining({ text: "second" })]);
    expect(promptJSON(sidecarInput).parent_transcript_feed_note).toBe(
      "History added since your last assessment — continue the same review conversation.",
    );
    expect(host.decisionCommands.at(-1)?.sessionThreadId).toBe(threadCreator.creations[0]?.reviewerThreadId);
    expect(threadCreator.closes.map((creation) => creation.reviewerThreadId)).toContain(sidecarCreation.reviewerThreadId);
    expect(host.closedThreads).toContain(sidecarCreation.reviewerThreadId);
    expect(approvalReviewerManager.parentTranscriptFeed({ generation: 1, messages: [first, second] }).messages).toEqual([second]);

    blocked.resolve(undefined);
    await busyReview;
  });

  test("returns stale custody when the durable sidecar close receipt cannot be applied", async () => {
    const blocked = deferred<void>();
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "safe")], {
      waitGate: async (command) => {
        const input = host.inputs.findLast((candidate) => candidate.sessionThreadId === command.sessionThreadId);
        if (input?.kind === "approval_review" && input.targetModelToolCallId === "tool_call_busy") {
          await blocked.promise;
        }
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator([], undefined, {
      ok: false,
      message: "scope_superseded",
      discardHotState: true,
    });
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 100,
    });
    const approvalReviewerManager = new AutoApprovalReviewerManager();
    const parent = userMessage("sesn_1", "msg_parent", "parent");
    const busyReview = reviewer(validReviewRequest({
      approvalReviewerManager,
      targetModelToolCallId: "tool_call_busy",
      parentTranscript: { generation: 1, messages: [parent] },
    }));
    await waitFor(() => host.waits.length >= 1);

    await expect(reviewer(validReviewRequest({
      approvalReviewerManager,
      targetModelToolCallId: "tool_call_sidecar",
      parentTranscript: { generation: 1, messages: [parent] },
    }))).resolves.toEqual({ type: "stale_custody" });
    expect(host.closedThreads).toHaveLength(0);

    blocked.resolve(undefined);
    await busyReview;
  });

  test("creates the first overlapping sidecar unseeded with a full parent feed", async () => {
    const blocked = deferred<void>();
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "safe")], {
      waitGate: async (command) => {
        if (host.waits[0]?.sessionThreadId === command.sessionThreadId) {
          await blocked.promise;
        }
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator();
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator, now: () => createdAt, waitTimeoutMs: 100 });
    const approvalReviewerManager = new AutoApprovalReviewerManager();
    const parent = userMessage("sesn_1", "msg_parent", "full parent");
    const trunk = reviewer(validReviewRequest({ approvalReviewerManager, targetModelToolCallId: "tool_call_trunk", parentTranscript: { generation: 1, messages: [parent] } }));
    await waitFor(() => host.waits.length >= 1);
    await reviewer(validReviewRequest({ approvalReviewerManager, targetModelToolCallId: "tool_call_sidecar", parentTranscript: { generation: 1, messages: [parent] } }));

    const sidecarCreation = threadCreator.creations.find((creation) => !creation.isTrunk);
    expect(JSON.parse(sidecarCreation?.threadContextPrefixJson ?? "{}")).toMatchObject({
      review_id: sidecarCreation?.reviewId,
      source_parent_thread_id: "thrd_parent",
      parent_boundary_event_id: "evt_msg_parent",
      runtime_messages_snapshot: [],
    });
    const sidecarInput = host.inputs.find((input) => input.sessionThreadId === sidecarCreation?.reviewerThreadId);
    expect(promptJSON(sidecarInput).parent_transcript_feed).toEqual([expect.objectContaining({ text: "full parent" })]);
    expect(promptJSON(sidecarInput).parent_transcript_feed_note).toBe(
      "Full parent transcript — treat this as your first assessment of this conversation; it may repeat evidence you have seen before.",
    );
    blocked.resolve(undefined);
    await trunk;
  });

  test("commits sidecar failures to the trunk ledger and closes only after the failure ACK", async () => {
    const blocked = deferred<void>();
    const host = new RecordingReviewerHost([assistantReviewerText("sesn_1", "not json")], {
      waitGate: async (command) => {
        if (host.waits[0]?.sessionThreadId === command.sessionThreadId) {
          await blocked.promise;
        }
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator();
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator, now: () => createdAt, waitTimeoutMs: 100 });
    const approvalReviewerManager = new AutoApprovalReviewerManager();
    const parent = userMessage("sesn_1", "msg_parent", "full parent");
    const trunk = reviewer(validReviewRequest({ approvalReviewerManager, targetModelToolCallId: "tool_call_trunk", parentTranscript: { generation: 1, messages: [parent] } }));
    await waitFor(() => host.waits.length >= 1);
    await expect(reviewer(validReviewRequest({ approvalReviewerManager, targetModelToolCallId: "tool_call_sidecar", parentTranscript: { generation: 1, messages: [parent] } }))).resolves.toEqual({
      type: "failed",
      message: "approval reviewer decision is not JSON",
    });

    const trunkThreadId = threadCreator.creations.find((creation) => creation.isTrunk)?.reviewerThreadId;
    const sidecarThreadId = threadCreator.creations.find((creation) => !creation.isTrunk)?.reviewerThreadId;
    await waitFor(() => sidecarThreadId !== undefined && host.closedThreads.includes(sidecarThreadId));
    expect(host.failureCommands.at(-1)?.sessionThreadId).toBe(trunkThreadId);
    expect(threadCreator.closes.at(-1)?.reviewerThreadId).toBe(sidecarThreadId);

    blocked.resolve(undefined);
    await trunk;
  });

  test("closes an admission-rejected sidecar without inventing an outcome or delaying ask fallback", async () => {
    const trunkGate = deferred<void>();
    const closeGate = deferred<void>();
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "safe")], {
      enqueueAccepted: (input) => input.kind !== "approval_review" || input.targetModelToolCallId !== "tool_call_rejected",
      waitGate: async (command) => {
        const input = host.inputs.findLast((candidate) => candidate.sessionThreadId === command.sessionThreadId);
        if (input?.kind === "approval_review" && input.targetModelToolCallId === "tool_call_trunk") {
          await trunkGate.promise;
        }
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator([], closeGate.promise);
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 100,
    });
    const manager = new AutoApprovalReviewerManager();
    const trunk = reviewer(validReviewRequest({ approvalReviewerManager: manager, targetModelToolCallId: "tool_call_trunk" }));
    await waitFor(() => host.waits.length === 1);

    await expect(Promise.race([
      reviewer(validReviewRequest({ approvalReviewerManager: manager, targetModelToolCallId: "tool_call_rejected" })),
      rejectAfter(100, "admission rejection was gated by sidecar close"),
    ])).resolves.toEqual({ type: "failed", message: "approval reviewer input was rejected" });

    const sidecarThreadId = threadCreator.creations.find((creation) => !creation.isTrunk)?.reviewerThreadId;
    await waitFor(() => threadCreator.closes.some((creation) => creation.reviewerThreadId === sidecarThreadId));
    expect(host.decisions).toHaveLength(0);
    expect(host.failures).toHaveLength(0);
    expect(host.closedThreads).not.toContain(sidecarThreadId);
    closeGate.resolve(undefined);
    await waitFor(() => sidecarThreadId !== undefined && host.closedThreads.includes(sidecarThreadId));
    trunkGate.resolve(undefined);
    await trunk;
  });

  test("retains a trunk when enqueue throws after potential admission without fabricating an outcome", async () => {
    const manager = new AutoApprovalReviewerManager();
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "must not be observed")], {
      enqueueThrows: () => true,
    });
    const threadCreator = new RecordingReviewerThreadCreator();
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 100,
    });

    await expect(Promise.race([
      reviewer(validReviewRequest({ approvalReviewerManager: manager, targetModelToolCallId: "tool_call_enqueue_throw_trunk" })),
      rejectAfter(100, "enqueue-throw trunk did not degrade promptly"),
    ])).resolves.toEqual({ type: "failed", message: "approval reviewer input was rejected" });

    const input = host.inputs[0];
    if (input?.kind !== "approval_review") {
      throw new Error("enqueue-throw trunk input was not recorded");
    }
    expect(manager.executionState(input.reviewId)).toEqual({ kind: "trunk", raceState: "pending" });
    const later = manager.beginReview("review_after_enqueue_throw_trunk");
    expect(later.kind).toBe("sidecar");
    later.release();
    expect(host.reviewerWaits).toHaveLength(0);
    expect(host.reviewerInterruptions).toHaveLength(0);
    expect(host.interruptions).toHaveLength(0);
    expect(host.inspections).toHaveLength(0);
    expect(host.decisions).toHaveLength(0);
    expect(host.failures).toHaveLength(0);
    expect(threadCreator.closes).toHaveLength(0);

    await Effect.runPromise(manager.dispose());
    expect(manager.executionState(input.reviewId)).toBeUndefined();
  });

  test("closes and releases a sidecar when enqueue throws without fabricating an outcome", async () => {
    const trunkGate = deferred<void>();
    const closeGate = deferred<void>();
    const manager = new AutoApprovalReviewerManager();
    let host: RecordingReviewerHost;
    host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "trunk only")], {
      enqueueThrows: (input) => input.kind === "approval_review"
        && input.targetModelToolCallId === "tool_call_enqueue_throw_sidecar",
      waitGate: async (command) => {
        const input = host.inputs.findLast((candidate) => candidate.sessionThreadId === command.sessionThreadId);
        if (input?.kind === "approval_review" && input.targetModelToolCallId === "tool_call_trunk_owner") {
          await trunkGate.promise;
        }
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator([], closeGate.promise);
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 100,
    });
    const trunk = reviewer(validReviewRequest({
      approvalReviewerManager: manager,
      targetModelToolCallId: "tool_call_trunk_owner",
    }));
    await waitForCondition(() => host.reviewerWaits.length === 1, "trunk owner wait");

    await expect(Promise.race([
      reviewer(validReviewRequest({
        approvalReviewerManager: manager,
        targetModelToolCallId: "tool_call_enqueue_throw_sidecar",
      })),
      rejectAfter(100, "enqueue-throw sidecar was gated by abandoned close"),
    ])).resolves.toEqual({ type: "failed", message: "approval reviewer input was rejected" });

    const sidecarInput = host.inputs.find((input) => input.kind === "approval_review"
      && input.targetModelToolCallId === "tool_call_enqueue_throw_sidecar");
    if (sidecarInput?.kind !== "approval_review") {
      throw new Error("enqueue-throw sidecar input was not recorded");
    }
    await waitForCondition(
      () => threadCreator.closes.some((creation) => creation.reviewId === sidecarInput.reviewId),
      "enqueue-throw sidecar abandoned close",
    );
    expect(manager.ephemeralReviewIds()).toContain(sidecarInput.reviewId);
    expect(host.closedThreads).not.toContain(sidecarInput.sessionThreadId);
    expect(host.reviewerInterruptions).toHaveLength(0);
    expect(host.interruptions).toHaveLength(0);
    expect(host.inspections.filter((control) => control.sessionThreadId === sidecarInput.sessionThreadId)).toHaveLength(0);
    expect(host.decisions.filter((event) => event.review_id === sidecarInput.reviewId)).toHaveLength(0);
    expect(host.failures.filter((event) => event.review_id === sidecarInput.reviewId)).toHaveLength(0);

    closeGate.resolve(undefined);
    await waitForCondition(() => !manager.ephemeralReviewIds().includes(sidecarInput.reviewId), "enqueue-throw sidecar release");
    expect(host.closedThreads).toContain(sidecarInput.sessionThreadId);
    trunkGate.resolve(undefined);
    await trunk;
    await Effect.runPromise(manager.dispose());
  });

  test("retains a tokenless accepted trunk until manager disposal without fabricating an outcome", async () => {
    const manager = new AutoApprovalReviewerManager();
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "must not be observed")], {
      reviewerExecutionToken: () => undefined,
    });
    const threadCreator = new RecordingReviewerThreadCreator();
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 100,
    });

    await expect(Promise.race([
      reviewer(validReviewRequest({ approvalReviewerManager: manager, targetModelToolCallId: "tool_call_tokenless_trunk" })),
      rejectAfter(100, "tokenless accepted trunk did not degrade promptly"),
    ])).resolves.toEqual({ type: "failed", message: "approval reviewer input was rejected" });

    const input = host.inputs[0];
    if (input?.kind !== "approval_review") {
      throw new Error("tokenless trunk input was not recorded");
    }
    expect(manager.executionState(input.reviewId)).toEqual({ kind: "trunk", raceState: "pending" });
    const later = manager.beginReview("review_after_tokenless_accepted_trunk");
    expect(later.kind).toBe("sidecar");
    later.release();
    expect(host.reviewerWaits).toHaveLength(0);
    expect(host.reviewerInterruptions).toHaveLength(0);
    expect(host.interruptions).toHaveLength(0);
    expect(host.inspections).toHaveLength(0);
    expect(host.decisions).toHaveLength(0);
    expect(host.failures).toHaveLength(0);
    expect(threadCreator.closes).toHaveLength(0);

    await Effect.runPromise(manager.dispose());
    expect(manager.executionState(input.reviewId)).toBeUndefined();
  });

  test("retains an accepted sidecar with a mismatched token without closing or fabricating an outcome", async () => {
    const trunkGate = deferred<void>();
    const manager = new AutoApprovalReviewerManager();
    let host: RecordingReviewerHost;
    host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "trunk only")], {
      reviewerExecutionToken: (input, runId) => input.kind === "approval_review"
        && input.targetModelToolCallId === "tool_call_mismatched_sidecar"
        ? { reviewId: input.reviewId, reviewerThreadId: "thrd_wrong_generation", runId }
        : input.kind === "approval_review"
          ? { reviewId: input.reviewId, reviewerThreadId: input.sessionThreadId, runId }
          : undefined,
      waitGate: async (command) => {
        const input = host.inputs.findLast((candidate) => candidate.sessionThreadId === command.sessionThreadId);
        if (input?.kind === "approval_review" && input.targetModelToolCallId === "tool_call_trunk_owner") {
          await trunkGate.promise;
        }
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator();
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 100,
    });
    const trunk = reviewer(validReviewRequest({
      approvalReviewerManager: manager,
      targetModelToolCallId: "tool_call_trunk_owner",
    }));
    await waitForCondition(() => host.reviewerWaits.length === 1, "trunk owner wait");

    await expect(Promise.race([
      reviewer(validReviewRequest({
        approvalReviewerManager: manager,
        targetModelToolCallId: "tool_call_mismatched_sidecar",
      })),
      rejectAfter(100, "mismatched-token sidecar did not degrade promptly"),
    ])).resolves.toEqual({ type: "failed", message: "approval reviewer input was rejected" });

    const sidecarInput = host.inputs.find((input) => input.kind === "approval_review"
      && input.targetModelToolCallId === "tool_call_mismatched_sidecar");
    if (sidecarInput?.kind !== "approval_review") {
      throw new Error("mismatched-token sidecar input was not recorded");
    }
    expect(manager.executionState(sidecarInput.reviewId)).toEqual({ kind: "sidecar", raceState: "pending" });
    expect(manager.ephemeralReviewIds()).toContain(sidecarInput.reviewId);
    expect(threadCreator.closes.filter((creation) => creation.reviewId === sidecarInput.reviewId)).toHaveLength(0);
    expect(host.reviewerWaits).toHaveLength(1);
    expect(host.reviewerInterruptions).toHaveLength(0);
    expect(host.interruptions).toHaveLength(0);
    expect(host.inspections.filter((control) => control.sessionThreadId === sidecarInput.sessionThreadId)).toHaveLength(0);
    expect(host.decisions.filter((event) => event.review_id === sidecarInput.reviewId)).toHaveLength(0);
    expect(host.failures.filter((event) => event.review_id === sidecarInput.reviewId)).toHaveLength(0);

    trunkGate.resolve(undefined);
    await trunk;
    expect(manager.ephemeralReviewIds()).toContain(sidecarInput.reviewId);
    expect(threadCreator.closes.filter((creation) => creation.reviewId === sidecarInput.reviewId)).toHaveLength(0);
    await Effect.runPromise(manager.dispose());
    expect(manager.ephemeralReviewIds()).not.toContain(sidecarInput.reviewId);
    expect(threadCreator.closes.filter((creation) => creation.reviewId === sidecarInput.reviewId)).toHaveLength(0);
  });

  test("closes a sidecar whose decision write is unacknowledged without inventing a failure event", async () => {
    const trunkGate = deferred<void>();
    const closeGate = deferred<void>();
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "safe")], {
      decisionAck: false,
      waitGate: async (command) => {
        const input = host.inputs.findLast((candidate) => candidate.sessionThreadId === command.sessionThreadId);
        if (input?.kind === "approval_review" && input.targetModelToolCallId === "tool_call_trunk") {
          await trunkGate.promise;
        }
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator([], closeGate.promise);
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 100,
    });
    const manager = new AutoApprovalReviewerManager();
    const trunk = reviewer(validReviewRequest({ approvalReviewerManager: manager, targetModelToolCallId: "tool_call_trunk" }));
    await waitFor(() => host.waits.length === 1);

    await expect(Promise.race([
      reviewer(validReviewRequest({ approvalReviewerManager: manager, targetModelToolCallId: "tool_call_unacknowledged" })),
      rejectAfter(100, "unacknowledged decision was gated by sidecar close"),
    ])).resolves.toEqual({ type: "failed", message: "approval reviewer decision was not acknowledged" });

    const sidecarThreadId = threadCreator.creations.find((creation) => !creation.isTrunk)?.reviewerThreadId;
    await waitFor(() => threadCreator.closes.some((creation) => creation.reviewerThreadId === sidecarThreadId));
    expect(host.decisions).toHaveLength(1);
    expect(host.failures).toHaveLength(0);
    closeGate.resolve(undefined);
    await waitFor(() => sidecarThreadId !== undefined && host.closedThreads.includes(sidecarThreadId));
    trunkGate.resolve(undefined);
    await trunk;
  });

  test("closes a sidecar after its bounded failure write remains unacknowledged", async () => {
    const trunkGate = deferred<void>();
    const closeGate = deferred<void>();
    const host = new RecordingReviewerHost([assistantReviewerText("sesn_1", "not json")], {
      failureCommitMode: "hang",
      waitGate: async (command) => {
        const input = host.inputs.findLast((candidate) => candidate.sessionThreadId === command.sessionThreadId);
        if (input?.kind === "approval_review" && input.targetModelToolCallId === "tool_call_trunk") {
          await trunkGate.promise;
        }
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator([], closeGate.promise);
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 100,
      failureCommitTimeoutMs: 1,
    });
    const manager = new AutoApprovalReviewerManager();
    const trunk = reviewer(validReviewRequest({ approvalReviewerManager: manager, targetModelToolCallId: "tool_call_trunk" }));
    await waitFor(() => host.waits.length === 1);

    await expect(Promise.race([
      reviewer(validReviewRequest({ approvalReviewerManager: manager, targetModelToolCallId: "tool_call_failure_unacknowledged" })),
      rejectAfter(100, "failure fallback was gated by sidecar close"),
    ])).resolves.toEqual({ type: "failed", message: "approval reviewer decision is not JSON" });

    const sidecarThreadId = threadCreator.creations.find((creation) => !creation.isTrunk)?.reviewerThreadId;
    await waitFor(() => threadCreator.closes.some((creation) => creation.reviewerThreadId === sidecarThreadId));
    expect(host.decisions).toHaveLength(0);
    expect(host.failures).toHaveLength(1);
    closeGate.resolve(undefined);
    await waitFor(() => sidecarThreadId !== undefined && host.closedThreads.includes(sidecarThreadId));
    trunkGate.resolve(undefined);
    await trunk;
  });

  test("reuses a fresh reviewer decision for the same normalized action", async () => {
    const host = new RecordingReviewerHost([
      assistantDecision("sesn_1", "deny", "needs user confirmation"),
    ]);
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });

    const approvalReviewerManager = new AutoApprovalReviewerManager();
    const first = await reviewer(validReviewRequest({
      approvalReviewerManager,
      targetModelToolCallId: "tool_call_1",
      actionJson: { content: "ok", path: "src/a.ts" },
    }));
    const second = await reviewer(validReviewRequest({
      approvalReviewerManager,
      targetModelToolCallId: "tool_call_1",
      actionJson: { path: "src/a.ts", content: "ok" },
    }));

    expect(first).toEqual({
      type: "decision",
      riskLevel: "low",
      userAuthorization: "high",
      outcome: "deny",
      message: "needs user confirmation",
    });
    expect(second).toEqual(first);
    expect(host.inputs).toHaveLength(1);
    expect(host.waits).toHaveLength(1);
    expect(host.inspections).toHaveLength(1);
    expect(host.decisions).toHaveLength(1);
  });

  test("does not reuse reviewer decisions across target tool calls", async () => {
    const host = new RecordingReviewerHost([
      assistantDecision("sesn_1", "deny", "needs user confirmation"),
    ]);
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });

    const approvalReviewerManager = new AutoApprovalReviewerManager();
    const first = await reviewer(validReviewRequest({
      approvalReviewerManager,
      targetModelToolCallId: "tool_call_first",
      actionJson: { content: "ok", path: "src/a.ts" },
    }));
    const second = await reviewer(validReviewRequest({
      approvalReviewerManager,
      targetModelToolCallId: "tool_call_second",
      actionJson: { path: "src/a.ts", content: "ok" },
    }));

    expect(first).toEqual({
      type: "decision",
      riskLevel: "low",
      userAuthorization: "high",
      outcome: "deny",
      message: "needs user confirmation",
    });
    expect(second).toEqual(first);
    expect(host.inputs).toHaveLength(2);
    expect(host.waits).toHaveLength(2);
    expect(host.inspections).toHaveLength(2);
    expect(host.decisions).toHaveLength(2);
    expect((host.inputs[0] as Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>).reviewId)
      .not.toBe((host.inputs[1] as Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>).reviewId);
  });

  test("does not reuse reviewer decisions across model request turns", async () => {
    const host = new RecordingReviewerHost([
      assistantDecision("sesn_1", "allow", "fresh request turn decision"),
    ]);
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });

    const approvalReviewerManager = new AutoApprovalReviewerManager();
    const first = await reviewer(validReviewRequest({
      approvalReviewerManager,
      modelRequestId: "mreq_first",
      actionJson: { content: "ok", path: "src/a.ts" },
    }));
    const second = await reviewer(validReviewRequest({
      approvalReviewerManager,
      modelRequestId: "mreq_second",
      actionJson: { path: "src/a.ts", content: "ok" },
    }));

    expect(first).toEqual({
      type: "decision",
      riskLevel: "low",
      userAuthorization: "high",
      outcome: "allow",
      message: "fresh request turn decision",
    });
    expect(second).toEqual(first);
    expect(host.inputs).toHaveLength(2);
    expect(host.waits).toHaveLength(2);
    expect(host.inspections).toHaveLength(2);
    expect(host.decisions).toHaveLength(2);
    expect((host.inputs[0] as Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>).reviewId)
      .not.toBe((host.inputs[1] as Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>).reviewId);
  });

  test("manager loss re-reviews the same target with a byte-stable idempotent input", async () => {
    const host = new RecordingReviewerHost([
      assistantDecision("sesn_1", "allow", "replayed target"),
    ]);
    const threadCreator = new RecordingReviewerThreadCreator();
    const firstReviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => "2026-07-06T00:00:01.000Z",
      waitTimeoutMs: 10,
    });
    const secondReviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => "2026-07-06T00:00:02.000Z",
      waitTimeoutMs: 10,
    });
    const request = validReviewRequest();

    await firstReviewer(request);
    await Effect.runPromise(request.approvalReviewerManager.dispose());
    await secondReviewer({ ...request, approvalReviewerManager: new AutoApprovalReviewerManager() });

    expect(host.inputs).toHaveLength(2);
    const first = host.inputs[0] as Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>;
    const second = host.inputs[1] as Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>;
    expect(second.reviewId).toBe(first.reviewId);
    expect(second.runtimeInputId).toBe(first.runtimeInputId);
    expect(second.promptItems).toEqual(first.promptItems);
    expect(host.decisions).toHaveLength(2);
  });

  test("fails closed when the durable reviewer decision is not acknowledged", async () => {
    const logger = new RecordingLogger();
    const host = new RecordingReviewerHost([
      assistantDecision("sesn_1", "allow", "low risk"),
    ], { decisionAck: false });
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10, logger });

    await expect(reviewer(validReviewRequest())).resolves.toEqual({
      type: "failed",
      message: "approval reviewer decision was not acknowledged",
    });
    expect(host.decisions).toHaveLength(1);
    expect(host.failures).toHaveLength(0);
    expect(logger.records).toEqual([
      expect.objectContaining({
        event: "approval_review_failure",
        "approval.failure_kind": "runtime_failure",
        "error.message_safe": "approval reviewer decision was not acknowledged",
      }),
    ]);
  });

  test("records timeout approval_review.failure on the reviewer trunk", async () => {
    const host = new RecordingReviewerHost([], { waitTimedOut: true });
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });

    await expect(reviewer(validReviewRequest())).resolves.toEqual({
      type: "failed",
      message: "approval reviewer timed out",
    });
    expect(host.failures).toEqual([
      expect.objectContaining({
        type: "approval_review.failure",
        parent_thread_id: "thrd_parent",
        target_model_tool_call_id: "tool_call_1",
        target_tool_name: "Write",
        failure_kind: "timeout",
        message: "approval reviewer timed out",
      }),
    ]);
  });

  test("keeps a timed-out trunk leased until its owner run settles without delaying ask fallback", async () => {
    const interruptGate = deferred<void>();
    let host: RecordingReviewerHost;
    host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "safe")], {
      interruptGate: async () => await interruptGate.promise,
      waitTimedOut: (command, timeoutMs): boolean => {
        const input = host.inputs.findLast((candidate) => candidate.sessionThreadId === command.sessionThreadId);
        return timeoutMs !== undefined
          && input?.kind === "approval_review"
          && input.targetModelToolCallId === "tool_call_timeout";
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator();
    const manager = new AutoApprovalReviewerManager();
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 10,
    });

    await expect(Promise.race([
      reviewer(validReviewRequest({ approvalReviewerManager: manager, targetModelToolCallId: "tool_call_timeout" })),
      rejectAfter(100, "timeout ask fallback waited for owner shutdown"),
    ])).resolves.toEqual({ type: "failed", message: "approval reviewer timed out" });
    await waitForCondition(() => host.interruptions.length === 1, "timed-out trunk interruption");

    await expect(reviewer(validReviewRequest({
      approvalReviewerManager: manager,
      targetModelToolCallId: "tool_call_overlap",
    }))).resolves.toMatchObject({ type: "decision", outcome: "allow" });
    const overlapInput = host.inputs.findLast((input) => input.kind === "approval_review" && input.targetModelToolCallId === "tool_call_overlap");
    expect(threadCreator.creations.find((creation) => creation.reviewerThreadId === overlapInput?.sessionThreadId)?.isTrunk).toBe(false);

    interruptGate.resolve(undefined);
    await waitForCondition(() => {
      const lease = manager.beginReview("probe_after_timeout_settlement");
      const isTrunk = lease.kind === "trunk";
      lease.release();
      return isTrunk;
    }, "trunk lease release after owner settlement");
  });

  test("keeps a timed-out trunk leased when owner interruption is not confirmed", async () => {
    const host = new RecordingReviewerHost([], {
      interruptOk: false,
      waitTimedOut: true,
    });
    const manager = new AutoApprovalReviewerManager();
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator: new RecordingReviewerThreadCreator(),
      now: () => createdAt,
      waitTimeoutMs: 10,
    });

    await expect(Promise.race([
      reviewer(validReviewRequest({ approvalReviewerManager: manager, targetModelToolCallId: "tool_call_interrupt_failure" })),
      rejectAfter(100, "timeout ask fallback waited for failed owner interruption"),
    ])).resolves.toEqual({ type: "failed", message: "approval reviewer timed out" });
    await waitForCondition(() => host.failures.length === 1 && host.interruptions.length === 1, "timeout failure cleanup attempts");

    const overlapLease = manager.beginReview("probe_after_failed_interrupt");
    expect(overlapLease.kind).toBe("sidecar");
    overlapLease.release();
    expect(host.failures).toHaveLength(1);
    await Effect.runPromise(manager.dispose());
  });

  test("records parse_failure approval_review.failure for malformed reviewer output", async () => {
    const host = new RecordingReviewerHost([
      assistantReviewerText("sesn_1", "not json"),
    ]);
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });

    await expect(reviewer(validReviewRequest())).resolves.toEqual({
      type: "failed",
      message: "approval reviewer decision is not JSON",
    });
    expect(host.failures).toEqual([
      expect.objectContaining({
        type: "approval_review.failure",
        failure_kind: "parse_failure",
        message: "approval reviewer decision is not JSON",
      }),
    ]);
  });

  test("rejects schema-invalid reviewer decisions before an allow outcome can authorize the tool", async () => {
    for (const invalid of [
      { risk_level: "low", user_authorization: "high", outcome: "allow" },
      { risk_level: "low", user_authorization: "high", outcome: "allow", rationale: 42 },
      { risk_level: "low", user_authorization: "high", outcome: "allow", rationale: "safe", extra: true },
    ]) {
      const host = new RecordingReviewerHost([
        assistantReviewerText("sesn_1", JSON.stringify(invalid)),
      ]);
      const reviewer = createRuntimeApprovalReviewer(() => host, {
        model: platformReviewerModel,
        threadCreator: new RecordingReviewerThreadCreator(),
        now: () => createdAt,
        waitTimeoutMs: 10,
      });

      const result = await reviewer(validReviewRequest());

      expect(result.type).toBe("failed");
      expect(host.decisions).toHaveLength(0);
      expect(host.failures).toEqual([
        expect.objectContaining({ type: "approval_review.failure", failure_kind: "parse_failure" }),
      ]);
    }
  });

  test("rejects fenced or prose-wrapped allow decisions as parse failures", async () => {
    const decision = JSON.stringify({
      risk_level: "low",
      user_authorization: "high",
      outcome: "allow",
      rationale: "safe",
    });
    for (const wrapped of [`\`\`\`json\n${decision}\n\`\`\``, `Decision follows: ${decision}`]) {
      const host = new RecordingReviewerHost([
        assistantReviewerText("sesn_1", wrapped),
      ]);
      const reviewer = createRuntimeApprovalReviewer(() => host, {
        model: platformReviewerModel,
        threadCreator: new RecordingReviewerThreadCreator(),
        now: () => createdAt,
        waitTimeoutMs: 10,
      });

      await expect(reviewer(validReviewRequest())).resolves.toEqual({
        type: "failed",
        message: "approval reviewer decision is not JSON",
      });
      expect(host.decisions).toHaveLength(0);
      expect(host.failures).toEqual([
        expect.objectContaining({ type: "approval_review.failure", failure_kind: "parse_failure" }),
      ]);
    }
  });

  test("records runtime_failure approval_review.failure for an empty reviewer response", async () => {
    const host = new RecordingReviewerHost([]);
    const reviewer = createRuntimeApprovalReviewer(() => host, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });

    await expect(reviewer(validReviewRequest())).resolves.toEqual({
      type: "failed",
      message: "approval reviewer returned no decision",
    });
    expect(host.failures).toEqual([
      expect.objectContaining({
        type: "approval_review.failure",
        failure_kind: "runtime_failure",
        message: "approval reviewer returned no decision",
      }),
    ]);
  });

  test("does not reuse a prior reviewer verdict when the current response is empty", async () => {
    const host = new RecordingReviewerHost([
      assistantDecision("sesn_1", "allow", "prior review only"),
      userMessage("sesn_1", "msg_current_review_prompt", "current review prompt"),
    ]);
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator: new RecordingReviewerThreadCreator(),
      now: () => createdAt,
      waitTimeoutMs: 10,
    });

    await expect(reviewer(validReviewRequest())).resolves.toEqual({
      type: "failed",
      message: "approval reviewer returned no decision",
    });
    expect(host.decisions).toHaveLength(0);
    expect(host.failures).toEqual([
      expect.objectContaining({ failure_kind: "runtime_failure" }),
    ]);
  });

  test("does not gate public ask fallback on a hung failure-record commit", async () => {
    const host = new RecordingReviewerHost([
      assistantReviewerText("sesn_1", "{\"risk_level\":\"low\",\"user_authorization\":\"high\",\"outcome\":\"maybe\",\"rationale\":\"invalid\"}"),
    ], { failureCommitMode: "hang" });
    const reviewer = createRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator: new RecordingReviewerThreadCreator(),
      now: () => createdAt,
      waitTimeoutMs: 10,
      failureCommitTimeoutMs: 1,
    });

    await expect(Promise.race([
      reviewer(validReviewRequest()),
      rejectAfter(100, "reviewer fallback timed out"),
    ])).resolves.toEqual({
      type: "failed",
      message: "approval reviewer decision outcome is invalid",
    });
    expect(host.failures).toEqual([
      expect.objectContaining({
        failure_kind: "parse_failure",
        message: "approval reviewer decision outcome is invalid",
      }),
    ]);
  });

  test("swallows duplicate and conflicting failure-record outcomes", async () => {
    const duplicateHost = new RecordingReviewerHost([], { failureCommitMode: "duplicate" });
    const duplicateReviewer = createRuntimeApprovalReviewer(() => duplicateHost, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });
    await expect(duplicateReviewer(validReviewRequest())).resolves.toEqual({
      type: "failed",
      message: "approval reviewer returned no decision",
    });

    const conflictingHost = new RecordingReviewerHost([], { failureCommitMode: "reject" });
    const conflictingReviewer = createRuntimeApprovalReviewer(() => conflictingHost, { model: platformReviewerModel, threadCreator: new RecordingReviewerThreadCreator(), now: () => createdAt, waitTimeoutMs: 10 });
    await expect(conflictingReviewer(validReviewRequest())).resolves.toEqual({
      type: "failed",
      message: "approval reviewer returned no decision",
    });
    expect(duplicateHost.failures).toHaveLength(1);
    expect(conflictingHost.failures).toHaveLength(1);
  });

  test("cancellation during enqueue retains the trunk until the accepted exact run is interrupted and joined", async () => {
    const enqueueRelease = deferred<void>();
    const manager = new AutoApprovalReviewerManager();
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "late")], {
      enqueueGate: async () => await enqueueRelease.promise,
    });
    const reviewer = createEffectRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator: new RecordingReviewerThreadCreator(),
      now: () => createdAt,
      waitTimeoutMs: 10_000,
    });

    await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const scope = yield* Scope.Scope;
      const fiber = yield* Effect.forkIn(reviewer(validReviewRequest({ approvalReviewerManager: manager })), scope);
      yield* Effect.promise(() => waitForCondition(() => host.inputs.length === 1, "reviewer enqueue registration"));
      yield* Fiber.interrupt(fiber);
    })));

    const reviewId = host.inputs[0]?.kind === "approval_review" ? host.inputs[0].reviewId : "";
    expect(manager.executionState(reviewId)?.raceState).toBe("cancellation_won");
    const duringCancellation = manager.beginReview("review_during_enqueue_cancellation");
    expect(duringCancellation.kind).toBe("sidecar");
    expect(manager.ephemeralReviewIds()).toContain("review_during_enqueue_cancellation");
    duringCancellation.release();

    enqueueRelease.resolve(undefined);
    await waitForCondition(() => manager.executionState(reviewId) === undefined, "enqueue cancellation settlement");
    expect(host.reviewerInterruptions).toHaveLength(1);
    expect(host.inspections).toHaveLength(0);
    expect(host.decisions).toHaveLength(0);
    expect(host.failures).toHaveLength(0);
    const afterCancellation = manager.beginReview("review_after_enqueue_cancellation");
    expect(afterCancellation.kind).toBe("trunk");
    afterCancellation.release();
  });

  test("cancellation during the active exact-generation wait retains the trunk through interrupt settlement", async () => {
    const waitRelease = deferred<void>();
    const interruptRelease = deferred<void>();
    const manager = new AutoApprovalReviewerManager();
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "late")], {
      waitGate: async () => await waitRelease.promise,
      interruptGate: async () => await interruptRelease.promise,
    });
    const reviewer = createEffectRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator: new RecordingReviewerThreadCreator(),
      now: () => createdAt,
      waitTimeoutMs: 10_000,
    });

    await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const scope = yield* Scope.Scope;
      const fiber = yield* Effect.forkIn(reviewer(validReviewRequest({ approvalReviewerManager: manager })), scope);
      yield* Effect.promise(() => waitForCondition(() => host.reviewerWaits.length === 1, "exact reviewer wait registration"));
      yield* Fiber.interrupt(fiber);
    })));

    const reviewId = host.inputs[0]?.kind === "approval_review" ? host.inputs[0].reviewId : "";
    await waitForCondition(() => host.reviewerInterruptions.length === 1, "exact reviewer interruption");
    expect(manager.executionState(reviewId)?.raceState).toBe("cancellation_won");
    const duringCancellation = manager.beginReview("review_during_wait_cancellation");
    expect(duringCancellation.kind).toBe("sidecar");
    expect(manager.ephemeralReviewIds()).toContain("review_during_wait_cancellation");
    duringCancellation.release();
    expect(host.inspections).toHaveLength(0);
    expect(host.decisions).toHaveLength(0);
    expect(host.failures).toHaveLength(0);

    interruptRelease.resolve(undefined);
    waitRelease.resolve(undefined);
    await waitForCondition(() => manager.executionState(reviewId) === undefined, "active wait cancellation settlement");
    const afterCancellation = manager.beginReview("review_after_wait_cancellation");
    expect(afterCancellation.kind).toBe("trunk");
    afterCancellation.release();
  });

  test("cancelling an active sidecar retains membership until exact-run interrupt and abandoned close settle", async () => {
    const trunkWaitRelease = deferred<void>();
    const sidecarWaitRelease = deferred<void>();
    const sidecarInterruptRelease = deferred<void>();
    const manager = new AutoApprovalReviewerManager();
    let host: RecordingReviewerHost;
    host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "safe")], {
      waitGate: async (command) => {
        const input = host.inputs.findLast((candidate) => candidate.sessionThreadId === command.sessionThreadId);
        if (input?.kind === "approval_review" && input.targetModelToolCallId === "tool_call_trunk_owner") {
          await trunkWaitRelease.promise;
        }
        if (input?.kind === "approval_review" && input.targetModelToolCallId === "tool_call_cancelled_sidecar") {
          await sidecarWaitRelease.promise;
        }
      },
      interruptGate: async (command) => {
        const input = host.inputs.findLast((candidate) => candidate.sessionThreadId === command.sessionThreadId);
        if (input?.kind === "approval_review" && input.targetModelToolCallId === "tool_call_cancelled_sidecar") {
          await sidecarInterruptRelease.promise;
        }
      },
    });
    const threadCreator = new RecordingReviewerThreadCreator();
    const reviewer = createEffectRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator,
      now: () => createdAt,
      waitTimeoutMs: 10_000,
    });
    const trunk = Effect.runPromise(reviewer(validReviewRequest({
      approvalReviewerManager: manager,
      targetModelToolCallId: "tool_call_trunk_owner",
    })));
    await waitForCondition(() => host.reviewerWaits.length === 1, "trunk owner wait");

    await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const scope = yield* Scope.Scope;
      const sidecarFiber = yield* Effect.forkIn(reviewer(validReviewRequest({
        approvalReviewerManager: manager,
        targetModelToolCallId: "tool_call_cancelled_sidecar",
      })), scope);
      yield* Effect.promise(() => waitForCondition(() => host.reviewerWaits.length === 2, "sidecar exact wait"));
      yield* Fiber.interrupt(sidecarFiber);
    })));

    const sidecarInput = host.inputs.find((input) => input.kind === "approval_review" && input.targetModelToolCallId === "tool_call_cancelled_sidecar");
    if (sidecarInput?.kind !== "approval_review") {
      throw new Error("cancelled sidecar input was not recorded");
    }
    const sidecarReviewId = sidecarInput.reviewId;
    await waitForCondition(() => host.reviewerInterruptions.some((token) => token.reviewId === sidecarReviewId), "sidecar exact interruption");
    expect(manager.ephemeralReviewIds()).toContain(sidecarReviewId);
    expect(threadCreator.closes.filter((creation) => creation.reviewId === sidecarReviewId)).toHaveLength(0);

    sidecarInterruptRelease.resolve(undefined);
    sidecarWaitRelease.resolve(undefined);
    await waitForCondition(() => !manager.ephemeralReviewIds().includes(sidecarReviewId), "sidecar cancellation close settlement");
    expect(threadCreator.closes.filter((creation) => creation.reviewId === sidecarReviewId)).toHaveLength(1);
    expect(host.closedThreads).toContain(sidecarInput.sessionThreadId);
    expect(host.decisions.filter((event) => event.review_id === sidecarReviewId)).toHaveLength(0);
    expect(host.failures.filter((event) => event.review_id === sidecarReviewId)).toHaveLength(0);

    trunkWaitRelease.resolve(undefined);
    await trunk;
  });

  test("outcome ownership wins before the durable decision write and survives later caller cancellation", async () => {
    const decisionRelease = deferred<void>();
    const manager = new AutoApprovalReviewerManager();
    const host = new RecordingReviewerHost([assistantDecision("sesn_1", "allow", "target A")], {
      decisionGate: async () => await decisionRelease.promise,
    });
    const reviewer = createEffectRuntimeApprovalReviewer(() => host, {
      model: platformReviewerModel,
      threadCreator: new RecordingReviewerThreadCreator(),
      now: () => createdAt,
      waitTimeoutMs: 10_000,
    });
    const request = validReviewRequest({ approvalReviewerManager: manager });

    await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const scope = yield* Scope.Scope;
      const fiber = yield* Effect.forkIn(reviewer(request), scope);
      yield* Effect.promise(() => waitForCondition(() => host.decisionCommands.length === 1, "decision write start"));
      yield* Fiber.interrupt(fiber);
    })));

    const reviewId = host.inputs[0]?.kind === "approval_review" ? host.inputs[0].reviewId : "";
    expect(manager.executionState(reviewId)?.raceState).toBe("outcome_won");
    const duringWrite = manager.beginReview("review_during_outcome_write");
    expect(duringWrite.kind).toBe("sidecar");
    duringWrite.release();

    decisionRelease.resolve(undefined);
    await waitForCondition(() => manager.executionState(reviewId) === undefined, "outcome write settlement");
    expect(host.decisions).toEqual([expect.objectContaining({
      review_id: reviewId,
      target_model_tool_call_id: "tool_call_1",
      rationale: "target A",
    })]);
    expect(await Effect.runPromise(reviewer(request))).toMatchObject({ type: "decision", message: "target A" });
    expect(host.inputs).toHaveLength(1);
  });
});

class RecordingReviewerHost {
  readonly inputs: RuntimeAcceptedInputState[] = [];
  readonly waits: RuntimeThreadControlState[] = [];
  readonly inspections: RuntimeThreadControlState[] = [];
  readonly decisions: Array<Extract<SessionEvent, { readonly type: "approval_review.decision" }>> = [];
  readonly failures: Array<Extract<SessionEvent, { readonly type: "approval_review.failure" }>> = [];
  readonly decisionCommands: RuntimeAcceptedInputState[] = [];
  readonly failureCommands: RuntimeAcceptedInputState[] = [];
  readonly closedThreads: string[] = [];
  readonly interruptions: string[] = [];

  constructor(
    private readonly messages: readonly RuntimeMessage[],
    private readonly options: {
      readonly decisionAck?: boolean;
      readonly decisionGate?: (() => Promise<void>) | undefined;
      readonly enqueueAccepted?: ((input: RuntimeAcceptedInputState) => boolean) | undefined;
      readonly enqueueGate?: (() => Promise<void>) | undefined;
      readonly enqueueThrows?: ((input: RuntimeAcceptedInputState) => boolean) | undefined;
      readonly reviewerExecutionToken?: (
        input: RuntimeAcceptedInputState,
        runId: number,
      ) => SessionManager.ReviewerExecutionToken | undefined;
      readonly failureCommitMode?: "ok" | "duplicate" | "reject" | "hang";
      readonly steps?: string[];
      readonly waitOk?: boolean;
      readonly waitTimedOut?: boolean | ((command: RuntimeThreadControlState, timeoutMs: number | undefined) => boolean);
      readonly inspectObserved?: boolean;
      readonly throwAt?: "preload" | "wait" | "inspect";
      readonly waitGate?: ((command: RuntimeThreadControlState) => Promise<void>) | undefined;
      readonly interruptGate?: ((command: RuntimeThreadControlState) => Promise<void>) | undefined;
      readonly interruptOk?: boolean;
    } = {},
  ) {}

  async enqueueThreadInput(input: RuntimeAcceptedInputState): Promise<SessionManager.AcceptInputResult> {
    this.options.steps?.push("enqueue-input");
    this.inputs.push(input);
    await this.options.enqueueGate?.();
    if (this.options.enqueueThrows?.(input) === true) {
      throw new Error("secret enqueue failure");
    }
    if (this.options.enqueueAccepted?.(input) === false) {
      return { ok: false, sessionId: input.sessionId, reason: "local_session_capacity_exceeded" };
    }
    const reviewerExecutionToken = this.options.reviewerExecutionToken === undefined
      ? input.kind === "approval_review"
        ? { reviewId: input.reviewId, reviewerThreadId: input.sessionThreadId, runId: this.inputs.length }
        : undefined
      : this.options.reviewerExecutionToken(input, this.inputs.length);
    return {
      ok: true,
      sessionId: input.sessionId,
      created: false,
      started: true,
      pendingWake: false,
      ...(reviewerExecutionToken === undefined ? {} : { reviewerExecutionToken }),
    };
  }

  async preloadThread(input: Omit<RuntimeThreadPreloadState, "messages" | "runtimeBindingToken" | "pendingToolUses" | "coldCoverage">): Promise<SessionManager.ThreadLifecycleResult> {
    this.options.steps?.push("preload-thread");
    if (this.options.throwAt === "preload") {
      throw new Error("secret preload failure");
    }
    return { ok: true, sessionId: input.sessionId, sessionThreadId: input.sessionThreadId, applied: true };
  }

  async interruptThread(command: RuntimeThreadControlState): Promise<SessionManager.ThreadLifecycleResult> {
    this.interruptions.push(command.sessionThreadId);
    await this.options.interruptGate?.(command);
    if (this.options.interruptOk === false) {
      return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "thread_busy" };
    }
    return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
  }

  readonly reviewerWaits: SessionManager.ReviewerExecutionToken[] = [];
  readonly reviewerInterruptions: SessionManager.ReviewerExecutionToken[] = [];

  async interruptReviewerExecution(
    command: RuntimeThreadControlState,
    token: SessionManager.ReviewerExecutionToken,
  ): Promise<SessionManager.ReviewerExecutionControlResult> {
    this.reviewerInterruptions.push(token);
    this.interruptions.push(command.sessionThreadId);
    await this.options.interruptGate?.(command);
    if (this.options.interruptOk === false) {
      return {
        ok: false,
        sessionId: command.sessionId,
        sessionThreadId: command.sessionThreadId,
        reason: "thread_busy",
      };
    }
    return {
      ok: true,
      sessionId: command.sessionId,
      sessionThreadId: command.sessionThreadId,
      applied: true,
      terminal: true,
    };
  }

  async waitReviewerExecution(
    command: RuntimeThreadControlState,
    token: SessionManager.ReviewerExecutionToken,
    timeoutMs: number | undefined,
    abortSignal?: AbortSignal,
  ): Promise<SessionManager.ReviewerExecutionWaitResult> {
    this.reviewerWaits.push(token);
    const wait = async (): Promise<SessionManager.ReviewerExecutionWaitResult> => {
      const result = await this.waitThread(command, timeoutMs);
      if (!result.ok) {
        return {
          ok: false,
          sessionId: command.sessionId,
          sessionThreadId: command.sessionThreadId,
          reason: "reviewer_execution_mismatch",
        };
      }
      return {
        ok: true,
        sessionId: result.sessionId,
        sessionThreadId: result.sessionThreadId,
        ...(result.status === undefined ? {} : { status: result.status }),
        terminal: !result.timedOut,
        timedOut: result.timedOut,
      };
    };
    if (abortSignal === undefined) {
      return await wait();
    }
    return await Promise.race<SessionManager.ReviewerExecutionWaitResult>([
      wait(),
      new Promise<SessionManager.ReviewerExecutionWaitResult>((_resolve, reject) => {
        abortSignal.addEventListener("abort", () => reject(new Error("reviewer wait cancelled")), { once: true });
      }),
    ]);
  }

  async inspectReviewerExecution(
    command: RuntimeThreadControlState,
    _token: SessionManager.ReviewerExecutionToken,
  ): Promise<SessionManager.ReviewerExecutionSnapshotResult> {
    const result = await this.inspectThread(command);
    if (!result.ok || !result.observed || result.status === undefined) {
      return {
        ok: false,
        sessionId: command.sessionId,
        sessionThreadId: command.sessionThreadId,
        reason: "reviewer_execution_mismatch",
      };
    }
    return {
      ok: true,
      sessionId: result.sessionId,
      sessionThreadId: result.sessionThreadId,
      observed: true,
      status: result.status,
      messages: result.messages,
    };
  }

  async markThreadClosed(command: RuntimeThreadControlState): Promise<SessionManager.ThreadLifecycleResult> {
    this.closedThreads.push(command.sessionThreadId);
    return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
  }

  async markThreadActive(): Promise<SessionManager.ThreadLifecycleResult> {
    throw new Error("markThreadActive must not be used by approval reviewer");
  }

  async waitThread(command: RuntimeThreadControlState, timeoutMs?: number): Promise<SessionManager.ThreadWaitResult> {
    this.waits.push(command);
    if (this.options.throwAt === "wait") {
      throw new Error("secret wait failure");
    }
    await this.options.waitGate?.(command);
    if (this.options.waitOk === false) {
      return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "thread_busy" };
    }
    const timedOut = typeof this.options.waitTimedOut === "function"
      ? this.options.waitTimedOut(command, timeoutMs)
      : timeoutMs !== undefined && this.options.waitTimedOut === true;
    return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, observed: true, status: "idle", timedOut };
  }

  async inspectThread(command: RuntimeThreadControlState): Promise<SessionManager.ThreadSnapshotResult> {
    this.inspections.push(command);
    if (this.options.throwAt === "inspect") {
      throw new Error("secret inspect failure");
    }
    return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, observed: this.options.inspectObserved !== false, status: "idle", messages: this.messages };
  }

  async commitApprovalReviewDecision(
    command: RuntimeAcceptedInputState,
    event: Extract<SessionEvent, { readonly type: "approval_review.decision" }>,
  ): Promise<SessionEventWriterAppendResult> {
    this.decisionCommands.push(command);
    this.decisions.push(event);
    await this.options.decisionGate?.();
    if (this.options.decisionAck === false) {
      return {
        ok: false,
        error: {
          type: "session-event-writer",
          code: "unavailable",
          message: "append failed",
          retryable: false,
          fatal: false,
          sessionId: event.review_id,
          writeId: "rwrite_decision",
        },
      };
    }
    return { ok: true, writeId: "rwrite_decision", eventId: "evt_decision", processedAt: createdAt };
  }

  async commitApprovalReviewFailure(
    command: RuntimeAcceptedInputState,
    event: Extract<SessionEvent, { readonly type: "approval_review.failure" }>,
  ): Promise<SessionEventWriterAppendResult> {
    this.failureCommands.push(command);
    this.failures.push(event);
    if (this.options.failureCommitMode === "hang") {
      return await new Promise<SessionEventWriterAppendResult>(() => undefined);
    }
    if (this.options.failureCommitMode === "reject") {
      throw new Error("already exists");
    }
    if (this.options.failureCommitMode === "duplicate") {
      return { ok: true, writeId: "rwrite_failure", eventId: "evt_existing_failure", processedAt: createdAt };
    }
    return { ok: true, writeId: "rwrite_failure", eventId: "evt_failure", processedAt: createdAt };
  }
}

class RecordingLogger {
  readonly records: Array<Record<string, unknown>> = [];

  info(record: Record<string, unknown>): void {
    this.records.push(record);
  }

  error(record: Record<string, unknown>): void {
    this.records.push(record);
  }
}

class RecordingReviewerThreadCreator {
  readonly creations: ApprovalReviewerThreadCreation[] = [];
  readonly closes: ApprovalReviewerThreadCreation[] = [];

  constructor(
    private readonly steps: string[] = [],
    private readonly closeGate?: Promise<void>,
    private readonly closeResult:
      | { readonly ok: true }
      | { readonly ok: false; readonly message: string; readonly discardHotState?: boolean } = { ok: true },
  ) {}

  async createApprovalReviewerThread(input: ApprovalReviewerThreadCreation) {
    this.steps.push("create-thread");
    this.creations.push(input);
    return { ok: true as const };
  }

  async closeApprovalReviewerThread(input: ApprovalReviewerThreadCreation) {
    this.closes.push(input);
    await this.closeGate;
    return this.closeResult;
  }
}

function validReviewRequest(overrides: Partial<RuntimeApprovalReviewRequest> = {}): RuntimeApprovalReviewRequest {
  return {
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_parent",
    bindingId: "bind_1",
    bindingGeneration: 7,
    targetPodUid: "pod_1",
    runtimeBindingToken: "rtbt_1",
    modelRequestId: "mreq_1",
    targetModelToolCallId: "tool_call_1",
    targetToolName: "Write",
    actionJson: { path: "src/a.ts", content: "ok" },
    approvalReviewerManager: new AutoApprovalReviewerManager(),
    parentTranscript: { generation: 1, messages: [userMessage("sesn_1")] },
    currentRequestTurnMessages: [],
    siblingToolCalls: [{ modelToolCallId: "tool_call_1", toolName: "Write", actionJson: { path: "src/a.ts", content: "ok" } }],
    policyContext: { approvalMode: "approve_for_me", permissionPolicy: "always_ask" },
    currentModel: { providerId: "anthropic", modelId: "claude-test" },
    ...overrides,
  };
}

function rejectAfter(ms: number, message: string): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(message)), ms);
  });
}

function promptJSON(input: RuntimeAcceptedInputState | undefined): {
  readonly parent_transcript_feed_note: string;
  readonly parent_transcript_feed: readonly unknown[];
  readonly current_assistant_draft: readonly unknown[];
} {
  if (input?.kind !== "approval_review") {
    throw new Error("expected approval review input");
  }
  const part = input.promptItems[0]?.parts[0];
  if (part?.type !== "text") {
    throw new Error("expected approval review text prompt");
  }
  return JSON.parse(part.text) as {
    readonly parent_transcript_feed_note: string;
    readonly parent_transcript_feed: readonly unknown[];
    readonly current_assistant_draft: readonly unknown[];
  };
}

function deferred<T>(): { readonly promise: Promise<T>; readonly resolve: (value: T) => void } {
  let resolveValue: (value: T) => void = () => undefined;
  const promise = new Promise<T>((resolve) => {
    resolveValue = resolve;
  });
  return { promise, resolve: resolveValue };
}

async function waitForCondition(predicate: () => boolean, label: string): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`timed out waiting for ${label}`);
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("condition was not observed");
}
