/**
 * Adapts Runtime Core approval requests to isolated internal reviewer-thread
 * execution. The agent loop calls the reviewer returned by this module when
 * ToolGate requires a model decision; Runtime Pod command assembly supplies
 * the platform reviewer model, packaged policy assets, and Bridge-backed thread
 * lifecycle adapter.
 *
 * The orchestration keeps review and sidecar identities deterministic, reuses
 * one freshly minted trunk id for the parent hot lifetime, serializes trunk work
 * while using sidecars for overlap, labels parent evidence as a full re-anchor or
 * cursor delta, and lets cancellation race with exactly one outcome owner. It
 * validates the reviewer's exact JSON result before asking the hot
 * Runtime host to commit a decision or bounded failure event. In turn it calls
 * RuntimeSubAgentRunHost for preload, enqueue, wait, inspection, interruption,
 * hot close, and durable review writes, and calls the injected thread creator
 * for Bridge-owned child-thread creation and closure.
 */
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import type { RuntimeApprovalReviewer, RuntimeApprovalReviewRequest, RuntimeModelRef } from "@tetral/agent-runtime-core/src/agent-loop/agent-loop.js";
import { DurableRuntimeMessageSchema, RuntimeMessageSchema } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeJsonValue, RuntimeMessage, SessionEvent } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeAcceptedInputState, RuntimeThreadControlState } from "@tetral/agent-runtime-core/src/session/session-state.js";
import type { ReviewerExecutionToken } from "@tetral/agent-runtime-core/src/session/session-manager.js";
import type { ApprovalReviewRiskLevel, ApprovalReviewUserAuthorization, ApprovalReviewerOutcome } from "@tetral/agent-runtime-core/src/tools/tool-gate.js";
import { Effect, Fiber } from "effect";
import { semanticErrorFields } from "@tetral/ts-observability";
import type { RuntimeSubAgentRunHost } from "./core-hosts.js";
import type { RuntimePodLogger } from "./logger.js";

const DefaultReviewerWaitTimeoutMs = 120_000;
const DefaultFailureCommitTimeoutMs = 250;
const FailureMessageMaxLength = 512;
const ParentTranscriptDeltaNote = "History added since your last assessment — continue the same review conversation.";
const ParentTranscriptReanchorNote = "Full parent transcript — treat this as your first assessment of this conversation; it may repeat evidence you have seen before.";

type ApprovalReviewFailureEvent = Extract<SessionEvent, { readonly type: "approval_review.failure" }>;
type ApprovalReviewFailureKind = ApprovalReviewFailureEvent["failure_kind"];
type ParsedApprovalReviewerOutcome =
  | Extract<ApprovalReviewerOutcome, { readonly type: "decision" }>
  | { readonly type: "failed"; readonly message: string; readonly failureKind: ApprovalReviewFailureKind };

/** Supplies the policy instructions and exact JSON output schema for reviewer runs. */
export interface ApprovalReviewerAssets {
  /** Platform-owned instructions added to approval-reviewer provider requests. */
  readonly policyPrompt: string;
  /** Serialized schema included in each review prompt and parsed before use. */
  readonly outputSchemaJson: string;
}

/**
 * Provides the durable child-thread lifecycle used around hot reviewer
 * execution. The Runtime Pod implementation delegates these operations to the
 * Agent Runtime Bridge and reports failures as bounded, model-independent
 * messages.
 */
export interface RuntimeApprovalReviewerThreadCreator {
  /** Ensures the review trunk or sidecar exists before Runtime Core preloads it. */
  readonly createApprovalReviewerThread: (
    input: ApprovalReviewerThreadCreation,
  ) => Promise<
    | { readonly ok: true }
    | { readonly ok: false; readonly message: string }
  >;
  /** Durably closes a sidecar before the corresponding hot thread is released. */
  readonly closeApprovalReviewerThread: (
    input: ApprovalReviewerThreadCreation,
  ) => Promise<
    | { readonly ok: true }
    | { readonly ok: false; readonly message: string }
  >;
}

/**
 * Describes one durable reviewer thread operation with the parent request scope
 * and reviewer identity needed by the Bridge adapter. A trunk uses the manager's
 * fresh hot-lifetime id and starts without a fork seed; a sidecar uses its
 * deterministic review id and carries a serialized parent transcript snapshot.
 */
export interface ApprovalReviewerThreadCreation {
  readonly request: RuntimeApprovalReviewRequest;
  readonly reviewId: string;
  readonly reviewerThreadId: string;
  readonly isTrunk: boolean;
  readonly threadContextPrefixJson: string;
}

/**
 * Creates the approval callback consumed by the Runtime Core agent loop.
 *
 * The returned effect coordinates review leases through the request's
 * approval-reviewer manager, runs the selected trunk or sidecar through the
 * supplied hot Runtime host, accepts only an exact allow-or-deny JSON shape,
 * and commits the resulting decision before caching and returning it. Runtime,
 * timeout, and parse failures stay in the value channel as failed reviews;
 * caller cancellation interrupts the effect and triggers separately owned
 * settlement and cleanup.
 */
export function createRuntimeApprovalReviewer(
  hostRef: () => RuntimeSubAgentRunHost | undefined,
  options: {
    readonly model: RuntimeModelRef;
    readonly threadCreator: RuntimeApprovalReviewerThreadCreator;
    readonly createId?: (prefix: string) => string;
    readonly now?: () => string;
    readonly waitTimeoutMs?: number;
    readonly failureCommitTimeoutMs?: number;
    readonly registerCommitScope?: (input: Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>) => () => void;
    readonly logger?: RuntimePodLogger;
    readonly assets?: ApprovalReviewerAssets;
  },
): RuntimeApprovalReviewer {
  const now = options.now ?? (() => new Date().toISOString());
  const waitTimeoutMs = options.waitTimeoutMs ?? DefaultReviewerWaitTimeoutMs;
  const failureCommitTimeoutMs = options.failureCommitTimeoutMs ?? DefaultFailureCommitTimeoutMs;
  const createId = options.createId ?? ((prefix: string) => `${prefix}_${randomUUID()}`);
  const reviewerModel = options.model;
  const assets = options.assets ?? loadApprovalReviewerAssets();
  return (request) => Effect.gen(function* () {
    const manager = request.approvalReviewerManager;
    const cacheKey = reviewCacheKey(request);
    const cached = manager.decisionFor(cacheKey);
    if (cached !== undefined) {
      return cached;
    }
    const reviewId = stableId("arvw", cacheKey);
    const host = hostRef();
    if (host === undefined) {
      logApprovalReviewFailure(options.logger, request, reviewId, "runtime_failure", "approval reviewer is unavailable");
      return { type: "failed", message: "approval reviewer is unavailable" };
    }

    const failWithLog = (failureKind: ApprovalReviewFailureKind, message: string): ApprovalReviewerOutcome => {
      logApprovalReviewFailure(options.logger, request, reviewId, failureKind, message);
      return { type: "failed", message };
    };
    const trunkThreadId = manager.trunkThreadId(createId);
    const trunkCreation: ApprovalReviewerThreadCreation = {
      request,
      reviewId,
      reviewerThreadId: trunkThreadId,
      isTrunk: true,
      threadContextPrefixJson: "",
    };
    let ensured: { readonly ok: true } | { readonly ok: false; readonly message: string };
    ensured = yield* Effect.promise(() => options.threadCreator.createApprovalReviewerThread(trunkCreation)).pipe(
      Effect.catchCause(() => Effect.succeed({ ok: false as const, message: "approval reviewer trunk creation failed" })),
    );
    if (!ensured.ok) {
      return failWithLog("runtime_failure", ensured.message);
    }

    const lease = manager.beginReview(reviewId);
    const trunkSnapshot = manager.trunkSnapshot();
    const currentParentBoundaryEventId = parentTranscriptBoundaryEventId(request);
    if (currentParentBoundaryEventId === undefined) {
      lease.release();
      return failWithLog("runtime_failure", "approval reviewer parent transcript has no durable boundary");
    }
    const executionThreadId = lease.kind === "trunk"
      ? trunkThreadId
      : stableId("thrd_aprv_sidecar", request.workspaceId, request.sessionId, request.sessionThreadId, reviewId);
    const executionCreation: ApprovalReviewerThreadCreation = lease.kind === "trunk"
      ? trunkCreation
      : {
          request,
          reviewId,
          reviewerThreadId: executionThreadId,
          isTrunk: false,
          threadContextPrefixJson: JSON.stringify({
            source_parent_thread_id: request.sessionThreadId,
            parent_boundary_event_id: trunkSnapshot?.parentBoundaryEventId ?? currentParentBoundaryEventId,
            review_id: reviewId,
            fork_turns: "all",
            runtime_messages_snapshot: trunkSnapshot?.messages ?? [],
          }),
        };
    const feed = lease.kind === "sidecar" && trunkSnapshot === undefined
      ? { reanchored: true, messages: [...request.parentTranscript.messages] }
      : manager.parentTranscriptFeed(request.parentTranscript);
    const control = reviewThreadControl(request, executionThreadId, reviewId);
    const input = approvalReviewInput(
      request,
      control,
      reviewId,
      reviewerModel,
      assets,
      approvalReviewCreatedAt(request, now),
      feed.messages,
      feed.reanchored,
    );
    const trunkCommitInput = { ...input, sessionThreadId: trunkThreadId };
    const unregisterCommitScope = options.registerCommitScope?.(trunkCommitInput);
    let sidecarCreated = false;
    let releaseInFinally = true;
    let resourcesReleased = false;
    const releaseResources = () => {
      if (resourcesReleased) {
        return;
      }
      resourcesReleased = true;
      unregisterCommitScope?.();
      lease.release();
    };
    const settleCancellation = (): Effect.Effect<void> => Effect.gen(function* () {
      const token = lease.executionToken();
      if (token !== undefined) {
        const interrupted = yield* Effect.promise(() => host.interruptReviewerExecution(control, token)).pipe(
          Effect.catchCause(() => Effect.succeed(undefined)),
        );
        if (interrupted === undefined || !interrupted.ok || !interrupted.terminal) {
          return yield* Effect.never;
        }
      }
      if (lease.kind === "sidecar" && sidecarCreated) {
        yield* closeApprovalReviewerSidecarEffect(
          host,
          options.threadCreator,
          executionCreation,
          control,
          options.logger,
          request,
          reviewId,
        );
      }
    });
    const failWithRecord = (
      failureKind: ApprovalReviewFailureKind,
      message: string,
      ownerSettlement: Effect.Effect<void> | undefined = undefined,
    ): Effect.Effect<ApprovalReviewerOutcome> => Effect.gen(function* () {
      if (!lease.claimOutcome()) {
        yield* settleCancellation();
        return { type: "failed", message };
      }
      const failureCommit = approvalReviewFailureCommitEffect(
        host,
        trunkCommitInput,
        approvalReviewFailureEvent(trunkCommitInput, failureKind, message),
        failureCommitTimeoutMs,
        lease.kind === "sidecar" && sidecarCreated ? { creation: executionCreation, control } : undefined,
        options.threadCreator,
        options.logger,
        request,
      );
      yield* manager.fork(
        (ownerSettlement === undefined
          ? failureCommit
          : Effect.all([failureCommit, ownerSettlement], { concurrency: "unbounded" }).pipe(Effect.asVoid)
        ).pipe(Effect.ensuring(Effect.sync(releaseResources))),
      );
      releaseInFinally = false;
      yield* Effect.yieldNow;
      return { type: "failed", message };
    });

    const owner = Effect.gen(function* () {
      if (lease.kind === "sidecar") {
        const created = yield* Effect.promise(() => options.threadCreator.createApprovalReviewerThread(executionCreation)).pipe(
          Effect.catchCause(() => Effect.succeed(undefined)),
        );
        if (created === undefined || !created.ok) {
          return yield* failWithRecord("runtime_failure", created?.message ?? "approval reviewer sidecar creation failed");
        }
        sidecarCreated = true;
      }
      if (lease.raceState() === "cancellation_won") {
        yield* settleCancellation();
        return { type: "failed" as const, message: "approval reviewer was cancelled" };
      }
      const preloaded = yield* Effect.promise(() => host.preloadThread({
          ...control,
          thread: input.thread,
        })).pipe(Effect.catchCause(() => Effect.succeed(undefined)));
      if (preloaded === undefined || (!preloaded.ok && preloaded.reason !== "thread_busy")) {
        return yield* failWithRecord("runtime_failure", "approval reviewer preload failed");
      }
      if (lease.raceState() === "cancellation_won") {
        yield* settleCancellation();
        return { type: "failed" as const, message: "approval reviewer was cancelled" };
      }
      const accepted = yield* Effect.promise(() => host.enqueueThreadInput(input)).pipe(
        Effect.catchCause(() => Effect.succeed(undefined)),
      );
      const executionToken = accepted?.ok === true ? accepted.reviewerExecutionToken : undefined;
      const tokenInstalled = executionToken !== undefined
        && executionToken.reviewId === reviewId
        && executionToken.reviewerThreadId === executionThreadId
        && lease.installExecutionToken(executionToken);
      if (lease.raceState() === "cancellation_won" && tokenInstalled) {
        yield* settleCancellation();
        return { type: "failed" as const, message: "approval reviewer was cancelled" };
      }
      if (accepted?.ok === true) {
        if (!tokenInstalled || executionToken === undefined) {
          yield* manager.fork(
            Effect.never.pipe(Effect.ensuring(Effect.sync(releaseResources))),
          );
          releaseInFinally = false;
          yield* Effect.yieldNow;
          return failWithLog("runtime_failure", "approval reviewer input was rejected");
        }
      } else {
        if (accepted === undefined && lease.kind === "trunk") {
          yield* manager.fork(
            Effect.never.pipe(Effect.ensuring(Effect.sync(releaseResources))),
          );
          releaseInFinally = false;
          yield* Effect.yieldNow;
          return failWithLog("runtime_failure", "approval reviewer input was rejected");
        }
        if (lease.kind === "sidecar" && sidecarCreated) {
          yield* manager.fork(
            closeApprovalReviewerSidecarEffect(
              host,
              options.threadCreator,
              executionCreation,
              control,
              options.logger,
              request,
              reviewId,
            ).pipe(Effect.ensuring(Effect.sync(releaseResources))),
          );
          releaseInFinally = false;
          yield* Effect.yieldNow;
        }
        return failWithLog("runtime_failure", "approval reviewer input was rejected");
      }
      const waitAbortController = new AbortController();
      const unregisterCancellation = lease.onCancellation(() => waitAbortController.abort());
      const waited = yield* Effect.promise(() => host.waitReviewerExecution(
        control,
        executionToken,
        waitTimeoutMs,
        waitAbortController.signal,
      )).pipe(
        Effect.catchCause(() => Effect.succeed(undefined)),
        Effect.ensuring(Effect.sync(unregisterCancellation)),
      );
      if (lease.raceState() === "cancellation_won") {
        yield* settleCancellation();
        return { type: "failed" as const, message: "approval reviewer was cancelled" };
      }
      if (waited === undefined || !waited.ok || (!waited.timedOut && !waited.terminal)) {
        return yield* failWithRecord("runtime_failure", "approval reviewer wait failed");
      }
      if (waited.timedOut) {
        return yield* failWithRecord(
          "timeout",
          "approval reviewer timed out",
          settleTimedOutReviewerOwnerEffect(host, control, executionToken),
        );
      }
      const snapshot = yield* Effect.promise(() => host.inspectReviewerExecution(control, executionToken)).pipe(
        Effect.catchCause(() => Effect.succeed(undefined)),
      );
      if (snapshot === undefined || !snapshot.ok || !snapshot.observed) {
        return yield* failWithRecord("runtime_failure", "approval reviewer decision is unavailable");
      }
      const decision = parseApprovalDecision(snapshot.messages);
      if (decision.type !== "decision") {
        return yield* failWithRecord(decision.failureKind, decision.message);
      }
      if (!lease.claimOutcome()) {
        yield* settleCancellation();
        return { type: "failed" as const, message: "approval reviewer was cancelled" };
      }
      const committed = yield* Effect.promise(() => host.commitApprovalReviewDecision(trunkCommitInput, approvalReviewDecisionEvent(trunkCommitInput, decision))).pipe(
        Effect.timeoutOption(`${Math.max(0, waitTimeoutMs)} millis`),
        Effect.map((result) => result._tag === "Some" ? result.value : undefined),
        Effect.catchCause(() => Effect.succeed(undefined)),
      );
      if (committed === undefined || !committed.ok) {
        if (lease.kind === "sidecar" && sidecarCreated) {
          yield* manager.fork(
            closeApprovalReviewerSidecarEffect(
              host,
              options.threadCreator,
              executionCreation,
              control,
              options.logger,
              request,
              reviewId,
            ).pipe(Effect.ensuring(Effect.sync(releaseResources))),
          );
          releaseInFinally = false;
          yield* Effect.yieldNow;
        }
        return failWithLog("runtime_failure", "approval reviewer decision was not acknowledged");
      }
      if (lease.kind === "trunk") {
        manager.completeTrunkReview(
          request.parentTranscript,
          snapshot.messages,
          currentParentBoundaryEventId,
        );
      } else {
        yield* closeApprovalReviewerSidecarEffect(host, options.threadCreator, executionCreation, control, options.logger, request, reviewId);
      }
      manager.rememberDecision(cacheKey, decision);
      return decision;
    }).pipe(Effect.ensuring(Effect.sync(() => {
      if (releaseInFinally) {
        releaseResources();
      }
    })));

    const ownerFiber = yield* manager.fork(owner);
    return yield* Fiber.join(ownerFiber).pipe(
      Effect.onInterrupt(() => Effect.sync(() => {
        lease.cancel();
      })),
    );
  });
}

function settleTimedOutReviewerOwnerEffect(
  host: RuntimeSubAgentRunHost,
  control: RuntimeThreadControlState,
  token: ReviewerExecutionToken,
): Effect.Effect<void> {
  return Effect.promise(async () => {
    const interrupted = await host.interruptReviewerExecution(control, token);
    if (!interrupted.ok || !interrupted.terminal) {
      return false;
    }
    const waited = await host.waitReviewerExecution(control, token, undefined);
    return waited.ok && waited.terminal && !waited.timedOut;
  }).pipe(
    Effect.catchCause(() => Effect.succeed(false)),
    Effect.flatMap((settled) => settled ? Effect.void : Effect.never),
  );
}

function reviewCacheKey(request: RuntimeApprovalReviewRequest): string {
  return stableId("arvwc", request.workspaceId, request.sessionId, request.sessionThreadId, request.modelRequestId, request.targetModelToolCallId, request.targetToolName, canonicalJSON(request.actionJson), canonicalJSON(request.policyContext));
}

function approvalReviewCreatedAt(request: RuntimeApprovalReviewRequest, now: () => string): string {
  return request.currentRequestTurnMessages[0]?.createdAt
    ?? request.parentTranscript.messages.at(-1)?.createdAt
    ?? now();
}

function parentTranscriptBoundaryEventId(request: RuntimeApprovalReviewRequest): string | undefined {
  for (let index = request.parentTranscript.messages.length - 1; index >= 0; index -= 1) {
    const parsed = DurableRuntimeMessageSchema.safeParse(request.parentTranscript.messages[index]);
    if (parsed.success) {
      return parsed.data.owningEventId;
    }
  }
  return undefined;
}

function reviewThreadControl(request: RuntimeApprovalReviewRequest, reviewerThreadId: string, reviewId: string): RuntimeThreadControlState {
  return {
    requestId: stableId("req", "approval-review", reviewId),
    workspaceId: request.workspaceId,
    sessionId: request.sessionId,
    sessionThreadId: reviewerThreadId,
    bindingId: request.bindingId,
    bindingGeneration: request.bindingGeneration,
    targetPodUid: request.targetPodUid,
    runtimeInputId: stableId("rin", "approval-review", reviewId),
    eventIds: [stableId("sevt", "approval-review", reviewId)],
    sequenceFrom: 0,
    sequenceTo: 0,
  };
}

function approvalReviewInput(
  request: RuntimeApprovalReviewRequest,
  control: RuntimeThreadControlState,
  reviewId: string,
  reviewerModel: RuntimeModelRef,
  assets: ApprovalReviewerAssets,
  createdAt: string,
  parentTranscriptFeed: readonly RuntimeMessage[],
  parentTranscriptReanchored: boolean,
): Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }> {
  return {
    ...control,
    kind: "approval_review",
    reviewId,
    parentThreadId: request.sessionThreadId,
    targetModelToolCallId: request.targetModelToolCallId,
    targetToolName: request.targetToolName,
    promptItems: [approvalReviewPromptMessage(
      request,
      reviewId,
      reviewerModel,
      assets,
      createdAt,
      parentTranscriptFeed,
      parentTranscriptReanchored,
    )],
    outputSchemaJson: assets.outputSchemaJson,
    thread: {
      parentThreadId: request.sessionThreadId,
      role: "approval_reviewer",
      visibility: "internal",
      agentType: "approval_reviewer",
      status: "idle",
    },
  };
}

function approvalReviewPromptMessage(
  request: RuntimeApprovalReviewRequest,
  reviewId: string,
  reviewerModel: RuntimeModelRef,
  assets: ApprovalReviewerAssets,
  createdAt: string,
  parentTranscriptFeed: readonly RuntimeMessage[],
  parentTranscriptReanchored: boolean,
): RuntimeMessage {
  const messageId = stableId("msg", "approval-review", reviewId, "prompt");
  const text = JSON.stringify({
    output_schema: JSON.parse(assets.outputSchemaJson) as RuntimeJsonValue,
    review_id: reviewId,
    parent_thread_id: request.sessionThreadId,
    target_model_tool_call_id: request.targetModelToolCallId,
    target_tool_name: request.targetToolName,
    action_json: request.actionJson,
    parent_transcript_feed_note: parentTranscriptReanchored
      ? ParentTranscriptReanchorNote
      : ParentTranscriptDeltaNote,
    parent_transcript_feed: renderTranscriptEvidence(parentTranscriptFeed),
    current_assistant_draft: renderTranscriptEvidence(request.currentRequestTurnMessages),
    sibling_tool_calls: request.siblingToolCalls,
    policy_context: request.policyContext,
  }, null, 2);
  return RuntimeMessageSchema.parse({
    id: messageId,
    sessionId: request.sessionId,
    role: "user",
    origin: "runtime",
    sequence: 0,
    status: "completed",
    createdAt,
    parts: [{
      id: stableId("part", messageId, "text"),
      sessionId: request.sessionId,
      messageId,
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

/**
 * Loads the packaged reviewer policy and output schema used by Runtime Pod
 * startup and validates that the schema asset is JSON before returning it.
 */
export function loadApprovalReviewerAssets(): ApprovalReviewerAssets {
  const policyPrompt = readFileSync(new URL("./assets/approval-reviewer-policy.md", import.meta.url), "utf8").trim();
  const outputSchemaJson = readFileSync(new URL("./assets/approval-reviewer-output-schema.json", import.meta.url), "utf8").trim();
  JSON.parse(outputSchemaJson);
  return { policyPrompt, outputSchemaJson };
}

function renderTranscriptEvidence(messages: readonly RuntimeMessage[]): readonly RuntimeJsonValue[] {
  return messages.map((message) => ({
      role: message.role,
      status: message.status,
      text: message.parts
        .filter((part) => part.type === "text")
        .map((part) => part.text)
        .join("\n"),
      reasoning: message.parts
        .filter((part) => part.type === "reasoning")
        .map((part) => part.text)
        .join("\n"),
      tool_calls: message.parts
        .filter((part) => part.type === "tool")
        .map((part) => ({
          tool_call_id: part.toolCallId,
          tool_name: part.toolName,
          status: part.state.status,
          ...(part.toolUseEventId !== undefined ? { tool_use_event_id: part.toolUseEventId } : {}),
          ...("input" in part.state && part.state.input !== undefined
            ? {
                input: part.state.input.value ?? part.state.input.preview,
                input_truncated: part.state.input.truncated,
              }
            : {}),
        })),
    }));
}

function parseApprovalDecision(messages: readonly RuntimeMessage[]): ParsedApprovalReviewerOutcome {
  const text = latestAssistantText(messages);
  if (text === undefined) {
    return { type: "failed", failureKind: "runtime_failure", message: "approval reviewer returned no decision" };
  }
  const parsed = parseJSONDecision(text);
  if (!isRecord(parsed)) {
    return { type: "failed", failureKind: "parse_failure", message: "approval reviewer decision is not JSON" };
  }
  if (!hasExactApprovalDecisionFields(parsed)) {
    return { type: "failed", failureKind: "parse_failure", message: "approval reviewer decision fields are invalid" };
  }
  const outcome = parsed.outcome;
  if (outcome !== "allow" && outcome !== "deny") {
    return { type: "failed", failureKind: "parse_failure", message: "approval reviewer decision outcome is invalid" };
  }
  const riskLevel = parsed.risk_level;
  if (!isApprovalReviewRiskLevel(riskLevel)) {
    return { type: "failed", failureKind: "parse_failure", message: "approval reviewer decision risk_level is invalid" };
  }
  const userAuthorization = parsed.user_authorization;
  if (!isApprovalReviewUserAuthorization(userAuthorization)) {
    return { type: "failed", failureKind: "parse_failure", message: "approval reviewer decision user_authorization is invalid" };
  }
  const rationale = parsed.rationale;
  if (typeof rationale !== "string") {
    return { type: "failed", failureKind: "parse_failure", message: "approval reviewer decision rationale is invalid" };
  }
  return {
    type: "decision",
    riskLevel,
    userAuthorization,
    outcome,
    ...(rationale.length > 0 ? { message: rationale } : {}),
  };
}

function hasExactApprovalDecisionFields(value: Record<string, unknown>): boolean {
  const fields = Object.keys(value).sort();
  return fields.length === 4 && fields.every(
    (field, index) => field === ["outcome", "rationale", "risk_level", "user_authorization"][index],
  );
}

function approvalReviewDecisionEvent(
  input: Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>,
  decision: Extract<ApprovalReviewerOutcome, { readonly type: "decision" }>,
): Extract<SessionEvent, { readonly type: "approval_review.decision" }> {
  return {
    type: "approval_review.decision" as const,
    review_id: input.reviewId,
    parent_thread_id: input.parentThreadId,
    target_model_tool_call_id: input.targetModelToolCallId,
    target_tool_name: input.targetToolName,
    risk_level: decision.riskLevel,
    user_authorization: decision.userAuthorization,
    outcome: decision.outcome,
    rationale: decision.message ?? "",
  };
}

function approvalReviewFailureEvent(
  input: Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>,
  failureKind: ApprovalReviewFailureKind,
  message: string,
): ApprovalReviewFailureEvent {
  return {
    type: "approval_review.failure" as const,
    review_id: input.reviewId,
    parent_thread_id: input.parentThreadId,
    target_model_tool_call_id: input.targetModelToolCallId,
    target_tool_name: input.targetToolName,
    failure_kind: failureKind,
    message: safeReviewFailureMessage(message),
  };
}

function approvalReviewFailureCommitEffect(
  host: RuntimeSubAgentRunHost,
  input: Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>,
  event: ApprovalReviewFailureEvent,
  timeoutMs: number,
  sidecar: { readonly creation: ApprovalReviewerThreadCreation; readonly control: RuntimeThreadControlState } | undefined,
  threadCreator: RuntimeApprovalReviewerThreadCreator,
  logger: RuntimePodLogger | undefined,
  request: RuntimeApprovalReviewRequest,
): Effect.Effect<void> {
  return Effect.gen(function* () {
      const acknowledged = yield* commitApprovalReviewFailureBestEffort(host, input, event, timeoutMs);
      if (!acknowledged) {
        logApprovalReviewFailure(logger, request, input.reviewId, event.failure_kind, event.message);
        if (sidecar !== undefined) {
          yield* closeApprovalReviewerSidecarEffect(host, threadCreator, sidecar.creation, sidecar.control, logger, request, input.reviewId);
        }
        return;
      }
      if (sidecar !== undefined) {
        yield* closeApprovalReviewerSidecarEffect(host, threadCreator, sidecar.creation, sidecar.control, logger, request, input.reviewId);
      }
  }).pipe(Effect.catchCause(() => Effect.sync(() => {
      logApprovalReviewFailure(logger, request, input.reviewId, event.failure_kind, event.message);
  })));
}

function commitApprovalReviewFailureBestEffort(
  host: RuntimeSubAgentRunHost,
  input: Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }>,
  event: ApprovalReviewFailureEvent,
  timeoutMs: number,
): Effect.Effect<boolean> {
  return Effect.promise(() => host.commitApprovalReviewFailure(input, event)).pipe(
    Effect.map((result) => result.ok),
    Effect.catchCause(() => Effect.succeed(false)),
    Effect.timeoutOption(`${Math.max(0, timeoutMs)} millis`),
    Effect.map((result) => result._tag === "Some" ? result.value : false),
  );
}

function closeApprovalReviewerSidecarEffect(
  host: RuntimeSubAgentRunHost,
  threadCreator: RuntimeApprovalReviewerThreadCreator,
  creation: ApprovalReviewerThreadCreation,
  control: RuntimeThreadControlState,
  logger: RuntimePodLogger | undefined,
  request: RuntimeApprovalReviewRequest,
  reviewId: string,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    const durable = yield* Effect.promise(() => threadCreator.closeApprovalReviewerThread(creation));
    if (!durable.ok) {
      logApprovalReviewFailure(logger, request, reviewId, "runtime_failure", durable.message);
      return;
    }
    const hot = yield* Effect.promise(() => host.markThreadClosed(control));
    if (!hot.ok) {
      logApprovalReviewFailure(logger, request, reviewId, "runtime_failure", "approval reviewer sidecar hot close failed");
    }
  }).pipe(Effect.catchCause(() => Effect.sync(() => {
    logApprovalReviewFailure(logger, request, reviewId, "runtime_failure", "approval reviewer sidecar close failed");
  })));
}

function logApprovalReviewFailure(
  logger: RuntimePodLogger | undefined,
  request: RuntimeApprovalReviewRequest,
  reviewId: string,
  failureKind: ApprovalReviewFailureKind,
  message: string,
): void {
  const safeMessage = safeReviewFailureMessage(message);
  logger?.error({
    event: "approval_review_failure",
    "event.kind": "approval_review.failure",
    operation: "approval_review",
    component: "agent-runtime",
    "request.id": request.modelRequestId,
    "workspace.id": request.workspaceId,
    "session.id": request.sessionId,
    "thread.id": request.sessionThreadId,
    "review.id": reviewId,
    "target.tool_call.id": request.targetModelToolCallId,
    "target.tool.name": request.targetToolName,
    "approval.failure_kind": failureKind,
    message: safeMessage,
    ...semanticErrorFields({
      errorClass: "approval_review_failure",
      errorCode: failureKind,
      messageSafe: safeMessage,
    }),
  });
}

function safeReviewFailureMessage(message: string): string {
  const normalized = message.replace(/\s+/g, " ").trim();
  return normalized.length <= FailureMessageMaxLength ? normalized : `${normalized.slice(0, FailureMessageMaxLength - 3)}...`;
}

function latestAssistantText(messages: readonly RuntimeMessage[]): string | undefined {
  const message = messages.at(-1);
  if (message?.role !== "assistant") {
    return undefined;
  }
  const text = message.parts
    .filter((part) => part.type === "text")
    .map((part) => part.text)
    .join("\n")
    .trim();
  return text.length > 0 ? text : undefined;
}

function parseJSONDecision(text: string): unknown {
  try {
    return JSON.parse(text.trim());
  } catch {
    return undefined;
  }
}

function isApprovalReviewRiskLevel(value: unknown): value is ApprovalReviewRiskLevel {
  return value === "low" || value === "medium" || value === "high" || value === "critical";
}

function isApprovalReviewUserAuthorization(value: unknown): value is ApprovalReviewUserAuthorization {
  return value === "unknown" || value === "low" || value === "medium" || value === "high";
}

function canonicalJSON(value: RuntimeJsonValue): string {
  return JSON.stringify(sortJSON(value));
}

function sortJSON(value: RuntimeJsonValue): RuntimeJsonValue {
  if (Array.isArray(value)) {
    return value.map(sortJSON);
  }
  if (!isRecord(value)) {
    return value;
  }
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJSON(value[key] as RuntimeJsonValue)]));
}

function stableId(prefix: string, ...parts: readonly string[]): string {
  const hash = createHash("sha256");
  for (const part of parts) {
    hash.update(part);
    hash.update("\0");
  }
  return `${prefix}_${hash.digest("hex").slice(0, 24)}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
