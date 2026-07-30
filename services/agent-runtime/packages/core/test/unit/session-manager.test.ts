import { describe, expect, test } from "bun:test";
import { Context, Effect, Exit, Fiber, Layer, Scope } from "effect";
import {
  DurableRuntimeMessageSchema,
  normalizeRuntimeFailure,
  normalizeSessionEventWriterError,
} from "../../src/contracts/runtime.js";
import type {
  RuntimeAcceptedInputState,
  RuntimeAcceptedThreadMetadataState,
  RuntimeConfigPatchState,
  RuntimeControlInputDeclaration,
  RuntimeThreadControlState,
} from "../../src/session/session-state.js";
import * as AgentLoop from "../../src/agent-loop/agent-loop.js";
import * as Session from "../../src/session/session.js";
import * as SessionManager from "../../src/session/session-manager.js";
import type { RuntimeHotStateMetrics, RuntimeMetricsSink } from "../../src/runtime/metrics.js";
import {
  buildSessionManagerBridgeRuntimeMessage as bridgeRuntimeMessage,
  buildSessionManagerColdUserMessage as coldUserMessage,
  buildSessionManagerReviewerDecisionMessage as reviewerDecisionMessage,
  buildRuntimeControlCommitResult,
} from "./runtime-message-builders.js";

const timestamp = "2026-06-14T00:00:00.000Z";

function testControlCommit(
  scope: RuntimeThreadControlState,
  inputKind: "interrupt_control" | "tool_confirmation" = "interrupt_control",
) {
  return async (declaration: RuntimeControlInputDeclaration) =>
    buildRuntimeControlCommitResult(scope, inputKind, declaration);
}

function pendingApprovalToolPart(sessionId: string, toolUseEventId: string) {
  return {
    id: `part_${toolUseEventId}`,
    sessionId,
    messageId: `msg_${toolUseEventId}`,
    sequence: 0,
    createdAt: timestamp,
    type: "tool" as const,
    toolCallId: `call_${toolUseEventId}`,
    toolName: "Write",
    toolUseEventId,
    state: { status: "pending" as const },
  };
}

function pendingApprovalAssistantMessage(sessionId: string, toolUseEventId: string) {
  const toolPart = pendingApprovalToolPart(sessionId, toolUseEventId);
  return DurableRuntimeMessageSchema.parse({
    id: toolPart.messageId,
    sessionId,
    owningEventId: toolUseEventId,
    eventSequence: 1,
    sequence: 1,
    role: "assistant",
    origin: "agent",
    status: "completed",
    createdAt: timestamp,
    parts: [toolPart],
  });
}

function coldCoverage(overrides: {
  readonly pendingToolIds?: readonly string[];
  readonly pendingAttachmentIdentities?: readonly string[];
  readonly undeliveredMailDeliveryIds?: readonly string[];
} = {}) {
  return {
    pendingToolIds: overrides.pendingToolIds ?? [],
    pendingAttachmentIdentities: overrides.pendingAttachmentIdentities ?? [],
    undeliveredMailDeliveryIds: overrides.undeliveredMailDeliveryIds ?? [],
  };
}

function acceptedInput(
  sessionId: string,
  runtimeInputId = `rin_${sessionId}`,
  sessionThreadId = `thrd_${sessionId}`,
): RuntimeAcceptedInputState {
  return {
    requestId: `req_${runtimeInputId}`,
    workspaceId: "wksp_test",
    sessionId,
    sessionThreadId,
    bindingId: `bind_${sessionId}`,
    bindingGeneration: 1,
    targetPodUid: `pod_${sessionId}`,
    runtimeInputId,
    eventIds: [`sevt_${runtimeInputId}`],
    sequenceFrom: 1,
    sequenceTo: 1,
    kind: "messages",
    payloadJson: "{}",
  };
}

function approvalReviewInput(
  sessionId: string,
  runtimeInputId: string,
  sessionThreadId: string,
  parentThreadId: string,
): RuntimeAcceptedInputState {
  return {
    ...acceptedInput(sessionId, runtimeInputId, sessionThreadId),
    kind: "approval_review",
    reviewId: `arvw_${runtimeInputId}`,
    parentThreadId,
    targetModelToolCallId: `tool_call_${runtimeInputId}`,
    targetToolName: "Write",
    promptItems: [],
    outputSchemaJson: "{}",
    thread: {
      parentThreadId,
      role: "approval_reviewer",
      visibility: "internal",
      agentType: "approval_reviewer",
      status: "idle",
    },
  };
}

function agentMailInput(
  sessionId: string,
  runtimeInputId: string,
  sessionThreadId: string,
  sourceThreadId: string,
  thread: RuntimeAcceptedThreadMetadataState,
): Extract<RuntimeAcceptedInputState, { readonly kind: "inter_agent_message" }> {
  return {
    ...threadControl(sessionId, runtimeInputId, sessionThreadId),
    kind: "inter_agent_message",
    deliveryId: runtimeInputId.replace("agent_mail:", ""),
    sourceThreadId,
    sourceToolUseEventId: `sevt_${sourceThreadId}`,
    message: bridgeRuntimeMessage(sessionId, `completion from ${sourceThreadId}`),
    thread,
  };
}

function threadControl(
  sessionId: string,
  runtimeInputId = `rin_control_${sessionId}`,
  sessionThreadId = `thrd_${sessionId}`,
): RuntimeThreadControlState {
  return {
    requestId: `req_${runtimeInputId}`,
    workspaceId: "wksp_test",
    sessionId,
    sessionThreadId,
    bindingId: `bind_${sessionId}`,
    bindingGeneration: 1,
    targetPodUid: `pod_${sessionId}`,
    runtimeInputId,
    eventIds: [`sevt_${runtimeInputId}`],
    sequenceFrom: 1,
    sequenceTo: 1,
  };
}

interface RunRecord {
  readonly sessionId: string;
  readonly session: Session.Session;
  readonly args: readonly unknown[];
  readonly release: (result?: AgentLoop.AgentLoopRunResult) => void;
}

interface CrashRunRecord {
  readonly sessionId: string;
  readonly session: Session.Session;
  readonly releaseCrash: () => void;
}

interface ControlledAgentLoop {
  readonly runs: RunRecord[];
  readonly layer: Layer.Layer<AgentLoop.Service>;
}

interface ControlledCrashAgentLoop {
  readonly runs: CrashRunRecord[];
  readonly layer: Layer.Layer<AgentLoop.Service>;
}

interface InterruptRecordingAgentLoop {
  readonly runs: Array<{ readonly sessionId: string; readonly session: Session.Session }>;
  readonly interruptions: Array<{
    readonly sessionId: string;
    readonly runtimeShutdownRequested: boolean;
  }>;
  readonly layer: Layer.Layer<AgentLoop.Service>;
}

interface InterruptCleanupAgentLoop {
  readonly runs: RunRecord[];
  readonly cleanupStarted: Promise<void>;
  readonly releaseCleanup: () => void;
  readonly layer: Layer.Layer<AgentLoop.Service>;
}

interface ReviewerInterruptCleanupAgentLoop extends ControlledAgentLoop {
  readonly reviewerCleanupStarted: Promise<void>;
  readonly releaseReviewerCleanup: () => void;
}

interface ReviewerGenerationAgentLoop {
  readonly observedReviewerInputIds: string[];
  readonly targetAStarted: Promise<void>;
  readonly cleanupStarted: Promise<void>;
  readonly releaseCleanup: () => void;
  readonly targetBStarted: Promise<void>;
  readonly releaseTargetB: () => void;
  readonly layer: Layer.Layer<AgentLoop.Service>;
}

interface FollowUpCleanupAgentLoop {
  readonly runs: RunRecord[];
  readonly followUpCleanupStarted: Promise<void>;
  readonly releaseFollowUpCleanup: () => void;
  readonly layer: Layer.Layer<AgentLoop.Service>;
}

function agentLoopService(overrides: Pick<AgentLoop.Interface, "run"> & Partial<AgentLoop.Interface>): AgentLoop.Interface {
  return AgentLoop.Service.of({
    closeFailedRun: () => Effect.succeed({ type: "landed" }),
    seedRuntimeModel: () => {},
    installLoadedPendingToolUses: () => Effect.succeed({ ok: true }),
    ...overrides,
  });
}

function makeControlledAgentLoop(overrides: Partial<AgentLoop.Interface> = {}): ControlledAgentLoop {
  const runs: RunRecord[] = [];
  const layer = Layer.succeed(
    AgentLoop.Service,
    agentLoopService({
      run: (...args: readonly [Session.Session, ...unknown[]]) =>
        Effect.promise(
          () =>
            new Promise<AgentLoop.AgentLoopRunResult>((resolve) => {
              const session = args[0];
              runs.push({
                sessionId: session.sessionId,
                session,
                args,
                release: (result) => {
                  const acceptedInput = session.state.peekAcceptedInput();
                  if (acceptedInput !== undefined) {
                    session.state.acknowledgeAcceptedInput(acceptedInput.runtimeInputId);
                  }
                  resolve(result ?? { type: "completed" as const, modelMessageCount: 0 });
                },
              });
            }),
        ),
      ...overrides,
    }),
  );
  return { runs, layer };
}

function makeInputConsumingControlledAgentLoop(): ControlledAgentLoop {
  const runs: RunRecord[] = [];
  const layer = Layer.succeed(
    AgentLoop.Service,
    agentLoopService({
      run: (session) =>
        Effect.promise(
          () =>
            new Promise<AgentLoop.AgentLoopRunResult>((resolve) => {
              let acceptedInput = session.state.peekAcceptedInput();
              while (acceptedInput !== undefined) {
                session.state.acknowledgeAcceptedInput(acceptedInput.runtimeInputId);
                acceptedInput = session.state.peekAcceptedInput();
              }
              runs.push({
                sessionId: session.sessionId,
                session,
                args: [session],
                release: (result) => resolve(result ?? { type: "completed" as const, modelMessageCount: 0 }),
              });
            }),
        ),
    }),
  );
  return { runs, layer };
}

function makeInterruptRecordingAgentLoop(): InterruptRecordingAgentLoop {
  const runs: Array<{ readonly sessionId: string; readonly session: Session.Session }> = [];
  const interruptions: InterruptRecordingAgentLoop["interruptions"] = [];
  const layer = Layer.succeed(
    AgentLoop.Service,
    agentLoopService({
      run: (session) =>
        Effect.sync(() => {
          runs.push({ sessionId: session.sessionId, session });
        }).pipe(
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() => Effect.promise(async () => {
            interruptions.push({
              sessionId: session.sessionId,
              runtimeShutdownRequested: session.state.runtimeShutdownRequested(),
            });
            if (session.state.userInterruptRequested()) {
              const runtimeInputId = session.state.userInterruptCommand()?.runtimeInputId;
              await session.state.commitUserInterruptInput({ drafts: [], pendingToolCancellations: [] });
              if (runtimeInputId !== undefined) {
                session.state.completeUserInterrupt(runtimeInputId);
              }
            }
          })),
        ),
    }),
  );
  return { runs, interruptions, layer };
}

function makeInterruptCleanupAgentLoop(): InterruptCleanupAgentLoop {
  const runs: RunRecord[] = [];
  let cleanupStartedResolve: () => void = () => {};
  let releaseCleanupResolve: () => void = () => {};
  const cleanupStarted = new Promise<void>((resolve) => {
    cleanupStartedResolve = resolve;
  });
  const cleanupReleased = new Promise<void>((resolve) => {
    releaseCleanupResolve = resolve;
  });
  const layer = Layer.succeed(
    AgentLoop.Service,
    agentLoopService({
      run: (session) => {
        const runIndex = runs.length;
        if (runIndex === 0) {
          return Effect.sync(() => {
            runs.push({
              sessionId: session.sessionId,
              session,
              args: [session],
              release: () => {},
            });
          }).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() => Effect.promise(async () => {
              cleanupStartedResolve();
              await cleanupReleased;
              if (session.state.userInterruptRequested()) {
                await session.state.commitUserInterruptInput({ drafts: [], pendingToolCancellations: [] });
                const runtimeInputId = session.state.userInterruptCommand()?.runtimeInputId;
                if (runtimeInputId !== undefined) {
                  session.state.completeUserInterrupt(runtimeInputId);
                }
              }
            })),
          );
        }
        return Effect.promise(
          () =>
            new Promise<AgentLoop.AgentLoopRunResult>((resolve) => {
              runs.push({
                sessionId: session.sessionId,
                session,
                args: [session],
                release: (value) => resolve(value ?? { type: "completed", modelMessageCount: 0 }),
              });
            }),
        );
      },
    }),
  );
  return { runs, cleanupStarted, releaseCleanup: releaseCleanupResolve, layer };
}

function makeReviewerInterruptCleanupAgentLoop(reviewerThreadId: string): ReviewerInterruptCleanupAgentLoop {
  const runs: RunRecord[] = [];
  let reviewerCleanupStartedResolve: () => void = () => {};
  let releaseReviewerCleanupResolve: () => void = () => {};
  const reviewerCleanupStarted = new Promise<void>((resolve) => {
    reviewerCleanupStartedResolve = resolve;
  });
  const reviewerCleanupReleased = new Promise<void>((resolve) => {
    releaseReviewerCleanupResolve = resolve;
  });
  const layer = Layer.succeed(
    AgentLoop.Service,
    agentLoopService({
      run: (session) => {
        if (session.identity.sessionThreadId === reviewerThreadId) {
          return Effect.sync(() => {
            runs.push({
              sessionId: session.sessionId,
              session,
              args: [session],
              release: () => {},
            });
          }).pipe(
            Effect.flatMap(() => Effect.never),
            Effect.onInterrupt(() => Effect.promise(async () => {
              reviewerCleanupStartedResolve();
              await reviewerCleanupReleased;
            })),
          );
        }
        return Effect.promise(
          () => new Promise<AgentLoop.AgentLoopRunResult>((resolve) => {
            runs.push({
              sessionId: session.sessionId,
              session,
              args: [session],
              release: (result) => resolve(result ?? { type: "completed", modelMessageCount: 0 }),
            });
          }),
        );
      },
    }),
  );
  return {
    runs,
    reviewerCleanupStarted,
    releaseReviewerCleanup: releaseReviewerCleanupResolve,
    layer,
  };
}

function makeReviewerGenerationAgentLoop(): ReviewerGenerationAgentLoop {
  let runIndex = 0;
  const observedReviewerInputIds: string[] = [];
  let cleanupStartedResolve: () => void = () => {};
  let releaseCleanupResolve: () => void = () => {};
  let targetBStartedResolve: () => void = () => {};
  let releaseTargetBResolve: () => void = () => {};
  let targetAStartedResolve: () => void = () => {};
  const targetAStarted = new Promise<void>((resolve) => {
    targetAStartedResolve = resolve;
  });
  const cleanupStarted = new Promise<void>((resolve) => {
    cleanupStartedResolve = resolve;
  });
  const cleanupReleased = new Promise<void>((resolve) => {
    releaseCleanupResolve = resolve;
  });
  const targetBStarted = new Promise<void>((resolve) => {
    targetBStartedResolve = resolve;
  });
  const targetBReleased = new Promise<void>((resolve) => {
    releaseTargetBResolve = resolve;
  });
  const layer = Layer.succeed(
    AgentLoop.Service,
    agentLoopService({
      run: (session) => {
        const currentRun = runIndex;
        runIndex += 1;
        const acceptedInput = session.state.peekAcceptedInput();
        if (acceptedInput?.kind === "approval_review") {
          observedReviewerInputIds.push(acceptedInput.runtimeInputId);
        }
        const recordDecision = Effect.sync(() => {
          session.state.contextManager.appendMessage(reviewerDecisionMessage(
            session.sessionId,
            currentRun === 0 ? "msg_target_a" : "msg_target_b",
            currentRun === 0 ? "target A verdict" : "target B verdict",
          ));
        });
        if (currentRun > 0) {
          return recordDecision.pipe(Effect.andThen(Effect.promise(async () => {
              targetBStartedResolve();
              await targetBReleased;
              return { type: "completed" as const, modelMessageCount: 1 };
            })));
        }
        return recordDecision.pipe(
          Effect.andThen(Effect.sync(targetAStartedResolve)),
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() => Effect.promise(async () => {
            cleanupStartedResolve();
            await cleanupReleased;
          })),
        );
      },
    }),
  );
  return {
    observedReviewerInputIds,
    targetAStarted,
    cleanupStarted,
    releaseCleanup: releaseCleanupResolve,
    targetBStarted,
    releaseTargetB: releaseTargetBResolve,
    layer,
  };
}

function makeFollowUpCleanupAgentLoop(): FollowUpCleanupAgentLoop {
  const runs: RunRecord[] = [];
  let cleanupStartedResolve: () => void = () => {};
  let releaseCleanupResolve: () => void = () => {};
  const followUpCleanupStarted = new Promise<void>((resolve) => {
    cleanupStartedResolve = resolve;
  });
  const cleanupReleased = new Promise<void>((resolve) => {
    releaseCleanupResolve = resolve;
  });
  const layer = Layer.succeed(
    AgentLoop.Service,
    agentLoopService({
      run: (session) => {
        const runIndex = runs.length;
        const run = Effect.promise(
          () =>
            new Promise<AgentLoop.AgentLoopRunResult>((resolve) => {
              runs.push({
                sessionId: session.sessionId,
                session,
                args: [session],
                release: (result) => {
                  const acceptedInput = session.state.peekAcceptedInput();
                  if (acceptedInput !== undefined) {
                    session.state.acknowledgeAcceptedInput(acceptedInput.runtimeInputId);
                  }
                  resolve(result ?? { type: "completed", modelMessageCount: 0 });
                },
              });
            }),
        );
        return runIndex === 1
          ? run.pipe(
              Effect.ensuring(
                Effect.promise(async () => {
                  cleanupStartedResolve();
                  await cleanupReleased;
                }),
              ),
            )
          : run;
      },
    }),
  );
  return { runs, followUpCleanupStarted, releaseFollowUpCleanup: releaseCleanupResolve, layer };
}

function makeControlledCrashAgentLoop(
  mode: "fail" | "die" | "reject",
  overrides: Partial<AgentLoop.Interface> = {},
): ControlledCrashAgentLoop {
  const runs: CrashRunRecord[] = [];
  const layer = Layer.succeed(
    AgentLoop.Service,
    agentLoopService({
      run: (session) => {
        if (mode === "reject") {
          return Effect.promise(
            () =>
              new Promise<AgentLoop.AgentLoopRunResult>((_resolve, reject) => {
                runs.push({
                  sessionId: session.sessionId,
                  session,
                  releaseCrash: () => reject(new Error(hostileText)),
                });
              }),
          );
        }
        const release = Effect.promise(
          () =>
            new Promise<void>((resolve) => {
              runs.push({
                sessionId: session.sessionId,
                session,
                releaseCrash: resolve,
              });
            }),
        );
        if (mode === "fail") {
          return release.pipe(
            Effect.flatMap(() =>
              Effect.fail(
                normalizeRuntimeFailure({
                  type: "runtime",
                  code: "runtime_invalid_sequence",
                  retryable: false,
                  fatal: true,
                  reason: "runtime_contract_validation",
                }),
              ),
            ),
          );
        }
        return release.pipe(Effect.flatMap(() => Effect.die(new Error("agent loop defect"))));
      },
      ...overrides,
    }),
  );
  return { runs, layer };
}

const hostileText = [
  "CONTROL_INPUT_DUMMY_TOKEN_CANARY",
  "select secret from sessions",
  "postgres://user:pass@example.invalid/db",
  "authorization: bearer raw-secret",
  "system prompt raw backend payload marker",
  "raw backend payload marker",
  "raw provider payload marker",
].join(" ");

const forbiddenHostileFragments = [
  "CONTROL_INPUT_DUMMY_TOKEN_CANARY",
  "select secret from sessions",
  "postgres://user:pass@example.invalid/db",
  "authorization: bearer raw-secret",
  "system prompt raw backend payload marker",
  "raw backend payload marker",
  "raw provider payload marker",
] as const;

function sessionManagerLayer(
  agentLoop: { readonly layer: Layer.Layer<AgentLoop.Service> },
  options: {
    readonly maxLocalSessions?: number;
    readonly metrics?: RuntimeMetricsSink;
    readonly loadThreadContext?: SessionManager.LayerOptions["loadThreadContext"];
    readonly closeoutMonotonicMs?: SessionManager.LayerOptions["closeoutMonotonicMs"];
    readonly closeoutSleep?: SessionManager.LayerOptions["closeoutSleep"];
    readonly recordCloseoutEvent?: SessionManager.LayerOptions["recordCloseoutEvent"];
  } = {},
): Layer.Layer<SessionManager.Service> {
  return SessionManager.layer({
    maxLocalSessions: options.maxLocalSessions ?? 10,
    now: () => timestamp,
    ...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
    ...(options.loadThreadContext !== undefined ? { loadThreadContext: options.loadThreadContext } : {}),
    ...(options.closeoutMonotonicMs !== undefined ? { closeoutMonotonicMs: options.closeoutMonotonicMs } : {}),
    ...(options.closeoutSleep !== undefined ? { closeoutSleep: options.closeoutSleep } : {}),
    ...(options.recordCloseoutEvent !== undefined ? { recordCloseoutEvent: options.recordCloseoutEvent } : {}),
  }).pipe(Layer.provide(agentLoop.layer));
}

async function withSessionManager<T>(
  layer: Layer.Layer<SessionManager.Service>,
  useManager: (manager: TestSessionManager) => Promise<T>,
): Promise<T> {
  const { manager, scope } = await Effect.runPromise(
    Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(layer, layerScope);
      return { manager: Context.get(context, SessionManager.Service), scope: layerScope };
    }),
  );
  try {
    return await useManager(testSessionManager(manager));
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
}

type TestRunStartResult =
  | { readonly ok: true; readonly sessionId: string; readonly created: boolean; readonly started: boolean }
  | { readonly ok: false; readonly sessionId: string; readonly reason: "local_session_capacity_exceeded" };

type TestSessionManager = SessionManager.Interface & {
  readonly startTestRunThroughAcceptedInput: (sessionId: string) => Effect.Effect<TestRunStartResult>;
};

let nextTestRunInput = 0;

function testSessionManager(manager: SessionManager.Interface): TestSessionManager {
  const startTestRunThroughAcceptedInput = (sessionId: string): Effect.Effect<TestRunStartResult> =>
    Effect.promise(async () => {
      let joinedActiveRun = false;
      for (;;) {
        const inspected = await Effect.runPromise(manager.inspectThread(threadControl(sessionId)));
        if (inspected.ok && inspected.observed && inspected.status === "running") {
          joinedActiveRun = true;
          await new Promise((resolve) => setTimeout(resolve, 1));
          continue;
        }
        if (joinedActiveRun && inspected.ok && inspected.observed) {
          return { ok: true, sessionId, created: false, started: false };
        }
        nextTestRunInput += 1;
        const accepted = await Effect.runPromise(manager.acceptInput(acceptedInput(sessionId, `rin_test_run_${nextTestRunInput}`)));
        if (!accepted.ok) {
          return { ok: false, sessionId, reason: "local_session_capacity_exceeded" };
        }
        return { ok: true, sessionId, created: accepted.created, started: accepted.started };
      }
    });
  Object.defineProperty(manager, "startTestRunThroughAcceptedInput", { value: startTestRunThroughAcceptedInput, enumerable: false });
  return manager as TestSessionManager;
}

async function waitForRuns(agentLoop: ControlledAgentLoop, count: number): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (agentLoop.runs.length >= count) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`expected ${count} AgentLoop runs, observed ${agentLoop.runs.length}`);
}

async function waitForCondition(predicate: () => boolean | Promise<boolean>, label: string): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (await predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`timed out waiting for ${label}`);
}

async function waitForThreadIdle(manager: SessionManager.Interface, sessionId: string, sessionThreadId: string): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const result = await Effect.runPromise(manager.inspectThread(threadControl(sessionId, undefined, sessionThreadId)));
    if (result.ok && result.observed && result.status === "idle") {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`expected ${sessionThreadId} to become idle`);
}

async function waitForCrashRuns(agentLoop: ControlledCrashAgentLoop, count: number): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (agentLoop.runs.length >= count) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`expected ${count} AgentLoop crash runs, observed ${agentLoop.runs.length}`);
}

async function waitForInterruptRecordingRuns(agentLoop: InterruptRecordingAgentLoop, count: number): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (agentLoop.runs.length >= count) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`expected ${count} AgentLoop runs, observed ${agentLoop.runs.length}`);
}

async function waitForIdleCleanup(manager: SessionManager.Interface, sessionId: string): Promise<SessionManager.CleanupSessionResult> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const result = await Effect.runPromise(manager.cleanupSession(sessionId, threadControl(sessionId)));
    if (result.ok) {
      return result;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`expected idle cleanup for ${sessionId}`);
}

function expectNoHostileFragments(value: unknown): void {
  const serialized = JSON.stringify(value);
  for (const fragment of forbiddenHostileFragments) {
    expect(serialized).not.toContain(fragment);
  }
}

function fatalRunResult(reason: "persistence_failed" | "event_write_failed" | "crashed"): AgentLoop.AgentLoopRunResult {
  return {
    type: "failed",
    error: normalizeRuntimeFailure({
      type: reason === "persistence_failed" ? "message-store" : reason === "event_write_failed" ? "session-event-writer" : "runtime",
      code: reason === "crashed" ? "runtime_invalid_sequence" : "unavailable",
      retryable: true,
      fatal: false,
      ...(reason === "crashed" ? { reason: "runtime_contract_validation" } : {}),
    }),
    releaseSession: { reason },
  };
}

class RecordingRuntimeMetrics implements RuntimeMetricsSink {
  readonly hotStates: RuntimeHotStateMetrics[] = [];

  constructor(private readonly onHotState?: (snapshot: RuntimeHotStateMetrics) => void) {}

  recordHotState(snapshot: RuntimeHotStateMetrics): void {
    this.hotStates.push(snapshot);
    this.onHotState?.(snapshot);
  }

  addActiveToolFibers(): void {}

  addPendingApprovals(): void {}

  observeProviderStreamDuration(): void {}

  observeEventWriteLatency(): void {}

  observeContextLoadLatency(): void {}

  recordCleanupCommandOutcome(): void {}

  latestHotState(): RuntimeHotStateMetrics | undefined {
    return this.hotStates.at(-1);
  }
}

describe("SessionManager", () => {
  test("reviewer cancellation removes its queued input before idle admission can reenter", async () => {
    const sessionId = "sesn_reviewer_cancel_reentry";
    const reviewerThreadId = "thrd_reviewer_cancel_reentry";
    const parentThreadId = "thrd_parent_cancel_reentry";
    const agentLoop = makeReviewerGenerationAgentLoop();
    const targetBInput = approvalReviewInput(sessionId, "rin_target_b_reentry", reviewerThreadId, parentThreadId);
    let managerRef: TestSessionManager | undefined;
    let reentryArmed = false;
    let reentryResult: Promise<SessionManager.AcceptInputResult> | undefined;
    const metrics = new RecordingRuntimeMetrics((snapshot) => {
      if (!reentryArmed || snapshot.activeFibers !== 0 || managerRef === undefined) {
        return;
      }
      reentryArmed = false;
      reentryResult = Effect.runPromise(managerRef.acceptInput(targetBInput));
    });

    await withSessionManager(sessionManagerLayer(agentLoop, { metrics }), async (manager) => {
      managerRef = manager;
      const targetA = await Effect.runPromise(manager.acceptInput(
        approvalReviewInput(sessionId, "rin_target_a_reentry", reviewerThreadId, parentThreadId),
      ));
      if (!targetA.ok || targetA.reviewerExecutionToken === undefined) {
        throw new Error("target A did not receive a reviewer execution token");
      }
      await agentLoop.targetAStarted;
      const cancellation = Effect.runPromise(manager.interruptReviewerExecution(
        threadControl(sessionId, "rin_target_a_reentry_control", reviewerThreadId),
        targetA.reviewerExecutionToken,
      ));
      await agentLoop.cleanupStarted;

      reentryArmed = true;
      agentLoop.releaseCleanup();
      await cancellation;
      await waitForCondition(() => reentryResult !== undefined, "reviewer reentrant admission");
      expect(await reentryResult).toMatchObject({ ok: true, started: true });
      await agentLoop.targetBStarted;
      expect(agentLoop.observedReviewerInputIds).toEqual(["rin_target_a_reentry", "rin_target_b_reentry"]);
      agentLoop.releaseTargetB();
    });
  });

  test("correlates target A cancellation and target B review to distinct terminal run generations", async () => {
    const sessionId = "sesn_reviewer_generation";
    const reviewerThreadId = "thrd_reviewer_generation";
    const parentThreadId = "thrd_parent_generation";
    const agentLoop = makeReviewerGenerationAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const targetAInput = approvalReviewInput(sessionId, "rin_target_a", reviewerThreadId, parentThreadId);
      const targetA = await Effect.runPromise(manager.acceptInput(targetAInput));
      expect(targetA).toMatchObject({ ok: true, started: true });
      if (!targetA.ok || targetA.reviewerExecutionToken === undefined) {
        throw new Error("target A did not receive a reviewer execution token");
      }
      await agentLoop.targetAStarted;

      const targetAControl = threadControl(sessionId, "rin_target_a_control", reviewerThreadId);
      const cancellation = Effect.runPromise(manager.interruptReviewerExecution(
        targetAControl,
        targetA.reviewerExecutionToken,
      ));
      await agentLoop.cleanupStarted;

      const targetBWhileAIsLive = await Effect.runPromise(manager.acceptInput(
        approvalReviewInput(sessionId, "rin_target_b_early", reviewerThreadId, parentThreadId),
      ));
      expect(targetBWhileAIsLive).toEqual({
        ok: false,
        sessionId,
        reason: "thread_busy",
      });
      expect(await Effect.runPromise(manager.inspectReviewerExecution(
        targetAControl,
        targetA.reviewerExecutionToken,
      ))).toMatchObject({ ok: false, reason: "thread_busy" });

      agentLoop.releaseCleanup();
      expect(await cancellation).toMatchObject({ ok: true, terminal: true, applied: true });

      const targetBInput = approvalReviewInput(sessionId, "rin_target_b", reviewerThreadId, parentThreadId);
      const targetB = await Effect.runPromise(manager.acceptInput(targetBInput));
      expect(targetB).toMatchObject({ ok: true, started: true });
      if (!targetB.ok || targetB.reviewerExecutionToken === undefined) {
        throw new Error("target B did not receive a reviewer execution token");
      }
      expect(targetB.reviewerExecutionToken.runId).not.toBe(targetA.reviewerExecutionToken.runId);

      const targetBControl = threadControl(sessionId, "rin_target_b_control", reviewerThreadId);
      await agentLoop.targetBStarted;
      expect(agentLoop.observedReviewerInputIds).toEqual(["rin_target_a", "rin_target_b"]);
      expect(await Effect.runPromise(manager.waitReviewerExecution(
        targetBControl,
        targetA.reviewerExecutionToken,
        10,
      ))).toMatchObject({ ok: false, reason: "reviewer_execution_mismatch" });
      expect(await Effect.runPromise(manager.interruptReviewerExecution(
        targetBControl,
        targetA.reviewerExecutionToken,
      ))).toMatchObject({ ok: false, reason: "reviewer_execution_mismatch" });
      expect(await Effect.runPromise(manager.inspectReviewerExecution(
        targetBControl,
        targetB.reviewerExecutionToken,
      ))).toMatchObject({ ok: false, reason: "thread_busy" });
      agentLoop.releaseTargetB();
      expect(await Effect.runPromise(manager.waitReviewerExecution(
        targetBControl,
        targetB.reviewerExecutionToken,
        undefined,
      ))).toMatchObject({ ok: true, terminal: true, timedOut: false });
      const targetBSnapshot = await Effect.runPromise(manager.inspectReviewerExecution(
        targetBControl,
        targetB.reviewerExecutionToken,
      ));
      if (!targetBSnapshot.ok) {
        throw new Error("target B snapshot was unavailable");
      }
      expect(targetBSnapshot.messages.map((message) => message.id)).toEqual(["msg_target_a", "msg_target_b"]);
      expect(targetBSnapshot).toMatchObject({
        ok: true,
        observed: true,
      });
      expect(await Effect.runPromise(manager.inspectReviewerExecution(
        targetBControl,
        targetA.reviewerExecutionToken,
      ))).toMatchObject({ ok: false, reason: "reviewer_execution_mismatch" });
    });
  });

  test("approval reviewer threads reject completion mail without starting a run", async () => {
    const sessionId = "sesn_reviewer_mail_rejected";
    const reviewerThreadId = "thrd_reviewer_mail_rejected";
    const parentThreadId = "thrd_reviewer_mail_parent";
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl(sessionId, "rin_preload_reviewer_mail", reviewerThreadId),
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: coldCoverage(),
        messages: [],
        thread: {
          parentThreadId,
          role: "approval_reviewer",
          visibility: "internal",
          agentType: "approval_reviewer",
          status: "idle",
        },
      }))).toMatchObject({ ok: true, applied: true });

      expect(await Effect.runPromise(manager.acceptInput(agentMailInput(
        sessionId,
        "agent_mail:delivery_reviewer_mail",
        reviewerThreadId,
        "thrd_child_reviewer_mail",
        {
          parentThreadId,
          role: "approval_reviewer",
          visibility: "internal",
          agentType: "approval_reviewer",
          status: "idle",
        },
      )))).toEqual({
        ok: false,
        sessionId,
        reason: "thread_not_receivable",
      });
      expect(agentLoop.runs).toEqual([]);
    });
  });
  test("reports hot-state session, thread, and run fiber gauges through injected metrics", async () => {
    const agentLoop = makeControlledAgentLoop();
    const metrics = new RecordingRuntimeMetrics();
    await withSessionManager(sessionManagerLayer(agentLoop, { metrics }), async (manager) => {
      await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_1"));
      await waitForRuns(agentLoop, 1);
      expect(metrics.latestHotState()).toEqual({
        activeSessions: 1,
        activeThreads: 1,
        activeFibers: 1,
        pendingApprovals: 0,
      });

      agentLoop.runs[0]?.release();
      await waitForCondition(() => metrics.latestHotState()?.activeFibers === 0, "metrics active fiber release");
      expect(metrics.latestHotState()).toEqual({
        activeSessions: 1,
        activeThreads: 1,
        activeFibers: 0,
        pendingApprovals: 0,
      });
    });
  });

  test("accepted input creates one session and does not duplicate an in-flight AgentLoop", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const first = await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_1"));
      await waitForRuns(agentLoop, 1);
      const second = Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_1"));
      await new Promise((resolve) => setTimeout(resolve, 5));
      expect(first).toEqual({ ok: true, sessionId: "sesn_1", created: true, started: true });
      expect(agentLoop.runs.map((run) => run.sessionId)).toEqual(["sesn_1"]);
      expect(agentLoop.runs[0]?.args).toHaveLength(2);
      agentLoop.runs[0]?.release();
      expect(await second).toEqual({ ok: true, sessionId: "sesn_1", created: false, started: false });
    });
  });

  test("acceptInput marks one pending wake while running and starts exactly one follow-up run", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      await Effect.runPromise(
        Effect.gen(function* () {
        const first = yield* manager.acceptInput(acceptedInput("sesn_1", "rin_1"));
        const second = yield* manager.acceptInput(acceptedInput("sesn_1", "rin_2"));
        const third = yield* manager.acceptInput(acceptedInput("sesn_1", "rin_3"));
        expect(first).toEqual({ ok: true, sessionId: "sesn_1", created: true, started: true, pendingWake: false });
        expect(second).toEqual({ ok: true, sessionId: "sesn_1", created: false, started: false, pendingWake: true });
        expect(third).toEqual({ ok: true, sessionId: "sesn_1", created: false, started: false, pendingWake: true });
        }),
      );

      await waitForRuns(agentLoop, 1);
      agentLoop.runs[0]?.release();
      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs.map((run) => run.sessionId)).toEqual(["sesn_1", "sesn_1"]);
      expect(agentLoop.runs[1]?.session).toBe(agentLoop.runs[0]?.session);
      expect(agentLoop.runs[1]?.session.state.contextManager).toBe(agentLoop.runs[0]?.session.state.contextManager);
      agentLoop.runs[1]?.release();
    });
  });

  test("duplicate accepted input is idempotent and does not schedule an empty follow-up", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_duplicate")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForRuns(agentLoop, 1);
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_duplicate")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        started: false,
        pendingWake: false,
        duplicate: true,
      });
      agentLoop.runs[0]?.release();
      await new Promise((resolve) => setTimeout(resolve, 5));
      expect(agentLoop.runs).toHaveLength(1);
    });
  });

  test("cold recovery treats an input whose source event is already projected as a duplicate", async () => {
    const agentLoop = makeControlledAgentLoop();
    const input = acceptedInput("sesn_cold_input_replay", "rin_cold_input_replay");
    const durableMessage = {
      ...coldUserMessage(input.sessionId),
      owningEventId: input.eventIds[0]!,
      eventSequence: input.sequenceFrom,
    };
    await withSessionManager(sessionManagerLayer(agentLoop, {
      loadThreadContext: async () => ({
        ...threadControl(input.sessionId, input.runtimeInputId, input.sessionThreadId),
        runtimeBindingToken: "runtime-binding-token",
        messages: [durableMessage],
        coldCoverage: coldCoverage(),
      }),
    }), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(input))).toEqual({
        ok: true,
        sessionId: input.sessionId,
        created: true,
        started: false,
        pendingWake: false,
        duplicate: true,
      });
      expect(agentLoop.runs).toEqual([]);
    });
  });

  test("cold installation applies the triggering command before admitting its first run", async () => {
    const sessionId = "sesn_install_trigger_order";
    const threadId = "thrd_install_trigger_order";
    const trigger = acceptedInput(sessionId, "rin_install_trigger_order", threadId);
    const mail = agentMailInput(
      sessionId,
      "agent_mail:delivery_install_trigger_order",
      threadId,
      "thrd_install_trigger_child",
      { role: "main", visibility: "public", agentType: "general", status: "idle" },
    );
    const observedAcceptedInputCounts: number[] = [];
    const agentLoop = {
      layer: Layer.succeed(
        AgentLoop.Service,
        agentLoopService({
          run: (session) => Effect.sync(() => {
            observedAcceptedInputCounts.push(session.state.acceptedInputCount());
            return { type: "completed" as const, modelMessageCount: 0 };
          }),
        }),
      ),
    };
    await withSessionManager(sessionManagerLayer(agentLoop, {
      loadThreadContext: async () => ({
        ...threadControl(sessionId, trigger.runtimeInputId, threadId),
        runtimeBindingToken: "runtime-binding-token",
        messages: [],
        pendingAgentMail: [mail],
        coldCoverage: coldCoverage({
          undeliveredMailDeliveryIds: [mail.deliveryId],
        }),
      }),
    }), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(trigger))).toMatchObject({
        ok: true,
        sessionId,
        started: true,
      });
      await waitForCondition(() => observedAcceptedInputCounts.length > 0, "installed trigger run");
      expect(observedAcceptedInputCounts[0]).toBe(2);
    });
  });

  test("interruptControl cancels an active run and deduplicates a later idle interrupt", async () => {
    const agentLoop = makeInterruptRecordingAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1")))).toMatchObject({
        ok: true,
        sessionId: "sesn_1",
        started: true,
      });
      await waitForInterruptRecordingRuns(agentLoop, 1);
      const session = agentLoop.runs[0]?.session;
      if (session === undefined) {
        throw new Error("expected active session");
      }

      const activeInterrupt = { ...threadControl("sesn_1"), runtimeInputId: "rin_interrupt", sequenceTo: 9 };
      expect(await Effect.runPromise(manager.interruptControl(
        "sesn_1",
        activeInterrupt,
        testControlCommit(activeInterrupt),
      ))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        interrupted: true,
        idleInterrupt: false,
      });
      expect(agentLoop.interruptions).toEqual([
        {
          sessionId: "sesn_1",
          runtimeShutdownRequested: false,
        },
      ]);
      session.state.recordPendingApprovalToolJob({
        toolUseEventId: "sevt_idle_interrupt_tool",
        modelRequestId: "mreq_idle_interrupt",
        source: { providerId: "fake", modelId: "fake-chat" },
        assistantMessage: pendingApprovalAssistantMessage("sesn_1", "sevt_idle_interrupt_tool"),
        toolPart: pendingApprovalToolPart("sesn_1", "sevt_idle_interrupt_tool"),
        entry: {} as never,
        job: {
          id: "mreq_idle_interrupt:tool-1",
          modelOrder: 0,
          toolUseEventId: "sevt_idle_interrupt_tool",
          modelToolCallId: "tool-1",
          kind: "builtin",
          name: "Write",
          route: { kind: "gateway", operation: "RunWeb" },
          input: { file_path: "src/a.ts" },
          runPolicy: { mode: "parallel_safe", conflictKeys: null },
          gateState: "waiting_approval",
          approvalSource: "user",
        },
        committedMessages: [],
      });
      session.state.contextManager.appendMessage(
        pendingApprovalAssistantMessage("sesn_1", "sevt_idle_interrupt_tool"),
      );
      let idleInterruptCommits = 0;
      const idleInterrupt = { ...threadControl("sesn_1"), runtimeInputId: "rin_idle_interrupt", sequenceTo: 10 };
      expect(await Effect.runPromise(manager.interruptControl("sesn_1", idleInterrupt, async (declaration) => {
        idleInterruptCommits += 1;
        return buildRuntimeControlCommitResult(idleInterrupt, "interrupt_control", declaration);
      }))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        interrupted: false,
        idleInterrupt: true,
      });
      expect(idleInterruptCommits).toBe(1);
      expect(session.state.contextManager.messages()).toHaveLength(2);
      expect(session.state.contextManager.messages().at(-1)?.parts).toEqual([
        expect.objectContaining({
          type: "tool",
          toolUseEventId: "sevt_idle_interrupt_tool",
          state: expect.objectContaining({ status: "cancelled" }),
        }),
      ]);
      expect(session.state.pendingApprovalToolJobs()).toHaveLength(0);
      expect(await Effect.runPromise(manager.interruptControl(
        "sesn_1",
        idleInterrupt,
        async (declaration) => {
          idleInterruptCommits += 1;
          return buildRuntimeControlCommitResult(idleInterrupt, "interrupt_control", declaration);
        },
      ))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        interrupted: false,
        idleInterrupt: true,
        duplicate: true,
      });
      expect(idleInterruptCommits).toBe(1);
      expect(session.state.contextManager.messages()).toHaveLength(2);
      expect(agentLoop.runs).toHaveLength(1);
      expect(agentLoop.interruptions).toHaveLength(1);
    });
  });

  test("concurrent idle interrupt replay commits one frozen declaration", async () => {
    const agentLoop = makeInterruptRecordingAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const sessionId = "sesn_concurrent_idle_interrupt";
      const threadId = "thrd_concurrent_idle_interrupt";
      const preload = threadControl(sessionId, "rin_preload_concurrent_idle_interrupt", threadId);
      expect(await Effect.runPromise(manager.preloadThread({
        ...preload,
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: coldCoverage(),
        messages: [],
      }))).toMatchObject({ ok: true, applied: true });

      let releaseFirstCommit: (() => void) | undefined;
      const firstCommitGate = new Promise<void>((resolve) => {
        releaseFirstCommit = resolve;
      });
      let firstCommitStarted: (() => void) | undefined;
      const firstCommitStart = new Promise<void>((resolve) => {
        firstCommitStarted = resolve;
      });
      let commitCount = 0;
      const interrupt = {
        ...threadControl(sessionId, "rin_concurrent_idle_interrupt", threadId),
        sequenceTo: 11,
      };
      const commit = async (declaration: RuntimeControlInputDeclaration) => {
        commitCount += 1;
        firstCommitStarted?.();
        await firstCommitGate;
        return buildRuntimeControlCommitResult(interrupt, "interrupt_control", declaration);
      };

      const first = Effect.runPromise(manager.interruptControl(sessionId, interrupt, commit));
      await firstCommitStart;
      const second = Effect.runPromise(manager.interruptControl(sessionId, interrupt, commit));
      releaseFirstCommit?.();

      expect(await Promise.all([first, second])).toEqual([
        {
          ok: true,
          sessionId,
          created: false,
          interrupted: false,
          idleInterrupt: true,
        },
        {
          ok: true,
          sessionId,
          created: false,
          interrupted: false,
          idleInterrupt: true,
          duplicate: true,
        },
      ]);
      expect(commitCount).toBe(1);
    });
  });

  test("distinct active interrupts settle in order across the first run closeout", async () => {
    const agentLoop = makeInterruptRecordingAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const sessionId = "sesn_ordered_active_interrupts";
      const threadId = "thrd_ordered_active_interrupts";
      await Effect.runPromise(manager.acceptInput(acceptedInput(sessionId, "rin_active_owner", threadId)));
      await waitForInterruptRecordingRuns(agentLoop, 1);
      agentLoop.runs[0]?.session.state.acknowledgeAcceptedInput("rin_active_owner");

      let firstCommitStarted: (() => void) | undefined;
      const firstCommitStart = new Promise<void>((resolve) => {
        firstCommitStarted = resolve;
      });
      let releaseFirstCommit: (() => void) | undefined;
      const firstCommitGate = new Promise<void>((resolve) => {
        releaseFirstCommit = resolve;
      });
      const first = {
        ...threadControl(sessionId, "rin_interrupt_first", threadId),
        sequenceFrom: 9,
        sequenceTo: 9,
      };
      const second = {
        ...threadControl(sessionId, "rin_interrupt_second", threadId),
        sequenceFrom: 10,
        sequenceTo: 10,
      };
      const committed: string[] = [];
      const firstResult = Effect.runPromise(manager.interruptControl(sessionId, first, async (declaration) => {
        committed.push(first.runtimeInputId);
        firstCommitStarted?.();
        await firstCommitGate;
        return buildRuntimeControlCommitResult(first, "interrupt_control", declaration);
      }));
      await firstCommitStart;
      let secondSettled = false;
      const secondResult = Effect.runPromise(manager.interruptControl(sessionId, second, async (declaration) => {
        committed.push(second.runtimeInputId);
        return buildRuntimeControlCommitResult(second, "interrupt_control", declaration);
      })).then((result) => {
        secondSettled = true;
        return result;
      });
      await Promise.resolve();
      expect(secondSettled).toBe(false);

      releaseFirstCommit?.();
      expect(await firstResult).toMatchObject({ ok: true, interrupted: true });
      expect(agentLoop.runs).toHaveLength(1);
      const settledSecond = await secondResult;
      expect(settledSecond).toMatchObject({ ok: true, idleInterrupt: true });
      expect(committed).toEqual([first.runtimeInputId, second.runtimeInputId]);
    });
  });

  test("retryable idle interrupt replay retries the frozen declaration", async () => {
    const agentLoop = makeInterruptRecordingAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const sessionId = "sesn_retryable_idle_interrupt";
      const threadId = "thrd_retryable_idle_interrupt";
      const preload = threadControl(sessionId, "rin_preload_retryable_idle_interrupt", threadId);
      expect(await Effect.runPromise(manager.preloadThread({
        ...preload,
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: coldCoverage(),
        messages: [],
      }))).toMatchObject({ ok: true, applied: true });

      const interrupt = {
        ...threadControl(sessionId, "rin_retryable_idle_interrupt", threadId),
        sequenceTo: 11,
      };
      const declarations: RuntimeControlInputDeclaration[] = [];
      const commit = async (declaration: RuntimeControlInputDeclaration) => {
        declarations.push(declaration);
        if (declarations.length === 1) {
          return {
            ok: false,
            retryable: true,
            errorCode: "bridge_token_unavailable",
          } as const;
        }
        return buildRuntimeControlCommitResult(interrupt, "interrupt_control", declaration);
      };

      expect(await Effect.runPromise(manager.interruptControl(sessionId, interrupt, commit))).toEqual({
        ok: false,
        sessionId,
        reason: "context_load_failed",
      });
      expect(await Effect.runPromise(manager.interruptControl(sessionId, interrupt, commit))).toEqual({
        ok: true,
        sessionId,
        created: false,
        interrupted: false,
        idleInterrupt: true,
      });
      expect(declarations).toHaveLength(2);
      expect(declarations[1]).toEqual(declarations[0]);
    });
  });

  test("idle config commands update shared hot state without starting AgentLoop work", async () => {
    const agentLoop = makeControlledAgentLoop();

    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl("sesn_1", "rin_preload_config"),
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: coldCoverage(),
        messages: [],
      }))).toMatchObject({ ok: true, applied: true });
      expect(
        await Effect.runPromise(
          manager.applyRuntimeConfigPatch("sesn_1", { ...threadControl("sesn_1"),
            runtimeInputId: "rin_config",
            generation: 5,
            payloadJson: "{\"config_generation\":5}",
          }),
        ),
      ).toEqual({ ok: true, sessionId: "sesn_1", created: false, applied: true });
      expect(agentLoop.runs).toEqual([]);

      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_1"))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        started: true,
      });
      await waitForRuns(agentLoop, 1);
      const session = agentLoop.runs[0]?.session;
      expect(session?.configuration.runtimeConfigPatch()).toEqual({
        generation: 5,
        payloadJson: "{\"config_generation\":5}",
      });
      agentLoop.runs[0]?.release();
    });
  });

  test("task notification committed during a run schedules one follow-up turn", async () => {
    const agentLoop = makeControlledAgentLoop();
    const sessionId = "sesn_task_notification_wake";
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput(sessionId)))).toMatchObject({
        ok: true,
        started: true,
      });
      await waitForRuns(agentLoop, 1);
      expect(await Effect.runPromise(manager.commitTaskNotification(sessionId, {
        ...threadControl(sessionId, "rin_task_notification_wake"),
        taskId: "task_notification_wake",
        sourceToolUseEventId: "sevt_task_notification_wake",
        status: "completed",
        payloadJson: "{\"status\":\"completed\"}",
      }, async () => ({
        ok: true,
        committedMessage: bridgeRuntimeMessage(sessionId, "task completed"),
      })))).toMatchObject({ ok: true, applied: true });

      agentLoop.runs[0]!.release();
      await waitForRuns(agentLoop, 2);
      agentLoop.runs[1]!.release();
    });
  });

  test("one failed shared-state initializer prevents sibling cold loads from creating a second session state", async () => {
    const agentLoop = makeControlledAgentLoop();
    let loadCount = 0;
    let releaseFailedLoad: (() => void) | undefined;
    let observeFailedLoad: (() => void) | undefined;
    let releaseSiblingLoad: (() => void) | undefined;
    let observeSiblingLoad: (() => void) | undefined;
    const failedLoadStarted = new Promise<void>((resolve) => {
      observeFailedLoad = resolve;
    });
    const failedLoadGate = new Promise<void>((resolve) => {
      releaseFailedLoad = resolve;
    });
    const siblingLoadStarted = new Promise<void>((resolve) => {
      observeSiblingLoad = resolve;
    });
    const siblingLoadGate = new Promise<void>((resolve) => {
      releaseSiblingLoad = resolve;
    });
    const loadThreadContext: NonNullable<SessionManager.LayerOptions["loadThreadContext"]> = async (command) => {
      loadCount += 1;
      if (loadCount === 1) {
        observeFailedLoad?.();
        await failedLoadGate;
        throw new Error("shared cold load failed");
      }
      if (loadCount === 2) {
        observeSiblingLoad?.();
        await siblingLoadGate;
      }
      return {
        ...command,
        runtimeBindingToken: `runtime-binding-token-${command.sessionThreadId}`,
        coldCoverage: coldCoverage(),
        messages: [],
        thread: command.sessionThreadId === "thrd_shared_initializer"
          ? { role: "main", visibility: "public", status: "idle" }
          : {
            parentThreadId: "thrd_shared_initializer",
            role: "subagent",
            visibility: "public",
            status: "idle",
          },
      };
    };

    await withSessionManager(sessionManagerLayer(agentLoop, { loadThreadContext }), async (manager) => {
      const initializer = Effect.runPromise(manager.ensureThreadInstalled(
        threadControl("sesn_shared_install_failure", "rin_initializer", "thrd_shared_initializer"),
      ));
      await failedLoadStarted;
      const sibling = Effect.runPromise(manager.ensureThreadInstalled(
        threadControl("sesn_shared_install_failure", "rin_sibling", "thrd_shared_sibling"),
      ));
      await siblingLoadStarted;
      releaseFailedLoad?.();

      await expect(initializer).resolves.toEqual({
        ok: false,
        sessionId: "sesn_shared_install_failure",
        sessionThreadId: "thrd_shared_initializer",
        reason: "context_load_failed",
      });
      await expect(Effect.runPromise(manager.ensureThreadInstalled(
        threadControl("sesn_shared_install_failure", "rin_early_retry", "thrd_shared_initializer"),
      ))).resolves.toEqual({
        ok: false,
        sessionId: "sesn_shared_install_failure",
        sessionThreadId: "thrd_shared_initializer",
        reason: "context_load_failed",
      });
      expect(loadCount).toBe(2);

      releaseSiblingLoad?.();
      await expect(sibling).resolves.toEqual({
        ok: false,
        sessionId: "sesn_shared_install_failure",
        sessionThreadId: "thrd_shared_sibling",
        reason: "context_load_failed",
      });
      expect(await Effect.runPromise(manager.inspectThread(
        threadControl("sesn_shared_install_failure", "rin_inspect", "thrd_shared_sibling"),
      ))).toEqual({
        ok: true,
        sessionId: "sesn_shared_install_failure",
        sessionThreadId: "thrd_shared_sibling",
        observed: false,
        messages: [],
      });

      await expect(Effect.runPromise(manager.ensureThreadInstalled(
        threadControl("sesn_shared_install_failure", "rin_retry", "thrd_shared_initializer"),
      ))).resolves.toEqual({
        ok: true,
        sessionId: "sesn_shared_install_failure",
        sessionThreadId: "thrd_shared_initializer",
        applied: true,
      });
      expect(loadCount).toBe(3);
    });
  });

  test("existing child thread keeps parent and role metadata across accepted input", async () => {
    const agentLoop = makeControlledAgentLoop();

    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(
        await Effect.runPromise(
          manager.preloadThread({
            ...threadControl("sesn_child", "rin_preload_child", "thrd_child"),
            thread: {
              parentThreadId: "thrd_parent",
              role: "subagent",
              visibility: "internal",
              status: "idle",
            },
            runtimeBindingToken: "runtime-binding-token-child",
            coldCoverage: coldCoverage(),
            messages: [],
          }),
        ),
      ).toEqual({ ok: true, sessionId: "sesn_child", sessionThreadId: "thrd_child", applied: true });

      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_child", "rin_child_follow", "thrd_child")))).toEqual({
        ok: true,
        sessionId: "sesn_child",
        created: false,
        started: true,
        pendingWake: false,
      });
      await waitForRuns(agentLoop, 1);

      expect(agentLoop.runs[0]?.session.identity).toMatchObject({
        parentThreadId: "thrd_parent",
        threadRole: "subagent",
        runtimeBindingToken: "runtime-binding-token-child",
      });
      agentLoop.runs[0]?.release();
    });
  });

  test("preloadThread restores running background task handles before task notification settlement", async () => {
    const agentLoop = makeControlledAgentLoop();
    const committedMessage = bridgeRuntimeMessage("sesn_cold_background", "background task completed after cold load");

    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(
        await Effect.runPromise(
          manager.preloadThread({
            ...threadControl("sesn_cold_background", "rin_preload_background"),
            runtimeBindingToken: "runtime-binding-token-background",
            coldCoverage: coldCoverage(),
            messages: [],
            backgroundTools: [{
              taskId: "task_cold_background",
              sourceToolUseEventId: "sevt_tool_background",
            }],
          }),
        ),
      ).toEqual({ ok: true, sessionId: "sesn_cold_background", sessionThreadId: "thrd_sesn_cold_background", applied: true });

      expect(
        await Effect.runPromise(
          manager.commitTaskNotification("sesn_cold_background", {
            ...threadControl("sesn_cold_background", "rin_task_background"),
            taskId: "task_cold_background",
            sourceToolUseEventId: "sevt_tool_background",
            status: "completed",
            payloadJson: "{\"task_id\":\"task_cold_background\",\"source_tool_use_event_id\":\"sevt_tool_background\",\"status\":\"completed\"}",
          }, async () => ({ ok: true, committedMessage })),
        ),
      ).toEqual({ ok: true, sessionId: "sesn_cold_background", created: false, applied: true });

      await waitForRuns(agentLoop, 1);
      expect(agentLoop.runs[0]?.session.state.backgroundTool("task_cold_background")).toMatchObject({
        taskId: "task_cold_background",
        sourceToolUseEventId: "sevt_tool_background",
        status: "terminal",
        terminalNotification: expect.objectContaining({ runtimeInputId: "rin_task_background", status: "completed" }),
      });
      agentLoop.runs[0]?.release();
    });
  });

  test("preload admits stored completion mail through the waking path and leaves an empty preload idle", async () => {
    const base = makeControlledAgentLoop();
    const layer = sessionManagerLayer(base);
    const sessionID = "sesn_preload_agent_mail";
    const threadID = "thrd_preload_agent_mail_main";
    const thread: RuntimeAcceptedThreadMetadataState = {
      role: "main",
      visibility: "public",
      agentType: "general",
      status: "idle",
    };
    const mail = agentMailInput(
      sessionID,
      "agent_mail:delivery_preload_agent_mail",
      threadID,
      "thrd_preload_agent_mail_child",
      thread,
    );

    await withSessionManager(layer, async (manager) => {
      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl(sessionID, "rin_preload_agent_mail", threadID),
        runtimeBindingToken: "runtime-binding-token",
        messages: [],
        thread,
        pendingAgentMail: [mail],
        coldCoverage: coldCoverage({
          undeliveredMailDeliveryIds: [mail.deliveryId],
        }),
      }))).toMatchObject({ ok: true, applied: true });
      await waitForRuns(base, 1);
      expect(await Effect.runPromise(manager.inspectThread(
        threadControl(sessionID, "rin_inspect_agent_mail", threadID),
      ))).toMatchObject({ observed: true, status: "running" });
      base.runs[0]?.release({ type: "completed", modelMessageCount: 1 });

      const emptySessionID = "sesn_preload_agent_mail_empty";
      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl(emptySessionID, "rin_preload_agent_mail_empty", "thrd_preload_agent_mail_empty"),
        runtimeBindingToken: "runtime-binding-token",
        messages: [],
        thread,
        pendingAgentMail: [],
        coldCoverage: coldCoverage(),
      }))).toMatchObject({ ok: true, applied: true });
      expect(base.runs).toHaveLength(1);
      expect(await Effect.runPromise(manager.inspectThread(
        threadControl(emptySessionID, "rin_inspect_agent_mail_empty", "thrd_preload_agent_mail_empty"),
      ))).toMatchObject({ observed: true, status: "idle" });
    });
  });

  test("cold preload drains four completion mails as four turns without a stranded wake", async () => {
    const agentLoop = makeControlledAgentLoop();
    const sessionID = "sesn_preload_agent_mail_page";
    const threadID = "thrd_preload_agent_mail_page_main";
    const thread: RuntimeAcceptedThreadMetadataState = {
      role: "main",
      visibility: "public",
      agentType: "general",
      status: "idle",
    };
    const mails = [1, 2, 3, 4].map((index) => agentMailInput(
      sessionID,
      `agent_mail:delivery_preload_agent_mail_page_${index}`,
      threadID,
      `thrd_preload_agent_mail_page_child_${index}`,
      thread,
    ));
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl(sessionID, "rin_preload_agent_mail_page", threadID),
        runtimeBindingToken: "runtime-binding-token",
        messages: [],
        thread,
        pendingAgentMail: mails,
        coldCoverage: coldCoverage({
          undeliveredMailDeliveryIds: mails.map((mail) => mail.deliveryId),
        }),
      }))).toMatchObject({ ok: true, applied: true });

      for (let index = 0; index < mails.length; index += 1) {
        await waitForRuns(agentLoop, index + 1);
        expect(agentLoop.runs[index]!.session.state.peekAcceptedInput()?.runtimeInputId).toBe(
          mails[index]!.runtimeInputId,
        );
        agentLoop.runs[index]!.release({ type: "completed", modelMessageCount: 1 });
      }
      await waitForThreadIdle(manager, sessionID, threadID);
      expect(agentLoop.runs).toHaveLength(mails.length);
      expect(await Effect.runPromise(manager.acceptInput(mails[3]!))).toMatchObject({
        ok: true,
        started: false,
        pendingWake: false,
      });
    });
  });

  test("completion mail admitted to a busy resident thread is a pending-wake success", async () => {
    const agentLoop = makeControlledAgentLoop();
    const layer = sessionManagerLayer(agentLoop);
    const sessionID = "sesn_busy_agent_mail";
    const threadID = "thrd_busy_agent_mail_main";
    const thread: RuntimeAcceptedThreadMetadataState = {
      role: "main",
      visibility: "public",
      agentType: "general",
      status: "idle",
    };
    await withSessionManager(layer, async (manager) => {
      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl(sessionID, "rin_preload_busy_agent_mail", threadID),
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: coldCoverage(),
        messages: [],
        thread,
      }))).toMatchObject({ ok: true, applied: true });
      expect(await Effect.runPromise(manager.acceptInput(
        acceptedInput(sessionID, "rin_busy_agent_mail_first", threadID),
      ))).toMatchObject({ ok: true, started: true });
      await waitForRuns(agentLoop, 1);
      const mail = agentMailInput(
        sessionID,
        "agent_mail:delivery_busy_agent_mail",
        threadID,
        "thrd_busy_agent_mail_child",
        thread,
      );
      expect(await Effect.runPromise(manager.acceptInput(mail))).toEqual({
        ok: true,
        sessionId: sessionID,
        created: false,
        started: false,
        pendingWake: true,
      });
      agentLoop.runs[0]?.release({ type: "completed", modelMessageCount: 1 });
      await waitForRuns(agentLoop, 2);
      agentLoop.runs[1]?.release({ type: "completed", modelMessageCount: 1 });
    });
  });

  test("pulling a queued completion mail cancels its pending wake instead of starting an empty run", async () => {
    const agentLoop = makeInputConsumingControlledAgentLoop();
    const layer = sessionManagerLayer(agentLoop);
    const sessionID = "sesn_pulled_agent_mail_wake";
    const threadID = "thrd_pulled_agent_mail_wake_main";
    const thread: RuntimeAcceptedThreadMetadataState = {
      role: "main",
      visibility: "public",
      agentType: "general",
      status: "idle",
    };
    await withSessionManager(layer, async (manager) => {
      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl(sessionID, "rin_preload_pulled_agent_mail_wake", threadID),
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: coldCoverage(),
        messages: [],
        thread,
      }))).toMatchObject({ ok: true, applied: true });
      expect(await Effect.runPromise(manager.acceptInput(
        acceptedInput(sessionID, "rin_pulled_agent_mail_wake_first", threadID),
      ))).toMatchObject({ ok: true, started: true });
      await waitForRuns(agentLoop, 1);

      const mail = agentMailInput(
        sessionID,
        "agent_mail:delivery_pulled_agent_mail_wake",
        threadID,
        "thrd_pulled_agent_mail_wake_child",
        thread,
      );
      expect(await Effect.runPromise(manager.acceptInput(mail))).toMatchObject({
        ok: true,
        pendingWake: true,
      });
      expect(await Effect.runPromise(manager.markAgentMailPulled(
        threadControl(sessionID, "rin_pull_agent_mail_wake", threadID),
        mail.deliveryId,
      ))).toMatchObject({ ok: true, applied: true });

      agentLoop.runs[0]?.release({ type: "completed", modelMessageCount: 1 });
      expect(await Effect.runPromise(manager.waitThread(
        threadControl(sessionID, "rin_wait_pulled_agent_mail_wake", threadID),
        undefined,
      ))).toMatchObject({ ok: true, observed: true, timedOut: false });
      expect(agentLoop.runs).toHaveLength(1);
    });
  });

  test("cold preload installs both pending attachment origins before the next run", async () => {
    const agentLoop = makeControlledAgentLoop();
    const pendingAttachments = [{
      transient: {
        attachmentRef: "att_cold",
        sourceToolUseEventId: "sevt_tool_cold",
        sourcePath: "mcp:github/chart.png",
        pageRange: "",
        detail: "auto",
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: "chart.png",
    }, {
      transient: undefined,
      fileBacked: {
        sourceEventId: "sevt_user_cold",
        fileId: "file_cold",
      },
      mime: "application/pdf",
      filename: "brief.pdf",
    }];

    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl("sesn_cold_attachments", "rin_preload_attachments"),
        runtimeBindingToken: "runtime-binding-token-attachments",
        messages: [],
        pendingAttachments,
        coldCoverage: coldCoverage({
          pendingAttachmentIdentities: [
            "transient:sevt_tool_cold:att_cold",
            "file:sevt_user_cold:file_cold",
          ],
        }),
      }))).toEqual({
        ok: true,
        sessionId: "sesn_cold_attachments",
        sessionThreadId: "thrd_sesn_cold_attachments",
        applied: true,
      });

      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_cold_attachments"))).toMatchObject({
        ok: true,
        sessionId: "sesn_cold_attachments",
        created: false,
        started: true,
      });
      await waitForRuns(agentLoop, 1);
      expect(agentLoop.runs[0]?.session.state.pendingAttachments()).toEqual(pendingAttachments);
      agentLoop.runs[0]?.release();
    });
  });

  test("cold preload installs generation-fenced MCP manifests before pending MCP approval recovery", async () => {
    const manifestsObservedDuringRecovery: Array<ReturnType<Session.Session["configuration"]["patches"]>> = [];
    const agentLoop = makeControlledAgentLoop({
      installLoadedPendingToolUses: (session) => Effect.sync(() => {
        manifestsObservedDuringRecovery.push([...session.configuration.patches()]);
        return { ok: true as const };
      }),
    });
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const control = threadControl("sesn_mcp_cold_pending", "rin_mcp_cold_pending");
      const result = await Effect.runPromise(manager.preloadThread({
        ...control,
        runtimeBindingToken: "runtime-binding-token-mcp-cold",
        messages: [coldUserMessage("sesn_mcp_cold_pending")],
        runtimeConfigPatch: {
          ...control,
          generation: 5,
          coldLoad: true,
          installedBuiltinFamily: "claude",
          payloadJson: JSON.stringify({
            config_generation: 5,
            runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] },
            tool_policy: { mcpToolsets: [{ mcpServerName: "github" }] },
          }),
        },
        mcpManifests: [{
          ...control,
          runtimeInputId: "runtime_config_update:mcp_manifest:sesn_mcp_cold_pending:github:7",
          generation: 7,
          mcpServerName: "github",
          manifestETag: "etag_7",
          payloadJson: JSON.stringify({
            mcp_manifest: {
              mcp_server_name: "github",
              manifest_etag: "etag_7",
              manifest_generation: 7,
              tools: [
                { name: "Read", description: "must collide again at Runtime", input_schema: { type: "object" } },
                { name: "github_search", description: "Search GitHub", input_schema: { type: "object" } },
              ],
            },
          }),
        }],
        pendingToolUses: [{
          toolUseEventId: "sevt_mcp_pending",
          modelRequestId: "mrq_mcp_pending",
          modelToolCallId: "toolu_mcp_pending",
          toolName: "github_search",
          kind: "approval",
          input: { query: "tetral" },
          status: "pending",
          expiresAt: "2026-06-14T00:30:00.000Z",
        }],
        coldCoverage: coldCoverage({
          pendingToolIds: ["sevt_mcp_pending"],
        }),
      }));
      expect(result).toEqual({
        ok: true,
        sessionId: "sesn_mcp_cold_pending",
        sessionThreadId: "thrd_sesn_mcp_cold_pending",
        applied: true,
      });

      expect(manifestsObservedDuringRecovery).toHaveLength(1);
      expect(manifestsObservedDuringRecovery[0]).toContainEqual(expect.objectContaining({
        generation: 7,
        mcpServerName: "github",
        manifestETag: "etag_7",
      }));
    });
  });

  test("cold preload seeds the runtime model before pending tool use recovery", async () => {
    const modelsObservedDuringRecovery: unknown[] = [];
    const agentLoop = makeControlledAgentLoop({
      seedRuntimeModel: (session) => {
        session.state.updateCurrentModel({ providerId: "seeded", modelId: "before-install" });
      },
      installLoadedPendingToolUses: (session) => Effect.sync(() => {
        modelsObservedDuringRecovery.push(session.state.currentModel());
        return { ok: true as const };
      }),
    });
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const control = threadControl("sesn_seed_before_install", "rin_seed_before_install");
      const result = await Effect.runPromise(manager.preloadThread({
        ...control,
        runtimeBindingToken: "runtime-binding-token-seed-order",
        messages: [coldUserMessage("sesn_seed_before_install")],
        runtimeConfigPatch: {
          ...control,
          generation: 3,
          coldLoad: true,
          installedBuiltinFamily: "claude",
          payloadJson: JSON.stringify({
            config_generation: 3,
            runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] },
          }),
        },
        pendingToolUses: [{
          toolUseEventId: "sevt_seed_pending",
          modelRequestId: "mrq_seed_pending",
          modelToolCallId: "toolu_seed_pending",
          toolName: "Read",
          kind: "approval",
          input: { path: "README.md" },
          status: "pending",
          expiresAt: "2026-06-14T00:30:00.000Z",
        }],
        coldCoverage: coldCoverage({
          pendingToolIds: ["sevt_seed_pending"],
        }),
      }));
      expect(result).toEqual({
        ok: true,
        sessionId: "sesn_seed_before_install",
        sessionThreadId: "thrd_sesn_seed_before_install",
        applied: true,
      });
      expect(modelsObservedDuringRecovery).toEqual([{ providerId: "seeded", modelId: "before-install" }]);
    });
  });

  test("tool confirmation wakes a thread when a ToolJob is pending approval", async () => {
    const agentLoop = makeControlledAgentLoop();

    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_1"))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
      });
      await waitForRuns(agentLoop, 1);
      const session = agentLoop.runs[0]?.session;
      expect(session).toBeDefined();
      session?.state.recordPendingApprovalToolJob({
        toolUseEventId: "sevt_tool_1",
        modelRequestId: "mreq_1",
        source: { providerId: "fake", modelId: "fake-chat" },
        assistantMessage: pendingApprovalAssistantMessage("sesn_1", "sevt_tool_1"),
        toolPart: pendingApprovalToolPart("sesn_1", "sevt_tool_1"),
        entry: {} as never,
        job: {
          id: "mreq_1:tool-1",
          modelOrder: 0,
          toolUseEventId: "sevt_tool_1",
          modelToolCallId: "tool-1",
          kind: "builtin",
          name: "Write",
          route: { kind: "gateway", operation: "RunWeb" },
          input: { file_path: "src/a.ts" },
          runPolicy: { mode: "parallel_safe", conflictKeys: null },
          gateState: "waiting_approval",
          approvalSource: "user",
        },
        committedMessages: [],
      });

      const confirmationCommand = {
        ...threadControl("sesn_1"),
        runtimeInputId: "rin_confirm",
        sourceEventId: "sevt_confirm_1",
        eventIds: ["sevt_confirm_1"],
        toolUseEventId: "sevt_tool_1",
        decision: "allow" as const,
      };
      let confirmationCommits = 0;
      const commitConfirmation = async (declaration: RuntimeControlInputDeclaration) => {
        confirmationCommits += 1;
        return testControlCommit(confirmationCommand, "tool_confirmation")(declaration);
      };
      expect(
        await Effect.runPromise(
          manager.resolveToolConfirmation(
            "sesn_1",
            confirmationCommand,
            commitConfirmation,
          ),
        ),
      ).toEqual({ ok: true, sessionId: "sesn_1", created: false, applied: true });
      expect(session?.state.contextManager.messages().at(-1)?.parts).toEqual([
        expect.objectContaining({ type: "text", text: "Approval allowed" }),
      ]);
      expect(agentLoop.runs).toHaveLength(1);
      const messagesAfterConfirmation = session?.state.contextManager.messages().length;
      expect(
        await Effect.runPromise(
          manager.resolveToolConfirmation(
            "sesn_1",
            confirmationCommand,
            commitConfirmation,
          ),
        ),
      ).toEqual({ ok: true, sessionId: "sesn_1", created: false, applied: false });
      expect(confirmationCommits).toBe(1);
      expect(session?.state.contextManager.messages()).toHaveLength(messagesAfterConfirmation ?? 0);

      agentLoop.runs[0]?.release();
      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs[1]?.session).toBe(session);
      agentLoop.runs[1]?.release();
    });
  });

  test("stale control receipts discard the addressed hot thread without applying their payload", async () => {
    const agentLoop = makeControlledAgentLoop();
    const sessionId = "sesn_stale_control_receipt";
    const scope = threadControl(sessionId, "rin_stale_control_receipt");

    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput(sessionId))).toMatchObject({
        ok: true,
        started: true,
      });
      await waitForRuns(agentLoop, 1);
      const session = agentLoop.runs[0]!.session;
      session.state.recordPendingApprovalToolJob({
        toolUseEventId: "sevt_stale_control_tool",
        modelRequestId: "mreq_stale_control",
        source: { providerId: "fake", modelId: "fake-chat" },
        assistantMessage: pendingApprovalAssistantMessage(sessionId, "sevt_stale_control_tool"),
        toolPart: pendingApprovalToolPart(sessionId, "sevt_stale_control_tool"),
        entry: {} as never,
        job: {
          id: "mreq_stale_control:tool-1",
          modelOrder: 0,
          toolUseEventId: "sevt_stale_control_tool",
          modelToolCallId: "tool-1",
          kind: "builtin",
          name: "Write",
          route: { kind: "gateway", operation: "RunWeb" },
          input: { file_path: "src/a.ts" },
          runPolicy: { mode: "parallel_safe", conflictKeys: null },
          gateState: "waiting_approval",
          approvalSource: "user",
        },
        committedMessages: [],
      });

      expect(await Effect.runPromise(manager.resolveToolConfirmation(sessionId, {
        ...scope,
        sourceEventId: scope.eventIds[0]!,
        toolUseEventId: "sevt_stale_control_tool",
        decision: "allow",
      }, async () => ({ ok: true, stale: true })))).toEqual({
        ok: true,
        sessionId,
        created: false,
        applied: false,
        stale: true,
      });
      expect(session.state.pendingApprovalToolJobs()).toHaveLength(0);
      expect(await Effect.runPromise(manager.inspectThread(scope))).toMatchObject({
        ok: true,
        observed: false,
      });
    });
  });

  test("a stale task-notification receipt removes its idle cold residency", async () => {
    const agentLoop = makeControlledAgentLoop();
    const sessionId = "sesn_stale_task_receipt";
    const threadId = "thrd_stale_task_receipt";
    const scope = threadControl(sessionId, "rin_stale_task_receipt", threadId);

    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.preloadThread({
        ...scope,
        runtimeBindingToken: "runtime-binding-token",
        messages: [],
        backgroundTools: [{
          taskId: "task_stale_receipt",
          sourceToolUseEventId: "sevt_stale_receipt",
        }],
        coldCoverage: coldCoverage(),
      }))).toMatchObject({ ok: true, applied: true });

      expect(await Effect.runPromise(manager.commitTaskNotification(sessionId, {
        ...scope,
        taskId: "task_stale_receipt",
        sourceToolUseEventId: "sevt_stale_receipt",
        status: "completed",
        payloadJson: "{\"status\":\"completed\"}",
      }, async () => ({ ok: true, stale: true })))).toMatchObject({
        ok: true,
        applied: false,
        stale: true,
      });
      expect(await Effect.runPromise(manager.inspectThread(scope))).toMatchObject({
        ok: true,
        observed: false,
      });
    });
  });

  test("runtime config patch is installed only while the target thread is idle", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(
        await Effect.runPromise(
          manager.applyRuntimeConfigPatch("sesn_cold", {
            ...threadControl("sesn_cold", "rin_config_cold"),
            generation: 6,
            payloadJson: "{\"config_generation\":6}",
          }),
        ),
      ).toEqual({ ok: true, sessionId: "sesn_cold", created: false, applied: false, noResidency: true });
      expect(agentLoop.runs).toEqual([]);

      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_second_session_1")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForRuns(agentLoop, 1);

      expect(
        await Effect.runPromise(
          manager.applyRuntimeConfigPatch("sesn_1", {
            ...threadControl("sesn_1", "rin_config_busy"),
            generation: 6,
            payloadJson: "{\"config_generation\":6}",
          }),
        ),
      ).toEqual({ ok: false, sessionId: "sesn_1", reason: "control_busy" });

      agentLoop.runs[0]?.release();
      let idlePatch: SessionManager.RuntimeControlResult | undefined;
      for (let attempt = 0; attempt < 100 && idlePatch === undefined; attempt += 1) {
        const result = await Effect.runPromise(
          manager.applyRuntimeConfigPatch("sesn_1", {
            ...threadControl("sesn_1", "rin_config_idle"),
            generation: 6,
            payloadJson: "{\"config_generation\":6}",
          }),
        );
        if (result.ok) {
          idlePatch = result;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      expect(idlePatch).toEqual({ ok: true, sessionId: "sesn_1", created: false, applied: true });
    });
  });

  test("runtime config patch updates every idle resident thread before ACK", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      for (const sessionThreadId of ["thrd_main", "thrd_child"]) {
        expect(await Effect.runPromise(manager.preloadThread({
          ...threadControl("sesn_config_all", `rin_preload_${sessionThreadId}`, sessionThreadId),
          runtimeBindingToken: "runtime-binding-token",
          coldCoverage: coldCoverage(),
          messages: [],
        }))).toMatchObject({ ok: true, applied: true });
      }
      const patch = {
        ...threadControl("sesn_config_all", "rin_config_all", "thrd_main"),
        generation: 7,
        payloadJson: "{\"config_generation\":7}",
      };
      const expectedPatch = {
        generation: 7,
        payloadJson: "{\"config_generation\":7}",
      };

      expect(await Effect.runPromise(manager.applyRuntimeConfigPatch("sesn_config_all", patch))).toEqual({
        ok: true,
        sessionId: "sesn_config_all",
        created: false,
        applied: true,
      });

      for (const sessionThreadId of ["thrd_main", "thrd_child"]) {
        expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_config_all", `rin_run_${sessionThreadId}`, sessionThreadId)))).toMatchObject({
          ok: true,
          started: true,
        });
        await waitForRuns(agentLoop, agentLoop.runs.length + 1);
        expect(agentLoop.runs.at(-1)?.session.configuration.runtimeConfigPatch()).toEqual(expectedPatch);
        agentLoop.runs.at(-1)?.release();
        await waitForThreadIdle(manager, "sesn_config_all", sessionThreadId);
      }
    });
  });

  test("busy sibling rejects a session config patch without partially updating idle threads", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      for (const sessionThreadId of ["thrd_main", "thrd_child"]) {
        expect(await Effect.runPromise(manager.preloadThread({
          ...threadControl("sesn_config_busy", `rin_preload_${sessionThreadId}`, sessionThreadId),
          runtimeBindingToken: "runtime-binding-token",
          coldCoverage: coldCoverage(),
          messages: [],
        }))).toMatchObject({ ok: true, applied: true });
      }
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_config_busy", "rin_child_busy", "thrd_child")))).toMatchObject({
        ok: true,
        started: true,
      });
      await waitForRuns(agentLoop, 1);

      expect(await Effect.runPromise(manager.applyRuntimeConfigPatch("sesn_config_busy", {
        ...threadControl("sesn_config_busy", "rin_config_rejected", "thrd_main"),
        generation: 8,
        payloadJson: "{\"config_generation\":8}",
      }))).toEqual({ ok: false, sessionId: "sesn_config_busy", reason: "control_busy" });

      agentLoop.runs[0]?.release();
      await waitForThreadIdle(manager, "sesn_config_busy", "thrd_child");
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_config_busy", "rin_main_after_reject", "thrd_main")))).toMatchObject({
        ok: true,
        started: true,
      });
      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs[1]?.session.configuration.runtimeConfigPatch()).toBeUndefined();
      agentLoop.runs[1]?.release();
    });
  });

  test("runtime config and MCP manifest generations use independent monotonic stale gates", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_create")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForRuns(agentLoop, 1);
      const session = agentLoop.runs[0]?.session;
      agentLoop.runs[0]?.release();

      const applyWhenIdle = async (command: Parameters<typeof manager.applyRuntimeConfigPatch>[1]): Promise<SessionManager.RuntimeControlResult> => {
        for (let attempt = 0; attempt < 100; attempt += 1) {
          const result = await Effect.runPromise(manager.applyRuntimeConfigPatch("sesn_1", command));
          if (result.ok) {
            return result;
          }
          await new Promise((resolve) => setTimeout(resolve, 1));
        }
        throw new Error("session did not become idle");
      };

      expect(
        await applyWhenIdle({
          ...threadControl("sesn_1", "rin_config_5"),
          generation: 5,
          payloadJson: "{\"config_generation\":5}",
        }),
      ).toEqual({ ok: true, sessionId: "sesn_1", created: false, applied: true });
      expect(
        await applyWhenIdle({
          ...threadControl("sesn_1", "rin_mcp_github_1"),
          generation: 1,
          mcpServerName: "github",
          manifestETag: "etag_1",
          payloadJson: "{\"mcp_manifest\":{\"mcp_server_name\":\"github\",\"manifest_etag\":\"etag_1\",\"manifest_generation\":1,\"tools\":[]}}",
        }),
      ).toEqual({ ok: true, sessionId: "sesn_1", created: false, applied: true });
      expect(
        await applyWhenIdle({
          ...threadControl("sesn_1", "rin_mcp_github_1_dup"),
          generation: 1,
          mcpServerName: "github",
          manifestETag: "etag_1",
          payloadJson: "{\"mcp_manifest\":{\"mcp_server_name\":\"github\",\"manifest_etag\":\"etag_1\",\"manifest_generation\":1,\"tools\":[]}}",
        }),
      ).toEqual({ ok: true, sessionId: "sesn_1", created: false, applied: false });
      expect(
        await applyWhenIdle({
          ...threadControl("sesn_1", "rin_config_4"),
          generation: 4,
          payloadJson: "{\"config_generation\":4}",
        }),
      ).toEqual({ ok: true, sessionId: "sesn_1", created: false, applied: false });
      expect(
        await applyWhenIdle({
          ...threadControl("sesn_1", "rin_config_6"),
          generation: 6,
          payloadJson: "{\"config_generation\":6}",
        }),
      ).toEqual({ ok: true, sessionId: "sesn_1", created: false, applied: true });

      expect(session?.configuration.patches()).toEqual([
        {
          generation: 6,
          payloadJson: "{\"config_generation\":6}",
        },
        {
          generation: 1,
          mcpServerName: "github",
          manifestETag: "etag_1",
          payloadJson: "{\"mcp_manifest\":{\"mcp_server_name\":\"github\",\"manifest_etag\":\"etag_1\",\"manifest_generation\":1,\"tools\":[]}}",
        },
      ]);
    });
  });

  test("input accepted during interrupt cleanup starts one follow-up after the run scope closes", async () => {
    const agentLoop = makeInterruptCleanupAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_first")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForRuns(agentLoop, 1);
      const firstSession = agentLoop.runs[0]?.session;

      const interruptCommand = { ...threadControl("sesn_1", "rin_interrupt"), sequenceTo: 9 };
      const interrupt = Effect.runPromise(manager.interruptControl(
        "sesn_1",
        interruptCommand,
        testControlCommit(interruptCommand),
      ));
      await agentLoop.cleanupStarted;

      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_after_interrupt")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        started: false,
        pendingWake: true,
      });

      agentLoop.releaseCleanup();
      expect(await interrupt).toEqual({ ok: true, sessionId: "sesn_1", created: false, interrupted: true, idleInterrupt: false });
      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs[1]?.session).toBe(firstSession);
      agentLoop.runs[1]?.release();
    });
  });

  test("interrupt discards pre-fence queued input and preserves input accepted during closeout", async () => {
    const agentLoop = makeInterruptCleanupAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput({ ...acceptedInput("sesn_fence", "rin_active"), sequenceTo: 1 }))).toMatchObject({
        ok: true,
        started: true,
      });
      await waitForRuns(agentLoop, 1);
      const session = agentLoop.runs[0]?.session;
      session?.state.acknowledgeAcceptedInput("rin_active");

      expect(await Effect.runPromise(manager.acceptInput({ ...acceptedInput("sesn_fence", "rin_before_fence"), sequenceTo: 5 }))).toMatchObject({
        ok: true,
        pendingWake: true,
      });
      const interruptCommand = {
        ...threadControl("sesn_fence", "rin_interrupt_fence"),
        sequenceTo: 9,
      };
      const interrupt = Effect.runPromise(manager.interruptControl(
        "sesn_fence",
        interruptCommand,
        testControlCommit(interruptCommand),
      ));
      await agentLoop.cleanupStarted;

      expect(await Effect.runPromise(manager.acceptInput({ ...acceptedInput("sesn_fence", "rin_after_fence"), sequenceFrom: 10, sequenceTo: 10 }))).toMatchObject({
        ok: true,
        pendingWake: true,
      });
      agentLoop.releaseCleanup();
      await expect(interrupt).resolves.toMatchObject({ ok: true, interrupted: true });
      await waitForRuns(agentLoop, 2);

      expect(session?.state.acceptedInputCount()).toBe(1);
      expect(session?.state.peekAcceptedInput()?.runtimeInputId).toBe("rin_after_fence");
      agentLoop.runs[1]?.release();
    });
  });

  test("positive interrupt fence preserves queued completion mail for one follow-up presentation", async () => {
    const agentLoop = makeInterruptCleanupAgentLoop();
    const sessionID = "sesn_interrupt_agent_mail";
    const threadID = "thrd_interrupt_agent_mail_main";
    const thread: RuntimeAcceptedThreadMetadataState = {
      role: "main",
      visibility: "public",
      agentType: "general",
      status: "idle",
    };
    const mail = agentMailInput(
      sessionID,
      "agent_mail:delivery_interrupt_agent_mail",
      threadID,
      "thrd_interrupt_agent_mail_child",
      thread,
    );
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput({
        ...acceptedInput(sessionID, "rin_interrupt_agent_mail_active", threadID),
        sequenceTo: 1,
      }))).toMatchObject({ ok: true, started: true });
      await waitForRuns(agentLoop, 1);
      agentLoop.runs[0]!.session.state.acknowledgeAcceptedInput("rin_interrupt_agent_mail_active");
      expect(await Effect.runPromise(manager.acceptInput(mail))).toMatchObject({
        ok: true,
        started: false,
        pendingWake: true,
      });

      const interruptCommand = {
        ...threadControl(sessionID, "rin_interrupt_agent_mail_fence", threadID),
        sequenceTo: 9,
      };
      const interrupt = Effect.runPromise(manager.interruptControl(
        sessionID,
        interruptCommand,
        testControlCommit(interruptCommand),
      ));
      await agentLoop.cleanupStarted;
      agentLoop.releaseCleanup();
      await expect(interrupt).resolves.toMatchObject({ ok: true, interrupted: true });

      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs[1]!.session.state.peekAcceptedInput()?.runtimeInputId).toBe(mail.runtimeInputId);
      agentLoop.runs[1]!.session.state.acknowledgeAcceptedInput(mail.runtimeInputId);
      agentLoop.runs[1]!.release({ type: "completed", modelMessageCount: 1 });
      await waitForThreadIdle(manager, sessionID, threadID);
      expect(await Effect.runPromise(manager.acceptInput(mail))).toMatchObject({
        ok: true,
        started: false,
        pendingWake: false,
      });
      expect(agentLoop.runs).toHaveLength(2);
    });
  });

  test("shutdownActiveRuns interrupts active AgentLoop owner fibers", async () => {
    const agentLoop = makeInterruptRecordingAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_shutdown")))).toMatchObject({
        ok: true,
        sessionId: "sesn_shutdown",
        started: true,
      });
      await waitForInterruptRecordingRuns(agentLoop, 1);
      const session = agentLoop.runs[0]?.session;
      if (session === undefined) {
        throw new Error("expected active session");
      }

      await Effect.runPromise(manager.shutdownActiveRuns());

      expect(agentLoop.interruptions).toEqual([
        {
          sessionId: "sesn_shutdown",
          runtimeShutdownRequested: true,
        },
      ]);
      expect(session.state.runtimeShutdownRequested()).toBe(false);
      expect(await Effect.runPromise(manager.cleanupSession("sesn_shutdown", threadControl("sesn_shutdown")))).toEqual({
        ok: true,
        sessionId: "sesn_shutdown",
        cleaned: false,
      });
    });
  });

  test("shutdownActiveRuns reaches active runs without pending interrupt, cleanup, or unbind", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_shutdown")))).toEqual({
        ok: true,
        sessionId: "sesn_shutdown",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForRuns(agentLoop, 1);
      const session = agentLoop.runs[0]?.session;
      if (session === undefined) {
        throw new Error("expected active session");
      }
      const shutdown = Effect.runPromise(manager.shutdownActiveRuns());
      await new Promise((resolve) => setTimeout(resolve, 1));
      agentLoop.runs[0]?.release({ type: "interrupted" });
      await shutdown;

      expect(session.state.runtimeShutdownRequested()).toBe(false);
      expect(agentLoop.runs[0]?.args).toHaveLength(2);
      expect(await Effect.runPromise(manager.cleanupSession("sesn_shutdown", threadControl("sesn_shutdown")))).toEqual({
        ok: true,
        sessionId: "sesn_shutdown",
        cleaned: false,
      });
    });
  });

  test("shutdown joins cold installation and rejects a late preload result", async () => {
    const agentLoop = makeControlledAgentLoop();
    let reportLoadStarted = (): void => undefined;
    let releaseLoad = (): void => undefined;
    const loadStarted = new Promise<void>((resolve) => {
      reportLoadStarted = resolve;
    });
    const loadGate = new Promise<void>((resolve) => {
      releaseLoad = resolve;
    });
    const command = threadControl("sesn_shutdown_install", "rin_shutdown_install");
    await withSessionManager(sessionManagerLayer(agentLoop, {
      loadThreadContext: async () => {
        reportLoadStarted();
        await loadGate;
        return {
          ...command,
          runtimeBindingToken: "runtime-binding-token-shutdown-install",
          coldCoverage: coldCoverage(),
          messages: [],
          thread: { role: "main", visibility: "public", status: "idle" },
        };
      },
    }), async (manager) => {
      const installation = Effect.runPromise(manager.ensureThreadInstalled(command));
      await loadStarted;

      await Effect.runPromise(manager.shutdownActiveRuns());
      releaseLoad();

      await expect(installation).resolves.toMatchObject({
        ok: false,
        reason: "context_load_failed",
      });
      expect(await Effect.runPromise(manager.inspectThread(command))).toEqual({
        ok: true,
        sessionId: command.sessionId,
        sessionThreadId: command.sessionThreadId,
        observed: false,
        messages: [],
      });
    });
  });

  test("accepted input restarts an existing idle session with the same hot ContextManager", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_1"))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
      });
      await waitForRuns(agentLoop, 1);
      const firstSession = agentLoop.runs[0]?.session;
      expect(firstSession).toBeDefined();
      firstSession?.state.contextManager.appendMessage({
        id: "msg_1",
        sessionId: "sesn_1",
        role: "user",
        origin: "user",
        sequence: 0,
        status: "completed",
        createdAt: timestamp,
        parts: [],
      });
      agentLoop.runs[0]?.release();

      for (let attempt = 0; attempt < 100 && agentLoop.runs.length === 1; attempt += 1) {
        const result = await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_1"));
        if (result.ok && result.started) {
          expect(result).toEqual({ ok: true, sessionId: "sesn_1", created: false, started: true });
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs[1]?.session).toBe(firstSession);
      expect(agentLoop.runs[1]?.session.state.contextManager.messages()).toHaveLength(1);
      agentLoop.runs[1]?.release();

      for (let attempt = 0; attempt < 100 && agentLoop.runs.length === 2; attempt += 1) {
        const result = await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_restart_after_idle")));
        if (result.ok && result.started) {
          expect(result).toEqual({ ok: true, sessionId: "sesn_1", created: false, started: true, pendingWake: false });
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      await waitForRuns(agentLoop, 3);
      expect(agentLoop.runs[2]?.session).toBe(firstSession);
      expect(agentLoop.runs[2]?.session.state.contextManager.messages()).toHaveLength(1);
      agentLoop.runs[2]?.release();
    });
  });

  test("concurrent same-thread accept calls coalesce behind one active run", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const [first, second, third] = await Promise.all([
        Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_concurrent_1"))),
        Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_concurrent_2"))),
        Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_concurrent_3"))),
      ]);

      await waitForRuns(agentLoop, 1);
      expect([first, second, third].filter((result) => result.ok && result.started)).toHaveLength(1);
      expect([first, second, third].filter((result) => result.ok && result.pendingWake)).toHaveLength(2);
      agentLoop.runs[0]?.release();
      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs.map((run) => run.sessionId)).toEqual(["sesn_1", "sesn_1"]);
      agentLoop.runs[1]?.release();
    });
  });

  test("concurrent thread waiters join one owner run", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_join", "rin_join")));
      await waitForRuns(agentLoop, 1);

      const first = Effect.runPromise(manager.waitThread(threadControl("sesn_join"), undefined));
      const second = Effect.runPromise(manager.waitThread(threadControl("sesn_join"), undefined));
      await new Promise((resolve) => setTimeout(resolve, 5));
      expect(agentLoop.runs).toHaveLength(1);

      agentLoop.runs[0]?.release();
      expect(await Promise.all([first, second])).toEqual([
        expect.objectContaining({ ok: true, observed: true, status: "idle", timedOut: false }),
        expect.objectContaining({ ok: true, observed: true, status: "idle", timedOut: false }),
      ]);
      expect(agentLoop.runs).toHaveLength(1);
    });
  });

  test("cancelling a joined thread waiter does not cancel the owner run", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_join_cancel", "rin_join_cancel")));
      await waitForRuns(agentLoop, 1);

      const waiter = Effect.runFork(manager.waitThread(threadControl("sesn_join_cancel"), undefined));
      await new Promise((resolve) => setTimeout(resolve, 5));
      await Effect.runPromise(Fiber.interrupt(waiter));

      expect(agentLoop.runs).toHaveLength(1);
      expect(await Effect.runPromise(manager.inspectThread(threadControl("sesn_join_cancel")))).toMatchObject({
        ok: true,
        observed: true,
        status: "running",
      });
      agentLoop.runs[0]?.release();
      await waitForThreadIdle(manager, "sesn_join_cancel", "thrd_sesn_join_cancel");
    });
  });

  test("closing a waiter scope detaches it without consuming pending input", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_waiter_scope", "rin_active")));
      await waitForRuns(agentLoop, 1);
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_waiter_scope", "rin_pending")))).toMatchObject({
        ok: true,
        pendingWake: true,
      });
      const session = agentLoop.runs[0]?.session;
      expect(session?.state.acceptedInputCount()).toBe(2);

      const waiterScope = await Effect.runPromise(Scope.make());
      const waiter = await Effect.runPromise(
        Effect.forkIn(manager.waitThread(threadControl("sesn_waiter_scope"), undefined), waiterScope),
      );
      await Effect.runPromise(Scope.close(waiterScope, Exit.void));
      const waiterExit = await Effect.runPromise(Fiber.await(waiter));

      expect(Exit.isFailure(waiterExit)).toBe(true);
      expect(session?.state.acceptedInputCount()).toBe(2);
      expect(await Effect.runPromise(manager.inspectThread(threadControl("sesn_waiter_scope")))).toMatchObject({ status: "running" });
      agentLoop.runs[0]?.release();
      await waitForRuns(agentLoop, 2);
      agentLoop.runs[1]?.release();
    });
  });

  test("wake during follow-up scope cleanup starts exactly one later follow-up", async () => {
    const agentLoop = makeFollowUpCleanupAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_follow_cleanup", "rin_first")));
      await waitForRuns(agentLoop, 1);
      await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_follow_cleanup", "rin_second")));
      agentLoop.runs[0]?.release();
      await waitForRuns(agentLoop, 2);

      agentLoop.runs[1]?.release();
      await agentLoop.followUpCleanupStarted;
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_follow_cleanup", "rin_during_cleanup")))).toMatchObject({
        ok: true,
        started: false,
        pendingWake: true,
      });
      agentLoop.releaseFollowUpCleanup();

      await waitForRuns(agentLoop, 3);
      expect(agentLoop.runs).toHaveLength(3);
      agentLoop.runs[2]?.release();
      await waitForThreadIdle(manager, "sesn_follow_cleanup", "thrd_sesn_follow_cleanup");
      expect(agentLoop.runs).toHaveLength(3);
    });
  });

  test("payload-like extra input cannot reach AgentLoop through SessionManager", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const unsafeManager = manager as unknown as {
        readonly acceptInput: (command: ReturnType<typeof acceptedInput>, payload: unknown) => ReturnType<typeof manager.acceptInput>;
      };
      expect(await Effect.runPromise(unsafeManager.acceptInput(acceptedInput("sesn_1", "rin_hostile_first"), { runtimeMessage: hostileText }))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForRuns(agentLoop, 1);
      expect(agentLoop.runs[0]?.args).toHaveLength(2);
      expect(agentLoop.runs[0]?.args[0]).toBe(agentLoop.runs[0]?.session);
      agentLoop.runs[0]?.release();

      for (let attempt = 0; attempt < 100 && agentLoop.runs.length === 1; attempt += 1) {
        const result = await Effect.runPromise(unsafeManager.acceptInput(acceptedInput("sesn_1"), { providerRequest: hostileText }));
        if (result.ok && result.started) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs[1]?.args).toHaveLength(2);
      expect(agentLoop.runs[1]?.args[0]).toBe(agentLoop.runs[1]?.session);
      agentLoop.runs[1]?.release();
    });
  });

  test("different sessions have isolated manager-owned run markers and ContextManagers", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_event_write_initial")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
        pendingWake: false,
      });
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_2"))).toEqual({
        ok: true,
        sessionId: "sesn_2",
        created: true,
        started: true,
      });

      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs.map((run) => run.sessionId).sort()).toEqual(["sesn_1", "sesn_2"]);
      expect(agentLoop.runs[0]?.session.state.contextManager).not.toBe(agentLoop.runs[1]?.session.state.contextManager);
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_crashed_initial")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        started: false,
        pendingWake: true,
      });
      agentLoop.runs[0]?.release();
      agentLoop.runs[1]?.release();
      await waitForRuns(agentLoop, 3);
      expect(agentLoop.runs.map((run) => run.sessionId).sort()).toEqual(["sesn_1", "sesn_1", "sesn_2"]);
      agentLoop.runs[2]?.release();
    });
  });

  test("fatal thread release disposes its approval reviewer manager", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_reviewer_release")));
      await waitForRuns(agentLoop, 1);
      const reviewerManager = agentLoop.runs[0]?.session.approvalReviewerManager;
      expect(reviewerManager?.isDisposed()).toBe(false);

      agentLoop.runs[0]?.release(fatalRunResult("crashed"));
      for (let attempt = 0; attempt < 100 && reviewerManager?.isDisposed() !== true; attempt += 1) {
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      expect(reviewerManager?.isDisposed()).toBe(true);
    });
  });

  test("fatal public parent release closes its active reviewer resident through run settlement", async () => {
    const sessionId = "sesn_parent_reviewer_release";
    const parentThreadId = "thrd_parent_release";
    const reviewerThreadId = "thrd_reviewer_trunk_release";
    const agentLoop = makeReviewerInterruptCleanupAgentLoop(reviewerThreadId);
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const unrelatedThreadId = "thrd_unrelated_release";
      await Effect.runPromise(manager.preloadThread({
        ...threadControl(sessionId, "rin_parent_preload", parentThreadId),
        thread: { role: "main", visibility: "public", status: "idle" },
        runtimeBindingToken: "runtime-binding-token-parent",
        coldCoverage: coldCoverage(),
        messages: [],
      }));
      await Effect.runPromise(manager.acceptInput(acceptedInput(sessionId, "rin_parent_release", parentThreadId)));
      await Effect.runPromise(manager.acceptInput(approvalReviewInput(
        sessionId,
        "rin_reviewer_release",
        reviewerThreadId,
        parentThreadId,
      )));
      await Effect.runPromise(manager.preloadThread({
        ...threadControl(sessionId, "rin_unrelated_release", unrelatedThreadId),
        thread: { role: "subagent", visibility: "public", status: "idle" },
        runtimeBindingToken: "runtime-binding-token-unrelated",
        coldCoverage: coldCoverage(),
        messages: [],
      }));
      await waitForRuns(agentLoop, 2);

      agentLoop.runs[0]?.release(fatalRunResult("crashed"));
      expect(await Promise.race([
        agentLoop.reviewerCleanupStarted.then(() => true),
        new Promise<false>((resolve) => setTimeout(() => resolve(false), 100)),
      ])).toBe(true);
      expect(await Effect.runPromise(manager.inspectThread(threadControl(
        sessionId,
        "rin_inspect_reviewer_closing",
        reviewerThreadId,
      )))).toMatchObject({ observed: true, status: "closed_for_runtime" });
      expect(await Effect.runPromise(manager.inspectThread(threadControl(
        sessionId,
        "rin_inspect_parent_during_reviewer_close",
        parentThreadId,
      )))).toMatchObject({ observed: true });
      expect(await Effect.runPromise(manager.inspectThread(threadControl(
        sessionId,
        "rin_inspect_unrelated_during_reviewer_close",
        unrelatedThreadId,
      )))).toMatchObject({ observed: true });

      agentLoop.releaseReviewerCleanup();
      await waitForCondition(async () => {
        const snapshot = await Effect.runPromise(manager.inspectThread(threadControl(
          sessionId,
          "rin_inspect_reviewer_released",
          reviewerThreadId,
        )));
        return snapshot.ok && snapshot.observed === false;
      }, "reviewer resident release after owner settlement");
      expect(await Effect.runPromise(manager.inspectThread(threadControl(
        sessionId,
        "rin_inspect_parent_released",
        parentThreadId,
      )))).toMatchObject({ observed: false });
      expect(await Effect.runPromise(manager.inspectThread(threadControl(
        sessionId,
        "rin_inspect_unrelated",
        unrelatedThreadId,
      )))).toMatchObject({ observed: true });
    });
  });

  test("session cleanup disposes every thread approval reviewer manager", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_reviewer_cleanup", "rin_a", "thrd_a")));
      await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_reviewer_cleanup", "rin_b", "thrd_b")));
      await waitForRuns(agentLoop, 2);
      const reviewerManagers = agentLoop.runs.map((run) => run.session.approvalReviewerManager);
      agentLoop.runs[0]?.release();
      agentLoop.runs[1]?.release();

      const cleanup = await waitForIdleCleanup(manager, "sesn_reviewer_cleanup");
      expect(cleanup).toEqual({ ok: true, sessionId: "sesn_reviewer_cleanup", cleaned: true });
      expect(reviewerManagers.every((reviewerManager) => reviewerManager?.isDisposed() === true)).toBe(true);
    });
  });

  test("same session id in different workspaces owns separate hot sessions and cleanup", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      const workspaceA = acceptedInput("sesn_shared", "rin_workspace_a", "thrd_workspace_a");
      const workspaceB = {
        ...acceptedInput("sesn_shared", "rin_workspace_b", "thrd_workspace_b"),
        workspaceId: "wksp_other",
        bindingId: "bind_workspace_b",
        targetPodUid: "pod_workspace_b",
      };
      expect(await Effect.runPromise(manager.acceptInput(workspaceA))).toEqual({
        ok: true,
        sessionId: "sesn_shared",
        created: true,
        started: true,
        pendingWake: false,
      });
      expect(await Effect.runPromise(manager.acceptInput(workspaceB))).toEqual({
        ok: true,
        sessionId: "sesn_shared",
        created: true,
        started: true,
        pendingWake: false,
      });

      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs.map((run) => run.session.identity.workspaceId).sort()).toEqual(["wksp_other", "wksp_test"]);
      expect(agentLoop.runs[0]?.session.state.contextManager).not.toBe(agentLoop.runs[1]?.session.state.contextManager);
      expect(agentLoop.runs[0]?.session.toolCoordinator).not.toBe(agentLoop.runs[1]?.session.toolCoordinator);
      agentLoop.runs[0]?.release();
      agentLoop.runs[1]?.release();

      const cleanupA = await waitForIdleCleanup(manager, "sesn_shared");
      expect(cleanupA).toEqual({ ok: true, sessionId: "sesn_shared", cleaned: true });
      let cleanupB: SessionManager.CleanupSessionResult = { ok: false, sessionId: "sesn_shared", reason: "session_busy" };
      for (let attempt = 0; attempt < 100 && !cleanupB.ok; attempt += 1) {
        cleanupB = await Effect.runPromise(
          manager.cleanupSession("sesn_shared", {
            ...threadControl("sesn_shared", "rin_cleanup_workspace_b", "thrd_workspace_b"),
            workspaceId: "wksp_other",
            bindingId: "bind_workspace_b",
            targetPodUid: "pod_workspace_b",
          }),
        );
        if (!cleanupB.ok) {
          await new Promise((resolve) => setTimeout(resolve, 1));
        }
      }
      expect(cleanupB).toEqual({ ok: true, sessionId: "sesn_shared", cleaned: true });
    });
  });

  test("same session different threads own independent run slots and ContextManagers", async () => {
    const agentLoop = makeControlledAgentLoop();
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_thread_a", "thrd_a")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
        pendingWake: false,
      });
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_thread_b", "thrd_b")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        started: true,
        pendingWake: false,
      });

      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs.map((run) => run.session.identity.sessionThreadId).sort()).toEqual(["thrd_a", "thrd_b"]);
      expect(agentLoop.runs[0]?.session.state.contextManager).not.toBe(agentLoop.runs[1]?.session.state.contextManager);
      expect(agentLoop.runs[0]?.session.toolCoordinator).toBe(agentLoop.runs[1]?.session.toolCoordinator);
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_thread_a_follow", "thrd_a")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        started: false,
        pendingWake: true,
      });

      agentLoop.runs[0]?.release();
      agentLoop.runs[1]?.release();
      await waitForRuns(agentLoop, 3);
      expect(agentLoop.runs[2]?.session.identity.sessionThreadId).toBe("thrd_a");
      expect(agentLoop.runs[2]?.session).toBe(agentLoop.runs.find((run) => run.session.identity.sessionThreadId === "thrd_a")?.session);
      agentLoop.runs[2]?.release();
    });
  });

  test("cleanup rejects running sessions, removes idle sessions without durable unbind, and releases capacity", async () => {
    const agentLoop = makeControlledAgentLoop();
    const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

    await withSessionManager(layer, async (manager) => {
      await Effect.runPromise(
        Effect.gen(function* () {
        expect(yield* manager.cleanupSession("missing", threadControl("missing"))).toEqual({ ok: true, sessionId: "missing", cleaned: false });
        expect(yield* manager.startTestRunThroughAcceptedInput("sesn_1")).toEqual({ ok: true, sessionId: "sesn_1", created: true, started: true });
        expect(yield* manager.startTestRunThroughAcceptedInput("sesn_2")).toEqual({
          ok: false,
          sessionId: "sesn_2",
          reason: "local_session_capacity_exceeded",
        });
        expect(yield* manager.cleanupSession("sesn_1", threadControl("sesn_1"))).toEqual({ ok: false, sessionId: "sesn_1", reason: "session_busy" });
        }),
      );

      await waitForRuns(agentLoop, 1);
      agentLoop.runs[0]?.release();

      expect(await waitForIdleCleanup(manager, "sesn_1")).toEqual({ ok: true, sessionId: "sesn_1", cleaned: true });
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_2"))).toEqual({
        ok: true,
        sessionId: "sesn_2",
        created: true,
        started: true,
      });
      await waitForRuns(agentLoop, 2);
      agentLoop.runs[1]?.release();
    });
  });

  test("cleanup rejects an inter-turn accepted queue even when no run slot is active", async () => {
    const agentLoop = makeControlledAgentLoop();
    const sessionID = "sesn_cleanup_inter_turn_queue";
    const threadID = "thrd_cleanup_inter_turn_queue";
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(
        acceptedInput(sessionID, "rin_cleanup_inter_turn_initial", threadID),
      ))).toMatchObject({ ok: true, started: true });
      await waitForRuns(agentLoop, 1);
      const session = agentLoop.runs[0]!.session;
      agentLoop.runs[0]!.release({ type: "completed", modelMessageCount: 1 });
      await waitForThreadIdle(manager, sessionID, threadID);

      const queued = acceptedInput(sessionID, "rin_cleanup_inter_turn_queued", threadID);
      expect(session.state.enqueueAcceptedInput(queued)).toBe("applied");
      expect(await Effect.runPromise(manager.cleanupSession(
        sessionID,
        threadControl(sessionID, "rin_cleanup_inter_turn_busy", threadID),
      ))).toEqual({ ok: false, sessionId: sessionID, reason: "session_busy" });

      session.state.acknowledgeAcceptedInput(queued.runtimeInputId);
      expect(await Effect.runPromise(manager.cleanupSession(
        sessionID,
        threadControl(sessionID, "rin_cleanup_inter_turn_drained", threadID),
      ))).toEqual({ ok: true, sessionId: sessionID, cleaned: true });
    });
  });

  test("cleanup rejects a thread command while its durable receipt is still pending", async () => {
    const agentLoop = makeControlledAgentLoop();
    const sessionId = "sesn_cleanup_receipt_awaiting";
    const threadId = "thrd_cleanup_receipt_awaiting";
    let resolveCommit!: (value: { readonly ok: true; readonly committedMessage: ReturnType<typeof bridgeRuntimeMessage> }) => void;
    const commit = new Promise<{ readonly ok: true; readonly committedMessage: ReturnType<typeof bridgeRuntimeMessage> }>((resolve) => {
      resolveCommit = resolve;
    });
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl(sessionId, "rin_cleanup_receipt_preload", threadId),
        runtimeBindingToken: "runtime-binding-token",
        messages: [],
        backgroundTools: [{
          taskId: "task_cleanup_receipt",
          sourceToolUseEventId: "sevt_cleanup_receipt",
        }],
        coldCoverage: coldCoverage(),
      }))).toMatchObject({ ok: true, applied: true });

      const notification = Effect.runPromise(manager.commitTaskNotification(sessionId, {
        ...threadControl(sessionId, "rin_cleanup_receipt_notification", threadId),
        taskId: "task_cleanup_receipt",
        sourceToolUseEventId: "sevt_cleanup_receipt",
        status: "completed",
        payloadJson: "{\"status\":\"completed\"}",
      }, async () => await commit));
      await new Promise((resolve) => setTimeout(resolve, 1));

      expect(await Effect.runPromise(manager.cleanupSession(
        sessionId,
        threadControl(sessionId, "rin_cleanup_receipt_attempt", threadId),
      ))).toEqual({ ok: false, sessionId, reason: "session_busy" });

      resolveCommit({ ok: true, committedMessage: bridgeRuntimeMessage(sessionId, "task complete") });
      await expect(notification).resolves.toMatchObject({ ok: true, applied: true });
      await waitForRuns(agentLoop, 1);
      agentLoop.runs[0]!.release();
    });
  });

  test("fatal persistence run discards hot state and releases local capacity", async () => {
    const agentLoop = makeControlledAgentLoop();
    const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

    await withSessionManager(layer, async (manager) => {
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_1"))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
      });
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_2"))).toEqual({
        ok: false,
        sessionId: "sesn_2",
        reason: "local_session_capacity_exceeded",
      });
      await waitForRuns(agentLoop, 1);
      agentLoop.runs[0]?.release(fatalRunResult("persistence_failed"));

      for (let attempt = 0; attempt < 100 && agentLoop.runs.length === 1; attempt += 1) {
        const result = await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_2"));
        if (result.ok && result.started) {
          expect(result).toEqual({ ok: true, sessionId: "sesn_2", created: true, started: true });
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }

      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs[1]?.sessionId).toBe("sesn_2");
      expect(await Effect.runPromise(manager.cleanupSession("sesn_1", threadControl("sesn_1")))).toEqual({ ok: true, sessionId: "sesn_1", cleaned: false });
      agentLoop.runs[1]?.release();
    });
  });

  test("value-level failed exit releases queued completion mail for cold redelivery", async () => {
    const agentLoop = makeControlledAgentLoop();
    const sessionID = "sesn_failed_agent_mail_redelivery";
    const threadID = "thrd_failed_agent_mail_redelivery_main";
    const thread: RuntimeAcceptedThreadMetadataState = {
      role: "main",
      visibility: "public",
      agentType: "general",
      status: "idle",
    };
    const mail = agentMailInput(
      sessionID,
      "agent_mail:delivery_failed_agent_mail_redelivery",
      threadID,
      "thrd_failed_agent_mail_redelivery_child",
      thread,
    );
    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl(sessionID, "rin_preload_failed_agent_mail_redelivery", threadID),
        runtimeBindingToken: "runtime-binding-token",
        coldCoverage: coldCoverage(),
        messages: [],
        thread,
      }))).toMatchObject({ ok: true, applied: true });
      expect(await Effect.runPromise(manager.acceptInput(
        acceptedInput(sessionID, "rin_failed_agent_mail_active", threadID),
      ))).toMatchObject({ ok: true, started: true });
      await waitForRuns(agentLoop, 1);
      expect(await Effect.runPromise(manager.acceptInput(mail))).toMatchObject({
        ok: true,
        started: false,
        pendingWake: true,
      });

      const failure = fatalRunResult("persistence_failed");
      if (failure.type !== "failed") {
        throw new Error("expected failed run fixture");
      }
      agentLoop.runs[0]?.release({ type: "failed", error: failure.error });
      await waitForCondition(async () => {
        const snapshot = await Effect.runPromise(manager.inspectThread(
          threadControl(sessionID, "rin_inspect_failed_agent_mail_release", threadID),
        ));
        return snapshot.ok && !snapshot.observed;
      }, "failed agent-mail recipient release");

      expect(await Effect.runPromise(manager.preloadThread({
        ...threadControl(sessionID, "rin_reload_failed_agent_mail_redelivery", threadID),
        runtimeBindingToken: "runtime-binding-token",
        messages: [],
        thread,
        pendingAgentMail: [mail],
        coldCoverage: coldCoverage({
          undeliveredMailDeliveryIds: [mail.deliveryId],
        }),
      }))).toMatchObject({ ok: true, applied: true });
      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs[1]?.session.state.peekAcceptedInput()?.runtimeInputId).toBe(mail.runtimeInputId);
      agentLoop.runs[1]?.release({ type: "completed", modelMessageCount: 1 });
    });
  });

  test("discard-requested interruption removes hot state before the next command", async () => {
    const agentLoop = makeControlledAgentLoop();
    const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

    await withSessionManager(layer, async (manager) => {
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_discard_hot_state"))).toEqual({
        ok: true,
        sessionId: "sesn_discard_hot_state",
        created: true,
        started: true,
      });
      await waitForRuns(agentLoop, 1);
      const staleSession = agentLoop.runs[0]?.session;
      agentLoop.runs[0]?.release({ type: "interrupted", discardHotState: true });

      let accepted: SessionManager.AcceptInputResult | undefined;
      for (let attempt = 0; attempt < 100 && accepted === undefined; attempt += 1) {
        const result = await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_discard_hot_state", "rin_after_discard")));
        if (result.ok && result.started) {
          accepted = result;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }

      expect(accepted).toEqual({
        ok: true,
        sessionId: "sesn_discard_hot_state",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs[1]?.session).not.toBe(staleSession);
      agentLoop.runs[1]?.release();
    });
  });

  test("fatal event-write run drops pending restart and discards hot state", async () => {
    const agentLoop = makeControlledAgentLoop();
    const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

    await withSessionManager(layer, async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_event_write_initial")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForRuns(agentLoop, 1);
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_event_write_follow")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        started: false,
        pendingWake: true,
      });
      agentLoop.runs[0]?.release(fatalRunResult("event_write_failed"));

      for (let attempt = 0; attempt < 100 && agentLoop.runs.length === 1; attempt += 1) {
        const result = await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_2"));
        if (result.ok && result.started) {
          expect(result).toEqual({ ok: true, sessionId: "sesn_2", created: true, started: true });
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }

      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs.map((run) => run.sessionId)).toEqual(["sesn_1", "sesn_2"]);
      agentLoop.runs[1]?.release();
    });
  });

  test("fatal crashed run clears state, drops pending restart, and discards hot state", async () => {
    const agentLoop = makeControlledAgentLoop();
    const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

    await withSessionManager(layer, async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_crashed_initial")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForRuns(agentLoop, 1);
      agentLoop.runs[0]?.session.state.contextManager.appendMessage({
        id: "msg_1",
        sessionId: "sesn_1",
        role: "user",
        origin: "user",
        sequence: 0,
        status: "completed",
        createdAt: timestamp,
        parts: [],
      });
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1", "rin_crashed_follow")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: false,
        started: false,
        pendingWake: true,
      });
      agentLoop.runs[0]?.release(fatalRunResult("crashed"));

      for (let attempt = 0; attempt < 100 && agentLoop.runs.length === 1; attempt += 1) {
        const result = await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_2"));
        if (result.ok && result.started) {
          expect(result).toEqual({ ok: true, sessionId: "sesn_2", created: true, started: true });
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }

      await waitForRuns(agentLoop, 2);
      expect(agentLoop.runs[0]?.session.state.contextManager.messages()).toEqual([]);
      expect(await Effect.runPromise(manager.cleanupSession("sesn_1", threadControl("sesn_1")))).toEqual({ ok: true, sessionId: "sesn_1", cleaned: false });
      agentLoop.runs[1]?.release();
    });
  });

  test("AgentLoop Effect failure clears hot state, drops pending wake, and releases capacity", async () => {
    const agentLoop = makeControlledCrashAgentLoop("fail");
    const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

    await withSessionManager(layer, async (manager) => {
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_fail", "rin_fail_initial")))).toEqual({
        ok: true,
        sessionId: "sesn_fail",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForCrashRuns(agentLoop, 1);
      agentLoop.runs[0]?.session.state.contextManager.appendMessage({
        id: "msg_1",
        sessionId: "sesn_fail",
        role: "user",
        origin: "user",
        sequence: 0,
        status: "completed",
        createdAt: timestamp,
        parts: [],
      });
      expect(await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_fail", "rin_fail_follow")))).toEqual({
        ok: true,
        sessionId: "sesn_fail",
        created: false,
        started: false,
        pendingWake: true,
      });

      agentLoop.runs[0]?.releaseCrash();
      let replacement: TestRunStartResult | undefined;
      for (let attempt = 0; attempt < 100 && replacement === undefined; attempt += 1) {
        const result = await Effect.runPromise(manager.startTestRunThroughAcceptedInput("replacement_fail"));
        if (result.ok && result.started) {
          replacement = result;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      expect(replacement).toEqual({ ok: true, sessionId: "replacement_fail", created: true, started: true });
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_fail"))).toEqual({
        ok: false,
        sessionId: "sesn_fail",
        reason: "local_session_capacity_exceeded",
      });
      await waitForCrashRuns(agentLoop, 2);
      expect(agentLoop.runs[1]?.sessionId).toBe("replacement_fail");
      expect(agentLoop.runs[0]?.session.state.contextManager.messages()).toEqual([]);
      expect(await Effect.runPromise(manager.cleanupSession("sesn_fail", threadControl("sesn_fail")))).toEqual({ ok: true, sessionId: "sesn_fail", cleaned: false });
    });
  });

  test("AgentLoop Effect defect removes crashed entry and permits a later accepted-input run", async () => {
    const agentLoop = makeControlledCrashAgentLoop("die");
    const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

    await withSessionManager(layer, async (manager) => {
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_die"))).toEqual({
        ok: true,
        sessionId: "sesn_die",
        created: true,
        started: true,
      });
      await waitForCrashRuns(agentLoop, 1);
      const firstSession = agentLoop.runs[0]?.session;
      firstSession?.state.contextManager.appendMessage({
        id: "msg_1",
        sessionId: "sesn_die",
        role: "user",
        origin: "user",
        sequence: 0,
        status: "completed",
        createdAt: timestamp,
        parts: [],
      });
      agentLoop.runs[0]?.releaseCrash();

      for (let attempt = 0; attempt < 100 && agentLoop.runs.length === 1; attempt += 1) {
        const result = await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_die"));
        if (result.ok && result.started) {
          expect(result).toEqual({
            ok: true,
            sessionId: "sesn_die",
            created: true,
            started: true,
          });
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }

      await waitForCrashRuns(agentLoop, 2);
      expect(agentLoop.runs[1]?.session).not.toBe(firstSession);
      expect(agentLoop.runs[1]?.session.state.contextManager.messages()).toEqual([]);
    });
  });

  test("AgentLoop defect retains the live thread until durable closeout finishes", async () => {
    let closeoutStartedResolve: () => void = () => {};
    let closeoutReleaseResolve: () => void = () => {};
    const closeoutStarted = new Promise<void>((resolve) => {
      closeoutStartedResolve = resolve;
    });
    const closeoutRelease = new Promise<void>((resolve) => {
      closeoutReleaseResolve = resolve;
    });
    const agentLoop = makeControlledCrashAgentLoop("die", {
      closeFailedRun: () => Effect.promise(async () => {
        closeoutStartedResolve();
        await closeoutRelease;
        return { type: "landed" as const };
      }),
    });
    const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

    await withSessionManager(layer, async (manager) => {
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_closeout_fence"))).toMatchObject({
        ok: true,
        started: true,
      });
      await waitForCrashRuns(agentLoop, 1);
      agentLoop.runs[0]?.releaseCrash();
      await closeoutStarted;

      expect(await Effect.runPromise(manager.inspectThread(threadControl("sesn_closeout_fence")))).toMatchObject({
        observed: true,
      });
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_replacement_before_closeout"))).toEqual({
        ok: false,
        sessionId: "sesn_replacement_before_closeout",
        reason: "local_session_capacity_exceeded",
      });
      expect(await Effect.runPromise(
        manager.cleanupSession("sesn_closeout_fence", threadControl("sesn_closeout_fence")),
      )).toEqual({
        ok: false,
        sessionId: "sesn_closeout_fence",
        reason: "session_busy",
      });

      closeoutReleaseResolve();
      await waitForCondition(async () => {
        const snapshot = await Effect.runPromise(manager.inspectThread(threadControl("sesn_closeout_fence")));
        return snapshot.ok && !snapshot.observed;
      }, "failed-run release after durable closeout");
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_replacement_after_closeout"))).toMatchObject({
        ok: true,
        started: true,
      });
      await waitForCrashRuns(agentLoop, 2);
    });
  });

  test("failed-run closeout retries whole-envelope attempts with exponential backoff before release", async () => {
    let attempts = 0;
    const sleeps: number[] = [];
    const events: SessionManager.RuntimeCloseoutEvent[] = [];
    const agentLoop = makeControlledCrashAgentLoop("die", {
      closeFailedRun: () => Effect.sync(() => {
        attempts += 1;
        return attempts < 3
          ? {
              type: "retry" as const,
              error: normalizeSessionEventWriterError({ code: "unavailable" }),
            }
          : { type: "landed" as const };
      }),
    });
    const layer = sessionManagerLayer(agentLoop, {
      maxLocalSessions: 1,
      closeoutMonotonicMs: () => 1_000,
      closeoutSleep: async (durationMs) => {
        sleeps.push(durationMs);
        return true;
      },
      recordCloseoutEvent: (event) => events.push(event),
    });

    await withSessionManager(layer, async (manager) => {
      await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_closeout_retry"));
      await waitForCrashRuns(agentLoop, 1);
      agentLoop.runs[0]?.releaseCrash();
      await waitForCondition(async () => {
        const snapshot = await Effect.runPromise(manager.inspectThread(threadControl("sesn_closeout_retry")));
        return snapshot.ok && !snapshot.observed;
      }, "whole-envelope retry closeout");
    });

    expect(attempts).toBe(3);
    expect(sleeps).toEqual([
      SessionManager.CloseoutRetryInitialBackoffMs,
      SessionManager.CloseoutRetryInitialBackoffMs * 2,
    ]);
    expect(events).toEqual([]);
  });

  test("pod-wide closeout observations count only stalled entries across alarm, recovery, and failure", async () => {
    let now = 0;
    let ledgerAvailable = false;
    let attempts = 0;
    const sleepers: Array<() => void> = [];
    const events: SessionManager.RuntimeCloseoutEvent[] = [];
    const agentLoop = makeControlledCrashAgentLoop("die", {
      closeFailedRun: () => Effect.sync(() => {
        attempts += 1;
        return ledgerAvailable
          ? { type: "landed" as const }
          : {
              type: "retry" as const,
              error: normalizeSessionEventWriterError({ code: "unavailable" }),
            };
      }),
    });
    const layer = sessionManagerLayer(agentLoop, {
      maxLocalSessions: 2,
      closeoutMonotonicMs: () => now,
      closeoutSleep: async (_durationMs, signal) =>
        await new Promise<boolean>((resolve) => {
          if (signal.aborted) {
            resolve(false);
            return;
          }
          const release = () => resolve(true);
          sleepers.push(release);
          signal.addEventListener("abort", () => resolve(false), { once: true });
        }),
      recordCloseoutEvent: (event) => {
        events.push(event);
        if (event.event === "runtime_closeout_recovered") {
          throw new Error("recovered sink failed");
        }
      },
    });

    await withSessionManager(layer, async (manager) => {
      await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_closeout_stall_a"));
      await waitForCrashRuns(agentLoop, 1);
      const joinedA = Effect.runPromise(manager.waitThread(
        threadControl("sesn_closeout_stall_a"),
        undefined,
      ));
      agentLoop.runs[0]?.releaseCrash();
      await waitForCondition(() => Promise.resolve(sleepers.length === 1), "old closeout backoff");

      now = SessionManager.CloseoutStalledAlarmThresholdMs - 1;
      await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_closeout_stall_b"));
      await waitForCrashRuns(agentLoop, 2);
      const joinedB = Effect.runPromise(manager.waitThread(
        threadControl("sesn_closeout_stall_b"),
        undefined,
      ));
      agentLoop.runs[1]?.releaseCrash();
      await waitForCondition(() => Promise.resolve(sleepers.length === 2), "fresh closeout backoff");

      now = SessionManager.CloseoutStalledAlarmThresholdMs;
      sleepers.shift()?.();
      await waitForCondition(
        () => Promise.resolve(events.some((event) => event.event === "runtime_closeout_stalled") && sleepers.length === 2),
        "stalled closeout alarm",
      );
      expect(events).toEqual([{
        event: "runtime_closeout_stalled",
        activeCloseouts: 1,
      }]);
      sleepers.shift()?.();
      await waitForCondition(() => Promise.resolve(sleepers.length === 2), "old and fresh retry backoffs");

      ledgerAvailable = true;
      sleepers.splice(0).forEach((release) => release());
      await waitForCondition(async () => {
        const [left, right] = await Promise.all([
          Effect.runPromise(manager.inspectThread(threadControl("sesn_closeout_stall_a"))),
          Effect.runPromise(manager.inspectThread(threadControl("sesn_closeout_stall_b"))),
        ]);
        return left.ok && right.ok && !left.observed && !right.observed;
      }, "stalled closeout recovery");
      expect(await Promise.all([joinedA, joinedB])).toEqual([
        expect.objectContaining({ ok: true, observed: true, timedOut: false }),
        expect.objectContaining({ ok: true, observed: true, timedOut: false }),
      ]);
    });

    expect(attempts).toBeGreaterThanOrEqual(6);
    expect(events).toEqual([
      { event: "runtime_closeout_stalled", activeCloseouts: 1 },
      { event: "runtime_closeout_recovered", activeCloseouts: 0 },
    ]);
  });

  test("superseded and unrepairable closeouts release immediately with only the unrepairable record", async () => {
    for (const disposition of ["superseded", "unrepairable"] as const) {
      const events: SessionManager.RuntimeCloseoutEvent[] = [];
      const agentLoop = makeControlledCrashAgentLoop("die", {
        closeFailedRun: () => Effect.succeed({
          type: disposition,
          error: normalizeSessionEventWriterError({
            code: disposition,
          }),
        }),
      });
      const layer = sessionManagerLayer(agentLoop, {
        maxLocalSessions: 1,
        recordCloseoutEvent: (event) => events.push(event),
      });

      await withSessionManager(layer, async (manager) => {
        const sessionId = `sesn_closeout_${disposition}`;
        await Effect.runPromise(manager.startTestRunThroughAcceptedInput(sessionId));
        await waitForCrashRuns(agentLoop, 1);
        agentLoop.runs[0]?.releaseCrash();
        await waitForCondition(async () => {
          const snapshot = await Effect.runPromise(manager.inspectThread(threadControl(sessionId)));
          return snapshot.ok && !snapshot.observed;
        }, `${disposition} closeout release`);
      });

      expect(events).toEqual(disposition === "unrepairable"
        ? [{
            event: "runtime_closeout_unrepairable",
            activeCloseouts: 1,
            errorCode: "unrepairable",
          }]
        : []);
    }
  });

  test("a throwing unrepairable observer cannot retain the failed run slot", async () => {
    const agentLoop = makeControlledCrashAgentLoop("die", {
      closeFailedRun: () => Effect.succeed({
        type: "unrepairable",
        error: normalizeSessionEventWriterError({ code: "ack_mismatch" }),
      }),
    });
    const layer = sessionManagerLayer(agentLoop, {
      maxLocalSessions: 1,
      recordCloseoutEvent: () => {
        throw new Error("closeout observer failed");
      },
    });

    await withSessionManager(layer, async (manager) => {
      await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_closeout_observer_defect"));
      await waitForCrashRuns(agentLoop, 1);
      const joined = Effect.runPromise(manager.waitThread(
        threadControl("sesn_closeout_observer_defect"),
        undefined,
      ));
      agentLoop.runs[0]?.releaseCrash();
      await waitForCondition(async () => {
        const snapshot = await Effect.runPromise(manager.inspectThread(threadControl("sesn_closeout_observer_defect")));
        return snapshot.ok && !snapshot.observed;
      }, "unrepairable closeout release after observer defect");
      expect(await joined).toMatchObject({ ok: true, observed: true, timedOut: false });
      expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_after_observer_defect"))).toMatchObject({
        ok: true,
        started: true,
      });
      await waitForCrashRuns(agentLoop, 2);
    });
  });

  test("shutdown races a parked failed-run backoff and releases inside the drain path", async () => {
    let backoffStartedResolve: () => void = () => {};
    const backoffStarted = new Promise<void>((resolve) => {
      backoffStartedResolve = resolve;
    });
    const agentLoop = makeControlledCrashAgentLoop("die", {
      closeFailedRun: () => Effect.succeed({
        type: "retry",
        error: normalizeSessionEventWriterError({ code: "unavailable" }),
      }),
    });
    const layer = sessionManagerLayer(agentLoop, {
      maxLocalSessions: 1,
      closeoutSleep: async (_durationMs, signal) =>
        await new Promise<boolean>((resolve) => {
          backoffStartedResolve();
          signal.addEventListener("abort", () => resolve(false), { once: true });
        }),
    });

    await withSessionManager(layer, async (manager) => {
      await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_closeout_shutdown"));
      await waitForCrashRuns(agentLoop, 1);
      agentLoop.runs[0]?.releaseCrash();
      await backoffStarted;
      await Effect.runPromise(manager.shutdownActiveRuns());
      expect(await Effect.runPromise(manager.inspectThread(threadControl("sesn_closeout_shutdown")))).toMatchObject({
        observed: false,
      });
    });
  });

  test("shutdown during a failed-run observation window releases after that window reports timeout", async () => {
    let observationStartedResolve: () => void = () => {};
    let observationWindowResolve: () => void = () => {};
    const observationStarted = new Promise<void>((resolve) => {
      observationStartedResolve = resolve;
    });
    const observationWindow = new Promise<void>((resolve) => {
      observationWindowResolve = resolve;
    });
    const agentLoop = makeControlledCrashAgentLoop("die", {
      closeFailedRun: () => Effect.promise(async () => {
        observationStartedResolve();
        await observationWindow;
        return {
          type: "retry" as const,
          error: normalizeSessionEventWriterError({ code: "timeout" }),
        };
      }),
    });

    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_closeout_shutdown_window"));
      await waitForCrashRuns(agentLoop, 1);
      agentLoop.runs[0]?.releaseCrash();
      await observationStarted;

      let shutdownSettled = false;
      const shutdown = Effect.runPromise(manager.shutdownActiveRuns()).finally(() => {
        shutdownSettled = true;
      });
      await Promise.resolve();
      expect(shutdownSettled).toBe(false);

      observationWindowResolve();
      await shutdown;
      expect(await Effect.runPromise(manager.inspectThread(
        threadControl("sesn_closeout_shutdown_window"),
      ))).toMatchObject({ observed: false });
    });
  });

  test("interrupt during failed-run closeout waits for the closeout disposition", async () => {
    let closeoutStartedResolve: () => void = () => {};
    let closeoutReleaseResolve: () => void = () => {};
    const closeoutStarted = new Promise<void>((resolve) => {
      closeoutStartedResolve = resolve;
    });
    const closeoutRelease = new Promise<void>((resolve) => {
      closeoutReleaseResolve = resolve;
    });
    const agentLoop = makeControlledCrashAgentLoop("die", {
      closeFailedRun: () => Effect.promise(async () => {
        closeoutStartedResolve();
        await closeoutRelease;
        return { type: "landed" as const };
      }),
    });

    await withSessionManager(sessionManagerLayer(agentLoop), async (manager) => {
      await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_closeout_interrupt"));
      await waitForCrashRuns(agentLoop, 1);
      agentLoop.runs[0]?.releaseCrash();
      await closeoutStarted;

      let interruptSettled = false;
      const interruptCommand = {
        ...threadControl("sesn_closeout_interrupt", "rin_closeout_interrupt"),
        sequenceTo: 1,
      };
      const interrupt = Effect.runPromise(manager.interruptControl(
        "sesn_closeout_interrupt",
        interruptCommand,
        testControlCommit(interruptCommand),
      )).finally(() => {
        interruptSettled = true;
      });
      await Promise.resolve();
      expect(interruptSettled).toBe(false);
      expect(await Effect.runPromise(manager.inspectThread(
        threadControl("sesn_closeout_interrupt", "rin_closeout_interrupt_inspect"),
      ))).toMatchObject({ observed: true });

      closeoutReleaseResolve();
      await expect(interrupt).resolves.toEqual({
        ok: false,
        sessionId: "sesn_closeout_interrupt",
        reason: "control_busy",
      });
      expect(await Effect.runPromise(manager.inspectThread(
        threadControl("sesn_closeout_interrupt", "rin_closeout_interrupt_after"),
      ))).toMatchObject({ observed: false });
    });
  });

  test("closing a child cancels and releases its entire hot descendant subtree", async () => {
    const runs: Session.Session[] = [];
    const interruptedThreadIds: string[] = [];
    const layer = sessionManagerLayer({
      layer: Layer.succeed(
        AgentLoop.Service,
        agentLoopService({
          run: (session) => Effect.sync(() => {
            runs.push(session);
          }).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() => Effect.sync(() => {
              interruptedThreadIds.push(session.identity.sessionThreadId);
            })),
          ),
        }),
      ),
    });
    const sessionId = "sesn_close_tree";
    const childId = "thrd_close_tree_child";
    const grandchildId = "thrd_close_tree_grandchild";

    await withSessionManager(layer, async (manager) => {
      const threads = [
        {
          id: childId,
          runtimeInputId: "rin_close_tree_child",
          metadata: {
            parentThreadId: "thrd_close_tree_main",
            role: "subagent" as const,
            visibility: "public" as const,
            taskName: "child",
            agentType: "worker" as const,
            status: "idle" as const,
          },
        },
        {
          id: grandchildId,
          runtimeInputId: "rin_close_tree_grandchild",
          metadata: {
            parentThreadId: childId,
            role: "subagent" as const,
            visibility: "public" as const,
            taskName: "grandchild",
            agentType: "worker" as const,
            status: "idle" as const,
          },
        },
      ];
      for (const thread of threads) {
        expect(await Effect.runPromise(manager.preloadThread({
          ...threadControl(sessionId, `rin_preload_${thread.id}`, thread.id),
          runtimeBindingToken: "runtime-binding-token",
          coldCoverage: coldCoverage(),
          messages: [],
          thread: thread.metadata,
        }))).toMatchObject({ ok: true, applied: true });
        expect(await Effect.runPromise(manager.acceptInput(acceptedInput(sessionId, thread.runtimeInputId, thread.id)))).toMatchObject({
          ok: true,
          started: true,
        });
      }
      await waitForCondition(() => runs.length === 2, "child subtree runs");

      expect(await Effect.runPromise(manager.markThreadClosed(threadControl(sessionId, "rin_close_tree", childId)))).toEqual({
        ok: true,
        sessionId,
        sessionThreadId: childId,
        applied: true,
        runExitOutcome: "interrupt_applied",
      });
      expect(await Effect.runPromise(manager.inspectThread(threadControl(sessionId, "rin_close_tree_child_inspect", childId)))).toMatchObject({
        observed: false,
      });
      expect(await Effect.runPromise(manager.inspectThread(threadControl(sessionId, "rin_close_tree_grandchild_inspect", grandchildId)))).toMatchObject({
        observed: false,
      });
      expect(interruptedThreadIds.sort()).toEqual([childId, grandchildId].sort());
    });
  });

  test("AgentLoop rejected promise removes crashed entry without exposing hostile rejection text", async () => {
    const agentLoop = makeControlledCrashAgentLoop("reject");
    const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

    await withSessionManager(layer, async (manager) => {
      const first = await Effect.runPromise(manager.startTestRunThroughAcceptedInput("sesn_reject"));
      expect(first).toEqual({
        ok: true,
        sessionId: "sesn_reject",
        created: true,
        started: true,
      });
      await waitForCrashRuns(agentLoop, 1);
      agentLoop.runs[0]?.releaseCrash();

      let replacement: TestRunStartResult | undefined;
      for (let attempt = 0; attempt < 100 && replacement === undefined; attempt += 1) {
        const result = await Effect.runPromise(manager.startTestRunThroughAcceptedInput("replacement_reject"));
        if (result.ok && result.started) {
          replacement = result;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }

      expect(replacement).toEqual({
        ok: true,
        sessionId: "replacement_reject",
        created: true,
        started: true,
      });
      expectNoHostileFragments(first);
      expectNoHostileFragments(replacement);
      expectNoHostileFragments(await Effect.runPromise(manager.cleanupSession("sesn_reject", threadControl("sesn_reject"))));
      await waitForCrashRuns(agentLoop, 2);
    });
  });

  test("same-session accept after fatal discard starts a fresh run instead of queuing on the old entry", async () => {
    for (const reason of ["persistence_failed", "event_write_failed", "crashed"] as const) {
      const agentLoop = makeControlledAgentLoop();
      const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

      await withSessionManager(layer, async (manager) => {
        expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput(`accept_${reason}`))).toEqual({
          ok: true,
          sessionId: `accept_${reason}`,
          created: true,
          started: true,
        });
        await waitForRuns(agentLoop, 1);
        const fatalSession = agentLoop.runs[0]?.session;
        agentLoop.runs[0]?.release(fatalRunResult(reason));

        let accepted: SessionManager.AcceptInputResult | undefined;
        for (let attempt = 0; attempt < 100 && accepted === undefined; attempt += 1) {
          const result = await Effect.runPromise(manager.acceptInput(acceptedInput(`accept_${reason}`)));
          if (result.ok && result.started) {
            accepted = result;
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 1));
        }
        expect(accepted).toEqual({ ok: true, sessionId: `accept_${reason}`, created: true, started: true, pendingWake: false });
        await waitForRuns(agentLoop, 2);
        expect(agentLoop.runs[1]?.session).not.toBe(fatalSession);
        agentLoop.runs[1]?.release();
      });
    }
  });

  test("replacement session capacity is available after fatal discard", async () => {
    for (const reason of ["persistence_failed", "event_write_failed", "crashed"] as const) {
      const agentLoop = makeControlledAgentLoop();
      const layer = sessionManagerLayer(agentLoop, { maxLocalSessions: 1 });

      await withSessionManager(layer, async (manager) => {
        expect(await Effect.runPromise(manager.startTestRunThroughAcceptedInput(`old_${reason}`))).toEqual({
          ok: true,
          sessionId: `old_${reason}`,
          created: true,
          started: true,
        });
        await waitForRuns(agentLoop, 1);
        agentLoop.runs[0]?.release(fatalRunResult(reason));

        let replacement: TestRunStartResult | undefined;
        for (let attempt = 0; attempt < 100 && replacement === undefined; attempt += 1) {
          const result = await Effect.runPromise(manager.startTestRunThroughAcceptedInput(`replacement_${reason}`));
          if (result.ok && result.started) {
            replacement = result;
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 1));
        }
        expect(replacement).toEqual({ ok: true, sessionId: `replacement_${reason}`, created: true, started: true });
        await waitForRuns(agentLoop, 2);

        const joined = Effect.runPromise(manager.startTestRunThroughAcceptedInput(`replacement_${reason}`));
        await new Promise((resolve) => setTimeout(resolve, 5));
        expect(agentLoop.runs).toHaveLength(2);
        expect(await Effect.runPromise(manager.cleanupSession(`old_${reason}`, threadControl(`old_${reason}`)))).toEqual({
          ok: true,
          sessionId: `old_${reason}`,
          cleaned: false,
        });
        agentLoop.runs[1]?.release();
        expect(await joined).toEqual({
          ok: true,
          sessionId: `replacement_${reason}`,
          created: false,
          started: false,
        });
      });
    }
  });

  test("manager layer exposes only the contract command surface", async () => {
    const agentLoop = makeControlledAgentLoop();
    const keys = await withSessionManager(
      sessionManagerLayer(agentLoop),
      async (manager) => Object.keys(manager).sort(),
    );

    expect(keys).toEqual([
      "acceptInput",
      "applyRuntimeConfigPatch",
      "cleanupSession",
      "commitTaskNotification",
      "ensureThreadInstalled",
      "inspectReviewerExecution",
      "inspectThread",
      "interruptControl",
      "interruptReviewerExecution",
      "interruptThread",
      "markAgentMailPulled",
      "markThreadActive",
      "markThreadClosed",
      "preloadThread",
      "resolveToolConfirmation",
      "shutdownActiveRuns",
      "waitReviewerExecution",
      "waitThread",
    ]);
  });
});
