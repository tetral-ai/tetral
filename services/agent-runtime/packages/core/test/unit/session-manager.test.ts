import {
	describe,
	expect,
	test } from "bun:test";
import { Context,
	Effect,
	Exit,
	Fiber,
	Layer,
	Scope } from "effect";
import {
	normalizeRuntimeFailure,
	normalizeSessionEventWriterError,
	RuntimeContextEntrySchema,
} from "../../src/contracts/runtime.js";
import type {
	RuntimeHotStateMetrics,
	RuntimeMetricsSink,
} from "../../src/runtime/metrics.js";
import * as SessionManager from "../../src/session/session-manager.js";
import type { FailedRunCloseoutResult } from "../../src/thread-loop/closeout.js";
import * as ThreadLoop from "../../src/thread-loop/thread-loop.js";
import type * as ThreadRuntime from "../../src/thread-loop/thread-runtime.js";
import type {
	RuntimeAcceptedInputState,
} from "../../src/thread-loop/input/accepted-input.js";
import type {
	RuntimeAcceptedThreadMetadataState,
} from "../../src/thread-loop/input/accepted-input.js";
import type {
	RuntimeControlInputDeclaration,
} from "../../src/thread-loop/input/control-input.js";
import type {
	RuntimeConfigPatchState,
} from "../../src/thread-loop/input/preload.js";

const timestamp = "2026-06-14T00:00:00.000Z";

function testControlCommit(
	_scope: { readonly sessionId: string },
	inputKind: "interrupt" | "tool_confirmation" = "interrupt",
) {
	return async (declaration: RuntimeControlInputDeclaration) =>
		buildRuntimeControlCommitResult(inputKind, declaration);
}

function buildRuntimeControlCommitResult(
	inputKind: "interrupt" | "tool_confirmation",
	declaration: RuntimeControlInputDeclaration,
) {
	if (declaration.inputKind !== inputKind) {
		return {
			ok: false as const,
			retryable: false,
			errorCode: "input_kind_mismatch",
		};
	}
	return {
		ok: true as const,
		type: "committed" as const,
		assignedContextSequences: inputKind === "tool_confirmation" ? [1] : [],
		pendingAttachments: [],
		interruptToolResults: [],
	};
}

function pendingApprovalToolPart(_sessionId: string, toolUseEventId: string) {
	return {
		type: "tool" as const,
		modelToolCallId: `call_${toolUseEventId}`,
		toolName: "Write",
		toolUseEventId,
		state: { status: "pending" as const },
	};
}

function pendingApprovalAssistantEntry(
	sessionId: string,
	toolUseEventId: string,
	messageSequence = 1,
) {
	const toolPart = pendingApprovalToolPart(sessionId, toolUseEventId);
	return RuntimeContextEntrySchema.parse({
		messageSequence,
		contextKind: "assistant",
		parts: [
			{
				type: "tool_call",
				modelToolCallId: toolPart.modelToolCallId,
				toolName: toolPart.toolName,
				canonicalInput: {},
			},
		],
	});
}

function coldUserEntry(_sessionId: string) {
	return RuntimeContextEntrySchema.parse({
		messageSequence: 1,
		contextKind: "user",
		parts: [{ type: "text", text: "test input" }],
	});
}

function reviewerDecisionEntry(
	_sessionId: string,
	_id: string,
	rationale: string,
) {
	return RuntimeContextEntrySchema.parse({
		messageSequence: _id.endsWith("target_b") ? 2 : 1,
		contextKind: "assistant",
		parts: [
			{
				type: "text",
				text: JSON.stringify({
					risk_level: "low",
					user_authorization: "high",
					outcome: "allow",
					rationale,
				}),
			},
		],
	});
}

function acceptedInput(
	sessionId: string,
	runtimeInputId = `rin_${sessionId}`,
	sessionThreadId = `thrd_${sessionId}`,
): Extract<RuntimeAcceptedInputState, { readonly kind: "messages" }> {
	return {
		workspaceId: "wksp_test",
		sessionId,
		sessionThreadId,
		bindingId: `bind_${sessionId}`,
		bindingGeneration: 1,
		targetPodUid: `pod_${sessionId}`,
		runtimeInputId,
		inputOrder: 1,
		kind: "messages",
		contentJson: JSON.stringify({
			messages: [{ parts: [{ type: "text", text: "test input" }] }],
		}),
	};
}

function approvalReviewInput(
	sessionId: string,
	runtimeInputId: string,
	sessionThreadId: string,
	parentThreadId: string,
): RuntimeAcceptedInputState {
	return {
		workspaceId: "wksp_test",
		sessionId,
		sessionThreadId,
		bindingId: `bind_${sessionId}`,
		bindingGeneration: 1,
		targetPodUid: `pod_${sessionId}`,
		runtimeInputId,
		inputOrder: 1,
		kind: "approval_review",
		reviewId: `arvw_${runtimeInputId}`,
		parentThreadId,
		targetModelToolCallId: `tool_call_${runtimeInputId}`,
		targetToolName: "Write",
		promptText: [],
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
): Extract<
	RuntimeAcceptedInputState,
	{ readonly kind: "inter_agent_message" }
> {
	return {
		workspaceId: "wksp_test",
		sessionId,
		sessionThreadId,
		bindingId: `bind_${sessionId}`,
		bindingGeneration: 1,
		targetPodUid: `pod_${sessionId}`,
		runtimeInputId,
		kind: "inter_agent_message",
		deliveryId: runtimeInputId.replace("agent_mail:", ""),
		content: `completion from ${sourceThreadId}`,
		thread,
	};
}

function threadControl(
	sessionId: string,
	runtimeInputId = `rin_control_${sessionId}`,
	sessionThreadId = `thrd_${sessionId}`,
): SessionManager.RuntimeInterruptControlCommand {
	return {
		workspaceId: "wksp_test",
		sessionId,
		sessionThreadId,
		bindingId: `bind_${sessionId}`,
		bindingGeneration: 1,
		targetPodUid: `pod_${sessionId}`,
		runtimeInputId,
		origin: "user",
		interruptLeaseRef: {
			jobId: `qjob_${runtimeInputId}`,
			leaseToken: `lease_${runtimeInputId}`,
			partitionKey: `session:wksp_test:${sessionId}`,
			dedupeKey: `runtime_input:wksp_test:${sessionId}:${runtimeInputId}`,
		},
	};
}

function runtimeConfigScope(sessionId: string, configIdentity: string) {
	return {
		workspaceId: "wksp_test",
		sessionId,
		bindingId: `bind_${sessionId}`,
		bindingGeneration: 1,
		targetPodUid: `pod_${sessionId}`,
		configIdentity,
	};
}

function cleanupControl(
	sessionId: string,
	cleanupOperationId = `cleanup_${sessionId}`,
): SessionManager.RuntimeCleanupSessionCommand {
	return {
		workspaceId: "wksp_test",
		sessionId,
		bindingId: `bind_${sessionId}`,
		bindingGeneration: 1,
		targetPodUid: `pod_${sessionId}`,
		cleanupOperationId,
	};
}

interface RunRecord {
	readonly sessionId: string;
	readonly session: ThreadRuntime.ThreadRuntime;
	readonly args: readonly unknown[];
	readonly release: (result?: ThreadLoop.ThreadLoopRunResult) => void;
}

interface CrashRunRecord {
	readonly sessionId: string;
	readonly session: ThreadRuntime.ThreadRuntime;
	readonly releaseCrash: () => void;
}

interface ControlledThreadLoop {
	readonly runs: RunRecord[];
	readonly layer: Layer.Layer<ThreadLoop.Service>;
}

interface ControlledCrashThreadLoop {
	readonly runs: CrashRunRecord[];
	readonly layer: Layer.Layer<ThreadLoop.Service>;
}

interface InterruptRecordingThreadLoop {
	readonly runs: Array<{
		readonly sessionId: string;
		readonly session: ThreadRuntime.ThreadRuntime;
	}>;
	readonly interruptions: Array<{
		readonly sessionId: string;
		readonly runtimeShutdownRequested: boolean;
	}>;
	readonly layer: Layer.Layer<ThreadLoop.Service>;
}

interface InterruptCleanupThreadLoop {
	readonly runs: RunRecord[];
	readonly cleanupStarted: Promise<void>;
	readonly releaseCleanup: () => void;
	readonly observedInterruptLeaseRefs: Array<
		ReturnType<ThreadLoop.ThreadLoopRunCustody["interruptLeaseRef"]>
	>;
	readonly layer: Layer.Layer<ThreadLoop.Service>;
}

interface ReviewerInterruptCleanupThreadLoop extends ControlledThreadLoop {
	readonly reviewerCleanupStarted: Promise<void>;
	readonly releaseReviewerCleanup: () => void;
}

interface ReviewerGenerationThreadLoop {
	readonly observedReviewerInputIds: string[];
	readonly targetAStarted: Promise<void>;
	readonly cleanupStarted: Promise<void>;
	readonly releaseCleanup: () => void;
	readonly targetBStarted: Promise<void>;
	readonly releaseTargetB: () => void;
	readonly layer: Layer.Layer<ThreadLoop.Service>;
}

interface FollowUpCleanupThreadLoop {
	readonly runs: RunRecord[];
	readonly followUpCleanupStarted: Promise<void>;
	readonly releaseFollowUpCleanup: () => void;
	readonly layer: Layer.Layer<ThreadLoop.Service>;
}

function threadLoopService(
	overrides: Pick<ThreadLoop.Interface, "run"> & Partial<ThreadLoop.Interface>,
): ThreadLoop.Interface {
	return ThreadLoop.Service.of({
		closeFailedRun: () =>
			Effect.succeed({ type: "landed", disposition: "terminal" }),
		closeRecoveredOpenRequestForInterrupt: () =>
			Effect.succeed({ type: "interrupted" }),
		settleIdleInterrupt: ThreadLoop.settleIdleInterrupt,
		settleToolConfirmation: ThreadLoop.settleToolConfirmation,
		seedRuntimeModel: () => {},
		installLoadedPendingToolUses: () => Effect.succeed({ ok: true }),
		installLoadedSandboxExecutions: () => Effect.succeed({ ok: true }),
		...overrides,
	});
}

function makeControlledThreadLoop(
	overrides: Partial<ThreadLoop.Interface> = {},
): ControlledThreadLoop {
	const runs: RunRecord[] = [];
	const layer = Layer.succeed(
		ThreadLoop.Service,
		threadLoopService({
			run: (...args: readonly [ThreadRuntime.ThreadRuntime, ...unknown[]]) =>
				Effect.promise(
					() =>
						new Promise<ThreadLoop.ThreadLoopRunResult>((resolve) => {
							const session = args[0];
							runs.push({
								sessionId: session.sessionId,
								session,
								args,
								release: (result) => {
									const acceptedInput = session.state.peekAcceptedInput();
									if (acceptedInput !== undefined) {
										session.state.acknowledgeAcceptedInput(
											acceptedInput.runtimeInputId,
										);
									}
									resolve(
										result ?? {
											type: "completed" as const,
											modelMessageCount: 0,
										},
									);
								},
							});
						}),
				),
			...overrides,
		}),
	);
	return { runs, layer };
}

function makeInputConsumingControlledThreadLoop(): ControlledThreadLoop {
	const runs: RunRecord[] = [];
	const layer = Layer.succeed(
		ThreadLoop.Service,
		threadLoopService({
			run: (session) =>
				Effect.promise(
					() =>
						new Promise<ThreadLoop.ThreadLoopRunResult>((resolve) => {
							let acceptedInput = session.state.peekAcceptedInput();
							while (acceptedInput !== undefined) {
								session.state.acknowledgeAcceptedInput(
									acceptedInput.runtimeInputId,
								);
								acceptedInput = session.state.peekAcceptedInput();
							}
							runs.push({
								sessionId: session.sessionId,
								session,
								args: [session],
								release: (result) =>
									resolve(
										result ?? {
											type: "completed" as const,
											modelMessageCount: 0,
										},
									),
							});
						}),
				),
		}),
	);
	return { runs, layer };
}

function makeInterruptRecordingThreadLoop(): InterruptRecordingThreadLoop {
	const runs: Array<{
		readonly sessionId: string;
		readonly session: ThreadRuntime.ThreadRuntime;
	}> = [];
	const interruptions: InterruptRecordingThreadLoop["interruptions"] = [];
	const layer = Layer.succeed(
		ThreadLoop.Service,
		threadLoopService({
			run: (session) =>
				Effect.sync(() => {
					runs.push({ sessionId: session.sessionId, session });
				}).pipe(
					Effect.andThen(Effect.never),
					Effect.onInterrupt(() =>
						Effect.promise(async () => {
							interruptions.push({
								sessionId: session.sessionId,
								runtimeShutdownRequested:
									session.state.runtimeShutdownRequested(),
							});
							if (session.state.userInterruptRequested()) {
								const runtimeInputId =
									session.state.userInterruptCommand()?.runtimeInputId;
								await session.state.commitUserInterruptInput({
									inputKind: "interrupt",
								});
								if (runtimeInputId !== undefined) {
									session.state.completeUserInterrupt(runtimeInputId);
								}
							}
						}),
					),
				),
		}),
	);
	return { runs, interruptions, layer };
}

function makeInterruptCleanupThreadLoop(): InterruptCleanupThreadLoop {
	const runs: RunRecord[] = [];
	const observedInterruptLeaseRefs: Array<
		ReturnType<ThreadLoop.ThreadLoopRunCustody["interruptLeaseRef"]>
	> = [];
	let cleanupStartedResolve: () => void = () => {};
	let releaseCleanupResolve: () => void = () => {};
	const cleanupStarted = new Promise<void>((resolve) => {
		cleanupStartedResolve = resolve;
	});
	const cleanupReleased = new Promise<void>((resolve) => {
		releaseCleanupResolve = resolve;
	});
	const layer = Layer.succeed(
		ThreadLoop.Service,
		threadLoopService({
			run: (session, custody) => {
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
						Effect.onInterrupt(() =>
							Effect.promise(async () => {
								cleanupStartedResolve();
								await cleanupReleased;
								if (session.state.userInterruptRequested()) {
									await session.state.commitUserInterruptInput({
										inputKind: "interrupt",
									});
									const runtimeInputId =
										session.state.userInterruptCommand()?.runtimeInputId;
									if (runtimeInputId !== undefined) {
										observedInterruptLeaseRefs.push(
											custody.interruptLeaseRef(runtimeInputId),
										);
										session.state.completeUserInterrupt(runtimeInputId);
									}
								}
							}),
						),
					);
				}
				return Effect.promise(
					() =>
						new Promise<ThreadLoop.ThreadLoopRunResult>((resolve) => {
							runs.push({
								sessionId: session.sessionId,
								session,
								args: [session],
								release: (value) =>
									resolve(value ?? { type: "completed", modelMessageCount: 0 }),
							});
						}),
				);
			},
		}),
	);
	return {
		runs,
		cleanupStarted,
		releaseCleanup: releaseCleanupResolve,
		observedInterruptLeaseRefs,
		layer,
	};
}

function makeReviewerInterruptCleanupThreadLoop(
	reviewerThreadId: string,
): ReviewerInterruptCleanupThreadLoop {
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
		ThreadLoop.Service,
		threadLoopService({
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
						Effect.onInterrupt(() =>
							Effect.promise(async () => {
								reviewerCleanupStartedResolve();
								await reviewerCleanupReleased;
							}),
						),
					);
				}
				return Effect.promise(
					() =>
						new Promise<ThreadLoop.ThreadLoopRunResult>((resolve) => {
							runs.push({
								sessionId: session.sessionId,
								session,
								args: [session],
								release: (result) =>
									resolve(
										result ?? { type: "completed", modelMessageCount: 0 },
									),
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

function makeReviewerGenerationThreadLoop(): ReviewerGenerationThreadLoop {
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
		ThreadLoop.Service,
		threadLoopService({
			run: (session) => {
				const currentRun = runIndex;
				runIndex += 1;
				const acceptedInput = session.state.peekAcceptedInput();
				if (acceptedInput?.kind === "approval_review") {
					observedReviewerInputIds.push(acceptedInput.runtimeInputId);
				}
				const recordDecision = Effect.sync(() => {
					if (acceptedInput !== undefined) {
						session.state.acknowledgeAcceptedInput(
							acceptedInput.runtimeInputId,
						);
					}
					session.state.contextManager.appendEntry(
						reviewerDecisionEntry(
							session.sessionId,
							currentRun === 0 ? "msg_target_a" : "msg_target_b",
							currentRun === 0 ? "target A verdict" : "target B verdict",
						),
					);
				});
				if (currentRun > 0) {
					return recordDecision.pipe(
						Effect.andThen(
							Effect.promise(async () => {
								targetBStartedResolve();
								await targetBReleased;
								return { type: "completed" as const, modelMessageCount: 1 };
							}),
						),
					);
				}
				return recordDecision.pipe(
					Effect.andThen(Effect.sync(targetAStartedResolve)),
					Effect.andThen(Effect.never),
					Effect.onInterrupt(() =>
						Effect.promise(async () => {
							cleanupStartedResolve();
							await cleanupReleased;
						}),
					),
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

function makeFollowUpCleanupThreadLoop(): FollowUpCleanupThreadLoop {
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
		ThreadLoop.Service,
		threadLoopService({
			run: (session) => {
				const runIndex = runs.length;
				const run = Effect.promise(
					() =>
						new Promise<ThreadLoop.ThreadLoopRunResult>((resolve) => {
							runs.push({
								sessionId: session.sessionId,
								session,
								args: [session],
								release: (result) => {
									const acceptedInput = session.state.peekAcceptedInput();
									if (acceptedInput !== undefined) {
										session.state.acknowledgeAcceptedInput(
											acceptedInput.runtimeInputId,
										);
									}
									resolve(
										result ?? { type: "completed", modelMessageCount: 0 },
									);
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
	return {
		runs,
		followUpCleanupStarted,
		releaseFollowUpCleanup: releaseCleanupResolve,
		layer,
	};
}

function makeControlledCrashThreadLoop(
	mode: "fail" | "die" | "reject",
	overrides: Partial<ThreadLoop.Interface> = {},
): ControlledCrashThreadLoop {
	const runs: CrashRunRecord[] = [];
	const layer = Layer.succeed(
		ThreadLoop.Service,
		threadLoopService({
			run: (session) => {
				if (mode === "reject") {
					return Effect.promise(
						() =>
							new Promise<ThreadLoop.ThreadLoopRunResult>(
								(_resolve, reject) => {
									runs.push({
										sessionId: session.sessionId,
										session,
										releaseCrash: () => reject(new Error(hostileText)),
									});
								},
							),
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
				return release.pipe(
					Effect.flatMap(() => Effect.die(new Error("thread loop defect"))),
				);
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
	threadLoop: { readonly layer: Layer.Layer<ThreadLoop.Service> },
	options: {
		readonly maxLocalSessions?: number;
		readonly metrics?: RuntimeMetricsSink;
		readonly loadThreadContext?: SessionManager.LayerOptions["loadThreadContext"];
		readonly closeoutMonotonicMs?: SessionManager.LayerOptions["closeoutMonotonicMs"];
		readonly closeoutSleep?: SessionManager.LayerOptions["closeoutSleep"];
		readonly recordCloseoutEvent?: SessionManager.LayerOptions["recordCloseoutEvent"];
		readonly recordMCPManifestUpdate?: SessionManager.LayerOptions["recordMCPManifestUpdate"];
		readonly resolveMCPManifestEligibility?: SessionManager.LayerOptions["resolveMCPManifestEligibility"];
	} = {},
): Layer.Layer<SessionManager.Service> {
	return SessionManager.layer({
		maxLocalSessions: options.maxLocalSessions ?? 10,
		now: () => timestamp,
		...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
		...(options.loadThreadContext !== undefined
			? { loadThreadContext: options.loadThreadContext }
			: {}),
		...(options.closeoutMonotonicMs !== undefined
			? { closeoutMonotonicMs: options.closeoutMonotonicMs }
			: {}),
		...(options.closeoutSleep !== undefined
			? { closeoutSleep: options.closeoutSleep }
			: {}),
		...(options.recordCloseoutEvent !== undefined
			? { recordCloseoutEvent: options.recordCloseoutEvent }
			: {}),
		...(options.recordMCPManifestUpdate !== undefined
			? { recordMCPManifestUpdate: options.recordMCPManifestUpdate }
			: {}),
		...(options.resolveMCPManifestEligibility !== undefined
			? { resolveMCPManifestEligibility: options.resolveMCPManifestEligibility }
			: {}),
	}).pipe(Layer.provide(threadLoop.layer));
}

async function withSessionManager<T>(
	layer: Layer.Layer<SessionManager.Service>,
	useManager: (manager: TestSessionManager) => Promise<T>,
): Promise<T> {
	const { manager, scope } = await Effect.runPromise(
		Effect.gen(function* () {
			const layerScope = yield* Scope.make();
			const context = yield* Layer.buildWithScope(layer, layerScope);
			return {
				manager: Context.get(context, SessionManager.Service),
				scope: layerScope,
			};
		}),
	);
	try {
		return await useManager(testSessionManager(manager));
	} finally {
		await Effect.runPromise(Scope.close(scope, Exit.void));
	}
}

type TestRunStartResult =
	| {
			readonly ok: true;
			readonly sessionId: string;
			readonly created: boolean;
			readonly started: boolean;
	  }
	| {
			readonly ok: false;
			readonly sessionId: string;
			readonly reason: "local_session_capacity_exceeded";
	  };

type TestSessionManager = SessionManager.Interface & {
	readonly startTestRunThroughAcceptedInput: (
		sessionId: string,
	) => Effect.Effect<TestRunStartResult>;
};

let nextTestRunInput = 0;

function testSessionManager(
	manager: SessionManager.Interface,
): TestSessionManager {
	const startTestRunThroughAcceptedInput = (
		sessionId: string,
	): Effect.Effect<TestRunStartResult> =>
		Effect.promise(async () => {
			let joinedActiveRun = false;
			for (;;) {
				const inspected = await Effect.runPromise(
					manager.inspectThread(threadControl(sessionId)),
				);
				if (
					inspected.ok &&
					inspected.observed &&
					inspected.status === "running"
				) {
					joinedActiveRun = true;
					await new Promise((resolve) => setTimeout(resolve, 1));
					continue;
				}
				if (joinedActiveRun && inspected.ok && inspected.observed) {
					return { ok: true, sessionId, created: false, started: false };
				}
				nextTestRunInput += 1;
				const accepted = await Effect.runPromise(
					manager.acceptInput(
						acceptedInput(sessionId, `rin_test_run_${nextTestRunInput}`),
					),
				);
				if (!accepted.ok) {
					return {
						ok: false,
						sessionId,
						reason: "local_session_capacity_exceeded",
					};
				}
				return {
					ok: true,
					sessionId,
					created: accepted.created,
					started: accepted.started,
				};
			}
		});
	Object.defineProperty(manager, "startTestRunThroughAcceptedInput", {
		value: startTestRunThroughAcceptedInput,
		enumerable: false,
	});
	return manager as TestSessionManager;
}

async function waitForRuns(
	threadLoop: ControlledThreadLoop,
	count: number,
): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (threadLoop.runs.length >= count) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
	throw new Error(
		`expected ${count} ThreadLoop runs, observed ${threadLoop.runs.length}`,
	);
}

async function waitForCondition(
	predicate: () => boolean | Promise<boolean>,
	label: string,
): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (await predicate()) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
	throw new Error(`timed out waiting for ${label}`);
}

async function waitForThreadIdle(
	manager: SessionManager.Interface,
	sessionId: string,
	sessionThreadId: string,
): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		const result = await Effect.runPromise(
			manager.inspectThread(
				threadControl(sessionId, undefined, sessionThreadId),
			),
		);
		if (result.ok && result.observed && result.status === "idle") {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
	throw new Error(`expected ${sessionThreadId} to become idle`);
}

async function waitForCrashRuns(
	threadLoop: ControlledCrashThreadLoop,
	count: number,
): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (threadLoop.runs.length >= count) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
	throw new Error(
		`expected ${count} ThreadLoop crash runs, observed ${threadLoop.runs.length}`,
	);
}

async function waitForInterruptRecordingRuns(
	threadLoop: InterruptRecordingThreadLoop,
	count: number,
): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (threadLoop.runs.length >= count) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
	throw new Error(
		`expected ${count} ThreadLoop runs, observed ${threadLoop.runs.length}`,
	);
}

async function waitForIdleCleanup(
	manager: SessionManager.Interface,
	sessionId: string,
): Promise<SessionManager.CleanupSessionResult> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		const result = await Effect.runPromise(
			manager.cleanupSession(sessionId, cleanupControl(sessionId)),
		);
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

function fatalRunResult(
	reason:
		| "terminated"
		| "persistence_failed"
		| "event_write_failed"
		| "crashed",
): ThreadLoop.ThreadLoopRunResult {
	const runtimeFailure = reason === "terminated" || reason === "crashed";
	return {
		type: "failed",
		error: normalizeRuntimeFailure({
			type:
				reason === "persistence_failed"
					? "message-store"
					: reason === "event_write_failed"
						? "session-event-writer"
						: "runtime",
			code: runtimeFailure ? "runtime_invalid_sequence" : "unavailable",
			retryable: reason !== "terminated",
			fatal: reason === "terminated",
			...(runtimeFailure ? { reason: "runtime_contract_validation" } : {}),
		}),
		releaseSession: { reason },
	};
}

class RecordingRuntimeMetrics implements RuntimeMetricsSink {
	readonly hotStates: RuntimeHotStateMetrics[] = [];

	constructor(
		private readonly onHotState?: (snapshot: RuntimeHotStateMetrics) => void,
	) {}

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
		const threadLoop = makeReviewerGenerationThreadLoop();
		const targetBInput = approvalReviewInput(
			sessionId,
			"rin_target_b_reentry",
			reviewerThreadId,
			parentThreadId,
		);
		let managerRef: TestSessionManager | undefined;
		let reentryArmed = false;
		let reentryResult: Promise<SessionManager.AcceptInputResult> | undefined;
		const metrics = new RecordingRuntimeMetrics((snapshot) => {
			if (
				!reentryArmed ||
				snapshot.activeFibers !== 0 ||
				managerRef === undefined
			) {
				return;
			}
			reentryArmed = false;
			reentryResult = Effect.runPromise(managerRef.acceptInput(targetBInput));
		});

		await withSessionManager(
			sessionManagerLayer(threadLoop, { metrics }),
			async (manager) => {
				managerRef = manager;
				const targetA = await Effect.runPromise(
					manager.acceptInput(
						approvalReviewInput(
							sessionId,
							"rin_target_a_reentry",
							reviewerThreadId,
							parentThreadId,
						),
					),
				);
				if (!targetA.ok || targetA.reviewerExecutionToken === undefined) {
					throw new Error(
						"target A did not receive a reviewer execution token",
					);
				}
				await threadLoop.targetAStarted;
				const cancellation = Effect.runPromise(
					manager.interruptReviewerExecution(
						threadControl(
							sessionId,
							"rin_target_a_reentry_control",
							reviewerThreadId,
						),
						targetA.reviewerExecutionToken,
					),
				);
				await threadLoop.cleanupStarted;

				reentryArmed = true;
				threadLoop.releaseCleanup();
				await cancellation;
				await Effect.runPromise(
					manager.releaseReviewerExecution(
						threadControl(
							sessionId,
							"rin_target_a_reentry_control",
							reviewerThreadId,
						),
						targetA.reviewerExecutionToken,
					),
				);
				await waitForCondition(
					() => reentryResult !== undefined,
					"reviewer reentrant admission",
				);
				expect(await reentryResult).toMatchObject({ ok: true, started: true });
				await threadLoop.targetBStarted;
				expect(threadLoop.observedReviewerInputIds).toEqual([
					"rin_target_a_reentry",
					"rin_target_b_reentry",
				]);
				threadLoop.releaseTargetB();
			},
		);
	});

	test("correlates target A cancellation and target B review to distinct terminal run generations", async () => {
		const sessionId = "sesn_reviewer_generation";
		const reviewerThreadId = "thrd_reviewer_generation";
		const parentThreadId = "thrd_parent_generation";
		const threadLoop = makeReviewerGenerationThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				const targetAInput = approvalReviewInput(
					sessionId,
					"rin_target_a",
					reviewerThreadId,
					parentThreadId,
				);
				const targetA = await Effect.runPromise(
					manager.acceptInput(targetAInput),
				);
				expect(targetA).toMatchObject({ ok: true, started: true });
				if (!targetA.ok || targetA.reviewerExecutionToken === undefined) {
					throw new Error(
						"target A did not receive a reviewer execution token",
					);
				}
				await threadLoop.targetAStarted;

				const targetAControl = threadControl(
					sessionId,
					"rin_target_a_control",
					reviewerThreadId,
				);
				const cancellation = Effect.runPromise(
					manager.interruptReviewerExecution(
						targetAControl,
						targetA.reviewerExecutionToken,
					),
				);
				await threadLoop.cleanupStarted;

				const targetBWhileAIsLive = await Effect.runPromise(
					manager.acceptInput(
						approvalReviewInput(
							sessionId,
							"rin_target_b_early",
							reviewerThreadId,
							parentThreadId,
						),
					),
				);
				expect(targetBWhileAIsLive).toEqual({
					ok: false,
					sessionId,
					reason: "thread_busy",
				});
				expect(
					await Effect.runPromise(
						manager.inspectReviewerExecution(
							targetAControl,
							targetA.reviewerExecutionToken,
						),
					),
				).toMatchObject({ ok: false, reason: "thread_busy" });

				threadLoop.releaseCleanup();
				expect(await cancellation).toMatchObject({
					ok: true,
					terminal: true,
					applied: true,
				});
				expect(
					await Effect.runPromise(
						manager.releaseReviewerExecution(
							targetAControl,
							targetA.reviewerExecutionToken,
						),
					),
				).toMatchObject({ ok: true, terminal: true, applied: true });

				const targetBInput = approvalReviewInput(
					sessionId,
					"rin_target_b",
					reviewerThreadId,
					parentThreadId,
				);
				const targetB = await Effect.runPromise(
					manager.acceptInput(targetBInput),
				);
				expect(targetB).toMatchObject({ ok: true, started: true });
				if (!targetB.ok || targetB.reviewerExecutionToken === undefined) {
					throw new Error(
						"target B did not receive a reviewer execution token",
					);
				}
				expect(targetB.reviewerExecutionToken.runId).not.toBe(
					targetA.reviewerExecutionToken.runId,
				);

				const targetBControl = threadControl(
					sessionId,
					"rin_target_b_control",
					reviewerThreadId,
				);
				await threadLoop.targetBStarted;
				expect(threadLoop.observedReviewerInputIds).toEqual([
					"rin_target_a",
					"rin_target_b",
				]);
				expect(
					await Effect.runPromise(
						manager.waitReviewerExecution(
							targetBControl,
							targetA.reviewerExecutionToken,
							10,
						),
					),
				).toMatchObject({ ok: false, reason: "reviewer_execution_mismatch" });
				expect(
					await Effect.runPromise(
						manager.interruptReviewerExecution(
							targetBControl,
							targetA.reviewerExecutionToken,
						),
					),
				).toMatchObject({ ok: false, reason: "reviewer_execution_mismatch" });
				expect(
					await Effect.runPromise(
						manager.inspectReviewerExecution(
							targetBControl,
							targetB.reviewerExecutionToken,
						),
					),
				).toMatchObject({ ok: false, reason: "thread_busy" });
				threadLoop.releaseTargetB();
				expect(
					await Effect.runPromise(
						manager.waitReviewerExecution(
							targetBControl,
							targetB.reviewerExecutionToken,
							undefined,
						),
					),
				).toMatchObject({ ok: true, terminal: true, timedOut: false });
				const targetBSnapshot = await Effect.runPromise(
					manager.inspectReviewerExecution(
						targetBControl,
						targetB.reviewerExecutionToken,
					),
				);
				if (!targetBSnapshot.ok) {
					throw new Error("target B snapshot was unavailable");
				}
				expect(
					targetBSnapshot.entries.map((entry) => entry.messageSequence),
				).toEqual([1, 2]);
				expect(targetBSnapshot).toMatchObject({
					ok: true,
					observed: true,
				});
				expect(
					await Effect.runPromise(
						manager.inspectReviewerExecution(
							targetBControl,
							targetA.reviewerExecutionToken,
						),
					),
				).toMatchObject({ ok: false, reason: "reviewer_execution_mismatch" });
			},
		);
	});

	test("retains a durably idle failed reviewer until its exact token is evicted", async () => {
		const sessionId = "sesn_reviewer_failed_idle";
		const reviewerThreadId = "thrd_reviewer_failed_idle";
		const siblingThreadId = "thrd_reviewer_sibling";
		const parentThreadId = "thrd_reviewer_parent";
		let closeoutCalls = 0;
		const threadLoop = makeControlledThreadLoop({
			closeFailedRun: () =>
				Effect.sync(() => {
					closeoutCalls += 1;
					return {
						type: "landed" as const,
						disposition: "continuation" as const,
					};
				}),
		});
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl(
								sessionId,
								"rin_preload_sibling",
								siblingThreadId,
							),
							runtimeBindingToken: "runtime-binding-token",
							contextEntries: [],
							thread: {
								parentThreadId,
								role: "approval_reviewer",
								visibility: "internal",
								agentType: "approval_reviewer",
								status: "idle",
							},
						}),
					),
				).toMatchObject({ ok: true, applied: true });

				const accepted = await Effect.runPromise(
					manager.acceptInput(
						approvalReviewInput(
							sessionId,
							"rin_failed_idle",
							reviewerThreadId,
							parentThreadId,
						),
					),
				);
				if (!accepted.ok || accepted.reviewerExecutionToken === undefined) {
					throw new Error("failed reviewer did not receive an execution token");
				}
				await waitForRuns(threadLoop, 1);
				const requestFailure = fatalRunResult("terminated");
				if (requestFailure.type !== "failed") {
					throw new Error("expected a failed reviewer result");
				}
				threadLoop.runs[0]?.release({
					type: "failed",
					error: requestFailure.error,
				});
				const control = threadControl(
					sessionId,
					"rin_failed_idle_control",
					reviewerThreadId,
				);
				expect(
					await Effect.runPromise(
						manager.waitReviewerExecution(
							control,
							accepted.reviewerExecutionToken,
							undefined,
						),
					),
				).toMatchObject({
					ok: true,
					status: "idle",
					terminal: true,
					timedOut: false,
				});
				expect(closeoutCalls).toBe(1);

				expect(
					await Effect.runPromise(
						manager.evictReviewerExecution(
							control,
							accepted.reviewerExecutionToken,
						),
					),
				).toMatchObject({ ok: true, applied: true, terminal: true });
				expect(
					await Effect.runPromise(manager.inspectThread(control)),
				).toMatchObject({ ok: true, observed: false });
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(sessionId, "rin_sibling_inspect", siblingThreadId),
						),
					),
				).toMatchObject({ ok: true, observed: true, status: "idle" });
			},
		);
	});

	test("approval reviewer threads reject completion mail without starting a run", async () => {
		const sessionId = "sesn_reviewer_mail_rejected";
		const reviewerThreadId = "thrd_reviewer_mail_rejected";
		const parentThreadId = "thrd_reviewer_mail_parent";
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl(
								sessionId,
								"rin_preload_reviewer_mail",
								reviewerThreadId,
							),
							runtimeBindingToken: "runtime-binding-token",
							contextEntries: [],
							thread: {
								parentThreadId,
								role: "approval_reviewer",
								visibility: "internal",
								agentType: "approval_reviewer",
								status: "idle",
							},
						}),
					),
				).toMatchObject({ ok: true, applied: true });

				expect(
					await Effect.runPromise(
						manager.acceptInput(
							agentMailInput(
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
							),
						),
					),
				).toEqual({
					ok: false,
					sessionId,
					reason: "thread_not_receivable",
				});
				expect(threadLoop.runs).toEqual([]);
			},
		);
	});
	test("reports hot-state session, thread, and run fiber gauges through injected metrics", async () => {
		const threadLoop = makeControlledThreadLoop();
		const metrics = new RecordingRuntimeMetrics();
		await withSessionManager(
			sessionManagerLayer(threadLoop, { metrics }),
			async (manager) => {
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_1"),
				);
				await waitForRuns(threadLoop, 1);
				expect(metrics.latestHotState()).toEqual({
					activeSessions: 1,
					activeThreads: 1,
					activeFibers: 1,
					pendingApprovals: 0,
				});

				threadLoop.runs[0]?.release();
				await waitForCondition(
					() => metrics.latestHotState()?.activeFibers === 0,
					"metrics active fiber release",
				);
				expect(metrics.latestHotState()).toEqual({
					activeSessions: 1,
					activeThreads: 1,
					activeFibers: 0,
					pendingApprovals: 0,
				});
			},
		);
	});

	test("accepted input creates one session and does not duplicate an in-flight ThreadLoop", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				const first = await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_1"),
				);
				await waitForRuns(threadLoop, 1);
				const second = Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_1"),
				);
				await new Promise((resolve) => setTimeout(resolve, 5));
				expect(first).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: true,
					started: true,
				});
				expect(threadLoop.runs.map((run) => run.sessionId)).toEqual(["sesn_1"]);
				expect(threadLoop.runs[0]?.args).toHaveLength(2);
				threadLoop.runs[0]?.release();
				expect(await second).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					started: false,
				});
			},
		);
	});

	test("accepted facts installed while running reduce to exactly one follow-up run", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				await Effect.runPromise(
					Effect.gen(function* () {
						const first = yield* manager.acceptInput(
							acceptedInput("sesn_1", "rin_1"),
						);
						const second = yield* manager.acceptInput(
							acceptedInput("sesn_1", "rin_2"),
						);
						const third = yield* manager.acceptInput(
							acceptedInput("sesn_1", "rin_3"),
						);
						expect(first).toEqual({
							ok: true,
							sessionId: "sesn_1",
							created: true,
							started: true,
						});
						expect(second).toEqual({
							ok: true,
							sessionId: "sesn_1",
							created: false,
							started: false,
						});
						expect(third).toEqual({
							ok: true,
							sessionId: "sesn_1",
							created: false,
							started: false,
						});
					}),
				);

				await waitForRuns(threadLoop, 1);
				threadLoop.runs[0]?.release();
				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs.map((run) => run.sessionId)).toEqual([
					"sesn_1",
					"sesn_1",
				]);
				expect(threadLoop.runs[1]?.session).toBe(threadLoop.runs[0]?.session);
				expect(threadLoop.runs[1]?.session.state.contextManager).toBe(
					threadLoop.runs[0]?.session.state.contextManager,
				);
				threadLoop.runs[1]?.release();
			},
		);
	});

	test("input acceptance racing ThreadLoop completion retains exactly one successor run", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_finish_race", "rin_finish_race_first"),
						),
					),
				).toMatchObject({
					ok: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);

				const acceptedDuringCompletion = Effect.runPromise(
					manager.acceptInput(
						acceptedInput("sesn_finish_race", "rin_finish_race_next"),
					),
				);
				const completedRun = Promise.resolve().then(() =>
					threadLoop.runs[0]?.release(),
				);
				const [accepted] = await Promise.all([
					acceptedDuringCompletion,
					completedRun,
				]);
				expect(accepted).toMatchObject({
					ok: true,
					sessionId: "sesn_finish_race",
				});

				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs.map((run) => run.sessionId)).toEqual([
					"sesn_finish_race",
					"sesn_finish_race",
				]);
				expect(
					threadLoop.runs[1]?.session.state.peekAcceptedInput()?.runtimeInputId,
				).toBe("rin_finish_race_next");
				threadLoop.runs[1]?.release();
				await waitForThreadIdle(
					manager,
					"sesn_finish_race",
					"thrd_sesn_finish_race",
				);
				expect(threadLoop.runs).toHaveLength(2);
			},
		);
	});

	test("duplicate accepted input is idempotent and does not schedule an empty follow-up", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_1", "rin_duplicate")),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				expect(
					await Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_1", "rin_duplicate")),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					started: false,
					duplicate: true,
				});
				threadLoop.runs[0]?.release();
				await new Promise((resolve) => setTimeout(resolve, 5));
				expect(threadLoop.runs).toHaveLength(1);
			},
		);
	});

	test("cold installation applies the triggering command before admitting its first run", async () => {
		const sessionId = "sesn_install_trigger_order";
		const threadId = "thrd_install_trigger_order";
		const trigger = acceptedInput(
			sessionId,
			"rin_install_trigger_order",
			threadId,
		);
		const mail = agentMailInput(
			sessionId,
			"agent_mail:delivery_install_trigger_order",
			threadId,
			"thrd_install_trigger_child",
			{
				role: "main",
				visibility: "public",
				agentType: "general",
				status: "idle",
			},
		);
		const observedAcceptedInputCounts: number[] = [];
		const threadLoop = {
			layer: Layer.succeed(
				ThreadLoop.Service,
				threadLoopService({
					run: (session) =>
						Effect.sync(() => {
							observedAcceptedInputCounts.push(
								session.state.acceptedInputCount(),
							);
							return { type: "completed" as const, modelMessageCount: 0 };
						}),
				}),
			),
		};
		await withSessionManager(
			sessionManagerLayer(threadLoop, {
				loadThreadContext: async () => ({
					...threadControl(sessionId, trigger.runtimeInputId, threadId),
					runtimeBindingToken: "runtime-binding-token",
					contextEntries: [],
					pendingAgentMail: [mail],
				}),
			}),
			async (manager) => {
				expect(
					await Effect.runPromise(manager.acceptInput(trigger)),
				).toMatchObject({
					ok: true,
					sessionId,
					started: true,
				});
				await waitForCondition(
					() => observedAcceptedInputCounts.length > 0,
					"installed trigger run",
				);
				expect(observedAcceptedInputCounts[0]).toBe(2);
			},
		);
	});

	test("concurrent idle interrupt replay preflights twice but mutates once", async () => {
		const sessionId = "sesn_concurrent_idle_interrupt";
		const threadId = "thrd_concurrent_idle_interrupt";
		const threadLoop = makeInterruptRecordingThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				await Effect.runPromise(
					manager.preloadThread({
						...threadControl(sessionId, "rin_preload", threadId),
						runtimeBindingToken: "runtime-binding-token",
						contextEntries: [],
					}),
				);
				const interrupt = {
					...threadControl(sessionId, "rin_concurrent_idle", threadId),
					inputOrder: 3,
				};
				let commits = 0;
				const commit = async (declaration: RuntimeControlInputDeclaration) => {
					commits += 1;
					return commits === 1
						? buildRuntimeControlCommitResult("interrupt", declaration)
						: ({ ok: true, stale: true } as const);
				};
				const [first, replay] = await Promise.all([
					Effect.runPromise(manager.interruptControl(sessionId, interrupt, commit)),
					Effect.runPromise(manager.interruptControl(sessionId, interrupt, commit)),
				]);
				expect(first).toMatchObject({ ok: true, idleInterrupt: true });
				expect(replay).toMatchObject({
					ok: true,
					idleInterrupt: true,
					duplicate: true,
					stale: true,
				});
				expect(commits).toBe(2);
			},
		);
	});

	test("interruptControl cancels active work and settles later idle tools", async () => {
		const threadLoop = makeInterruptRecordingThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(manager.acceptInput(acceptedInput("sesn_1"))),
				).toMatchObject({
					ok: true,
					sessionId: "sesn_1",
					started: true,
				});
				await waitForInterruptRecordingRuns(threadLoop, 1);
				const session = threadLoop.runs[0]?.session;
				if (session === undefined) {
					throw new Error("expected active session");
				}

				const activeInterrupt = {
					...threadControl("sesn_1"),
					runtimeInputId: "rin_interrupt",
					inputOrder: 9,
				};
				expect(
					await Effect.runPromise(
						manager.interruptControl(
							"sesn_1",
							activeInterrupt,
							testControlCommit(activeInterrupt),
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					interrupted: true,
					idleInterrupt: false,
				});
				expect(threadLoop.interruptions).toEqual([
					{
						sessionId: "sesn_1",
						runtimeShutdownRequested: false,
					},
				]);
				session.state.recordPendingApprovalToolJob({
					toolUseEventId: "sevt_idle_interrupt_tool",
					modelRequestId: "mreq_idle_interrupt",
					source: { providerId: "fake", modelId: "fake-chat" },
					assistantMessageSequence: pendingApprovalAssistantEntry(
						"sesn_1",
						"sevt_idle_interrupt_tool",
					).messageSequence,
					toolPart: pendingApprovalToolPart(
						"sesn_1",
						"sevt_idle_interrupt_tool",
					),
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
				});
				const approvalMessage = pendingApprovalAssistantEntry(
					"sesn_1",
					"sevt_idle_interrupt_tool",
				);
				const sandboxMessage = pendingApprovalAssistantEntry(
					"sesn_1",
					"sevt_idle_interrupt_sandbox",
					2,
				);
				session.state.contextManager.appendEntry(approvalMessage);
				session.state.contextManager.appendEntry(sandboxMessage);
				session.state.recordPendingSandboxExecutionJob({
					recoveryKind: "sandbox_execution",
					toolUseEventId: "sevt_idle_interrupt_sandbox",
					modelRequestId: "mreq_idle_interrupt",
					source: { providerId: "fake", modelId: "fake-chat" },
					assistantMessageSequence: sandboxMessage.messageSequence,
					toolPart: pendingApprovalToolPart(
						"sesn_1",
						"sevt_idle_interrupt_sandbox",
					),
					entry: {} as never,
					job: {
						id: "mreq_idle_interrupt:tool-2",
						modelOrder: 1,
						toolUseEventId: "sevt_idle_interrupt_sandbox",
						modelToolCallId: "tool-2",
						kind: "builtin",
						name: "Write",
						route: {
							kind: "sandbox",
							operation: "RunTool",
							helperSubcommand: "write",
						},
						input: { file_path: "src/b.ts" },
						runPolicy: { mode: "parallel_safe", conflictKeys: null },
						gateState: "runnable",
					},
				});
				session.state.installThreadTurn(
					{
						pendingInputContextSequences: [],
						request: {
							modelRequestId: "mreq_idle_interrupt",
							requestStartEventId: "sevt_idle_interrupt_start",
							requestKind: "agent_provider_request",
							contextThroughMessageSequence: 0,
							requestEnd: {
								eventId: "sevt_idle_interrupt_end",
								isError: false,
			providerContextRetention: { disposition: "completed", toolUseEventIds: [], repairEventIds: [] },
							},
							toolMembers: [
								{
									memberKind: "public_tool_use",
									modelToolCallId: "tool-1",
									toolUseEventId: "sevt_idle_interrupt_tool",
									toolName: "Write",
								},
								{
									memberKind: "public_tool_use",
									modelToolCallId: "tool-2",
									toolUseEventId: "sevt_idle_interrupt_sandbox",
									toolName: "Write",
								},
							],
						},
						idleCloseout: {
							eventId: "sevt_idle_interrupt_requires_action",
							stopReason: "requires_action",
						},
					},
					{
						routes: [
							{
								toolUseEventId: "sevt_idle_interrupt_tool",
								disposition: "requires_user_action",
							},
							{
								toolUseEventId: "sevt_idle_interrupt_sandbox",
								disposition: "resume_sandbox_execution",
							},
						],
					},
				);
				let idleInterruptCommits = 0;
				const idleInterrupt = {
					...threadControl("sesn_1"),
					runtimeInputId: "rin_idle_interrupt",
					inputOrder: 10,
				};
				expect(
					await Effect.runPromise(
						manager.interruptControl(
							"sesn_1",
							idleInterrupt,
							async (declaration) => {
								idleInterruptCommits += 1;
								const result = buildRuntimeControlCommitResult(
									"interrupt",
									declaration,
								);
								if (!result.ok) return result;
								return {
									...result,
									interruptToolResults: [
										{
											toolUseEventId: "sevt_idle_interrupt_tool",
											result: { type: "cancelled" as const },
										},
										{
											toolUseEventId: "sevt_idle_interrupt_sandbox",
											result: {
												type: "error" as const,
												error: {
													type: "runtime_invalid_sequence",
													message:
														"Sandbox outcome is unknown after interruption.",
													retryable: false,
												},
											},
										},
									],
								};
							},
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					interrupted: false,
					idleInterrupt: true,
				});
				expect(idleInterruptCommits).toBe(1);
				expect(
					session.state.contextManager
						.entries()
						.flatMap((entry) => entry.parts)
						.flatMap((part) =>
							part.type === "tool_result"
								? [
										{
											modelToolCallId: part.modelToolCallId,
											status: part.result.type,
										},
									]
								: [],
						),
				).toEqual([
					{
						modelToolCallId: "call_sevt_idle_interrupt_tool",
						status: "cancelled",
					},
					{
						modelToolCallId: "call_sevt_idle_interrupt_sandbox",
						status: "error",
					},
				]);
				expect(session.state.pendingApprovalToolJobs()).toHaveLength(0);
				expect(session.state.pendingSandboxExecutionJobs()).toHaveLength(0);
				expect(session.state.threadTurnTransition()).toMatchObject({
					state: { state: "idle" },
					nextStep: { action: "await_input" },
				});
				expect(idleInterruptCommits).toBe(1);
				expect(session.state.contextManager.entries()).toHaveLength(2);
				expect(threadLoop.runs).toHaveLength(1);
				expect(threadLoop.interruptions).toHaveLength(1);
			},
		);
	});

	test("invalid idle interrupt projection evicts hot state before the next cold retry", async () => {
		const threadLoop = makeInterruptRecordingThreadLoop();
		const sessionId = "sesn_invalid_interrupt_projection";
		const threadId = "thrd_invalid_interrupt_projection";
		let loads = 0;
		await withSessionManager(
			sessionManagerLayer(threadLoop, {
				loadThreadContext: async (command) => {
					loads += 1;
					return {
						...command,
						runtimeBindingToken: "runtime-binding-token",
						contextEntries: [],
					};
				},
			}),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput(
								sessionId,
								"rin_invalid_projection_owner",
								threadId,
							),
						),
					),
				).toMatchObject({ ok: true, started: true });
				await waitForInterruptRecordingRuns(threadLoop, 1);
				const session = threadLoop.runs[0]?.session;
				if (session === undefined) throw new Error("expected resident session");

				const stop = {
					...threadControl(sessionId, "rin_invalid_projection_stop", threadId),
					inputOrder: 2,
				};
				expect(
					await Effect.runPromise(
						manager.interruptControl(sessionId, stop, testControlCommit(stop)),
					),
				).toMatchObject({ ok: true, interrupted: true });

				session.state.contextManager.appendEntry(
					pendingApprovalAssistantEntry(sessionId, "tool_invalid_projection"),
				);
				const interrupt = {
					...threadControl(sessionId, "rin_invalid_projection", threadId),
					inputOrder: 3,
				};
				const loadsBeforeInvalidReceipt = loads;
				const invalid = await Effect.runPromise(
					manager.interruptControl(
						sessionId,
						interrupt,
						async (declaration) => {
							const committed = buildRuntimeControlCommitResult(
								"interrupt",
								declaration,
							);
							if (!committed.ok) return committed;
							return {
								...committed,
								interruptToolResults: [
									{
										toolUseEventId: "unknown_tool_projection",
										result: { type: "cancelled" as const },
									},
								],
							};
						},
					),
				);
				expect(invalid).toEqual({
					ok: false,
					sessionId,
					reason: "context_load_failed",
				});
				expect(loads).toBe(loadsBeforeInvalidReceipt);

				expect(
					await Effect.runPromise(
						manager.interruptControl(
							sessionId,
							interrupt,
							testControlCommit(interrupt),
						),
					),
				).toMatchObject({ ok: true, idleInterrupt: true });
				expect(loads).toBe(loadsBeforeInvalidReceipt + 1);
			},
		);
	});

	test("a concurrent interrupt fails closed without waiting inside Runtime", async () => {
		const threadLoop = makeInterruptCleanupThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				const sessionId = "sesn_ordered_active_interrupts";
				const threadId = "thrd_ordered_active_interrupts";
				await Effect.runPromise(
					manager.acceptInput(
						acceptedInput(sessionId, "rin_active_owner", threadId),
					),
				);
				await waitForRuns(threadLoop, 1);
				const first = {
					...threadControl(sessionId, "rin_interrupt_first", threadId),
					inputOrder: 9,
				};
				const second = {
					...threadControl(sessionId, "rin_interrupt_second", threadId),
					inputOrder: 10,
				};
				const committed: string[] = [];
				const firstResult = Effect.runPromise(
					manager.interruptControl(sessionId, first, async (declaration) => {
						committed.push(first.runtimeInputId);
						return buildRuntimeControlCommitResult("interrupt", declaration);
					}),
				);
				await threadLoop.cleanupStarted;
				const secondResult = await Effect.runPromise(
					manager.interruptControl(sessionId, second, async (declaration) => {
						committed.push(second.runtimeInputId);
						return buildRuntimeControlCommitResult("interrupt", declaration);
					}),
				);
				expect(secondResult).toEqual({
					ok: false,
					sessionId,
					reason: "control_busy",
				});

				threadLoop.releaseCleanup();
				expect(await firstResult).toMatchObject({
					ok: true,
					interrupted: true,
				});
				expect(threadLoop.runs).toHaveLength(1);
				expect(committed).toEqual([first.runtimeInputId]);
				expect(threadLoop.observedInterruptLeaseRefs).toEqual([
					first.interruptLeaseRef,
				]);
			},
		);
	});

	test("retryable idle interrupt replay retries the frozen declaration", async () => {
		const threadLoop = makeInterruptRecordingThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				const sessionId = "sesn_retryable_idle_interrupt";
				const threadId = "thrd_retryable_idle_interrupt";
				const preload = threadControl(
					sessionId,
					"rin_preload_retryable_idle_interrupt",
					threadId,
				);
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...preload,
							runtimeBindingToken: "runtime-binding-token",
							contextEntries: [],
						}),
					),
				).toMatchObject({ ok: true, applied: true });

				const interrupt = {
					...threadControl(sessionId, "rin_retryable_idle_interrupt", threadId),
					inputOrder: 11,
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
					return buildRuntimeControlCommitResult("interrupt", declaration);
				};

				expect(
					await Effect.runPromise(
						manager.interruptControl(sessionId, interrupt, commit),
					),
				).toEqual({
					ok: false,
					sessionId,
					reason: "context_load_failed",
				});
				expect(
					await Effect.runPromise(
						manager.interruptControl(sessionId, interrupt, commit),
					),
				).toEqual({
					ok: true,
					sessionId,
					created: false,
					interrupted: false,
					idleInterrupt: true,
				});
				expect(declarations).toHaveLength(2);
				expect(declarations[1]).toEqual(declarations[0]);
			},
		);
	});

	test("idle config commands update shared hot state without starting ThreadLoop work", async () => {
		const threadLoop = makeControlledThreadLoop();

		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl("sesn_1", "rin_preload_config"),
							runtimeBindingToken: "runtime-binding-token",
							contextEntries: [],
						}),
					),
				).toMatchObject({ ok: true, applied: true });
				expect(
					await Effect.runPromise(
						manager.applyRuntimeConfigPatch("sesn_1", {
							...runtimeConfigScope("sesn_1", "session:5"),
							generation: 5,
							contentJson: '{"config_generation":5}',
						}),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					applied: true,
				});
				expect(threadLoop.runs).toEqual([]);

				expect(
					await Effect.runPromise(
						manager.startTestRunThroughAcceptedInput("sesn_1"),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				const session = threadLoop.runs[0]?.session;
				expect(session?.configuration.runtimeConfigPatch()).toEqual({
					generation: 5,
					contentJson: '{"config_generation":5}',
				});
				threadLoop.runs[0]?.release();
			},
		);
	});

	test("task notification received during a run remains pending for the follow-up turn", async () => {
		const threadLoop = makeControlledThreadLoop();
		const sessionId = "sesn_task_notification_wake";
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(acceptedInput(sessionId)),
					),
				).toMatchObject({
					ok: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				expect(
					await Effect.runPromise(
						manager.commitTaskNotification(sessionId, {
							...threadControl(sessionId, "rin_task_notification_wake"),
							inputOrder: 1,
							taskId: "task_notification_wake",
							sourceToolUseEventId: "sevt_task_notification_wake",
							status: "completed",
							notificationJson: '{"status":"completed"}',
						}),
					),
				).toMatchObject({ ok: true, applied: true });
				expect(
					threadLoop.runs[0]!.session.state.contextManager.entries().flatMap(
						(entry) => entry.parts,
					),
				).not.toContainEqual(
					expect.objectContaining({ type: "text", text: "task completed" }),
				);

				threadLoop.runs[0]!.release();
				await waitForRuns(threadLoop, 2);
				expect(
					threadLoop.runs[1]!.session.state.peekAcceptedInput(),
				).toMatchObject({
					kind: "task_notification",
					runtimeInputId: "rin_task_notification_wake",
					taskId: "task_notification_wake",
				});
				threadLoop.runs[1]!.release();
			},
		);
	});

	test("task notification that starts a child wins over a trailing hot resume projection", async () => {
		const threadLoop = makeControlledThreadLoop();
		const sessionId = "sesn_task_notification_resume_race";
		const threadId = "thrd_task_notification_resume_race";
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl(
								sessionId,
								"rin_preload_task_notification_resume",
								threadId,
							),
							runtimeBindingToken: "runtime-binding-token",
							contextEntries: [],
							thread: {
								parentThreadId: "thrd_task_notification_resume_parent",
								role: "subagent",
								visibility: "public",
								taskName: "notification worker",
								agentType: "worker",
								status: "idle",
							},
						}),
					),
				).toMatchObject({ ok: true, applied: true });

				expect(
					await Effect.runPromise(
						manager.commitTaskNotification(sessionId, {
							...threadControl(
								sessionId,
								"task_notification:task_notification_resume",
								threadId,
							),
							inputOrder: 1,
							taskId: "task_notification_resume",
							sourceToolUseEventId: "sevt_task_notification_resume",
							status: "completed",
							notificationJson: '{"status":"completed"}',
						}),
					),
				).toMatchObject({ ok: true, applied: true });
				await waitForRuns(threadLoop, 1);
				expect(
					threadLoop.runs[0]?.session.state.peekAcceptedInput(),
				).toMatchObject({
					kind: "task_notification",
					taskId: "task_notification_resume",
				});

				expect(
					await Effect.runPromise(
						manager.markThreadActive(
							threadControl(
								sessionId,
								"rin_hot_resume_after_notification",
								threadId,
							),
						),
					),
				).toEqual({
					ok: false,
					sessionId,
					sessionThreadId: threadId,
					reason: "thread_busy",
				});
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								sessionId,
								"rin_inspect_notification_resume",
								threadId,
							),
						),
					),
				).toMatchObject({ observed: true, status: "running" });

				threadLoop.runs[0]?.release({
					type: "completed",
					modelMessageCount: 1,
				});
				await waitForCondition(
					() => threadLoop.runs.length === 1,
					"single task notification run",
				);
				expect(threadLoop.runs).toHaveLength(1);
			},
		);
	});

	test("one failed shared-state initializer prevents sibling cold loads from creating a second shared state", async () => {
		const threadLoop = makeControlledThreadLoop();
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
		const loadThreadContext: NonNullable<
			SessionManager.LayerOptions["loadThreadContext"]
		> = async (command) => {
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
				contextEntries: [],
				thread:
					command.sessionThreadId === "thrd_shared_initializer"
						? { role: "main", visibility: "public", status: "idle" }
						: {
								parentThreadId: "thrd_shared_initializer",
								role: "subagent",
								visibility: "public",
								status: "idle",
							},
			};
		};

		await withSessionManager(
			sessionManagerLayer(threadLoop, { loadThreadContext }),
			async (manager) => {
				const initializer = Effect.runPromise(
					manager.ensureThreadInstalled(
						threadControl(
							"sesn_shared_install_failure",
							"rin_initializer",
							"thrd_shared_initializer",
						),
					),
				);
				await failedLoadStarted;
				const sibling = Effect.runPromise(
					manager.ensureThreadInstalled(
						threadControl(
							"sesn_shared_install_failure",
							"rin_sibling",
							"thrd_shared_sibling",
						),
					),
				);
				await siblingLoadStarted;
				releaseFailedLoad?.();

				await expect(initializer).resolves.toEqual({
					ok: false,
					sessionId: "sesn_shared_install_failure",
					sessionThreadId: "thrd_shared_initializer",
					reason: "context_load_failed",
					retryable: false,
				});
				await expect(
					Effect.runPromise(
						manager.ensureThreadInstalled(
							threadControl(
								"sesn_shared_install_failure",
								"rin_early_retry",
								"thrd_shared_initializer",
							),
						),
					),
				).resolves.toEqual({
					ok: false,
					sessionId: "sesn_shared_install_failure",
					sessionThreadId: "thrd_shared_initializer",
					reason: "context_load_failed",
					retryable: false,
				});
				expect(loadCount).toBe(2);

				releaseSiblingLoad?.();
				await expect(sibling).resolves.toEqual({
					ok: false,
					sessionId: "sesn_shared_install_failure",
					sessionThreadId: "thrd_shared_sibling",
					reason: "context_load_failed",
				});
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								"sesn_shared_install_failure",
								"rin_inspect",
								"thrd_shared_sibling",
							),
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_shared_install_failure",
					sessionThreadId: "thrd_shared_sibling",
					observed: false,
					entries: [],
				});

				await expect(
					Effect.runPromise(
						manager.ensureThreadInstalled(
							threadControl(
								"sesn_shared_install_failure",
								"rin_retry",
								"thrd_shared_initializer",
							),
						),
					),
				).resolves.toEqual({
					ok: true,
					sessionId: "sesn_shared_install_failure",
					sessionThreadId: "thrd_shared_initializer",
					applied: true,
				});
				expect(loadCount).toBe(3);
			},
		);
	});

	test("existing child thread keeps parent and role metadata across accepted input", async () => {
		const threadLoop = makeControlledThreadLoop();

		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
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
							contextEntries: [],
						}),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_child",
					sessionThreadId: "thrd_child",
					applied: true,
				});

				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_child", "rin_child_follow", "thrd_child"),
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_child",
					created: false,
					started: true,
				});
				await waitForRuns(threadLoop, 1);

				expect(threadLoop.runs[0]?.session.identity).toMatchObject({
					parentThreadId: "thrd_parent",
					threadRole: "subagent",
					runtimeBindingToken: "runtime-binding-token-child",
				});
				threadLoop.runs[0]?.release();
			},
		);
	});

	test("task notification settlement does not require a resident background-task snapshot", async () => {
		const threadLoop = makeControlledThreadLoop();

		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl(
								"sesn_cold_background",
								"rin_preload_background",
							),
							runtimeBindingToken: "runtime-binding-token-background",
							contextEntries: [],
						}),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_cold_background",
					sessionThreadId: "thrd_sesn_cold_background",
					applied: true,
				});

				expect(
					await Effect.runPromise(
						manager.commitTaskNotification("sesn_cold_background", {
							...threadControl("sesn_cold_background", "rin_task_background"),
							inputOrder: 1,
							taskId: "task_cold_background",
							sourceToolUseEventId: "sevt_tool_background",
							status: "completed",
							notificationJson:
								'{"task_id":"task_cold_background","source_tool_use_event_id":"sevt_tool_background","status":"completed"}',
						}),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_cold_background",
					created: false,
					applied: true,
				});

				await waitForRuns(threadLoop, 1);
				expect(
					threadLoop.runs[0]?.session.state.peekAcceptedInput(),
				).toMatchObject({
					kind: "task_notification",
					runtimeInputId: "rin_task_background",
				});
				threadLoop.runs[0]?.release();
			},
		);
	});

	test("preload admits stored completion mail through the waking path and leaves an empty preload idle", async () => {
		const base = makeControlledThreadLoop();
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
			expect(
				await Effect.runPromise(
					manager.preloadThread({
						...threadControl(sessionID, "rin_preload_agent_mail", threadID),
						runtimeBindingToken: "runtime-binding-token",
						contextEntries: [],
						thread,
						pendingAgentMail: [mail],
					}),
				),
			).toMatchObject({ ok: true, applied: true });
			await waitForRuns(base, 1);
			expect(
				await Effect.runPromise(
					manager.inspectThread(
						threadControl(sessionID, "rin_inspect_agent_mail", threadID),
					),
				),
			).toMatchObject({ observed: true, status: "running" });
			base.runs[0]?.release({ type: "completed", modelMessageCount: 1 });

			const emptySessionID = "sesn_preload_agent_mail_empty";
			expect(
				await Effect.runPromise(
					manager.preloadThread({
						...threadControl(
							emptySessionID,
							"rin_preload_agent_mail_empty",
							"thrd_preload_agent_mail_empty",
						),
						runtimeBindingToken: "runtime-binding-token",
						contextEntries: [],
						thread,
						pendingAgentMail: [],
					}),
				),
			).toMatchObject({ ok: true, applied: true });
			expect(base.runs).toHaveLength(1);
			expect(
				await Effect.runPromise(
					manager.inspectThread(
						threadControl(
							emptySessionID,
							"rin_inspect_agent_mail_empty",
							"thrd_preload_agent_mail_empty",
						),
					),
				),
			).toMatchObject({ observed: true, status: "idle" });
		});
	});

	test("cold preload drains four completion mails as four turns without a stranded wake", async () => {
		const threadLoop = makeControlledThreadLoop();
		const sessionID = "sesn_preload_agent_mail_page";
		const threadID = "thrd_preload_agent_mail_page_main";
		const thread: RuntimeAcceptedThreadMetadataState = {
			role: "main",
			visibility: "public",
			agentType: "general",
			status: "idle",
		};
		const mails = [1, 2, 3, 4].map((index) =>
			agentMailInput(
				sessionID,
				`agent_mail:delivery_preload_agent_mail_page_${index}`,
				threadID,
				`thrd_preload_agent_mail_page_child_${index}`,
				thread,
			),
		);
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl(
								sessionID,
								"rin_preload_agent_mail_page",
								threadID,
							),
							runtimeBindingToken: "runtime-binding-token",
							contextEntries: [],
							thread,
							pendingAgentMail: mails,
						}),
					),
				).toMatchObject({ ok: true, applied: true });

				for (let index = 0; index < mails.length; index += 1) {
					await waitForRuns(threadLoop, index + 1);
					expect(
						threadLoop.runs[index]!.session.state.peekAcceptedInput()
							?.runtimeInputId,
					).toBe(mails[index]!.runtimeInputId);
					threadLoop.runs[index]!.release({
						type: "completed",
						modelMessageCount: 1,
					});
				}
				await waitForThreadIdle(manager, sessionID, threadID);
				expect(threadLoop.runs).toHaveLength(mails.length);
				expect(
					await Effect.runPromise(manager.acceptInput(mails[3]!)),
				).toMatchObject({
					ok: true,
					started: true,
				});
				await waitForRuns(threadLoop, mails.length + 1);
				threadLoop.runs[mails.length]!.release({
					type: "completed",
					modelMessageCount: 0,
				});
			},
		);
	});

	test("completion mail admitted to a busy resident thread remains reducer-visible", async () => {
		const threadLoop = makeControlledThreadLoop();
		const layer = sessionManagerLayer(threadLoop);
		const sessionID = "sesn_busy_agent_mail";
		const threadID = "thrd_busy_agent_mail_main";
		const thread: RuntimeAcceptedThreadMetadataState = {
			role: "main",
			visibility: "public",
			agentType: "general",
			status: "idle",
		};
		await withSessionManager(layer, async (manager) => {
			expect(
				await Effect.runPromise(
					manager.preloadThread({
						...threadControl(
							sessionID,
							"rin_preload_busy_agent_mail",
							threadID,
						),
						runtimeBindingToken: "runtime-binding-token",
						contextEntries: [],
						thread,
					}),
				),
			).toMatchObject({ ok: true, applied: true });
			expect(
				await Effect.runPromise(
					manager.acceptInput(
						acceptedInput(sessionID, "rin_busy_agent_mail_first", threadID),
					),
				),
			).toMatchObject({ ok: true, started: true });
			await waitForRuns(threadLoop, 1);
			const mail = agentMailInput(
				sessionID,
				"agent_mail:delivery_busy_agent_mail",
				threadID,
				"thrd_busy_agent_mail_child",
				thread,
			);
			const acceptedMail = await Effect.runPromise(manager.acceptInput(mail));
			expect(acceptedMail).toMatchObject({
				ok: true,
				sessionId: sessionID,
				created: false,
				started: false,
			});
			threadLoop.runs[0]?.release({ type: "completed", modelMessageCount: 1 });
			await waitForRuns(threadLoop, 2);
			threadLoop.runs[1]?.release({ type: "completed", modelMessageCount: 1 });
		});
	});

	test("cold preload installs both pending attachment origins before the next run", async () => {
		const threadLoop = makeControlledThreadLoop();
		const pendingAttachments = [
			{
				transient: {
					attachmentRef: "att_cold",
					sourcePath: "mcp:github/chart.png",
					pageRange: "",
					detail: "auto",
				},
				fileBacked: undefined,
				mime: "image/png",
				filename: "chart.png",
			},
			{
				transient: undefined,
				fileBacked: {
					sourceEventId: "sevt_user_cold",
					fileId: "file_cold",
				},
				mime: "application/pdf",
				filename: "brief.pdf",
			},
		];

		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl(
								"sesn_cold_attachments",
								"rin_preload_attachments",
							),
							runtimeBindingToken: "runtime-binding-token-attachments",
							contextEntries: [],
							pendingAttachments,
						}),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_cold_attachments",
					sessionThreadId: "thrd_sesn_cold_attachments",
					applied: true,
				});

				await waitForRuns(threadLoop, 1);
				expect(threadLoop.runs[0]?.session.state.pendingAttachments()).toEqual(
					pendingAttachments,
				);
				expect(threadLoop.runs[0]?.session.state.threadTurnTransition()).toMatchObject(
					{
						checkpoint: { pendingInputContextSequences: [] },
						state: { state: "ready_to_request" },
						nextStep: { action: "prepare_next_request" },
					},
				);
				threadLoop.runs[0]?.release();
			},
		);
	});

	test("cold preload installs generation-fenced MCP manifests before pending MCP approval recovery", async () => {
		const manifestsObservedDuringRecovery: Array<
			ReturnType<ThreadRuntime.ThreadRuntime["configuration"]["patches"]>
		> = [];
		const manifestUpdates: SessionManager.RuntimeMCPManifestUpdateEvent[] = [];
		const threadLoop = makeControlledThreadLoop({
			installLoadedPendingToolUses: (session) =>
				Effect.sync(() => {
					manifestsObservedDuringRecovery.push([
						...session.configuration.patches(),
					]);
					return { ok: true as const };
				}),
		});
		await withSessionManager(
			sessionManagerLayer(threadLoop, {
				recordMCPManifestUpdate: (event) => manifestUpdates.push(event),
			}),
			async (manager) => {
				const control = threadControl(
					"sesn_mcp_cold_pending",
					"rin_mcp_cold_pending",
				);
				const result = await Effect.runPromise(
					manager.preloadThread({
						...control,
						runtimeBindingToken: "runtime-binding-token-mcp-cold",
						contextEntries: [coldUserEntry("sesn_mcp_cold_pending")],
						runtimeConfigPatch: {
							...control,
							configIdentity: "runtime_config",
							generation: 5,
							coldLoad: true,
							installedBuiltinFamily: "claude",
							contentJson: JSON.stringify({
								config_generation: 5,
								runtime_config: {
									installedTools: [
										{ type: "tetral_agent_toolset", family: "claude" },
									],
								},
								tool_policy: { mcpToolsets: [{ mcpServerName: "github" }] },
							}),
						},
						mcpManifests: [
							{
								...control,
								configIdentity: "mcp_manifest:github",
								generation: 7,
								mcpServerName: "github",
								manifestETag: "etag_7",
								contentJson: JSON.stringify({
									mcp_manifest: {
										mcp_server_name: "github",
										manifest_etag: "etag_7",
										manifest_generation: 7,
										tools: [
											{
												name: "Read",
												description: "must collide again at Runtime",
												input_schema: { type: "object" },
											},
											{
												name: "github_search",
												description: "Search GitHub",
												input_schema: { type: "object" },
											},
										],
									},
								}),
							},
						],
						pendingToolUses: [
							{
								toolUseEventId: "sevt_mcp_pending",
								modelRequestId: "mrq_mcp_pending",
								modelToolCallId: "toolu_mcp_pending",
								toolName: "github_search",
								input: { query: "tetral" },
								status: "pending",
							},
						],
					}),
				);
				expect(result).toEqual({
					ok: true,
					sessionId: "sesn_mcp_cold_pending",
					sessionThreadId: "thrd_sesn_mcp_cold_pending",
					applied: true,
				});

				expect(manifestsObservedDuringRecovery).toHaveLength(1);
				expect(manifestsObservedDuringRecovery[0]).toContainEqual(
					expect.objectContaining({
						generation: 7,
						mcpServerName: "github",
						manifestETag: "etag_7",
					}),
				);
				expect(manifestUpdates).toEqual([
					expect.objectContaining({
						disposition: "applied",
						source: "cold_load",
						receivedGeneration: 7,
						currentGeneration: 7,
						mcpServerName: "github",
					}),
				]);
				expect(manifestUpdates[0]?.toolCatalogEligible).toBe(false);
			},
		);
	});

	test("recovery joining another installation fences a reclaimed installer before publication", async () => {
		const threadLoop = makeControlledThreadLoop();
		const command = threadControl("sesn_recovery_install_join");
		let reportFirstLoad = (): void => undefined;
		let releaseFirstLoad = (): void => undefined;
		const firstLoadStarted = new Promise<void>((resolve) => {
			reportFirstLoad = resolve;
		});
		const firstLoadGate = new Promise<void>((resolve) => {
			releaseFirstLoad = resolve;
		});
		const recovery = {
			jobId: "qjob_current_recovery",
			leaseToken: "lease_current_recovery",
			partitionKey: "session:wksp_test:sesn_recovery_install_join",
			dedupeKey:
				"runtime_recovery:wksp_test:sesn_recovery_install_join:evt_recovery_install_join",
		};
		const oldRecovery = {
			...recovery,
			leaseToken: "lease_reclaimed_recovery",
		};
		const observedRecoveries: Array<typeof recovery | undefined> = [];
		await withSessionManager(
			sessionManagerLayer(threadLoop, {
				loadThreadContext: async (loadCommand, loadOptions) => {
					observedRecoveries.push(loadOptions?.recovery);
					if (observedRecoveries.length === 1) {
						reportFirstLoad();
						await firstLoadGate;
					}
					return {
						...loadCommand,
						runtimeBindingToken: "runtime-binding-token-recovery-install-join",
						contextEntries: [
							RuntimeContextEntrySchema.parse({
								messageSequence: 1,
								contextKind: "user",
								parts: [
									{
										type: "text",
										text:
											loadOptions?.recovery?.leaseToken === recovery.leaseToken
												? "current recovery context"
												: "reclaimed recovery context",
									},
								],
							}),
						],
						thread: { role: "main", visibility: "public", status: "idle" },
					};
				},
			}),
			async (manager) => {
				const reclaimedInstall = Effect.runPromise(
					manager.ensureThreadInstalled(command, {
						loadOptions: { recovery: oldRecovery },
					}),
				);
				await firstLoadStarted;
				const currentRecovery = Effect.runPromise(
					manager.ensureThreadInstalled(command, {
						startPendingWork: true,
						loadOptions: { recovery },
					}),
				);
				releaseFirstLoad();
				await expect(reclaimedInstall).resolves.toMatchObject({
					ok: false,
					reason: "context_load_failed",
				});
				await expect(currentRecovery).resolves.toMatchObject({
					ok: true,
					applied: true,
				});
				expect(observedRecoveries).toEqual([oldRecovery, recovery]);
				await expect(
					Effect.runPromise(manager.inspectThread(command)),
				).resolves.toMatchObject({
					ok: true,
					observed: true,
					entries: [
						expect.objectContaining({
							parts: [{ type: "text", text: "current recovery context" }],
						}),
					],
				});
			},
		);
	});

	test("recovery does not inherit another installation failure", async () => {
		const threadLoop = makeControlledThreadLoop();
		const command = threadControl("sesn_recovery_install_failure");
		let reportFirstLoad = (): void => undefined;
		let releaseFirstLoad = (): void => undefined;
		const firstLoadStarted = new Promise<void>((resolve) => {
			reportFirstLoad = resolve;
		});
		const firstLoadGate = new Promise<void>((resolve) => {
			releaseFirstLoad = resolve;
		});
		const recovery = {
			jobId: "qjob_current_recovery_failure",
			leaseToken: "lease_current_recovery_failure",
			partitionKey: "session:wksp_test:sesn_recovery_install_failure",
			dedupeKey:
				"runtime_recovery:wksp_test:sesn_recovery_install_failure:evt_recovery_install_failure",
		};
		let loadCount = 0;
		await withSessionManager(
			sessionManagerLayer(threadLoop, {
				loadThreadContext: async (loadCommand, loadOptions) => {
					loadCount += 1;
					if (loadCount === 1) {
						reportFirstLoad();
						await firstLoadGate;
						throw new Error("unrelated installation failed");
					}
					expect(loadOptions?.recovery).toEqual(recovery);
					return {
						...loadCommand,
						runtimeBindingToken: "runtime-binding-token-recovery-install-failure",
						contextEntries: [],
						thread: { role: "main", visibility: "public", status: "idle" },
					};
				},
			}),
			async (manager) => {
				const unrelatedInstall = Effect.runPromise(
					manager.ensureThreadInstalled(command),
				);
				await firstLoadStarted;
				const currentRecovery = Effect.runPromise(
					manager.ensureThreadInstalled(command, {
						startPendingWork: true,
						loadOptions: { recovery },
					}),
				);
				releaseFirstLoad();
				await expect(unrelatedInstall).resolves.toMatchObject({
					ok: false,
					reason: "context_load_failed",
				});
				await expect(currentRecovery).resolves.toMatchObject({
					ok: true,
					applied: true,
				});
				expect(loadCount).toBe(2);
			},
		);
	});

	test("thread close admits behind prior work without retaining the Session gate", async () => {
		const sessionId = "sesn_close_command_order";
		const mainThreadId = `thrd_${sessionId}`;
		const siblingThreadId = "thrd_close_command_sibling";
		const toolUseEventId = "sevt_close_command_tool";
		let reportCommitStarted = (): void => undefined;
		let releaseCommit = (): void => undefined;
		const commitStarted = new Promise<void>((resolve) => {
			reportCommitStarted = resolve;
		});
		const commitGate = new Promise<void>((resolve) => {
			releaseCommit = resolve;
		});
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.startTestRunThroughAcceptedInput(sessionId),
					),
				).toMatchObject({ ok: true, started: true });
				await waitForRuns(threadLoop, 1);
				const session = threadLoop.runs[0]?.session;
				expect(session).toBeDefined();
				threadLoop.runs[0]?.release();
				await waitForThreadIdle(manager, sessionId, mainThreadId);
				session?.state.installThreadTurn(
					{
						pendingInputContextSequences: [],
						request: {
							modelRequestId: "mreq_close_command_order",
							requestStartEventId: "sevt_close_command_request",
							requestKind: "agent_provider_request",
							contextThroughMessageSequence: 0,
							toolMembers: [
								{
									memberKind: "public_tool_use",
									modelToolCallId: "call_close_command_tool",
									toolUseEventId,
									toolName: "Write",
								},
							],
						},
					},
					{
						routes: [
							{
								toolUseEventId,
								disposition: "requires_user_action",
							},
						],
					},
				);
				session?.state.recordPendingApprovalToolJob({
					toolUseEventId,
					modelRequestId: "mreq_close_command_order",
					source: { providerId: "fake", modelId: "fake-chat" },
					assistantMessageSequence: pendingApprovalAssistantEntry(
						sessionId,
						toolUseEventId,
					).messageSequence,
					toolPart: pendingApprovalToolPart(sessionId, toolUseEventId),
					entry: {} as never,
					job: {
						id: "mreq_close_command_order:call_close_command_tool",
						modelOrder: 0,
						toolUseEventId,
						modelToolCallId: "call_close_command_tool",
						kind: "builtin",
						name: "Write",
						route: { kind: "gateway", operation: "RunWeb" },
						input: { file_path: "close-order.txt" },
						runPolicy: { mode: "parallel_safe", conflictKeys: null },
						gateState: "waiting_approval",
						approvalSource: "user",
					},
				});
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl(
								sessionId,
								"rin_close_order_sibling_preload",
								siblingThreadId,
							),
							runtimeBindingToken: "runtime-binding-token-close-order",
							contextEntries: [],
							thread: {
								parentThreadId: mainThreadId,
								role: "subagent",
								visibility: "public",
								status: "idle",
							},
						}),
					),
				).toMatchObject({ ok: true, applied: true });

				const confirmationAheadOfClose = Effect.runPromise(
					manager.resolveToolConfirmation(
						sessionId,
						{
							...threadControl(sessionId),
							runtimeInputId: "rin_close_order_confirmation",
							toolUseEventId,
							decision: "allow",
						},
						async (declaration) => {
							reportCommitStarted();
							await commitGate;
							return buildRuntimeControlCommitResult(
								"tool_confirmation",
								declaration,
							);
						},
					),
				);
				await commitStarted;
				const mainClose = Effect.runPromise(
					manager.markThreadClosed(
						threadControl(
							sessionId,
							"rin_close_order_main_close",
							mainThreadId,
						),
					),
				);
				await waitForCondition(async () => {
					const snapshot = await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								sessionId,
								"rin_close_order_main_inspect",
								mainThreadId,
							),
						),
					);
					return (
						snapshot.ok &&
						"observed" in snapshot &&
						snapshot.observed &&
						snapshot.status === "closed_for_runtime"
					);
				}, "main close admission");
				await expect(
					Effect.runPromise(
						manager.acceptInput(
							acceptedInput(
								sessionId,
								"rin_close_order_late",
								mainThreadId,
							),
						),
					),
				).resolves.toMatchObject({ ok: false, reason: "thread_busy" });

				const siblingClose = Effect.runPromise(
					manager.markThreadClosed(
						threadControl(
							sessionId,
							"rin_close_order_sibling_close",
							siblingThreadId,
						),
					),
				);
				await expect(siblingClose).resolves.toMatchObject({
					ok: true,
					applied: true,
				});
				releaseCommit();
				await expect(confirmationAheadOfClose).resolves.toMatchObject({
					ok: true,
					applied: true,
				});
				await expect(mainClose).resolves.toMatchObject({
					ok: true,
					applied: true,
				});
			},
		);
	});

	test("manifest update observation follows the effective generation and skips nonresident ACKs", async () => {
		const manifestUpdates: SessionManager.RuntimeMCPManifestUpdateEvent[] = [];
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop, {
				recordMCPManifestUpdate: (event) => manifestUpdates.push(event),
				resolveMCPManifestEligibility: (_patches, serverName) =>
					serverName === "github",
			}),
			async (manager) => {
				const cold = {
					...threadControl("sesn_manifest_observation", "rin_manifest_cold"),
					runtimeBindingToken: "runtime-binding-token",
					contextEntries: [],
				};
				expect(
					await Effect.runPromise(manager.preloadThread(cold)),
				).toMatchObject({ ok: true, applied: true });
				const patch = {
					...runtimeConfigScope("sesn_manifest_observation", "mcp:github:9"),
					generation: 9,
					mcpServerName: "github",
					manifestETag: "etag_9",
					contentJson: JSON.stringify({
						mcp_manifest: {
							mcp_server_name: "github",
							manifest_etag: "etag_9",
							manifest_generation: 9,
							tools: [],
						},
					}),
				};
				expect(
					await Effect.runPromise(
						manager.applyRuntimeConfigPatch("sesn_manifest_observation", patch),
					),
				).toMatchObject({ ok: true, applied: true });
				expect(
					await Effect.runPromise(
						manager.applyRuntimeConfigPatch("sesn_manifest_observation", {
							...patch,
							configIdentity: "mcp:github:8",
							generation: 8,
						}),
					),
				).toMatchObject({ ok: true, applied: false });
				expect(
					await Effect.runPromise(
						manager.applyRuntimeConfigPatch("sesn_manifest_absent", {
							...patch,
							...runtimeConfigScope("sesn_manifest_absent", "mcp:github:9"),
						}),
					),
				).toMatchObject({ ok: true, noResidency: true });
				expect(
					manifestUpdates.map(
						({
							disposition,
							receivedGeneration,
							currentGeneration,
							source,
						}) => ({
							disposition,
							receivedGeneration,
							currentGeneration,
							source,
						}),
					),
				).toEqual([
					{
						disposition: "applied",
						receivedGeneration: 9,
						currentGeneration: 9,
						source: "runtime_config_update",
					},
					{
						disposition: "stale",
						receivedGeneration: 8,
						currentGeneration: 9,
						source: "runtime_config_update",
					},
				]);
				expect(
					manifestUpdates.every((event) => event.toolCatalogEligible),
				).toBe(true);
			},
		);
	});

	test("cold preload seeds the runtime model before pending tool use recovery", async () => {
		const modelsObservedDuringRecovery: unknown[] = [];
		const threadLoop = makeControlledThreadLoop({
			seedRuntimeModel: (session) => {
				session.state.updateCurrentModel({
					providerId: "seeded",
					modelId: "before-install",
				});
			},
			installLoadedPendingToolUses: (session) =>
				Effect.sync(() => {
					modelsObservedDuringRecovery.push(session.state.currentModel());
					return { ok: true as const };
				}),
		});
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				const control = threadControl(
					"sesn_seed_before_install",
					"rin_seed_before_install",
				);
				const result = await Effect.runPromise(
					manager.preloadThread({
						...control,
						runtimeBindingToken: "runtime-binding-token-seed-order",
						contextEntries: [coldUserEntry("sesn_seed_before_install")],
						runtimeConfigPatch: {
							...control,
							configIdentity: "runtime_config",
							generation: 3,
							coldLoad: true,
							installedBuiltinFamily: "claude",
							contentJson: JSON.stringify({
								config_generation: 3,
								runtime_config: {
									installedTools: [
										{ type: "tetral_agent_toolset", family: "claude" },
									],
								},
							}),
						},
						pendingToolUses: [
							{
								toolUseEventId: "sevt_seed_pending",
								modelRequestId: "mrq_seed_pending",
								modelToolCallId: "toolu_seed_pending",
								toolName: "Read",
								input: { path: "README.md" },
								status: "pending",
							},
						],
					}),
				);
				expect(result).toEqual({
					ok: true,
					sessionId: "sesn_seed_before_install",
					sessionThreadId: "thrd_sesn_seed_before_install",
					applied: true,
				});
				expect(modelsObservedDuringRecovery).toEqual([
					{ providerId: "seeded", modelId: "before-install" },
				]);
			},
		);
	});

	test("cold preload rejects overlap between approval and sandbox execution recovery", async () => {
		let approvalInstalls = 0;
		let executionInstalls = 0;
		const threadLoop = makeControlledThreadLoop({
			installLoadedPendingToolUses: () =>
				Effect.sync(() => {
					approvalInstalls += 1;
					return { ok: true as const };
				}),
			installLoadedSandboxExecutions: () =>
				Effect.sync(() => {
					executionInstalls += 1;
					return { ok: true as const };
				}),
		});
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				const sessionID = "sesn_cold_recovery_overlap";
				const toolUseEventID = "sevt_cold_recovery_overlap";
				const message = pendingApprovalAssistantEntry(
					sessionID,
					toolUseEventID,
				);
				const control = threadControl(sessionID, "rin_cold_recovery_overlap");
				const shared = {
					toolUseEventId: toolUseEventID,
					modelRequestId: "mrq_cold_recovery_overlap",
					modelToolCallId: `call_${toolUseEventID}`,
					toolName: "Write",
					input: {},
				};
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...control,
							runtimeBindingToken: "runtime-binding-token-overlap",
							contextEntries: [message],
							pendingToolUses: [
								{
									...shared,
									status: "resolving",
									decision: "allow",
								},
							],
							pendingSandboxExecutions: [
								{ ...shared, executionState: "running" },
							],
						}),
					),
				).toEqual({
					ok: false,
					sessionId: sessionID,
					sessionThreadId: control.sessionThreadId,
					reason: "context_load_failed",
				});
				expect(approvalInstalls).toBe(0);
				expect(executionInstalls).toBe(0);
			},
		);
	});

	test("tool confirmation wakes a thread when a ToolJob is pending approval", async () => {
		const threadLoop = makeControlledThreadLoop();

		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.startTestRunThroughAcceptedInput("sesn_1"),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				const session = threadLoop.runs[0]?.session;
				expect(session).toBeDefined();
				session?.state.installThreadTurn(
					{
						pendingInputContextSequences: [],
						request: {
							modelRequestId: "mreq_1",
							requestStartEventId: "sevt_request_start_1",
							requestKind: "agent_provider_request",
							contextThroughMessageSequence: 0,
							requestEnd: {
								eventId: "sevt_request_end_1",
								isError: false,
			providerContextRetention: { disposition: "completed", toolUseEventIds: [], repairEventIds: [] },
							},
							toolMembers: [
								{
									memberKind: "public_tool_use",
									modelToolCallId: "tool-1",
									toolUseEventId: "sevt_tool_1",
									toolName: "Write",
								},
							],
						},
					},
					{
						routes: [
							{
								toolUseEventId: "sevt_tool_1",
								disposition: "requires_user_action",
							},
						],
					},
				);
				session?.state.recordPendingApprovalToolJob({
					toolUseEventId: "sevt_tool_1",
					modelRequestId: "mreq_1",
					source: { providerId: "fake", modelId: "fake-chat" },
					assistantMessageSequence: pendingApprovalAssistantEntry(
						"sesn_1",
						"sevt_tool_1",
					).messageSequence,
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
				});

				const confirmationCommand = {
					...threadControl("sesn_1"),
					runtimeInputId: "rin_confirm",
					toolUseEventId: "sevt_tool_1",
					decision: "allow" as const,
				};
				let confirmationCommits = 0;
				const commitConfirmation = async (
					declaration: RuntimeControlInputDeclaration,
				) => {
					confirmationCommits += 1;
					const result = buildRuntimeControlCommitResult(
						"tool_confirmation",
						declaration,
					);
					return result;
				};
				expect(
					await Effect.runPromise(
						manager.resolveToolConfirmation(
							"sesn_1",
							confirmationCommand,
							commitConfirmation,
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					applied: true,
				});
				expect(session?.state.contextManager.entries().at(-1)?.parts).toEqual([
					expect.objectContaining({ type: "text", text: "Approval allowed" }),
				]);
				const confirmationMessage = session?.state.contextManager
					.entries()
					.at(-1);
				expect(confirmationMessage).toBeDefined();
				expect(
					session?.state.threadTurnTransition().checkpoint
						.pendingInputContextSequences,
				).toEqual([confirmationMessage!.messageSequence]);
				expect(threadLoop.runs).toHaveLength(1);
				const messagesAfterConfirmation =
					session?.state.contextManager.entries().length;
				session?.state.removePendingApprovalToolJob("sevt_tool_1");
				expect(
					await Effect.runPromise(
						manager.resolveToolConfirmation(
							"sesn_1",
							confirmationCommand,
							commitConfirmation,
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					applied: false,
				});
				expect(confirmationCommits).toBe(2);
				expect(session?.state.contextManager.entries()).toHaveLength(
					messagesAfterConfirmation ?? 0,
				);

				threadLoop.runs[0]?.release();
				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs[1]?.session).toBe(session);
				threadLoop.runs[1]?.release();
			},
		);
	});

	test("stale control receipts discard the addressed hot thread without applying their payload", async () => {
		const threadLoop = makeControlledThreadLoop();
		const sessionId = "sesn_stale_control_receipt";
		const scope = threadControl(sessionId, "rin_stale_control_receipt");

		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.startTestRunThroughAcceptedInput(sessionId),
					),
				).toMatchObject({
					ok: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				const session = threadLoop.runs[0]!.session;
				session.state.recordPendingApprovalToolJob({
					toolUseEventId: "sevt_stale_control_tool",
					modelRequestId: "mreq_stale_control",
					source: { providerId: "fake", modelId: "fake-chat" },
					assistantMessageSequence: pendingApprovalAssistantEntry(
						sessionId,
						"sevt_stale_control_tool",
					).messageSequence,
					toolPart: pendingApprovalToolPart(
						sessionId,
						"sevt_stale_control_tool",
					),
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
				});

				expect(
					await Effect.runPromise(
						manager.resolveToolConfirmation(
							sessionId,
							{
								...scope,
								toolUseEventId: "sevt_stale_control_tool",
								decision: "allow",
							},
							async () => ({ ok: true, stale: true }),
						),
					),
				).toEqual({
					ok: true,
					sessionId,
					created: false,
					applied: false,
					stale: true,
				});
				expect(session.state.pendingApprovalToolJobs()).toHaveLength(0);
				expect(
					await Effect.runPromise(manager.inspectThread(scope)),
				).toMatchObject({
					ok: true,
					observed: false,
				});
			},
		);
	});

	test("runtime config patch is installed only while the target thread is idle", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.applyRuntimeConfigPatch("sesn_cold", {
							...runtimeConfigScope("sesn_cold", "session:6"),
							generation: 6,
							contentJson: '{"config_generation":6}',
						}),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_cold",
					created: false,
					applied: false,
					noResidency: true,
				});
				expect(threadLoop.runs).toEqual([]);

				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_1", "rin_second_session_1"),
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);

				expect(
					await Effect.runPromise(
						manager.applyRuntimeConfigPatch("sesn_1", {
							...runtimeConfigScope("sesn_1", "session:6"),
							generation: 6,
							contentJson: '{"config_generation":6}',
						}),
					),
				).toEqual({ ok: false, sessionId: "sesn_1", reason: "control_busy" });

				threadLoop.runs[0]?.release();
				let idlePatch: SessionManager.RuntimeControlResult | undefined;
				for (
					let attempt = 0;
					attempt < 100 && idlePatch === undefined;
					attempt += 1
				) {
					const result = await Effect.runPromise(
						manager.applyRuntimeConfigPatch("sesn_1", {
							...runtimeConfigScope("sesn_1", "session:6"),
							generation: 6,
							contentJson: '{"config_generation":6}',
						}),
					);
					if (result.ok) {
						idlePatch = result;
						break;
					}
					await new Promise((resolve) => setTimeout(resolve, 1));
				}
				expect(idlePatch).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					applied: true,
				});
			},
		);
	});

	test("runtime config patch updates every idle resident thread before ACK", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				for (const sessionThreadId of ["thrd_main", "thrd_child"]) {
					expect(
						await Effect.runPromise(
							manager.preloadThread({
								...threadControl(
									"sesn_config_all",
									`rin_preload_${sessionThreadId}`,
									sessionThreadId,
								),
								runtimeBindingToken: "runtime-binding-token",
								contextEntries: [],
							}),
						),
					).toMatchObject({ ok: true, applied: true });
				}
				const patch = {
					...runtimeConfigScope("sesn_config_all", "session:7"),
					generation: 7,
					contentJson: '{"config_generation":7}',
				};
				const expectedPatch = {
					generation: 7,
					contentJson: '{"config_generation":7}',
				};

				expect(
					await Effect.runPromise(
						manager.applyRuntimeConfigPatch("sesn_config_all", patch),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_config_all",
					created: false,
					applied: true,
				});

				for (const sessionThreadId of ["thrd_main", "thrd_child"]) {
					expect(
						await Effect.runPromise(
							manager.acceptInput(
								acceptedInput(
									"sesn_config_all",
									`rin_run_${sessionThreadId}`,
									sessionThreadId,
								),
							),
						),
					).toMatchObject({
						ok: true,
						started: true,
					});
					await waitForRuns(threadLoop, threadLoop.runs.length + 1);
					expect(
						threadLoop.runs.at(-1)?.session.configuration.runtimeConfigPatch(),
					).toEqual(expectedPatch);
					threadLoop.runs.at(-1)?.release();
					await waitForThreadIdle(manager, "sesn_config_all", sessionThreadId);
				}
			},
		);
	});

	test("busy sibling rejects a session config patch without partially updating idle threads", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				for (const sessionThreadId of ["thrd_main", "thrd_child"]) {
					expect(
						await Effect.runPromise(
							manager.preloadThread({
								...threadControl(
									"sesn_config_busy",
									`rin_preload_${sessionThreadId}`,
									sessionThreadId,
								),
								runtimeBindingToken: "runtime-binding-token",
								contextEntries: [],
							}),
						),
					).toMatchObject({ ok: true, applied: true });
				}
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_config_busy", "rin_child_busy", "thrd_child"),
						),
					),
				).toMatchObject({
					ok: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);

				expect(
					await Effect.runPromise(
						manager.applyRuntimeConfigPatch("sesn_config_busy", {
							...runtimeConfigScope("sesn_config_busy", "session:8"),
							generation: 8,
							contentJson: '{"config_generation":8}',
						}),
					),
				).toEqual({
					ok: false,
					sessionId: "sesn_config_busy",
					reason: "control_busy",
				});

				threadLoop.runs[0]?.release();
				await waitForThreadIdle(manager, "sesn_config_busy", "thrd_child");
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput(
								"sesn_config_busy",
								"rin_main_after_reject",
								"thrd_main",
							),
						),
					),
				).toMatchObject({
					ok: true,
					started: true,
				});
				await waitForRuns(threadLoop, 2);
				expect(
					threadLoop.runs[1]?.session.configuration.runtimeConfigPatch(),
				).toBeUndefined();
				threadLoop.runs[1]?.release();
			},
		);
	});

	test("runtime config and MCP manifest generations use independent monotonic stale gates", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_1", "rin_create")),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				const session = threadLoop.runs[0]?.session;
				threadLoop.runs[0]?.release();

				const applyWhenIdle = async (
					command: Parameters<typeof manager.applyRuntimeConfigPatch>[1],
				): Promise<SessionManager.RuntimeControlResult> => {
					for (let attempt = 0; attempt < 100; attempt += 1) {
						const result = await Effect.runPromise(
							manager.applyRuntimeConfigPatch("sesn_1", command),
						);
						if (result.ok) {
							return result;
						}
						await new Promise((resolve) => setTimeout(resolve, 1));
					}
					throw new Error("session did not become idle");
				};

				expect(
					await applyWhenIdle({
						...runtimeConfigScope("sesn_1", "session:5"),
						generation: 5,
						contentJson: '{"config_generation":5}',
					}),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					applied: true,
				});
				expect(
					await applyWhenIdle({
						...runtimeConfigScope("sesn_1", "mcp:github:1"),
						generation: 1,
						mcpServerName: "github",
						manifestETag: "etag_1",
						contentJson:
							'{"mcp_manifest":{"mcp_server_name":"github","manifest_etag":"etag_1","manifest_generation":1,"tools":[]}}',
					}),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					applied: true,
				});
				expect(
					await applyWhenIdle({
						...runtimeConfigScope("sesn_1", "mcp:github:1"),
						generation: 1,
						mcpServerName: "github",
						manifestETag: "etag_1",
						contentJson:
							'{"mcp_manifest":{"mcp_server_name":"github","manifest_etag":"etag_1","manifest_generation":1,"tools":[]}}',
					}),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					applied: false,
				});
				expect(
					await applyWhenIdle({
						...runtimeConfigScope("sesn_1", "session:4"),
						generation: 4,
						contentJson: '{"config_generation":4}',
					}),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					applied: false,
				});
				expect(
					await applyWhenIdle({
						...runtimeConfigScope("sesn_1", "session:6"),
						generation: 6,
						contentJson: '{"config_generation":6}',
					}),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					applied: true,
				});

				expect(session?.configuration.patches()).toEqual([
					{
						generation: 6,
						contentJson: '{"config_generation":6}',
					},
					{
						generation: 1,
						mcpServerName: "github",
						manifestETag: "etag_1",
						contentJson:
							'{"mcp_manifest":{"mcp_server_name":"github","manifest_etag":"etag_1","manifest_generation":1,"tools":[]}}',
					},
				]);
			},
		);
	});

	test("input accepted during interrupt cleanup starts one follow-up after the run scope closes", async () => {
		const threadLoop = makeInterruptCleanupThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_1", "rin_first")),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				const firstSession = threadLoop.runs[0]?.session;

				const interruptCommand = {
					...threadControl("sesn_1", "rin_interrupt"),
					inputOrder: 9,
				};
				const interrupt = Effect.runPromise(
					manager.interruptControl(
						"sesn_1",
						interruptCommand,
						testControlCommit(interruptCommand),
					),
				);
				await threadLoop.cleanupStarted;

				expect(
					await Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_1", "rin_after_interrupt")),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					started: false,
				});

				threadLoop.releaseCleanup();
				expect(await interrupt).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					interrupted: true,
					idleInterrupt: false,
				});
				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs[1]?.session).toBe(firstSession);
				threadLoop.runs[1]?.release();
			},
		);
	});

	test("interrupt discards pre-fence queued input and preserves input accepted during closeout", async () => {
		const threadLoop = makeInterruptCleanupThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput({
							...acceptedInput("sesn_fence", "rin_active"),
							inputOrder: 1,
						}),
					),
				).toMatchObject({
					ok: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				const session = threadLoop.runs[0]?.session;
				session?.state.acknowledgeAcceptedInput("rin_active");

				expect(
					await Effect.runPromise(
						manager.acceptInput({
							...acceptedInput("sesn_fence", "rin_before_fence"),
							inputOrder: 5,
						}),
					),
				).toMatchObject({
					ok: true,
				});
				const interruptCommand = {
					...threadControl("sesn_fence", "rin_interrupt_fence"),
					inputOrder: 9,
				};
				const interrupt = Effect.runPromise(
					manager.interruptControl(
						"sesn_fence",
						interruptCommand,
						testControlCommit(interruptCommand),
					),
				);
				await threadLoop.cleanupStarted;

				expect(
					await Effect.runPromise(
						manager.acceptInput({
							...acceptedInput("sesn_fence", "rin_after_fence"),
							inputOrder: 10,
						}),
					),
				).toMatchObject({
					ok: true,
				});
				threadLoop.releaseCleanup();
				await expect(interrupt).resolves.toMatchObject({
					ok: true,
					interrupted: true,
				});
				await waitForRuns(threadLoop, 2);

				expect(session?.state.acceptedInputCount()).toBe(1);
				expect(session?.state.peekAcceptedInput()?.runtimeInputId).toBe(
					"rin_after_fence",
				);
				threadLoop.runs[1]?.release();
			},
		);
	});

	test("positive interrupt fence preserves queued completion mail for one follow-up presentation", async () => {
		const threadLoop = makeInterruptCleanupThreadLoop();
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
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput({
							...acceptedInput(
								sessionID,
								"rin_interrupt_agent_mail_active",
								threadID,
							),
							inputOrder: 1,
						}),
					),
				).toMatchObject({ ok: true, started: true });
				await waitForRuns(threadLoop, 1);
				threadLoop.runs[0]!.session.state.acknowledgeAcceptedInput(
					"rin_interrupt_agent_mail_active",
				);
				expect(
					await Effect.runPromise(manager.acceptInput(mail)),
				).toMatchObject({
					ok: true,
					started: false,
				});

				const interruptCommand = {
					...threadControl(
						sessionID,
						"rin_interrupt_agent_mail_fence",
						threadID,
					),
					inputOrder: 9,
				};
				const interrupt = Effect.runPromise(
					manager.interruptControl(
						sessionID,
						interruptCommand,
						testControlCommit(interruptCommand),
					),
				);
				await threadLoop.cleanupStarted;
				threadLoop.releaseCleanup();
				await expect(interrupt).resolves.toMatchObject({
					ok: true,
					interrupted: true,
				});

				await waitForRuns(threadLoop, 2);
				expect(
					threadLoop.runs[1]!.session.state.peekAcceptedInput()?.runtimeInputId,
				).toBe(mail.runtimeInputId);
				threadLoop.runs[1]!.session.state.acknowledgeAcceptedInput(
					mail.runtimeInputId,
				);
				threadLoop.runs[1]!.release({
					type: "completed",
					modelMessageCount: 1,
				});
				await waitForThreadIdle(manager, sessionID, threadID);
				expect(
					await Effect.runPromise(manager.acceptInput(mail)),
				).toMatchObject({
					ok: true,
					started: true,
				});
				await waitForRuns(threadLoop, 3);
				threadLoop.runs[2]!.release({
					type: "completed",
					modelMessageCount: 0,
				});
			},
		);
	});

	test("shutdownActiveRuns interrupts active ThreadLoop owner fibers", async () => {
		const threadLoop = makeInterruptRecordingThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_shutdown")),
					),
				).toMatchObject({
					ok: true,
					sessionId: "sesn_shutdown",
					started: true,
				});
				await waitForInterruptRecordingRuns(threadLoop, 1);
				const session = threadLoop.runs[0]?.session;
				if (session === undefined) {
					throw new Error("expected active session");
				}

				await Effect.runPromise(manager.shutdownActiveRuns());

				expect(threadLoop.interruptions).toEqual([
					{
						sessionId: "sesn_shutdown",
						runtimeShutdownRequested: true,
					},
				]);
				expect(session.state.runtimeShutdownRequested()).toBe(false);
				expect(
					await Effect.runPromise(
						manager.cleanupSession(
							"sesn_shutdown",
							cleanupControl("sesn_shutdown"),
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_shutdown",
					cleaned: false,
				});
			},
		);
	});

	test("shutdownActiveRuns reaches active runs without pending interrupt, cleanup, or unbind", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_shutdown")),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_shutdown",
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				const session = threadLoop.runs[0]?.session;
				if (session === undefined) {
					throw new Error("expected active session");
				}
				const shutdown = Effect.runPromise(manager.shutdownActiveRuns());
				await new Promise((resolve) => setTimeout(resolve, 1));
				threadLoop.runs[0]?.release({ type: "interrupted" });
				await shutdown;

				expect(session.state.runtimeShutdownRequested()).toBe(false);
				expect(threadLoop.runs[0]?.args).toHaveLength(2);
				expect(
					await Effect.runPromise(
						manager.cleanupSession(
							"sesn_shutdown",
							cleanupControl("sesn_shutdown"),
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_shutdown",
					cleaned: false,
				});
			},
		);
	});

	test("shutdown joins cold installation and rejects a late preload result", async () => {
		const threadLoop = makeControlledThreadLoop();
		let reportLoadStarted = (): void => undefined;
		let releaseLoad = (): void => undefined;
		const loadStarted = new Promise<void>((resolve) => {
			reportLoadStarted = resolve;
		});
		const loadGate = new Promise<void>((resolve) => {
			releaseLoad = resolve;
		});
		const command = threadControl(
			"sesn_shutdown_install",
			"rin_shutdown_install",
		);
		await withSessionManager(
			sessionManagerLayer(threadLoop, {
				loadThreadContext: async () => {
					reportLoadStarted();
					await loadGate;
					return {
						...command,
						runtimeBindingToken: "runtime-binding-token-shutdown-install",
						contextEntries: [],
						thread: { role: "main", visibility: "public", status: "idle" },
					};
				},
			}),
			async (manager) => {
				const installation = Effect.runPromise(
					manager.ensureThreadInstalled(command),
				);
				await loadStarted;

				await Effect.runPromise(manager.shutdownActiveRuns());
				releaseLoad();

				await expect(installation).resolves.toMatchObject({
					ok: false,
					reason: "context_load_failed",
				});
				expect(await Effect.runPromise(manager.inspectThread(command))).toEqual(
					{
						ok: true,
						sessionId: command.sessionId,
						sessionThreadId: command.sessionThreadId,
						observed: false,
						entries: [],
					},
				);
			},
		);
	});

	test("accepted input restarts an existing idle session with the same hot ContextManager", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.startTestRunThroughAcceptedInput("sesn_1"),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				const firstSession = threadLoop.runs[0]?.session;
				expect(firstSession).toBeDefined();
				firstSession?.state.contextManager.appendEntry({
					messageSequence: 1,
					contextKind: "user",
					parts: [],
				});
				threadLoop.runs[0]?.release();

				for (
					let attempt = 0;
					attempt < 100 && threadLoop.runs.length === 1;
					attempt += 1
				) {
					const result = await Effect.runPromise(
						manager.startTestRunThroughAcceptedInput("sesn_1"),
					);
					if (result.ok && result.started) {
						expect(result).toEqual({
							ok: true,
							sessionId: "sesn_1",
							created: false,
							started: true,
						});
						break;
					}
					await new Promise((resolve) => setTimeout(resolve, 1));
				}
				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs[1]?.session).toBe(firstSession);
				expect(
					threadLoop.runs[1]?.session.state.contextManager.entries(),
				).toHaveLength(1);
				threadLoop.runs[1]?.release();

				for (
					let attempt = 0;
					attempt < 100 && threadLoop.runs.length === 2;
					attempt += 1
				) {
					const result = await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_1", "rin_restart_after_idle"),
						),
					);
					if (result.ok && result.started) {
						expect(result).toEqual({
							ok: true,
							sessionId: "sesn_1",
							created: false,
							started: true,
						});
						break;
					}
					await new Promise((resolve) => setTimeout(resolve, 1));
				}
				await waitForRuns(threadLoop, 3);
				expect(threadLoop.runs[2]?.session).toBe(firstSession);
				expect(
					threadLoop.runs[2]?.session.state.contextManager.entries(),
				).toHaveLength(1);
				threadLoop.runs[2]?.release();
			},
		);
	});

	test("concurrent same-thread accept calls coalesce behind one active run", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				const [first, second, third] = await Promise.all([
					Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_1", "rin_concurrent_1")),
					),
					Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_1", "rin_concurrent_2")),
					),
					Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_1", "rin_concurrent_3")),
					),
				]);

				await waitForRuns(threadLoop, 1);
				expect(
					[first, second, third].filter(
						(result) => result.ok && result.started,
					),
				).toHaveLength(1);
				expect(
					[first, second, third].filter(
						(result) => result.ok && !result.started,
					),
				).toHaveLength(2);
				threadLoop.runs[0]?.release();
				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs.map((run) => run.sessionId)).toEqual([
					"sesn_1",
					"sesn_1",
				]);
				threadLoop.runs[1]?.release();
			},
		);
	});

	test("concurrent thread waiters join one owner run", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				await Effect.runPromise(
					manager.acceptInput(acceptedInput("sesn_join", "rin_join")),
				);
				await waitForRuns(threadLoop, 1);

				const first = Effect.runPromise(
					manager.waitThread(threadControl("sesn_join"), undefined),
				);
				const second = Effect.runPromise(
					manager.waitThread(threadControl("sesn_join"), undefined),
				);
				await new Promise((resolve) => setTimeout(resolve, 5));
				expect(threadLoop.runs).toHaveLength(1);

				threadLoop.runs[0]?.release();
				expect(await Promise.all([first, second])).toEqual([
					expect.objectContaining({
						ok: true,
						observed: true,
						status: "idle",
						timedOut: false,
					}),
					expect.objectContaining({
						ok: true,
						observed: true,
						status: "idle",
						timedOut: false,
					}),
				]);
				expect(threadLoop.runs).toHaveLength(1);
			},
		);
	});

	test("cancelling a joined thread waiter does not cancel the owner run", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				await Effect.runPromise(
					manager.acceptInput(
						acceptedInput("sesn_join_cancel", "rin_join_cancel"),
					),
				);
				await waitForRuns(threadLoop, 1);

				const waiter = Effect.runFork(
					manager.waitThread(threadControl("sesn_join_cancel"), undefined),
				);
				await new Promise((resolve) => setTimeout(resolve, 5));
				await Effect.runPromise(Fiber.interrupt(waiter));

				expect(threadLoop.runs).toHaveLength(1);
				expect(
					await Effect.runPromise(
						manager.inspectThread(threadControl("sesn_join_cancel")),
					),
				).toMatchObject({
					ok: true,
					observed: true,
					status: "running",
				});
				threadLoop.runs[0]?.release();
				await waitForThreadIdle(
					manager,
					"sesn_join_cancel",
					"thrd_sesn_join_cancel",
				);
			},
		);
	});

	test("closing a waiter scope detaches it without consuming pending input", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				await Effect.runPromise(
					manager.acceptInput(acceptedInput("sesn_waiter_scope", "rin_active")),
				);
				await waitForRuns(threadLoop, 1);
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_waiter_scope", "rin_pending"),
						),
					),
				).toMatchObject({
					ok: true,
				});
				const session = threadLoop.runs[0]?.session;
				expect(session?.state.acceptedInputCount()).toBe(2);

				const waiterScope = await Effect.runPromise(Scope.make());
				const waiter = await Effect.runPromise(
					Effect.forkIn(
						manager.waitThread(threadControl("sesn_waiter_scope"), undefined),
						waiterScope,
					),
				);
				await Effect.runPromise(Scope.close(waiterScope, Exit.void));
				const waiterExit = await Effect.runPromise(Fiber.await(waiter));

				expect(Exit.isFailure(waiterExit)).toBe(true);
				expect(session?.state.acceptedInputCount()).toBe(2);
				expect(
					await Effect.runPromise(
						manager.inspectThread(threadControl("sesn_waiter_scope")),
					),
				).toMatchObject({ status: "running" });
				threadLoop.runs[0]?.release();
				await waitForRuns(threadLoop, 2);
				threadLoop.runs[1]?.release();
			},
		);
	});

	test("wake during follow-up scope cleanup starts exactly one later follow-up", async () => {
		const threadLoop = makeFollowUpCleanupThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				await Effect.runPromise(
					manager.acceptInput(
						acceptedInput("sesn_follow_cleanup", "rin_first"),
					),
				);
				await waitForRuns(threadLoop, 1);
				await Effect.runPromise(
					manager.acceptInput(
						acceptedInput("sesn_follow_cleanup", "rin_second"),
					),
				);
				threadLoop.runs[0]?.release();
				await waitForRuns(threadLoop, 2);

				threadLoop.runs[1]?.release();
				await threadLoop.followUpCleanupStarted;
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_follow_cleanup", "rin_during_cleanup"),
						),
					),
				).toMatchObject({
					ok: true,
					started: false,
				});
				threadLoop.releaseFollowUpCleanup();

				await waitForRuns(threadLoop, 3);
				expect(threadLoop.runs).toHaveLength(3);
				threadLoop.runs[2]?.release();
				await waitForThreadIdle(
					manager,
					"sesn_follow_cleanup",
					"thrd_sesn_follow_cleanup",
				);
				expect(threadLoop.runs).toHaveLength(3);
			},
		);
	});

	test("payload-like extra input cannot reach ThreadLoop through SessionManager", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				const unsafeManager = manager as unknown as {
					readonly acceptInput: (
						command: ReturnType<typeof acceptedInput>,
						payload: unknown,
					) => ReturnType<typeof manager.acceptInput>;
				};
				expect(
					await Effect.runPromise(
						unsafeManager.acceptInput(
							acceptedInput("sesn_1", "rin_hostile_first"),
							{ runtimeMessage: hostileText },
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				expect(threadLoop.runs[0]?.args).toHaveLength(2);
				expect(threadLoop.runs[0]?.args[0]).toBe(threadLoop.runs[0]?.session);
				threadLoop.runs[0]?.release();

				for (
					let attempt = 0;
					attempt < 100 && threadLoop.runs.length === 1;
					attempt += 1
				) {
					const result = await Effect.runPromise(
						unsafeManager.acceptInput(acceptedInput("sesn_1"), {
							providerRequest: hostileText,
						}),
					);
					if (result.ok && result.started) {
						break;
					}
					await new Promise((resolve) => setTimeout(resolve, 1));
				}
				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs[1]?.args).toHaveLength(2);
				expect(threadLoop.runs[1]?.args[0]).toBe(threadLoop.runs[1]?.session);
				threadLoop.runs[1]?.release();
			},
		);
	});

	test("different sessions have isolated manager-owned run markers and ContextManagers", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_1", "rin_event_write_initial"),
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: true,
					started: true,
				});
				expect(
					await Effect.runPromise(
						manager.startTestRunThroughAcceptedInput("sesn_2"),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_2",
					created: true,
					started: true,
				});

				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs.map((run) => run.sessionId).sort()).toEqual([
					"sesn_1",
					"sesn_2",
				]);
				expect(threadLoop.runs[0]?.session.state.contextManager).not.toBe(
					threadLoop.runs[1]?.session.state.contextManager,
				);
				expect(
					await Effect.runPromise(
						manager.acceptInput(acceptedInput("sesn_1", "rin_crashed_initial")),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					started: false,
				});
				threadLoop.runs[0]?.release();
				threadLoop.runs[1]?.release();
				await waitForRuns(threadLoop, 3);
				expect(threadLoop.runs.map((run) => run.sessionId).sort()).toEqual([
					"sesn_1",
					"sesn_1",
					"sesn_2",
				]);
				threadLoop.runs[2]?.release();
			},
		);
	});

	test("fatal thread release disposes its approval reviewer manager", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				await Effect.runPromise(
					manager.acceptInput(acceptedInput("sesn_reviewer_release")),
				);
				await waitForRuns(threadLoop, 1);
				const reviewerManager =
					threadLoop.runs[0]?.session.approvalReviewerManager;
				expect(reviewerManager?.isDisposed()).toBe(false);

				threadLoop.runs[0]?.release(fatalRunResult("crashed"));
				for (
					let attempt = 0;
					attempt < 100 && reviewerManager?.isDisposed() !== true;
					attempt += 1
				) {
					await new Promise((resolve) => setTimeout(resolve, 1));
				}
				expect(reviewerManager?.isDisposed()).toBe(true);
			},
		);
	});

	test("terminal public parent release closes its active reviewer resident through run settlement", async () => {
		const sessionId = "sesn_parent_reviewer_release";
		const parentThreadId = "thrd_parent_release";
		const reviewerThreadId = "thrd_reviewer_trunk_release";
		const threadLoop = makeReviewerInterruptCleanupThreadLoop(reviewerThreadId);
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				const unrelatedThreadId = "thrd_unrelated_release";
				await Effect.runPromise(
					manager.preloadThread({
						...threadControl(sessionId, "rin_parent_preload", parentThreadId),
						thread: { role: "main", visibility: "public", status: "idle" },
						runtimeBindingToken: "runtime-binding-token-parent",
						contextEntries: [],
					}),
				);
				await Effect.runPromise(
					manager.acceptInput(
						acceptedInput(sessionId, "rin_parent_release", parentThreadId),
					),
				);
				await Effect.runPromise(
					manager.acceptInput(
						approvalReviewInput(
							sessionId,
							"rin_reviewer_release",
							reviewerThreadId,
							parentThreadId,
						),
					),
				);
				await Effect.runPromise(
					manager.preloadThread({
						...threadControl(
							sessionId,
							"rin_unrelated_release",
							unrelatedThreadId,
						),
						thread: { role: "subagent", visibility: "public", status: "idle" },
						runtimeBindingToken: "runtime-binding-token-unrelated",
						contextEntries: [],
					}),
				);
				await waitForRuns(threadLoop, 2);

				threadLoop.runs[0]?.release(fatalRunResult("terminated"));
				expect(
					await Promise.race([
						threadLoop.reviewerCleanupStarted.then(() => true),
						new Promise<false>((resolve) =>
							setTimeout(() => resolve(false), 100),
						),
					]),
				).toBe(true);
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								sessionId,
								"rin_inspect_reviewer_closing",
								reviewerThreadId,
							),
						),
					),
				).toMatchObject({ observed: true, status: "closed_for_runtime" });
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								sessionId,
								"rin_inspect_parent_during_reviewer_close",
								parentThreadId,
							),
						),
					),
				).toMatchObject({ observed: true });
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								sessionId,
								"rin_inspect_unrelated_during_reviewer_close",
								unrelatedThreadId,
							),
						),
					),
				).toMatchObject({ observed: true });

				threadLoop.releaseReviewerCleanup();
				await waitForCondition(async () => {
					const snapshot = await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								sessionId,
								"rin_inspect_reviewer_released",
								reviewerThreadId,
							),
						),
					);
					return snapshot.ok && snapshot.observed === false;
				}, "reviewer resident release after owner settlement");
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								sessionId,
								"rin_inspect_parent_released",
								parentThreadId,
							),
						),
					),
				).toMatchObject({ observed: false });
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								sessionId,
								"rin_inspect_unrelated",
								unrelatedThreadId,
							),
						),
					),
				).toMatchObject({ observed: true });
			},
		);
	});

	test("session cleanup disposes every thread approval reviewer manager", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				await Effect.runPromise(
					manager.acceptInput(
						acceptedInput("sesn_reviewer_cleanup", "rin_a", "thrd_a"),
					),
				);
				await Effect.runPromise(
					manager.acceptInput(
						acceptedInput("sesn_reviewer_cleanup", "rin_b", "thrd_b"),
					),
				);
				await waitForRuns(threadLoop, 2);
				const reviewerManagers = threadLoop.runs.map(
					(run) => run.session.approvalReviewerManager,
				);
				threadLoop.runs[0]?.release();
				threadLoop.runs[1]?.release();

				const cleanup = await waitForIdleCleanup(
					manager,
					"sesn_reviewer_cleanup",
				);
				expect(cleanup).toEqual({
					ok: true,
					sessionId: "sesn_reviewer_cleanup",
					cleaned: true,
				});
				expect(
					reviewerManagers.every(
						(reviewerManager) => reviewerManager?.isDisposed() === true,
					),
				).toBe(true);
			},
		);
	});

	test("same session id in different workspaces owns separate hot sessions and cleanup", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				const workspaceA = acceptedInput(
					"sesn_shared",
					"rin_workspace_a",
					"thrd_workspace_a",
				);
				const workspaceB = {
					...acceptedInput(
						"sesn_shared",
						"rin_workspace_b",
						"thrd_workspace_b",
					),
					workspaceId: "wksp_other",
					bindingId: "bind_workspace_b",
					targetPodUid: "pod_workspace_b",
				};
				expect(
					await Effect.runPromise(manager.acceptInput(workspaceA)),
				).toEqual({
					ok: true,
					sessionId: "sesn_shared",
					created: true,
					started: true,
				});
				expect(
					await Effect.runPromise(manager.acceptInput(workspaceB)),
				).toEqual({
					ok: true,
					sessionId: "sesn_shared",
					created: true,
					started: true,
				});

				await waitForRuns(threadLoop, 2);
				expect(
					threadLoop.runs.map((run) => run.session.identity.workspaceId).sort(),
				).toEqual(["wksp_other", "wksp_test"]);
				expect(threadLoop.runs[0]?.session.state.contextManager).not.toBe(
					threadLoop.runs[1]?.session.state.contextManager,
				);
				expect(threadLoop.runs[0]?.session.toolCoordinator).not.toBe(
					threadLoop.runs[1]?.session.toolCoordinator,
				);
				threadLoop.runs[0]?.release();
				threadLoop.runs[1]?.release();

				const cleanupA = await waitForIdleCleanup(manager, "sesn_shared");
				expect(cleanupA).toEqual({
					ok: true,
					sessionId: "sesn_shared",
					cleaned: true,
				});
				let cleanupB: SessionManager.CleanupSessionResult = {
					ok: false,
					sessionId: "sesn_shared",
					reason: "session_busy",
				};
				for (let attempt = 0; attempt < 100 && !cleanupB.ok; attempt += 1) {
					cleanupB = await Effect.runPromise(
						manager.cleanupSession("sesn_shared", {
							...cleanupControl("sesn_shared", "cleanup_workspace_b"),
							workspaceId: "wksp_other",
							bindingId: "bind_workspace_b",
							targetPodUid: "pod_workspace_b",
						}),
					);
					if (!cleanupB.ok) {
						await new Promise((resolve) => setTimeout(resolve, 1));
					}
				}
				expect(cleanupB).toEqual({
					ok: true,
					sessionId: "sesn_shared",
					cleaned: true,
				});
			},
		);
	});

	test("same session different threads own independent run slots and ContextManagers", async () => {
		const threadLoop = makeControlledThreadLoop();
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_1", "rin_thread_a", "thrd_a"),
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: true,
					started: true,
				});
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_1", "rin_thread_b", "thrd_b"),
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					started: true,
				});

				await waitForRuns(threadLoop, 2);
				expect(
					threadLoop.runs
						.map((run) => run.session.identity.sessionThreadId)
						.sort(),
				).toEqual(["thrd_a", "thrd_b"]);
				expect(threadLoop.runs[0]?.session.state.contextManager).not.toBe(
					threadLoop.runs[1]?.session.state.contextManager,
				);
				expect(threadLoop.runs[0]?.session.toolCoordinator).toBe(
					threadLoop.runs[1]?.session.toolCoordinator,
				);
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput("sesn_1", "rin_thread_a_follow", "thrd_a"),
						),
					),
				).toEqual({
					ok: true,
					sessionId: "sesn_1",
					created: false,
					started: false,
				});

				threadLoop.runs[0]?.release();
				threadLoop.runs[1]?.release();
				await waitForRuns(threadLoop, 3);
				expect(threadLoop.runs[2]?.session.identity.sessionThreadId).toBe(
					"thrd_a",
				);
				expect(threadLoop.runs[2]?.session).toBe(
					threadLoop.runs.find(
						(run) => run.session.identity.sessionThreadId === "thrd_a",
					)?.session,
				);
				threadLoop.runs[2]?.release();
			},
		);
	});

	test("cleanup rejects running sessions, removes idle sessions without durable unbind, and releases capacity", async () => {
		const threadLoop = makeControlledThreadLoop();
		const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

		await withSessionManager(layer, async (manager) => {
			await Effect.runPromise(
				Effect.gen(function* () {
					expect(
						yield* manager.cleanupSession("missing", cleanupControl("missing")),
					).toEqual({ ok: true, sessionId: "missing", cleaned: false });
					expect(
						yield* manager.startTestRunThroughAcceptedInput("sesn_1"),
					).toEqual({
						ok: true,
						sessionId: "sesn_1",
						created: true,
						started: true,
					});
					expect(
						yield* manager.startTestRunThroughAcceptedInput("sesn_2"),
					).toEqual({
						ok: false,
						sessionId: "sesn_2",
						reason: "local_session_capacity_exceeded",
					});
					expect(
						yield* manager.cleanupSession("sesn_1", cleanupControl("sesn_1")),
					).toEqual({ ok: false, sessionId: "sesn_1", reason: "session_busy" });
				}),
			);

			await waitForRuns(threadLoop, 1);
			threadLoop.runs[0]?.release();

			expect(await waitForIdleCleanup(manager, "sesn_1")).toEqual({
				ok: true,
				sessionId: "sesn_1",
				cleaned: true,
			});
			expect(
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_2"),
				),
			).toEqual({
				ok: true,
				sessionId: "sesn_2",
				created: true,
				started: true,
			});
			await waitForRuns(threadLoop, 2);
			threadLoop.runs[1]?.release();
		});
	});

	test("cleanup rejects an inter-turn accepted queue even when no run slot is active", async () => {
		const threadLoop = makeControlledThreadLoop();
		const sessionID = "sesn_cleanup_inter_turn_queue";
		const threadID = "thrd_cleanup_inter_turn_queue";
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput(
								sessionID,
								"rin_cleanup_inter_turn_initial",
								threadID,
							),
						),
					),
				).toMatchObject({ ok: true, started: true });
				await waitForRuns(threadLoop, 1);
				const session = threadLoop.runs[0]!.session;
				threadLoop.runs[0]!.release({
					type: "completed",
					modelMessageCount: 1,
				});
				await waitForThreadIdle(manager, sessionID, threadID);

				const queued = acceptedInput(
					sessionID,
					"rin_cleanup_inter_turn_queued",
					threadID,
				);
				expect(session.state.enqueueAcceptedInput(queued)).toBe("applied");
				expect(
					await Effect.runPromise(
						manager.cleanupSession(
							sessionID,
							cleanupControl(sessionID, "cleanup_inter_turn_busy"),
						),
					),
				).toEqual({ ok: false, sessionId: sessionID, reason: "session_busy" });

				session.state.acknowledgeAcceptedInput(queued.runtimeInputId);
				expect(
					await Effect.runPromise(
						manager.cleanupSession(
							sessionID,
							cleanupControl(sessionID, "cleanup_inter_turn_drained"),
						),
					),
				).toEqual({ ok: true, sessionId: sessionID, cleaned: true });
			},
		);
	});

	test("cleanup rejects an admitted task while its owning run has not settled it", async () => {
		const threadLoop = makeControlledThreadLoop();
		const sessionId = "sesn_cleanup_receipt_awaiting";
		const threadId = "thrd_cleanup_receipt_awaiting";
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl(
								sessionId,
								"rin_cleanup_receipt_preload",
								threadId,
							),
							runtimeBindingToken: "runtime-binding-token",
							contextEntries: [],
						}),
					),
				).toMatchObject({ ok: true, applied: true });

				expect(
					await Effect.runPromise(
						manager.commitTaskNotification(sessionId, {
							...threadControl(
								sessionId,
								"rin_cleanup_receipt_notification",
								threadId,
							),
							inputOrder: 1,
							taskId: "task_cleanup_receipt",
							sourceToolUseEventId: "sevt_cleanup_receipt",
							status: "completed",
							notificationJson: '{"status":"completed"}',
						}),
					),
				).toMatchObject({ ok: true, applied: true });
				await waitForRuns(threadLoop, 1);

				expect(
					await Effect.runPromise(
						manager.cleanupSession(
							sessionId,
							cleanupControl(sessionId, "cleanup_receipt_attempt"),
						),
					),
				).toEqual({ ok: false, sessionId, reason: "session_busy" });

				threadLoop.runs[0]!.release();
			},
		);
	});

	test("fatal persistence run discards hot state and releases local capacity", async () => {
		const threadLoop = makeControlledThreadLoop();
		const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

		await withSessionManager(layer, async (manager) => {
			expect(
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_1"),
				),
			).toEqual({
				ok: true,
				sessionId: "sesn_1",
				created: true,
				started: true,
			});
			expect(
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_2"),
				),
			).toEqual({
				ok: false,
				sessionId: "sesn_2",
				reason: "local_session_capacity_exceeded",
			});
			await waitForRuns(threadLoop, 1);
			threadLoop.runs[0]?.release(fatalRunResult("persistence_failed"));

			for (
				let attempt = 0;
				attempt < 100 && threadLoop.runs.length === 1;
				attempt += 1
			) {
				const result = await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_2"),
				);
				if (result.ok && result.started) {
					expect(result).toEqual({
						ok: true,
						sessionId: "sesn_2",
						created: true,
						started: true,
					});
					break;
				}
				await new Promise((resolve) => setTimeout(resolve, 1));
			}

			await waitForRuns(threadLoop, 2);
			expect(threadLoop.runs[1]?.sessionId).toBe("sesn_2");
			expect(
				await Effect.runPromise(
					manager.cleanupSession("sesn_1", cleanupControl("sesn_1")),
				),
			).toEqual({ ok: true, sessionId: "sesn_1", cleaned: false });
			threadLoop.runs[1]?.release();
		});
	});

	test("value-level continuation hands queued completion mail to the next hot owner", async () => {
		const threadLoop = makeControlledThreadLoop();
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
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl(
								sessionID,
								"rin_preload_failed_agent_mail_redelivery",
								threadID,
							),
							runtimeBindingToken: "runtime-binding-token",
							contextEntries: [],
							thread,
						}),
					),
				).toMatchObject({ ok: true, applied: true });
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput(
								sessionID,
								"rin_failed_agent_mail_active",
								threadID,
							),
						),
					),
				).toMatchObject({ ok: true, started: true });
				await waitForRuns(threadLoop, 1);
				expect(
					await Effect.runPromise(manager.acceptInput(mail)),
				).toMatchObject({
					ok: true,
					started: false,
				});

				const failure = fatalRunResult("persistence_failed");
				if (failure.type !== "failed") {
					throw new Error("expected failed run fixture");
				}
				threadLoop.runs[0]?.release({
					type: "failed",
					error: failure.error,
					closeoutDisposition: "continuation",
				});
				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs[1]?.session).toBe(threadLoop.runs[0]?.session);
				expect(
					threadLoop.runs[1]?.session.state.peekAcceptedInput()?.runtimeInputId,
				).toBe(mail.runtimeInputId);
				threadLoop.runs[1]?.release({
					type: "completed",
					modelMessageCount: 1,
				});
			},
		);
	});

	test("a user input accepted during failed-run closeout starts the next hot run", async () => {
		let duplicateCloseoutCalls = 0;
		const threadLoop = makeControlledThreadLoop({
			closeFailedRun: () =>
				Effect.sync(() => {
					duplicateCloseoutCalls += 1;
					return {
						type: "landed" as const,
						disposition: "continuation" as const,
					};
				}),
		});
		const sessionID = "sesn_failed_turn_follow_up";
		const threadID = "thrd_failed_turn_follow_up";
		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput(sessionID, "rin_failed_turn_initial", threadID),
						),
					),
				).toMatchObject({ ok: true, started: true });
				await waitForRuns(threadLoop, 1);
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput(sessionID, "rin_failed_turn_follow_up", threadID),
						),
					),
				).toMatchObject({ ok: true, started: false });

				const failure = fatalRunResult("crashed");
				if (failure.type !== "failed") {
					throw new Error("expected failed run fixture");
				}
				threadLoop.runs[0]?.release({
					type: "failed",
					error: failure.error,
					closeoutDisposition: "continuation",
				});
				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs[1]?.session).toBe(threadLoop.runs[0]?.session);
				expect(
					threadLoop.runs[1]?.session.state.peekAcceptedInput()?.runtimeInputId,
				).toBe("rin_failed_turn_follow_up");
				expect(duplicateCloseoutCalls).toBe(0);
				threadLoop.runs[1]?.release();
			},
		);
	});

	test("child close fences failed-closeout continuation before and after its decision", async () => {
		for (const closeOrdering of ["before_continuation", "after_continuation"] as const) {
			let closeoutStartedResolve: () => void = () => {};
			let closeoutReleaseResolve: () => void = () => {};
			const closeoutStarted = new Promise<void>((resolve) => {
				closeoutStartedResolve = resolve;
			});
			const closeoutRelease = new Promise<void>((resolve) => {
				closeoutReleaseResolve = resolve;
			});
			const threadLoop = makeControlledCrashThreadLoop("die", {
				closeFailedRun: () =>
					Effect.promise(async () => {
						closeoutStartedResolve();
						await closeoutRelease;
						return {
							type: "landed" as const,
							disposition: "continuation" as const,
						};
					}),
			});
			const sessionId = `sesn_child_closeout_${closeOrdering}`;
			const childId = `thrd_child_closeout_${closeOrdering}`;

			await withSessionManager(
				sessionManagerLayer(threadLoop),
				async (manager) => {
					expect(
						await Effect.runPromise(
							manager.preloadThread({
								...threadControl(sessionId, "rin_preload", childId),
								runtimeBindingToken: "runtime-binding-token",
								contextEntries: [],
								thread: {
									parentThreadId: `thrd_${sessionId}`,
									role: "subagent",
									visibility: "public",
									taskName: "closing-child",
									agentType: "worker",
									status: "idle",
								},
							}),
						),
					).toMatchObject({ ok: true, applied: true });
					expect(
						await Effect.runPromise(
							manager.acceptInput(
								acceptedInput(sessionId, "rin_initial", childId),
							),
						),
					).toMatchObject({ ok: true, started: true });
					await waitForCrashRuns(threadLoop, 1);
					expect(
						await Effect.runPromise(
							manager.acceptInput(
								acceptedInput(sessionId, "rin_follower", childId),
							),
						),
					).toMatchObject({ ok: true, started: false });

					threadLoop.runs[0]?.releaseCrash();
					await closeoutStarted;
					let closePromise: Promise<SessionManager.ThreadLifecycleResult>;
					if (closeOrdering === "before_continuation") {
						closePromise = Effect.runPromise(
							manager.markThreadClosed(
								threadControl(sessionId, "rin_close", childId),
							),
						);
						await Promise.resolve();
						closeoutReleaseResolve();
					} else {
						closeoutReleaseResolve();
						await waitForCrashRuns(threadLoop, 2);
						closePromise = Effect.runPromise(
							manager.markThreadClosed(
								threadControl(sessionId, "rin_close", childId),
							),
						);
					}
					expect(await closePromise).toMatchObject({ ok: true, applied: true });
					expect(threadLoop.runs).toHaveLength(
						closeOrdering === "before_continuation" ? 1 : 2,
					);
					expect(
						await Effect.runPromise(
							manager.inspectThread(
								threadControl(sessionId, "rin_inspect", childId),
							),
						),
					).toMatchObject({ observed: false });
				},
			);
		}
	});

	test("durable failed-closeout disposition owns every accepted follower family", async () => {
		for (const disposition of ["continuation", "terminal"] as const) {
			for (const inputKind of [
				"messages",
				"inter_agent_message",
				"task_notification",
				"rejection",
			] as const) {
				let closeoutStartedResolve: () => void = () => {};
				let closeoutReleaseResolve: () => void = () => {};
				const closeoutStarted = new Promise<void>((resolve) => {
					closeoutStartedResolve = resolve;
				});
				const closeoutRelease = new Promise<void>((resolve) => {
					closeoutReleaseResolve = resolve;
				});
				const threadLoop = makeControlledCrashThreadLoop("die", {
					closeFailedRun: () =>
						Effect.promise(async () => {
							closeoutStartedResolve();
							await closeoutRelease;
							return {
								type: "landed",
								disposition,
							} as unknown as FailedRunCloseoutResult;
						}),
				});
				const sessionId = `sesn_closeout_${disposition}_${inputKind}`;
				const threadId = `thrd_closeout_${disposition}_${inputKind}`;

				await withSessionManager(
					sessionManagerLayer(threadLoop),
					async (manager) => {
						expect(
							await Effect.runPromise(
								manager.acceptInput(
									acceptedInput(sessionId, `rin_initial_${inputKind}`, threadId),
								),
							),
						).toMatchObject({ ok: true, started: true });
						await waitForCrashRuns(threadLoop, 1);
						threadLoop.runs[0]?.session.state.acknowledgeAcceptedInput(
							`rin_initial_${inputKind}`,
						);
						threadLoop.runs[0]?.releaseCrash();
						await closeoutStarted;

						const runtimeInputId = `rin_follower_${disposition}_${inputKind}`;
						if (inputKind === "task_notification") {
							expect(
								await Effect.runPromise(
									manager.commitTaskNotification(sessionId, {
										...threadControl(sessionId, runtimeInputId, threadId),
										inputOrder: 2,
										taskId: `task_${disposition}`,
										sourceToolUseEventId: `sevt_${disposition}`,
										status: "completed",
										notificationJson: '{"status":"completed"}',
									}),
								),
							).toMatchObject({ ok: true, applied: true });
						} else {
							let follower: RuntimeAcceptedInputState;
							if (inputKind === "messages") {
								follower = acceptedInput(sessionId, runtimeInputId, threadId);
							} else if (inputKind === "inter_agent_message") {
								follower = agentMailInput(
									sessionId,
									runtimeInputId,
									threadId,
									`thrd_source_${disposition}`,
									{ role: "main", visibility: "public", status: "running" },
								);
							} else {
								const { contentJson: _contentJson, ...scope } = acceptedInput(
									sessionId,
									runtimeInputId,
									threadId,
								);
								follower = {
									...scope,
									kind: "rejection",
									reasonCode: "runtime_command_rejected",
								};
							}
							expect(
								await Effect.runPromise(manager.acceptInput(follower)),
							).toMatchObject({ ok: true, started: false });
						}

						closeoutReleaseResolve();
						if (disposition === "continuation") {
							await waitForCrashRuns(threadLoop, 2);
							expect(
								threadLoop.runs[1]?.session.state.peekAcceptedInput(),
							).toMatchObject({ kind: inputKind, runtimeInputId });
						} else {
							await waitForCondition(async () => {
								const snapshot = await Effect.runPromise(
									manager.inspectThread(threadControl(sessionId, "rin_inspect", threadId)),
								);
								return snapshot.ok && !snapshot.observed;
							}, "terminal failed-closeout release");
							expect(threadLoop.runs).toHaveLength(1);
						}
					},
				);
			}
		}
	});

	test("discard-requested interruption removes hot state before the next command", async () => {
		const threadLoop = makeControlledThreadLoop();
		const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

		await withSessionManager(layer, async (manager) => {
			expect(
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_discard_hot_state"),
				),
			).toEqual({
				ok: true,
				sessionId: "sesn_discard_hot_state",
				created: true,
				started: true,
			});
			await waitForRuns(threadLoop, 1);
			const staleSession = threadLoop.runs[0]?.session;
			threadLoop.runs[0]?.release({
				type: "interrupted",
				discardHotState: true,
			});

			let accepted: SessionManager.AcceptInputResult | undefined;
			for (
				let attempt = 0;
				attempt < 100 && accepted === undefined;
				attempt += 1
			) {
				const result = await Effect.runPromise(
					manager.acceptInput(
						acceptedInput("sesn_discard_hot_state", "rin_after_discard"),
					),
				);
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
			});
			await waitForRuns(threadLoop, 2);
			expect(threadLoop.runs[1]?.session).not.toBe(staleSession);
			threadLoop.runs[1]?.release();
		});
	});

	test("fatal event-write run drops pending restart and discards hot state", async () => {
		const threadLoop = makeControlledThreadLoop();
		const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

		await withSessionManager(layer, async (manager) => {
			expect(
				await Effect.runPromise(
					manager.acceptInput(
						acceptedInput("sesn_1", "rin_event_write_initial"),
					),
				),
			).toEqual({
				ok: true,
				sessionId: "sesn_1",
				created: true,
				started: true,
			});
			await waitForRuns(threadLoop, 1);
			expect(
				await Effect.runPromise(
					manager.acceptInput(
						acceptedInput("sesn_1", "rin_event_write_follow"),
					),
				),
			).toEqual({
				ok: true,
				sessionId: "sesn_1",
				created: false,
				started: false,
			});
			threadLoop.runs[0]?.release(fatalRunResult("event_write_failed"));

			for (
				let attempt = 0;
				attempt < 100 && threadLoop.runs.length === 1;
				attempt += 1
			) {
				const result = await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_2"),
				);
				if (result.ok && result.started) {
					expect(result).toEqual({
						ok: true,
						sessionId: "sesn_2",
						created: true,
						started: true,
					});
					break;
				}
				await new Promise((resolve) => setTimeout(resolve, 1));
			}

			await waitForRuns(threadLoop, 2);
			expect(threadLoop.runs.map((run) => run.sessionId)).toEqual([
				"sesn_1",
				"sesn_2",
			]);
			threadLoop.runs[1]?.release();
		});
	});

	test("fatal crashed run clears state, drops pending restart, and discards hot state", async () => {
		const threadLoop = makeControlledThreadLoop();
		const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

		await withSessionManager(layer, async (manager) => {
			expect(
				await Effect.runPromise(
					manager.acceptInput(acceptedInput("sesn_1", "rin_crashed_initial")),
				),
			).toEqual({
				ok: true,
				sessionId: "sesn_1",
				created: true,
				started: true,
			});
			await waitForRuns(threadLoop, 1);
			threadLoop.runs[0]?.session.state.contextManager.appendEntry({
				messageSequence: 1,
				contextKind: "user",
				parts: [],
			});
			expect(
				await Effect.runPromise(
					manager.acceptInput(acceptedInput("sesn_1", "rin_crashed_follow")),
				),
			).toEqual({
				ok: true,
				sessionId: "sesn_1",
				created: false,
				started: false,
			});
			threadLoop.runs[0]?.release(fatalRunResult("crashed"));

			for (
				let attempt = 0;
				attempt < 100 && threadLoop.runs.length === 1;
				attempt += 1
			) {
				const result = await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_2"),
				);
				if (result.ok && result.started) {
					expect(result).toEqual({
						ok: true,
						sessionId: "sesn_2",
						created: true,
						started: true,
					});
					break;
				}
				await new Promise((resolve) => setTimeout(resolve, 1));
			}

			await waitForRuns(threadLoop, 2);
			expect(
				threadLoop.runs[0]?.session.state.contextManager.entries(),
			).toEqual([]);
			expect(
				await Effect.runPromise(
					manager.cleanupSession("sesn_1", cleanupControl("sesn_1")),
				),
			).toEqual({ ok: true, sessionId: "sesn_1", cleaned: false });
			threadLoop.runs[1]?.release();
		});
	});

	test("ThreadLoop Effect failure clears admitted hot input and releases capacity", async () => {
		const threadLoop = makeControlledCrashThreadLoop("fail");
		const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

		await withSessionManager(layer, async (manager) => {
			expect(
				await Effect.runPromise(
					manager.acceptInput(acceptedInput("sesn_fail", "rin_fail_initial")),
				),
			).toEqual({
				ok: true,
				sessionId: "sesn_fail",
				created: true,
				started: true,
			});
			await waitForCrashRuns(threadLoop, 1);
			threadLoop.runs[0]?.session.state.contextManager.appendEntry({
				messageSequence: 1,
				contextKind: "user",
				parts: [],
			});
			expect(
				await Effect.runPromise(
					manager.acceptInput(acceptedInput("sesn_fail", "rin_fail_follow")),
				),
			).toEqual({
				ok: true,
				sessionId: "sesn_fail",
				created: false,
				started: false,
			});

			threadLoop.runs[0]?.releaseCrash();
			let replacement: TestRunStartResult | undefined;
			for (
				let attempt = 0;
				attempt < 100 && replacement === undefined;
				attempt += 1
			) {
				const result = await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("replacement_fail"),
				);
				if (result.ok && result.started) {
					replacement = result;
					break;
				}
				await new Promise((resolve) => setTimeout(resolve, 1));
			}
			expect(replacement).toEqual({
				ok: true,
				sessionId: "replacement_fail",
				created: true,
				started: true,
			});
			expect(
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_fail"),
				),
			).toEqual({
				ok: false,
				sessionId: "sesn_fail",
				reason: "local_session_capacity_exceeded",
			});
			await waitForCrashRuns(threadLoop, 2);
			expect(threadLoop.runs[1]?.sessionId).toBe("replacement_fail");
			expect(
				threadLoop.runs[0]?.session.state.contextManager.entries(),
			).toEqual([]);
			expect(
				await Effect.runPromise(
					manager.cleanupSession("sesn_fail", cleanupControl("sesn_fail")),
				),
			).toEqual({ ok: true, sessionId: "sesn_fail", cleaned: false });
		});
	});

	test("ThreadLoop Effect defect removes crashed entry and permits a later accepted-input run", async () => {
		const threadLoop = makeControlledCrashThreadLoop("die");
		const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

		await withSessionManager(layer, async (manager) => {
			expect(
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_die"),
				),
			).toEqual({
				ok: true,
				sessionId: "sesn_die",
				created: true,
				started: true,
			});
			await waitForCrashRuns(threadLoop, 1);
			const firstSession = threadLoop.runs[0]?.session;
			firstSession?.state.contextManager.appendEntry({
				messageSequence: 1,
				contextKind: "user",
				parts: [],
			});
			threadLoop.runs[0]?.releaseCrash();

			for (
				let attempt = 0;
				attempt < 100 && threadLoop.runs.length === 1;
				attempt += 1
			) {
				const result = await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_die"),
				);
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

			await waitForCrashRuns(threadLoop, 2);
			expect(threadLoop.runs[1]?.session).not.toBe(firstSession);
			expect(
				threadLoop.runs[1]?.session.state.contextManager.entries(),
			).toEqual([]);
		});
	});

	test("ThreadLoop defect retains the live thread until durable closeout finishes", async () => {
		let closeoutStartedResolve: () => void = () => {};
		let closeoutReleaseResolve: () => void = () => {};
		const closeoutStarted = new Promise<void>((resolve) => {
			closeoutStartedResolve = resolve;
		});
		const closeoutRelease = new Promise<void>((resolve) => {
			closeoutReleaseResolve = resolve;
		});
		const threadLoop = makeControlledCrashThreadLoop("die", {
			closeFailedRun: () =>
				Effect.promise(async () => {
					closeoutStartedResolve();
					await closeoutRelease;
					return {
						type: "landed" as const,
						disposition: "terminal" as const,
					};
				}),
		});
		const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

		await withSessionManager(layer, async (manager) => {
			expect(
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_closeout_fence"),
				),
			).toMatchObject({
				ok: true,
				started: true,
			});
			await waitForCrashRuns(threadLoop, 1);
			threadLoop.runs[0]?.releaseCrash();
			await closeoutStarted;

			expect(
				await Effect.runPromise(
					manager.inspectThread(threadControl("sesn_closeout_fence")),
				),
			).toMatchObject({
				observed: true,
			});
			expect(
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput(
						"sesn_replacement_before_closeout",
					),
				),
			).toEqual({
				ok: false,
				sessionId: "sesn_replacement_before_closeout",
				reason: "local_session_capacity_exceeded",
			});
			expect(
				await Effect.runPromise(
					manager.cleanupSession(
						"sesn_closeout_fence",
						cleanupControl("sesn_closeout_fence"),
					),
				),
			).toEqual({
				ok: false,
				sessionId: "sesn_closeout_fence",
				reason: "session_busy",
			});

			closeoutReleaseResolve();
			await waitForCondition(async () => {
				const snapshot = await Effect.runPromise(
					manager.inspectThread(threadControl("sesn_closeout_fence")),
				);
				return snapshot.ok && !snapshot.observed;
			}, "failed-run release after durable closeout");
			expect(
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput(
						"sesn_replacement_after_closeout",
					),
				),
			).toMatchObject({
				ok: true,
				started: true,
			});
			await waitForCrashRuns(threadLoop, 2);
		});
	});

	test("failed-run closeout retries whole-envelope attempts with exponential backoff before release", async () => {
		let attempts = 0;
		const sleeps: number[] = [];
		const events: SessionManager.RuntimeCloseoutEvent[] = [];
		const threadLoop = makeControlledCrashThreadLoop("die", {
			closeFailedRun: () =>
				Effect.sync(() => {
					attempts += 1;
					return attempts < 3
						? {
								type: "retry" as const,
								error: normalizeSessionEventWriterError({
									code: "unavailable",
								}),
							}
						: {
								type: "landed" as const,
								disposition: "terminal" as const,
							};
				}),
		});
		const layer = sessionManagerLayer(threadLoop, {
			maxLocalSessions: 1,
			closeoutMonotonicMs: () => 1_000,
			closeoutSleep: async (durationMs) => {
				sleeps.push(durationMs);
				return true;
			},
			recordCloseoutEvent: (event) => events.push(event),
		});

		await withSessionManager(layer, async (manager) => {
			await Effect.runPromise(
				manager.startTestRunThroughAcceptedInput("sesn_closeout_retry"),
			);
			await waitForCrashRuns(threadLoop, 1);
			threadLoop.runs[0]?.releaseCrash();
			await waitForCondition(async () => {
				const snapshot = await Effect.runPromise(
					manager.inspectThread(threadControl("sesn_closeout_retry")),
				);
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
		const threadLoop = makeControlledCrashThreadLoop("die", {
			closeFailedRun: () =>
				Effect.sync(() => {
					attempts += 1;
					return ledgerAvailable
						? {
								type: "landed" as const,
								disposition: "terminal" as const,
							}
						: {
								type: "retry" as const,
								error: normalizeSessionEventWriterError({
									code: "unavailable",
								}),
							};
				}),
		});
		const layer = sessionManagerLayer(threadLoop, {
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
					signal.addEventListener("abort", () => resolve(false), {
						once: true,
					});
				}),
			recordCloseoutEvent: (event) => {
				events.push(event);
				if (event.event === "runtime_closeout_recovered") {
					throw new Error("recovered sink failed");
				}
			},
		});

		await withSessionManager(layer, async (manager) => {
			await Effect.runPromise(
				manager.startTestRunThroughAcceptedInput("sesn_closeout_stall_a"),
			);
			await waitForCrashRuns(threadLoop, 1);
			const joinedA = Effect.runPromise(
				manager.waitThread(threadControl("sesn_closeout_stall_a"), undefined),
			);
			threadLoop.runs[0]?.releaseCrash();
			await waitForCondition(
				() => Promise.resolve(sleepers.length === 1),
				"old closeout backoff",
			);

			now = SessionManager.CloseoutStalledAlarmThresholdMs - 1;
			await Effect.runPromise(
				manager.startTestRunThroughAcceptedInput("sesn_closeout_stall_b"),
			);
			await waitForCrashRuns(threadLoop, 2);
			const joinedB = Effect.runPromise(
				manager.waitThread(threadControl("sesn_closeout_stall_b"), undefined),
			);
			threadLoop.runs[1]?.releaseCrash();
			await waitForCondition(
				() => Promise.resolve(sleepers.length === 2),
				"fresh closeout backoff",
			);

			now = SessionManager.CloseoutStalledAlarmThresholdMs;
			sleepers.shift()?.();
			await waitForCondition(
				() =>
					Promise.resolve(
						events.some(
							(event) => event.event === "runtime_closeout_stalled",
						) && sleepers.length === 2,
					),
				"stalled closeout alarm",
			);
			expect(events).toEqual([
				{
					event: "runtime_closeout_stalled",
					activeCloseouts: 1,
				},
			]);
			sleepers.shift()?.();
			await waitForCondition(
				() => Promise.resolve(sleepers.length === 2),
				"old and fresh retry backoffs",
			);

			ledgerAvailable = true;
			sleepers.splice(0).forEach((release) => release());
			await waitForCondition(async () => {
				const [left, right] = await Promise.all([
					Effect.runPromise(
						manager.inspectThread(threadControl("sesn_closeout_stall_a")),
					),
					Effect.runPromise(
						manager.inspectThread(threadControl("sesn_closeout_stall_b")),
					),
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

	test("superseded closeout releases while unrepairable accepted custody remains resident", async () => {
		for (const disposition of ["superseded", "unrepairable"] as const) {
			const events: SessionManager.RuntimeCloseoutEvent[] = [];
			const threadLoop = makeControlledCrashThreadLoop("die", {
				closeFailedRun: () =>
					Effect.succeed({
						type: disposition,
						error: normalizeSessionEventWriterError({
							code: disposition,
						}),
					}),
			});
			const layer = sessionManagerLayer(threadLoop, {
				maxLocalSessions: 1,
				recordCloseoutEvent: (event) => events.push(event),
			});

			await withSessionManager(layer, async (manager) => {
				const sessionId = `sesn_closeout_${disposition}`;
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput(sessionId),
				);
				await waitForCrashRuns(threadLoop, 1);
				threadLoop.runs[0]?.releaseCrash();
				await waitForCondition(async () => {
					const snapshot = await Effect.runPromise(
						manager.inspectThread(threadControl(sessionId)),
					);
					return (
						snapshot.ok &&
						snapshot.observed === (disposition === "unrepairable")
					);
				}, `${disposition} closeout disposition`);
				if (disposition === "unrepairable") {
					expect(threadLoop.runs[0]?.session.state.acceptedInputCount()).toBe(
						1,
					);
					const interrupt = {
						...threadControl(sessionId, "rin_unrepairable_interrupt"),
						inputOrder: 9,
					};
					expect(
						await Effect.runPromise(
							manager.interruptControl(
								sessionId,
								interrupt,
								testControlCommit(interrupt),
							),
						),
					).toMatchObject({
						ok: true,
						interrupted: false,
						idleInterrupt: true,
					});
					expect(threadLoop.runs[0]?.session.state.acceptedInputCount()).toBe(
						0,
					);
					expect(
						await Effect.runPromise(
							manager.inspectThread(threadControl(sessionId)),
						),
					).toMatchObject({ ok: true, observed: true, status: "idle" });
				}
			});

			expect(events).toEqual(
				disposition === "unrepairable"
					? [
							{
								event: "runtime_closeout_unrepairable",
								activeCloseouts: 1,
								errorCode: "unrepairable",
							},
						]
					: [],
			);
		}
	});

	test("a throwing unrepairable observer cannot bypass the accepted-custody fence", async () => {
		const threadLoop = makeControlledCrashThreadLoop("die", {
			closeFailedRun: () =>
				Effect.succeed({
					type: "unrepairable",
					error: normalizeSessionEventWriterError({ code: "ack_mismatch" }),
				}),
		});
		const layer = sessionManagerLayer(threadLoop, {
			maxLocalSessions: 1,
			recordCloseoutEvent: () => {
				throw new Error("closeout observer failed");
			},
		});

		await withSessionManager(layer, async (manager) => {
			await Effect.runPromise(
				manager.startTestRunThroughAcceptedInput(
					"sesn_closeout_observer_defect",
				),
			);
			await waitForCrashRuns(threadLoop, 1);
			const joined = Effect.runPromise(
				manager.waitThread(
					threadControl("sesn_closeout_observer_defect"),
					undefined,
				),
			);
			threadLoop.runs[0]?.releaseCrash();
			await waitForCondition(async () => {
				const snapshot = await Effect.runPromise(
					manager.inspectThread(threadControl("sesn_closeout_observer_defect")),
				);
				return snapshot.ok && snapshot.observed;
			}, "unrepairable accepted custody after observer defect");
			expect(await joined).toMatchObject({
				ok: true,
				observed: true,
				timedOut: false,
			});
			expect(threadLoop.runs[0]?.session.state.acceptedInputCount()).toBe(1);
			expect(
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput(
						"sesn_after_observer_defect",
					),
				),
			).toEqual({
				ok: false,
				sessionId: "sesn_after_observer_defect",
				reason: "local_session_capacity_exceeded",
			});
		});
	});

	test("shutdown races a parked failed-run backoff and releases inside the drain path", async () => {
		let backoffStartedResolve: () => void = () => {};
		const backoffStarted = new Promise<void>((resolve) => {
			backoffStartedResolve = resolve;
		});
		const threadLoop = makeControlledCrashThreadLoop("die", {
			closeFailedRun: () =>
				Effect.succeed({
					type: "retry",
					error: normalizeSessionEventWriterError({ code: "unavailable" }),
				}),
		});
		const layer = sessionManagerLayer(threadLoop, {
			maxLocalSessions: 1,
			closeoutSleep: async (_durationMs, signal) =>
				await new Promise<boolean>((resolve) => {
					backoffStartedResolve();
					signal.addEventListener("abort", () => resolve(false), {
						once: true,
					});
				}),
		});

		await withSessionManager(layer, async (manager) => {
			await Effect.runPromise(
				manager.startTestRunThroughAcceptedInput("sesn_closeout_shutdown"),
			);
			await waitForCrashRuns(threadLoop, 1);
			threadLoop.runs[0]?.releaseCrash();
			await backoffStarted;
			await Effect.runPromise(manager.shutdownActiveRuns());
			expect(
				await Effect.runPromise(
					manager.inspectThread(threadControl("sesn_closeout_shutdown")),
				),
			).toMatchObject({
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
		const threadLoop = makeControlledCrashThreadLoop("die", {
			closeFailedRun: () =>
				Effect.promise(async () => {
					observationStartedResolve();
					await observationWindow;
					return {
						type: "retry" as const,
						error: normalizeSessionEventWriterError({ code: "timeout" }),
					};
				}),
		});

		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput(
						"sesn_closeout_shutdown_window",
					),
				);
				await waitForCrashRuns(threadLoop, 1);
				threadLoop.runs[0]?.releaseCrash();
				await observationStarted;

				let shutdownSettled = false;
				const shutdown = Effect.runPromise(
					manager.shutdownActiveRuns(),
				).finally(() => {
					shutdownSettled = true;
				});
				await Promise.resolve();
				expect(shutdownSettled).toBe(false);

				observationWindowResolve();
				await shutdown;
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl("sesn_closeout_shutdown_window"),
						),
					),
				).toMatchObject({ observed: false });
			},
		);
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
		const threadLoop = makeControlledCrashThreadLoop("die", {
			closeFailedRun: () =>
				Effect.promise(async () => {
					closeoutStartedResolve();
					await closeoutRelease;
					return {
						type: "landed" as const,
						disposition: "terminal" as const,
					};
				}),
		});

		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("sesn_closeout_interrupt"),
				);
				await waitForCrashRuns(threadLoop, 1);
				threadLoop.runs[0]?.releaseCrash();
				await closeoutStarted;

				let interruptSettled = false;
				const interruptCommand = {
					...threadControl("sesn_closeout_interrupt", "rin_closeout_interrupt"),
					inputOrder: 1,
				};
				const interrupt = Effect.runPromise(
					manager.interruptControl(
						"sesn_closeout_interrupt",
						interruptCommand,
						testControlCommit(interruptCommand),
					),
				).finally(() => {
					interruptSettled = true;
				});
				await Promise.resolve();
				expect(interruptSettled).toBe(false);
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								"sesn_closeout_interrupt",
								"rin_closeout_interrupt_inspect",
							),
						),
					),
				).toMatchObject({ observed: true });

				closeoutReleaseResolve();
				await expect(interrupt).resolves.toEqual({
					ok: false,
					sessionId: "sesn_closeout_interrupt",
					reason: "control_busy",
				});
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(
								"sesn_closeout_interrupt",
								"rin_closeout_interrupt_after",
							),
						),
					),
				).toMatchObject({ observed: false });
			},
		);
	});

	test("closing a child cancels and releases its entire hot descendant subtree", async () => {
		const runs: ThreadRuntime.ThreadRuntime[] = [];
		const interruptedThreadIds: string[] = [];
		const layer = sessionManagerLayer({
			layer: Layer.succeed(
				ThreadLoop.Service,
				threadLoopService({
					run: (session) =>
						Effect.sync(() => {
							runs.push(session);
						}).pipe(
							Effect.andThen(Effect.never),
							Effect.onInterrupt(() =>
								Effect.sync(() => {
									interruptedThreadIds.push(session.identity.sessionThreadId);
								}),
							),
						),
				}),
			),
		});
		const sessionId = "sesn_close_tree";
		const childId = "thrd_close_tree_child";
		const grandchildId = "thrd_close_tree_grandchild";
		const siblingId = "thrd_close_tree_sibling";

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
				{
					id: siblingId,
					runtimeInputId: "rin_close_tree_sibling",
					metadata: {
						parentThreadId: "thrd_close_tree_main",
						role: "subagent" as const,
						visibility: "public" as const,
						taskName: "sibling",
						agentType: "worker" as const,
						status: "idle" as const,
					},
				},
			];
			for (const thread of threads) {
				expect(
					await Effect.runPromise(
						manager.preloadThread({
							...threadControl(
								sessionId,
								`rin_preload_${thread.id}`,
								thread.id,
							),
							runtimeBindingToken: "runtime-binding-token",
							contextEntries: [],
							thread: thread.metadata,
						}),
					),
				).toMatchObject({ ok: true, applied: true });
				expect(
					await Effect.runPromise(
						manager.acceptInput(
							acceptedInput(sessionId, thread.runtimeInputId, thread.id),
						),
					),
				).toMatchObject({
					ok: true,
					started: true,
				});
			}
			await waitForCondition(() => runs.length === 3, "child and sibling runs");

			expect(
				await Effect.runPromise(
					manager.markThreadClosed(
						threadControl(sessionId, "rin_close_tree", childId),
					),
				),
			).toEqual({
				ok: true,
				sessionId,
				sessionThreadId: childId,
				applied: true,
				runExitOutcome: "interrupt_applied",
			});
			expect(
				await Effect.runPromise(
					manager.inspectThread(
						threadControl(sessionId, "rin_close_tree_child_inspect", childId),
					),
				),
			).toMatchObject({
				observed: false,
			});
			expect(
				await Effect.runPromise(
					manager.inspectThread(
						threadControl(
							sessionId,
							"rin_close_tree_grandchild_inspect",
							grandchildId,
						),
					),
				),
			).toMatchObject({
				observed: false,
			});
			expect(interruptedThreadIds.sort()).toEqual(
				[childId, grandchildId].sort(),
			);
			expect(
				await Effect.runPromise(
					manager.inspectThread(
						threadControl(
							sessionId,
							"rin_close_tree_sibling_inspect",
							siblingId,
						),
					),
				),
			).toMatchObject({ observed: true, status: "running" });
		});
	});

	test("releasing a failed parent leaves its resident child independently owned", async () => {
		const threadLoop = makeControlledThreadLoop();
		const sessionId = "sesn_parent_release_child_owned";
		const parentId = "thrd_parent_release";
		const childId = "thrd_parent_release_child";

		await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => {
				for (const thread of [
					{
						id: parentId,
						inputId: "rin_parent_release",
						metadata: {
							role: "main" as const,
							visibility: "public" as const,
							status: "idle" as const,
						},
					},
					{
						id: childId,
						inputId: "rin_parent_release_child",
						metadata: {
							parentThreadId: parentId,
							role: "subagent" as const,
							visibility: "public" as const,
							taskName: "independent-child",
							agentType: "worker" as const,
							status: "idle" as const,
						},
					},
				]) {
					expect(
						await Effect.runPromise(
							manager.preloadThread({
								...threadControl(sessionId, `rin_preload_${thread.id}`, thread.id),
								runtimeBindingToken: "runtime-binding-token",
								contextEntries: [],
								thread: thread.metadata,
							}),
						),
					).toMatchObject({ ok: true, applied: true });
					expect(
						await Effect.runPromise(
							manager.acceptInput(
								acceptedInput(sessionId, thread.inputId, thread.id),
							),
						),
					).toMatchObject({ ok: true, started: true });
				}
				await waitForRuns(threadLoop, 2);
				const parentRun = threadLoop.runs.find(
					(run) => run.session.identity.sessionThreadId === parentId,
				);
				parentRun?.release(fatalRunResult("persistence_failed"));

				await waitForCondition(async () => {
					const parent = await Effect.runPromise(
						manager.inspectThread(
							threadControl(sessionId, "rin_parent_release_inspect", parentId),
						),
					);
					return parent.ok && !parent.observed;
				}, "failed parent release");
				expect(
					await Effect.runPromise(
						manager.inspectThread(
							threadControl(sessionId, "rin_child_owned_inspect", childId),
						),
					),
				).toMatchObject({ observed: true, status: "running" });
			},
		);
	});

	test("ThreadLoop rejected promise removes crashed entry without exposing hostile rejection text", async () => {
		const threadLoop = makeControlledCrashThreadLoop("reject");
		const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

		await withSessionManager(layer, async (manager) => {
			const first = await Effect.runPromise(
				manager.startTestRunThroughAcceptedInput("sesn_reject"),
			);
			expect(first).toEqual({
				ok: true,
				sessionId: "sesn_reject",
				created: true,
				started: true,
			});
			await waitForCrashRuns(threadLoop, 1);
			threadLoop.runs[0]?.releaseCrash();

			let replacement: TestRunStartResult | undefined;
			for (
				let attempt = 0;
				attempt < 100 && replacement === undefined;
				attempt += 1
			) {
				const result = await Effect.runPromise(
					manager.startTestRunThroughAcceptedInput("replacement_reject"),
				);
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
			expectNoHostileFragments(
				await Effect.runPromise(
					manager.cleanupSession("sesn_reject", cleanupControl("sesn_reject")),
				),
			);
			await waitForCrashRuns(threadLoop, 2);
		});
	});

	test("same-session accept after fatal discard starts a fresh run instead of queuing on the old entry", async () => {
		for (const reason of [
			"persistence_failed",
			"event_write_failed",
			"crashed",
		] as const) {
			const threadLoop = makeControlledThreadLoop();
			const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

			await withSessionManager(layer, async (manager) => {
				expect(
					await Effect.runPromise(
						manager.startTestRunThroughAcceptedInput(`accept_${reason}`),
					),
				).toEqual({
					ok: true,
					sessionId: `accept_${reason}`,
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				const fatalSession = threadLoop.runs[0]?.session;
				threadLoop.runs[0]?.release(fatalRunResult(reason));

				let accepted: SessionManager.AcceptInputResult | undefined;
				for (
					let attempt = 0;
					attempt < 100 && accepted === undefined;
					attempt += 1
				) {
					const result = await Effect.runPromise(
						manager.acceptInput(acceptedInput(`accept_${reason}`)),
					);
					if (result.ok && result.started) {
						accepted = result;
						break;
					}
					await new Promise((resolve) => setTimeout(resolve, 1));
				}
				expect(accepted).toEqual({
					ok: true,
					sessionId: `accept_${reason}`,
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 2);
				expect(threadLoop.runs[1]?.session).not.toBe(fatalSession);
				threadLoop.runs[1]?.release();
			});
		}
	});

	test("replacement session capacity is available after fatal discard", async () => {
		for (const reason of [
			"persistence_failed",
			"event_write_failed",
			"crashed",
		] as const) {
			const threadLoop = makeControlledThreadLoop();
			const layer = sessionManagerLayer(threadLoop, { maxLocalSessions: 1 });

			await withSessionManager(layer, async (manager) => {
				expect(
					await Effect.runPromise(
						manager.startTestRunThroughAcceptedInput(`old_${reason}`),
					),
				).toEqual({
					ok: true,
					sessionId: `old_${reason}`,
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 1);
				threadLoop.runs[0]?.release(fatalRunResult(reason));

				let replacement: TestRunStartResult | undefined;
				for (
					let attempt = 0;
					attempt < 100 && replacement === undefined;
					attempt += 1
				) {
					const result = await Effect.runPromise(
						manager.startTestRunThroughAcceptedInput(`replacement_${reason}`),
					);
					if (result.ok && result.started) {
						replacement = result;
						break;
					}
					await new Promise((resolve) => setTimeout(resolve, 1));
				}
				expect(replacement).toEqual({
					ok: true,
					sessionId: `replacement_${reason}`,
					created: true,
					started: true,
				});
				await waitForRuns(threadLoop, 2);

				const joined = Effect.runPromise(
					manager.startTestRunThroughAcceptedInput(`replacement_${reason}`),
				);
				await new Promise((resolve) => setTimeout(resolve, 5));
				expect(threadLoop.runs).toHaveLength(2);
				expect(
					await Effect.runPromise(
						manager.cleanupSession(
							`old_${reason}`,
							cleanupControl(`old_${reason}`),
						),
					),
				).toEqual({
					ok: true,
					sessionId: `old_${reason}`,
					cleaned: false,
				});
				threadLoop.runs[1]?.release();
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
		const threadLoop = makeControlledThreadLoop();
		const keys = await withSessionManager(
			sessionManagerLayer(threadLoop),
			async (manager) => Object.keys(manager).sort(),
		);

		expect(keys).toEqual([
			"acceptInput",
			"applyRuntimeConfigPatch",
			"cleanupSession",
			"commitTaskNotification",
			"ensureThreadInstalled",
			"evictReviewerExecution",
			"inspectReviewerExecution",
			"inspectThread",
			"interruptControl",
			"interruptReviewerExecution",
			"markThreadActive",
			"markThreadClosed",
			"preloadThread",
			"releaseReviewerExecution",
			"resolveToolConfirmation",
			"shutdownActiveRuns",
			"waitReviewerExecution",
			"waitThread",
		]);
	});
});
