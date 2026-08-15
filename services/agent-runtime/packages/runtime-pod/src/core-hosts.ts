/**
 * @packageDocumentation
 * Owns the scoped Effect runtime that backs Runtime Pod command, sub-agent, cleanup, and shutdown
 * operations. The process composition root builds these hosts, the gRPC runtime service and tool
 * orchestration call them, and each adapter runs the corresponding SessionRunHost effect. The
 * module keeps one layer scope for the host lifetime and delegates cold-install single-flight
 * ownership to SessionManager before control operations that require resident state. It exposes active-run
 * shutdown and scope release as separate operations that the process composition root orders.
 */

import type * as ContextLoader from "@tetral/agent-runtime-core/src/context/context-loader.js";
import type { RuntimeLoadedAgentMail } from "@tetral/agent-runtime-core/src/context/context-loader.js";
import type {
	SessionEvent,
	SessionEventWriterAppendResult,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeMetricsSink } from "@tetral/agent-runtime-core/src/runtime/metrics.js";
import { createSessionEventWriter } from "@tetral/agent-runtime-core/src/runtime/session-event-writer.js";
import type {
	RuntimeCloseoutEvent,
	RuntimeMCPManifestUpdateEvent,
} from "@tetral/agent-runtime-core/src/session/session-manager.js";
import * as SessionManager from "@tetral/agent-runtime-core/src/session/session-manager.js";
import * as SessionRunHost from "@tetral/agent-runtime-core/src/session-run-host/session-run-host.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeApprovalReviewAcceptedInputState,
	RuntimeThreadAddressState,
	RuntimeThreadPreloadState,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import type {
	ThreadToolRouteView,
	ThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { deriveThreadTurnDecision } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-reducer.js";
import { Context, Effect, Exit, Layer, Scope } from "effect";
import type { RuntimeCoreCleanupHost } from "./cleanup-controller.js";
import type { RuntimePodLogger } from "./logger.js";
import { recordCheckpointReconstructionFailure } from "./logger.js";
import type {
	RuntimeAgentMailCommand,
	RuntimeSessionRunHost,
} from "./runtime-service.js";

/**
 * Groups the promise-based Runtime Core host surfaces and the two operations that end their shared
 * process lifetime.
 */
export interface RuntimeCoreHosts {
	readonly commandRunHost: RuntimeSessionRunHost;
	readonly subAgentRunHost: RuntimeSubAgentRunHost;
	readonly cleanupRunHost: RuntimeCoreCleanupHost;
	readonly shutdownActiveRuns: () => Promise<void>;
	readonly close: () => Promise<void>;
}

/**
 * Exposes child-thread and approval-reviewer operations to tool orchestration without leaking the
 * Effect service or scope across the Runtime Pod package boundary.
 */
export interface RuntimeSubAgentRunHost {
	readonly enqueueThreadInput: (
		input: RuntimeAcceptedInputState,
	) => Promise<SessionManager.AcceptInputResult>;
	readonly preloadThread: (
		input: Omit<
			RuntimeThreadPreloadState,
			| "contextEntries"
			| "openRequestDraft"
			| "turnCheckpoint"
			| "turnToolRouteView"
			| "runtimeBindingToken"
			| "runtimeConfigPatch"
			| "mcpManifests"
			| "pendingToolUses"
			| "pendingSandboxExecutions"
			| "pendingAttachments"
			| "pendingAgentMail"
		>,
	) => Promise<SessionManager.ThreadLifecycleResult>;
	readonly evictReviewerExecution: (
		command: RuntimeThreadAddressState,
		token: SessionManager.ReviewerExecutionToken,
	) => Promise<SessionManager.ReviewerExecutionControlResult>;
	readonly interruptReviewerExecution: (
		command: RuntimeThreadAddressState,
		token: SessionManager.ReviewerExecutionToken,
	) => Promise<SessionManager.ReviewerExecutionControlResult>;
	readonly releaseReviewerExecution: (
		command: RuntimeThreadAddressState,
		token: SessionManager.ReviewerExecutionToken,
	) => Promise<SessionManager.ReviewerExecutionControlResult>;
	readonly markThreadClosed: (
		command: Parameters<SessionRunHost.Interface["handleMarkThreadClosed"]>[0],
	) => Promise<SessionManager.ThreadLifecycleResult>;
	readonly markThreadActive: (
		command: Parameters<SessionRunHost.Interface["handleMarkThreadActive"]>[0],
	) => Promise<SessionManager.ThreadLifecycleResult>;
	readonly waitThread: (
		command: Parameters<SessionRunHost.Interface["handleWaitThread"]>[0],
		timeoutMs: number | undefined,
		abortSignal?: AbortSignal | undefined,
	) => Promise<SessionManager.ThreadWaitResult>;
	readonly pullAgentMail?: (
		command: RuntimeThreadAddressState,
		sourceThreadId: string,
	) => Promise<
		{ readonly deliveryId: string; readonly finalMessage: string } | undefined
	>;
	readonly waitReviewerExecution: (
		command: RuntimeThreadAddressState,
		token: SessionManager.ReviewerExecutionToken,
		timeoutMs: number | undefined,
		abortSignal?: AbortSignal | undefined,
	) => Promise<SessionManager.ReviewerExecutionWaitResult>;
	readonly inspectThread: (
		command: Parameters<SessionRunHost.Interface["handleInspectThread"]>[0],
	) => Promise<SessionManager.ThreadSnapshotResult>;
	readonly inspectReviewerExecution: (
		command: RuntimeThreadAddressState,
		token: SessionManager.ReviewerExecutionToken,
	) => Promise<SessionManager.ReviewerExecutionSnapshotResult>;
	readonly commitApprovalReviewDecision: (
		command: RuntimeApprovalReviewAcceptedInputState,
		event: Extract<SessionEvent, { readonly type: "approval_review.decision" }>,
	) => Promise<SessionEventWriterAppendResult>;
	readonly commitApprovalReviewFailure: (
		command: RuntimeApprovalReviewAcceptedInputState,
		event: Extract<SessionEvent, { readonly type: "approval_review.failure" }>,
	) => Promise<SessionEventWriterAppendResult>;
}

/** Configures capacity, time, context, ThreadLoop, and metrics for one Runtime Core host scope. */
export interface RuntimeCoreHostsOptions {
	readonly maxLocalSessions: number;
	readonly maxConcurrentTools?: number | undefined;
	readonly now: () => string;
	readonly contextLoader: ContextLoader.ContextLoader;
	readonly threadLoop: ThreadLoop.ThreadLoopRuntimeOptions;
	readonly metrics?: RuntimeMetricsSink | undefined;
	readonly recordCloseoutEvent?:
		| ((event: RuntimeCloseoutEvent) => void)
		| undefined;
	readonly recordMCPManifestUpdate?:
		| ((event: RuntimeMCPManifestUpdateEvent) => void)
		| undefined;
	readonly resolveMCPManifestEligibility?: SessionManager.LayerOptions["resolveMCPManifestEligibility"];
	readonly logger?: RuntimePodLogger | undefined;
}

/**
 * Builds the ThreadLoop, Session Manager, and SessionRunHost layers in one explicit Effect scope and
 * returns promise adapters for network commands and in-process tools. Cold installation is owned by
 * SessionManager, accepted messages are converted to Runtime Core input only after installation, and
 * callers close the returned hosts to release the layer scope.
 */
export async function buildRuntimeCoreHosts(
	options: RuntimeCoreHostsOptions,
): Promise<RuntimeCoreHosts> {
	const reviewerFailureWriter = createSessionEventWriter({
		append: (envelope) =>
			options.threadLoop.sessionEventWriter.append(envelope),
		sleep: async (durationMs) =>
			await options.threadLoop.runtime.sleep(
				durationMs,
				new AbortController().signal,
			),
	});
	const threadLoopLayer = ThreadLoop.layer({
		...options.threadLoop,
		...(options.contextLoader.refreshRuntimeBindingToken !== undefined
			? {
					refreshRuntimeBindingToken: (identity, refreshOptions) =>
						options.contextLoader.refreshRuntimeBindingToken?.(
							identity,
							refreshOptions,
						) ?? Promise.resolve(identity.runtimeBindingToken),
				}
			: {}),
		...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
	}).pipe(Layer.provide(ThreadLoop.contextLoaderLayer(options.contextLoader)));
	const managerLayer = SessionManager.layer({
		maxLocalSessions: options.maxLocalSessions,
		maxConcurrentTools: options.maxConcurrentTools ?? 8,
		now: options.now,
		...(options.contextLoader.loadThreadContext !== undefined
			? {
					loadThreadContext: async (
						command: RuntimeThreadAddressState,
					): Promise<RuntimeThreadPreloadState> => {
						const context =
							await options.contextLoader.loadThreadContext!(command);
						const pendingAgentMail: Array<
							Extract<
								RuntimeAcceptedInputState,
								{ readonly kind: "inter_agent_message" }
							>
						> = [];
						if (
							(context.pendingAgentMail?.length ?? 0) > 0 &&
							context.thread === undefined
						) {
							throw new Error(
								"pending agent mail requires durable thread lineage",
							);
						}
						for (const mail of context.pendingAgentMail ?? []) {
							pendingAgentMail.push(
								acceptedLoadedAgentMail(command, mail, context.thread),
							);
						}
						let turnCheckpoint: ThreadTurnCheckpoint;
						let turnToolRouteView: ThreadToolRouteView;
						try {
							turnCheckpoint = extractThreadTurnCheckpoint({
								contextEntries: context.contextEntries,
								facts: context.turnFacts,
							});
							turnToolRouteView = extractColdThreadToolRouteView({
								checkpoint: turnCheckpoint,
								pendingToolUses: context.pendingToolUses ?? [],
								pendingSandboxExecutions:
									context.pendingSandboxExecutions ?? [],
							});
							if (context.thread?.status === "closed_for_runtime") {
								validateClosedThreadResumeCheckpoint(
									turnCheckpoint,
									turnToolRouteView,
									context.pendingToolUses ?? [],
									context.pendingSandboxExecutions ?? [],
								);
							}
						} catch (error) {
							recordCheckpointReconstructionFailure(options.logger, command);
							throw error;
						}
						return {
							...command,
							...(context.thread !== undefined
								? { thread: context.thread }
								: {}),
							contextEntries: context.contextEntries,
							...(context.openRequestDraft !== undefined
								? { openRequestDraft: context.openRequestDraft }
								: {}),
							turnCheckpoint,
							turnToolRouteView,
							...(context.threadContextPrefix !== undefined
								? { threadContextPrefix: context.threadContextPrefix }
								: {}),
							runtimeBindingToken: context.runtimeBindingToken,
							...(context.runtimeConfigPatch !== undefined
								? { runtimeConfigPatch: context.runtimeConfigPatch }
								: {}),
							...(context.mcpManifests !== undefined
								? { mcpManifests: context.mcpManifests }
								: {}),
							...(context.pendingToolUses !== undefined
								? { pendingToolUses: context.pendingToolUses }
								: {}),
							...(context.pendingSandboxExecutions !== undefined
								? { pendingSandboxExecutions: context.pendingSandboxExecutions }
								: {}),
							...(context.pendingAttachments !== undefined
								? { pendingAttachments: context.pendingAttachments }
								: {}),
							pendingAgentMail,
						};
					},
				}
			: {}),
		...(options.contextLoader.refreshRuntimeBindingToken !== undefined
			? {
					refreshRuntimeBindingToken: (identity, refreshOptions) =>
						options.contextLoader.refreshRuntimeBindingToken?.(
							identity,
							refreshOptions,
						) ?? Promise.resolve(identity.runtimeBindingToken),
				}
			: {}),
		...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
		closeoutMonotonicMs: options.threadLoop.runtime.monotonicMs,
		closeoutSleep: options.threadLoop.runtime.sleep,
		...(options.recordCloseoutEvent !== undefined
			? { recordCloseoutEvent: options.recordCloseoutEvent }
			: {}),
		...(options.recordMCPManifestUpdate !== undefined
			? { recordMCPManifestUpdate: options.recordMCPManifestUpdate }
			: {}),
		...(options.resolveMCPManifestEligibility !== undefined
			? { resolveMCPManifestEligibility: options.resolveMCPManifestEligibility }
			: {}),
	}).pipe(Layer.provide(threadLoopLayer));
	const hostLayer = SessionRunHost.layer.pipe(Layer.provide(managerLayer));
	const { host, scope } = await Effect.runPromise(
		Effect.gen(function* () {
			const layerScope = yield* Scope.make();
			const context = yield* Layer.buildWithScope(hostLayer, layerScope);
			return {
				host: Context.get(context, SessionRunHost.Service),
				scope: layerScope,
			};
		}),
	);
	return {
		commandRunHost: {
			handleAcceptInput: async (command) => {
				const acceptedInput = runtimeAcceptedInputFromCommand(command);
				const result = await Effect.runPromise(
					host.handleAcceptInput(acceptedInput),
				);
				if (result.ok) {
					return result;
				}
				return {
					ok: false,
					sessionId: result.sessionId,
					reason:
						result.reason === "context_load_failed"
							? "context_load_failed"
							: result.reason === "local_session_capacity_exceeded"
								? "local_session_capacity_exceeded"
								: "control_conflict",
					...(result.reason === "context_load_failed" &&
					result.retryable !== undefined
						? { retryable: result.retryable }
						: {}),
				};
			},
			handleAgentMail: async (command) => {
				try {
					const result = await Effect.runPromise(
						host.handleAcceptInput(acceptedAgentMailCommand(command)),
					);
					if (!result.ok) {
						return {
							ok: false,
							sessionId: command.sessionId,
							reason:
								result.reason === "local_session_capacity_exceeded"
									? "local_session_capacity_exceeded"
									: "thread_not_receivable",
							...(result.reason === "context_load_failed" &&
							result.retryable !== undefined
								? {
										reason: "context_load_failed" as const,
										retryable: result.retryable,
									}
								: {}),
						} as const;
					}
					return {
						ok: true,
						sessionId: command.sessionId,
						applied: result.duplicate !== true || result.started,
					};
				} catch {
					return {
						ok: false,
						sessionId: command.sessionId,
						reason: "context_load_failed",
						retryable: false,
					};
				}
			},
			handleInterruptControl: async (sessionId, command, commitInput) =>
				await Effect.runPromise(
					host.handleInterruptControl(sessionId, command, commitInput),
				),
			handleToolConfirmation: async (sessionId, command, commit) =>
				await Effect.runPromise(
					host.handleToolConfirmation(sessionId, command, commit),
				),
			handleTaskNotification: async (sessionId, command) =>
				await Effect.runPromise(
					host.handleTaskNotification(sessionId, command),
				),
			handleRuntimeConfigPatch: async (sessionId, command) => {
				return await Effect.runPromise(
					host.handleRuntimeConfigPatch(sessionId, command),
				);
			},
		},
		subAgentRunHost: {
			enqueueThreadInput: async (input) =>
				await Effect.runPromise(host.handleAcceptInput(input)),
			preloadThread: async (input) => {
				return await Effect.runPromise(host.handleEnsureThreadInstalled(input));
			},
			evictReviewerExecution: async (command, token) =>
				await Effect.runPromise(
					host.handleEvictReviewerExecution(command, token),
				),
			interruptReviewerExecution: async (command, token) =>
				await Effect.runPromise(
					host.handleInterruptReviewerExecution(command, token),
				),
			releaseReviewerExecution: async (command, token) =>
				await Effect.runPromise(
					host.handleReleaseReviewerExecution(command, token),
				),
			markThreadClosed: async (command) =>
				await Effect.runPromise(host.handleMarkThreadClosed(command)),
			markThreadActive: async (command) =>
				await Effect.runPromise(host.handleMarkThreadActive(command)),
			waitThread: async (command, timeoutMs, abortSignal) =>
				await Effect.runPromise(
					host.handleWaitThread(command, timeoutMs),
					abortSignal === undefined ? undefined : { signal: abortSignal },
				),
			pullAgentMail: async (command, sourceThreadId) => {
				if (options.contextLoader.readAgentMail === undefined) {
					throw new Error("agent mail reader is unavailable");
				}
				const resolved = await options.contextLoader.readAgentMail(
					command,
					sourceThreadId,
				);
				if (resolved === undefined) return undefined;
				return {
					deliveryId: resolved.deliveryId,
					finalMessage: resolved.content,
				};
			},
			waitReviewerExecution: async (command, token, timeoutMs, abortSignal) =>
				await Effect.runPromise(
					host.handleWaitReviewerExecution(command, token, timeoutMs),
					abortSignal === undefined ? undefined : { signal: abortSignal },
				),
			inspectThread: async (command) =>
				await Effect.runPromise(host.handleInspectThread(command)),
			inspectReviewerExecution: async (command, token) =>
				await Effect.runPromise(
					host.handleInspectReviewerExecution(command, token),
				),
			commitApprovalReviewDecision: async (command, event) =>
				await options.threadLoop.sessionEventWriter.append({
					workspaceId: command.workspaceId,
					sessionId: command.sessionId,
					sessionThreadId: command.sessionThreadId,
					bindingId: command.bindingId,
					bindingGeneration: command.bindingGeneration,
					targetPodUid: command.targetPodUid,
					writeId: `rwrite_${event.review_id}_decision`,
					event,
				}),
			commitApprovalReviewFailure: async (command, event) =>
				await reviewerFailureWriter.append({
					workspaceId: command.workspaceId,
					sessionId: command.sessionId,
					sessionThreadId: command.sessionThreadId,
					bindingId: command.bindingId,
					bindingGeneration: command.bindingGeneration,
					targetPodUid: command.targetPodUid,
					writeId: `rwrite_${event.review_id}_failure`,
					event,
				}),
		},
		cleanupRunHost: {
			handleCleanupSession: async (scope) =>
				await Effect.runPromise(
					host.handleCleanupSession(scope.sessionId, scope),
				),
		},
		shutdownActiveRuns: async () => {
			await Effect.runPromise(host.shutdownActiveRuns());
		},
		close: async () => {
			await Effect.runPromise(Scope.close(scope, Exit.void));
		},
	};
}

/**
 * Accepts only a quiescent cold checkpoint for resume. Deferred task
 * notifications are intentionally absent from this execution checkpoint and
 * are reactivated by Bridge after the durable lifecycle transition wins.
 */
export function validateClosedThreadResumeCheckpoint(
	checkpoint: ThreadTurnCheckpoint,
	routeView: ThreadToolRouteView,
	pendingToolUses: readonly unknown[],
	pendingSandboxExecutions: readonly unknown[],
): void {
	const decision = deriveThreadTurnDecision(checkpoint, routeView);
	const incompleteToolUse =
		checkpoint.request?.toolMembers.some(
			(member) =>
				member.memberKind === "public_tool_use" &&
				member.terminalResult === undefined,
		) ?? false;
	if (
		checkpoint.executionRunId !== undefined ||
		checkpoint.interruptEventId !== undefined ||
		checkpoint.terminalCloseout !== undefined ||
		(checkpoint.request?.requestEnd === undefined &&
			checkpoint.request !== undefined) ||
		incompleteToolUse ||
		pendingToolUses.length !== 0 ||
		pendingSandboxExecutions.length !== 0 ||
		routeView.routes.length !== 0 ||
		decision.state.state !== "idle" ||
		decision.action.action !== "await_input"
	) {
		throw new Error(
			"closed Thread resume requires a quiescent durable checkpoint",
		);
	}
}

function acceptedLoadedAgentMail(
	command: RuntimeThreadAddressState,
	mail: RuntimeLoadedAgentMail,
	thread: RuntimeThreadPreloadState["thread"],
): Extract<
	RuntimeAcceptedInputState,
	{ readonly kind: "inter_agent_message" }
> {
	return {
		...command,
		runtimeInputId: `agent_mail:${mail.deliveryId}`,
		kind: "inter_agent_message",
		deliveryId: mail.deliveryId,
		content: mail.content,
		thread,
	};
}

function acceptedAgentMailCommand(
	command: RuntimeAgentMailCommand,
): Extract<
	RuntimeAcceptedInputState,
	{ readonly kind: "inter_agent_message" }
> {
	return {
		...command,
		kind: "inter_agent_message",
	};
}

function runtimeAcceptedInputFromCommand(
	command: Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0],
): RuntimeAcceptedInputState {
	if (command.kind === "rejection") {
		return { ...command };
	}
	return {
		workspaceId: command.workspaceId,
		sessionId: command.sessionId,
		sessionThreadId: command.sessionThreadId,
		bindingId: command.bindingId,
		bindingGeneration: command.bindingGeneration,
		targetPodUid: command.targetPodUid,
		runtimeInputId: command.runtimeInputId,
		inputOrder: command.inputOrder,
		kind: "messages",
		contentJson: command.contentJson,
	};
}
