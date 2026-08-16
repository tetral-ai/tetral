/**
 * Authenticated, method-specific Bridge-to-Runtime Pod ingress.
 *
 * Each RPC validates only its owned contract. The method identifies the
 * operation; no command-kind discriminator, Event range, pod address echo, or
 * universal response envelope crosses the boundary.
 */

import type { Metadata } from "@grpc/grpc-js";
import { status } from "@grpc/grpc-js";
import type {
	RuntimeInterruptToolResult,
	RuntimeProviderAttachment,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeMetricsSink } from "@tetral/agent-runtime-core/src/runtime/metrics.js";
import { NoopRuntimeMetricsSink } from "@tetral/agent-runtime-core/src/runtime/metrics.js";
import type {
	RuntimeControlInputCommitResult,
	RuntimeControlInputDeclaration,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import {
	validateAcceptAgentMailRequest,
	validateAcceptInputRequest,
	validateAcceptTaskNotificationRequest,
	validateApplyRuntimeConfigRequest,
	validateCleanupSessionRequest,
	validateInterruptRequest,
	validateResolveToolConfirmationRequest,
} from "@tetral/agent-runtime-protocol/src/bounds.js";
import type {
	AcceptAgentMailRequest,
	AcceptAgentMailResponse,
	AcceptInputRequest,
	AcceptInputResponse,
	AcceptTaskNotificationRequest,
	AcceptTaskNotificationResponse,
	ApplyRuntimeConfigRequest,
	ApplyRuntimeConfigResponse,
	CleanupSessionRequest,
	CleanupSessionResponse,
	InterruptRequest,
	InterruptResponse,
	ResolveToolConfirmationRequest,
	ResolveToolConfirmationResponse,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import {
	AcceptAgentMailFailure,
	AcceptInputFailure,
	AcceptInputRejectionReason,
	AcceptTaskNotificationFailure,
	ApplyRuntimeConfigFailure,
	CleanupSessionFailure,
	CleanupSessionReason,
	InterruptFailure,
	InterruptOrigin,
	ResolveToolConfirmationFailure,
	ToolConfirmationDecision,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import { semanticErrorFields } from "@tetral/ts-observability";
import type { ServiceAccountIdentity } from "./auth.js";
import { GrpcStatusError } from "./errors.js";
import type { RuntimeCommandLease } from "./lifecycle.js";
import type {
	RuntimeIngressRejectionPhase,
	RuntimeIngressRejectionReason,
	RuntimePodLogger,
} from "./logger.js";

export { GrpcStatusError } from "./errors.js";

export interface RuntimeAuthenticator {
	readonly authenticate: (input: {
		readonly metadata: Metadata;
		readonly method: string;
	}) => Promise<
		| { readonly ok: true; readonly serviceAccount: ServiceAccountIdentity }
		| {
				readonly ok: false;
				readonly code: "Unauthenticated" | "PermissionDenied";
				readonly message: string;
		  }
	>;
}

/** Current session binding authority shared by session-addressed methods. */
export interface RuntimeSessionScope {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
}

/** Current session binding plus the thread selected by a thread-addressed method. */
export interface RuntimeThreadScope extends RuntimeSessionScope {
	readonly sessionThreadId: string;
}

/** One active Runtime Inbox identity. */
export interface RuntimeInputScope extends RuntimeThreadScope {
	readonly runtimeInputId: string;
}

export type RuntimeAcceptInputCommand = RuntimeInputScope & {
	readonly inputOrder: number;
} & (
		| { readonly kind: "messages"; readonly contentJson: string }
		| {
				readonly kind: "rejection";
				readonly reasonCode:
					| "runtime_command_payload_too_large"
					| "runtime_command_rejected";
		  }
	);

export interface RuntimeAgentMailCommand extends RuntimeInputScope {
	readonly kind: "inter_agent_message";
	readonly deliveryId: string;
	readonly content: string;
}

export interface RuntimeTaskNotificationCommand extends RuntimeInputScope {
	readonly kind: "task_notification";
	readonly inputOrder: number;
	readonly taskId: string;
	readonly sourceToolUseEventId: string;
	readonly status: "completed" | "failed" | "cancelled" | "expired";
	readonly notificationJson: string;
}

export interface RuntimeInterruptCommand extends RuntimeInputScope {
	readonly inputOrder: number;
	readonly origin: "user" | "agent";
}

export interface RuntimeToolConfirmationCommand extends RuntimeInputScope {
	readonly toolUseEventId: string;
	readonly decision: "allow" | "deny";
	readonly denyMessage?: string | undefined;
}

export interface RuntimeConfigCommand extends RuntimeSessionScope {
	readonly configIdentity: string;
	readonly generation: number;
	readonly mcpServerName?: string | undefined;
	readonly manifestETag?: string | undefined;
	readonly manifestReadiness?: "ready" | "unready";
	readonly manifestDiagnostic?: string | undefined;
	readonly contentJson: string;
}

export interface RuntimeCleanupCommand extends RuntimeSessionScope {
	readonly cleanupOperationId: string;
}

export interface RuntimeSessionRunHost {
	readonly handleAcceptInput: (
		command: RuntimeAcceptInputCommand,
	) => Promise<
		| {
				readonly ok: true;
				readonly sessionId: string;
				readonly created: boolean;
				readonly started: boolean;
				readonly duplicate?: true | undefined;
		  }
		| {
				readonly ok: false;
				readonly sessionId: string;
				readonly reason:
					| "local_session_capacity_exceeded"
					| "control_conflict"
					| "context_load_failed";
				readonly retryable?: boolean | undefined;
		  }
	>;
	readonly handleAgentMail: (
		command: RuntimeAgentMailCommand,
	) => Promise<
		| {
				readonly ok: true;
				readonly sessionId: string;
				readonly applied: boolean;
		  }
		| {
				readonly ok: false;
				readonly sessionId: string;
				readonly reason:
					| "local_session_capacity_exceeded"
					| "thread_not_receivable"
					| "context_load_failed";
				readonly retryable?: boolean | undefined;
		  }
	>;
	readonly handleInterruptControl: (
		sessionId: string,
		command: RuntimeInterruptCommand,
		commitInterruptInput: (
			declaration: RuntimeControlInputDeclaration,
		) => Promise<RuntimeControlInputCommitResult>,
	) => Promise<
		| {
				readonly ok: true;
				readonly sessionId: string;
				readonly created: boolean;
				readonly interrupted: boolean;
				readonly idleInterrupt: boolean;
				readonly duplicate?: true | undefined;
				readonly stale?: true | undefined;
		  }
		| {
				readonly ok: false;
				readonly sessionId: string;
				readonly reason:
					| "local_session_capacity_exceeded"
					| "control_busy"
					| "context_load_failed";
				readonly retryable?: boolean | undefined;
				readonly errorCode?: string | number | undefined;
		  }
	>;
	readonly handleToolConfirmation: (
		sessionId: string,
		command: RuntimeToolConfirmationCommand,
		commit: (
			declaration: RuntimeControlInputDeclaration,
		) => Promise<RuntimeControlInputCommitResult>,
	) => Promise<
		| {
				readonly ok: true;
				readonly sessionId: string;
				readonly created: boolean;
				readonly applied: boolean;
				readonly stale?: true | undefined;
		  }
		| {
				readonly ok: false;
				readonly sessionId: string;
				readonly reason:
					| "local_session_capacity_exceeded"
					| "control_busy"
					| "control_conflict"
					| "context_load_failed";
		  }
	>;
	readonly handleTaskNotification: (
		sessionId: string,
		command: RuntimeTaskNotificationCommand,
	) => Promise<
		| {
				readonly ok: true;
				readonly sessionId: string;
				readonly created: boolean;
				readonly applied: boolean;
		  }
		| {
				readonly ok: false;
				readonly sessionId: string;
				readonly reason:
					| "local_session_capacity_exceeded"
					| "control_busy"
					| "control_conflict"
					| "context_load_failed";
		  }
	>;
	readonly handleRuntimeConfigPatch: (
		sessionId: string,
		command: RuntimeConfigCommand,
	) => Promise<
		| {
				readonly ok: true;
				readonly sessionId: string;
				readonly created: boolean;
				readonly applied: boolean;
				readonly noResidency?: true | undefined;
		  }
		| {
				readonly ok: false;
				readonly sessionId: string;
				readonly reason:
					| "local_session_capacity_exceeded"
					| "control_busy"
					| "control_conflict"
					| "context_load_failed";
		  }
	>;
}

export interface RuntimeControlInputCommitter {
	readonly commitControlInput: (input: {
		readonly scope: RuntimeInputScope;
		readonly inputKind: "interrupt_control" | "tool_confirmation";
	}) => Promise<
		| { readonly ok: true; readonly type: "stale" }
		| {
				readonly ok: true;
				readonly type: "committed";
				readonly assignedContextSequences: readonly number[];
				readonly pendingAttachments: readonly RuntimeProviderAttachment[];
				readonly interruptToolResults: readonly RuntimeInterruptToolResult[];
		  }
		| {
				readonly ok: false;
				readonly retryable: boolean;
				readonly errorCode: string;
				readonly message: string;
		  }
	>;
}

export interface RuntimeCleanupController {
	readonly startCleanup: (
		scope: RuntimeCleanupCommand,
		reason: "expired" | "operator_requested",
	) => Promise<
		| {
				readonly ok: true;
				readonly sessionId: string;
				readonly completion: Promise<{
					readonly ok: true;
					readonly sessionId: string;
					readonly cleaned: boolean;
				}>;
		  }
		| {
				readonly ok: false;
				readonly sessionId: string;
				readonly reason: "session_busy";
		  }
	>;
}

export interface RuntimeCommandRunner {
	readonly runCommand: <Response>(
		command: (lease: RuntimeCommandLease) => Promise<Response>,
	) => Promise<Response>;
}

export interface RuntimeControlServiceOptions {
	readonly ownPod: {
		readonly namespace: string;
		readonly name: string;
		readonly uid: string;
		readonly ip: string;
	};
	readonly allowedBridge: ServiceAccountIdentity;
	readonly authenticator: RuntimeAuthenticator;
	readonly runHost: RuntimeSessionRunHost;
	readonly controlInputCommitter?: RuntimeControlInputCommitter;
	readonly cleanupController: RuntimeCleanupController;
	readonly logger: RuntimePodLogger;
	readonly ready: () => boolean;
	readonly commandRunner?: RuntimeCommandRunner;
	readonly metrics?: RuntimeMetricsSink | undefined;
}

const Methods = {
	acceptInput: "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptInput",
	acceptAgentMail:
		"/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptAgentMail",
	acceptTaskNotification:
		"/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptTaskNotification",
	interrupt: "/tetral.agent_runtime.v1.AgentRuntimePodService/Interrupt",
	resolveToolConfirmation:
		"/tetral.agent_runtime.v1.AgentRuntimePodService/ResolveToolConfirmation",
	applyRuntimeConfig:
		"/tetral.agent_runtime.v1.AgentRuntimePodService/ApplyRuntimeConfig",
	cleanupSession:
		"/tetral.agent_runtime.v1.AgentRuntimePodService/CleanupSession",
} as const;

interface InFlight<Response> {
	readonly identity: string;
	readonly result: Promise<Response>;
	readonly resolve: (response: Response) => void;
	readonly reject: (error: unknown) => void;
}

interface ActiveSessionBinding {
	readonly bindingId: string;
	readonly bindingGeneration: number;
}

interface RuntimeConfigMemoEntry {
	readonly configIdentity: string;
	readonly contentJson: string;
}

interface IngressRejectionDiagnostic {
	readonly phase: RuntimeIngressRejectionPhase;
	readonly reason: RuntimeIngressRejectionReason;
	readonly grpcCode?: string | undefined;
}

interface MethodExecution<Request extends RuntimeSessionScope, Response> {
	readonly request: Request;
	readonly metadata: Metadata;
	readonly method: string;
	readonly operation: string;
	readonly operationId: string;
	readonly dedupeKey: string;
	readonly identity: () => string;
	readonly validate: (request: Request) => {
		readonly ok: boolean;
		readonly message?: string;
	};
	readonly selectedPodRejected: () => Response;
	readonly bindingRejected: () => Response;
	readonly identityRejected: () => Response;
	readonly duplicate: (response: Response) => Response;
	readonly rejected: (response: Response) => boolean;
	readonly rejectionDiagnostic?:
		| ((response: Response) => IngressRejectionDiagnostic)
		| undefined;
	readonly apply: () => Promise<Response>;
	readonly acceptedBinding: boolean;
}

export class RuntimeControlService {
	private acceptingCommands = true;
	private readonly inFlight = new Map<string, InFlight<unknown>>();
	private readonly activeBindings = new Map<string, ActiveSessionBinding>();
	// One entry per active configuration family is enough to detect payload
	// conflicts for the current identity. A newer generation supersedes the old
	// identity; non-resident sessions and completed cleanup retain no memo state.
	private readonly configContentMemo = new Map<
		string,
		Map<string, RuntimeConfigMemoEntry>
	>();

	constructor(private readonly options: RuntimeControlServiceOptions) {}

	beginShutdown(): void {
		this.acceptingCommands = false;
	}

	async acceptInput(
		request: AcceptInputRequest,
		metadata: Metadata,
	): Promise<AcceptInputResponse> {
		const scope = inputScope(request);
		return await this.executeMethod({
			request,
			metadata,
			method: Methods.acceptInput,
			operation: "AcceptInput",
			operationId: request.runtimeInputId,
			dedupeKey: inputDedupeKey("input", request),
			identity: () => stableIdentity(request),
			validate: validateAcceptInputRequest,
			selectedPodRejected: () =>
				acceptInputRejected(
					AcceptInputFailure.ACCEPT_INPUT_FAILURE_SELECTED_POD_MISMATCH,
					true,
				),
			bindingRejected: () =>
				acceptInputRejected(
					AcceptInputFailure.ACCEPT_INPUT_FAILURE_BINDING_MISMATCH,
					true,
				),
			identityRejected: () =>
				acceptInputRejected(
					AcceptInputFailure.ACCEPT_INPUT_FAILURE_IDENTITY_CONFLICT,
					false,
				),
			duplicate: (response) =>
				response.rejected === undefined ? { duplicate: {} } : response,
			rejected: (response) => response.rejected !== undefined,
			apply: async () => {
				const result = await this.options.runHost.handleAcceptInput(
					acceptInputCommand(request, scope),
				);
				if (!result.ok) {
					if (result.reason === "control_conflict") {
						return acceptInputRejected(
							AcceptInputFailure.ACCEPT_INPUT_FAILURE_IDENTITY_CONFLICT,
							false,
						);
					}
					if (result.reason === "context_load_failed") {
						return acceptInputRejected(
							AcceptInputFailure.ACCEPT_INPUT_FAILURE_CONTEXT_LOAD_FAILED,
							result.retryable ?? false,
						);
					}
					throw new GrpcStatusError(
						status.RESOURCE_EXHAUSTED,
						"local runtime capacity exceeded",
					);
				}
				return result.duplicate === true ? { duplicate: {} } : { accepted: {} };
			},
			acceptedBinding: true,
		});
	}

	async acceptAgentMail(
		request: AcceptAgentMailRequest,
		metadata: Metadata,
	): Promise<AcceptAgentMailResponse> {
		const scope = inputScope(request);
		return await this.executeMethod({
			request,
			metadata,
			method: Methods.acceptAgentMail,
			operation: "AcceptAgentMail",
			operationId: request.runtimeInputId,
			dedupeKey: inputDedupeKey("agent-mail", request),
			identity: () => stableIdentity(request),
			validate: validateAcceptAgentMailRequest,
			selectedPodRejected: () =>
				acceptAgentMailRejected(
					AcceptAgentMailFailure.ACCEPT_AGENT_MAIL_FAILURE_SELECTED_POD_MISMATCH,
					true,
				),
			bindingRejected: () =>
				acceptAgentMailRejected(
					AcceptAgentMailFailure.ACCEPT_AGENT_MAIL_FAILURE_BINDING_MISMATCH,
					true,
				),
			identityRejected: () =>
				acceptAgentMailRejected(
					AcceptAgentMailFailure.ACCEPT_AGENT_MAIL_FAILURE_IDENTITY_CONFLICT,
					false,
				),
			duplicate: (response) =>
				response.rejected === undefined ? { duplicate: {} } : response,
			rejected: (response) => response.rejected !== undefined,
			apply: async () => {
				const result = await this.options.runHost.handleAgentMail({
					...scope,
					kind: "inter_agent_message",
					deliveryId: request.deliveryId,
					content: request.content,
				});
				if (!result.ok) {
					if (result.reason === "local_session_capacity_exceeded") {
						throw new GrpcStatusError(
							status.RESOURCE_EXHAUSTED,
							"local runtime capacity exceeded",
						);
					}
					const reason =
						result.reason === "thread_not_receivable"
							? AcceptAgentMailFailure.ACCEPT_AGENT_MAIL_FAILURE_TARGET_NOT_RECEIVABLE
							: AcceptAgentMailFailure.ACCEPT_AGENT_MAIL_FAILURE_CONTEXT_LOAD_FAILED;
					return acceptAgentMailRejected(
						reason,
						result.reason === "context_load_failed"
							? (result.retryable ?? false)
							: false,
					);
				}
				return result.applied ? { accepted: {} } : { duplicate: {} };
			},
			acceptedBinding: true,
		});
	}

	async acceptTaskNotification(
		request: AcceptTaskNotificationRequest,
		metadata: Metadata,
	): Promise<AcceptTaskNotificationResponse> {
		const scope = inputScope(request);
		return await this.executeMethod({
			request,
			metadata,
			method: Methods.acceptTaskNotification,
			operation: "AcceptTaskNotification",
			operationId: request.runtimeInputId,
			dedupeKey: inputDedupeKey("task-notification", request),
			identity: () => stableIdentity(request),
			validate: validateAcceptTaskNotificationRequest,
			selectedPodRejected: () =>
				acceptTaskRejected(
					AcceptTaskNotificationFailure.ACCEPT_TASK_NOTIFICATION_FAILURE_SELECTED_POD_MISMATCH,
					true,
				),
			bindingRejected: () =>
				acceptTaskRejected(
					AcceptTaskNotificationFailure.ACCEPT_TASK_NOTIFICATION_FAILURE_BINDING_MISMATCH,
					true,
				),
			identityRejected: () =>
				acceptTaskRejected(
					AcceptTaskNotificationFailure.ACCEPT_TASK_NOTIFICATION_FAILURE_IDENTITY_CONFLICT,
					false,
				),
			duplicate: (response) =>
				response.rejected === undefined ? { duplicate: {} } : response,
			rejected: (response) => response.rejected !== undefined,
			apply: async () => {
				const content = taskNotificationContent(request.notificationJson);
				const result = await this.options.runHost.handleTaskNotification(
					request.sessionId,
					{
						...scope,
						kind: "task_notification",
						inputOrder: request.inputOrder,
						taskId: content.taskId,
						sourceToolUseEventId: content.sourceToolUseEventId,
						status: content.status,
						notificationJson: request.notificationJson,
					},
				);
				if (!result.ok) {
					return acceptTaskRejected(
						taskFailure(result.reason),
						result.reason === "control_busy" ||
							result.reason === "context_load_failed",
					);
				}
				return result.applied ? { accepted: {} } : { duplicate: {} };
			},
			acceptedBinding: true,
		});
	}

	async interrupt(
		request: InterruptRequest,
		metadata: Metadata,
	): Promise<InterruptResponse> {
		const scope = inputScope(request);
		return await this.executeMethod({
			request,
			metadata,
			method: Methods.interrupt,
			operation: "Interrupt",
			operationId: request.runtimeInputId,
			dedupeKey: inputDedupeKey("interrupt", request),
			identity: () => stableIdentity(request),
			validate: validateInterruptRequest,
			selectedPodRejected: () =>
				interruptRejected(
					InterruptFailure.INTERRUPT_FAILURE_SELECTED_POD_MISMATCH,
					true,
				),
			bindingRejected: () =>
				interruptRejected(
					InterruptFailure.INTERRUPT_FAILURE_BINDING_MISMATCH,
					true,
				),
			identityRejected: () =>
				interruptRejected(
					InterruptFailure.INTERRUPT_FAILURE_IDENTITY_CONFLICT,
					false,
				),
			duplicate: (response) =>
				response.rejected === undefined ? { duplicate: {} } : response,
			rejected: (response) => response.rejected !== undefined,
			apply: async () => {
				let committed: RuntimeControlInputCommitResult | undefined;
				const result = await this.options.runHost.handleInterruptControl(
					request.sessionId,
					{
						...scope,
						inputOrder: request.inputOrder,
						origin:
							request.origin === InterruptOrigin.INTERRUPT_ORIGIN_AGENT
								? "agent"
								: "user",
					},
					async (declaration) => {
						committed = await this.commitControlInput(scope, declaration);
						return committed;
					},
				);
				if (!result.ok) {
					if (committed?.ok === false) {
						return interruptRejected(
							commitInterruptFailure(committed.errorCode),
							committed.retryable,
						);
					}
					if (result.reason === "local_session_capacity_exceeded") {
						throw new GrpcStatusError(
							status.RESOURCE_EXHAUSTED,
							"local runtime capacity exceeded",
						);
					}
					return interruptRejected(
						result.reason === "control_busy"
							? InterruptFailure.INTERRUPT_FAILURE_CONTROL_BUSY
							: InterruptFailure.INTERRUPT_FAILURE_CONTEXT_LOAD_FAILED,
						result.retryable ?? result.reason === "control_busy",
					);
				}
				if (result.duplicate === true) {
					return { duplicate: {} };
				}
				if (result.idleInterrupt && committed === undefined) {
					throw new GrpcStatusError(
						status.INTERNAL,
						"idle interrupt completed without a durable declaration",
					);
				}
				return { accepted: {} };
			},
			acceptedBinding: true,
		});
	}

	async resolveToolConfirmation(
		request: ResolveToolConfirmationRequest,
		metadata: Metadata,
	): Promise<ResolveToolConfirmationResponse> {
		const scope = inputScope(request);
		return await this.executeMethod({
			request,
			metadata,
			method: Methods.resolveToolConfirmation,
			operation: "ResolveToolConfirmation",
			operationId: request.runtimeInputId,
			dedupeKey: inputDedupeKey("tool-confirmation", request),
			identity: () => stableIdentity(request),
			validate: validateResolveToolConfirmationRequest,
			selectedPodRejected: () =>
				resolveToolRejected(
					ResolveToolConfirmationFailure.RESOLVE_TOOL_CONFIRMATION_FAILURE_SELECTED_POD_MISMATCH,
					true,
				),
			bindingRejected: () =>
				resolveToolRejected(
					ResolveToolConfirmationFailure.RESOLVE_TOOL_CONFIRMATION_FAILURE_BINDING_MISMATCH,
					true,
				),
			identityRejected: () =>
				resolveToolRejected(
					ResolveToolConfirmationFailure.RESOLVE_TOOL_CONFIRMATION_FAILURE_IDENTITY_CONFLICT,
					false,
				),
			duplicate: (response) =>
				response.rejected === undefined ? { duplicate: {} } : response,
			rejected: (response) => response.rejected !== undefined,
			apply: async () => {
				let committed: RuntimeControlInputCommitResult | undefined;
				const result = await this.options.runHost.handleToolConfirmation(
					request.sessionId,
					{
						...scope,
						toolUseEventId: request.toolUseEventId,
						decision:
							request.decision ===
							ToolConfirmationDecision.TOOL_CONFIRMATION_DECISION_DENY
								? "deny"
								: "allow",
						...(request.denyMessage === undefined
							? {}
							: { denyMessage: request.denyMessage }),
					},
					async (declaration) => {
						committed = await this.commitControlInput(scope, declaration);
						return committed;
					},
				);
				if (!result.ok && committed?.ok === false) {
					return resolveToolRejected(
						commitToolFailure(committed.errorCode),
						committed.retryable,
					);
				}
				if (!result.ok) {
					if (result.reason === "local_session_capacity_exceeded") {
						throw new GrpcStatusError(
							status.RESOURCE_EXHAUSTED,
							"local runtime capacity exceeded",
						);
					}
					if (result.reason === "control_busy") {
						throw new GrpcStatusError(
							status.UNAVAILABLE,
							"runtime control command busy",
						);
					}
					return resolveToolRejected(
						result.reason === "control_conflict"
							? ResolveToolConfirmationFailure.RESOLVE_TOOL_CONFIRMATION_FAILURE_CONTROL_CONFLICT
							: ResolveToolConfirmationFailure.RESOLVE_TOOL_CONFIRMATION_FAILURE_CONTEXT_LOAD_FAILED,
						false,
					);
				}
				if ("stale" in result) {
					return { stale: {} };
				}
				return result.applied ? { accepted: {} } : { duplicate: {} };
			},
			acceptedBinding: true,
		});
	}

	async applyRuntimeConfig(
		request: ApplyRuntimeConfigRequest,
		metadata: Metadata,
	): Promise<ApplyRuntimeConfigResponse> {
		const scope = sessionScope(request);
		const configIdentity = runtimeConfigIdentity(request);
		return await this.executeMethod({
			request,
			metadata,
			method: Methods.applyRuntimeConfig,
			operation: "ApplyRuntimeConfig",
			operationId: configIdentity,
			dedupeKey: `runtime-config\u0000${sessionKey(request)}\u0000${configIdentity}`,
			identity: () => stableIdentity(request),
			validate: validateApplyRuntimeConfigRequest,
			selectedPodRejected: () =>
				applyConfigRejected(
					ApplyRuntimeConfigFailure.APPLY_RUNTIME_CONFIG_FAILURE_SELECTED_POD_MISMATCH,
					true,
				),
			bindingRejected: () =>
				applyConfigRejected(
					ApplyRuntimeConfigFailure.APPLY_RUNTIME_CONFIG_FAILURE_BINDING_MISMATCH,
					true,
				),
			identityRejected: () =>
				applyConfigRejected(
					ApplyRuntimeConfigFailure.APPLY_RUNTIME_CONFIG_FAILURE_IDENTITY_CONFLICT,
					false,
				),
			duplicate: (result) =>
				result.rejected === undefined ? { duplicate: {} } : result,
			rejected: (result) => result.rejected !== undefined,
			rejectionDiagnostic: (result) =>
				result.rejected?.reason ===
				ApplyRuntimeConfigFailure.APPLY_RUNTIME_CONFIG_FAILURE_IDENTITY_CONFLICT
					? { phase: "identity", reason: "identity_conflict" }
					: { phase: "application", reason: "operation_rejected" },
			apply: async () => {
				// Authentication, bounds, selected-Pod, and binding checks in executeMethod
				// deliberately precede both content parsing and this process-local memo probe.
				const command = runtimeConfigCommand(request, scope);
				const memoSessionKey = sessionKey(request);
				const memoFamily = runtimeConfigMemoFamily(command);
				const sessionMemo = this.configContentMemo.get(memoSessionKey);
				const existing = sessionMemo?.get(memoFamily);
				if (
					existing?.configIdentity === command.configIdentity &&
					existing.contentJson !== command.contentJson
				) {
					return applyConfigRejected(
						ApplyRuntimeConfigFailure.APPLY_RUNTIME_CONFIG_FAILURE_IDENTITY_CONFLICT,
						false,
					);
				}
				const result = await this.options.runHost.handleRuntimeConfigPatch(
					request.sessionId,
					command,
				);
				if (!result.ok) {
					return applyConfigRejected(
						configFailure(result.reason),
						result.reason === "control_busy" ||
							result.reason === "context_load_failed",
					);
				}
				if (result.noResidency === true) {
					this.deleteRuntimeConfigMemo(memoSessionKey, memoFamily);
					return { noResidency: {} };
				}
				const nextSessionMemo =
					sessionMemo ?? new Map<string, RuntimeConfigMemoEntry>();
				nextSessionMemo.set(memoFamily, {
					configIdentity: command.configIdentity,
					contentJson: command.contentJson,
				});
				this.configContentMemo.set(memoSessionKey, nextSessionMemo);
				return result.applied ? { applied: {} } : { duplicate: {} };
			},
			acceptedBinding: true,
		});
	}

	async cleanupSession(
		request: CleanupSessionRequest,
		metadata: Metadata,
	): Promise<CleanupSessionResponse> {
		const scope: RuntimeCleanupCommand = {
			...sessionScope(request),
			cleanupOperationId: request.cleanupOperationId,
		};
		return await this.executeMethod({
			request,
			metadata,
			method: Methods.cleanupSession,
			operation: "CleanupSession",
			operationId: request.cleanupOperationId,
			dedupeKey: `cleanup\u0000${sessionKey(request)}\u0000${request.cleanupOperationId}`,
			identity: () => stableIdentity(request),
			validate: validateCleanupSessionRequest,
			selectedPodRejected: () =>
				cleanupRejected(
					CleanupSessionFailure.CLEANUP_SESSION_FAILURE_SELECTED_POD_MISMATCH,
					true,
				),
			bindingRejected: () =>
				cleanupRejected(
					CleanupSessionFailure.CLEANUP_SESSION_FAILURE_BINDING_MISMATCH,
					true,
				),
			identityRejected: () =>
				cleanupRejected(
					CleanupSessionFailure.CLEANUP_SESSION_FAILURE_CLEANUP_FAILED,
					false,
				),
			duplicate: (response) =>
				response.rejected === undefined ? { duplicate: {} } : response,
			rejected: (response) => response.rejected !== undefined,
			apply: async () => {
				const cleanup = await this.options.cleanupController.startCleanup(
					scope,
					request.reason === CleanupSessionReason.CLEANUP_SESSION_REASON_EXPIRED
						? "expired"
						: "operator_requested",
				);
				if (!cleanup.ok) {
					runtimeMetrics(this.options).recordCleanupCommandOutcome("rejected");
					return cleanupRejected(
						CleanupSessionFailure.CLEANUP_SESSION_FAILURE_SESSION_BUSY,
						true,
					);
				}
				runtimeMetrics(this.options).recordCleanupCommandOutcome("accepted");
				try {
					await cleanup.completion;
					this.configContentMemo.delete(sessionKey(request));
					runtimeMetrics(this.options).recordCleanupCommandOutcome("completed");
					return { completed: {} };
				} catch {
					runtimeMetrics(this.options).recordCleanupCommandOutcome("failed");
					return cleanupRejected(
						CleanupSessionFailure.CLEANUP_SESSION_FAILURE_CLEANUP_FAILED,
						true,
					);
				}
			},
			acceptedBinding: false,
		});
	}

	private async executeMethod<Request extends RuntimeSessionScope, Response>(
		execution: MethodExecution<Request, Response>,
	): Promise<Response> {
		const startedAt = Date.now();
		let caller: { readonly serviceAccount: ServiceAccountIdentity };
		try {
			caller = await this.authorize(execution.metadata, execution.method);
		} catch (error) {
			this.logOutcome(
				execution,
				undefined,
				startedAt,
				{
					phase: "authentication",
					reason: "authentication_rejected",
					grpcCode: grpcCodeText(error),
				},
				false,
			);
			throw error;
		}
		let rejectionRecorded = false;
		const recordRejection = (
			diagnostic: IngressRejectionDiagnostic,
			identityTrusted: boolean,
		): void => {
			rejectionRecorded = true;
			this.logOutcome(
				execution,
				caller,
				startedAt,
				diagnostic,
				identityTrusted,
			);
		};
		const run = async (lease?: RuntimeCommandLease): Promise<Response> => {
			try {
				this.ensureAccepting(lease !== undefined);
				lease?.throwIfAborted();
			} catch (error) {
				recordRejection(
					{
						phase: "lifecycle",
						reason: "runtime_not_accepting",
						grpcCode: grpcCodeText(error),
					},
					false,
				);
				throw error;
			}
			const validation = execution.validate(execution.request);
			if (!validation.ok) {
				recordRejection(
					{
						phase: "request_validation",
						reason: "invalid_request",
						grpcCode: "InvalidArgument",
					},
					false,
				);
				throw new GrpcStatusError(
					status.INVALID_ARGUMENT,
					validation.message ?? "invalid internal request",
				);
			}
			if (execution.request.targetPodUid !== this.options.ownPod.uid) {
				const response = execution.selectedPodRejected();
				recordRejection(
					{ phase: "selected_pod", reason: "selected_pod_mismatch" },
					true,
				);
				return response;
			}
			if (!this.activeBindingMatches(execution.request)) {
				const response = execution.bindingRejected();
				recordRejection({ phase: "binding", reason: "binding_mismatch" }, true);
				return response;
			}
			const identity = execution.identity();
			const existing = this.inFlight.get(execution.dedupeKey) as
				| InFlight<Response>
				| undefined;
			if (existing !== undefined) {
				if (existing.identity !== identity) {
					const response = execution.identityRejected();
					recordRejection(
						{ phase: "identity", reason: "identity_conflict" },
						true,
					);
					return response;
				}
				let settled: Response;
				try {
					settled = await existing.result;
				} catch (error) {
					recordRejection(grpcRejectionDiagnostic(error), true);
					throw error;
				}
				const response = execution.duplicate(settled);
				if (execution.rejected(response)) {
					recordRejection(
						execution.rejectionDiagnostic?.(response) ?? {
							phase: "application",
							reason: "operation_rejected",
						},
						true,
					);
				} else {
					this.logOutcome(execution, caller, startedAt, undefined, true);
				}
				return response;
			}
			const reservation = createInFlight<Response>(identity);
			this.inFlight.set(execution.dedupeKey, reservation as InFlight<unknown>);
			const unregisterAbort = lease?.onAbort(() => {
				this.inFlight.delete(execution.dedupeKey);
				reservation.reject(
					new GrpcStatusError(
						status.FAILED_PRECONDITION,
						"runtime pod shutdown drain timed out",
					),
				);
			});
			try {
				const response = await execution.apply();
				lease?.throwIfAborted();
				if (!execution.rejected(response)) {
					this.recordAcceptedBinding(
						execution.request,
						execution.acceptedBinding,
					);
				}
				this.inFlight.delete(execution.dedupeKey);
				reservation.resolve(response);
				unregisterAbort?.();
				if (execution.rejected(response)) {
					recordRejection(
						execution.rejectionDiagnostic?.(response) ?? {
							phase: "application",
							reason: "operation_rejected",
						},
						true,
					);
				} else {
					this.logOutcome(execution, caller, startedAt, undefined, true);
				}
				return response;
			} catch (error) {
				unregisterAbort?.();
				this.inFlight.delete(execution.dedupeKey);
				reservation.reject(error);
				if (!rejectionRecorded) {
					recordRejection(grpcRejectionDiagnostic(error), true);
				}
				if (error instanceof GrpcStatusError) {
					throw error;
				}
				throw new GrpcStatusError(
					status.INTERNAL,
					"runtime pod command failed",
				);
			}
		};
		try {
			return this.options.commandRunner === undefined
				? await run()
				: await this.options.commandRunner.runCommand(
						async (lease) => await run(lease),
					);
		} catch (error) {
			if (!rejectionRecorded) {
				recordRejection(
					{
						phase: "lifecycle",
						reason: "runtime_not_accepting",
						grpcCode: grpcCodeText(error),
					},
					false,
				);
			}
			throw error;
		}
	}

	private async commitControlInput(
		scope: RuntimeInputScope,
		declaration: RuntimeControlInputDeclaration,
	): Promise<RuntimeControlInputCommitResult> {
		if (this.options.controlInputCommitter === undefined) {
			throw new GrpcStatusError(
				status.INTERNAL,
				"control input durable committer is unavailable",
			);
		}
		const result = await this.options.controlInputCommitter.commitControlInput({
			scope,
			inputKind:
				declaration.inputKind === "interrupt"
					? "interrupt_control"
					: "tool_confirmation",
		});
		if (!result.ok) {
			return {
				ok: false,
				retryable: result.retryable,
				errorCode: result.errorCode,
			};
		}
		if (result.type === "stale") {
			return { ok: true, stale: true };
		}
		return {
			ok: true,
			type: result.type,
			assignedContextSequences: result.assignedContextSequences,
			pendingAttachments: result.pendingAttachments,
			interruptToolResults: result.interruptToolResults,
		};
	}

	private deleteRuntimeConfigMemo(
		memoSessionKey: string,
		memoFamily: string,
	): void {
		const sessionMemo = this.configContentMemo.get(memoSessionKey);
		if (sessionMemo === undefined) {
			return;
		}
		sessionMemo.delete(memoFamily);
		if (sessionMemo.size === 0) {
			this.configContentMemo.delete(memoSessionKey);
		}
	}

	private async authorize(
		metadata: Metadata,
		method: string,
	): Promise<{ readonly serviceAccount: ServiceAccountIdentity }> {
		const result = await this.options.authenticator.authenticate({
			metadata,
			method,
		});
		if (!result.ok) {
			throw new GrpcStatusError(
				result.code === "Unauthenticated"
					? status.UNAUTHENTICATED
					: status.PERMISSION_DENIED,
				result.message,
			);
		}
		if (
			result.serviceAccount.namespace !==
				this.options.allowedBridge.namespace ||
			result.serviceAccount.name !== this.options.allowedBridge.name
		) {
			throw new GrpcStatusError(status.PERMISSION_DENIED, "permission denied");
		}
		return { serviceAccount: result.serviceAccount };
	}

	private ensureAccepting(acceptedByLifecycle: boolean): void {
		if (
			!acceptedByLifecycle &&
			(!this.options.ready() || !this.acceptingCommands)
		) {
			throw new GrpcStatusError(
				status.FAILED_PRECONDITION,
				"runtime pod not ready",
			);
		}
	}

	private activeBindingMatches(scope: RuntimeSessionScope): boolean {
		const active = this.activeBindings.get(sessionKey(scope));
		return (
			active === undefined ||
			(active.bindingId === scope.bindingId &&
				active.bindingGeneration === scope.bindingGeneration)
		);
	}

	private recordAcceptedBinding(
		scope: RuntimeSessionScope,
		retain: boolean,
	): void {
		if (!retain) {
			this.activeBindings.delete(sessionKey(scope));
			return;
		}
		this.activeBindings.set(sessionKey(scope), {
			bindingId: scope.bindingId,
			bindingGeneration: scope.bindingGeneration,
		});
	}

	private logOutcome<Request extends RuntimeSessionScope, Response>(
		execution: MethodExecution<Request, Response>,
		caller: { readonly serviceAccount: ServiceAccountIdentity } | undefined,
		startedAt: number,
		rejection: IngressRejectionDiagnostic | undefined,
		identityTrusted: boolean,
	): void {
		const rejected = rejection !== undefined;
		const errorFields =
			rejection !== undefined
				? semanticErrorFields({
						errorClass: "runtime_command_rejected",
						errorCode: rejection.reason,
						messageSafe: "runtime command rejected",
					})
				: {};
		try {
			this.options.logger.info({
				event: rejected
					? "runtime_command_rejected"
					: "runtime_command_accepted",
				"event.kind": rejected
					? "runtime_command_rejected"
					: "runtime_command_accepted",
				operation: execution.operation,
				component: "runtime-control-service",
				"grpc.method": execution.method,
				"grpc.code": rejection?.grpcCode ?? "OK",
				...(caller === undefined
					? {}
					: {
							"caller.service_account": serviceAccountText(
								caller.serviceAccount,
							),
						}),
				"duration.ms": Date.now() - startedAt,
				...(identityTrusted
					? {
							"workspace.id": execution.request.workspaceId,
							"session.id": execution.request.sessionId,
							...(hasThread(execution.request)
								? { "thread.id": execution.request.sessionThreadId }
								: {}),
							"operation.id": execution.operationId,
							"binding.id": execution.request.bindingId,
						}
					: {}),
				...(rejection === undefined
					? {}
					: { phase: rejection.phase, reason: rejection.reason }),
				...errorFields,
			});
		} catch {
			// Command handling is authoritative; ingress diagnostics are fail-open.
		}
	}
}

function sessionScope(input: RuntimeSessionScope): RuntimeSessionScope {
	return {
		workspaceId: input.workspaceId,
		sessionId: input.sessionId,
		bindingId: input.bindingId,
		bindingGeneration: input.bindingGeneration,
		targetPodUid: input.targetPodUid,
	};
}

function inputScope(input: RuntimeInputScope): RuntimeInputScope {
	return {
		...sessionScope(input),
		sessionThreadId: input.sessionThreadId,
		runtimeInputId: input.runtimeInputId,
	};
}

function acceptInputCommand(
	request: AcceptInputRequest,
	scope: RuntimeInputScope,
): RuntimeAcceptInputCommand {
	if (request.rejection !== undefined) {
		return {
			...scope,
			inputOrder: request.inputOrder,
			kind: "rejection",
			reasonCode:
				request.rejection.reason ===
				AcceptInputRejectionReason.ACCEPT_INPUT_REJECTION_REASON_PAYLOAD_TOO_LARGE
					? "runtime_command_payload_too_large"
					: "runtime_command_rejected",
		};
	}
	return {
		...scope,
		inputOrder: request.inputOrder,
		kind: "messages",
		contentJson: request.messagesJson ?? "",
	};
}

function taskNotificationContent(notificationJson: string): {
	readonly taskId: string;
	readonly sourceToolUseEventId: string;
	readonly status: RuntimeTaskNotificationCommand["status"];
} {
	const payload = parseObjectContent(notificationJson, "task notification");
	const taskId = stringField(payload, "task_id");
	const sourceToolUseEventId = stringField(payload, "source_tool_use_event_id");
	const statusValue = stringField(payload, "status");
	if (
		taskId === undefined ||
		taskId.length === 0 ||
		sourceToolUseEventId === undefined ||
		sourceToolUseEventId.length === 0 ||
		(statusValue !== "completed" &&
			statusValue !== "failed" &&
			statusValue !== "cancelled" &&
			statusValue !== "expired")
	) {
		throw new GrpcStatusError(
			status.INVALID_ARGUMENT,
			"invalid task notification content",
		);
	}
	for (const field of ["stdout", "stderr"] as const) {
		const value = payload[field];
		if (
			!isObjectRecord(value) ||
			typeof value.text !== "string" ||
			typeof value.truncated !== "boolean"
		) {
			throw new GrpcStatusError(
				status.INVALID_ARGUMENT,
				"invalid task notification content",
			);
		}
	}
	return { taskId, sourceToolUseEventId, status: statusValue };
}

function runtimeConfigCommand(
	request: ApplyRuntimeConfigRequest,
	scope: RuntimeSessionScope,
): RuntimeConfigCommand {
	if (request.sessionConfig !== undefined) {
		return {
			...scope,
			configIdentity: `session:${request.sessionConfig.generation}`,
			generation: request.sessionConfig.generation,
			contentJson: request.sessionConfig.contentJson,
		};
	}
	const manifest = request.mcpManifest!;
	const payload = parseObjectContent(manifest.contentJson, "runtime config");
	const manifestPayload = isObjectRecord(payload.mcp_manifest)
		? payload.mcp_manifest
		: payload;
	const readinessValue = stringField(manifestPayload, "readiness") ?? "ready";
	const manifestETag = stringField(manifestPayload, "manifest_etag");
	const diagnostic = stringField(manifestPayload, "diagnostic");
	if (readinessValue !== "ready" && readinessValue !== "unready") {
		throw new GrpcStatusError(
			status.INVALID_ARGUMENT,
			"invalid runtime config content",
		);
	}
	if (readinessValue === "ready" && manifestETag === undefined) {
		throw new GrpcStatusError(
			status.INVALID_ARGUMENT,
			"invalid runtime config content",
		);
	}
	return {
		...scope,
		configIdentity: `mcp:${manifest.mcpServerName}:${manifest.generation}`,
		generation: manifest.generation,
		mcpServerName: manifest.mcpServerName,
		...(manifestETag === undefined ? {} : { manifestETag }),
		manifestReadiness: readinessValue,
		...(diagnostic === undefined ? {} : { manifestDiagnostic: diagnostic }),
		contentJson: manifest.contentJson,
	};
}

function runtimeConfigIdentity(request: ApplyRuntimeConfigRequest): string {
	if (request.sessionConfig !== undefined) {
		return `session:${request.sessionConfig.generation}`;
	}
	if (request.mcpManifest !== undefined) {
		return `mcp:${request.mcpManifest.mcpServerName}:${request.mcpManifest.generation}`;
	}
	// Validation rejects a missing variant before this value can enter a trusted log.
	return "invalid-runtime-config";
}

function runtimeConfigMemoFamily(command: RuntimeConfigCommand): string {
	return command.mcpServerName === undefined
		? "session"
		: `mcp:${command.mcpServerName}`;
}

function stableIdentity(value: unknown): string {
	return JSON.stringify(value);
}

function sessionKey(input: {
	readonly workspaceId: string;
	readonly sessionId: string;
}): string {
	return `${input.workspaceId}\u0000${input.sessionId}`;
}

function inputDedupeKey(method: string, input: RuntimeInputScope): string {
	return `${method}\u0000${input.workspaceId}\u0000${input.runtimeInputId}`;
}

function createInFlight<Response>(identity: string): InFlight<Response> {
	let resolve: (response: Response) => void = () => undefined;
	let reject: (error: unknown) => void = () => undefined;
	const result = new Promise<Response>((done, fail) => {
		resolve = done;
		reject = fail;
	});
	result.catch(() => undefined);
	return { identity, result, resolve, reject };
}

function acceptInputRejected(
	reason: AcceptInputFailure,
	retryable: boolean,
): AcceptInputResponse {
	return { rejected: { reason, retryable } };
}

function acceptAgentMailRejected(
	reason: AcceptAgentMailFailure,
	retryable: boolean,
): AcceptAgentMailResponse {
	return { rejected: { reason, retryable } };
}

function acceptTaskRejected(
	reason: AcceptTaskNotificationFailure,
	retryable: boolean,
): AcceptTaskNotificationResponse {
	return { rejected: { reason, retryable } };
}

function interruptRejected(
	reason: InterruptFailure,
	retryable: boolean,
): InterruptResponse {
	return { rejected: { reason, retryable } };
}

function resolveToolRejected(
	reason: ResolveToolConfirmationFailure,
	retryable: boolean,
): ResolveToolConfirmationResponse {
	return { rejected: { reason, retryable } };
}

function applyConfigRejected(
	reason: ApplyRuntimeConfigFailure,
	retryable: boolean,
): ApplyRuntimeConfigResponse {
	return { rejected: { reason, retryable } };
}

function cleanupRejected(
	reason: CleanupSessionFailure,
	retryable: boolean,
): CleanupSessionResponse {
	return { rejected: { reason, retryable } };
}

function taskFailure(reason: string): AcceptTaskNotificationFailure {
	switch (reason) {
		case "control_busy":
			return AcceptTaskNotificationFailure.ACCEPT_TASK_NOTIFICATION_FAILURE_CONTROL_BUSY;
		case "control_conflict":
			return AcceptTaskNotificationFailure.ACCEPT_TASK_NOTIFICATION_FAILURE_CONTROL_CONFLICT;
		case "context_load_failed":
			return AcceptTaskNotificationFailure.ACCEPT_TASK_NOTIFICATION_FAILURE_CONTEXT_LOAD_FAILED;
		default:
			throw new GrpcStatusError(
				status.RESOURCE_EXHAUSTED,
				"local runtime capacity exceeded",
			);
	}
}

function configFailure(reason: string): ApplyRuntimeConfigFailure {
	switch (reason) {
		case "control_busy":
			return ApplyRuntimeConfigFailure.APPLY_RUNTIME_CONFIG_FAILURE_CONTROL_BUSY;
		case "control_conflict":
			return ApplyRuntimeConfigFailure.APPLY_RUNTIME_CONFIG_FAILURE_CONTROL_CONFLICT;
		case "context_load_failed":
			return ApplyRuntimeConfigFailure.APPLY_RUNTIME_CONFIG_FAILURE_CONTEXT_LOAD_FAILED;
		default:
			throw new GrpcStatusError(
				status.RESOURCE_EXHAUSTED,
				"local runtime capacity exceeded",
			);
	}
}

function commitInterruptFailure(errorCode: string | number): InterruptFailure {
	return typeof errorCode === "string" &&
		(errorCode.includes("unavailable") || errorCode.includes("token"))
		? InterruptFailure.INTERRUPT_FAILURE_DURABLE_COMMIT_UNAVAILABLE
		: InterruptFailure.INTERRUPT_FAILURE_DURABLE_COMMIT_REJECTED;
}

function commitToolFailure(
	errorCode: string | number,
): ResolveToolConfirmationFailure {
	return typeof errorCode === "string" &&
		(errorCode.includes("unavailable") || errorCode.includes("token"))
		? ResolveToolConfirmationFailure.RESOLVE_TOOL_CONFIRMATION_FAILURE_DURABLE_COMMIT_UNAVAILABLE
		: ResolveToolConfirmationFailure.RESOLVE_TOOL_CONFIRMATION_FAILURE_DURABLE_COMMIT_REJECTED;
}

function parseObjectContent(
	contentJson: string,
	kind: string,
): Record<string, unknown> {
	try {
		const parsed = JSON.parse(contentJson) as unknown;
		if (!isObjectRecord(parsed)) {
			throw new Error("not an object");
		}
		return parsed;
	} catch {
		throw new GrpcStatusError(
			status.INVALID_ARGUMENT,
			`invalid ${kind} content`,
		);
	}
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringField(
	payload: Record<string, unknown>,
	field: string,
): string | undefined {
	const value = payload[field];
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

function hasThread(value: RuntimeSessionScope): value is RuntimeThreadScope {
	return (
		"sessionThreadId" in value && typeof value.sessionThreadId === "string"
	);
}

function serviceAccountText(serviceAccount: ServiceAccountIdentity): string {
	return `${serviceAccount.namespace}/${serviceAccount.name}`;
}

function runtimeMetrics(
	options: RuntimeControlServiceOptions,
): RuntimeMetricsSink {
	return options.metrics ?? NoopRuntimeMetricsSink;
}

function grpcRejectionDiagnostic(error: unknown): IngressRejectionDiagnostic {
	if (!(error instanceof GrpcStatusError)) {
		return {
			phase: "application",
			reason: "internal_failure",
			grpcCode: "Internal",
		};
	}
	switch (error.code) {
		case status.INVALID_ARGUMENT:
			return {
				phase: "content_parse",
				reason: "invalid_content",
				grpcCode: "InvalidArgument",
			};
		case status.RESOURCE_EXHAUSTED:
			return {
				phase: "application",
				reason: "resource_exhausted",
				grpcCode: "ResourceExhausted",
			};
		case status.UNAVAILABLE:
			return {
				phase: "application",
				reason: "operation_unavailable",
				grpcCode: "Unavailable",
			};
		case status.FAILED_PRECONDITION:
			return {
				phase: "lifecycle",
				reason: "runtime_not_accepting",
				grpcCode: "FailedPrecondition",
			};
		default:
			return {
				phase: "application",
				reason: "internal_failure",
				grpcCode: grpcCodeText(error),
			};
	}
}

function grpcCodeText(error: unknown): string {
	if (!(error instanceof GrpcStatusError)) return "Internal";
	switch (error.code) {
		case status.OK:
			return "OK";
		case status.CANCELLED:
			return "Cancelled";
		case status.INVALID_ARGUMENT:
			return "InvalidArgument";
		case status.FAILED_PRECONDITION:
			return "FailedPrecondition";
		case status.PERMISSION_DENIED:
			return "PermissionDenied";
		case status.RESOURCE_EXHAUSTED:
			return "ResourceExhausted";
		case status.UNAUTHENTICATED:
			return "Unauthenticated";
		case status.UNAVAILABLE:
			return "Unavailable";
		default:
			return "Internal";
	}
}
