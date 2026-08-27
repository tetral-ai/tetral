import type { RuntimeContextEntry } from "../../contracts/runtime.js";

/** Durable address and binding fence shared by commands for one resident thread. */
export interface RuntimeThreadAddressState {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
}

/** Queue-owned input identity and ordering fact admitted to one thread. */
export interface RuntimeAcceptedInputScopeState
	extends RuntimeThreadAddressState {
	readonly runtimeInputId: string;
	readonly inputOrder: number;
}

export type RuntimeThreadRoleState = "main" | "subagent" | "approval_reviewer";
export type RuntimeThreadVisibilityState = "public" | "internal";

/** Runtime-side thread lifecycle projected to durable public status events. */
export type RuntimeThreadStatusState =
	| "idle"
	| "running"
	| "requires_action"
	| "closed_for_runtime"
	| "rescheduling"
	| "terminated"
	| "failed";

export type RuntimeSubAgentTypeState = "general" | "research" | "worker";

/** Durable thread metadata accepted when a command first makes a thread resident. */
export interface RuntimeAcceptedThreadMetadataState {
	readonly parentThreadId?: string | undefined;
	readonly parentTaskName?: string | undefined;
	readonly role?: RuntimeThreadRoleState | undefined;
	readonly visibility?: RuntimeThreadVisibilityState | undefined;
	readonly taskName?: string | undefined;
	readonly agentType?:
		| RuntimeSubAgentTypeState
		| "approval_reviewer"
		| undefined;
	readonly status?: RuntimeThreadStatusState | undefined;
}

/** User-message input delivered as an opaque Bridge-classified payload. */
export interface RuntimeCommittedContextAcceptedInputState
	extends RuntimeAcceptedInputScopeState {
	readonly kind: "messages";
	readonly contentJson: string;
}

/** Fenced inter-agent delivery carrying its durably sourced user-message draft. */
export interface RuntimeInterAgentAcceptedInputState
	extends RuntimeThreadAddressState {
	readonly runtimeInputId: string;
	readonly kind: "inter_agent_message";
	readonly deliveryId: string;
	readonly content: string;
	readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
}

/** Internal reviewer input carrying the target case, transcript, and output schema. */
export interface RuntimeApprovalReviewAcceptedInputState
	extends RuntimeAcceptedInputScopeState {
	readonly kind: "approval_review";
	readonly reviewId: string;
	readonly parentThreadId: string;
	readonly targetModelToolCallId: string;
	readonly targetToolName: string;
	readonly promptText: readonly string[];
	readonly outputSchemaJson: string;
	readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
}

/** Authorizes replacement of one undeliverable input with a rejection projection. */
export interface RuntimeRejectionAcceptedInputState
	extends RuntimeAcceptedInputScopeState {
	readonly kind: "rejection";
	readonly reasonCode:
		| "runtime_command_payload_too_large"
		| "runtime_command_rejected";
}

/** Terminal background-task command before its durable context commit. */
export interface RuntimeTaskNotificationCommandState
	extends RuntimeAcceptedInputScopeState {
	readonly taskId: string;
	readonly sourceToolUseEventId: string;
	readonly status: "completed" | "failed" | "cancelled" | "expired";
	readonly notificationJson: string;
}

/** Terminal task fact waiting for the next serialized semantic turn. */
export interface RuntimeTaskNotificationAcceptedInputState
	extends RuntimeTaskNotificationCommandState {
	readonly kind: "task_notification";
}

/** Terminal task fact and its committed context entry installed together. */
export interface RuntimeTaskNotificationState
	extends RuntimeTaskNotificationCommandState {
	readonly committedEntry: RuntimeContextEntry;
}

/** Accepted input variants queued for one thread under their durable identities. */
export type RuntimeAcceptedInputState =
	| RuntimeCommittedContextAcceptedInputState
	| RuntimeInterAgentAcceptedInputState
	| RuntimeApprovalReviewAcceptedInputState
	| RuntimeRejectionAcceptedInputState
	| RuntimeTaskNotificationAcceptedInputState;
