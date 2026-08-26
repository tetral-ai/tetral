import type {
	RuntimeContextEntry,
	RuntimeJsonValue,
	RuntimeOpenRequestDraft,
	RuntimeProviderAttachment,
} from "../../contracts/runtime.js";
import type { ThreadContextPrefix } from "../../session/context-manager.js";
import type { RuntimeConfigurationPatch } from "../../session/session-configuration.js";
import type {
	ThreadToolRouteView,
	ThreadTurnCheckpoint,
} from "../turn/checkpoint.js";
import type {
	RuntimeAcceptedThreadMetadataState,
	RuntimeInterAgentAcceptedInputState,
	RuntimeThreadAddressState,
} from "./accepted-input.js";

/** Generation-fenced runtime or per-server MCP configuration patch. */
export interface RuntimeConfigPatchState extends RuntimeConfigurationPatch {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly configIdentity: string;
}

/** Complete cold baseline installed before a resident thread serves commands. */
export interface RuntimeThreadPreloadState extends RuntimeThreadAddressState {
	readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
	readonly contextEntries: readonly RuntimeContextEntry[];
	readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
	readonly turnCheckpoint?: ThreadTurnCheckpoint | undefined;
	readonly turnToolRouteView?: ThreadToolRouteView | undefined;
	readonly threadContextPrefix?: ThreadContextPrefix | undefined;
	readonly runtimeBindingToken: string;
	readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
	readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
	readonly pendingToolUses?:
		| readonly RuntimePreloadedPendingToolUseState[]
		| undefined;
	readonly pendingSandboxExecutions?:
		| readonly RuntimePreloadedSandboxExecutionState[]
		| undefined;
	readonly pendingAttachments?: readonly RuntimeProviderAttachment[] | undefined;
	readonly pendingAgentMail?:
		| readonly RuntimeInterAgentAcceptedInputState[]
		| undefined;
}

/** Durable pending Tool state restored before a cold thread resumes. */
export interface RuntimePreloadedPendingToolUseState {
	readonly toolUseEventId: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly toolName: string;
	readonly input: RuntimeJsonValue;
	readonly decision?: "allow" | "deny" | undefined;
	readonly denyMessage?: string | undefined;
	readonly status: "pending" | "resolving";
}

/** Durable accepted Sandbox execution restored before its Tool Result exists. */
export interface RuntimePreloadedSandboxExecutionState {
	readonly toolUseEventId: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly toolName: string;
	readonly input: RuntimeJsonValue;
	readonly executionState:
		| "pending"
		| "preparing"
		| "running"
		| "waiting_activation"
		| "waiting_materialization"
		| "terminal_unconsumed";
}
