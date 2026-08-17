/**
 * @packageDocumentation
 * Defines the single cold thread-load boundary and the durable declaration
 * boundary used by Runtime orchestration. It guards the separation between a
 * direct durable cold facts and typed commands applied during one hot
 * residency. SessionManager calls the cold loader once per ThreadEntry;
 * ThreadLoop calls the declaration writer and binding-token adapter.
 */
import { Context, Layer } from "effect";
import type {
	RuntimeContextEntry,
	RuntimeInterruptToolResult,
	RuntimeJsonValue,
	RuntimeOpenRequestDraft,
	RuntimeProviderAttachment,
} from "../contracts/runtime.js";
import type { ThreadContextPrefix } from "../session/context-manager.js";
import type { RuntimeThreadIdentity } from "../thread-loop/thread-runtime.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeAcceptedThreadMetadataState,
	RuntimeConfigPatchState,
	RuntimePreloadedSandboxExecutionState,
	RuntimeThreadAddressState,
} from "../thread-loop/thread-state.js";
import type { ThreadTurnLoadFacts } from "../thread-loop/thread-turn-checkpoint.js";

/** Operations through which session orchestration cold-loads and commits thread state. */
export interface ContextLoader {
	/**
	 * Reads one complete durable-fact baseline for a ThreadEntry.
	 * A residency never calls this operation again after installation.
	 */
	readonly loadThreadContext?: (command: RuntimeThreadAddressState) => Promise<{
		readonly contextEntries: readonly RuntimeContextEntry[];
		readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
		readonly turnFacts: ThreadTurnLoadFacts;
		readonly threadContextPrefix?: ThreadContextPrefix | undefined;
		readonly runtimeBindingToken: string;
		readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
		readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
		readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
		readonly pendingToolUses?:
			| readonly RuntimeLoadedPendingToolUse[]
			| undefined;
		readonly pendingSandboxExecutions?:
			| readonly RuntimePreloadedSandboxExecutionState[]
			| undefined;
		readonly pendingAttachments?:
			| readonly RuntimeProviderAttachment[]
			| undefined;
		readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
	}>;
	/** Refreshes the binding credential without changing durable declaration state; stale custody rejects as superseded. */
	readonly refreshRuntimeBindingToken?: (
		identity: RuntimeThreadIdentity,
		options?: { readonly force?: boolean | undefined },
	) => Promise<string>;
	/** Commits one accepted input and returns only the facts consumed immediately by HotState. */
	readonly commitAcceptedInput?: (
		input: RuntimeAcceptedInputState,
		options?: { readonly approvalReviewText?: readonly string[] | undefined },
	) => Promise<AcceptedInputCommitResult>;
	/** Reads the latest durable envelope owned by the addressed target thread. */
	readonly readAgentMail?: (
		command: RuntimeThreadAddressState,
		sourceThreadId: string,
	) => Promise<RuntimeLoadedAgentMail | undefined>;
}

/** Result of committing one accepted command before mutating its hot thread state. */
export type AcceptedInputCommitResult =
	| {
			readonly type: "committed" | "duplicate";
			readonly assignedContextSequences: readonly number[];
			readonly pendingAttachments: readonly RuntimeProviderAttachment[];
			readonly interruptToolResults: readonly RuntimeInterruptToolResult[];
	  }
	| { readonly type: "task_notification_deferred" }
	| {
			readonly type: "task_notification_rejected";
			readonly errorCode:
				| "task_notification_result_invalid"
				| "task_notification_message_invalid"
				| "task_notification_payload_mismatch";
	  }
	| { readonly type: "stale_custody" }
	| { readonly type: "barrier_stale_custody" };

/** Durable pending tool-use state reconstructed during a cold thread load. */
export interface RuntimeLoadedPendingToolUse {
	readonly toolUseEventId: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly toolName: string;
	readonly input: RuntimeJsonValue;
	readonly decision?: "allow" | "deny" | undefined;
	readonly denyMessage?: string | undefined;
	readonly status: "pending" | "resolving";
}

/** Unconsumed agent mail reconstructed from the target thread's durable inbox. */
export interface RuntimeLoadedAgentMail {
	readonly deliveryId: string;
	readonly content: string;
}

/** Effect service tag through which orchestration obtains the active context loader. */
export class ContextLoaderService extends Context.Service<
	ContextLoaderService,
	ContextLoader
>()("tetral-agent/ContextLoader") {}

/** Provides a concrete context loader as an Effect layer. */
export function layer(
	loader: ContextLoader,
): Layer.Layer<ContextLoaderService> {
	return Layer.succeed(ContextLoaderService, ContextLoaderService.of(loader));
}
