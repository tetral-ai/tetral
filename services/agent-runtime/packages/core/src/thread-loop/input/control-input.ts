import type {
	RuntimeInterruptToolResult,
	RuntimeProviderAttachment,
} from "../../contracts/runtime.js";
import type { RuntimeThreadAddressState } from "./accepted-input.js";

/** Input identity for a control command that is not ordinary queued input. */
export interface RuntimeControlInputState extends RuntimeThreadAddressState {
	readonly runtimeInputId: string;
}

/** Interrupt identity retained by hot control state without Queue authority. */
export interface RuntimeInterruptCommandState extends RuntimeControlInputState {
	readonly origin: "user" | "agent";
}

export interface RuntimeControlInputDeclaration {
	readonly inputKind: "interrupt" | "tool_confirmation";
}

export type RuntimeControlInputCommitResult =
	| {
			readonly ok: true;
			readonly stale: true;
			readonly barrierStale?: true | undefined;
	  }
	| { readonly ok: true; readonly joined: true }
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
			readonly errorCode: string | number;
	  };

export type RuntimeControlInputCommit = (
	declaration: RuntimeControlInputDeclaration,
) => Promise<RuntimeControlInputCommitResult>;

export interface RuntimeControlInputCommitApplication {
	readonly declaration: RuntimeControlInputDeclaration;
	readonly result: RuntimeControlInputCommitResult;
}

/** Recorded user decision applied to one durable pending Tool Use. */
export interface RuntimeToolConfirmationState extends RuntimeControlInputState {
	readonly toolUseEventId: string;
	readonly decision: "allow" | "deny";
	readonly denyMessage?: string | undefined;
}
