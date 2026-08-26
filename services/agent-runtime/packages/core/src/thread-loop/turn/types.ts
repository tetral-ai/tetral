/**
 * @packageDocumentation
 * Immutable vocabulary returned by the Reducer. State and nextStep describe the
 * stable lifecycle; dispatch is a one-time stack-local handoff consumed only by
 * ThreadLoop.
 */

import type { ThreadTurnCheckpoint } from "./checkpoint.js";

export type ThreadTurnState =
	| { readonly state: "idle" }
	| { readonly state: "ready_to_request" }
	| { readonly state: "request_open"; readonly modelRequestId: string }
	| { readonly state: "request_sealed"; readonly modelRequestId: string }
	| {
			readonly state: "waiting_for_tool_results";
			readonly modelRequestId: string;
	  }
	| { readonly state: "ready_to_finish" };

export type ThreadTurnNextStep =
	| { readonly action: "await_input" }
	| { readonly action: "prepare_next_request" }
	| { readonly action: "await_request_end"; readonly modelRequestId: string }
	| {
			readonly action: "resume_tool_routes";
			readonly modelRequestId: string;
			readonly toolUseEventIds: readonly string[];
	  }
	| {
			readonly action: "await_tool_results";
			readonly modelRequestId: string;
			readonly toolUseEventIds: readonly string[];
	  }
	| {
			readonly action: "finish_idle";
			readonly stopReason:
				| { readonly type: "end_turn" }
				| {
						readonly type: "requires_action";
						readonly eventIds: readonly string[];
				  }
				| { readonly type: "retries_exhausted" };
	  }
	| {
			readonly action: "continue_after_compaction";
			readonly modelRequestId: string;
	  }
	| { readonly action: "complete_reviewer"; readonly modelRequestId: string }
	| {
			readonly action: "apply_request_retry_or_reschedule";
			readonly modelRequestId: string;
	  }
	| {
			readonly action: "commit_accepted_input";
			readonly runtimeInputId: string;
	  }
	| { readonly action: "close_interrupted"; readonly modelRequestId?: string }
	| { readonly action: "close_failed"; readonly modelRequestId?: string };

export type ThreadTurnDispatch =
	| {
			readonly dispatch: "start_provider_request";
			readonly modelRequestId: string;
	  }
	| { readonly dispatch: "route_tool_use"; readonly toolUseEventId: string };

export interface ThreadTurnSnapshot {
	readonly state: ThreadTurnState;
	readonly nextStep: ThreadTurnNextStep;
}

export interface ThreadTurnTransition extends ThreadTurnSnapshot {
	readonly checkpoint: ThreadTurnCheckpoint;
	readonly dispatch?: ThreadTurnDispatch | undefined;
}

/** Decision-only projection of active input that is not Message context. */
export interface ThreadActiveInputView {
	readonly hasPendingAttachments: boolean;
}

/** Identifies an impossible durable fact or Thread-turn transition. */
export class ThreadTurnContractError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ThreadTurnContractError";
	}
}
