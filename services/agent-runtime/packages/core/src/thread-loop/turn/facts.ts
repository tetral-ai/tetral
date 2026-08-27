/**
 * @packageDocumentation
 * Closed set of already committed lifecycle facts accepted by the Thread-turn
 * Reducer. Ordinary queued input and control commands are separate contracts.
 */

import type { ThreadTurnCheckpoint } from "./checkpoint.js";

export type ThreadTurnFact =
	| {
			readonly fact: "run_opened";
			readonly eventId: string;
	  }
	| {
			readonly fact: "inputs_committed";
			readonly eventId: string;
			readonly contextSequences: readonly number[];
	  }
	| {
			readonly fact: "request_started";
			readonly eventId: string;
			readonly modelRequestId: string;
			readonly requestKind: NonNullable<
				ThreadTurnCheckpoint["request"]
			>["requestKind"];
			readonly contextThroughMessageSequence: number;
			readonly consumedInputContextSequences: readonly number[];
	  }
	| {
			readonly fact: "tool_use_committed";
			readonly eventId: string;
			readonly modelRequestId: string;
			readonly modelToolCallId: string;
			readonly toolName: string;
	  }
	| {
			readonly fact: "internal_tool_repair_committed";
			readonly eventId: string;
			readonly modelRequestId: string;
			readonly modelToolCallId: string;
			readonly toolName: string;
	  }
	| {
			readonly fact: "tool_result_committed";
			readonly toolUseEventId: string;
			readonly outcome: "success" | "error" | "cancelled" | "unknown";
	  }
	| {
			readonly fact: "request_ended";
			readonly eventId: string;
			readonly modelRequestId: string;
			readonly isError: boolean;
			readonly errorKind?: string;
			readonly providerContextRetention: NonNullable<
				ThreadTurnCheckpoint["request"]
			>["requestEnd"] extends infer T
				? T extends { readonly providerContextRetention: infer R }
					? R
					: never
				: never;
			readonly reschedule?: {
				readonly attempt: number;
				readonly effectiveDeadline: string;
				readonly providerAttempts: number;
				readonly compactionAttempts: number;
			};
	  }
	| {
			readonly fact: "finish_idle_committed";
			readonly eventId: string;
			readonly stopReason:
				| { readonly type: "end_turn"; readonly failedRun?: true }
				| {
						readonly type: "requires_action";
						readonly eventIds: readonly string[];
				  }
				| {
						readonly type: "retries_exhausted";
						readonly failureEventId: string;
						readonly failedRun?: true;
				  };
	  }
	| {
			readonly fact: "interrupt_committed";
			readonly eventId: string;
	  }
	| {
			readonly fact: "terminal_closeout_committed";
			readonly eventId: string;
			readonly failureEventId: string;
			readonly disposition: "retries_exhausted" | "terminated";
	  };
