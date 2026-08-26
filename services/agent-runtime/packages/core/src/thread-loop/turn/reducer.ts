/**
 * @packageDocumentation
 * Pure deterministic Thread-turn rules. The Reducer accepts one checkpoint,
 * read-only views and optionally one committed Fact, then returns an immutable
 * transition. It owns no mutable data, I/O, timers, clients or dispatch
 * execution; ThreadLoop owns Fact application and any returned dispatch.
 */

import type {
	ThreadToolRouteView,
	ThreadTurnCheckpoint,
} from "./checkpoint.js";
import {
	parseThreadToolRouteView,
	parseThreadTurnCheckpoint,
} from "./checkpoint.js";
import type { ThreadTurnFact } from "./facts.js";
import type {
	ThreadActiveInputView,
	ThreadTurnDispatch,
	ThreadTurnNextStep,
	ThreadTurnSnapshot,
	ThreadTurnState,
	ThreadTurnTransition,
} from "./types.js";
import { ThreadTurnContractError } from "./types.js";

export function deriveThreadTurnSnapshot(
	checkpointInput: ThreadTurnCheckpoint,
	routeViewInput: ThreadToolRouteView,
	acceptedInputIds: readonly string[],
	activeInputView: ThreadActiveInputView,
): ThreadTurnSnapshot {
	const checkpoint = parseThreadTurnCheckpoint(checkpointInput);
	const routeView = parseThreadToolRouteView(routeViewInput);
	const modelRequestId = checkpoint.request?.modelRequestId;

	if (checkpoint.interruptEventId !== undefined) {
		if (checkpoint.executionRunId === undefined) {
			const acceptedInput = commitAcceptedInputDecision(
				checkpoint,
				acceptedInputIds,
			);
			if (acceptedInput !== undefined) {
				return acceptedInput;
			}
			return { state: { state: "idle" }, nextStep: { action: "await_input" } };
		}
		return {
			state: controlState(checkpoint),
			nextStep: optionalModelRequestAction("close_interrupted", modelRequestId),
		};
	}
	if (checkpoint.terminalCloseout !== undefined) {
		if (checkpoint.terminalCloseout.disposition === "retries_exhausted") {
			const acceptedInput = commitAcceptedInputDecision(
				checkpoint,
				acceptedInputIds,
			);
			if (acceptedInput !== undefined) {
				return acceptedInput;
			}
		}
		return { state: { state: "idle" }, nextStep: { action: "await_input" } };
	}

	const request = checkpoint.request;
	if (request === undefined) {
		const acceptedInput = commitAcceptedInputDecision(
			checkpoint,
			acceptedInputIds,
		);
		if (acceptedInput !== undefined) {
			return acceptedInput;
		}
		if (checkpoint.idleCloseout?.stopReason === "end_turn") {
			return { state: { state: "idle" }, nextStep: { action: "await_input" } };
		}
		if (
			checkpoint.pendingInputContextSequences.length > 0 ||
			activeInputView.hasPendingAttachments
		) {
			return readyToRequest();
		}
		if (checkpoint.executionRunId !== undefined) {
			return {
				state: { state: "ready_to_finish" },
				nextStep: { action: "finish_idle", stopReason: { type: "end_turn" } },
			};
		}
		return { state: { state: "idle" }, nextStep: { action: "await_input" } };
	}

	if (request.requestEnd === undefined) {
		return {
			state: { state: "request_open", modelRequestId: request.modelRequestId },
			nextStep: {
				action: "await_request_end",
				modelRequestId: request.modelRequestId,
			},
		};
	}

	const publicMembers = request.toolMembers.filter(
		(member) => member.memberKind === "public_tool_use",
	);
	const incompleteMembers = publicMembers.filter(
		(member) => member.terminalResult === undefined,
	);
	validateRouteOwnership(request.toolMembers, incompleteMembers, routeView);

	if (request.toolMembers.length > 0 && incompleteMembers.length > 0) {
		const resumeToolUseEventIds = incompleteMembers
			.filter((member) => {
				const disposition = routeDisposition(routeView, member.toolUseEventId);
				return (
					disposition === "resume_approval_settlement" ||
					disposition === "resume_sandbox_execution"
				);
			})
			.map((member) => member.toolUseEventId);
		if (resumeToolUseEventIds.length > 0) {
			return {
				state: {
					state: "waiting_for_tool_results",
					modelRequestId: request.modelRequestId,
				},
				nextStep: {
					action: "resume_tool_routes",
					modelRequestId: request.modelRequestId,
					toolUseEventIds: resumeToolUseEventIds,
				},
			};
		}

		const hasHotExecution = incompleteMembers.some(
			(member) =>
				routeDisposition(routeView, member.toolUseEventId) === "hot_execution",
		);
		if (hasHotExecution) {
			return {
				state: {
					state: "waiting_for_tool_results",
					modelRequestId: request.modelRequestId,
				},
				nextStep: {
					action: "await_tool_results",
					modelRequestId: request.modelRequestId,
					toolUseEventIds: incompleteMembers.map(
						(member) => member.toolUseEventId,
					),
				},
			};
		}

		const requiresActionEventIds = incompleteMembers
			.filter(
				(member) =>
					routeDisposition(routeView, member.toolUseEventId) ===
					"requires_user_action",
			)
			.map((member) => member.toolUseEventId);
		if (
			requiresActionEventIds.length > 0 &&
			checkpoint.idleCloseout?.stopReason !== "requires_action"
		) {
			return {
				state: {
					state: "waiting_for_tool_results",
					modelRequestId: request.modelRequestId,
				},
				nextStep: {
					action: "finish_idle",
					stopReason: {
						type: "requires_action",
						eventIds: requiresActionEventIds,
					},
				},
			};
		}

		return {
			state: {
				state: "waiting_for_tool_results",
				modelRequestId: request.modelRequestId,
			},
			nextStep: {
				action: "await_tool_results",
				modelRequestId: request.modelRequestId,
				toolUseEventIds: incompleteMembers.map(
					(member) => member.toolUseEventId,
				),
			},
		};
	}

	if (request.requestEnd.isError || request.requestEnd.reschedule !== undefined) {
		return {
			state: {
				state: "request_sealed",
				modelRequestId: request.modelRequestId,
			},
			nextStep: {
				action: "apply_request_retry_or_reschedule",
				modelRequestId: request.modelRequestId,
			},
		};
	}
	if (request.requestKind === "compaction_summary") {
		return {
			state: { state: "ready_to_request" },
			nextStep: {
				action: "continue_after_compaction",
				modelRequestId: request.modelRequestId,
			},
		};
	}

	if (
		request.toolMembers.length > 0 ||
		checkpoint.pendingInputContextSequences.length > 0 ||
		activeInputView.hasPendingAttachments
	) {
		const acceptedInput = commitAcceptedInputDecision(
			checkpoint,
			acceptedInputIds,
		);
		if (acceptedInput !== undefined) {
			return acceptedInput;
		}
		return readyToRequest();
	}

	const acceptedInput = commitAcceptedInputDecision(
		checkpoint,
		acceptedInputIds,
	);
	if (acceptedInput !== undefined) {
		return acceptedInput;
	}

	if (request.requestKind === "approval_reviewer") {
		return {
			state: {
				state: "request_sealed",
				modelRequestId: request.modelRequestId,
			},
			nextStep: {
				action: "complete_reviewer",
				modelRequestId: request.modelRequestId,
			},
		};
	}

	if (checkpoint.idleCloseout?.stopReason === "end_turn") {
		return { state: { state: "idle" }, nextStep: { action: "await_input" } };
	}
	return {
		state: { state: "ready_to_finish" },
		nextStep: { action: "finish_idle", stopReason: { type: "end_turn" } },
	};
}

export function initializeThreadTurnTransition(
	checkpointInput: ThreadTurnCheckpoint,
	routeView: ThreadToolRouteView,
	acceptedInputIds: readonly string[],
	activeInputView: ThreadActiveInputView,
): ThreadTurnTransition {
	const checkpoint = parseThreadTurnCheckpoint(checkpointInput);
	return {
		checkpoint,
		...deriveThreadTurnSnapshot(
			checkpoint,
			routeView,
			acceptedInputIds,
			activeInputView,
		),
	};
}

export function reduceThreadTurn(
	current: ThreadTurnTransition,
	fact: ThreadTurnFact,
	routeView: ThreadToolRouteView,
	acceptedInputIds: readonly string[],
	activeInputView: ThreadActiveInputView,
): ThreadTurnTransition {
	const eventId =
		fact.fact === "tool_result_committed" ? undefined : fact.eventId;
	if (eventId !== undefined) {
		assertDurableIdentity(eventId, "eventId");
	}
	switch (fact.fact) {
		case "run_opened": {
			if (current.checkpoint.executionRunId === fact.eventId) {
				return stableTransition(
					current.checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			const request = current.checkpoint.request;
			const preserveActiveRequest =
				current.checkpoint.interruptEventId === undefined &&
				request?.requestEnd !== undefined &&
				!request.requestEnd.isError &&
				(request.toolMembers.some(
					(member) =>
						member.memberKind === "public_tool_use" &&
						member.terminalResult === undefined,
				) ||
					current.nextStep.action === "prepare_next_request" ||
					current.nextStep.action === "continue_after_compaction" ||
					current.nextStep.action === "complete_reviewer");
			const checkpoint = parseThreadTurnCheckpoint({
				executionRunId: fact.eventId,
				pendingInputContextSequences:
					current.checkpoint.pendingInputContextSequences,
				...(preserveActiveRequest && request !== undefined ? { request } : {}),
			});
			return stableTransition(
				checkpoint,
				routeView,
				acceptedInputIds,
				activeInputView,
			);
		}
		case "inputs_committed": {
			if (fact.contextSequences.length === 0) {
				return stableTransition(
					current.checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			if (
				fact.contextSequences.every((sequence) =>
					current.checkpoint.pendingInputContextSequences.includes(sequence),
				)
			) {
				return stableTransition(
					current.checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			const checkpoint = applyCommittedInputs(
				current.checkpoint,
				fact.contextSequences,
			);
			return stableTransition(
				checkpoint,
				routeView,
				acceptedInputIds,
				activeInputView,
			);
		}
		case "request_started": {
			if (current.checkpoint.request?.requestStartEventId === fact.eventId) {
				return stableTransition(
					current.checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			const startAuthorized =
				current.state.state === "ready_to_request" ||
				current.nextStep.action === "apply_request_retry_or_reschedule";
			if (!startAuthorized) {
				throw new ThreadTurnContractError(
					`cannot start Request from ${current.state.state}`,
				);
			}
			assertDurableIdentity(fact.modelRequestId, "modelRequestId");
			if (
				!Number.isSafeInteger(fact.contextThroughMessageSequence) ||
				fact.contextThroughMessageSequence < 0
			) {
				throw new ThreadTurnContractError(
					"contextThroughMessageSequence must be a non-negative safe integer",
				);
			}
			const consumed = new Set(fact.consumedInputContextSequences);
			if (consumed.size !== fact.consumedInputContextSequences.length) {
				throw new ThreadTurnContractError(
					"consumedInputContextSequences must be unique",
				);
			}
			for (const sequence of consumed) {
				if (
					!current.checkpoint.pendingInputContextSequences.includes(sequence)
				) {
					throw new ThreadTurnContractError(
						"Request Start consumed an unknown input context sequence",
					);
				}
			}
			const checkpoint = parseThreadTurnCheckpoint({
				...(current.checkpoint.executionRunId !== undefined
					? { executionRunId: current.checkpoint.executionRunId }
					: {}),
				pendingInputContextSequences:
					current.checkpoint.pendingInputContextSequences.filter(
						(sequence) => !consumed.has(sequence),
					),
				request: {
					modelRequestId: fact.modelRequestId,
					requestStartEventId: fact.eventId,
					requestKind: fact.requestKind,
					contextThroughMessageSequence: fact.contextThroughMessageSequence,
					toolMembers: [],
				},
			});
			return transitionWithDispatch(
				checkpoint,
				routeView,
				acceptedInputIds,
				activeInputView,
				{
					dispatch: "start_provider_request",
					modelRequestId: fact.modelRequestId,
				},
			);
		}
		case "tool_use_committed": {
			const request = currentRequest(current.checkpoint, fact.modelRequestId);
			if (
				request.toolMembers.some(
					(member) =>
						member.memberKind === "public_tool_use" &&
						member.toolUseEventId === fact.eventId,
				)
			) {
				return stableTransition(
					current.checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			if (request.requestEnd !== undefined) {
				throw new ThreadTurnContractError(
					"cannot append Tool Use after Request End",
				);
			}
			assertDurableIdentity(fact.modelToolCallId, "modelToolCallId");
			assertDurableIdentity(fact.toolName, "toolName");
			if (
				request.toolMembers.some(
					(member) => member.modelToolCallId === fact.modelToolCallId,
				)
			) {
				throw new ThreadTurnContractError(
					"modelToolCallId must be unique within a request",
				);
			}
			const checkpoint = replaceRequest(current.checkpoint, {
				...request,
				toolMembers: [
					...request.toolMembers,
					{
						memberKind: "public_tool_use",
						modelToolCallId: fact.modelToolCallId,
						toolUseEventId: fact.eventId,
						toolName: fact.toolName,
					},
				],
			});
			if (
				checkpoint.interruptEventId !== undefined ||
				checkpoint.terminalCloseout !== undefined
			) {
				return stableTransition(checkpoint, routeView, [], activeInputView);
			}
			return transitionWithDispatch(
				checkpoint,
				routeView,
				acceptedInputIds,
				activeInputView,
				{ dispatch: "route_tool_use", toolUseEventId: fact.eventId },
			);
		}
		case "internal_tool_repair_committed": {
			const request = currentRequest(current.checkpoint, fact.modelRequestId);
			if (
				request.toolMembers.some(
					(member) =>
						member.memberKind === "internal_tool_repair" &&
						member.repairEventId === fact.eventId,
				)
			) {
				return stableTransition(
					current.checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			if (request.requestEnd !== undefined) {
				throw new ThreadTurnContractError(
					"cannot append internal Tool repair after Request End",
				);
			}
			if (
				request.toolMembers.some(
					(member) => member.modelToolCallId === fact.modelToolCallId,
				)
			) {
				throw new ThreadTurnContractError(
					"modelToolCallId must be unique within a request",
				);
			}
			const checkpoint = replaceRequest(current.checkpoint, {
				...request,
				toolMembers: [
					...request.toolMembers,
					{
						memberKind: "internal_tool_repair",
						modelToolCallId: fact.modelToolCallId,
						toolName: fact.toolName,
						repairEventId: fact.eventId,
						outcome: "error",
					},
				],
			});
			return stableTransition(
				checkpoint,
				routeView,
				acceptedInputIds,
				activeInputView,
			);
		}
		case "tool_result_committed": {
			const request = current.checkpoint.request;
			if (request === undefined) {
				throw new ThreadTurnContractError(
					"Tool Result does not name a request member",
				);
			}
			let matched = false;
			let replayed = false;
			const toolMembers = request.toolMembers.map((member) => {
				if (
					member.memberKind !== "public_tool_use" ||
					member.toolUseEventId !== fact.toolUseEventId
				) {
					return member;
				}
				matched = true;
				if (member.terminalResult !== undefined) {
					if (member.terminalResult.outcome === fact.outcome) {
						replayed = true;
						return member;
					}
					throw new ThreadTurnContractError(
						"Tool Use has a conflicting terminal Tool Result",
					);
				}
				return {
					...member,
					terminalResult: { outcome: fact.outcome },
				} as const;
			});
			if (!matched) {
				throw new ThreadTurnContractError(
					"Tool Result does not name a request member",
				);
			}
			if (replayed) {
				return stableTransition(
					current.checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			const activeCheckpoint =
				current.checkpoint.idleCloseout === undefined
					? current.checkpoint
					: parseThreadTurnCheckpoint({
							...current.checkpoint,
							idleCloseout: undefined,
						});
			return stableTransition(
				replaceRequest(activeCheckpoint, { ...request, toolMembers }),
				routeView,
				acceptedInputIds,
				activeInputView,
			);
		}
		case "request_ended": {
			const request = currentRequest(current.checkpoint, fact.modelRequestId);
			if (request.requestEnd !== undefined) {
				if (request.requestEnd.eventId === fact.eventId) {
					return stableTransition(
						current.checkpoint,
						routeView,
						acceptedInputIds,
						activeInputView,
					);
				}
				throw new ThreadTurnContractError("Request already has a durable End");
			}
			const checkpoint = replaceRequest(current.checkpoint, {
				...request,
				requestEnd: {
					eventId: fact.eventId,
					isError: fact.isError,
					...(fact.errorKind !== undefined
						? { errorKind: fact.errorKind }
						: {}),
					providerContextRetention: fact.providerContextRetention,
					...(fact.reschedule !== undefined
						? { reschedule: fact.reschedule }
						: {}),
				},
			});
			return stableTransition(
				checkpoint,
				routeView,
				acceptedInputIds,
				activeInputView,
			);
		}
		case "finish_idle_committed": {
			if (
				current.checkpoint.idleCloseout?.eventId === fact.eventId ||
				current.checkpoint.terminalCloseout?.closeoutEventId === fact.eventId
			) {
				return stableTransition(
					current.checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			if (fact.stopReason.type === "requires_action") {
				if (current.state.state !== "waiting_for_tool_results") {
					throw new ThreadTurnContractError(
						`cannot finish requires_action from ${current.state.state}`,
					);
				}
				if (
					current.nextStep.action !== "finish_idle" ||
					current.nextStep.stopReason.type !== "requires_action"
				) {
					throw new ThreadTurnContractError(
						"requires_action ACK does not match the current FinishIdle action",
					);
				}
				if (
					!sameIdentitySet(
						fact.stopReason.eventIds,
						current.nextStep.stopReason.eventIds,
					)
				) {
					throw new ThreadTurnContractError(
						"requires_action ACK event IDs do not match the declared closeout",
					);
				}
				const checkpoint = parseThreadTurnCheckpoint({
					...checkpointWithoutExecutionRun(current.checkpoint),
					idleCloseout: {
						eventId: fact.eventId,
						stopReason: "requires_action",
					},
				});
				const request = checkpoint.request;
				if (request === undefined) {
					throw new ThreadTurnContractError(
						"requires_action closeout has no request",
					);
				}
				return stableTransition(
					checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			if (fact.stopReason.type === "retries_exhausted") {
				if (
					current.state.state !== "request_sealed" &&
					fact.stopReason.failedRun !== true
				) {
					throw new ThreadTurnContractError(
						`cannot finish retries_exhausted from ${current.state.state}`,
					);
				}
				assertDurableIdentity(fact.stopReason.failureEventId, "failureEventId");
				const checkpoint = parseThreadTurnCheckpoint({
					pendingInputContextSequences:
						current.checkpoint.pendingInputContextSequences,
					terminalCloseout: {
						failureEventId: fact.stopReason.failureEventId,
						closeoutEventId: fact.eventId,
						disposition: "retries_exhausted",
					},
					idleCloseout: {
						eventId: fact.eventId,
						stopReason: "retries_exhausted",
					},
				});
				return stableTransition(
					checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			const closesCurrentAction =
				(current.nextStep.action === "finish_idle" &&
					current.nextStep.stopReason.type === "end_turn") ||
				current.nextStep.action === "complete_reviewer" ||
				current.nextStep.action === "close_interrupted" ||
				current.nextStep.action === "apply_request_retry_or_reschedule" ||
				(current.state.state === "idle" &&
					current.nextStep.action === "await_input") ||
				fact.stopReason.failedRun === true;
			if (!closesCurrentAction) {
				throw new ThreadTurnContractError(
					`cannot finish end_turn from ${current.state.state}`,
				);
			}
			const checkpoint = parseThreadTurnCheckpoint({
				pendingInputContextSequences:
					current.checkpoint.pendingInputContextSequences,
				idleCloseout: {
					eventId: fact.eventId,
					stopReason: fact.stopReason.type,
				},
			});
			return {
				checkpoint,
				state: { state: "idle" },
				nextStep: { action: "await_input" },
			};
		}
		case "interrupt_committed": {
			if (current.checkpoint.interruptEventId === fact.eventId) {
				return stableTransition(
					current.checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			const checkpoint = parseThreadTurnCheckpoint({
				...current.checkpoint,
				interruptEventId: fact.eventId,
			});
			return stableTransition(
				checkpoint,
				routeView,
				acceptedInputIds,
				activeInputView,
			);
		}
		case "terminal_closeout_committed": {
			if (
				current.checkpoint.terminalCloseout?.closeoutEventId === fact.eventId
			) {
				return stableTransition(
					current.checkpoint,
					routeView,
					acceptedInputIds,
					activeInputView,
				);
			}
			const checkpoint = parseThreadTurnCheckpoint({
				pendingInputContextSequences:
					current.checkpoint.pendingInputContextSequences,
				terminalCloseout: {
					failureEventId: fact.failureEventId,
					closeoutEventId: fact.eventId,
					disposition: fact.disposition,
				},
			});
			return stableTransition(checkpoint, routeView, [], activeInputView);
		}
	}
}

function stableTransition(
	checkpoint: ThreadTurnCheckpoint,
	routeView: ThreadToolRouteView,
	acceptedInputIds: readonly string[],
	activeInputView: ThreadActiveInputView,
): ThreadTurnTransition {
	return {
		checkpoint,
		...deriveThreadTurnSnapshot(
			checkpoint,
			routeView,
			acceptedInputIds,
			activeInputView,
		),
	};
}

function transitionWithDispatch(
	checkpoint: ThreadTurnCheckpoint,
	routeView: ThreadToolRouteView,
	acceptedInputIds: readonly string[],
	activeInputView: ThreadActiveInputView,
	dispatch: ThreadTurnDispatch,
): ThreadTurnTransition {
	return {
		...stableTransition(
			checkpoint,
			routeView,
			acceptedInputIds,
			activeInputView,
		),
		dispatch,
	};
}

function commitAcceptedInputDecision(
	checkpoint: ThreadTurnCheckpoint,
	acceptedInputIds: readonly string[],
): ThreadTurnSnapshot | undefined {
	const runtimeInputId = acceptedInputIds[0];
	if (runtimeInputId === undefined) {
		return undefined;
	}
	assertDurableIdentity(runtimeInputId, "runtimeInputId");
	return {
		state: controlState(checkpoint),
		nextStep: { action: "commit_accepted_input", runtimeInputId },
	};
}

function applyCommittedInputs(
	checkpoint: ThreadTurnCheckpoint,
	contextSequences: readonly number[],
): ThreadTurnCheckpoint {
	const nextSequences = [...checkpoint.pendingInputContextSequences];
	const pendingBeforeCommit = new Set(nextSequences);
	const addedByCommit = new Set<number>();
	for (const sequence of contextSequences) {
		if (!Number.isSafeInteger(sequence) || sequence <= 0) {
			throw new ThreadTurnContractError(
				"context sequence must be a positive safe integer",
			);
		}
		// Cold reconstruction may already contain a context entry whose commit ACK
		// was lost. Replaying that durable commit applies no second checkpoint entry.
		if (pendingBeforeCommit.has(sequence)) {
			continue;
		}
		if (addedByCommit.has(sequence)) {
			throw new ThreadTurnContractError(
				"committed input context sequence is already pending",
			);
		}
		addedByCommit.add(sequence);
		nextSequences.push(sequence);
	}
	return parseThreadTurnCheckpoint({
		...checkpoint,
		pendingInputContextSequences: nextSequences,
	});
}

function replaceRequest(
	checkpoint: ThreadTurnCheckpoint,
	request: NonNullable<ThreadTurnCheckpoint["request"]>,
): ThreadTurnCheckpoint {
	return parseThreadTurnCheckpoint({ ...checkpoint, request });
}

function checkpointWithoutExecutionRun(
	checkpoint: ThreadTurnCheckpoint,
): ThreadTurnCheckpoint {
	return parseThreadTurnCheckpoint({
		pendingInputContextSequences: checkpoint.pendingInputContextSequences,
		...(checkpoint.request !== undefined
			? { request: checkpoint.request }
			: {}),
		...(checkpoint.interruptEventId !== undefined
			? { interruptEventId: checkpoint.interruptEventId }
			: {}),
		...(checkpoint.terminalCloseout !== undefined
			? { terminalCloseout: checkpoint.terminalCloseout }
			: {}),
		...(checkpoint.idleCloseout !== undefined
			? { idleCloseout: checkpoint.idleCloseout }
			: {}),
	});
}

function currentRequest(
	checkpoint: ThreadTurnCheckpoint,
	modelRequestId: string,
): NonNullable<ThreadTurnCheckpoint["request"]> {
	const request = checkpoint.request;
	if (request === undefined || request.modelRequestId !== modelRequestId) {
		throw new ThreadTurnContractError(
			"durable fact does not belong to the current model request",
		);
	}
	return request;
}

function validateRouteOwnership(
	allMembers: NonNullable<ThreadTurnCheckpoint["request"]>["toolMembers"],
	incompleteMembers: readonly Extract<
		NonNullable<ThreadTurnCheckpoint["request"]>["toolMembers"][number],
		{ readonly memberKind: "public_tool_use" }
	>[],
	routeView: ThreadToolRouteView,
): void {
	const publicMemberIds = new Set(
		allMembers
			.filter((member) => member.memberKind === "public_tool_use")
			.map((member) => member.toolUseEventId),
	);
	for (const route of routeView.routes) {
		if (!publicMemberIds.has(route.toolUseEventId)) {
			throw new ThreadTurnContractError(
				"tool route does not name a request member",
			);
		}
	}
	for (const member of incompleteMembers) {
		const count = routeView.routes.filter(
			(route) => route.toolUseEventId === member.toolUseEventId,
		).length;
		if (count !== 1) {
			throw new ThreadTurnContractError(
				"sealed non-terminal Tool Use has no route",
			);
		}
	}
}

function routeDisposition(
	routeView: ThreadToolRouteView,
	toolUseEventId: string,
): ThreadToolRouteView["routes"][number]["disposition"] {
	const route = routeView.routes.find(
		(candidate) => candidate.toolUseEventId === toolUseEventId,
	);
	if (route === undefined) {
		throw new ThreadTurnContractError(
			"sealed non-terminal Tool Use has no route",
		);
	}
	return route.disposition;
}

function readyToRequest(): ThreadTurnSnapshot {
	return {
		state: { state: "ready_to_request" },
		nextStep: { action: "prepare_next_request" },
	};
}

function controlState(checkpoint: ThreadTurnCheckpoint): ThreadTurnState {
	const request = checkpoint.request;
	if (request?.requestEnd !== undefined) {
		return { state: "request_sealed", modelRequestId: request.modelRequestId };
	}
	if (request !== undefined) {
		return { state: "request_open", modelRequestId: request.modelRequestId };
	}
	return { state: "idle" };
}

function optionalModelRequestAction(
	action: "close_interrupted" | "close_failed",
	modelRequestId: string | undefined,
): ThreadTurnNextStep {
	return modelRequestId === undefined ? { action } : { action, modelRequestId };
}

function assertDurableIdentity(value: string, label: string): void {
	if (value.length === 0) {
		throw new ThreadTurnContractError(`${label} must be non-empty`);
	}
}

function sameIdentitySet(
	left: readonly string[],
	right: readonly string[],
): boolean {
	if (left.length !== right.length || new Set(left).size !== left.length) {
		return false;
	}
	const rightSet = new Set(right);
	return left.every((identity) => rightSet.has(identity));
}
