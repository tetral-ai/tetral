import {
  parseThreadToolRouteView,
  parseThreadTurnCheckpoint,
} from "./thread-turn-checkpoint.js";
import type {
  ThreadToolRouteView,
  ThreadTurnCheckpoint,
} from "./thread-turn-checkpoint.js";

export type ThreadTurnState =
  | { readonly state: "idle" }
  | { readonly state: "ready_to_request" }
  | { readonly state: "request_open"; readonly modelRequestId: string }
  | { readonly state: "request_sealed"; readonly modelRequestId: string }
  | { readonly state: "waiting_for_tool_results"; readonly modelRequestId: string }
  | { readonly state: "ready_to_finish" };

export type ThreadTurnAction =
  | { readonly action: "none" }
  | { readonly action: "await_input" }
  | { readonly action: "prepare_next_request" }
  | { readonly action: "start_provider_request"; readonly modelRequestId: string }
  | { readonly action: "await_request_end"; readonly modelRequestId: string }
  | { readonly action: "dispatch_tool_use"; readonly toolUseEventId: string }
  | { readonly action: "reconcile_request_seal"; readonly modelRequestId: string }
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
        | { readonly type: "requires_action"; readonly eventIds: readonly string[] }
        | { readonly type: "retries_exhausted" };
    }
  | { readonly action: "continue_after_compaction"; readonly modelRequestId: string }
  | { readonly action: "complete_reviewer"; readonly modelRequestId: string }
  | { readonly action: "apply_request_retry_or_reschedule"; readonly modelRequestId: string }
  | { readonly action: "commit_accepted_input"; readonly runtimeInputId: string }
  | { readonly action: "close_interrupted"; readonly modelRequestId?: string }
  | { readonly action: "close_failed"; readonly modelRequestId?: string };

export interface ThreadTurnDecision {
  readonly state: ThreadTurnState;
  readonly action: ThreadTurnAction;
}

export interface ThreadTurnReduction extends ThreadTurnDecision {
  readonly checkpoint: ThreadTurnCheckpoint;
  readonly appliedEventIds: readonly string[];
}

/** Identifies an impossible durable fact or Thread-turn transition. */
export class ThreadTurnContractError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ThreadTurnContractError";
  }
}

export type ThreadTurnFact =
  | {
      readonly fact: "run_opened";
      readonly eventId: string;
    }
  | {
      readonly fact: "inputs_committed";
      readonly eventId: string;
      readonly messageIds: readonly string[];
    }
  | {
      readonly fact: "request_started";
      readonly eventId: string;
      readonly modelRequestId: string;
      readonly requestKind: NonNullable<ThreadTurnCheckpoint["request"]>["requestKind"];
      readonly contextThroughMessageSequence: number;
      readonly consumedInputMessageIds: readonly string[];
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
      readonly eventId: string;
      readonly toolUseEventId: string;
      readonly outcome: "success" | "error" | "cancelled" | "unknown";
    }
  | {
      readonly fact: "request_ended";
      readonly eventId: string;
      readonly modelRequestId: string;
      readonly isError: boolean;
      readonly errorKind?: string;
      readonly rescheduled: boolean;
    }
  | {
      readonly fact: "finish_idle_committed";
      readonly eventId: string;
      readonly stopReason:
        | { readonly type: "end_turn"; readonly failedRun?: true }
        | { readonly type: "requires_action"; readonly eventIds: readonly string[] }
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

export function deriveThreadTurnDecision(
  checkpointInput: ThreadTurnCheckpoint,
  routeViewInput: ThreadToolRouteView,
  acceptedInputIds: readonly string[] = [],
): ThreadTurnDecision {
  const checkpoint = parseThreadTurnCheckpoint(checkpointInput);
  const routeView = parseThreadToolRouteView(routeViewInput);
  const modelRequestId = checkpoint.request?.modelRequestId;

  if (checkpoint.interruptEventId !== undefined) {
    if (checkpoint.executionRunId === undefined) {
      const acceptedInput = commitAcceptedInputDecision(checkpoint, acceptedInputIds);
      if (acceptedInput !== undefined) {
        return acceptedInput;
      }
      return { state: { state: "idle" }, action: { action: "await_input" } };
    }
    return {
      state: controlState(checkpoint),
      action: optionalModelRequestAction("close_interrupted", modelRequestId),
    };
  }
  if (checkpoint.terminalCloseout !== undefined) {
    return { state: { state: "idle" }, action: { action: "await_input" } };
  }

  const request = checkpoint.request;
  if (request === undefined) {
    const acceptedInput = commitAcceptedInputDecision(checkpoint, acceptedInputIds);
    if (acceptedInput !== undefined) {
      return acceptedInput;
    }
    if (checkpoint.idleCloseout?.stopReason === "end_turn") {
      return { state: { state: "idle" }, action: { action: "await_input" } };
    }
    if (checkpoint.pendingInputMessageIds.length > 0) {
      return readyToRequest();
    }
    if (checkpoint.executionRunId !== undefined) {
      return {
        state: { state: "ready_to_finish" },
        action: { action: "finish_idle", stopReason: { type: "end_turn" } },
      };
    }
    return { state: { state: "idle" }, action: { action: "await_input" } };
  }

  if (request.requestEnd === undefined) {
    return {
      state: { state: "request_open", modelRequestId: request.modelRequestId },
      action: { action: "await_request_end", modelRequestId: request.modelRequestId },
    };
  }

  if (request.requestEnd.isError || request.requestEnd.rescheduled) {
    return {
      state: { state: "request_sealed", modelRequestId: request.modelRequestId },
      action: { action: "apply_request_retry_or_reschedule", modelRequestId: request.modelRequestId },
    };
  }
  if (request.requestKind === "compaction_summary") {
    return {
      state: { state: "ready_to_request" },
      action: { action: "continue_after_compaction", modelRequestId: request.modelRequestId },
    };
  }
  const publicMembers = request.toolMembers.filter((member) => member.memberKind === "public_tool_use");
  const incompleteMembers = publicMembers.filter((member) => member.terminalResult === undefined);
  validateRouteOwnership(request.toolMembers, incompleteMembers, routeView);

  if (request.toolMembers.length > 0 && incompleteMembers.length > 0) {
    const resumeToolUseEventIds = incompleteMembers
      .filter((member) => {
        const disposition = routeDisposition(routeView, member.toolUseEventId);
        return disposition === "resume_approval_settlement" || disposition === "resume_sandbox_execution";
      })
      .map((member) => member.toolUseEventId);
    if (resumeToolUseEventIds.length > 0) {
      return {
        state: { state: "waiting_for_tool_results", modelRequestId: request.modelRequestId },
        action: {
          action: "resume_tool_routes",
          modelRequestId: request.modelRequestId,
          toolUseEventIds: resumeToolUseEventIds,
        },
      };
    }

    const hasHotExecution = incompleteMembers.some(
      (member) => routeDisposition(routeView, member.toolUseEventId) === "hot_execution",
    );
    if (hasHotExecution) {
      return {
        state: { state: "waiting_for_tool_results", modelRequestId: request.modelRequestId },
        action: {
          action: "await_tool_results",
          modelRequestId: request.modelRequestId,
          toolUseEventIds: incompleteMembers.map((member) => member.toolUseEventId),
        },
      };
    }

    const requiresActionEventIds = incompleteMembers
      .filter((member) => routeDisposition(routeView, member.toolUseEventId) === "requires_user_action")
      .map((member) => member.toolUseEventId);
    if (requiresActionEventIds.length > 0 && checkpoint.idleCloseout?.stopReason !== "requires_action") {
      return {
        state: { state: "waiting_for_tool_results", modelRequestId: request.modelRequestId },
        action: {
          action: "finish_idle",
          stopReason: { type: "requires_action", eventIds: requiresActionEventIds },
        },
      };
    }

    return {
      state: { state: "waiting_for_tool_results", modelRequestId: request.modelRequestId },
      action: {
        action: "await_tool_results",
        modelRequestId: request.modelRequestId,
        toolUseEventIds: incompleteMembers.map((member) => member.toolUseEventId),
      },
    };
  }

  if (request.toolMembers.length > 0 || checkpoint.pendingInputMessageIds.length > 0) {
    const acceptedInput = commitAcceptedInputDecision(checkpoint, acceptedInputIds);
    if (acceptedInput !== undefined) {
      return acceptedInput;
    }
    return readyToRequest();
  }

  const acceptedInput = commitAcceptedInputDecision(checkpoint, acceptedInputIds);
  if (acceptedInput !== undefined) {
    return acceptedInput;
  }

  if (request.requestKind === "approval_reviewer") {
    return {
      state: { state: "request_sealed", modelRequestId: request.modelRequestId },
      action: { action: "complete_reviewer", modelRequestId: request.modelRequestId },
    };
  }

  if (checkpoint.idleCloseout?.stopReason === "end_turn") {
    return { state: { state: "idle" }, action: { action: "await_input" } };
  }
  return {
    state: { state: "ready_to_finish" },
    action: { action: "finish_idle", stopReason: { type: "end_turn" } },
  };
}

export function initializeThreadTurnReduction(
  checkpointInput: ThreadTurnCheckpoint,
  routeView: ThreadToolRouteView,
  acceptedInputIds: readonly string[] = [],
): ThreadTurnReduction {
  const checkpoint = parseThreadTurnCheckpoint(checkpointInput);
  return {
    checkpoint,
    appliedEventIds: [],
    ...deriveThreadTurnDecision(checkpoint, routeView, acceptedInputIds),
  };
}

export function reduceThreadTurn(
  current: ThreadTurnReduction,
  fact: ThreadTurnFact,
  routeView: ThreadToolRouteView,
  acceptedInputIds: readonly string[] = [],
): ThreadTurnReduction {
  assertDurableIdentity(fact.eventId, "eventId");
  if (current.appliedEventIds.includes(fact.eventId)) {
    if (fact.fact === "run_opened") {
      return current;
    }
    return { ...current, action: { action: "none" } };
  }

  const appliedEventIds = [...current.appliedEventIds, fact.eventId];
  switch (fact.fact) {
    case "run_opened": {
      if (current.checkpoint.executionRunId === fact.eventId) {
        return stableReduction(current.checkpoint, appliedEventIds, routeView, acceptedInputIds);
      }
      const request = current.checkpoint.request;
      const preserveActiveRequest = current.checkpoint.interruptEventId === undefined &&
        request?.requestEnd !== undefined &&
        !request.requestEnd.isError &&
        (
          request.toolMembers.some((member) =>
            member.memberKind === "public_tool_use" && member.terminalResult === undefined
          ) ||
          current.action.action === "prepare_next_request" ||
          current.action.action === "continue_after_compaction" ||
          current.action.action === "complete_reviewer"
        );
      const checkpoint = parseThreadTurnCheckpoint({
        executionRunId: fact.eventId,
        pendingInputMessageIds: current.checkpoint.pendingInputMessageIds,
        ...(preserveActiveRequest && request !== undefined
          ? { request }
          : {}),
      });
      return stableReduction(checkpoint, appliedEventIds, routeView, acceptedInputIds);
    }
    case "inputs_committed": {
      const checkpoint = applyCommittedInputs(current.checkpoint, fact.messageIds);
      return stableReduction(checkpoint, appliedEventIds, routeView, acceptedInputIds);
    }
    case "request_started": {
      const startAuthorized = current.state.state === "ready_to_request" ||
        current.action.action === "apply_request_retry_or_reschedule";
      if (!startAuthorized) {
        throw new ThreadTurnContractError(`cannot start Request from ${current.state.state}`);
      }
      assertDurableIdentity(fact.modelRequestId, "modelRequestId");
      if (!Number.isSafeInteger(fact.contextThroughMessageSequence) || fact.contextThroughMessageSequence < 0) {
        throw new ThreadTurnContractError("contextThroughMessageSequence must be a non-negative safe integer");
      }
      const consumed = new Set(fact.consumedInputMessageIds);
      if (consumed.size !== fact.consumedInputMessageIds.length) {
        throw new ThreadTurnContractError("consumedInputMessageIds must be unique");
      }
      for (const messageId of consumed) {
        if (!current.checkpoint.pendingInputMessageIds.includes(messageId)) {
          throw new ThreadTurnContractError("Request Start consumed an unknown input message");
        }
      }
      const checkpoint = parseThreadTurnCheckpoint({
        ...(current.checkpoint.executionRunId !== undefined
          ? { executionRunId: current.checkpoint.executionRunId }
          : {}),
        pendingInputMessageIds: current.checkpoint.pendingInputMessageIds.filter((id) => !consumed.has(id)),
        request: {
          modelRequestId: fact.modelRequestId,
          requestStartEventId: fact.eventId,
          requestKind: fact.requestKind,
          contextThroughMessageSequence: fact.contextThroughMessageSequence,
          toolMembers: [],
        },
      });
      return {
        checkpoint,
        appliedEventIds,
        state: { state: "request_open", modelRequestId: fact.modelRequestId },
        action: { action: "start_provider_request", modelRequestId: fact.modelRequestId },
      };
    }
    case "tool_use_committed": {
      const request = currentRequest(current.checkpoint, fact.modelRequestId);
      if (request.requestEnd !== undefined) {
        throw new ThreadTurnContractError("cannot append Tool Use after Request End");
      }
      assertDurableIdentity(fact.modelToolCallId, "modelToolCallId");
      assertDurableIdentity(fact.toolName, "toolName");
      if (request.toolMembers.some((member) => member.modelToolCallId === fact.modelToolCallId)) {
        throw new ThreadTurnContractError("modelToolCallId must be unique within a request");
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
      if (checkpoint.interruptEventId !== undefined || checkpoint.terminalCloseout !== undefined) {
        return stableReduction(checkpoint, appliedEventIds, routeView);
      }
      return {
        checkpoint,
        appliedEventIds,
        state: { state: "request_open", modelRequestId: request.modelRequestId },
        action: { action: "dispatch_tool_use", toolUseEventId: fact.eventId },
      };
    }
    case "internal_tool_repair_committed": {
      const request = currentRequest(current.checkpoint, fact.modelRequestId);
      if (request.requestEnd !== undefined) {
        throw new ThreadTurnContractError("cannot append internal Tool repair after Request End");
      }
      if (request.toolMembers.some((member) => member.modelToolCallId === fact.modelToolCallId)) {
        throw new ThreadTurnContractError("modelToolCallId must be unique within a request");
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
      return stableReduction(checkpoint, appliedEventIds, routeView, acceptedInputIds);
    }
    case "tool_result_committed": {
      const request = current.checkpoint.request;
      if (request === undefined) {
        throw new ThreadTurnContractError("Tool Result does not name a request member");
      }
      let matched = false;
      const toolMembers = request.toolMembers.map((member) => {
        if (member.memberKind !== "public_tool_use" || member.toolUseEventId !== fact.toolUseEventId) {
          return member;
        }
        matched = true;
        if (member.terminalResult !== undefined) {
          throw new ThreadTurnContractError("Tool Use already has a terminal Tool Result");
        }
        return {
          ...member,
          terminalResult: { resultEventId: fact.eventId, outcome: fact.outcome },
        } as const;
      });
      if (!matched) {
        throw new ThreadTurnContractError("Tool Result does not name a request member");
      }
      const activeCheckpoint = current.checkpoint.idleCloseout === undefined
        ? current.checkpoint
        : parseThreadTurnCheckpoint({
            ...current.checkpoint,
            idleCloseout: undefined,
          });
      return stableReduction(
        replaceRequest(activeCheckpoint, { ...request, toolMembers }),
        appliedEventIds,
        routeView,
        acceptedInputIds,
      );
    }
    case "request_ended": {
      const request = currentRequest(current.checkpoint, fact.modelRequestId);
      if (request.requestEnd !== undefined) {
        throw new ThreadTurnContractError("Request already has a durable End");
      }
      const checkpoint = replaceRequest(current.checkpoint, {
        ...request,
        requestEnd: {
          eventId: fact.eventId,
          isError: fact.isError,
          ...(fact.errorKind !== undefined ? { errorKind: fact.errorKind } : {}),
          rescheduled: fact.rescheduled,
        },
      });
      return {
        checkpoint,
        appliedEventIds,
        state: { state: "request_sealed", modelRequestId: request.modelRequestId },
        action: { action: "reconcile_request_seal", modelRequestId: request.modelRequestId },
      };
    }
    case "finish_idle_committed": {
      if (fact.stopReason.type === "requires_action") {
        if (current.state.state !== "waiting_for_tool_results") {
          throw new ThreadTurnContractError(`cannot finish requires_action from ${current.state.state}`);
        }
        if (current.action.action !== "finish_idle" || current.action.stopReason.type !== "requires_action") {
          throw new ThreadTurnContractError("requires_action ACK does not match the current FinishIdle action");
        }
        if (!sameIdentitySet(fact.stopReason.eventIds, current.action.stopReason.eventIds)) {
          throw new ThreadTurnContractError("requires_action ACK event IDs do not match the declared closeout");
        }
        const checkpoint = parseThreadTurnCheckpoint({
          ...checkpointWithoutExecutionRun(current.checkpoint),
          idleCloseout: { eventId: fact.eventId, stopReason: "requires_action" },
        });
        const request = checkpoint.request;
        if (request === undefined) {
          throw new ThreadTurnContractError("requires_action closeout has no request");
        }
        return {
          checkpoint,
          appliedEventIds,
          state: { state: "waiting_for_tool_results", modelRequestId: request.modelRequestId },
          action: {
            action: "await_tool_results",
            modelRequestId: request.modelRequestId,
            toolUseEventIds: request.toolMembers.flatMap((member) => {
              if (member.memberKind !== "public_tool_use" || member.terminalResult !== undefined) {
                return [];
              }
              return [member.toolUseEventId];
            }),
          },
        };
      }
      if (fact.stopReason.type === "retries_exhausted") {
        if (current.state.state !== "request_sealed" && fact.stopReason.failedRun !== true) {
          throw new ThreadTurnContractError(`cannot finish retries_exhausted from ${current.state.state}`);
        }
        assertDurableIdentity(fact.stopReason.failureEventId, "failureEventId");
        const checkpoint = parseThreadTurnCheckpoint({
          pendingInputMessageIds: current.checkpoint.pendingInputMessageIds,
          terminalCloseout: {
            failureEventId: fact.stopReason.failureEventId,
            closeoutEventId: fact.eventId,
            disposition: "retries_exhausted",
          },
          idleCloseout: { eventId: fact.eventId, stopReason: "retries_exhausted" },
        });
        return stableReduction(checkpoint, appliedEventIds, routeView, acceptedInputIds);
      }
      const closesCurrentAction = (
        current.action.action === "finish_idle" && current.action.stopReason.type === "end_turn"
      ) || current.action.action === "complete_reviewer" ||
        current.action.action === "close_interrupted" ||
        current.action.action === "apply_request_retry_or_reschedule" ||
        (current.state.state === "idle" && current.action.action === "await_input") ||
        fact.stopReason.failedRun === true;
      if (!closesCurrentAction) {
        throw new ThreadTurnContractError(`cannot finish end_turn from ${current.state.state}`);
      }
      const checkpoint = parseThreadTurnCheckpoint({
        pendingInputMessageIds: current.checkpoint.pendingInputMessageIds,
        idleCloseout: { eventId: fact.eventId, stopReason: fact.stopReason.type },
      });
      return {
        checkpoint,
        appliedEventIds,
        state: { state: "idle" },
        action: { action: "await_input" },
      };
    }
    case "interrupt_committed": {
      const checkpoint = parseThreadTurnCheckpoint({
        ...current.checkpoint,
        interruptEventId: fact.eventId,
      });
      return stableReduction(checkpoint, appliedEventIds, routeView, acceptedInputIds);
    }
    case "terminal_closeout_committed": {
      const checkpoint = parseThreadTurnCheckpoint({
        pendingInputMessageIds: current.checkpoint.pendingInputMessageIds,
        terminalCloseout: {
          failureEventId: fact.failureEventId,
          closeoutEventId: fact.eventId,
          disposition: fact.disposition,
        },
      });
      return stableReduction(checkpoint, appliedEventIds, routeView);
    }
  }
}

export function reconcileThreadTurnSeal(
  current: ThreadTurnReduction,
  routeView: ThreadToolRouteView,
  acceptedInputIds: readonly string[] = [],
): ThreadTurnReduction {
  if (current.state.state !== "request_sealed" || current.action.action !== "reconcile_request_seal") {
    throw new ThreadTurnContractError("only a newly sealed request can be reconciled");
  }
  return stableReduction(current.checkpoint, current.appliedEventIds, routeView, acceptedInputIds);
}

/** Consumes a one-shot hot dispatch edge and exposes its durable stable action. */
export function consumeThreadTurnEdge(
  current: ThreadTurnReduction,
  routeView: ThreadToolRouteView,
  acceptedInputIds: readonly string[] = [],
): ThreadTurnReduction {
  if (current.action.action !== "start_provider_request" && current.action.action !== "dispatch_tool_use") {
    throw new ThreadTurnContractError("only a one-shot Thread-turn edge can be consumed");
  }
  return stableReduction(current.checkpoint, current.appliedEventIds, routeView, acceptedInputIds);
}

function stableReduction(
  checkpoint: ThreadTurnCheckpoint,
  appliedEventIds: readonly string[],
  routeView: ThreadToolRouteView,
  acceptedInputIds: readonly string[] = [],
): ThreadTurnReduction {
  return {
    checkpoint,
    appliedEventIds,
    ...deriveThreadTurnDecision(checkpoint, routeView, acceptedInputIds),
  };
}

function commitAcceptedInputDecision(
  checkpoint: ThreadTurnCheckpoint,
  acceptedInputIds: readonly string[],
): ThreadTurnDecision | undefined {
  const runtimeInputId = acceptedInputIds[0];
  if (runtimeInputId === undefined) {
    return undefined;
  }
  assertDurableIdentity(runtimeInputId, "runtimeInputId");
  return {
    state: controlState(checkpoint),
    action: { action: "commit_accepted_input", runtimeInputId },
  };
}

function applyCommittedInputs(
  checkpoint: ThreadTurnCheckpoint,
  messageIds: readonly string[],
): ThreadTurnCheckpoint {
  const nextIds = [...checkpoint.pendingInputMessageIds];
  const pendingBeforeReceipt = new Set(nextIds);
  const addedByReceipt = new Set<string>();
  for (const messageId of messageIds) {
    assertDurableIdentity(messageId, "messageId");
    // Cold reconstruction may already contain a Message whose commit ACK was
    // lost. Replaying that durable receipt applies no second checkpoint entry.
    if (pendingBeforeReceipt.has(messageId)) {
      continue;
    }
    if (addedByReceipt.has(messageId)) {
      throw new ThreadTurnContractError("committed input message is already pending");
    }
    addedByReceipt.add(messageId);
    nextIds.push(messageId);
  }
  return parseThreadTurnCheckpoint({ ...checkpoint, pendingInputMessageIds: nextIds });
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
    pendingInputMessageIds: checkpoint.pendingInputMessageIds,
    ...(checkpoint.request !== undefined ? { request: checkpoint.request } : {}),
    ...(checkpoint.interruptEventId !== undefined ? { interruptEventId: checkpoint.interruptEventId } : {}),
    ...(checkpoint.terminalCloseout !== undefined ? { terminalCloseout: checkpoint.terminalCloseout } : {}),
    ...(checkpoint.idleCloseout !== undefined ? { idleCloseout: checkpoint.idleCloseout } : {}),
  });
}

function currentRequest(
  checkpoint: ThreadTurnCheckpoint,
  modelRequestId: string,
): NonNullable<ThreadTurnCheckpoint["request"]> {
  const request = checkpoint.request;
  if (request === undefined || request.modelRequestId !== modelRequestId) {
    throw new ThreadTurnContractError("durable fact does not belong to the current model request");
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
      throw new ThreadTurnContractError("tool route does not name a request member");
    }
  }
  for (const member of incompleteMembers) {
    const count = routeView.routes.filter((route) => route.toolUseEventId === member.toolUseEventId).length;
    if (count !== 1) {
      throw new ThreadTurnContractError("sealed non-terminal Tool Use has no route");
    }
  }
}

function routeDisposition(
  routeView: ThreadToolRouteView,
  toolUseEventId: string,
): ThreadToolRouteView["routes"][number]["disposition"] {
  const route = routeView.routes.find((candidate) => candidate.toolUseEventId === toolUseEventId);
  if (route === undefined) {
    throw new ThreadTurnContractError("sealed non-terminal Tool Use has no route");
  }
  return route.disposition;
}

function readyToRequest(): ThreadTurnDecision {
  return {
    state: { state: "ready_to_request" },
    action: { action: "prepare_next_request" },
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
): ThreadTurnAction {
  return modelRequestId === undefined ? { action } : { action, modelRequestId };
}

function assertDurableIdentity(value: string, label: string): void {
  if (value.length === 0) {
    throw new ThreadTurnContractError(`${label} must be non-empty`);
  }
}

function sameIdentitySet(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length || new Set(left).size !== left.length) {
    return false;
  }
  const rightSet = new Set(right);
  return left.every((identity) => rightSet.has(identity));
}
