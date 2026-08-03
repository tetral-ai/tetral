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

export type ThreadTurnFact =
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
        | { readonly type: "end_turn" }
        | { readonly type: "requires_action"; readonly eventIds: readonly string[] }
        | { readonly type: "retries_exhausted"; readonly failureEventId: string };
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
): ThreadTurnDecision {
  const checkpoint = parseThreadTurnCheckpoint(checkpointInput);
  const routeView = parseThreadToolRouteView(routeViewInput);
  const modelRequestId = checkpoint.request?.modelRequestId;

  if (checkpoint.interruptEventId !== undefined) {
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
    if (checkpoint.pendingInputMessageIds.length > 0) {
      return readyToRequest();
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
  if (request.requestKind === "approval_reviewer") {
    return {
      state: { state: "request_sealed", modelRequestId: request.modelRequestId },
      action: { action: "complete_reviewer", modelRequestId: request.modelRequestId },
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
    return readyToRequest();
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
): ThreadTurnReduction {
  const checkpoint = parseThreadTurnCheckpoint(checkpointInput);
  return {
    checkpoint,
    appliedEventIds: [],
    ...deriveThreadTurnDecision(checkpoint, routeView),
  };
}

export function reduceThreadTurn(
  current: ThreadTurnReduction,
  fact: ThreadTurnFact,
  routeView: ThreadToolRouteView,
): ThreadTurnReduction {
  assertDurableIdentity(fact.eventId, "eventId");
  if (current.appliedEventIds.includes(fact.eventId)) {
    return { ...current, action: { action: "none" } };
  }

  const appliedEventIds = [...current.appliedEventIds, fact.eventId];
  switch (fact.fact) {
    case "inputs_committed": {
      const checkpoint = applyCommittedInputs(current.checkpoint, fact.messageIds);
      return stableReduction(checkpoint, appliedEventIds, routeView);
    }
    case "request_started": {
      if (current.state.state !== "ready_to_request") {
        throw new Error(`cannot start Request from ${current.state.state}`);
      }
      assertDurableIdentity(fact.modelRequestId, "modelRequestId");
      if (!Number.isSafeInteger(fact.contextThroughMessageSequence) || fact.contextThroughMessageSequence < 0) {
        throw new Error("contextThroughMessageSequence must be a non-negative safe integer");
      }
      const consumed = new Set(fact.consumedInputMessageIds);
      if (consumed.size !== fact.consumedInputMessageIds.length) {
        throw new Error("consumedInputMessageIds must be unique");
      }
      for (const messageId of consumed) {
        if (!current.checkpoint.pendingInputMessageIds.includes(messageId)) {
          throw new Error("Request Start consumed an unknown input message");
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
        throw new Error("cannot append Tool Use after Request End");
      }
      assertDurableIdentity(fact.modelToolCallId, "modelToolCallId");
      assertDurableIdentity(fact.toolName, "toolName");
      if (request.toolMembers.some((member) => member.modelToolCallId === fact.modelToolCallId)) {
        throw new Error("modelToolCallId must be unique within a request");
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
        throw new Error("cannot append internal Tool repair after Request End");
      }
      if (request.toolMembers.some((member) => member.modelToolCallId === fact.modelToolCallId)) {
        throw new Error("modelToolCallId must be unique within a request");
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
      return stableReduction(checkpoint, appliedEventIds, routeView);
    }
    case "tool_result_committed": {
      const request = current.checkpoint.request;
      if (request === undefined) {
        throw new Error("Tool Result does not name a request member");
      }
      let matched = false;
      const toolMembers = request.toolMembers.map((member) => {
        if (member.memberKind !== "public_tool_use" || member.toolUseEventId !== fact.toolUseEventId) {
          return member;
        }
        matched = true;
        if (member.terminalResult !== undefined) {
          throw new Error("Tool Use already has a terminal Tool Result");
        }
        return {
          ...member,
          terminalResult: { resultEventId: fact.eventId, outcome: fact.outcome },
        } as const;
      });
      if (!matched) {
        throw new Error("Tool Result does not name a request member");
      }
      return stableReduction(
        replaceRequest(current.checkpoint, { ...request, toolMembers }),
        appliedEventIds,
        routeView,
      );
    }
    case "request_ended": {
      const request = currentRequest(current.checkpoint, fact.modelRequestId);
      if (request.requestEnd !== undefined) {
        throw new Error("Request already has a durable End");
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
          throw new Error(`cannot finish requires_action from ${current.state.state}`);
        }
        if (current.action.action !== "finish_idle" || current.action.stopReason.type !== "requires_action") {
          throw new Error("requires_action ACK does not match the current FinishIdle action");
        }
        if (!sameIdentitySet(fact.stopReason.eventIds, current.action.stopReason.eventIds)) {
          throw new Error("requires_action ACK event IDs do not match the declared closeout");
        }
        const checkpoint = parseThreadTurnCheckpoint({
          ...checkpointWithoutExecutionRun(current.checkpoint),
          idleCloseout: { eventId: fact.eventId, stopReason: "requires_action" },
        });
        const request = checkpoint.request;
        if (request === undefined) {
          throw new Error("requires_action closeout has no request");
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
        if (current.state.state !== "request_sealed") {
          throw new Error(`cannot finish retries_exhausted from ${current.state.state}`);
        }
        assertDurableIdentity(fact.stopReason.failureEventId, "failureEventId");
        const checkpoint = parseThreadTurnCheckpoint({
          ...checkpointWithoutExecutionRun(current.checkpoint),
          terminalCloseout: {
            failureEventId: fact.stopReason.failureEventId,
            closeoutEventId: fact.eventId,
            disposition: "retries_exhausted",
          },
          idleCloseout: { eventId: fact.eventId, stopReason: "retries_exhausted" },
        });
        return stableReduction(checkpoint, appliedEventIds, routeView);
      }
      if (fact.stopReason.type === "end_turn" && current.state.state !== "ready_to_finish") {
        throw new Error(`cannot finish end_turn from ${current.state.state}`);
      }
      const checkpoint = parseThreadTurnCheckpoint({
        ...checkpointWithoutExecutionRun(current.checkpoint),
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
      return stableReduction(checkpoint, appliedEventIds, routeView);
    }
    case "terminal_closeout_committed": {
      const checkpoint = parseThreadTurnCheckpoint({
        ...current.checkpoint,
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
): ThreadTurnReduction {
  if (current.state.state !== "request_sealed" || current.action.action !== "reconcile_request_seal") {
    throw new Error("only a newly sealed request can be reconciled");
  }
  return stableReduction(current.checkpoint, current.appliedEventIds, routeView);
}

function stableReduction(
  checkpoint: ThreadTurnCheckpoint,
  appliedEventIds: readonly string[],
  routeView: ThreadToolRouteView,
): ThreadTurnReduction {
  return {
    checkpoint,
    appliedEventIds,
    ...deriveThreadTurnDecision(checkpoint, routeView),
  };
}

function applyCommittedInputs(
  checkpoint: ThreadTurnCheckpoint,
  messageIds: readonly string[],
): ThreadTurnCheckpoint {
  const nextIds = [...checkpoint.pendingInputMessageIds];
  for (const messageId of messageIds) {
    assertDurableIdentity(messageId, "messageId");
    if (nextIds.includes(messageId)) {
      throw new Error("committed input message is already pending");
    }
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
    throw new Error("durable fact does not belong to the current model request");
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
      throw new Error("tool route does not name a request member");
    }
  }
  for (const member of incompleteMembers) {
    const count = routeView.routes.filter((route) => route.toolUseEventId === member.toolUseEventId).length;
    if (count !== 1) {
      throw new Error("sealed non-terminal Tool Use has no route");
    }
  }
}

function routeDisposition(
  routeView: ThreadToolRouteView,
  toolUseEventId: string,
): ThreadToolRouteView["routes"][number]["disposition"] {
  const route = routeView.routes.find((candidate) => candidate.toolUseEventId === toolUseEventId);
  if (route === undefined) {
    throw new Error("sealed non-terminal Tool Use has no route");
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
    throw new Error(`${label} must be non-empty`);
  }
}

function sameIdentitySet(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length || new Set(left).size !== left.length) {
    return false;
  }
  const rightSet = new Set(right);
  return left.every((identity) => rightSet.has(identity));
}
