import { describe, expect, test } from "bun:test";
import {
  deriveThreadTurnDecision,
  initializeThreadTurnReduction,
  reconcileThreadTurnSeal,
  reduceThreadTurn,
} from "../../../src/thread-loop/thread-turn-reducer.js";
import type {
  ThreadToolRouteView,
  ThreadTurnCheckpoint,
} from "../../../src/thread-loop/thread-turn-checkpoint.js";

const noRoutes: ThreadToolRouteView = { routes: [] };

describe("Thread-turn reducer", () => {
  test("derives idle and plural committed-input readiness", () => {
    expect(deriveThreadTurnDecision({ pendingInputMessageIds: [] }, noRoutes)).toEqual({
      state: { state: "idle" },
      action: { action: "await_input" },
    });
    expect(deriveThreadTurnDecision({
      pendingInputMessageIds: ["msg_1", "msg_2"],
    }, noRoutes)).toEqual({
      state: { state: "ready_to_request" },
      action: { action: "prepare_next_request" },
    });
  });

  test("starts one prepared provider request only on first local Request Start ACK application", () => {
    const initial = initializeThreadTurnReduction({
      executionRunId: "run_1",
      pendingInputMessageIds: ["msg_1", "msg_2"],
    }, noRoutes);
    const started = reduceThreadTurn(initial, {
      fact: "request_started",
      eventId: "event_start",
      modelRequestId: "request_1",
      requestKind: "agent_provider_request",
      contextThroughMessageSequence: 2,
      consumedInputMessageIds: ["msg_1", "msg_2"],
    }, noRoutes);

    expect(started).toMatchObject({
      checkpoint: {
        pendingInputMessageIds: [],
        request: { modelRequestId: "request_1" },
      },
      state: { state: "request_open", modelRequestId: "request_1" },
      action: { action: "start_provider_request", modelRequestId: "request_1" },
    });

    expect(reduceThreadTurn(started, {
      fact: "request_started",
      eventId: "event_start",
      modelRequestId: "request_1",
      requestKind: "agent_provider_request",
      contextThroughMessageSequence: 2,
      consumedInputMessageIds: ["msg_1", "msg_2"],
    }, noRoutes)).toMatchObject({
      state: { state: "request_open", modelRequestId: "request_1" },
      action: { action: "none" },
    });
  });

  test("keeps streaming Tool Use and early Tool Result inside an open request", () => {
    const opened = openRequest();
    const toolUse = reduceThreadTurn(opened, {
      fact: "tool_use_committed",
      eventId: "event_tool_1",
      modelRequestId: "request_1",
      modelToolCallId: "call_1",
      toolName: "Read",
    }, noRoutes);
    expect(toolUse).toMatchObject({
      state: { state: "request_open", modelRequestId: "request_1" },
      action: { action: "dispatch_tool_use", toolUseEventId: "event_tool_1" },
    });

    const result = reduceThreadTurn(toolUse, {
      fact: "tool_result_committed",
      eventId: "event_result_1",
      toolUseEventId: "event_tool_1",
      outcome: "success",
    }, noRoutes);
    expect(result).toMatchObject({
      checkpoint: {
        request: {
          toolMembers: [{ terminalResult: { resultEventId: "event_result_1" } }],
        },
      },
      state: { state: "request_open", modelRequestId: "request_1" },
      action: { action: "await_request_end", modelRequestId: "request_1" },
    });

    expect(reduceThreadTurn(toolUse, {
      fact: "tool_use_committed",
      eventId: "event_tool_1",
      modelRequestId: "request_1",
      modelToolCallId: "call_1",
      toolName: "Read",
    }, noRoutes)).toMatchObject({
      state: { state: "request_open", modelRequestId: "request_1" },
      action: { action: "none" },
    });
  });

  test("makes Request End an explicit seal before continuing", () => {
    const withTerminalTool = terminalToolRequest();
    const sealed = reduceThreadTurn(withTerminalTool, {
      fact: "request_ended",
      eventId: "event_end",
      modelRequestId: "request_1",
      isError: false,
      rescheduled: false,
    }, noRoutes);

    expect(sealed).toMatchObject({
      state: { state: "request_sealed", modelRequestId: "request_1" },
      action: { action: "reconcile_request_seal", modelRequestId: "request_1" },
    });
    expect(reconcileThreadTurnSeal(sealed, noRoutes)).toMatchObject({
      state: { state: "ready_to_request" },
      action: { action: "prepare_next_request" },
    });
  });

  test("waits for the whole sealed tool set and continues exactly after the last terminal receipt", () => {
    let reduction = sealRequestWithTools([
      ["event_tool_1", "call_1"],
      ["event_tool_2", "call_2"],
      ["event_tool_3", "call_3"],
      ["event_tool_4", "call_4"],
    ]);
    const routes: ThreadToolRouteView = {
      routes: [
        { toolUseEventId: "event_tool_1", disposition: "hot_execution" },
        { toolUseEventId: "event_tool_2", disposition: "hot_execution" },
        { toolUseEventId: "event_tool_3", disposition: "hot_execution" },
        { toolUseEventId: "event_tool_4", disposition: "hot_execution" },
      ],
    };
    reduction = reconcileThreadTurnSeal(reduction, routes);
    expect(reduction).toMatchObject({
      state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
      action: {
        action: "await_tool_results",
        toolUseEventIds: ["event_tool_1", "event_tool_2", "event_tool_3", "event_tool_4"],
      },
    });

    for (const [index, toolUseEventId] of ["event_tool_1", "event_tool_2", "event_tool_3"].entries()) {
      reduction = reduceThreadTurn(reduction, {
        fact: "tool_result_committed",
        eventId: `event_result_${index + 1}`,
        toolUseEventId,
        outcome: "success",
      }, routesForOutstanding(routes, toolUseEventId));
      expect(reduction.state.state).toBe("waiting_for_tool_results");
      expect(reduction.action.action).toBe("await_tool_results");
    }

    reduction = reduceThreadTurn(reduction, {
      fact: "tool_result_committed",
      eventId: "event_result_4",
      toolUseEventId: "event_tool_4",
      outcome: "error",
    }, noRoutes);
    expect(reduction).toMatchObject({
      state: { state: "ready_to_request" },
      action: { action: "prepare_next_request" },
    });
  });

  test("distinguishes empty-seal completion from later committed input", () => {
    const emptySeal = sealedCheckpoint([]);
    expect(deriveThreadTurnDecision(emptySeal, noRoutes)).toEqual({
      state: { state: "ready_to_finish" },
      action: { action: "finish_idle", stopReason: { type: "end_turn" } },
    });
    expect(deriveThreadTurnDecision({
      ...emptySeal,
      pendingInputMessageIds: ["msg_later"],
    }, noRoutes)).toEqual({
      state: { state: "ready_to_request" },
      action: { action: "prepare_next_request" },
    });

    const readyToFinish = initializeThreadTurnReduction(emptySeal, noRoutes);
    expect(reduceThreadTurn(readyToFinish, {
      fact: "finish_idle_committed",
      eventId: "event_idle",
      stopReason: { type: "end_turn" },
    }, noRoutes)).toMatchObject({
      checkpoint: { idleCloseout: { eventId: "event_idle", stopReason: "end_turn" } },
      state: { state: "idle" },
      action: { action: "await_input" },
    });
  });

  test("treats internal invalid-tool repair as a terminal synthetic member", () => {
    const repaired = reduceThreadTurn(openRequest(), {
      fact: "internal_tool_repair_committed",
      eventId: "event_repair",
      modelRequestId: "request_1",
      modelToolCallId: "call_invalid",
      toolName: "missing_tool",
    }, noRoutes);
    expect(repaired).toMatchObject({
      state: { state: "request_open", modelRequestId: "request_1" },
      action: { action: "await_request_end", modelRequestId: "request_1" },
    });

    const sealed = reduceThreadTurn(repaired, {
      fact: "request_ended",
      eventId: "event_end",
      modelRequestId: "request_1",
      isError: false,
      rescheduled: false,
    }, noRoutes);
    expect(reconcileThreadTurnSeal(sealed, noRoutes)).toMatchObject({
      state: { state: "ready_to_request" },
      action: { action: "prepare_next_request" },
    });
  });

  test("emits a bounded cold resume action before converging to the stable wait", () => {
    const checkpoint = sealedCheckpoint([
      {
        memberKind: "public_tool_use",
        modelToolCallId: "call_1",
        toolUseEventId: "event_tool_1",
        toolName: "Bash",
      },
      {
        memberKind: "public_tool_use",
        modelToolCallId: "call_2",
        toolUseEventId: "event_tool_2",
        toolName: "Write",
      },
    ]);
    const coldRoutes: ThreadToolRouteView = {
      routes: [
        { toolUseEventId: "event_tool_1", disposition: "resume_sandbox_execution" },
        { toolUseEventId: "event_tool_2", disposition: "resume_approval_settlement" },
      ],
    };
    expect(deriveThreadTurnDecision(checkpoint, coldRoutes)).toEqual({
      state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
      action: {
        action: "resume_tool_routes",
        modelRequestId: "request_1",
        toolUseEventIds: ["event_tool_1", "event_tool_2"],
      },
    });
    expect(deriveThreadTurnDecision(checkpoint, {
      routes: [
        { toolUseEventId: "event_tool_1", disposition: "hot_execution" },
        { toolUseEventId: "event_tool_2", disposition: "hot_execution" },
      ],
    })).toEqual({
      state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
      action: {
        action: "await_tool_results",
        modelRequestId: "request_1",
        toolUseEventIds: ["event_tool_1", "event_tool_2"],
      },
    });
  });

  test("preserves sealed approval wait across FinishIdle requires_action", () => {
    const checkpoint = sealedCheckpoint([
      {
        memberKind: "public_tool_use",
        modelToolCallId: "call_1",
        toolUseEventId: "event_tool_1",
        toolName: "Write",
      },
    ]);
    const routes: ThreadToolRouteView = {
      routes: [{ toolUseEventId: "event_tool_1", disposition: "requires_user_action" }],
    };
    const waiting = initializeThreadTurnReduction(checkpoint, routes);
    expect(waiting).toMatchObject({
      state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
      action: {
        action: "finish_idle",
        stopReason: { type: "requires_action", eventIds: ["event_tool_1"] },
      },
    });

    const idleReceipt = reduceThreadTurn(waiting, {
      fact: "finish_idle_committed",
      eventId: "event_idle",
      stopReason: { type: "requires_action", eventIds: ["event_tool_1"] },
    }, routes);
    expect(idleReceipt).toMatchObject({
      state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
      action: { action: "await_tool_results", toolUseEventIds: ["event_tool_1"] },
    });
    expect(idleReceipt.checkpoint.executionRunId).toBeUndefined();
  });

  test("does not finish requires_action while another sealed tool is still executing", () => {
    const checkpoint = sealedCheckpoint([
      {
        memberKind: "public_tool_use",
        modelToolCallId: "call_running",
        toolUseEventId: "event_tool_running",
        toolName: "Read",
      },
      {
        memberKind: "public_tool_use",
        modelToolCallId: "call_approval",
        toolUseEventId: "event_tool_approval",
        toolName: "Write",
      },
    ]);
    const routes: ThreadToolRouteView = {
      routes: [
        { toolUseEventId: "event_tool_running", disposition: "hot_execution" },
        { toolUseEventId: "event_tool_approval", disposition: "requires_user_action" },
      ],
    };
    expect(deriveThreadTurnDecision(checkpoint, routes)).toEqual({
      state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
      action: {
        action: "await_tool_results",
        modelRequestId: "request_1",
        toolUseEventIds: ["event_tool_running", "event_tool_approval"],
      },
    });
    const waiting = initializeThreadTurnReduction(checkpoint, routes);
    expect(() => reduceThreadTurn(waiting, {
      fact: "finish_idle_committed",
      eventId: "event_idle",
      stopReason: { type: "requires_action", eventIds: ["event_tool_approval"] },
    }, routes)).toThrow("requires_action ACK does not match the current FinishIdle action");
  });

  test("rejects a requires-action ACK whose event set differs from the declared closeout", () => {
    const checkpoint = sealedCheckpoint([
      {
        memberKind: "public_tool_use",
        modelToolCallId: "call_1",
        toolUseEventId: "event_tool_1",
        toolName: "Write",
      },
    ]);
    const routes: ThreadToolRouteView = {
      routes: [{ toolUseEventId: "event_tool_1", disposition: "requires_user_action" }],
    };
    const waiting = initializeThreadTurnReduction(checkpoint, routes);
    expect(() => reduceThreadTurn(waiting, {
      fact: "finish_idle_committed",
      eventId: "event_idle",
      stopReason: { type: "requires_action", eventIds: ["event_other"] },
    }, routes)).toThrow("requires_action ACK event IDs do not match the declared closeout");
  });

  test("records an interrupt-prioritized Tool Use ACK without dispatching its side effect", () => {
    const interrupted = reduceThreadTurn(openRequest(), {
      fact: "interrupt_committed",
      eventId: "event_interrupt",
    }, noRoutes);
    const lateToolUse = reduceThreadTurn(interrupted, {
      fact: "tool_use_committed",
      eventId: "event_tool_1",
      modelRequestId: "request_1",
      modelToolCallId: "call_1",
      toolName: "Bash",
    }, noRoutes);

    expect(lateToolUse.checkpoint.request?.toolMembers).toHaveLength(1);
    expect(lateToolUse).toMatchObject({
      state: { state: "request_open", modelRequestId: "request_1" },
      action: { action: "close_interrupted", modelRequestId: "request_1" },
    });
  });

  test("makes retries-exhausted FinishIdle hot reduction reconstructible", () => {
    const sealed = reduceThreadTurn(openRequest(), {
      fact: "request_ended",
      eventId: "event_end",
      modelRequestId: "request_1",
      isError: true,
      errorKind: "provider_stream_error",
      rescheduled: false,
    }, noRoutes);
    const finished = reduceThreadTurn(sealed, {
      fact: "finish_idle_committed",
      eventId: "event_idle",
      stopReason: {
        type: "retries_exhausted",
        failureEventId: "event_error",
      },
    }, noRoutes);

    expect(finished.checkpoint).toMatchObject({
      terminalCloseout: {
        failureEventId: "event_error",
        closeoutEventId: "event_idle",
        disposition: "retries_exhausted",
      },
    });
    expect(finished.checkpoint.executionRunId).toBeUndefined();
    expect(deriveThreadTurnDecision(finished.checkpoint, noRoutes)).toEqual({
      state: finished.state,
      action: finished.action,
    });
  });

  test("routes compaction, reviewer, retry, interrupt, and terminal closeout before ordinary continuation", () => {
    expect(deriveThreadTurnDecision(sealedCheckpoint([], "compaction_summary"), noRoutes)).toMatchObject({
      state: { state: "ready_to_request" },
      action: { action: "continue_after_compaction", modelRequestId: "request_1" },
    });
    expect(deriveThreadTurnDecision(sealedCheckpoint([], "approval_reviewer"), noRoutes)).toMatchObject({
      state: { state: "request_sealed", modelRequestId: "request_1" },
      action: { action: "complete_reviewer", modelRequestId: "request_1" },
    });
    for (const requestKind of ["compaction_summary", "approval_reviewer"] as const) {
      expect(deriveThreadTurnDecision(sealedCheckpoint([], requestKind, {
        isError: true,
        rescheduled: false,
        errorKind: "provider_stream_error",
      }), noRoutes)).toMatchObject({
        state: { state: "request_sealed", modelRequestId: "request_1" },
        action: { action: "apply_request_retry_or_reschedule", modelRequestId: "request_1" },
      });
    }
    expect(deriveThreadTurnDecision(sealedCheckpoint([], "agent_provider_request", {
      isError: true,
      rescheduled: true,
      errorKind: "provider_unavailable",
    }), noRoutes)).toMatchObject({
      action: { action: "apply_request_retry_or_reschedule", modelRequestId: "request_1" },
    });
    expect(deriveThreadTurnDecision({
      ...sealedCheckpoint([]),
      interruptEventId: "event_interrupt",
    }, noRoutes)).toEqual({
      state: { state: "request_sealed", modelRequestId: "request_1" },
      action: { action: "close_interrupted", modelRequestId: "request_1" },
    });
    expect(deriveThreadTurnDecision({
      ...sealedCheckpoint([]),
      terminalCloseout: {
        failureEventId: "event_error",
        closeoutEventId: "event_terminated",
        disposition: "terminated",
      },
    }, noRoutes)).toEqual({
      state: { state: "idle" },
      action: { action: "await_input" },
    });
  });

  test("fails closed for orphan results, post-seal Tool Uses, and missing sealed routes", () => {
    expect(() => reduceThreadTurn(openRequest(), {
      fact: "tool_result_committed",
      eventId: "event_result",
      toolUseEventId: "event_missing",
      outcome: "success",
    }, noRoutes)).toThrow("Tool Result does not name a request member");

    const sealed = initializeThreadTurnReduction(sealedCheckpoint([]), noRoutes);
    expect(() => reduceThreadTurn(sealed, {
      fact: "tool_use_committed",
      eventId: "event_tool",
      modelRequestId: "request_1",
      modelToolCallId: "call_1",
      toolName: "Read",
    }, noRoutes)).toThrow("cannot append Tool Use after Request End");

    expect(() => deriveThreadTurnDecision(sealedCheckpoint([
      {
        memberKind: "public_tool_use",
        modelToolCallId: "call_1",
        toolUseEventId: "event_tool_1",
        toolName: "Read",
      },
    ]), noRoutes)).toThrow("sealed non-terminal Tool Use has no route");
  });
});

function openRequest() {
  return initializeThreadTurnReduction({
    executionRunId: "run_1",
    pendingInputMessageIds: [],
    request: {
      modelRequestId: "request_1",
      requestStartEventId: "event_start",
      requestKind: "agent_provider_request",
      contextThroughMessageSequence: 1,
      toolMembers: [],
    },
  }, noRoutes);
}

function terminalToolRequest() {
  const withTool = reduceThreadTurn(openRequest(), {
    fact: "tool_use_committed",
    eventId: "event_tool_1",
    modelRequestId: "request_1",
    modelToolCallId: "call_1",
    toolName: "Read",
  }, noRoutes);
  return reduceThreadTurn(withTool, {
    fact: "tool_result_committed",
    eventId: "event_result_1",
    toolUseEventId: "event_tool_1",
    outcome: "success",
  }, noRoutes);
}

function sealRequestWithTools(tools: readonly (readonly [string, string])[]) {
  let reduction = openRequest();
  for (const [toolUseEventId, modelToolCallId] of tools) {
    reduction = reduceThreadTurn(reduction, {
      fact: "tool_use_committed",
      eventId: toolUseEventId,
      modelRequestId: "request_1",
      modelToolCallId,
      toolName: "Read",
    }, noRoutes);
  }
  return reduceThreadTurn(reduction, {
    fact: "request_ended",
    eventId: "event_end",
    modelRequestId: "request_1",
    isError: false,
    rescheduled: false,
  }, noRoutes);
}

function sealedCheckpoint(
  toolMembers: NonNullable<ThreadTurnCheckpoint["request"]>["toolMembers"],
  requestKind: NonNullable<ThreadTurnCheckpoint["request"]>["requestKind"] = "agent_provider_request",
  requestEnd: { readonly isError: boolean; readonly rescheduled: boolean; readonly errorKind?: string } = {
    isError: false,
    rescheduled: false,
  },
): ThreadTurnCheckpoint {
  return {
    executionRunId: "run_1",
    pendingInputMessageIds: [],
    request: {
      modelRequestId: "request_1",
      requestStartEventId: "event_start",
      requestKind,
      contextThroughMessageSequence: 1,
      requestEnd: {
        eventId: "event_end",
        ...requestEnd,
      },
      toolMembers,
    },
  };
}

function routesForOutstanding(
  routes: ThreadToolRouteView,
  settledToolUseEventId: string,
): ThreadToolRouteView {
  return {
    routes: routes.routes.filter((route) => route.toolUseEventId !== settledToolUseEventId),
  };
}
