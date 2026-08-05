import { describe, expect, test } from "bun:test";
import { DurableRuntimeMessageSchema } from "../../../src/contracts/runtime.js";
import type { DurableRuntimeMessage } from "../../../src/contracts/runtime.js";
import {
  extractColdThreadToolRouteView,
  extractThreadTurnCheckpoint,
  ThreadToolRouteViewSchema,
  ThreadTurnCheckpointSchema,
  ThreadTurnLoadFactsSchema,
} from "../../../src/thread-loop/thread-turn-checkpoint.js";
import type {
    ThreadToolRouteView,
    ThreadTurnLoadFacts,
} from "../../../src/thread-loop/thread-turn-checkpoint.js";
import {
  consumeThreadTurnEdge,
  initializeThreadTurnReduction,
  reconcileThreadTurnSeal,
  reduceThreadTurn,
} from "../../../src/thread-loop/thread-turn-reducer.js";
import type {
    ThreadTurnFact,
    ThreadTurnReduction,
} from "../../../src/thread-loop/thread-turn-reducer.js";

describe("ThreadTurnCheckpoint", () => {
  test("accepts the closed durable read model", () => {
    expect(ThreadTurnCheckpointSchema.parse({
      executionRunId: "run_1",
      pendingInputMessageIds: ["msg_2"],
      request: {
        modelRequestId: "request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: 1,
        requestEnd: {
          eventId: "event_end",
          isError: false,
          rescheduled: false,
        },
        toolMembers: [
          {
            memberKind: "public_tool_use",
            modelToolCallId: "call_1",
            toolUseEventId: "event_tool_1",
            toolName: "Read",
          },
          {
            memberKind: "internal_tool_repair",
            modelToolCallId: "call_2",
            toolName: "missing_tool",
            repairEventId: "event_repair_1",
            outcome: "error",
          },
        ],
      },
    })).toMatchObject({
      executionRunId: "run_1",
      pendingInputMessageIds: ["msg_2"],
    });
  });

  test("rejects ambiguous request membership and malformed durable identities", () => {
    const duplicateCallId = {
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: 1,
        toolMembers: [
          {
            memberKind: "public_tool_use",
            modelToolCallId: "call_1",
            toolUseEventId: "event_tool_1",
            toolName: "Read",
          },
          {
            memberKind: "internal_tool_repair",
            modelToolCallId: "call_1",
            toolName: "missing_tool",
            repairEventId: "event_repair_1",
            outcome: "error",
          },
        ],
      },
    } as const;

    expect(() => ThreadTurnCheckpointSchema.parse(duplicateCallId)).toThrow(
      "modelToolCallId must be unique within a request",
    );
    expect(() => ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: ["msg_1", "msg_1"],
    })).toThrow("pendingInputMessageIds must be unique");
    expect(() => ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: -1,
        toolMembers: [],
      },
    })).toThrow();
    expect(() => ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: Number.MAX_SAFE_INTEGER + 1,
        toolMembers: [],
      },
    })).toThrow();
  });

  test("rejects duplicate or incomplete route ownership", () => {
    expect(() => ThreadToolRouteViewSchema.parse({
      routes: [
        { toolUseEventId: "event_tool_1", disposition: "hot_execution" },
        { toolUseEventId: "event_tool_1", disposition: "requires_user_action" },
      ],
    })).toThrow("tool route ownership must be unique");
    expect(() => ThreadToolRouteViewSchema.parse({
      routes: [{ toolUseEventId: "", disposition: "hot_execution" }],
    })).toThrow();
  });

  test("rejects a requires-action closeout after its sealed request was erased", () => {
    expect(() => ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: [],
      idleCloseout: { eventId: "event_idle", stopReason: "requires_action" },
    })).toThrow("requires_action closeout must retain a sealed incomplete Tool Use");
    expect(() => ThreadTurnCheckpointSchema.parse({
      pendingInputMessageIds: [],
      request: {
        modelRequestId: "request_1",
        requestStartEventId: "event_start",
        requestKind: "agent_provider_request",
        contextThroughMessageSequence: 1,
        requestEnd: { eventId: "event_end", isError: false, rescheduled: false },
        toolMembers: [],
      },
      idleCloseout: { eventId: "event_idle", stopReason: "requires_action" },
    })).toThrow("requires_action closeout must retain a sealed incomplete Tool Use");
  });
});

const createdAt = "2026-08-03T00:00:00.000Z";
function durableTextMessage(input: {
    readonly id: string;
    readonly sequence: number;
    readonly eventId: string;
    readonly eventSequence: number;
    readonly role: "user" | "assistant";
}) {
    return DurableRuntimeMessageSchema.parse({
        id: input.id,
        sessionId: "session_1",
        role: input.role,
        origin: input.role === "user" ? "user" : "agent",
        sequence: input.sequence,
        status: "completed",
        owningEventId: input.eventId,
        eventSequence: input.eventSequence,
        createdAt,
        parts: [{
                id: `${input.id}_part`,
                sessionId: "session_1",
                messageId: input.id,
                sequence: 0,
                type: "text",
                text: input.id,
                truncated: false,
                status: "completed",
                createdAt,
            }],
    });
}
function durableToolMessage() {
    return DurableRuntimeMessageSchema.parse({
        id: "message_assistant",
        sessionId: "session_1",
        role: "assistant",
        origin: "agent",
        sequence: 2,
        status: "completed",
        owningEventId: "event_tool_result",
        eventSequence: 6,
        createdAt,
        parts: [{
                id: "message_assistant_tool",
                sessionId: "session_1",
                messageId: "message_assistant",
                sequence: 0,
                type: "tool",
                toolCallId: "tool_call_1",
                toolName: "Read",
                toolUseEventId: "event_tool_use",
                toolEvent: { kind: "tool" },
                state: {
                    status: "completed",
                    input: { value: {}, preview: "{}", truncated: false },
                    output: { text: "done", truncated: false },
                },
                createdAt,
                completedAt: createdAt,
            }],
    });
}
function durableOpenToolMessage() {
    return DurableRuntimeMessageSchema.parse({
        id: "message_tool_use",
        sessionId: "session_1",
        role: "assistant",
        origin: "agent",
        sequence: 2,
        status: "streaming",
        owningEventId: "event_tool_use",
        eventSequence: 4,
        createdAt,
        parts: [{
                id: "message_tool_use_part",
                sessionId: "session_1",
                messageId: "message_tool_use",
                sequence: 0,
                type: "tool",
                toolCallId: "tool_call_1",
                toolName: "Read",
                toolUseEventId: "event_tool_use",
                toolEvent: { kind: "tool" },
                state: {
                    status: "running",
                    input: { value: {}, preview: "{}", truncated: false },
                },
                createdAt,
            }],
    });
}
function durableInternalRepairMessage() {
    return DurableRuntimeMessageSchema.parse({
        id: "message_repair",
        sessionId: "session_1",
        role: "assistant",
        origin: "agent",
        sequence: 1,
        status: "completed",
        owningEventId: "event_repair",
        eventSequence: 2,
        createdAt,
        parts: [{
                id: "message_repair_tool",
                sessionId: "session_1",
                messageId: "message_repair",
                sequence: 0,
                type: "tool",
                toolCallId: "tool_call_repair",
                toolName: "unknown_tool",
                toolEvent: { kind: "tool" },
                state: {
                    status: "error",
                    input: { value: {}, preview: "{}", truncated: false },
                    error: { type: "invalid_tool", message: "invalid tool" },
                },
                createdAt,
                completedAt: createdAt,
            }],
    });
}
function durableIdleCleanupMessage() {
    return DurableRuntimeMessageSchema.parse({
        id: "message_idle_cleanup",
        sessionId: "session_1",
        role: "assistant",
        origin: "agent",
        sequence: 3,
        status: "completed",
        owningEventId: "event_tool_result",
        eventSequence: 7,
        createdAt,
        parts: [{
                id: "message_idle_cleanup_tool",
                sessionId: "session_1",
                messageId: "message_idle_cleanup",
                sequence: 0,
                type: "tool",
                toolCallId: "tool_call_1",
                toolName: "Read",
                toolUseEventId: "event_tool_use",
                toolEvent: { kind: "tool" },
                state: {
                    status: "error",
                    input: { value: {}, preview: "{}", truncated: false },
                    error: { type: "cleanup_expired", message: "expired" },
                },
                createdAt,
                completedAt: createdAt,
            }],
    });
}

type ToolRouteDisposition = ThreadToolRouteView["routes"][number]["disposition"];
type ToolOutcome = Extract<ThreadTurnFact, { readonly fact: "tool_result_committed" }>["outcome"];
type ColdPendingToolUse = Parameters<typeof extractColdThreadToolRouteView>[0]["pendingToolUses"][number];
type ColdPendingSandboxExecution = Parameters<typeof extractColdThreadToolRouteView>[0]["pendingSandboxExecutions"][number];

interface DurableToolReference {
    readonly modelRequestId: string;
    readonly modelToolCallId: string;
    readonly toolUseEventId: string;
    readonly toolName: string;
    readonly messageId: string;
}

/** Exercises the hot reducer and cold extractor after every durable operation. */
class DurableTurnPrefixHarness {
    readonly #scenario: string;
    readonly #events: ThreadTurnLoadFacts["events"][number][] = [];
    readonly #messages: DurableRuntimeMessage[] = [];
    readonly #messageLineage: ThreadTurnLoadFacts["messageLineage"][number][] = [];
    readonly #routes = new Map<string, ToolRouteDisposition>();
    readonly #pendingToolUses = new Map<string, ColdPendingToolUse>();
    readonly #pendingSandboxExecutions = new Map<string, ColdPendingSandboxExecution>();
    readonly #toolReferences = new Map<string, DurableToolReference>();
    #hot: ThreadTurnReduction = initializeThreadTurnReduction(
        { pendingInputMessageIds: [] },
        { routes: [] },
    );
    #eventSequence = 0;
    #messageSequence = 0;
    #identitySequence = 0;
    #prefixSequence = 0;
    #coldRouteDifferenceCount = 0;
    #coldResumeActionCount = 0;

    constructor(scenario: string) {
        this.#scenario = scenario;
    }

    get checkpoint(): ThreadTurnReduction["checkpoint"] {
        return this.#hot.checkpoint;
    }

    get coldRouteDifferenceCount(): number {
        return this.#coldRouteDifferenceCount;
    }

    get coldResumeActionCount(): number {
        return this.#coldResumeActionCount;
    }

    openRun(): void {
        const eventId = this.#nextIdentity("running");
        this.#events.push({
            eventId,
            eventSequence: this.#nextEventSequence(),
            type: "session.status_running",
        });
        this.#hot = reduceThreadTurn(this.#hot, { fact: "run_opened", eventId }, this.#routeView());
        this.#assertConverges("run opened");
    }

    commitInput(sourceKind: "messages" | "approval_review" | "agent_mail" = "messages"): string {
        const messageId = this.#nextIdentity("input_message");
        const eventId = this.#nextIdentity("input_event");
        const eventSequence = this.#nextEventSequence();
        const message = durableTextMessage({
            id: messageId,
            sequence: this.#nextMessageSequence(),
            eventId,
            eventSequence,
            role: "user",
        });
        this.#messages.push(message);
        this.#messageLineage.push({
            messageId,
            messageSequence: message.sequence,
            owningEventId: eventId,
            entries: [{
                lineageKind: "declaration_receipt",
                operationKind: "commit_inputs",
                sourceKind,
                sourceId: this.#nextIdentity("input_source"),
                eventId,
                eventSequence,
                disposition: "created",
            }],
        });
        this.#hot = reduceThreadTurn(this.#hot, {
            fact: "inputs_committed",
            eventId,
            messageIds: [messageId],
        }, this.#routeView());
        this.#assertConverges(`${sourceKind} input committed`);
        return messageId;
    }

    startRequest(
        requestKind: "agent_provider_request" | "compaction_summary" | "approval_reviewer" = "agent_provider_request",
    ): string {
        const eventId = this.#nextIdentity("request_start");
        const modelRequestId = this.#nextIdentity("model_request");
        const consumedInputMessageIds = [...this.#hot.checkpoint.pendingInputMessageIds];
        const contextThroughMessageSequence = this.#messageSequence;
        this.#events.push({
            eventId,
            eventSequence: this.#nextEventSequence(),
            type: "span.model_request_start",
            modelRequestId,
            requestStart: { requestKind, contextThroughMessageSequence },
        });
        this.#hot = reduceThreadTurn(this.#hot, {
            fact: "request_started",
            eventId,
            modelRequestId,
            requestKind,
            contextThroughMessageSequence,
            consumedInputMessageIds,
        }, this.#routeView());
        expect(this.#hot.action.action).toBe("start_provider_request");
        this.#hot = consumeThreadTurnEdge(this.#hot, this.#routeView());
        this.#assertConverges(`${requestKind} request started`);
        return modelRequestId;
    }

    commitToolUse(
        modelRequestId: string,
        disposition: ToolRouteDisposition = "hot_execution",
        toolName = "Read",
    ): DurableToolReference {
        const eventId = this.#nextIdentity("tool_use");
        const modelToolCallId = this.#nextIdentity("tool_call");
        const messageId = this.#nextIdentity("tool_message");
        const eventSequence = this.#nextEventSequence();
        const messageSequence = this.#nextMessageSequence();
        const message = DurableRuntimeMessageSchema.parse({
            id: messageId,
            sessionId: "session_1",
            role: "assistant",
            origin: "agent",
            sequence: messageSequence,
            status: "streaming",
            owningEventId: eventId,
            eventSequence,
            createdAt,
            parts: [{
                id: `${messageId}_part`,
                sessionId: "session_1",
                messageId,
                sequence: 0,
                type: "tool",
                toolCallId: modelToolCallId,
                toolName,
                toolUseEventId: eventId,
                toolEvent: { kind: "tool" },
                state: {
                    status: "running",
                    input: { value: {}, preview: "{}", truncated: false },
                },
                createdAt,
            }],
        });
        this.#messages.push(message);
        this.#messageLineage.push({
            messageId,
            messageSequence,
            owningEventId: eventId,
            modelRequestId,
            entries: [{
                lineageKind: "declaration_receipt",
                operationKind: "write_event",
                sourceKind: "agent.tool_use",
                sourceId: this.#nextIdentity("tool_use_source"),
                eventId,
                eventSequence,
                disposition: "created",
            }],
        });
        this.#events.push({
            eventId,
            eventSequence,
            type: "agent.tool_use",
            modelRequestId,
            toolUse: { modelToolCallId, toolName },
        });
        this.#routes.set(eventId, disposition);
        if (disposition === "requires_user_action" || disposition === "resume_approval_settlement") {
            this.#pendingToolUses.set(eventId, {
                toolUseEventId: eventId,
                modelRequestId,
                modelToolCallId,
                toolName,
                status: disposition === "requires_user_action" ? "pending" : "resolving",
                ...(disposition === "resume_approval_settlement" ? { decision: "allow" as const } : {}),
            });
        } else {
        this.#pendingSandboxExecutions.set(eventId, {
                toolUseEventId: eventId,
                modelRequestId,
                modelToolCallId,
                toolName,
            });
        }
        this.#toolReferences.set(eventId, { modelRequestId, modelToolCallId, toolUseEventId: eventId, toolName, messageId });
        this.#hot = reduceThreadTurn(this.#hot, {
            fact: "tool_use_committed",
            eventId,
            modelRequestId,
            modelToolCallId,
            toolName,
        }, this.#routeView());
        expect(this.#hot.action.action).toBe("dispatch_tool_use");
        this.#hot = consumeThreadTurnEdge(this.#hot, this.#routeView());
        this.#assertConverges("Tool Use committed");
        return { modelRequestId, modelToolCallId, toolUseEventId: eventId, toolName, messageId };
    }

    commitToolResult(tool: DurableToolReference, outcome: ToolOutcome = "success"): void {
        const eventId = this.#nextIdentity("tool_result");
        const eventSequence = this.#nextEventSequence();
        const messageIndex = this.#messages.findIndex((message) => message.id === tool.messageId);
        const currentMessage = this.#messages[messageIndex];
        if (currentMessage === undefined) {
            throw new Error("test Tool Use projection is missing");
        }
        const terminalState = outcome === "success"
            ? {
                status: "completed" as const,
                input: { value: {}, preview: "{}", truncated: false },
                output: { text: "done", truncated: false },
            }
            : {
                status: outcome === "cancelled" ? "cancelled" as const : "error" as const,
                input: { value: {}, preview: "{}", truncated: false },
                error: { type: `tool_${outcome}`, message: `tool ${outcome}` },
            };
        this.#messages[messageIndex] = DurableRuntimeMessageSchema.parse({
            ...currentMessage,
            status: "completed",
            owningEventId: eventId,
            eventSequence,
            parts: currentMessage.parts.map((part) =>
                part.type === "tool"
                    ? { ...part, state: terminalState, completedAt: createdAt }
                    : part
            ),
        });
        const lineageIndex = this.#messageLineage.findIndex((lineage) => lineage.messageId === tool.messageId);
        const currentLineage = this.#messageLineage[lineageIndex];
        if (currentLineage === undefined) {
            throw new Error("test Tool Use lineage is missing");
        }
        this.#messageLineage[lineageIndex] = {
            ...currentLineage,
            owningEventId: eventId,
            entries: [
                ...currentLineage.entries,
                {
                    lineageKind: "declaration_receipt",
                    operationKind: "write_event",
                    sourceKind: "agent.tool_result",
                    sourceId: this.#nextIdentity("tool_result_source"),
                    eventId,
                    eventSequence,
                    disposition: "updated",
                },
            ],
        };
        this.#events.push({
            eventId,
            eventSequence,
            type: "agent.tool_result",
            modelRequestId: tool.modelRequestId,
            toolResult: { toolUseEventId: tool.toolUseEventId, outcome },
        });
        this.#routes.delete(tool.toolUseEventId);
        this.#pendingToolUses.delete(tool.toolUseEventId);
        this.#pendingSandboxExecutions.delete(tool.toolUseEventId);
        this.#toolReferences.delete(tool.toolUseEventId);
        this.#hot = reduceThreadTurn(this.#hot, {
            fact: "tool_result_committed",
            eventId,
            toolUseEventId: tool.toolUseEventId,
            outcome,
        }, this.#routeView());
        this.#assertConverges(`Tool Result ${outcome} committed`);
    }

    endRequest(input: {
        readonly modelRequestId: string;
        readonly isError?: boolean;
        readonly errorKind?: string;
        readonly rescheduled?: boolean;
    }): void {
        const request = this.#hot.checkpoint.request;
        if (request === undefined) {
            throw new Error("test request is missing");
        }
        const eventId = this.#nextIdentity("request_end");
        const isError = input.isError ?? false;
        const rescheduled = input.rescheduled ?? false;
        this.#events.push({
            eventId,
            eventSequence: this.#nextEventSequence(),
            type: "span.model_request_end",
            modelRequestId: input.modelRequestId,
            requestEnd: {
                requestStartEventId: request.requestStartEventId,
                isError,
                ...(input.errorKind !== undefined ? { errorKind: input.errorKind } : {}),
                rescheduled,
            },
        });
        this.#hot = reduceThreadTurn(this.#hot, {
            fact: "request_ended",
            eventId,
            modelRequestId: input.modelRequestId,
            isError,
            ...(input.errorKind !== undefined ? { errorKind: input.errorKind } : {}),
            rescheduled,
        }, this.#routeView());
        expect(this.#hot.action.action).toBe("reconcile_request_seal");
        this.#hot = reconcileThreadTurnSeal(this.#hot, this.#routeView());
        this.#assertConverges("Request End committed");
    }

    finishIdle(stopReason: { readonly type: "end_turn" } | { readonly type: "requires_action"; readonly eventIds: readonly string[] }): void {
        const eventId = this.#nextIdentity("idle");
        this.#events.push({
            eventId,
            eventSequence: this.#nextEventSequence(),
            type: "session.status_idle",
            idle: { stopReason: stopReason.type },
        });
        this.#hot = reduceThreadTurn(this.#hot, {
            fact: "finish_idle_committed",
            eventId,
            stopReason,
        }, this.#routeView());
        this.#assertConverges(`FinishIdle ${stopReason.type} committed`);
    }

    markRescheduled(): void {
        this.#events.push({
            eventId: this.#nextIdentity("rescheduled"),
            eventSequence: this.#nextEventSequence(),
            type: "session.status_rescheduled",
        });
        this.#assertConverges("reschedule status committed");
    }

    interrupt(): void {
        const eventId = this.#nextIdentity("interrupt");
        this.#events.push({
            eventId,
            eventSequence: this.#nextEventSequence(),
            type: "user.interrupt",
        });
        this.#hot = reduceThreadTurn(this.#hot, { fact: "interrupt_committed", eventId }, this.#routeView());
        this.#assertConverges("interrupt committed");
    }

    terminate(errorType = "runtime_pod_lost"): void {
        const request = this.#hot.checkpoint.request;
        if (request !== undefined && request.requestEnd === undefined) {
            this.endRequest({
                modelRequestId: request.modelRequestId,
                isError: true,
                errorKind: errorType,
            });
        }
        for (const tool of [...this.#toolReferences.values()]) {
            this.commitToolResult(tool, "error");
        }
        const failureEventId = this.#nextIdentity("failure");
        this.#events.push({
            eventId: failureEventId,
            eventSequence: this.#nextEventSequence(),
            type: "session.error",
            failure: { errorType, retryStatus: "terminal" },
        });
        this.#assertConverges("terminal failure committed");
        const closeoutEventId = this.#nextIdentity("terminated");
        this.#events.push({
            eventId: closeoutEventId,
            eventSequence: this.#nextEventSequence(),
            type: "session.status_terminated",
        });
        this.#hot = reduceThreadTurn(this.#hot, {
            fact: "terminal_closeout_committed",
            eventId: closeoutEventId,
            failureEventId,
            disposition: "terminated",
        }, this.#routeView());
        this.#assertConverges("terminal closeout committed");
    }

    exhaustRetries(modelRequestId: string): void {
        this.endRequest({
            modelRequestId,
            isError: true,
            errorKind: "provider_stream_error",
            rescheduled: false,
        });
        const failureEventId = this.#nextIdentity("failure");
        this.#events.push({
            eventId: failureEventId,
            eventSequence: this.#nextEventSequence(),
            type: "session.error",
            failure: { errorType: "provider_stream_error", retryStatus: "exhausted" },
        });
        this.#assertConverges("exhausted failure committed");
        const idleEventId = this.#nextIdentity("idle");
        this.#events.push({
            eventId: idleEventId,
            eventSequence: this.#nextEventSequence(),
            type: "session.status_idle",
            idle: { stopReason: "retries_exhausted" },
        });
        this.#hot = reduceThreadTurn(this.#hot, {
            fact: "finish_idle_committed",
            eventId: idleEventId,
            stopReason: { type: "retries_exhausted", failureEventId },
        }, this.#routeView());
        this.#assertConverges("retries-exhausted closeout committed");
    }

    #assertConverges(prefix: string): void {
        this.#prefixSequence += 1;
        const hotRouteView = this.#routeView();
        const coldCheckpoint = extractThreadTurnCheckpoint({
            messages: this.#messages,
            facts: ThreadTurnLoadFactsSchema.parse({
                events: this.#events,
                messageLineage: this.#messageLineage,
            }),
        });
        const coldRouteView = extractColdThreadToolRouteView({
            checkpoint: coldCheckpoint,
            pendingToolUses: [...this.#pendingToolUses.values()],
            pendingSandboxExecutions: [...this.#pendingSandboxExecutions.values()],
        });
        let cold = initializeThreadTurnReduction(coldCheckpoint, coldRouteView);
        if (JSON.stringify(coldRouteView) !== JSON.stringify(hotRouteView)) {
            this.#coldRouteDifferenceCount += 1;
        }
        if (cold.action.action === "resume_tool_routes") {
            this.#coldResumeActionCount += 1;
            const resumed = new Set(cold.action.toolUseEventIds);
            cold = initializeThreadTurnReduction(coldCheckpoint, {
                routes: coldRouteView.routes.map((route) => resumed.has(route.toolUseEventId)
                    ? { ...route, disposition: "hot_execution" as const }
                    : route),
            });
        }
        const label = `${this.#scenario}:${this.#prefixSequence}:${prefix}`;
        expect({ label, value: coldCheckpoint }).toEqual({ label, value: this.#hot.checkpoint });
        expect({ label, value: cold.state }).toEqual({ label, value: this.#hot.state });
        expect({ label, value: cold.action }).toEqual({ label, value: this.#hot.action });
    }

    #routeView(): ThreadToolRouteView {
        return {
            routes: [...this.#routes.entries()].map(([toolUseEventId, disposition]) => ({
                toolUseEventId,
                disposition,
            })),
        };
    }

    #nextIdentity(kind: string): string {
        this.#identitySequence += 1;
        return `${this.#scenario}_${kind}_${this.#identitySequence}`;
    }

    #nextEventSequence(): number {
        this.#eventSequence += 1;
        return this.#eventSequence;
    }

    #nextMessageSequence(): number {
        this.#messageSequence += 1;
        return this.#messageSequence;
    }
}

describe("Thread turn cold extraction", () => {
    test("every durable prefix converges through multi-request tool continuation", () => {
        const trace = new DurableTurnPrefixHarness("multi_request");
        trace.openRun();
        trace.commitInput();
        trace.commitInput("agent_mail");
        const firstRequest = trace.startRequest();
        const firstTool = trace.commitToolUse(firstRequest, "hot_execution", "Read");
        const secondTool = trace.commitToolUse(firstRequest, "hot_execution", "Search");
        const thirdTool = trace.commitToolUse(firstRequest, "hot_execution", "Bash");
        const fourthTool = trace.commitToolUse(firstRequest, "hot_execution", "Write");
        trace.commitToolResult(firstTool);
        trace.commitInput();
        trace.commitInput("agent_mail");
        trace.endRequest({ modelRequestId: firstRequest });
        trace.commitToolResult(secondTool, "error");
        trace.commitToolResult(thirdTool);
        trace.commitToolResult(fourthTool);

        const secondRequest = trace.startRequest();
        const continuationTool = trace.commitToolUse(secondRequest, "hot_execution", "Read");
        trace.endRequest({ modelRequestId: secondRequest });
        trace.commitToolResult(continuationTool);

        const finalRequest = trace.startRequest();
        trace.endRequest({ modelRequestId: finalRequest });
        trace.finishIdle({ type: "end_turn" });
        expect(trace.coldRouteDifferenceCount).toBeGreaterThan(0);
        expect(trace.coldResumeActionCount).toBeGreaterThan(0);
    });

    test("every durable prefix converges through approval and reviewer routes", () => {
        const approval = new DurableTurnPrefixHarness("approval");
        approval.openRun();
        approval.commitInput();
        const approvalRequest = approval.startRequest();
        const allowed = approval.commitToolUse(approvalRequest, "requires_user_action", "Write");
        const denied = approval.commitToolUse(approvalRequest, "requires_user_action", "Bash");
        approval.endRequest({ modelRequestId: approvalRequest });
        approval.finishIdle({
            type: "requires_action",
            eventIds: [allowed.toolUseEventId, denied.toolUseEventId],
        });
        approval.openRun();
        approval.commitToolResult(allowed);
        approval.commitToolResult(denied, "error");
        const continuationRequest = approval.startRequest();
        approval.endRequest({ modelRequestId: continuationRequest });
        approval.finishIdle({ type: "end_turn" });

        const reviewer = new DurableTurnPrefixHarness("reviewer");
        reviewer.openRun();
        reviewer.commitInput("approval_review");
        const reviewerRequest = reviewer.startRequest("approval_reviewer");
        reviewer.endRequest({ modelRequestId: reviewerRequest });
        reviewer.finishIdle({ type: "end_turn" });
    });

    test("every durable prefix converges through compaction and provider reschedule", () => {
        const compaction = new DurableTurnPrefixHarness("compaction");
        compaction.openRun();
        compaction.commitInput();
        const compactionRequest = compaction.startRequest("compaction_summary");
        compaction.commitInput();
        compaction.commitInput("agent_mail");
        compaction.endRequest({ modelRequestId: compactionRequest });
        const requestAfterCompaction = compaction.startRequest();
        compaction.endRequest({ modelRequestId: requestAfterCompaction });
        compaction.finishIdle({ type: "end_turn" });

        const retry = new DurableTurnPrefixHarness("reschedule");
        retry.openRun();
        retry.commitInput();
        const failedRequest = retry.startRequest();
        retry.endRequest({
            modelRequestId: failedRequest,
            isError: true,
            errorKind: "provider_stream_error",
            rescheduled: true,
        });
        retry.markRescheduled();
        const retriedRequest = retry.startRequest();
        retry.endRequest({ modelRequestId: retriedRequest });
        retry.finishIdle({ type: "end_turn" });
    });

    test("every durable prefix converges through interrupt races", () => {
        const beforeSeal = new DurableTurnPrefixHarness("interrupt_before_seal");
        beforeSeal.openRun();
        beforeSeal.commitInput();
        const openRequest = beforeSeal.startRequest();
        beforeSeal.commitToolUse(openRequest);
        beforeSeal.interrupt();

        const afterSeal = new DurableTurnPrefixHarness("interrupt_after_seal");
        afterSeal.openRun();
        afterSeal.commitInput();
        const sealedRequest = afterSeal.startRequest();
        afterSeal.commitToolUse(sealedRequest);
        afterSeal.endRequest({ modelRequestId: sealedRequest });
        afterSeal.interrupt();

        const afterLastResult = new DurableTurnPrefixHarness("interrupt_after_last_result");
        afterLastResult.openRun();
        afterLastResult.commitInput();
        const completedRequest = afterLastResult.startRequest();
        const completedTool = afterLastResult.commitToolUse(completedRequest);
        afterLastResult.endRequest({ modelRequestId: completedRequest });
        afterLastResult.commitToolResult(completedTool);
        afterLastResult.interrupt();

        const beforeLastResult = new DurableTurnPrefixHarness("interrupt_before_last_result");
        beforeLastResult.openRun();
        beforeLastResult.commitInput();
        const interruptedRequest = beforeLastResult.startRequest();
        const interruptedTool = beforeLastResult.commitToolUse(interruptedRequest);
        beforeLastResult.endRequest({ modelRequestId: interruptedRequest });
        beforeLastResult.interrupt();
        beforeLastResult.commitToolResult(interruptedTool, "cancelled");
    });

    test("every durable prefix converges through terminal and exhausted closeout", () => {
        const podLoss = new DurableTurnPrefixHarness("pod_loss");
        podLoss.openRun();
        podLoss.commitInput();
        const lostRequest = podLoss.startRequest();
        podLoss.commitToolUse(lostRequest);
        podLoss.terminate();

        const exhausted = new DurableTurnPrefixHarness("retry_exhaustion");
        exhausted.openRun();
        exhausted.commitInput();
        const failedRequest = exhausted.startRequest();
        exhausted.exhaustRetries(failedRequest);
    });

    test("hot reduction and cold extraction converge across a streamed tool request prefix", () => {
        const user = durableTextMessage({
            id: "message_equivalence_input",
            sequence: 1,
            eventId: "event_equivalence_input",
            eventSequence: 2,
            role: "user",
        });
        const openTool = DurableRuntimeMessageSchema.parse({
            ...durableOpenToolMessage(),
            id: "message_equivalence_tool",
            owningEventId: "event_equivalence_tool",
            parts: durableOpenToolMessage().parts.map((part) => ({
                ...part,
                id: "message_equivalence_tool_part",
                messageId: "message_equivalence_tool",
                toolCallId: "call_equivalence",
                toolUseEventId: "event_equivalence_tool",
            })),
        });
        const completedTool = DurableRuntimeMessageSchema.parse({
            ...openTool,
            status: "completed",
            owningEventId: "event_equivalence_result",
            eventSequence: 6,
            parts: openTool.parts.map((part) => ({
                ...part,
                state: {
                    status: "completed" as const,
                    input: { value: {}, preview: "{}", truncated: false },
                    output: { text: "done", truncated: false },
                },
                completedAt: createdAt,
            })),
        });
        const events = [
            { eventId: "event_equivalence_running", eventSequence: 1, type: "session.status_running" as const },
            {
                eventId: "event_equivalence_start",
                eventSequence: 3,
                type: "span.model_request_start" as const,
                modelRequestId: "request_equivalence",
                requestStart: { requestKind: "agent_provider_request" as const, contextThroughMessageSequence: 1 },
            },
            {
                eventId: "event_equivalence_tool",
                eventSequence: 4,
                type: "agent.tool_use" as const,
                modelRequestId: "request_equivalence",
                toolUse: { modelToolCallId: "call_equivalence", toolName: "Read" },
            },
            {
                eventId: "event_equivalence_end",
                eventSequence: 5,
                type: "span.model_request_end" as const,
                modelRequestId: "request_equivalence",
                requestEnd: { requestStartEventId: "event_equivalence_start", isError: false, rescheduled: false },
            },
            {
                eventId: "event_equivalence_result",
                eventSequence: 6,
                type: "agent.tool_result" as const,
                modelRequestId: "request_equivalence",
                toolResult: { toolUseEventId: "event_equivalence_tool", outcome: "success" as const },
            },
        ];
        const inputLineage = {
            messageId: user.id,
            messageSequence: user.sequence,
            owningEventId: user.owningEventId,
            entries: [{
                lineageKind: "declaration_receipt" as const,
                operationKind: "commit_inputs",
                sourceKind: "messages",
                sourceId: "input_equivalence",
                eventId: user.owningEventId,
                eventSequence: user.eventSequence,
                disposition: "created" as const,
            }],
        };
        const toolUseLineage = {
            lineageKind: "declaration_receipt" as const,
            operationKind: "write_event",
            sourceKind: "agent.tool_use",
            sourceId: "write_equivalence_tool",
            eventId: "event_equivalence_tool",
            eventSequence: 4,
            disposition: "created" as const,
        };
        const resultLineage = {
            lineageKind: "declaration_receipt" as const,
            operationKind: "write_event",
            sourceKind: "agent.tool_result",
            sourceId: "write_equivalence_result",
            eventId: "event_equivalence_result",
            eventSequence: 6,
            disposition: "updated" as const,
        };

        let hot = initializeThreadTurnReduction({ pendingInputMessageIds: [] }, { routes: [] });
        hot = reduceThreadTurn(hot, { fact: "run_opened", eventId: "event_equivalence_running" }, { routes: [] });
        hot = reduceThreadTurn(hot, {
            fact: "inputs_committed",
            eventId: "event_equivalence_input",
            messageIds: [user.id],
        }, { routes: [] });

        const prefixes = [
            { eventCount: 1, messages: [user], lineage: [inputLineage], routeView: { routes: [] } },
            { eventCount: 2, messages: [user], lineage: [inputLineage], routeView: { routes: [] } },
            {
                eventCount: 3,
                messages: [user, openTool],
                lineage: [inputLineage, {
                    messageId: openTool.id,
                    messageSequence: openTool.sequence,
                    owningEventId: openTool.owningEventId,
                    modelRequestId: "request_equivalence",
                    entries: [toolUseLineage],
                }],
                routeView: { routes: [{ toolUseEventId: "event_equivalence_tool", disposition: "hot_execution" as const }] },
            },
            {
                eventCount: 4,
                messages: [user, openTool],
                lineage: [inputLineage, {
                    messageId: openTool.id,
                    messageSequence: openTool.sequence,
                    owningEventId: openTool.owningEventId,
                    modelRequestId: "request_equivalence",
                    entries: [toolUseLineage],
                }],
                routeView: { routes: [{ toolUseEventId: "event_equivalence_tool", disposition: "hot_execution" as const }] },
            },
            {
                eventCount: 5,
                messages: [user, completedTool],
                lineage: [inputLineage, {
                    messageId: completedTool.id,
                    messageSequence: completedTool.sequence,
                    owningEventId: completedTool.owningEventId,
                    modelRequestId: "request_equivalence",
                    entries: [toolUseLineage, resultLineage],
                }],
                routeView: { routes: [] },
            },
        ];
        const hotFacts = [
            {
                fact: "request_started" as const,
                eventId: "event_equivalence_start",
                modelRequestId: "request_equivalence",
                requestKind: "agent_provider_request" as const,
                contextThroughMessageSequence: 1,
                consumedInputMessageIds: [user.id],
            },
            {
                fact: "tool_use_committed" as const,
                eventId: "event_equivalence_tool",
                modelRequestId: "request_equivalence",
                modelToolCallId: "call_equivalence",
                toolName: "Read",
            },
            {
                fact: "request_ended" as const,
                eventId: "event_equivalence_end",
                modelRequestId: "request_equivalence",
                isError: false,
                rescheduled: false,
            },
            {
                fact: "tool_result_committed" as const,
                eventId: "event_equivalence_result",
                toolUseEventId: "event_equivalence_tool",
                outcome: "success" as const,
            },
        ];
        const expectedEdgeActions = [
            "prepare_next_request",
            "start_provider_request",
            "dispatch_tool_use",
            "reconcile_request_seal",
            "prepare_next_request",
        ] as const;
        const expectedColdActions = [
            "prepare_next_request",
            "await_request_end",
            "await_request_end",
            "await_tool_results",
            "prepare_next_request",
        ] as const;

        for (const [index, prefix] of prefixes.entries()) {
            if (index > 0) {
                hot = reduceThreadTurn(hot, hotFacts[index - 1]!, prefix.routeView);
            }
            expect(hot.action.action).toBe(expectedEdgeActions[index]!);
            if (hot.action.action === "start_provider_request" || hot.action.action === "dispatch_tool_use") {
                hot = consumeThreadTurnEdge(hot, prefix.routeView);
            }
            if (hot.action.action === "reconcile_request_seal") {
                hot = reconcileThreadTurnSeal(hot, prefix.routeView);
            }
            const coldCheckpoint = extractThreadTurnCheckpoint({
                messages: prefix.messages,
                facts: ThreadTurnLoadFactsSchema.parse({
                    events: events.slice(0, prefix.eventCount),
                    messageLineage: prefix.lineage,
                }),
            });
            expect(coldCheckpoint).toEqual(hot.checkpoint);
            const cold = initializeThreadTurnReduction(coldCheckpoint, prefix.routeView);
            expect(cold.state).toEqual(hot.state);
            expect(cold.action.action).toBe(expectedColdActions[index]!);
            expect(cold.action).toEqual(hot.action);
        }
    });

    test("hot and cold checkpoints converge across ordinary and special closeout families", () => {
        const user = durableTextMessage({
            id: "message_family_input",
            sequence: 1,
            eventId: "event_family_input",
            eventSequence: 2,
            role: "user",
        });
        const inputLineage = {
            messageId: user.id,
            messageSequence: user.sequence,
            owningEventId: user.owningEventId,
            entries: [{
                lineageKind: "declaration_receipt" as const,
                operationKind: "commit_inputs",
                sourceKind: "messages",
                sourceId: "input_family",
                eventId: user.owningEventId,
                eventSequence: user.eventSequence,
                disposition: "created" as const,
            }],
        };
        const started = (requestKind: "agent_provider_request" | "compaction_summary" | "approval_reviewer") => {
            let hot = initializeThreadTurnReduction({ pendingInputMessageIds: [] }, { routes: [] });
            hot = reduceThreadTurn(hot, { fact: "run_opened", eventId: "event_family_running" }, { routes: [] });
            hot = reduceThreadTurn(hot, {
                fact: "inputs_committed",
                eventId: user.owningEventId,
                messageIds: [user.id],
            }, { routes: [] });
            return consumeThreadTurnEdge(reduceThreadTurn(hot, {
                fact: "request_started",
                eventId: "event_family_start",
                modelRequestId: "request_family",
                requestKind,
                contextThroughMessageSequence: 1,
                consumedInputMessageIds: [user.id],
            }, { routes: [] }), { routes: [] });
        };
        const baseEvents = [
            { eventId: "event_family_running", eventSequence: 1, type: "session.status_running" as const },
            {
                eventId: "event_family_start",
                eventSequence: 3,
                type: "span.model_request_start" as const,
                modelRequestId: "request_family",
                requestStart: { requestKind: "agent_provider_request" as const, contextThroughMessageSequence: 1 },
            },
        ];
        const compare = (input: {
            readonly messages: readonly ReturnType<typeof durableTextMessage>[];
            readonly events: ReturnType<typeof ThreadTurnLoadFactsSchema.parse>["events"];
            readonly lineage: ReturnType<typeof ThreadTurnLoadFactsSchema.parse>["messageLineage"];
            readonly hot: ReturnType<typeof initializeThreadTurnReduction>;
            readonly routeView?: Parameters<typeof initializeThreadTurnReduction>[1];
        }) => {
            const routeView = input.routeView ?? { routes: [] };
            const cold = extractThreadTurnCheckpoint({
                messages: input.messages,
                facts: ThreadTurnLoadFactsSchema.parse({ events: input.events, messageLineage: input.lineage }),
            });
            expect(cold).toEqual(input.hot.checkpoint);
            expect(initializeThreadTurnReduction(cold, routeView).state).toEqual(input.hot.state);
            expect(initializeThreadTurnReduction(cold, routeView).action).toEqual(input.hot.action);
        };

        let ordinary = reduceThreadTurn(started("agent_provider_request"), {
            fact: "request_ended",
            eventId: "event_family_end",
            modelRequestId: "request_family",
            isError: false,
            rescheduled: false,
        }, { routes: [] });
        ordinary = reconcileThreadTurnSeal(ordinary, { routes: [] });
        ordinary = reduceThreadTurn(ordinary, {
            fact: "finish_idle_committed",
            eventId: "event_family_idle",
            stopReason: { type: "end_turn" },
        }, { routes: [] });
        compare({
            messages: [user],
            events: [
                ...baseEvents,
                {
                    eventId: "event_family_end",
                    eventSequence: 4,
                    type: "span.model_request_end",
                    modelRequestId: "request_family",
                    requestEnd: { requestStartEventId: "event_family_start", isError: false, rescheduled: false },
                },
                { eventId: "event_family_idle", eventSequence: 5, type: "session.status_idle", idle: { stopReason: "end_turn" } },
            ],
            lineage: [inputLineage],
            hot: ordinary,
        });

        let compaction = reduceThreadTurn(started("compaction_summary"), {
            fact: "request_ended",
            eventId: "event_family_end",
            modelRequestId: "request_family",
            isError: false,
            rescheduled: false,
        }, { routes: [] });
        compaction = reconcileThreadTurnSeal(compaction, { routes: [] });
        compare({
            messages: [user],
            events: [
                baseEvents[0]!,
                { ...baseEvents[1]!, requestStart: { requestKind: "compaction_summary", contextThroughMessageSequence: 1 } },
                {
                    eventId: "event_family_end",
                    eventSequence: 4,
                    type: "span.model_request_end",
                    modelRequestId: "request_family",
                    requestEnd: { requestStartEventId: "event_family_start", isError: false, rescheduled: false },
                },
            ],
            lineage: [inputLineage],
            hot: compaction,
        });

        const approvalTool = DurableRuntimeMessageSchema.parse({
            ...durableOpenToolMessage(),
            id: "message_family_tool",
            owningEventId: "event_family_tool",
            parts: durableOpenToolMessage().parts.map((part) => ({
                ...part,
                id: "message_family_tool_part",
                messageId: "message_family_tool",
                toolCallId: "call_family_tool",
                toolUseEventId: "event_family_tool",
            })),
        });
        const approvalRoute = {
            routes: [{ toolUseEventId: "event_family_tool", disposition: "requires_user_action" as const }],
        };
        let approval = reduceThreadTurn(started("agent_provider_request"), {
            fact: "tool_use_committed",
            eventId: "event_family_tool",
            modelRequestId: "request_family",
            modelToolCallId: "call_family_tool",
            toolName: "Read",
        }, approvalRoute);
        approval = consumeThreadTurnEdge(approval, approvalRoute);
        approval = reduceThreadTurn(approval, {
            fact: "request_ended",
            eventId: "event_family_end",
            modelRequestId: "request_family",
            isError: false,
            rescheduled: false,
        }, approvalRoute);
        approval = reconcileThreadTurnSeal(approval, approvalRoute);
        approval = reduceThreadTurn(approval, {
            fact: "finish_idle_committed",
            eventId: "event_family_idle",
            stopReason: { type: "requires_action", eventIds: ["event_family_tool"] },
        }, approvalRoute);
        compare({
            messages: [user, approvalTool],
            events: [
                ...baseEvents,
                {
                    eventId: "event_family_tool",
                    eventSequence: 4,
                    type: "agent.tool_use",
                    modelRequestId: "request_family",
                    toolUse: { modelToolCallId: "call_family_tool", toolName: "Read" },
                },
                {
                    eventId: "event_family_end",
                    eventSequence: 5,
                    type: "span.model_request_end",
                    modelRequestId: "request_family",
                    requestEnd: { requestStartEventId: "event_family_start", isError: false, rescheduled: false },
                },
                { eventId: "event_family_idle", eventSequence: 6, type: "session.status_idle", idle: { stopReason: "requires_action" } },
            ],
            lineage: [inputLineage, {
                messageId: approvalTool.id,
                messageSequence: approvalTool.sequence,
                owningEventId: approvalTool.owningEventId,
                modelRequestId: "request_family",
                entries: [{
                    lineageKind: "declaration_receipt",
                    operationKind: "write_event",
                    sourceKind: "agent.tool_use",
                    sourceId: "write_family_tool",
                    eventId: "event_family_tool",
                    eventSequence: 4,
                    disposition: "created",
                }],
            }],
            hot: approval,
            routeView: approvalRoute,
        });

        let reviewer = reduceThreadTurn(started("approval_reviewer"), {
            fact: "request_ended",
            eventId: "event_family_end",
            modelRequestId: "request_family",
            isError: false,
            rescheduled: false,
        }, { routes: [] });
        reviewer = reconcileThreadTurnSeal(reviewer, { routes: [] });
        compare({
            messages: [user],
            events: [
                baseEvents[0]!,
                { ...baseEvents[1]!, requestStart: { requestKind: "approval_reviewer", contextThroughMessageSequence: 1 } },
                {
                    eventId: "event_family_end",
                    eventSequence: 4,
                    type: "span.model_request_end",
                    modelRequestId: "request_family",
                    requestEnd: { requestStartEventId: "event_family_start", isError: false, rescheduled: false },
                },
            ],
            lineage: [inputLineage],
            hot: reviewer,
        });

        let retry = reduceThreadTurn(started("agent_provider_request"), {
            fact: "request_ended",
            eventId: "event_family_end",
            modelRequestId: "request_family",
            isError: true,
            errorKind: "provider_stream_error",
            rescheduled: true,
        }, { routes: [] });
        retry = reconcileThreadTurnSeal(retry, { routes: [] });
        compare({
            messages: [user],
            events: [
                ...baseEvents,
                {
                    eventId: "event_family_end",
                    eventSequence: 4,
                    type: "span.model_request_end",
                    modelRequestId: "request_family",
                    requestEnd: {
                        requestStartEventId: "event_family_start",
                        isError: true,
                        errorKind: "provider_stream_error",
                        rescheduled: true,
                    },
                },
            ],
            lineage: [inputLineage],
            hot: retry,
        });

        const interrupted = reduceThreadTurn(started("agent_provider_request"), {
            fact: "interrupt_committed",
            eventId: "event_family_interrupt",
        }, { routes: [] });
        compare({
            messages: [user],
            events: [
                ...baseEvents,
                { eventId: "event_family_interrupt", eventSequence: 4, type: "user.interrupt" },
            ],
            lineage: [inputLineage],
            hot: interrupted,
        });

        const terminated = reduceThreadTurn(started("agent_provider_request"), {
            fact: "terminal_closeout_committed",
            eventId: "event_family_terminated",
            failureEventId: "event_family_failure",
            disposition: "terminated",
        }, { routes: [] });
        compare({
            messages: [user],
            events: [
                ...baseEvents,
                {
                    eventId: "event_family_failure",
                    eventSequence: 4,
                    type: "session.error",
                    failure: { errorType: "runtime_pod_lost", retryStatus: "terminal" },
                },
                { eventId: "event_family_terminated", eventSequence: 5, type: "session.status_terminated" },
            ],
            lineage: [inputLineage],
            hot: terminated,
        });

        let exhausted = reduceThreadTurn(started("agent_provider_request"), {
            fact: "request_ended",
            eventId: "event_family_end",
            modelRequestId: "request_family",
            isError: true,
            errorKind: "provider_stream_error",
            rescheduled: false,
        }, { routes: [] });
        exhausted = reduceThreadTurn(exhausted, {
            fact: "finish_idle_committed",
            eventId: "event_family_idle",
            stopReason: { type: "retries_exhausted", failureEventId: "event_family_failure" },
        }, { routes: [] });
        compare({
            messages: [user],
            events: [
                ...baseEvents,
                {
                    eventId: "event_family_end",
                    eventSequence: 4,
                    type: "span.model_request_end",
                    modelRequestId: "request_family",
                    requestEnd: {
                        requestStartEventId: "event_family_start",
                        isError: true,
                        errorKind: "provider_stream_error",
                        rescheduled: false,
                    },
                },
                {
                    eventId: "event_family_failure",
                    eventSequence: 5,
                    type: "session.error",
                    failure: { errorType: "provider_stream_error", retryStatus: "exhausted" },
                },
                { eventId: "event_family_idle", eventSequence: 6, type: "session.status_idle", idle: { stopReason: "retries_exhausted" } },
            ],
            lineage: [inputLineage],
            hot: exhausted,
        });
    });

    test("hot and cold checkpoints converge when an approval run settles one sibling and accepts new input", () => {
        const initialInput = durableTextMessage({
            id: "message_approval_initial",
            sequence: 1,
            eventId: "event_approval_initial",
            eventSequence: 2,
            role: "user",
        });
        const resumedInput = durableTextMessage({
            id: "message_approval_resumed",
            sequence: 3,
            eventId: "event_approval_resumed",
            eventSequence: 10,
            role: "user",
        });
        const basePart = durableToolMessage().parts[0];
        const toolMessage = DurableRuntimeMessageSchema.parse({
            ...durableToolMessage(),
            id: "message_approval_tools",
            sequence: 2,
            status: "streaming",
            owningEventId: "event_approval_result_1",
            eventSequence: 9,
            parts: [
                {
                    ...basePart,
                    id: "message_approval_tool_1",
                    messageId: "message_approval_tools",
                    toolCallId: "call_approval_1",
                    toolUseEventId: "event_approval_tool_1",
                },
                {
                    ...basePart,
                    id: "message_approval_tool_2",
                    messageId: "message_approval_tools",
                    sequence: 1,
                    toolCallId: "call_approval_2",
                    toolUseEventId: "event_approval_tool_2",
                    state: { status: "pending" },
                    completedAt: undefined,
                },
            ],
        });
        const remainingRoute = {
            routes: [{
                toolUseEventId: "event_approval_tool_2",
                disposition: "requires_user_action" as const,
            }],
        };

        let hot = initializeThreadTurnReduction({ pendingInputMessageIds: [] }, { routes: [] });
        hot = reduceThreadTurn(hot, { fact: "run_opened", eventId: "event_approval_running_1" }, { routes: [] });
        hot = reduceThreadTurn(hot, {
            fact: "inputs_committed",
            eventId: initialInput.owningEventId,
            messageIds: [initialInput.id],
        }, { routes: [] });
        hot = reduceThreadTurn(hot, {
            fact: "request_started",
            eventId: "event_approval_start",
            modelRequestId: "request_approval",
            requestKind: "agent_provider_request",
            contextThroughMessageSequence: 1,
            consumedInputMessageIds: [initialInput.id],
        }, { routes: [] });
        hot = consumeThreadTurnEdge(hot, { routes: [] });
        for (const index of [1, 2]) {
            hot = reduceThreadTurn(hot, {
                fact: "tool_use_committed",
                eventId: `event_approval_tool_${index}`,
                modelRequestId: "request_approval",
                modelToolCallId: `call_approval_${index}`,
                toolName: "Read",
            }, {
                routes: Array.from({ length: index }, (_, routeIndex) => ({
                    toolUseEventId: `event_approval_tool_${routeIndex + 1}`,
                    disposition: "requires_user_action" as const,
                })),
            });
            hot = consumeThreadTurnEdge(hot, {
                routes: Array.from({ length: index }, (_, routeIndex) => ({
                    toolUseEventId: `event_approval_tool_${routeIndex + 1}`,
                    disposition: "requires_user_action" as const,
                })),
            });
        }
        const bothRoutes = {
            routes: [1, 2].map((index) => ({
                toolUseEventId: `event_approval_tool_${index}`,
                disposition: "requires_user_action" as const,
            })),
        };
        hot = reduceThreadTurn(hot, {
            fact: "request_ended",
            eventId: "event_approval_end",
            modelRequestId: "request_approval",
            isError: false,
            rescheduled: false,
        }, bothRoutes);
        hot = reconcileThreadTurnSeal(hot, bothRoutes);
        hot = reduceThreadTurn(hot, {
            fact: "finish_idle_committed",
            eventId: "event_approval_idle",
            stopReason: {
                type: "requires_action",
                eventIds: ["event_approval_tool_1", "event_approval_tool_2"],
            },
        }, bothRoutes);
        hot = reduceThreadTurn(hot, { fact: "run_opened", eventId: "event_approval_running_2" }, bothRoutes);
        hot = reduceThreadTurn(hot, {
            fact: "tool_result_committed",
            eventId: "event_approval_result_1",
            toolUseEventId: "event_approval_tool_1",
            outcome: "success",
        }, remainingRoute);
        hot = reduceThreadTurn(hot, {
            fact: "inputs_committed",
            eventId: resumedInput.owningEventId,
            messageIds: [resumedInput.id],
        }, remainingRoute);

        const events = ThreadTurnLoadFactsSchema.parse({
            events: [
                { eventId: "event_approval_running_1", eventSequence: 1, type: "session.status_running" },
                {
                    eventId: "event_approval_start",
                    eventSequence: 3,
                    type: "span.model_request_start",
                    modelRequestId: "request_approval",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 1 },
                },
                {
                    eventId: "event_approval_tool_1",
                    eventSequence: 4,
                    type: "agent.tool_use",
                    modelRequestId: "request_approval",
                    toolUse: { modelToolCallId: "call_approval_1", toolName: "Read" },
                },
                {
                    eventId: "event_approval_tool_2",
                    eventSequence: 5,
                    type: "agent.tool_use",
                    modelRequestId: "request_approval",
                    toolUse: { modelToolCallId: "call_approval_2", toolName: "Read" },
                },
                {
                    eventId: "event_approval_end",
                    eventSequence: 6,
                    type: "span.model_request_end",
                    modelRequestId: "request_approval",
                    requestEnd: { requestStartEventId: "event_approval_start", isError: false, rescheduled: false },
                },
                {
                    eventId: "event_approval_idle",
                    eventSequence: 7,
                    type: "session.status_idle",
                    idle: { stopReason: "requires_action" },
                },
                { eventId: "event_approval_running_2", eventSequence: 8, type: "session.status_running" },
                {
                    eventId: "event_approval_result_1",
                    eventSequence: 9,
                    type: "agent.tool_result",
                    modelRequestId: "request_approval",
                    toolResult: { toolUseEventId: "event_approval_tool_1", outcome: "success" },
                },
            ],
            messageLineage: [
                {
                    messageId: initialInput.id,
                    messageSequence: initialInput.sequence,
                    owningEventId: initialInput.owningEventId,
                    entries: [{
                        lineageKind: "declaration_receipt",
                        operationKind: "commit_inputs",
                        sourceKind: "messages",
                        sourceId: "input_approval_initial",
                        eventId: initialInput.owningEventId,
                        eventSequence: initialInput.eventSequence,
                        disposition: "created",
                    }],
                },
                {
                    messageId: toolMessage.id,
                    messageSequence: toolMessage.sequence,
                    owningEventId: toolMessage.owningEventId,
                    modelRequestId: "request_approval",
                    entries: [
                        {
                            lineageKind: "declaration_receipt",
                            operationKind: "write_event",
                            sourceKind: "agent.tool_use",
                            sourceId: "write_approval_tool_1",
                            eventId: "event_approval_tool_1",
                            eventSequence: 4,
                            disposition: "created",
                        },
                        {
                            lineageKind: "declaration_receipt",
                            operationKind: "write_event",
                            sourceKind: "agent.tool_use",
                            sourceId: "write_approval_tool_2",
                            eventId: "event_approval_tool_2",
                            eventSequence: 5,
                            disposition: "updated",
                        },
                        {
                            lineageKind: "declaration_receipt",
                            operationKind: "write_event",
                            sourceKind: "agent.tool_result",
                            sourceId: "write_approval_result_1",
                            eventId: "event_approval_result_1",
                            eventSequence: 9,
                            disposition: "updated",
                        },
                    ],
                },
                {
                    messageId: resumedInput.id,
                    messageSequence: resumedInput.sequence,
                    owningEventId: resumedInput.owningEventId,
                    entries: [{
                        lineageKind: "declaration_receipt",
                        operationKind: "commit_inputs",
                        sourceKind: "messages",
                        sourceId: "input_approval_resumed",
                        eventId: resumedInput.owningEventId,
                        eventSequence: resumedInput.eventSequence,
                        disposition: "created",
                    }],
                },
            ],
        });
        const coldCheckpoint = extractThreadTurnCheckpoint({
            messages: [initialInput, toolMessage, resumedInput],
            facts: events,
        });
        expect(coldCheckpoint).toEqual(hot.checkpoint);
        expect(coldCheckpoint).toMatchObject({
            executionRunId: "event_approval_running_2",
            pendingInputMessageIds: [resumedInput.id],
            request: {
                toolMembers: [
                    { toolUseEventId: "event_approval_tool_1", terminalResult: { resultEventId: "event_approval_result_1" } },
                    { toolUseEventId: "event_approval_tool_2" },
                ],
            },
        });
        expect(initializeThreadTurnReduction(coldCheckpoint, remainingRoute)).toMatchObject({
            state: hot.state,
            action: hot.action,
        });
    });

    test("reconstructs sealed membership and only later durable input", () => {
        const messages = [
            durableTextMessage({
                id: "message_consumed",
                sequence: 1,
                eventId: "event_input_1",
                eventSequence: 1,
                role: "user",
            }),
            durableToolMessage(),
            durableTextMessage({
                id: "message_pending",
                sequence: 3,
                eventId: "event_input_2",
                eventSequence: 7,
                role: "user",
            }),
        ];
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_running",
                    eventSequence: 2,
                    type: "session.status_running",
                },
                {
                    eventId: "event_start",
                    eventSequence: 3,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: {
                        requestKind: "agent_provider_request",
                        contextThroughMessageSequence: 1,
                    },
                },
                {
                    eventId: "event_tool_use",
                    eventSequence: 4,
                    type: "agent.tool_use",
                    modelRequestId: "model_request_1",
                    toolUse: {
                        modelToolCallId: "tool_call_1",
                        toolName: "Read",
                    },
                },
                {
                    eventId: "event_end",
                    eventSequence: 5,
                    type: "span.model_request_end",
                    modelRequestId: "model_request_1",
                    requestEnd: {
                        requestStartEventId: "event_start",
                        isError: false,
                        rescheduled: false,
                    },
                },
                {
                    eventId: "event_tool_result",
                    eventSequence: 6,
                    type: "agent.tool_result",
                    modelRequestId: "model_request_1",
                    toolResult: {
                        toolUseEventId: "event_tool_use",
                        outcome: "success",
                    },
                },
            ],
            messageLineage: [
                {
                    messageId: "message_consumed",
                    messageSequence: 1,
                    owningEventId: "event_input_1",
                    entries: [{
                            lineageKind: "declaration_receipt",
                            operationKind: "commit_inputs",
                            sourceKind: "messages",
                            sourceId: "input_1",
                            eventId: "event_input_1",
                            eventSequence: 1,
                            disposition: "created",
                        }],
                },
                {
                    messageId: "message_assistant",
                    messageSequence: 2,
                    owningEventId: "event_tool_result",
                    modelRequestId: "model_request_1",
                    entries: [
                        {
                            lineageKind: "declaration_receipt",
                            operationKind: "write_event",
                            sourceKind: "agent.tool_use",
                            sourceId: "write_tool_use",
                            eventId: "event_tool_use",
                            eventSequence: 4,
                            disposition: "created",
                        },
                        {
                            lineageKind: "declaration_receipt",
                            operationKind: "write_event",
                            sourceKind: "agent.tool_result",
                            sourceId: "write_result",
                            eventId: "event_tool_result",
                            eventSequence: 6,
                            disposition: "updated",
                        },
                    ],
                },
                {
                    messageId: "message_pending",
                    messageSequence: 3,
                    owningEventId: "event_input_2",
                    entries: [{
                            lineageKind: "declaration_receipt",
                            operationKind: "commit_inputs",
                            sourceKind: "messages",
                            sourceId: "input_2",
                            eventId: "event_input_2",
                            eventSequence: 7,
                            disposition: "created",
                        }],
                },
            ],
        });
        expect(extractThreadTurnCheckpoint({ messages, facts })).toEqual({
            executionRunId: "event_running",
            pendingInputMessageIds: ["message_pending"],
            request: {
                modelRequestId: "model_request_1",
                requestStartEventId: "event_start",
                requestKind: "agent_provider_request",
                contextThroughMessageSequence: 1,
                requestEnd: {
                    eventId: "event_end",
                    isError: false,
                    rescheduled: false,
                },
                toolMembers: [{
                        memberKind: "public_tool_use",
                        modelToolCallId: "tool_call_1",
                        toolUseEventId: "event_tool_use",
                        toolName: "Read",
                        terminalResult: {
                            resultEventId: "event_tool_result",
                            outcome: "success",
                        },
                    }],
            },
        });
    });
    test("rejects post-seal members and unknown fact shapes", () => {
        expect(() => ThreadTurnLoadFactsSchema.parse({
            events: [{ eventId: "event_unknown", eventSequence: 1, type: "unknown.event" }],
            messageLineage: [],
        })).toThrow();
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 1,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_end",
                    eventSequence: 2,
                    type: "span.model_request_end",
                    modelRequestId: "model_request_1",
                    requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
                },
                {
                    eventId: "event_tool_use",
                    eventSequence: 3,
                    type: "agent.tool_use",
                    modelRequestId: "model_request_1",
                    toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
                },
            ],
            messageLineage: [],
        });
        expect(() => extractThreadTurnCheckpoint({ messages: [], facts })).toThrow("Tool Use cannot follow Request End");
    });
    test("fails closed when durable request and tool facts cannot form one checkpoint", () => {
        const endWithoutStart = ThreadTurnLoadFactsSchema.parse({
            events: [{
                eventId: "event_end",
                eventSequence: 1,
                type: "span.model_request_end",
                modelRequestId: "model_request_1",
                requestEnd: {
                    requestStartEventId: "event_missing_start",
                    isError: false,
                    rescheduled: false,
                },
            }],
            messageLineage: [],
        });
        expect(() => extractThreadTurnCheckpoint({
            messages: [],
            facts: endWithoutStart,
        })).toThrow("Request End has no matching Request Start");

        const resultWithoutUse = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 1,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: {
                        requestKind: "agent_provider_request",
                        contextThroughMessageSequence: 0,
                    },
                },
                {
                    eventId: "event_orphan_result",
                    eventSequence: 2,
                    type: "agent.tool_result",
                    modelRequestId: "model_request_1",
                    toolResult: {
                        toolUseEventId: "event_missing_tool_use",
                        outcome: "error",
                    },
                },
            ],
            messageLineage: [],
        });
        expect(() => extractThreadTurnCheckpoint({
            messages: [],
            facts: resultWithoutUse,
        })).toThrow("Tool Result has no matching Tool Use");

        const duplicateResultMessage = DurableRuntimeMessageSchema.parse({
            ...durableToolMessage(),
            owningEventId: "event_result_1",
            eventSequence: 4,
        });
        const duplicateResults = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 1,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: {
                        requestKind: "agent_provider_request",
                        contextThroughMessageSequence: 0,
                    },
                },
                {
                    eventId: "event_tool_use",
                    eventSequence: 2,
                    type: "agent.tool_use",
                    modelRequestId: "model_request_1",
                    toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
                },
                {
                    eventId: "event_end",
                    eventSequence: 3,
                    type: "span.model_request_end",
                    modelRequestId: "model_request_1",
                    requestEnd: {
                        requestStartEventId: "event_start",
                        isError: false,
                        rescheduled: false,
                    },
                },
                {
                    eventId: "event_result_1",
                    eventSequence: 4,
                    type: "agent.tool_result",
                    modelRequestId: "model_request_1",
                    toolResult: { toolUseEventId: "event_tool_use", outcome: "success" },
                },
                {
                    eventId: "event_result_2",
                    eventSequence: 5,
                    type: "agent.tool_result",
                    modelRequestId: "model_request_1",
                    toolResult: { toolUseEventId: "event_tool_use", outcome: "error" },
                },
            ],
            messageLineage: [{
                messageId: duplicateResultMessage.id,
                messageSequence: duplicateResultMessage.sequence,
                owningEventId: duplicateResultMessage.owningEventId,
                modelRequestId: "model_request_1",
                entries: [
                    {
                        lineageKind: "declaration_receipt",
                        operationKind: "write_event",
                        sourceKind: "agent.tool_use",
                        sourceId: "write_tool_use",
                        eventId: "event_tool_use",
                        eventSequence: 2,
                        disposition: "created",
                    },
                    {
                        lineageKind: "declaration_receipt",
                        operationKind: "write_event",
                        sourceKind: "agent.tool_result",
                        sourceId: "write_result_1",
                        eventId: "event_result_1",
                        eventSequence: 4,
                        disposition: "updated",
                    },
                ],
            }],
        });
        expect(() => extractThreadTurnCheckpoint({
            messages: [duplicateResultMessage],
            facts: duplicateResults,
        })).toThrow("Tool Use has duplicate terminal results");

        const publicMessage = DurableRuntimeMessageSchema.parse({
            ...durableOpenToolMessage(),
            sequence: 1,
            owningEventId: "event_tool_use",
            eventSequence: 2,
        });
        const repairMessage = DurableRuntimeMessageSchema.parse({
            ...durableInternalRepairMessage(),
            sequence: 2,
            owningEventId: "event_repair",
            eventSequence: 3,
            parts: durableInternalRepairMessage().parts.map((part) => ({
                ...part,
                toolCallId: "tool_call_1",
            })),
        });
        const duplicateCallId = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 1,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: {
                        requestKind: "agent_provider_request",
                        contextThroughMessageSequence: 0,
                    },
                },
                {
                    eventId: "event_tool_use",
                    eventSequence: 2,
                    type: "agent.tool_use",
                    modelRequestId: "model_request_1",
                    toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
                },
                {
                    eventId: "event_repair",
                    eventSequence: 3,
                    type: "agent.tool_result",
                    modelRequestId: "model_request_1",
                    toolResult: {
                        modelToolCallId: "tool_call_1",
                        toolName: "unknown_tool",
                        repairKind: "invalid_tool",
                        outcome: "error",
                    },
                },
                {
                    eventId: "event_end",
                    eventSequence: 4,
                    type: "span.model_request_end",
                    modelRequestId: "model_request_1",
                    requestEnd: {
                        requestStartEventId: "event_start",
                        isError: false,
                        rescheduled: false,
                    },
                },
            ],
            messageLineage: [
                {
                    messageId: publicMessage.id,
                    messageSequence: publicMessage.sequence,
                    owningEventId: publicMessage.owningEventId,
                    modelRequestId: "model_request_1",
                    entries: [{
                        lineageKind: "declaration_receipt",
                        operationKind: "write_event",
                        sourceKind: "agent.tool_use",
                        sourceId: "write_tool_use",
                        eventId: "event_tool_use",
                        eventSequence: 2,
                        disposition: "created",
                    }],
                },
                {
                    messageId: repairMessage.id,
                    messageSequence: repairMessage.sequence,
                    owningEventId: repairMessage.owningEventId,
                    modelRequestId: "model_request_1",
                    entries: [{
                        lineageKind: "declaration_receipt",
                        operationKind: "commit_internal_tool_repair",
                        sourceKind: "internal_tool_repair",
                        sourceId: "repair_1",
                        eventId: "event_repair",
                        eventSequence: 3,
                        disposition: "created",
                    }],
                },
            ],
        });
        expect(() => extractThreadTurnCheckpoint({
            messages: [publicMessage, repairMessage],
            facts: duplicateCallId,
        })).toThrow("modelToolCallId is duplicated within the request");
    });
    test("joins each cold non-terminal Tool Use to exactly one durable route", () => {
        const checkpoint = {
            pendingInputMessageIds: [],
            request: {
                modelRequestId: "model_request_1",
                requestStartEventId: "event_start",
                requestKind: "agent_provider_request" as const,
                contextThroughMessageSequence: 0,
                requestEnd: {
                    eventId: "event_end",
                    isError: false,
                    rescheduled: false,
                },
                toolMembers: [
                    {
                        memberKind: "public_tool_use" as const,
                        modelToolCallId: "tool_call_approval",
                        toolUseEventId: "event_tool_approval",
                        toolName: "Write",
                    },
                    {
                        memberKind: "public_tool_use" as const,
                        modelToolCallId: "tool_call_sandbox",
                        toolUseEventId: "event_tool_sandbox",
                        toolName: "Bash",
                    },
                ],
            },
        };
        const pendingToolUses = [{
                toolUseEventId: "event_tool_approval",
                modelRequestId: "model_request_1",
                modelToolCallId: "tool_call_approval",
                toolName: "Write",
                status: "pending" as const,
            }];
        const pendingSandboxExecutions = [{
                toolUseEventId: "event_tool_sandbox",
                modelRequestId: "model_request_1",
                modelToolCallId: "tool_call_sandbox",
                toolName: "Bash",
            }];
        expect(extractColdThreadToolRouteView({
            checkpoint,
            pendingToolUses,
            pendingSandboxExecutions,
        })).toEqual({
            routes: [
                { toolUseEventId: "event_tool_approval", disposition: "requires_user_action" },
                { toolUseEventId: "event_tool_sandbox", disposition: "resume_sandbox_execution" },
            ],
        });
        expect(() => extractColdThreadToolRouteView({
            checkpoint,
            pendingToolUses: [],
            pendingSandboxExecutions,
        })).toThrow("cold non-terminal Tool Use requires exactly one durable route");
        expect(() => extractColdThreadToolRouteView({
            checkpoint,
            pendingToolUses: [pendingToolUses[0]!, pendingToolUses[0]!],
            pendingSandboxExecutions,
        })).toThrow("cold non-terminal Tool Use requires exactly one durable route");
        expect(() => extractColdThreadToolRouteView({
            checkpoint,
            pendingToolUses,
            pendingSandboxExecutions: [
                pendingSandboxExecutions[0]!,
                { ...pendingSandboxExecutions[0]!, toolUseEventId: "event_tool_approval" },
            ],
        })).toThrow("cold Tool route identity does not match its request member");
    });
    test("interrupted cold recovery accepts only the complete pre-settlement or empty post-settlement route view", () => {
        const checkpoint = {
            pendingInputMessageIds: [],
            interruptEventId: "event_interrupt",
            request: {
                modelRequestId: "model_request_interrupted",
                requestStartEventId: "event_start_interrupted",
                requestKind: "agent_provider_request" as const,
                contextThroughMessageSequence: 0,
                requestEnd: {
                    eventId: "event_end_interrupted",
                    isError: false,
                    rescheduled: false,
                },
                toolMembers: [{
                    memberKind: "public_tool_use" as const,
                    modelToolCallId: "tool_call_interrupted",
                    toolUseEventId: "event_tool_interrupted",
                    toolName: "Write",
                }],
            },
        };

        const pendingToolUse = {
            toolUseEventId: "event_tool_interrupted",
            modelRequestId: "model_request_interrupted",
            modelToolCallId: "tool_call_interrupted",
            toolName: "Write",
            status: "pending" as const,
        };
        expect(extractColdThreadToolRouteView({
            checkpoint,
            pendingToolUses: [pendingToolUse],
            pendingSandboxExecutions: [],
        })).toEqual({
            routes: [{ toolUseEventId: "event_tool_interrupted", disposition: "requires_user_action" }],
        });
        const routeView = extractColdThreadToolRouteView({
            checkpoint,
            pendingToolUses: [],
            pendingSandboxExecutions: [],
        });
        expect(routeView).toEqual({ routes: [] });
        expect(initializeThreadTurnReduction(checkpoint, routeView)).toMatchObject({
            state: { state: "idle" },
            action: { action: "await_input" },
        });
    });
    test("rejects an unknown declaration lineage combination", () => {
        const message = durableTextMessage({
            id: "message_unknown",
            sequence: 1,
            eventId: "event_unknown",
            eventSequence: 1,
            role: "assistant",
        });
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [],
            messageLineage: [{
                    messageId: message.id,
                    messageSequence: message.sequence,
                    owningEventId: message.owningEventId,
                    entries: [{
                            lineageKind: "declaration_receipt",
                            operationKind: "invented_operation",
                            sourceKind: "invented_source",
                            sourceId: "invented_source_1",
                            eventId: message.owningEventId,
                            eventSequence: message.eventSequence,
                            disposition: "created",
                        }],
                }],
        });
        expect(() => extractThreadTurnCheckpoint({ messages: [message], facts })).toThrow("message declaration lineage is not recognized");
        const unknownWriteEvent = ThreadTurnLoadFactsSchema.parse({
            events: [],
            messageLineage: [{
                    messageId: message.id,
                    messageSequence: message.sequence,
                    owningEventId: message.owningEventId,
                    entries: [{
                            lineageKind: "declaration_receipt",
                            operationKind: "write_event",
                            sourceKind: "invented_future_event",
                            sourceId: "write_unknown_1",
                            eventId: message.owningEventId,
                            eventSequence: message.eventSequence,
                            disposition: "created",
                        }],
                }],
        });
        expect(() => extractThreadTurnCheckpoint({ messages: [message], facts: unknownWriteEvent })).toThrow("message declaration lineage is not recognized");
    });
    test("accepts every streaming Tool Use lineage entry on one assistant message", () => {
        const message = DurableRuntimeMessageSchema.parse({
            ...durableToolMessage(),
            owningEventId: "event_tool_use_2",
            eventSequence: 3,
            status: "streaming",
            parts: [
                {
                    ...durableToolMessage().parts[0],
                    state: { status: "pending" },
                },
                {
                    ...durableToolMessage().parts[0],
                    id: "message_assistant_tool_2",
                    sequence: 1,
                    toolCallId: "tool_call_2",
                    toolName: "Bash",
                    toolUseEventId: "event_tool_use_2",
                    state: { status: "pending" },
                },
            ],
        });
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 1,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_tool_use",
                    eventSequence: 2,
                    type: "agent.tool_use",
                    modelRequestId: "model_request_1",
                    toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
                },
                {
                    eventId: "event_tool_use_2",
                    eventSequence: 3,
                    type: "agent.tool_use",
                    modelRequestId: "model_request_1",
                    toolUse: { modelToolCallId: "tool_call_2", toolName: "Bash" },
                },
            ],
            messageLineage: [{
                    messageId: message.id,
                    messageSequence: message.sequence,
                    owningEventId: message.owningEventId,
                    modelRequestId: "model_request_1",
                    entries: [
                        {
                            lineageKind: "declaration_receipt",
                            operationKind: "write_event",
                            sourceKind: "agent.tool_use",
                            sourceId: "write_tool_use_1",
                            eventId: "event_tool_use",
                            eventSequence: 2,
                            disposition: "created",
                        },
                        {
                            lineageKind: "declaration_receipt",
                            operationKind: "write_event",
                            sourceKind: "agent.tool_use",
                            sourceId: "write_tool_use_2",
                            eventId: "event_tool_use_2",
                            eventSequence: 3,
                            disposition: "updated",
                        },
                    ],
                }],
        });
        expect(extractThreadTurnCheckpoint({ messages: [message], facts }).request?.toolMembers).toHaveLength(2);
    });
    test("reconstructs five sealed tools and continues only after the three outstanding results", () => {
        const basePart = durableToolMessage().parts[0];
        const message = DurableRuntimeMessageSchema.parse({
            ...durableToolMessage(),
            owningEventId: "event_result_2",
            eventSequence: 9,
            status: "streaming",
            parts: Array.from({ length: 5 }, (_, index) => ({
                ...basePart,
                id: `message_tool_${index + 1}`,
                sequence: index,
                toolCallId: `tool_call_${index + 1}`,
                toolUseEventId: `event_tool_${index + 1}`,
                state: index < 2
                    ? { status: "completed" as const, input: { value: {}, preview: "{}", truncated: false }, output: { text: "done", truncated: false } }
                    : { status: "running" as const, input: { value: {}, preview: "{}", truncated: false } },
                ...(index < 2 ? { completedAt: createdAt } : { completedAt: undefined }),
            })),
        });
        const events = [
            {
                eventId: "event_start",
                eventSequence: 1,
                type: "span.model_request_start" as const,
                modelRequestId: "model_request_1",
                requestStart: { requestKind: "agent_provider_request" as const, contextThroughMessageSequence: 0 },
            },
            ...Array.from({ length: 5 }, (_, index) => ({
                eventId: `event_tool_${index + 1}`,
                eventSequence: index + 2,
                type: "agent.tool_use" as const,
                modelRequestId: "model_request_1",
                toolUse: { modelToolCallId: `tool_call_${index + 1}`, toolName: "Read" },
            })),
            {
                eventId: "event_end",
                eventSequence: 7,
                type: "span.model_request_end" as const,
                modelRequestId: "model_request_1",
                requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
            },
            ...Array.from({ length: 2 }, (_, index) => ({
                eventId: `event_result_${index + 1}`,
                eventSequence: index + 8,
                type: "agent.tool_result" as const,
                modelRequestId: "model_request_1",
                toolResult: { toolUseEventId: `event_tool_${index + 1}`, outcome: "success" as const },
            })),
        ];
        const entries = [
            ...Array.from({ length: 5 }, (_, index) => ({
                lineageKind: "declaration_receipt" as const,
                operationKind: "write_event",
                sourceKind: "agent.tool_use",
                sourceId: `write_tool_${index + 1}`,
                eventId: `event_tool_${index + 1}`,
                eventSequence: index + 2,
                disposition: index === 0 ? "created" as const : "updated" as const,
            })),
            ...Array.from({ length: 2 }, (_, index) => ({
                lineageKind: "declaration_receipt" as const,
                operationKind: "write_event",
                sourceKind: "agent.tool_result",
                sourceId: `write_result_${index + 1}`,
                eventId: `event_result_${index + 1}`,
                eventSequence: index + 8,
                disposition: "updated" as const,
            })),
        ];
        const checkpoint = extractThreadTurnCheckpoint({
            messages: [message],
            facts: ThreadTurnLoadFactsSchema.parse({
                events,
                messageLineage: [{
                    messageId: message.id,
                    messageSequence: message.sequence,
                    owningEventId: message.owningEventId,
                    modelRequestId: "model_request_1",
                    entries,
                }],
            }),
        });
        expect(checkpoint.request?.toolMembers).toHaveLength(5);
        expect(checkpoint.request?.toolMembers.filter((member) =>
            member.memberKind === "public_tool_use" && member.terminalResult !== undefined
        )).toHaveLength(2);

        let hot = initializeThreadTurnReduction({ pendingInputMessageIds: ["message_trigger"] }, { routes: [] });
        hot = reduceThreadTurn(hot, {
            fact: "request_started",
            eventId: "event_start",
            modelRequestId: "model_request_1",
            requestKind: "agent_provider_request",
            contextThroughMessageSequence: 0,
            consumedInputMessageIds: ["message_trigger"],
        }, { routes: [] });
        hot = consumeThreadTurnEdge(hot, { routes: [] });
        for (const index of [1, 2, 3, 4, 5]) {
            hot = reduceThreadTurn(hot, {
                fact: "tool_use_committed",
                eventId: `event_tool_${index}`,
                modelRequestId: "model_request_1",
                modelToolCallId: `tool_call_${index}`,
                toolName: "Read",
            }, {
                routes: Array.from({ length: index }, (_, routeIndex) => ({
                    toolUseEventId: `event_tool_${routeIndex + 1}`,
                    disposition: "hot_execution" as const,
                })),
            });
            hot = consumeThreadTurnEdge(hot, {
                routes: Array.from({ length: index }, (_, routeIndex) => ({
                    toolUseEventId: `event_tool_${routeIndex + 1}`,
                    disposition: "hot_execution" as const,
                })),
            });
        }
        const allRoutes = {
            routes: [1, 2, 3, 4, 5].map((index) => ({
                toolUseEventId: `event_tool_${index}`,
                disposition: "hot_execution" as const,
            })),
        };
        hot = reduceThreadTurn(hot, {
            fact: "request_ended",
            eventId: "event_end",
            modelRequestId: "model_request_1",
            isError: false,
            rescheduled: false,
        }, allRoutes);
        hot = reconcileThreadTurnSeal(hot, allRoutes);
        for (const index of [1, 2]) {
            hot = reduceThreadTurn(hot, {
                fact: "tool_result_committed",
                eventId: `event_result_${index}`,
                toolUseEventId: `event_tool_${index}`,
                outcome: "success",
            }, {
                routes: Array.from({ length: 5 - index }, (_, offset) => ({
                    toolUseEventId: `event_tool_${index + offset + 1}`,
                    disposition: "hot_execution" as const,
                })),
            });
        }
        const outstandingRoutes = {
            routes: [3, 4, 5].map((index) => ({
                toolUseEventId: `event_tool_${index}`,
                disposition: "hot_execution" as const,
            })),
        };
        expect(checkpoint).toEqual(hot.checkpoint);
        expect(initializeThreadTurnReduction(checkpoint, outstandingRoutes)).toMatchObject({
            state: hot.state,
            action: hot.action,
        });

        let reduction = initializeThreadTurnReduction(checkpoint, {
            routes: [3, 4, 5].map((index) => ({
                toolUseEventId: `event_tool_${index}`,
                disposition: "hot_execution" as const,
            })),
        });
        for (const index of [3, 4]) {
            reduction = reduceThreadTurn(reduction, {
                fact: "tool_result_committed",
                eventId: `event_result_${index}`,
                toolUseEventId: `event_tool_${index}`,
                outcome: "success",
            }, {
                routes: Array.from({ length: 5 - index }, (_, offset) => ({
                    toolUseEventId: `event_tool_${index + offset + 1}`,
                    disposition: "hot_execution" as const,
                })),
            });
            expect(reduction.state.state).toBe("waiting_for_tool_results");
        }
        reduction = reduceThreadTurn(reduction, {
            fact: "tool_result_committed",
            eventId: "event_result_5",
            toolUseEventId: "event_tool_5",
            outcome: "success",
        }, { routes: [] });
        expect(reduction).toMatchObject({
            state: { state: "ready_to_request" },
            action: { action: "prepare_next_request" },
        });
    });
    test("rejects a Request Start boundary above the loaded durable message range", () => {
        const message = durableTextMessage({
            id: "message_input",
            sequence: 1,
            eventId: "event_input",
            eventSequence: 1,
            role: "user",
        });
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [{
                    eventId: "event_start",
                    eventSequence: 2,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 99 },
                }],
            messageLineage: [{
                    messageId: message.id,
                    messageSequence: message.sequence,
                    owningEventId: message.owningEventId,
                    entries: [{
                            lineageKind: "declaration_receipt",
                            operationKind: "commit_inputs",
                            sourceKind: "messages",
                            sourceId: "input_1",
                            eventId: message.owningEventId,
                            eventSequence: message.eventSequence,
                            disposition: "created",
                        }],
                }],
        });
        expect(() => extractThreadTurnCheckpoint({ messages: [message], facts })).toThrow("Request Start message boundary exceeds the loaded durable message range");
    });
  test("rejects a following Request Start while its predecessor is still open", () => {
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start_1",
                    eventSequence: 1,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_start_2",
                    eventSequence: 2,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_2",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
            ],
            messageLineage: [],
        });

        expect(() => extractThreadTurnCheckpoint({ messages: [], facts })).toThrow(
            "following Request Start requires its predecessor to be sealed",
        );
    });
    test("rejects a terminated closeout paired only with a retrying diagnostic", () => {
        expect(() => extractThreadTurnCheckpoint({
            messages: [],
            facts: {
                events: [
                    { eventId: "event_running", eventSequence: 1, type: "session.status_running" },
                    {
                        eventId: "event_retrying_failure",
                        eventSequence: 2,
                        type: "session.error",
                        failure: { errorType: "provider_unavailable", retryStatus: "retrying" },
                    },
                    { eventId: "event_terminated", eventSequence: 3, type: "session.status_terminated" },
                ],
                messageLineage: [],
            },
        })).toThrow("terminal closeout has no preceding failure fact");
    });
    test("rejects a Tool Use that precedes its Request Start", () => {
        const message = DurableRuntimeMessageSchema.parse({
            ...durableOpenToolMessage(),
            id: "message_early_tool",
            owningEventId: "event_early_tool",
            eventSequence: 1,
            parts: durableOpenToolMessage().parts.map((part) => ({
                ...part,
                id: "message_early_tool_part",
                messageId: "message_early_tool",
                toolCallId: "call_early_tool",
                toolUseEventId: "event_early_tool",
            })),
        });
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_early_tool",
                    eventSequence: 1,
                    type: "agent.tool_use",
                    modelRequestId: "request_early_tool",
                    toolUse: { modelToolCallId: "call_early_tool", toolName: "Read" },
                },
                {
                    eventId: "event_late_start",
                    eventSequence: 2,
                    type: "span.model_request_start",
                    modelRequestId: "request_early_tool",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_early_tool_end",
                    eventSequence: 3,
                    type: "span.model_request_end",
                    modelRequestId: "request_early_tool",
                    requestEnd: { requestStartEventId: "event_late_start", isError: false, rescheduled: false },
                },
            ],
            messageLineage: [{
                messageId: message.id,
                messageSequence: message.sequence,
                owningEventId: message.owningEventId,
                modelRequestId: "request_early_tool",
                entries: [{
                    lineageKind: "declaration_receipt",
                    operationKind: "write_event",
                    sourceKind: "agent.tool_use",
                    sourceId: "write_early_tool",
                    eventId: "event_early_tool",
                    eventSequence: 1,
                    disposition: "created",
                }],
            }],
        });

        expect(() => extractThreadTurnCheckpoint({ messages: [message], facts })).toThrow(
            "Tool Use cannot precede Request Start",
        );
    });
    test("requires Tool Use and internal repair facts to have matching declaration lineage", () => {
        const publicMessage = durableToolMessage();
        const publicFacts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 1,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_tool_use",
                    eventSequence: 4,
                    type: "agent.tool_use",
                    modelRequestId: "model_request_1",
                    toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
                },
            ],
            messageLineage: [{
                    messageId: publicMessage.id,
                    messageSequence: publicMessage.sequence,
                    owningEventId: publicMessage.owningEventId,
                    modelRequestId: "model_request_1",
                    entries: [{
                            lineageKind: "declaration_receipt",
                            operationKind: "commit_inputs",
                            sourceKind: "messages",
                            sourceId: "input_wrong_owner",
                            eventId: "event_tool_use",
                            eventSequence: 4,
                            disposition: "created",
                        }],
                }],
        });
        expect(() => extractThreadTurnCheckpoint({ messages: [publicMessage], facts: publicFacts })).toThrow("message declaration lineage does not match its message projection");
        const repairMessage = durableInternalRepairMessage();
        const repairFacts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 1,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_repair",
                    eventSequence: 2,
                    type: "agent.tool_result",
                    modelRequestId: "model_request_1",
                    toolResult: {
                        modelToolCallId: "tool_call_repair",
                        toolName: "unknown_tool",
                        repairKind: "invalid_tool",
                        outcome: "error",
                    },
                },
            ],
            messageLineage: [{
                    messageId: repairMessage.id,
                    messageSequence: repairMessage.sequence,
                    owningEventId: repairMessage.owningEventId,
                    modelRequestId: "model_request_1",
                    entries: [{
                            lineageKind: "declaration_receipt",
                            operationKind: "write_event",
                            sourceKind: "agent.tool_result",
                            sourceId: "wrong_repair_writer",
                            eventId: "event_repair",
                            eventSequence: 2,
                            disposition: "created",
                        }],
                }],
        });
        expect(() => extractThreadTurnCheckpoint({ messages: [repairMessage], facts: repairFacts })).toThrow("internal repair has no matching declaration lineage");
    });
    test("reconstructs a receipt-backed internal repair as one terminal member", () => {
        const message = durableInternalRepairMessage();
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 1,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_repair",
                    eventSequence: 2,
                    type: "agent.tool_result",
                    modelRequestId: "model_request_1",
                    toolResult: {
                        modelToolCallId: "tool_call_repair",
                        toolName: "unknown_tool",
                        repairKind: "invalid_tool",
                        outcome: "error",
                    },
                },
                {
                    eventId: "event_end",
                    eventSequence: 3,
                    type: "span.model_request_end",
                    modelRequestId: "model_request_1",
                    requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
                },
            ],
            messageLineage: [{
                    messageId: message.id,
                    messageSequence: message.sequence,
                    owningEventId: message.owningEventId,
                    modelRequestId: "model_request_1",
                    entries: [{
                            lineageKind: "declaration_receipt",
                            operationKind: "commit_internal_tool_repair",
                            sourceKind: "internal_tool_repair",
                            sourceId: "repair_1",
                            eventId: "event_repair",
                            eventSequence: 2,
                            disposition: "created",
                        }],
                }],
        });
        expect(extractThreadTurnCheckpoint({ messages: [message], facts }).request?.toolMembers).toEqual([{
                memberKind: "internal_tool_repair",
                modelToolCallId: "tool_call_repair",
                toolName: "unknown_tool",
                repairEventId: "event_repair",
                outcome: "error",
            }]);
    });
    test("reconstructs bridge_pod_loss_repair as a terminal result without a declaration receipt", () => {
        const message = durableToolMessage();
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 3,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_tool_use",
                    eventSequence: 4,
                    type: "agent.tool_use",
                    modelRequestId: "model_request_1",
                    toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
                },
                {
                    eventId: "event_end",
                    eventSequence: 5,
                    type: "span.model_request_end",
                    modelRequestId: "model_request_1",
                    requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
                },
                {
                    eventId: "event_tool_result",
                    eventSequence: 6,
                    type: "agent.tool_result",
                    modelRequestId: "model_request_1",
                    toolResult: { toolUseEventId: "event_tool_use", outcome: "error" },
                },
            ],
            messageLineage: [{
                    messageId: message.id,
                    messageSequence: message.sequence,
                    owningEventId: message.owningEventId,
                    modelRequestId: "model_request_1",
                    entries: [
                        {
                            lineageKind: "declaration_receipt",
                            operationKind: "write_event",
                            sourceKind: "agent.tool_use",
                            sourceId: "write_tool_use",
                            eventId: "event_tool_use",
                            eventSequence: 4,
                            disposition: "created",
                        },
                        {
                            lineageKind: "bridge_pod_loss_repair",
                            repairEventId: "event_tool_result",
                            eventSequence: 6,
                            toolUseEventId: "event_tool_use",
                            outcome: "error",
                        },
                    ],
                }],
        });
        expect(extractThreadTurnCheckpoint({ messages: [message], facts }).request?.toolMembers).toEqual([{
                memberKind: "public_tool_use",
                modelToolCallId: "tool_call_1",
                toolUseEventId: "event_tool_use",
                toolName: "Read",
                terminalResult: { resultEventId: "event_tool_result", outcome: "error" },
            }]);
    });
    test("reconstructs a runtime-termination Tool Result from its created projection", () => {
        const toolUseMessage = durableOpenToolMessage();
        const terminationMessage = DurableRuntimeMessageSchema.parse({
            ...durableToolMessage(),
            id: "message_runtime_termination",
            sequence: 3,
            owningEventId: "event_runtime_termination_result",
            eventSequence: 7,
            parts: durableToolMessage().parts.map((part) => ({
                ...part,
                id: "message_runtime_termination_tool",
                messageId: "message_runtime_termination",
                state: {
                    status: "cancelled" as const,
                    input: { value: {}, preview: "{}", truncated: false },
                    error: { type: "runtime_terminated", message: "Runtime terminated." },
                },
            })),
        });
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 3,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_tool_use",
                    eventSequence: 4,
                    type: "agent.tool_use",
                    modelRequestId: "model_request_1",
                    toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
                },
                {
                    eventId: "event_end",
                    eventSequence: 5,
                    type: "span.model_request_end",
                    modelRequestId: "model_request_1",
                    requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
                },
                {
                    eventId: "event_runtime_termination_result",
                    eventSequence: 7,
                    type: "agent.tool_result",
                    modelRequestId: "model_request_1",
                    toolResult: { toolUseEventId: "event_tool_use", outcome: "cancelled" },
                },
            ],
            messageLineage: [
                {
                    messageId: toolUseMessage.id,
                    messageSequence: toolUseMessage.sequence,
                    owningEventId: toolUseMessage.owningEventId,
                    modelRequestId: "model_request_1",
                    entries: [{
                        lineageKind: "declaration_receipt",
                        operationKind: "write_event",
                        sourceKind: "agent.tool_use",
                        sourceId: "write_tool_use",
                        eventId: "event_tool_use",
                        eventSequence: 4,
                        disposition: "created",
                    }],
                },
                {
                    messageId: terminationMessage.id,
                    messageSequence: terminationMessage.sequence,
                    owningEventId: terminationMessage.owningEventId,
                    modelRequestId: "model_request_1",
                    entries: [{
                        lineageKind: "declaration_receipt",
                        operationKind: "commit_runtime_termination",
                        sourceKind: "runtime_termination",
                        sourceId: "run_termination",
                        eventId: "event_runtime_termination_result",
                        eventSequence: 7,
                        disposition: "created",
                    }],
                },
            ],
        });

        expect(extractThreadTurnCheckpoint({
            messages: [toolUseMessage, terminationMessage],
            facts,
        }).request?.toolMembers).toContainEqual(expect.objectContaining({
            toolUseEventId: "event_tool_use",
            terminalResult: { resultEventId: "event_runtime_termination_result", outcome: "cancelled" },
        }));
    });
    test("keeps a post-seal MCP diagnostic outside terminal closeout", () => {
        const ordinary = durableToolMessage();
        const message = DurableRuntimeMessageSchema.parse({
            ...ordinary,
            parts: ordinary.parts.map((part) => ({
                ...part,
                toolName: "github_search",
                toolEvent: { kind: "mcp" as const, mcpServerName: "github" },
            })),
        });
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                {
                    eventId: "event_start",
                    eventSequence: 3,
                    type: "span.model_request_start",
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_tool_use",
                    eventSequence: 4,
                    type: "agent.mcp_tool_use",
                    modelRequestId: "model_request_1",
                    toolUse: { modelToolCallId: "tool_call_1", toolName: "github_search" },
                },
                {
                    eventId: "event_end",
                    eventSequence: 5,
                    type: "span.model_request_end",
                    modelRequestId: "model_request_1",
                    requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
                },
                {
                    eventId: "event_mcp_diagnostic",
                    eventSequence: 6,
                    type: "session.error",
                    failure: { errorType: "mcp_authentication_error", retryStatus: "terminal" },
                },
                {
                    eventId: "event_tool_result",
                    eventSequence: 7,
                    type: "agent.mcp_tool_result",
                    modelRequestId: "model_request_1",
                    toolResult: { toolUseEventId: "event_tool_use", outcome: "error" },
                },
            ],
            messageLineage: [{
                messageId: message.id,
                messageSequence: message.sequence,
                owningEventId: message.owningEventId,
                modelRequestId: "model_request_1",
                entries: [
                    {
                        lineageKind: "declaration_receipt",
                        operationKind: "write_event",
                        sourceKind: "agent.mcp_tool_use",
                        sourceId: "write_tool_use",
                        eventId: "event_tool_use",
                        eventSequence: 4,
                        disposition: "created",
                    },
                    {
                        lineageKind: "declaration_receipt",
                        operationKind: "write_event",
                        sourceKind: "agent.mcp_tool_result",
                        sourceId: "write_tool_result",
                        eventId: "event_tool_result",
                        eventSequence: 7,
                        disposition: "updated",
                    },
                ],
            }],
        });

        const checkpoint = extractThreadTurnCheckpoint({ messages: [message], facts });
        expect(checkpoint.terminalCloseout).toBeUndefined();
        expect(checkpoint.request?.toolMembers).toEqual([{
            memberKind: "public_tool_use",
            modelToolCallId: "tool_call_1",
            toolUseEventId: "event_tool_use",
            toolName: "github_search",
            terminalResult: { resultEventId: "event_tool_result", outcome: "error" },
        }]);
    });
    test("pairs retries-exhausted closeout with the preceding exhausted failure", () => {
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                { eventId: "event_running", eventSequence: 1, type: "session.status_running" },
                {
                    eventId: "event_failure",
                    eventSequence: 2,
                    type: "session.error",
                    failure: { errorType: "provider_stream_error", retryStatus: "exhausted" },
                },
                {
                    eventId: "event_idle",
                    eventSequence: 3,
                    type: "session.status_idle",
                    idle: { stopReason: "retries_exhausted" },
                },
                {
                    eventId: "event_later_diagnostic",
                    eventSequence: 4,
                    type: "session.error",
                    failure: { errorType: "mcp_authentication_error", retryStatus: "terminal" },
                },
            ],
            messageLineage: [],
        });
        expect(extractThreadTurnCheckpoint({ messages: [], facts }).terminalCloseout).toEqual({
            failureEventId: "event_failure",
            closeoutEventId: "event_idle",
            disposition: "retries_exhausted",
        });
    });
    test("a new execution run excludes the prior terminal request and closeout", () => {
        const input = durableTextMessage({
            id: "message_new_input",
            sequence: 2,
            eventId: "event_new_input",
            eventSequence: 7,
            role: "user",
        });
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                { eventId: "event_running_old", eventSequence: 1, type: "session.status_running" },
                {
                    eventId: "event_start_old",
                    eventSequence: 2,
                    type: "span.model_request_start",
                    modelRequestId: "request_old",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 1 },
                },
                {
                    eventId: "event_end_old",
                    eventSequence: 3,
                    type: "span.model_request_end",
                    modelRequestId: "request_old",
                    requestEnd: {
                        requestStartEventId: "event_start_old",
                        isError: true,
                        errorKind: "provider_stream_error",
                        rescheduled: false,
                    },
                },
                {
                    eventId: "event_failure_old",
                    eventSequence: 4,
                    type: "session.error",
                    failure: { errorType: "provider_stream_error", retryStatus: "exhausted" },
                },
                {
                    eventId: "event_idle_old",
                    eventSequence: 5,
                    type: "session.status_idle",
                    idle: { stopReason: "retries_exhausted" },
                },
                { eventId: "event_running_new", eventSequence: 6, type: "session.status_running" },
            ],
            messageLineage: [{
                messageId: input.id,
                messageSequence: input.sequence,
                owningEventId: input.owningEventId,
                entries: [{
                    lineageKind: "declaration_receipt",
                    operationKind: "commit_inputs",
                    sourceKind: "messages",
                    sourceId: "runtime_input_new",
                    eventId: input.owningEventId,
                    eventSequence: input.eventSequence,
                    disposition: "created",
                }],
            }],
        });
        expect(extractThreadTurnCheckpoint({ messages: [input], facts })).toEqual({
            executionRunId: "event_running_new",
            pendingInputMessageIds: ["message_new_input"],
        });
    });
    test("a new execution run does not resurrect input consumed by the prior Request Start", () => {
        const consumed = durableTextMessage({
            id: "message_consumed",
            sequence: 1,
            eventId: "event_consumed",
            eventSequence: 1,
            role: "user",
        });
        const pending = durableTextMessage({
            id: "message_pending",
            sequence: 2,
            eventId: "event_pending",
            eventSequence: 6,
            role: "user",
        });
        const lineage = (message: typeof consumed, sourceId: string) => ({
            messageId: message.id,
            messageSequence: message.sequence,
            owningEventId: message.owningEventId,
            entries: [{
                lineageKind: "declaration_receipt" as const,
                operationKind: "commit_inputs",
                sourceKind: "messages",
                sourceId,
                eventId: message.owningEventId,
                eventSequence: message.eventSequence,
                disposition: "created" as const,
            }],
        });
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                { eventId: "event_running_old", eventSequence: 2, type: "session.status_running" },
                {
                    eventId: "event_start_old",
                    eventSequence: 3,
                    type: "span.model_request_start",
                    modelRequestId: "request_old",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 1 },
                },
                {
                    eventId: "event_end_old",
                    eventSequence: 4,
                    type: "span.model_request_end",
                    modelRequestId: "request_old",
                    requestEnd: { requestStartEventId: "event_start_old", isError: false, rescheduled: false },
                },
                { eventId: "event_idle_old", eventSequence: 5, type: "session.status_idle", idle: { stopReason: "end_turn" } },
                { eventId: "event_running_new", eventSequence: 7, type: "session.status_running" },
            ],
            messageLineage: [lineage(consumed, "input_consumed"), lineage(pending, "input_pending")],
        });
        expect(extractThreadTurnCheckpoint({ messages: [consumed, pending], facts })).toEqual({
            executionRunId: "event_running_new",
            pendingInputMessageIds: ["message_pending"],
        });
    });

    test("an interrupted request closed idle is not reactivated by cold reconstruction", () => {
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                { eventId: "event_running", eventSequence: 1, type: "session.status_running" },
                {
                    eventId: "event_start",
                    eventSequence: 2,
                    type: "span.model_request_start",
                    modelRequestId: "request_interrupted",
                    requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 0 },
                },
                { eventId: "event_interrupt", eventSequence: 3, type: "user.interrupt" },
                {
                    eventId: "event_end",
                    eventSequence: 4,
                    type: "span.model_request_end",
                    modelRequestId: "request_interrupted",
                    requestEnd: {
                        requestStartEventId: "event_start",
                        isError: true,
                        errorKind: "runtime_interrupted",
                        rescheduled: false,
                    },
                },
                { eventId: "event_idle", eventSequence: 5, type: "session.status_idle", idle: { stopReason: "end_turn" } },
            ],
            messageLineage: [],
        });
        expect(extractThreadTurnCheckpoint({ messages: [], facts })).toEqual({
            pendingInputMessageIds: [],
            idleCloseout: { eventId: "event_idle", stopReason: "end_turn" },
        });
    });
    test("reconstructs an idle-cleanup result message created without a declaration receipt", () => {
        const toolMessage = durableOpenToolMessage();
        const resultMessage = durableIdleCleanupMessage();
        const facts = {
            events: [
                {
                    eventId: "event_running",
                    eventSequence: 1,
                    type: "session.status_running" as const,
                },
                {
                    eventId: "event_start",
                    eventSequence: 3,
                    type: "span.model_request_start" as const,
                    modelRequestId: "model_request_1",
                    requestStart: { requestKind: "agent_provider_request" as const, contextThroughMessageSequence: 0 },
                },
                {
                    eventId: "event_tool_use",
                    eventSequence: 4,
                    type: "agent.tool_use" as const,
                    modelRequestId: "model_request_1",
                    toolUse: { modelToolCallId: "tool_call_1", toolName: "Read" },
                },
                {
                    eventId: "event_end",
                    eventSequence: 5,
                    type: "span.model_request_end" as const,
                    modelRequestId: "model_request_1",
                    requestEnd: { requestStartEventId: "event_start", isError: false, rescheduled: false },
                },
                {
                    eventId: "event_requires_action_idle",
                    eventSequence: 6,
                    type: "session.status_idle" as const,
                    idle: { stopReason: "requires_action" },
                },
                {
                    eventId: "event_tool_result",
                    eventSequence: 7,
                    type: "agent.tool_result" as const,
                    modelRequestId: "model_request_1",
                    toolResult: { toolUseEventId: "event_tool_use", outcome: "error" as const },
                },
            ],
            messageLineage: [
                {
                    messageId: toolMessage.id,
                    messageSequence: toolMessage.sequence,
                    owningEventId: "event_tool_use",
                    modelRequestId: "model_request_1",
                    entries: [{
                            lineageKind: "declaration_receipt" as const,
                            operationKind: "write_event",
                            sourceKind: "agent.tool_use",
                            sourceId: "write_tool_use",
                            eventId: "event_tool_use",
                            eventSequence: 4,
                            disposition: "created" as const,
                        }],
                },
                {
                    messageId: resultMessage.id,
                    messageSequence: resultMessage.sequence,
                    owningEventId: resultMessage.owningEventId,
                    modelRequestId: "model_request_1",
                    entries: [{
                            lineageKind: "bridge_idle_cleanup_repair" as const,
                            repairEventId: "event_tool_result",
                            eventSequence: 7,
                            toolUseEventId: "event_tool_use",
                            outcome: "error" as const,
                        }],
                },
            ],
        };
        expect(extractThreadTurnCheckpoint({ messages: [toolMessage, resultMessage], facts }).request?.toolMembers).toEqual([{
                memberKind: "public_tool_use",
                modelToolCallId: "tool_call_1",
                toolUseEventId: "event_tool_use",
                toolName: "Read",
                terminalResult: { resultEventId: "event_tool_result", outcome: "error" },
            }]);
    });
    test("rejects an assistant message disguised as pending committed input", () => {
        const message = durableTextMessage({
            id: "message_assistant_input",
            sequence: 1,
            eventId: "event_assistant",
            eventSequence: 1,
            role: "assistant",
        });
        expect(() => extractThreadTurnCheckpoint({
            messages: [message],
            facts: {
                events: [],
                messageLineage: [{
                        messageId: message.id,
                        messageSequence: message.sequence,
                        owningEventId: message.owningEventId,
                        entries: [{
                                lineageKind: "declaration_receipt",
                                operationKind: "commit_inputs",
                                sourceKind: "messages",
                                sourceId: "input_1",
                                eventId: message.owningEventId,
                                eventSequence: message.eventSequence,
                                disposition: "created",
                            }],
                    }],
            },
        })).toThrow("message declaration lineage does not match its message projection");
    });
    test("does not resurrect an interrupt after its execution run is durably idle", () => {
        const facts = ThreadTurnLoadFactsSchema.parse({
            events: [
                { eventId: "event_running", eventSequence: 1, type: "session.status_running" },
                { eventId: "event_interrupt", eventSequence: 2, type: "user.interrupt" },
                {
                    eventId: "event_idle",
                    eventSequence: 3,
                    type: "session.status_idle",
                    idle: { stopReason: "end_turn" },
                },
            ],
            messageLineage: [],
        });
        expect(extractThreadTurnCheckpoint({ messages: [], facts })).toEqual({
            pendingInputMessageIds: [],
            idleCloseout: { eventId: "event_idle", stopReason: "end_turn" },
        });
    });
});
