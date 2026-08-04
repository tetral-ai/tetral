import { z } from "zod/v4";
import type { DurableRuntimeMessage } from "../contracts/runtime.js";
import {
  parseThreadToolRouteView,
  parseThreadTurnCheckpoint,
} from "./thread-turn-checkpoint.js";
import type { ThreadToolRouteView, ThreadTurnCheckpoint } from "./thread-turn-checkpoint.js";

const IdentitySchema = z.string().min(1);
const SequenceSchema = z.number().int().positive().max(Number.MAX_SAFE_INTEGER);
const MessageSequenceSchema = z.number().int().positive().max(Number.MAX_SAFE_INTEGER);
const RequestKindSchema = z.enum([
  "agent_provider_request",
  "compaction_summary",
  "approval_reviewer",
]);
const TerminalOutcomeSchema = z.enum(["success", "error", "cancelled", "unknown"]);

const RequestStartFactSchema = z.strictObject({
  requestKind: RequestKindSchema,
  contextThroughMessageSequence: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
});

const RequestEndFactSchema = z.strictObject({
  requestStartEventId: IdentitySchema,
  isError: z.boolean(),
  errorKind: IdentitySchema.optional(),
  rescheduled: z.boolean(),
});

const ToolUseFactSchema = z.strictObject({
  modelToolCallId: IdentitySchema,
  toolName: IdentitySchema,
});

const OrdinaryToolResultFactSchema = z.strictObject({
  toolUseEventId: IdentitySchema,
  outcome: TerminalOutcomeSchema,
});

const InternalToolRepairFactSchema = z.strictObject({
  modelToolCallId: IdentitySchema,
  toolName: IdentitySchema,
  repairKind: z.literal("invalid_tool"),
  outcome: z.literal("error"),
});

const TurnEventTypeSchema = z.enum([
  "session.status_running",
  "session.thread_status_running",
  "span.model_request_start",
  "span.model_request_end",
  "agent.tool_use",
  "agent.mcp_tool_use",
  "agent.tool_result",
  "agent.mcp_tool_result",
  "user.interrupt",
  "session.error",
  "session.status_rescheduled",
  "session.thread_status_rescheduled",
  "session.status_idle",
  "session.thread_status_idle",
  "session.status_terminated",
  "session.thread_status_terminated",
]);

const ThreadTurnEventFactSchema = z.strictObject({
  eventId: IdentitySchema,
  eventSequence: SequenceSchema,
  type: TurnEventTypeSchema,
  modelRequestId: IdentitySchema.optional(),
  requestStart: RequestStartFactSchema.optional(),
  requestEnd: RequestEndFactSchema.optional(),
  toolUse: ToolUseFactSchema.optional(),
  toolResult: z.union([OrdinaryToolResultFactSchema, InternalToolRepairFactSchema]).optional(),
  idle: z.strictObject({ stopReason: IdentitySchema }).optional(),
  failure: z.strictObject({
    errorType: IdentitySchema,
    retryStatus: z.enum(["retrying", "exhausted", "terminal"]),
  }).optional(),
}).superRefine((event, context) => {
  const specialized = [
    event.requestStart,
    event.requestEnd,
    event.toolUse,
    event.toolResult,
    event.idle,
    event.failure,
  ].filter((value) => value !== undefined).length;
  const requireModelRequest = (): void => {
    if (event.modelRequestId === undefined) {
      context.addIssue({ code: "custom", message: `${event.type} requires modelRequestId` });
    }
  };
  switch (event.type) {
    case "span.model_request_start":
      requireModelRequest();
      if (event.requestStart === undefined || specialized !== 1) {
        context.addIssue({ code: "custom", message: "Request Start fact is malformed" });
      }
      break;
    case "span.model_request_end":
      requireModelRequest();
      if (event.requestEnd === undefined || specialized !== 1) {
        context.addIssue({ code: "custom", message: "Request End fact is malformed" });
      }
      break;
    case "agent.tool_use":
    case "agent.mcp_tool_use":
      requireModelRequest();
      if (event.toolUse === undefined || specialized !== 1) {
        context.addIssue({ code: "custom", message: "Tool Use fact is malformed" });
      }
      break;
    case "agent.tool_result":
    case "agent.mcp_tool_result":
      requireModelRequest();
      if (event.toolResult === undefined || specialized !== 1) {
        context.addIssue({ code: "custom", message: "Tool Result fact is malformed" });
      }
      if (event.type === "agent.mcp_tool_result" && event.toolResult !== undefined && "repairKind" in event.toolResult) {
        context.addIssue({ code: "custom", message: "MCP Tool Result cannot be an internal repair" });
      }
      break;
    case "session.status_idle":
    case "session.thread_status_idle":
      if (event.modelRequestId !== undefined || event.idle === undefined || specialized !== 1) {
        context.addIssue({ code: "custom", message: "idle fact is malformed" });
      }
      break;
    case "session.error":
      if (event.modelRequestId !== undefined || event.failure === undefined || specialized !== 1) {
        context.addIssue({ code: "custom", message: "failure fact is malformed" });
      }
      break;
    default:
      if (event.modelRequestId !== undefined || specialized !== 0) {
        context.addIssue({ code: "custom", message: `${event.type} fact carries unsupported members` });
      }
  }
});

const DeclarationLineageEntrySchema = z.strictObject({
  lineageKind: z.literal("declaration_receipt"),
  operationKind: IdentitySchema,
  sourceKind: IdentitySchema,
  sourceId: IdentitySchema,
  eventId: IdentitySchema,
  eventSequence: SequenceSchema,
  disposition: z.enum(["created", "updated"]),
});

const PodLossRepairLineageEntrySchema = z.strictObject({
  lineageKind: z.literal("bridge_pod_loss_repair"),
  repairEventId: IdentitySchema,
  eventSequence: SequenceSchema,
  toolUseEventId: IdentitySchema,
  outcome: TerminalOutcomeSchema,
});

const IdleCleanupRepairLineageEntrySchema = z.strictObject({
  lineageKind: z.literal("bridge_idle_cleanup_repair"),
  repairEventId: IdentitySchema,
  eventSequence: SequenceSchema,
  toolUseEventId: IdentitySchema,
  outcome: z.literal("error"),
});

const MessageLineageEntrySchema = z.discriminatedUnion("lineageKind", [
  DeclarationLineageEntrySchema,
  PodLossRepairLineageEntrySchema,
  IdleCleanupRepairLineageEntrySchema,
]);

const RuntimeMessageLineageSchema = z.strictObject({
  messageId: IdentitySchema,
  messageSequence: MessageSequenceSchema,
  owningEventId: IdentitySchema,
  modelRequestId: IdentitySchema.optional(),
  entries: z.array(MessageLineageEntrySchema).min(1),
}).superRefine((lineage, context) => {
  let previous = 0;
  for (const entry of lineage.entries) {
    if (entry.eventSequence <= previous) {
      context.addIssue({ code: "custom", message: "message lineage must follow database event order" });
      break;
    }
    previous = entry.eventSequence;
  }
  const creationCount = lineage.entries.filter((entry) =>
    entry.lineageKind === "declaration_receipt" && entry.disposition === "created"
  ).length;
  const repairOnlyIdleCleanup =
    creationCount === 0 &&
    lineage.entries.length === 1 &&
    lineage.entries[0]?.lineageKind === "bridge_idle_cleanup_repair" &&
    lineage.entries[0].repairEventId === lineage.owningEventId;
  if (creationCount !== 1 && !repairOnlyIdleCleanup) {
    context.addIssue({ code: "custom", message: "message lineage requires one creation receipt" });
  }
});

export const ThreadTurnLoadFactsSchema = z.strictObject({
  events: z.array(ThreadTurnEventFactSchema),
  messageLineage: z.array(RuntimeMessageLineageSchema),
}).superRefine((facts, context) => {
  const eventIds = new Set<string>();
  let previousSequence = 0;
  for (const event of facts.events) {
    if (eventIds.has(event.eventId) || event.eventSequence <= previousSequence) {
      context.addIssue({ code: "custom", message: "turn events must have unique database order" });
      break;
    }
    eventIds.add(event.eventId);
    previousSequence = event.eventSequence;
  }
  if (new Set(facts.messageLineage.map((lineage) => lineage.messageId)).size !== facts.messageLineage.length) {
    context.addIssue({ code: "custom", message: "loaded messages must have one lineage record" });
  }
});

export type ThreadTurnLoadFacts = z.infer<typeof ThreadTurnLoadFactsSchema>;

interface ColdPendingToolUse {
  readonly toolUseEventId: string;
  readonly modelRequestId: string;
  readonly modelToolCallId: string;
  readonly toolName: string;
  readonly status: "pending" | "resolving";
  readonly decision?: "allow" | "deny" | undefined;
}

interface ColdPendingSandboxExecution {
  readonly toolUseEventId: string;
  readonly modelRequestId: string;
  readonly modelToolCallId: string;
  readonly toolName: string;
}

export function extractThreadTurnCheckpoint(input: {
  readonly messages: readonly DurableRuntimeMessage[];
  readonly facts: ThreadTurnLoadFacts;
}): ThreadTurnCheckpoint {
  const facts = ThreadTurnLoadFactsSchema.parse(input.facts);
  const messageLineage = validateLoadedMessageLineage(input.messages, facts.messageLineage);
  const starts = new Map<string, ThreadTurnLoadFacts["events"][number]>();
  const ends = new Map<string, ThreadTurnLoadFacts["events"][number]>();

  for (const event of facts.events) {
    if (event.type === "span.model_request_start") {
      const modelRequestId = event.modelRequestId!;
      if (starts.has(modelRequestId)) {
        throw new Error("model request has duplicate Request Start facts");
      }
      starts.set(modelRequestId, event);
    } else if (event.type === "span.model_request_end") {
      const modelRequestId = event.modelRequestId!;
      const start = starts.get(modelRequestId);
      if (start === undefined || event.requestEnd!.requestStartEventId !== start.eventId) {
        throw new Error("Request End has no matching Request Start");
      }
      if (ends.has(modelRequestId)) {
        throw new Error("model request has duplicate Request End facts");
      }
      ends.set(modelRequestId, event);
    }
  }

  validateRequestMembers(facts.events, starts, ends, input.messages, messageLineage);
  const newestStart = [...starts.values()].at(-1);
  const request = newestStart === undefined
    ? undefined
    : extractNewestRequest(newestStart, ends.get(newestStart.modelRequestId!), facts.events);
  const closeout = extractRunCloseout(facts.events);
  const boundary = newestStart?.requestStart?.contextThroughMessageSequence ?? 0;
  const highestLoadedMessageSequence = input.messages.reduce(
    (highest, message) => Math.max(highest, message.sequence),
    0,
  );
  if (boundary > highestLoadedMessageSequence) {
    throw new Error("Request Start message boundary exceeds the loaded durable message range");
  }
  const messagesById = new Map(input.messages.map((message) => [message.id, message]));
  const pendingInputMessageIds = [...messageLineage.values()]
    .filter((lineage) => {
      const message = messagesById.get(lineage.messageId)!;
      return lineage.messageSequence > boundary && messageLineageMakesRequestReady(message, lineage);
    })
    .sort((left, right) => left.messageSequence - right.messageSequence)
    .map((lineage) => lineage.messageId);

  return parseThreadTurnCheckpoint({
    ...(closeout.executionRunId !== undefined ? { executionRunId: closeout.executionRunId } : {}),
    pendingInputMessageIds,
    ...(request !== undefined ? { request } : {}),
    ...(closeout.interruptEventId !== undefined ? { interruptEventId: closeout.interruptEventId } : {}),
    ...(closeout.terminalCloseout !== undefined ? { terminalCloseout: closeout.terminalCloseout } : {}),
    ...(closeout.idleCloseout !== undefined ? { idleCloseout: closeout.idleCloseout } : {}),
  });
}

export function extractColdThreadToolRouteView(input: {
  readonly checkpoint: ThreadTurnCheckpoint;
  readonly pendingToolUses: readonly ColdPendingToolUse[];
  readonly pendingSandboxExecutions: readonly ColdPendingSandboxExecution[];
}): ThreadToolRouteView {
  const checkpoint = parseThreadTurnCheckpoint(input.checkpoint);
  const incompleteMembers = new Map(
    (checkpoint.request?.toolMembers ?? [])
      .filter((member): member is Extract<typeof member, { readonly memberKind: "public_tool_use" }> =>
        member.memberKind === "public_tool_use" && member.terminalResult === undefined
      )
      .map((member) => [member.toolUseEventId, member]),
  );
  const routeByToolUse = new Map<string, ThreadToolRouteView["routes"][number]>();

  const addRoute = (
    source: ColdPendingToolUse | ColdPendingSandboxExecution,
    disposition: ThreadToolRouteView["routes"][number]["disposition"],
  ): void => {
    const member = incompleteMembers.get(source.toolUseEventId);
    if (
      member === undefined ||
      checkpoint.request === undefined ||
      source.modelRequestId !== checkpoint.request.modelRequestId ||
      source.modelToolCallId !== member.modelToolCallId ||
      source.toolName !== member.toolName
    ) {
      throw new Error("cold Tool route identity does not match its request member");
    }
    if (routeByToolUse.has(source.toolUseEventId)) {
      throw new Error("cold non-terminal Tool Use requires exactly one durable route");
    }
    routeByToolUse.set(source.toolUseEventId, { toolUseEventId: source.toolUseEventId, disposition });
  };

  for (const pending of input.pendingToolUses) {
    if (pending.status === "pending" && pending.decision === undefined) {
      addRoute(pending, "requires_user_action");
      continue;
    }
    if (pending.status === "resolving" && pending.decision !== undefined) {
      addRoute(pending, "resume_approval_settlement");
      continue;
    }
    throw new Error("cold pending Tool Use disposition is malformed");
  }
  for (const pending of input.pendingSandboxExecutions) {
    addRoute(pending, "resume_sandbox_execution");
  }

  if (routeByToolUse.size !== incompleteMembers.size) {
    throw new Error("cold non-terminal Tool Use requires exactly one durable route");
  }
  return parseThreadToolRouteView({
    routes: [...incompleteMembers.keys()].map((toolUseEventId) => routeByToolUse.get(toolUseEventId)!),
  });
}

function validateLoadedMessageLineage(
  messages: readonly DurableRuntimeMessage[],
  lineageRecords: ThreadTurnLoadFacts["messageLineage"],
): Map<string, ThreadTurnLoadFacts["messageLineage"][number]> {
  const messagesById = new Map(messages.map((message) => [message.id, message]));
  const lineageById = new Map(lineageRecords.map((lineage) => [lineage.messageId, lineage]));
  if (messagesById.size !== messages.length || lineageById.size !== messagesById.size) {
    throw new Error("checkpoint message lineage does not match the loaded message set");
  }
  for (const [messageId, message] of messagesById) {
    const lineage = lineageById.get(messageId);
    if (
      lineage === undefined ||
      lineage.messageSequence !== message.sequence ||
      lineage.owningEventId !== message.owningEventId
    ) {
      throw new Error("checkpoint message lineage does not match the loaded message set");
    }
    for (const entry of lineage.entries) {
      if (entry.lineageKind === "declaration_receipt" && !declarationLineageRecognized(entry)) {
        throw new Error("message declaration lineage is not recognized");
      }
      if (
        entry.lineageKind === "declaration_receipt" &&
        declarationLineageMakesRequestReady(entry) &&
        (entry.disposition !== "created" || message.role !== "user" || entry.eventId !== lineage.owningEventId)
      ) {
        throw new Error("message declaration lineage does not match its message projection");
      }
    }
    const finalEntry = lineage.entries.at(-1)!;
    const finalEventId = finalEntry.lineageKind === "declaration_receipt"
      ? finalEntry.eventId
      : finalEntry.repairEventId;
    if (finalEventId !== lineage.owningEventId) {
      throw new Error("message lineage does not end at its owning event");
    }
  }
  return lineageById;
}

function declarationLineageRecognized(
  entry: Extract<ThreadTurnLoadFacts["messageLineage"][number]["entries"][number], { lineageKind: "declaration_receipt" }>,
): boolean {
  switch (entry.operationKind) {
    case "commit_inputs":
      return [
        "messages",
        "tool_confirmation",
        "approval_review",
        "agent_mail",
        "rejection",
        "interrupt_control",
      ].includes(entry.sourceKind);
    case "write_event":
      return [
        "agent.message",
        "agent.tool_use",
        "agent.mcp_tool_use",
        "agent.tool_result",
        "agent.mcp_tool_result",
      ].includes(entry.sourceKind);
    case "write_request_end":
      return entry.sourceKind === "model_request";
    case "commit_internal_tool_repair":
      return entry.sourceKind === "internal_tool_repair";
    case "commit_task_notification_result":
      return entry.sourceKind === "task_notification";
    case "finish_idle":
      return entry.sourceKind === "turn_closeout";
    case "commit_runtime_termination":
      return entry.sourceKind === "runtime_termination";
    default:
      return false;
  }
}

function validateRequestMembers(
  events: ThreadTurnLoadFacts["events"],
  starts: ReadonlyMap<string, ThreadTurnLoadFacts["events"][number]>,
  ends: ReadonlyMap<string, ThreadTurnLoadFacts["events"][number]>,
  messages: readonly DurableRuntimeMessage[],
  messageLineage: ReadonlyMap<string, ThreadTurnLoadFacts["messageLineage"][number]>,
): void {
  const toolUses = new Map<string, ThreadTurnLoadFacts["events"][number]>();
  const terminalResults = new Set<string>();
  const toolMessageParts = new Map<string, {
    readonly toolCallId: string;
    readonly toolName: string;
    readonly lineage: ThreadTurnLoadFacts["messageLineage"][number];
  }[]>();
  const unboundToolMessageParts: {
    readonly toolCallId: string;
    readonly toolName: string;
    readonly lineage: ThreadTurnLoadFacts["messageLineage"][number];
  }[] = [];
  for (const message of messages) {
    const lineage = messageLineage.get(message.id)!;
    for (const part of message.parts) {
      if (part.type !== "tool") {
        continue;
      }
      if (part.toolUseEventId !== undefined) {
        const projected = { toolCallId: part.toolCallId, toolName: part.toolName, lineage };
        toolMessageParts.set(part.toolUseEventId, [...(toolMessageParts.get(part.toolUseEventId) ?? []), projected]);
      } else {
        unboundToolMessageParts.push({ toolCallId: part.toolCallId, toolName: part.toolName, lineage });
      }
    }
  }

  for (const event of events) {
    if (event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use") {
      const modelRequestId = event.modelRequestId!;
      const start = starts.get(modelRequestId);
      const end = ends.get(modelRequestId);
      if (start === undefined) {
        throw new Error("Tool Use has no matching Request Start");
      }
      if (end !== undefined && event.eventSequence > end.eventSequence) {
        throw new Error("Tool Use cannot follow Request End");
      }
      const projected = (toolMessageParts.get(event.eventId) ?? []).filter((part) =>
        part.toolCallId === event.toolUse!.modelToolCallId &&
        part.toolName === event.toolUse!.toolName
      );
      if (projected.length === 0) {
        throw new Error("Tool Use does not match its assistant projection part");
      }
      const declared = projected.filter((part) =>
        part.lineage.entries.some((entry) =>
          entry.lineageKind === "declaration_receipt" &&
          entry.operationKind === "write_event" &&
          entry.sourceKind === event.type &&
          entry.eventId === event.eventId &&
          (entry.disposition === "created" || entry.disposition === "updated")
        )
      );
      if (declared.length !== 1) {
        throw new Error("Tool Use has no matching declaration lineage");
      }
      toolUses.set(event.eventId, event);
      continue;
    }
    if (event.type !== "agent.tool_result" && event.type !== "agent.mcp_tool_result") {
      continue;
    }
    const result = event.toolResult!;
    if ("repairKind" in result) {
      const end = ends.get(event.modelRequestId!);
      if (!starts.has(event.modelRequestId!)) {
        throw new Error("internal repair has no matching Request Start");
      }
      if (end !== undefined && event.eventSequence > end.eventSequence) {
        throw new Error("internal repair cannot follow Request End");
      }
      const matchingRepairs = unboundToolMessageParts.filter((part) =>
        part.toolCallId === result.modelToolCallId &&
        part.toolName === result.toolName &&
        part.lineage.modelRequestId === event.modelRequestId &&
        part.lineage.entries.some((entry) =>
          entry.lineageKind === "declaration_receipt" &&
          entry.operationKind === "commit_internal_tool_repair" &&
          entry.sourceKind === "internal_tool_repair" &&
          entry.eventId === event.eventId &&
          entry.disposition === "created"
        )
      );
      if (matchingRepairs.length !== 1) {
        throw new Error("internal repair has no matching declaration lineage");
      }
      continue;
    }
    const toolUse = toolUses.get(result.toolUseEventId);
    if (toolUse === undefined || toolUse.modelRequestId !== event.modelRequestId) {
      throw new Error("Tool Result has no matching Tool Use");
    }
    if (event.type === "agent.mcp_tool_result" && toolUse.type !== "agent.mcp_tool_use") {
      throw new Error("MCP Tool Result has the wrong Tool Use family");
    }
    if (event.type === "agent.tool_result" && toolUse.type !== "agent.tool_use") {
      throw new Error("Tool Result has the wrong Tool Use family");
    }
    if (terminalResults.has(result.toolUseEventId)) {
      throw new Error("Tool Use has duplicate terminal results");
    }
    const matchingResultProjections = (toolMessageParts.get(result.toolUseEventId) ?? []).filter((projection) =>
      projection.lineage.entries.some((entry) => {
      if (
        entry.lineageKind === "declaration_receipt" &&
        entry.eventId === event.eventId &&
        entry.disposition === "updated"
      ) {
        return (
          entry.operationKind === "write_event" && entry.sourceKind === event.type
        ) || (
          entry.operationKind === "commit_inputs" &&
          (entry.sourceKind === "tool_confirmation" || entry.sourceKind === "interrupt_control")
        );
      }
      return (
        (entry.lineageKind === "bridge_pod_loss_repair" || entry.lineageKind === "bridge_idle_cleanup_repair") &&
        entry.repairEventId === event.eventId &&
        entry.toolUseEventId === result.toolUseEventId
      );
      })
    );
    if (matchingResultProjections.length !== 1) {
      throw new Error("Tool Result has no matching message mutation lineage");
    }
    terminalResults.add(result.toolUseEventId);
  }
}

function extractNewestRequest(
  start: ThreadTurnLoadFacts["events"][number],
  end: ThreadTurnLoadFacts["events"][number] | undefined,
  events: ThreadTurnLoadFacts["events"],
): NonNullable<ThreadTurnCheckpoint["request"]> {
  const modelRequestId = start.modelRequestId!;
  const terminalByToolUse = new Map<string, { readonly resultEventId: string; readonly outcome: "success" | "error" | "cancelled" | "unknown" }>();
  const members: NonNullable<ThreadTurnCheckpoint["request"]>["toolMembers"][number][] = [];
  const modelToolCallIds = new Set<string>();
  for (const event of events) {
    if (event.modelRequestId !== modelRequestId) {
      continue;
    }
    if ((event.type === "agent.tool_result" || event.type === "agent.mcp_tool_result") && event.toolResult !== undefined && !("repairKind" in event.toolResult)) {
      terminalByToolUse.set(event.toolResult.toolUseEventId, {
        resultEventId: event.eventId,
        outcome: event.toolResult.outcome,
      });
    }
  }
  for (const event of events) {
    if (event.modelRequestId !== modelRequestId) {
      continue;
    }
    if (event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use") {
      if (modelToolCallIds.has(event.toolUse!.modelToolCallId)) {
        throw new Error("modelToolCallId is duplicated within the request");
      }
      modelToolCallIds.add(event.toolUse!.modelToolCallId);
      const terminalResult = terminalByToolUse.get(event.eventId);
      members.push({
        memberKind: "public_tool_use",
        modelToolCallId: event.toolUse!.modelToolCallId,
        toolUseEventId: event.eventId,
        toolName: event.toolUse!.toolName,
        ...(terminalResult !== undefined ? { terminalResult } : {}),
      });
    } else if (
      event.type === "agent.tool_result" &&
      event.toolResult !== undefined &&
      "repairKind" in event.toolResult
    ) {
      if (modelToolCallIds.has(event.toolResult.modelToolCallId)) {
        throw new Error("modelToolCallId is duplicated within the request");
      }
      modelToolCallIds.add(event.toolResult.modelToolCallId);
      members.push({
        memberKind: "internal_tool_repair",
        modelToolCallId: event.toolResult.modelToolCallId,
        toolName: event.toolResult.toolName,
        repairEventId: event.eventId,
        outcome: "error",
      });
    }
  }
  return {
    modelRequestId,
    requestStartEventId: start.eventId,
    requestKind: start.requestStart!.requestKind,
    contextThroughMessageSequence: start.requestStart!.contextThroughMessageSequence,
    ...(end?.requestEnd !== undefined
      ? {
          requestEnd: {
            eventId: end.eventId,
            isError: end.requestEnd.isError,
            ...(end.requestEnd.errorKind !== undefined ? { errorKind: end.requestEnd.errorKind } : {}),
            rescheduled: end.requestEnd.rescheduled,
          },
        }
      : {}),
    toolMembers: members,
  };
}

function messageLineageMakesRequestReady(
  message: DurableRuntimeMessage,
  lineage: ThreadTurnLoadFacts["messageLineage"][number],
): boolean {
  if (message.role !== "user") {
    return false;
  }
  return lineage.entries.some((entry) => {
    return entry.lineageKind === "declaration_receipt" &&
      entry.disposition === "created" &&
      declarationLineageMakesRequestReady(entry);
  });
}

function declarationLineageMakesRequestReady(
  entry: Extract<ThreadTurnLoadFacts["messageLineage"][number]["entries"][number], { lineageKind: "declaration_receipt" }>,
): boolean {
  return (
    entry.operationKind === "commit_inputs" &&
    (entry.sourceKind === "messages" || entry.sourceKind === "approval_review" || entry.sourceKind === "agent_mail")
  ) || (
    entry.operationKind === "commit_task_notification_result" && entry.sourceKind === "task_notification"
  );
}

function extractRunCloseout(events: ThreadTurnLoadFacts["events"]): Pick<
  ThreadTurnCheckpoint,
  "executionRunId" | "interruptEventId" | "terminalCloseout" | "idleCloseout"
> {
  const running = events.filter((event) =>
    event.type === "session.status_running" || event.type === "session.thread_status_running"
  ).at(-1);
  const relevant = running === undefined
    ? events
    : events.filter((event) => event.eventSequence >= running.eventSequence);
  const interrupt = relevant.filter((event) => event.type === "user.interrupt").at(-1);
  const idle = relevant.filter((event) =>
    event.type === "session.status_idle" || event.type === "session.thread_status_idle"
  ).at(-1);
  const terminated = relevant.filter((event) =>
    event.type === "session.status_terminated" || event.type === "session.thread_status_terminated"
  ).at(-1);
  const failure = relevant.filter((event) => event.type === "session.error").at(-1);
  const runClosed = idle !== undefined || terminated !== undefined;
  const closeoutSequence = Math.max(idle?.eventSequence ?? 0, terminated?.eventSequence ?? 0);
  const activeInterrupt = interrupt !== undefined && interrupt.eventSequence > closeoutSequence
    ? interrupt
    : undefined;
  const terminalCloseout = terminated !== undefined
    ? {
        failureEventId: failure?.eventId ?? terminated.eventId,
        closeoutEventId: terminated.eventId,
        disposition: "terminated" as const,
      }
    : idle?.idle?.stopReason === "retries_exhausted" && failure?.failure?.retryStatus === "exhausted"
      ? {
          failureEventId: failure.eventId,
          closeoutEventId: idle.eventId,
          disposition: "retries_exhausted" as const,
        }
      : undefined;
  return {
    ...(!runClosed && running !== undefined ? { executionRunId: running.eventId } : {}),
    ...(activeInterrupt !== undefined ? { interruptEventId: activeInterrupt.eventId } : {}),
    ...(terminalCloseout !== undefined ? { terminalCloseout } : {}),
    ...(idle !== undefined ? { idleCloseout: { eventId: idle.eventId, stopReason: idle.idle!.stopReason } } : {}),
  };
}
