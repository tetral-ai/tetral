import { z } from "zod/v4";

const DurableIdentitySchema = z.string().min(1);

const TerminalResultSchema = z.strictObject({
  resultEventId: DurableIdentitySchema,
  outcome: z.enum(["success", "error", "cancelled", "unknown"]),
});

const PublicToolUseMemberSchema = z.strictObject({
  memberKind: z.literal("public_tool_use"),
  modelToolCallId: DurableIdentitySchema,
  toolUseEventId: DurableIdentitySchema,
  toolName: DurableIdentitySchema,
  terminalResult: TerminalResultSchema.optional(),
});

const InternalToolRepairMemberSchema = z.strictObject({
  memberKind: z.literal("internal_tool_repair"),
  modelToolCallId: DurableIdentitySchema,
  toolName: DurableIdentitySchema,
  repairEventId: DurableIdentitySchema,
  outcome: z.literal("error"),
});

const RequestSchema = z.strictObject({
  modelRequestId: DurableIdentitySchema,
  requestStartEventId: DurableIdentitySchema,
  requestKind: z.enum([
    "agent_provider_request",
    "compaction_summary",
    "approval_reviewer",
  ]),
  contextThroughMessageSequence: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  requestEnd: z.strictObject({
    eventId: DurableIdentitySchema,
    isError: z.boolean(),
    errorKind: DurableIdentitySchema.optional(),
    rescheduled: z.boolean(),
  }).optional(),
  toolMembers: z.array(z.discriminatedUnion("memberKind", [
    PublicToolUseMemberSchema,
    InternalToolRepairMemberSchema,
  ])),
}).superRefine((request, context) => {
  const modelToolCallIds = new Set<string>();
  const toolUseEventIds = new Set<string>();
  const terminalResultEventIds = new Set<string>();

  for (const member of request.toolMembers) {
    if (modelToolCallIds.has(member.modelToolCallId)) {
      context.addIssue({
        code: "custom",
        message: "modelToolCallId must be unique within a request",
        path: ["toolMembers"],
      });
    }
    modelToolCallIds.add(member.modelToolCallId);

    if (member.memberKind === "internal_tool_repair") {
      continue;
    }
    if (toolUseEventIds.has(member.toolUseEventId)) {
      context.addIssue({
        code: "custom",
        message: "toolUseEventId must be unique within a request",
        path: ["toolMembers"],
      });
    }
    toolUseEventIds.add(member.toolUseEventId);

    if (member.terminalResult !== undefined) {
      if (terminalResultEventIds.has(member.terminalResult.resultEventId)) {
        context.addIssue({
          code: "custom",
          message: "resultEventId must be unique within a request",
          path: ["toolMembers"],
        });
      }
      terminalResultEventIds.add(member.terminalResult.resultEventId);
    }
  }
});

export const ThreadTurnCheckpointSchema = z.strictObject({
  executionRunId: DurableIdentitySchema.optional(),
  pendingInputMessageIds: z.array(DurableIdentitySchema),
  request: RequestSchema.optional(),
  interruptEventId: DurableIdentitySchema.optional(),
  terminalCloseout: z.strictObject({
    failureEventId: DurableIdentitySchema,
    closeoutEventId: DurableIdentitySchema,
    disposition: z.enum(["retries_exhausted", "terminated"]),
  }).optional(),
  idleCloseout: z.strictObject({
    eventId: DurableIdentitySchema,
    stopReason: DurableIdentitySchema,
  }).optional(),
}).superRefine((checkpoint, context) => {
  if (new Set(checkpoint.pendingInputMessageIds).size !== checkpoint.pendingInputMessageIds.length) {
    context.addIssue({
      code: "custom",
      message: "pendingInputMessageIds must be unique",
      path: ["pendingInputMessageIds"],
    });
  }
  if (checkpoint.idleCloseout?.stopReason === "requires_action") {
    const request = checkpoint.request;
    const hasIncompletePublicTool = request?.requestEnd !== undefined && request.toolMembers.some(
      (member) => member.memberKind === "public_tool_use" && member.terminalResult === undefined,
    );
    if (!hasIncompletePublicTool) {
      context.addIssue({
        code: "custom",
        message: "requires_action closeout must retain a sealed incomplete Tool Use",
        path: ["idleCloseout"],
      });
    }
  }
});

const ThreadToolRouteSchema = z.strictObject({
  toolUseEventId: DurableIdentitySchema,
  disposition: z.enum([
    "hot_execution",
    "requires_user_action",
    "resume_approval_settlement",
    "resume_sandbox_execution",
  ]),
});

export const ThreadToolRouteViewSchema = z.strictObject({
  routes: z.array(ThreadToolRouteSchema),
}).superRefine((view, context) => {
  const toolUseEventIds = new Set<string>();
  for (const route of view.routes) {
    if (toolUseEventIds.has(route.toolUseEventId)) {
      context.addIssue({
        code: "custom",
        message: "tool route ownership must be unique",
        path: ["routes"],
      });
    }
    toolUseEventIds.add(route.toolUseEventId);
  }
});

export interface ThreadTurnCheckpoint {
  readonly executionRunId?: string;
  readonly pendingInputMessageIds: readonly string[];
  readonly request?: {
    readonly modelRequestId: string;
    readonly requestStartEventId: string;
    readonly requestKind:
      | "agent_provider_request"
      | "compaction_summary"
      | "approval_reviewer";
    readonly contextThroughMessageSequence: number;
    readonly requestEnd?: {
      readonly eventId: string;
      readonly isError: boolean;
      readonly errorKind?: string;
      readonly rescheduled: boolean;
    };
    readonly toolMembers: readonly (
      | {
          readonly memberKind: "public_tool_use";
          readonly modelToolCallId: string;
          readonly toolUseEventId: string;
          readonly toolName: string;
          readonly terminalResult?: {
            readonly resultEventId: string;
            readonly outcome: "success" | "error" | "cancelled" | "unknown";
          };
        }
      | {
          readonly memberKind: "internal_tool_repair";
          readonly modelToolCallId: string;
          readonly toolName: string;
          readonly repairEventId: string;
          readonly outcome: "error";
        }
    )[];
  };
  readonly interruptEventId?: string;
  readonly terminalCloseout?: {
    readonly failureEventId: string;
    readonly closeoutEventId: string;
    readonly disposition: "retries_exhausted" | "terminated";
  };
  readonly idleCloseout?: {
    readonly eventId: string;
    readonly stopReason: string;
  };
}

export interface ThreadToolRouteView {
  readonly routes: readonly {
    readonly toolUseEventId: string;
    readonly disposition:
      | "hot_execution"
      | "requires_user_action"
      | "resume_approval_settlement"
      | "resume_sandbox_execution";
  }[];
}

export function parseThreadTurnCheckpoint(input: unknown): ThreadTurnCheckpoint {
  const parsed = ThreadTurnCheckpointSchema.parse(input);
  return {
    ...(parsed.executionRunId !== undefined ? { executionRunId: parsed.executionRunId } : {}),
    pendingInputMessageIds: parsed.pendingInputMessageIds,
    ...(parsed.request !== undefined
      ? {
          request: {
            modelRequestId: parsed.request.modelRequestId,
            requestStartEventId: parsed.request.requestStartEventId,
            requestKind: parsed.request.requestKind,
            contextThroughMessageSequence: parsed.request.contextThroughMessageSequence,
            ...(parsed.request.requestEnd !== undefined
              ? {
                  requestEnd: {
                    eventId: parsed.request.requestEnd.eventId,
                    isError: parsed.request.requestEnd.isError,
                    ...(parsed.request.requestEnd.errorKind !== undefined
                      ? { errorKind: parsed.request.requestEnd.errorKind }
                      : {}),
                    rescheduled: parsed.request.requestEnd.rescheduled,
                  },
                }
              : {}),
            toolMembers: parsed.request.toolMembers.map((member) => {
              if (member.memberKind === "internal_tool_repair") {
                return member;
              }
              return {
                memberKind: member.memberKind,
                modelToolCallId: member.modelToolCallId,
                toolUseEventId: member.toolUseEventId,
                toolName: member.toolName,
                ...(member.terminalResult !== undefined
                  ? { terminalResult: member.terminalResult }
                  : {}),
              };
            }),
          },
        }
      : {}),
    ...(parsed.interruptEventId !== undefined ? { interruptEventId: parsed.interruptEventId } : {}),
    ...(parsed.terminalCloseout !== undefined ? { terminalCloseout: parsed.terminalCloseout } : {}),
    ...(parsed.idleCloseout !== undefined ? { idleCloseout: parsed.idleCloseout } : {}),
  };
}

export function parseThreadToolRouteView(input: unknown): ThreadToolRouteView {
  return ThreadToolRouteViewSchema.parse(input);
}
