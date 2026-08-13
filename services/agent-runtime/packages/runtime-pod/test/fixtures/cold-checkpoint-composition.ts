import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { BridgeWriteStatus } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { extractColdThreadToolRouteView, extractThreadTurnCheckpoint } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { deriveThreadTurnDecision, initializeThreadTurnReduction, reduceThreadTurn } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-reducer.js";
import { DurableRuntimeMessageSchema, RuntimeMessageCreateSchema, RuntimeToolSettlementDeclarationSchema } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeDeclarationReceipt, RuntimeMessage } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { internalToolRepairKey } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { toGatewayRuntimeMessages } from "@tetral/agent-runtime-core/src/runtime/message-projection.js";
import {
  applyCompactionReceipt,
  applyInternalToolRepairReceipt,
  applyToolSettlementProjection,
  applyToolConfirmationReceipt,
} from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import { assembleProviderCallRequest } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import { lowerProviderRequest } from "../../../../../gateway/packages/lowering/src/request.js";
import { validateProviderRequest } from "../../../../../gateway/packages/protocol/src/bounds.js";
import { GatewayProviderRules } from "../../../../../gateway/packages/lowering/src/rules/index.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
  throw new Error("cold checkpoint composition input path is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
  readonly contextJson: string;
  readonly providerComposition?: boolean;
  readonly pendingToolUses?: readonly unknown[];
  readonly pendingSandboxExecutions?: readonly unknown[];
  readonly hotScenario?: {
    readonly baseContextJson: string;
    readonly receipt?: RuntimeDeclarationReceipt;
    readonly create?: unknown;
    readonly kind: "tool_confirmation" | "compaction" | "internal_repair" | "tool_settlement";
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly runtimeInputId?: string;
    readonly sourceEventId?: string;
    readonly modelRequestId?: string;
    readonly compactedThroughMessageSequence?: number;
    readonly pendingToolUses?: readonly unknown[];
    readonly pendingSandboxExecutions?: readonly unknown[];
    readonly confirmedToolUseEventId?: string;
    readonly repairKey?: string;
    readonly repairEventId?: string;
    readonly modelToolCallId?: string;
    readonly toolName?: string;
    readonly settlement?: unknown;
    readonly toolUseEventId?: string;
    readonly resultEventId?: string;
    readonly completedAt?: string;
  };
};
const rawContext = JSON.parse(input.contextJson) as { readonly messages?: readonly { readonly sessionId?: string }[] };
for (const message of rawContext.messages ?? []) {
  const parsed = DurableRuntimeMessageSchema.safeParse(message);
  if (!parsed.success) {
    throw new Error(`Bridge message does not match Runtime durable shape: ${JSON.stringify(parsed.error.issues)}`);
  }
}
const sessionId = rawContext.messages?.[0]?.sessionId ?? "composition_session";
const command = {
  requestId: "req_cold_checkpoint_composition",
  workspaceId: "default",
  sessionId,
  sessionThreadId: "composition_thread",
  bindingId: "composition_binding",
  bindingGeneration: 1,
  targetPodUid: "composition_pod",
  runtimeInputId: "composition_input",
  eventIds: [],
  sequenceFrom: 0,
  sequenceTo: 0,
  origin: "user" as const,
};
const loadContext = async (contextJson: string) => {
  const loader = new BridgeAPIContextLoader({
    address: "unused.test",
    tokenPath: "/unused/token",
    metadataFactory: async () => new Metadata(),
    client: {
      loadContext: (_request: unknown, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void) => {
        callback(null, {
          ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED },
          contextJson,
          runtimeBindingToken: "composition-binding-token",
        });
      },
    } as unknown as AgentRuntimeBridgeServiceClient,
  });
  return loader.loadThreadContext(command);
};
const context = await loadContext(input.contextJson);
const checkpoint = extractThreadTurnCheckpoint({ messages: context.messages, facts: context.turnFacts });
const toolRouteView = extractColdThreadToolRouteView({
  checkpoint,
  pendingToolUses: (input.pendingToolUses ?? context.pendingToolUses ?? []) as never,
  pendingSandboxExecutions: (input.pendingSandboxExecutions ?? context.pendingSandboxExecutions ?? []) as never,
});
let hot: {
  readonly checkpoint: unknown;
  readonly toolRouteView: unknown;
  readonly reducerAction: unknown;
  readonly toolPart?: unknown;
  readonly providerComposition?: unknown;
} | undefined;
if (input.hotScenario !== undefined) {
  const scenario = input.hotScenario;
  const base = await loadContext(scenario.baseContextJson);
  const baseCheckpoint = extractThreadTurnCheckpoint({ messages: base.messages, facts: base.turnFacts });
  let hotRouteView = extractColdThreadToolRouteView({
    checkpoint: baseCheckpoint,
    pendingToolUses: (scenario.pendingToolUses ?? base.pendingToolUses ?? []) as never,
    pendingSandboxExecutions: (scenario.pendingSandboxExecutions ?? base.pendingSandboxExecutions ?? []) as never,
  });
  let reduction = initializeThreadTurnReduction(baseCheckpoint, hotRouteView);
  if (scenario.kind === "tool_settlement") {
    if (
      scenario.toolUseEventId === undefined || scenario.resultEventId === undefined ||
      scenario.completedAt === undefined || scenario.settlement === undefined
    ) {
      throw new Error("Tool settlement hot composition identity is incomplete");
    }
    const declaration = RuntimeToolSettlementDeclarationSchema.parse({
      toolUseEventId: scenario.toolUseEventId,
      outcome: scenario.settlement,
    });
    const projectedMessages = applyToolSettlementProjection(
      base.messages,
      scenario.toolUseEventId,
      declaration.outcome,
      scenario.completedAt,
    );
    reduction = reduceThreadTurn(reduction, {
      fact: "tool_result_committed",
      eventId: scenario.resultEventId,
      toolUseEventId: scenario.toolUseEventId,
      outcome: declaration.outcome.type === "completed" ? "success" : declaration.outcome.type,
    }, hotRouteView);
    hotRouteView = {
      routes: hotRouteView.routes.filter((route) => route.toolUseEventId !== scenario.toolUseEventId),
    };
    const toolPart = projectedMessages.flatMap((message) => message.parts)
      .find((part) => part.type === "tool" && part.toolUseEventId === scenario.toolUseEventId);
    if (toolPart === undefined) throw new Error("hot Tool settlement projection is missing its Tool part");
    hot = {
      checkpoint: reduction.checkpoint,
      toolRouteView: hotRouteView,
      reducerAction: deriveThreadTurnDecision(reduction.checkpoint, hotRouteView).action,
      toolPart,
      ...(input.providerComposition === true
        ? { providerComposition: composeProviderRequests(projectedMessages) }
        : {}),
    };
  } else {
    const create = RuntimeMessageCreateSchema.parse(scenario.create);
    const receipt = scenario.receipt;
    if (receipt === undefined) throw new Error("hot composition receipt is missing");
    let hotProviderMessages: readonly RuntimeMessage[] | undefined;
    if (scenario.kind === "tool_confirmation") {
    if (scenario.runtimeInputId === undefined || scenario.sourceEventId === undefined || scenario.confirmedToolUseEventId === undefined) {
      throw new Error("tool confirmation hot composition identity is incomplete");
    }
    const message = applyToolConfirmationReceipt({
      sessionId: scenario.sessionId,
      sessionThreadId: scenario.sessionThreadId,
      runtimeInputId: scenario.runtimeInputId,
      sourceEventId: scenario.sourceEventId,
      create,
    }, receipt);
    hotRouteView = {
      routes: hotRouteView.routes.map((route) => route.toolUseEventId === scenario.confirmedToolUseEventId
        ? { ...route, disposition: "resume_approval_settlement" as const }
        : route),
    };
    reduction = reduceThreadTurn(reduction, {
      fact: "inputs_committed",
      eventId: receipt.events[0]!.eventId,
      messageIds: [message.id],
    }, hotRouteView);
    } else if (scenario.kind === "compaction") {
    if (scenario.modelRequestId === undefined || scenario.compactedThroughMessageSequence === undefined) {
      throw new Error("compaction hot composition identity is incomplete");
    }
    const requestEnd = receipt.events[0]!;
    const checkpointMessage = applyCompactionReceipt({
      sessionId: scenario.sessionId,
      sessionThreadId: scenario.sessionThreadId,
      modelRequestId: scenario.modelRequestId,
      requestEndEventId: requestEnd.eventId,
      compactedThroughMessageSequence: scenario.compactedThroughMessageSequence,
      create,
    }, receipt);
    reduction = reduceThreadTurn(reduction, {
      fact: "request_ended",
      eventId: requestEnd.eventId,
      modelRequestId: scenario.modelRequestId,
      isError: false,
      rescheduled: false,
    }, hotRouteView);
    reduction = reduceThreadTurn(reduction, {
      fact: "inputs_committed",
      eventId: receipt.events[1]!.eventId,
      messageIds: [checkpointMessage.id],
    }, hotRouteView);
    } else {
    if (
      scenario.modelRequestId === undefined || scenario.repairKey === undefined ||
      scenario.repairEventId === undefined || scenario.modelToolCallId === undefined ||
      scenario.toolName === undefined
    ) {
      throw new Error("internal repair hot composition identity is incomplete");
    }
    const repairMessage = applyInternalToolRepairReceipt({
      sessionId: scenario.sessionId,
      sessionThreadId: scenario.sessionThreadId,
      repairKey: scenario.repairKey,
      eventId: scenario.repairEventId,
      create,
    }, receipt);
    hotProviderMessages = [...base.messages, repairMessage];
    reduction = reduceThreadTurn(reduction, {
      fact: "internal_tool_repair_committed",
      eventId: scenario.repairEventId,
      modelRequestId: scenario.modelRequestId,
      modelToolCallId: scenario.modelToolCallId,
      toolName: scenario.toolName,
    }, hotRouteView);
    }
    hot = {
      checkpoint: reduction.checkpoint,
      toolRouteView: hotRouteView,
      reducerAction: reduction.action,
      ...(input.providerComposition === true
        ? { providerComposition: composeProviderRequests(hotProviderMessages ?? base.messages) }
        : {}),
    };
  }
}

process.stdout.write(JSON.stringify({
  checkpoint,
  toolRouteView,
  reducerAction: deriveThreadTurnDecision(checkpoint, toolRouteView).action,
  derivedRepairKeys: (checkpoint.request?.toolMembers ?? []).flatMap((member) =>
    member.memberKind === "internal_tool_repair"
      ? [internalToolRepairKey(checkpoint.request!.modelRequestId, member.modelToolCallId, member.toolName)]
      : []
  ),
  ...(input.providerComposition === true
    ? { providerComposition: composeProviderRequests(context.messages) }
    : {}),
  ...(hot === undefined ? {} : { hot }),
}));

function composeProviderRequests(messages: readonly RuntimeMessage[]) {
  const projected = toGatewayRuntimeMessages(messages);
  if (!projected.ok) {
    throw new Error(`Runtime provider projection failed: ${projected.error.code}`);
  }
  const strategies = GatewayProviderRules.map((rules) => {
    const assembled = assembleProviderCallRequest({
      identity: {
        workspaceId: "default",
        sessionId: messages[0]?.sessionId ?? "composition_session",
        sessionThreadId: "composition_thread",
        bindingId: "composition_binding",
        bindingGeneration: 1,
        targetPodUid: "composition_pod",
        runtimeBindingToken: "composition-binding-token",
      },
      requestId: "req_provider_composition",
      modelRequestId: "mreq_provider_composition",
      currentModel: { providerId: rules.providerId, modelId: rules.modelId },
      runtimeMessages: projected.messages,
      runtime: { systemInstructions: "provider composition", timeoutMs: 30_000 },
    });
    if (!assembled.ok) {
      throw new Error(`Runtime ProviderRequest assembly failed for ${rules.providerId}/${rules.modelId}`);
    }
    const validation = validateProviderRequest(assembled.request);
    return {
      providerId: rules.providerId,
      modelId: rules.modelId,
      providerFamily: rules.providerFamily,
      validation,
      ...(validation.ok
        ? {
            providerRequest: assembled.request,
            loweredMessages: lowerProviderRequest(assembled.request, rules, { modelOutputTokenLimit: 32_000 }).messages,
          }
        : {}),
    };
  });
  const carrierMessages = projected.messages;
  const toolParts = carrierMessages.flatMap((message) =>
    message.parts.flatMap((part) => part.tool === undefined ? [] : [part.tool])
  );
  return {
    carrierMessages,
    carrierHasToolUseEventIdProperty: toolParts.some((tool) => Object.hasOwn(tool, "toolUseEventId")),
    strategies,
  };
}
