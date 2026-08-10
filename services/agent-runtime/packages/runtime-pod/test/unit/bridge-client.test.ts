import { describe, expect, test } from "bun:test";
import { Metadata, status } from "@grpc/grpc-js";
import type { CallOptions } from "@grpc/grpc-js";
import {
  BridgeWriteStatus,
  DurableEventDisposition,
  DurableProjectionDisposition,
  ReceiptApplicationDisposition,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  AgentRuntimeBridgeServiceClient,
  CommitInputsRequest,
  CommitInternalToolRepairRequest,
  CommitRuntimeTerminationRequest,
  CommitTaskNotificationResultRequest,
  FinishIdleRequest,
  LoadContextRequest,
  RuntimeMessageCreate,
  RuntimePartCreate,
  RuntimeScope,
  WriteEventRequest,
  WriteRequestEndRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  RuntimeAcceptedInputState,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import {
  acceptedInputCreates,
  runtimeInternalToolRepairCreate,
  taskNotificationOperationId,
} from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import type {
  RuntimeInternalToolRepairCommit,
  SessionEventEnvelope,
  SessionEventWriterFinishIdleEnvelope,
  SessionEventWriterRequestEndEnvelope,
  SessionEventWriterRuntimeTerminationEnvelope,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import {
  BridgeAPIContextLoader,
  BridgeAPIControlInputCommitter,
  BridgeAPIEventWriter,
  BridgeAPIInternalToolRepairCommitter,
} from "../../src/bridge-client.js";
import {
  commitInputsDeclarationDigest,
  finishIdleDeclarationDigest,
  internalToolRepairDeclarationDigest,
  runtimeTerminationDeclarationDigest,
  taskNotificationDeclarationDigest,
  writeEventDeclarationDigest,
  writeRequestEndDeclarationDigest,
} from "../../src/runtime-declaration-wire.js";
import {
  buildBridgeClientRuntimeMessage as runtimeMessage,
} from "../../../core/test/unit/runtime-message-builders.js";

const now = "2026-08-08T00:00:00.000Z";

describe("Bridge runtime declaration adapters", () => {
  test("loads a populated cold context through the production context loader", async () => {
    const bridge = new DeclarationBridge();
    const loader = new BridgeAPIContextLoader(options(bridge));

    const loaded = await loader.loadThreadContext(control("rin_load"));

    expect(bridge.loadContextRequests).toHaveLength(1);
    expect(loaded.messages).toEqual([]);
    expect(loaded.coldCoverage).toEqual({
      pendingToolIds: [],
      pendingSandboxExecutionIds: [],
      pendingAttachmentIdentities: [],
      undeliveredMailDeliveryIds: [],
    });
  });

  test("commits accepted input as positional message creates", async () => {
    const bridge = new DeclarationBridge();
    const loader = new BridgeAPIContextLoader(options(bridge));
    const input = acceptedInput("rin_message");
    const creates = acceptedInputCreates(input);

    const result = await loader.commitAcceptedInput(input, { messageCreates: creates });

    expect(result).toMatchObject({ type: "receipt", inputDisposition: "committed", applicationDisposition: "current_custody" });
    if (result.type !== "receipt") {
      throw new Error("expected accepted-input receipt");
    }
    expect(result.receipt.operationId).toBe(input.runtimeInputId);
    expect(result.receipt.messages).toHaveLength(1);
    expect(bridge.commitInputsRequests[0]?.messageCreates).toHaveLength(1);
    expect(bridge.commitInputsRequests[0]?.messageCreates[0]).not.toHaveProperty("runtimeLocalId");
  });

  test("retries live task-notification lease contention and converges after custody settles", async () => {
    const bridge = new DeclarationBridge();
    bridge.taskNotificationErrors.push(status.ABORTED);
    const loader = new BridgeAPIContextLoader(options(bridge));
    const input = taskNotificationInput("rin_task_live_lease");
    const messageCreates = acceptedInputCreates(input);

    await expect(loader.commitAcceptedInput(input, { messageCreates })).rejects.toMatchObject({
      type: "context-loader",
      code: "unavailable",
      retryable: true,
      fatal: false,
    });
    const result = await loader.commitAcceptedInput(input, { messageCreates });

    expect(result).toMatchObject({ type: "receipt", inputDisposition: "committed" });
    expect(bridge.commitTaskNotificationRequests).toHaveLength(2);
  });

  test("treats missing task-notification Queue custody as terminal without retry", async () => {
    const bridge = new DeclarationBridge();
    bridge.taskNotificationErrors.push(status.FAILED_PRECONDITION);
    const loader = new BridgeAPIContextLoader(options(bridge));
    const input = taskNotificationInput("rin_task_missing_custody");

    await expect(loader.commitAcceptedInput(input, { messageCreates: acceptedInputCreates(input) })).rejects.toMatchObject({
      type: "context-loader",
      code: "schema_mismatch",
      retryable: false,
      fatal: true,
    });
    expect(bridge.commitTaskNotificationRequests).toHaveLength(1);
  });

  test("commits interrupt intent without a client-owned Tool census", async () => {
    const bridge = new DeclarationBridge();
    const committer = new BridgeAPIControlInputCommitter(options(bridge));
    const scope = control("rin_interrupt");

    const result = await committer.commitControlInput({
      scope,
      inputKind: "interrupt_control",
      messageCreates: [],
    });

    expect(result).toMatchObject({ ok: true, receipt: { operationId: "rin_interrupt", messages: [] } });
    expect(bridge.commitInputsRequests[0]).toMatchObject({
      runtimeInputId: "rin_interrupt",
      inputKind: "interrupt_control",
      messageCreates: [],
    });
  });

  test("projects Assistant append and Tool settlement as disjoint WriteEvent shapes", async () => {
    const bridge = new DeclarationBridge();
    const writer = new BridgeAPIEventWriter(options(bridge));
    const appendEnvelope: SessionEventEnvelope = {
      ...eventScope("write_member"),
      modelRequestId: "mrq_1",
      event: { type: "agent.message", content: [{ type: "text", text: "hello" }] },
      assistantPartAppend: {
        parts: [{ type: "text", text: "hello", truncated: false, status: "completed" }],
      },
    };

    const append = await writer.append(appendEnvelope);
    const settlement = await writer.append({
      ...eventScope("write_result"),
      modelRequestId: "mrq_1",
      event: { type: "agent.tool_result", tool_use_id: "sevt_tool_1", content: [{ type: "text", text: "done" }] },
      toolSettlement: {
        toolUseEventId: "sevt_tool_1",
        outcome: { type: "completed", output: { text: "done", truncated: false } },
      },
    });

    expect(append).toMatchObject({ ok: true, declaration: { receipt: { messages: [{ disposition: "created" }] } } });
    expect(settlement).toMatchObject({ ok: true, declaration: { receipt: { messages: [] } } });
    expect(bridge.writeEventRequests[0]?.assistantPartAppend?.parts).toHaveLength(1);
    expect(bridge.writeEventRequests[0]?.toolSettlement).toBeUndefined();
    expect(bridge.writeEventRequests[1]?.assistantPartAppend).toBeUndefined();
    expect(bridge.writeEventRequests[1]?.toolSettlement?.toolUseEventId).toBe("sevt_tool_1");
  });

  test("matches RequestEnd and joined interrupt receipts by operation identity", async () => {
    const bridge = new DeclarationBridge();
    const writer = new BridgeAPIEventWriter(options(bridge));
    const envelope: SessionEventWriterRequestEndEnvelope = {
      ...eventScope("write_request_end"),
      modelRequestId: "mrq_interrupt",
      modelRequestStartEventId: "sevt_request_start",
      isError: true,
      errorKind: "runtime_interrupted",
      finishReason: "cancelled",
      interruptSettlement: {
        runtimeInputId: "rin_interrupt_joined",
        eventIds: ["sevt_interrupt"],
        sequenceFrom: 8,
        sequenceTo: 8,
      },
    };

    const result = await writer.writeRequestEnd(envelope);

    expect(result).toMatchObject({
      ok: true,
      declaration: {
        receipt: { operationKind: "write_request_end", operationId: "mrq_interrupt" },
        relatedReceipts: [{ operationKind: "commit_inputs", operationId: "rin_interrupt_joined" }],
      },
    });
    expect(bridge.writeRequestEndRequests[0]?.interruptSettlement).toEqual({
      runtimeInputId: "rin_interrupt_joined",
      eventIds: ["sevt_interrupt"],
      sequenceFrom: 8,
      sequenceTo: 8,
    });
  });

  test("keeps trailing Assistant content on RequestEnd instead of a stable-reasoning side channel", async () => {
    const bridge = new DeclarationBridge();
    const writer = new BridgeAPIEventWriter(options(bridge));
    const envelope: SessionEventWriterRequestEndEnvelope = {
      ...eventScope("write_request_end_reasoning"),
      modelRequestId: "mrq_reasoning",
      modelRequestStartEventId: "sevt_reasoning_start",
      isError: false,
      finishReason: "stop",
      trailingPartAppend: {
        parts: [{
          type: "reasoning",
          text: "durable reasoning",
          providerPartId: "reasoning_1",
          providerMetadata: { openai: { encrypted_content: "cipher" } },
          truncated: false,
          status: "completed",
        }],
      },
    };

    const result = await writer.writeRequestEnd(envelope);

    expect(result).toMatchObject({ ok: true, declaration: { receipt: { messages: [{ parts: [{}] }] } } });
    expect(bridge.writeRequestEndRequests[0]?.trailingPartAppend?.parts).toHaveLength(1);
    expect(bridge.writeRequestEndRequests[0]).not.toHaveProperty("stableReasoningParts");
  });

  test("serializes idle completion create and atomic termination settlements", async () => {
    const bridge = new DeclarationBridge();
    const writer = new BridgeAPIEventWriter(options(bridge));
    const completionCreate = {
      messageKind: "completion_mail" as const,
      role: "user" as const,
      origin: "runtime" as const,
      status: "completed" as const,
      parts: [{ type: "text" as const, text: "complete", truncated: false, status: "completed" as const }],
    };
    const idleEnvelope: SessionEventWriterFinishIdleEnvelope = {
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thrd_1",
      bindingId: "bind_1",
      bindingGeneration: 3,
      targetPodUid: "pod_1",
      durableTurnId: "turn_1",
      stopReason: { type: "end_turn" },
      completionMailCreate: completionCreate,
    };
    const terminationEnvelope: SessionEventWriterRuntimeTerminationEnvelope = {
      ...eventScope("terminate_1"),
      failure: {
        type: "runtime",
        code: "runtime_invalid_sequence",
        message: "terminated",
        retryable: false,
        fatal: true,
        reason: "runtime_shutdown",
      },
      toolSettlements: [{
        toolUseEventId: "sevt_tool_open",
        outcome: { type: "cancelled" },
      }],
      completionMailCreate: completionCreate,
    };

    const idle = await writer.finishIdle(idleEnvelope);
    const terminated = await writer.commitRuntimeTermination(terminationEnvelope);

    expect(idle).toMatchObject({ ok: true, declaration: { receipt: { idleCloseout: { durableTurnId: "turn_1" } } } });
    expect(terminated).toMatchObject({ ok: true, declaration: { receipt: { operationId: "terminate_1" } } });
    expect(bridge.finishIdleRequests[0]?.completionMailCreate).toBeDefined();
    expect(bridge.terminationRequests[0]?.toolSettlements).toHaveLength(1);
    expect(bridge.terminationRequests[0]?.completionMailCreate).toBeDefined();
  });

  test("commits one identity-free internal repair create", async () => {
    const bridge = new DeclarationBridge();
    bridge.repairOperationId = "repair_1";
    const committer = new BridgeAPIInternalToolRepairCommitter(options(bridge));
    const messageCreate = runtimeInternalToolRepairCreate({
      part: {
        type: "tool",
        toolCallId: "call_invalid",
        toolName: "missing_tool",
        state: {
          status: "error",
          error: { type: "tool_error", message: "unknown tool", retryable: false },
        },
        startedAt: now,
        completedAt: now,
      },
    });
    const repair: RuntimeInternalToolRepairCommit = {
      ...eventScope("unused_write"),
      modelRequestId: "mrq_1",
      modelToolCallId: "call_invalid",
      toolName: "missing_tool",
      repairKey: "repair_1",
      messageCreate,
    };

    const result = await committer.commitInternalToolRepair(repair);

    expect(result).toMatchObject({ ok: true, declaration: { receipt: { operationId: "repair_1" } } });
    expect(bridge.repairRequests[0]?.messageCreate).toBeDefined();
    expect(bridge.repairRequests[0]?.messageCreate).not.toHaveProperty("runtimeLocalId");
  });
});

function options(bridge: DeclarationBridge) {
  return {
    address: "bridge.test:9090",
    tokenPath: "/var/run/token",
    client: bridge.client(),
    metadataFactory: async () => new Metadata(),
    sleep: async () => undefined,
  };
}

function control(runtimeInputId: string) {
  return {
    requestId: `req_${runtimeInputId}`,
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 3,
    targetPodUid: "pod_1",
    runtimeInputId,
    eventIds: [`sevt_${runtimeInputId}`],
    sequenceFrom: 7,
    sequenceTo: 7,
  };
}

function acceptedInput(runtimeInputId: string): RuntimeAcceptedInputState {
  return {
    ...control(runtimeInputId),
    kind: "messages",
    payloadJson: JSON.stringify({ messages: [runtimeMessage(`msg_${runtimeInputId}`, "hello")] }),
  };
}

function taskNotificationInput(runtimeInputId: string): Extract<RuntimeAcceptedInputState, { readonly kind: "task_notification" }> {
  return {
    ...control(runtimeInputId),
    kind: "task_notification",
    taskId: `task_${runtimeInputId}`,
    sourceToolUseEventId: `sevt_tool_${runtimeInputId}`,
    status: "completed",
    payloadJson: JSON.stringify({ status: "completed", text: "background task completed" }),
  };
}

function eventScope(writeId: string) {
  return {
    requestId: `req_${writeId}`,
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 3,
    targetPodUid: "pod_1",
    writeId,
  };
}

class DeclarationBridge {
  readonly loadContextRequests: LoadContextRequest[] = [];
  readonly commitInputsRequests: CommitInputsRequest[] = [];
  readonly writeEventRequests: WriteEventRequest[] = [];
  readonly writeRequestEndRequests: WriteRequestEndRequest[] = [];
  readonly finishIdleRequests: FinishIdleRequest[] = [];
  readonly terminationRequests: CommitRuntimeTerminationRequest[] = [];
  readonly commitTaskNotificationRequests: CommitTaskNotificationResultRequest[] = [];
  readonly repairRequests: CommitInternalToolRepairRequest[] = [];
  readonly taskNotificationErrors: number[] = [];
  repairOperationId = "repair";
  private eventSequence = 0;
  private messageSequence = 0;

  client(): AgentRuntimeBridgeServiceClient {
    return {
      loadContext: this.loadContext.bind(this),
      commitInputs: this.commitInputs.bind(this),
      writeEvent: this.writeEvent.bind(this),
      writeRequestEnd: this.writeRequestEnd.bind(this),
      finishIdle: this.finishIdle.bind(this),
      commitRuntimeTermination: this.commitRuntimeTermination.bind(this),
      commitTaskNotificationResult: this.commitTaskNotificationResult.bind(this),
      commitInternalToolRepair: this.commitInternalToolRepair.bind(this),
    } as unknown as AgentRuntimeBridgeServiceClient;
  }

  private loadContext(request: LoadContextRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.loadContextRequests.push(request);
    callback(null, {
      ack: ack("", BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED),
      contextJson: JSON.stringify({
        messages: [],
        turnFacts: { events: [], messageLineage: [] },
        coldCoverage: {
          pendingToolIds: [],
          pendingSandboxExecutionIds: [],
          pendingAttachmentIdentities: [],
          undeliveredMailDeliveryIds: [],
        },
        thread: { parentThreadId: null, role: "main", visibility: "public", taskName: null, agentType: "general", status: "idle" },
      }),
      runtimeBindingToken: "binding-token",
    });
    return grpcCall();
  }

  private commitInputs(request: CommitInputsRequest, _metadata: Metadata, _options: CallOptions, callback: (error: Error | null, response: unknown) => void): unknown {
    this.commitInputsRequests.push(request);
    const events = request.eventIds.map((eventId, index) => this.event(request, eventId, request.sequenceFrom + index, "existing"));
    callback(null, response(request, request.runtimeInputId, [receipt({
      request,
      operationKind: "commit_inputs",
      sourceKind: request.inputKind,
      operationId: request.runtimeInputId,
      digest: commitInputsDeclarationDigest(request),
      events,
      messages: request.messageCreates.map((create, index) => this.message(request, create, events[index]!.eventId)),
    })]));
    return grpcCall();
  }

  private commitTaskNotificationResult(
    request: CommitTaskNotificationResultRequest,
    _metadata: Metadata,
    callback: (error: Error | null, response: unknown) => void,
  ): unknown {
    this.commitTaskNotificationRequests.push(request);
    const errorCode = this.taskNotificationErrors.shift();
    if (errorCode !== undefined) {
      callback(Object.assign(new Error("task notification commit rejected"), { code: errorCode }), {});
      return grpcCall();
    }
    const event = this.event(request, `sevt_${request.runtimeInputId}`);
    const operationId = taskNotificationOperationId(request.runtimeInputId, request.taskId);
    callback(null, response(request, request.runtimeInputId, [receipt({
      request,
      operationKind: "commit_task_notification_result",
      sourceKind: "task_notification",
      operationId,
      digest: taskNotificationDeclarationDigest(request),
      events: [event],
      messages: request.messageCreate === undefined ? [] : [this.message(request, request.messageCreate, event.eventId)],
    })]));
    return grpcCall();
  }

  private writeEvent(request: WriteEventRequest, _metadata: Metadata, callback: (error: Error | null, responseValue: unknown) => void): unknown {
    this.writeEventRequests.push(request);
    const event = this.event(request, `sevt_${request.runtimeWriteId}`);
    const messages = request.assistantPartAppend === undefined
      ? []
      : [this.appendMessage(request, request.assistantPartAppend.parts, event.eventId)];
    const declarationReceipt = receipt({
      request,
      operationKind: "write_event",
      sourceKind: request.eventType,
      operationId: request.runtimeWriteId,
      digest: writeEventDeclarationDigest(request),
      events: [event],
      messages,
      ...(request.eventType === "span.model_request_start" ? {
        requestStart: { requestKind: request.requestKind, contextThroughMessageSequence: request.contextThroughMessageSequence ?? 0 },
      } : {}),
    });
    callback(null, {
      ...response(request, request.runtimeWriteId, [declarationReceipt]),
      eventId: event.eventId,
      sequence: event.eventSequence,
    });
    return grpcCall();
  }

  private writeRequestEnd(request: WriteRequestEndRequest, _metadata: Metadata, callback: (error: Error | null, responseValue: unknown) => void): unknown {
    this.writeRequestEndRequests.push(request);
    const event = this.event(request, `sevt_${request.runtimeWriteId}`);
    const main = receipt({
      request,
      operationKind: "write_request_end",
      sourceKind: "model_request",
      operationId: request.modelRequestId,
      digest: writeRequestEndDeclarationDigest(request),
      events: [event],
      messages: request.trailingPartAppend === undefined
        ? []
        : [this.appendMessage(request, request.trailingPartAppend.parts, event.eventId)],
    });
    const interrupt = request.interruptSettlement === undefined ? [] : [receipt({
      request,
      operationKind: "commit_inputs",
      sourceKind: "interrupt_control",
      operationId: request.interruptSettlement.runtimeInputId,
      digest: commitInputsDeclarationDigest({
        scope: request.scope,
        runtimeInputId: request.interruptSettlement.runtimeInputId,
        eventIds: request.interruptSettlement.eventIds,
        sequenceFrom: request.interruptSettlement.sequenceFrom,
        sequenceTo: request.interruptSettlement.sequenceTo,
        inputKind: "interrupt_control",
        messageCreates: [],
      }),
      events: request.interruptSettlement.eventIds.map((eventId, index) => this.event(
        request,
        eventId,
        request.interruptSettlement!.sequenceFrom + index,
        "existing",
      )),
      messages: [],
    })];
    callback(null, response(request, request.runtimeWriteId, [...interrupt, main]));
    return grpcCall();
  }

  private finishIdle(request: FinishIdleRequest, _metadata: Metadata, callback: (error: Error | null, responseValue: unknown) => void): unknown {
    this.finishIdleRequests.push(request);
    const idleEvent = this.event(request, `sevt_idle_${request.durableTurnId}`);
    const completionEvent = request.completionMailCreate === undefined ? undefined : this.event(request, `sevt_completion_${request.durableTurnId}`);
    const messages = request.completionMailCreate === undefined || completionEvent === undefined
      ? []
      : [this.message(request, request.completionMailCreate, completionEvent.eventId)];
    callback(null, response(request, request.durableTurnId, [receipt({
      request,
      operationKind: "finish_idle",
      sourceKind: "turn_closeout",
      operationId: request.durableTurnId,
      digest: finishIdleDeclarationDigest(request),
      events: completionEvent === undefined ? [idleEvent] : [idleEvent, completionEvent],
      messages,
      idleCloseout: {
        durableTurnId: request.durableTurnId,
        idleEventId: idleEvent.eventId,
        idleEventSequence: idleEvent.eventSequence,
        committedIdleAt: now,
      },
    })]));
    return grpcCall();
  }

  private commitRuntimeTermination(request: CommitRuntimeTerminationRequest, _metadata: Metadata, callback: (error: Error | null, responseValue: unknown) => void): unknown {
    this.terminationRequests.push(request);
    const toolEvents = request.toolSettlements.map((_settlement, index) => this.event(request, `sevt_termination_tool_${index}`));
    const completionEvent = request.completionMailCreate === undefined ? undefined : this.event(request, "sevt_termination_completion");
    const failureEvent = this.event(request, "sevt_termination_failure");
    const closeoutEvent = this.event(request, "sevt_termination_closeout");
    callback(null, response(request, request.runtimeWriteId, [receipt({
      request,
      operationKind: "commit_runtime_termination",
      sourceKind: "runtime_termination",
      operationId: request.runtimeWriteId,
      digest: runtimeTerminationDeclarationDigest(request),
      events: [...toolEvents, ...(completionEvent === undefined ? [] : [completionEvent]), failureEvent, closeoutEvent],
      messages: request.completionMailCreate === undefined || completionEvent === undefined
        ? []
        : [this.message(request, request.completionMailCreate, completionEvent.eventId)],
    })]));
    return grpcCall();
  }

  private commitInternalToolRepair(request: CommitInternalToolRepairRequest, _metadata: Metadata, callback: (error: Error | null, responseValue: unknown) => void): unknown {
    this.repairRequests.push(request);
    const event = this.event(request, "sevt_internal_repair");
    callback(null, response(request, this.repairOperationId, [receipt({
      request,
      operationKind: "commit_internal_tool_repair",
      sourceKind: "internal_tool_repair",
      operationId: this.repairOperationId,
      digest: internalToolRepairDeclarationDigest(request, this.repairOperationId),
      events: [event],
      messages: request.messageCreate === undefined ? [] : [this.message(request, request.messageCreate, event.eventId)],
    })]));
    return grpcCall();
  }

  private event(
    request: { readonly scope: RuntimeScope | undefined },
    eventId: string,
    forcedSequence?: number,
    disposition: "created" | "existing" = "created",
  ) {
    if (forcedSequence === undefined) this.eventSequence += 1;
    else this.eventSequence = Math.max(this.eventSequence, forcedSequence);
    return {
      sessionThreadId: request.scope?.sessionThreadId ?? "",
      eventId,
      eventSequence: forcedSequence ?? this.eventSequence,
      disposition: disposition === "created"
        ? DurableEventDisposition.DURABLE_EVENT_DISPOSITION_CREATED
        : DurableEventDisposition.DURABLE_EVENT_DISPOSITION_EXISTING,
    };
  }

  private message(
    request: { readonly scope: RuntimeScope | undefined },
    create: RuntimeMessageCreate,
    eventId: string,
  ) {
    this.messageSequence += 1;
    return messageStamp(request, create.parts, eventId, this.messageSequence);
  }

  private appendMessage(
    request: { readonly scope: RuntimeScope | undefined },
    parts: RuntimePartCreate[],
    eventId: string,
  ) {
    this.messageSequence += 1;
    return messageStamp(request, parts, eventId, this.messageSequence);
  }
}

function messageStamp(
  request: { readonly scope: RuntimeScope | undefined },
  parts: RuntimePartCreate[],
  owningEventId: string,
  messageSequence: number,
) {
  const messageId = `msg_${messageSequence}`;
  return {
    sessionThreadId: request.scope?.sessionThreadId ?? "",
    owningEventId,
    messageId,
    messageSequence,
    createdAt: now,
    updatedAt: now,
    disposition: DurableProjectionDisposition.DURABLE_PROJECTION_DISPOSITION_CREATED,
    parts: parts.map((_part, index) => ({
      partId: `part_${messageSequence}_${index}`,
      messageId,
      partSequence: index,
      createdAt: now,
      updatedAt: now,
      disposition: DurableProjectionDisposition.DURABLE_PROJECTION_DISPOSITION_CREATED,
    })),
  };
}

function receipt(input: {
  readonly request: { readonly scope: RuntimeScope | undefined };
  readonly operationKind: string;
  readonly sourceKind: string;
  readonly operationId: string;
  readonly digest: string;
  readonly events: readonly unknown[];
  readonly messages: readonly unknown[];
  readonly requestStart?: unknown;
  readonly idleCloseout?: unknown;
}) {
  return {
    sessionThreadId: input.request.scope?.sessionThreadId ?? "",
    operationKind: input.operationKind,
    sourceKind: input.sourceKind,
    operationId: input.operationId,
    declarationDigest: input.digest,
    events: input.events,
    messages: input.messages,
    pendingAttachmentDeltaJson: [],
    interruptToolProjections: [],
    prefixConsumptions: [],
    childLifecycle: [],
    requestReschedule: undefined,
    requestStart: input.requestStart,
    idleCloseout: input.idleCloseout,
    compactedThroughMessageSequence: undefined,
  };
}

function response(
  request: { readonly scope: RuntimeScope | undefined },
  writeId: string,
  receipts: readonly unknown[],
) {
  return {
    ack: ack(writeId, BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED),
    declaration: {
      receipts,
      observedBindingId: request.scope?.binding?.bindingId ?? "",
      observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
      applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
    },
  };
}

function ack(runtimeWriteId: string, writeStatus: BridgeWriteStatus) {
  return { status: writeStatus, runtimeInputId: "", runtimeWriteId, errorCode: "" };
}

function grpcCall(): { cancel(): void } {
  return { cancel() {} };
}
