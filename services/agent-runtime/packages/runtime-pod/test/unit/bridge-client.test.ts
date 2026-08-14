import { describe, expect, test } from "bun:test";
import { Metadata, status } from "@grpc/grpc-js";
import type { CallOptions } from "@grpc/grpc-js";
import {
  BridgeWriteStatus,
  DurableEventDisposition,
  DurableProjectionDisposition,
  ReceiptApplicationDisposition,
  RuntimeMessageCreateKind,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  AgentRuntimeBridgeServiceClient,
  CommitInputsRequest,
  CommitInternalToolRepairRequest,
  CommitRuntimeTerminationRequest,
  CommitRuntimeTerminationResponse,
  CommitTaskNotificationResultRequest,
  FinishIdleRequest,
  LoadContextRequest,
  RuntimeMessageCreate,
  RuntimePartCreate,
  RuntimeScope,
  SettleToolResultRequest,
  SettleToolResultResponse,
  WriteEventRequest,
  WriteRequestEndRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  RuntimeAcceptedInputState,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import { RuntimeMessageSchema } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
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
  SessionEventWriterToolSettlementEnvelope,
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

  test("logs a safe durable Tool-error discriminator when cold Message parsing fails", async () => {
    const secretMessage = "CANARY_TOOL_ERROR_MESSAGE";
    const secretInput = "CANARY_TOOL_INPUT";
    const secretProviderBody = "CANARY_PROVIDER_BODY";
    const bridge = new DeclarationBridge();
    bridge.loadContextJson = JSON.stringify({
      messages: [{
        id: "msg_invalid_tool_error",
        sessionId: "sesn_1",
        role: "assistant",
        origin: "agent",
        sequence: 1,
        status: "completed",
        createdAt: now,
        parts: [{
          id: "part_invalid_tool_error",
          sessionId: "sesn_1",
          messageId: "msg_invalid_tool_error",
          sequence: 0,
          type: "tool",
          toolCallId: "call_invalid_tool_error",
          toolName: "Read",
          state: {
            status: "error",
            input: { value: { file_path: secretInput }, preview: secretInput, truncated: false },
            error: { type: "runtime", message: secretMessage, retryable: false, provider_body: secretProviderBody },
          },
          createdAt: now,
          completedAt: now,
        }],
      }],
      turnFacts: { events: [], internalRepairs: [] },
      coldCoverage: {
        pendingToolIds: [],
        pendingSandboxExecutionIds: [],
        pendingAttachmentIdentities: [],
        undeliveredMailDeliveryIds: [],
      },
      thread: { parentThreadId: null, role: "main", visibility: "public", taskName: null, agentType: "general", status: "idle" },
    });
    const records: Record<string, unknown>[] = [];
    const loader = new BridgeAPIContextLoader({
      ...options(bridge),
      logger: {
        info: () => undefined,
        error: (record: Record<string, unknown>) => records.push(record),
      },
    });

    await expect(loader.loadThreadContext(control("rin_invalid_tool_error"))).rejects.toMatchObject({
      type: "context-loader",
      code: "schema_mismatch",
      retryable: false,
      fatal: true,
    });
    expect(records).toHaveLength(1);
    expect(records[0]).toEqual({
      event: "runtime_context_load_parse_failed",
      "event.kind": "runtime_context_load_parse_failed",
      component: "agent-runtime",
      message: "durable Runtime context projection was rejected",
      operation: "load_context",
      phase: "durable_message_parse",
      reason: "invalid_tool_error_shape",
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      "thread.id": "thrd_1",
    });
    const serializedRecords = JSON.stringify(records);
    expect(serializedRecords).not.toContain(secretMessage);
    expect(serializedRecords).not.toContain(secretInput);
    expect(serializedRecords).not.toContain(secretProviderBody);
  });

  test("logs one safe phase and reason for every rejected cold-context parse boundary", async () => {
    const canary = "CANARY_REJECTED_COLD_CONTEXT";
    const validContext = () => ({
      messages: [],
      turnFacts: { events: [], internalRepairs: [] },
      coldCoverage: {
        pendingToolIds: [],
        pendingSandboxExecutionIds: [],
        pendingAttachmentIdentities: [],
        undeliveredMailDeliveryIds: [],
      },
      thread: {
        parentThreadId: null,
        role: "main",
        visibility: "public",
        taskName: null,
        agentType: "general",
        status: "idle",
      },
    });
    const cases = [
      {
        name: "context JSON",
        contextJson: () => `{${canary}`,
        phase: "context_json_parse",
        reason: "invalid_context_json",
      },
      {
        name: "Message collection",
        contextJson: () => JSON.stringify({ ...validContext(), messages: canary }),
        phase: "message_collection_parse",
        reason: "invalid_message_collection_shape",
      },
      {
        name: "durable Message",
        contextJson: () => JSON.stringify({ ...validContext(), messages: [canary] }),
        phase: "durable_message_parse",
        reason: "invalid_durable_message_shape",
      },
      {
        name: "turn facts",
        contextJson: () => JSON.stringify({ ...validContext(), turnFacts: canary }),
        phase: "turn_facts_parse",
        reason: "invalid_turn_facts_shape",
      },
      {
        name: "thread context prefix",
        contextJson: () => JSON.stringify({ ...validContext(), threadContextPrefix: canary }),
        phase: "thread_context_prefix_parse",
        reason: "invalid_thread_context_prefix_shape",
      },
      {
        name: "thread metadata",
        contextJson: () => JSON.stringify({ ...validContext(), thread: canary }),
        phase: "thread_metadata_parse",
        reason: "invalid_thread_metadata_shape",
      },
      {
        name: "Runtime config",
        contextJson: () => JSON.stringify({
          ...validContext(),
          runtimeConfig: { configGeneration: canary, installedTools: [] },
        }),
        phase: "runtime_config_parse",
        reason: "invalid_runtime_config_shape",
      },
      {
        name: "MCP manifests",
        contextJson: () => JSON.stringify({ ...validContext(), mcpManifests: canary }),
        phase: "mcp_manifests_parse",
        reason: "invalid_mcp_manifests_shape",
      },
      {
        name: "pending Tool uses",
        contextJson: () => JSON.stringify({ ...validContext(), pendingToolUses: canary }),
        phase: "pending_tool_uses_parse",
        reason: "invalid_pending_tool_uses_shape",
      },
      {
        name: "pending Sandbox executions",
        contextJson: () => JSON.stringify({ ...validContext(), pendingSandboxExecutions: canary }),
        phase: "pending_sandbox_executions_parse",
        reason: "invalid_pending_sandbox_executions_shape",
      },
      {
        name: "background Tools",
        contextJson: () => JSON.stringify({ ...validContext(), backgroundTools: canary }),
        phase: "background_tools_parse",
        reason: "invalid_background_tools_shape",
      },
      {
        name: "pending attachments",
        contextJson: () => JSON.stringify({ ...validContext(), pendingAttachments: canary }),
        phase: "pending_attachments_parse",
        reason: "invalid_pending_attachments_shape",
      },
      {
        name: "pending agent mail",
        contextJson: () => JSON.stringify({ ...validContext(), pendingAgentMail: canary }),
        phase: "pending_agent_mail_parse",
        reason: "invalid_pending_agent_mail_shape",
      },
      {
        name: "cold coverage",
        contextJson: () => JSON.stringify({ ...validContext(), coldCoverage: canary }),
        phase: "cold_coverage_parse",
        reason: "invalid_cold_coverage_shape",
      },
    ] as const;

    for (const testCase of cases) {
      const bridge = new DeclarationBridge();
      bridge.loadContextJson = testCase.contextJson();
      const records: Record<string, unknown>[] = [];
      const loader = new BridgeAPIContextLoader({
        ...options(bridge),
        logger: {
          info: () => undefined,
          error: (record: Record<string, unknown>) => records.push(record),
        },
      });

      await expect(loader.loadThreadContext(control(`rin_invalid_${testCase.phase}`))).rejects.toMatchObject({
        type: "context-loader",
        code: "schema_mismatch",
        retryable: false,
        fatal: true,
      });
      expect(records, testCase.name).toEqual([{
        event: "runtime_context_load_parse_failed",
        "event.kind": "runtime_context_load_parse_failed",
        component: "agent-runtime",
        message: "durable Runtime context projection was rejected",
        operation: "load_context",
        phase: testCase.phase,
        reason: testCase.reason,
        "workspace.id": "wksp_1",
        "session.id": "sesn_1",
        "thread.id": "thrd_1",
      }]);
      expect(JSON.stringify(records), testCase.name).not.toContain(canary);
    }
  });

  test("keeps cold-context rejection authoritative when its diagnostic sink throws", async () => {
    const bridge = new DeclarationBridge();
    bridge.loadContextJson = "{";
    let diagnosticAttempts = 0;
    const loader = new BridgeAPIContextLoader({
      ...options(bridge),
      logger: {
        info: () => undefined,
        error: () => {
          diagnosticAttempts += 1;
          throw new Error("diagnostic sink unavailable");
        },
      },
    });

    await expect(loader.loadThreadContext(control("rin_invalid_json_fail_open"))).rejects.toMatchObject({
      type: "context-loader",
      code: "schema_mismatch",
      retryable: false,
      fatal: true,
    });
    expect(diagnosticAttempts).toBe(1);
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

  test("retains task-notification custody when raw validation has no durable disposition", async () => {
    const bridge = new DeclarationBridge();
    bridge.taskNotificationErrors.push(status.INVALID_ARGUMENT);
    const loader = new BridgeAPIContextLoader(options(bridge));
    const input = taskNotificationInput("rin_task_invalid_without_receipt");

    await expect(loader.commitAcceptedInput(input, {
      messageCreates: acceptedInputCreates(input),
    })).rejects.toMatchObject({
      type: "context-loader",
      code: "schema_mismatch",
      retryable: false,
      fatal: true,
    });
    expect(bridge.commitTaskNotificationRequests).toHaveLength(1);
  });

  test("contains a durable task-notification idempotency conflict to the exact input", async () => {
    const bridge = new DeclarationBridge();
    bridge.taskNotificationErrors.push(status.ALREADY_EXISTS);
    const loader = new BridgeAPIContextLoader(options(bridge));
    const input = taskNotificationInput("rin_task_payload_mismatch");

    await expect(loader.commitAcceptedInput(input, {
      messageCreates: acceptedInputCreates(input),
    })).resolves.toEqual({
      type: "task_notification_rejected",
      errorCode: "task_notification_payload_mismatch",
    });
    expect(bridge.commitTaskNotificationRequests).toHaveLength(1);
  });

  test("returns an input-scoped terminal disposition for a durable notification rejection", async () => {
    const bridge = new DeclarationBridge();
    bridge.taskNotificationRejections.push("task_notification_payload_mismatch");
    const loader = new BridgeAPIContextLoader(options(bridge));
    const input = taskNotificationInput("rin_task_rejected");

    const result = await loader.commitAcceptedInput(input, { messageCreates: acceptedInputCreates(input) });

    expect(result).toEqual({
      type: "task_notification_rejected",
      errorCode: "task_notification_payload_mismatch",
    });
    expect(bridge.commitTaskNotificationRequests).toHaveLength(1);
  });

  test("keeps a malformed task-notification receipt on the retryable uncertainty path", async () => {
    const bridge = new DeclarationBridge();
    bridge.corruptTaskNotificationDigest = true;
    const loader = new BridgeAPIContextLoader(options(bridge));
    const input = taskNotificationInput("rin_task_receipt_uncertain");

    await expect(loader.commitAcceptedInput(input, { messageCreates: acceptedInputCreates(input) })).rejects.toMatchObject({
      type: "context-loader",
      code: "unavailable",
      retryable: true,
      fatal: false,
    });
    expect(bridge.commitTaskNotificationRequests).toHaveLength(1);
  });

  test("lowers the shared reviewer declaration through the production CommitInputs adapter", async () => {
    const fixture = await Bun.file(new URL("../../../../testdata/reviewer-input-declaration.json", import.meta.url)).json() as {
      message: unknown;
      messageCreate: { messageInfo: unknown; part: unknown };
    };
    const bridge = new DeclarationBridge();
    const loader = new BridgeAPIContextLoader(options(bridge));
    const input: Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }> = {
      ...control("rin_reviewer_shared"),
      kind: "approval_review",
      reviewId: "review_shared",
      parentThreadId: "thrd_parent",
      targetModelToolCallId: "tool_call_shared",
      targetToolName: "Write",
      promptItems: [RuntimeMessageSchema.parse(fixture.message)],
      outputSchemaJson: `{"type":"object"}`,
      thread: {
        parentThreadId: "thrd_parent",
        role: "approval_reviewer",
        visibility: "internal",
        agentType: "approval_reviewer",
        status: "idle",
      },
    };

    const result = await loader.commitAcceptedInput(input, { messageCreates: acceptedInputCreates(input) });

    expect(result).toMatchObject({ type: "receipt", inputDisposition: "committed" });
    const lowered = bridge.commitInputsRequests[0]?.messageCreates[0];
    expect(lowered?.messageKind).toBe(RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_REVIEWER_INPUT);
    expect(JSON.parse(lowered?.messageInfoJson ?? "null")).toEqual(fixture.messageCreate.messageInfo);
    expect(JSON.parse(lowered?.parts[0]?.partJson ?? "null")).toEqual(fixture.messageCreate.part);
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

  test("projects Assistant append and Tool settlement through disjoint RPCs", async () => {
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
    const settlement = await writer.settleToolResult({
      ...settlementScope(),
      settlement: {
        toolUseEventId: "sevt_tool_1",
        outcome: { type: "completed", output: { text: "done", truncated: false } },
      },
    });

    expect(append).toMatchObject({ ok: true, declaration: { receipt: { messages: [{ disposition: "created" }] } } });
    expect(settlement).toEqual({ ok: true, result: { type: "committed" } });
    expect(bridge.writeEventRequests[0]?.assistantPartAppend?.parts).toHaveLength(1);
    expect(bridge.writeEventRequests).toHaveLength(1);
    expect(bridge.settleToolResultRequests[0]?.settlement?.toolUseEventId).toBe("sevt_tool_1");
  });

  test("projects every error-bearing Tool family through one durable error contract", async () => {
    const bridge = new DeclarationBridge();
    const writer = new BridgeAPIEventWriter(options(bridge));
    const cases = [
      {
        name: "ordinary",
        toolUseEventId: "sevt_ordinary",
        outcome: {
          type: "error" as const,
          error: { type: "runtime" as const, code: "provider_tool_protocol_error" as const, message: "ordinary failed", retryable: false, fatal: true, retryStatus: { type: "terminal" as const } },
        },
      },
      {
        name: "MCP",
        toolUseEventId: "sevt_mcp",
        outcome: {
          type: "error" as const,
          error: { type: "runtime" as const, code: "provider_tool_protocol_error" as const, message: "MCP failed", retryable: true, fatal: false, reason: "runtime_contract_validation" as const },
        },
      },
      {
        name: "Sandbox-backed",
        toolUseEventId: "sevt_sandbox",
        outcome: {
          type: "error" as const,
          error: { type: "runtime" as const, code: "provider_unavailable" as const, message: "Sandbox failed", retryable: true, fatal: false, retryStatus: { type: "retrying" as const, attempt: 1 } },
        },
      },
      {
        name: "error-bearing cancellation",
        toolUseEventId: "sevt_cancelled",
        outcome: {
          type: "cancelled" as const,
          error: { type: "runtime" as const, code: "provider_cancelled" as const, message: "Tool cancelled", retryable: false, fatal: false, reason: "runtime_shutdown" as const },
        },
      },
    ];

    for (const [index, candidate] of cases.entries()) {
      await writer.settleToolResult({
        ...settlementScope(),
        settlement: { toolUseEventId: candidate.toolUseEventId, outcome: candidate.outcome },
      });
      const settlement = bridge.settleToolResultRequests[index]?.settlement;
      const rawError = settlement?.error?.errorJson ?? settlement?.cancelled?.errorJson;
      expect(rawError, candidate.name).toBeDefined();
      expect(JSON.parse(rawError ?? "null"), candidate.name).toEqual({
        type: candidate.outcome.error.code,
        message: candidate.outcome.error.message,
        retryable: candidate.outcome.error.retryable,
      });
      expect(Object.keys(JSON.parse(rawError ?? "null")).sort(), candidate.name).toEqual(["message", "retryable", "type"]);
    }

    await writer.settleToolResult({
      ...settlementScope(),
      settlement: {
        toolUseEventId: "sevt_completed",
        outcome: { type: "completed", output: { text: "done", truncated: false } },
      },
    });
    expect(JSON.parse(bridge.settleToolResultRequests.at(-1)?.settlement?.completed?.outputJson ?? "null")).toEqual({
      text: "done",
      truncated: false,
    });
  });

  test("classifies a rejected reviewer failure ACK as deterministic", async () => {
    const bridge = new DeclarationBridge();
    bridge.writeEventRejections.push("reviewer_outcome_rejected");
    const writer = new BridgeAPIEventWriter(options(bridge));

    const result = await writer.append({
      ...eventScope("rwrite_review_failure"),
      event: {
        type: "approval_review.failure",
        review_id: "arvw_rejected",
        parent_thread_id: "thrd_parent",
        target_model_tool_call_id: "tool_call_rejected",
        target_tool_name: "Write",
        failure_kind: "parse_failure",
        message: "approval reviewer decision is invalid",
      },
    });

    expect(result).toMatchObject({ ok: false, error: { code: "ack_mismatch", retryable: false } });
    expect(bridge.writeEventRequests).toHaveLength(1);
  });

  test("recovers a reviewer failure receipt after the committed response is lost", async () => {
    const bridge = new DeclarationBridge();
    bridge.writeEventPostCommitErrors.push(status.UNKNOWN);
    const transport = new BridgeAPIEventWriter(options(bridge));
    const envelope = {
      ...eventScope("rwrite_review_failure_replay"),
      event: {
        type: "approval_review.failure" as const,
        review_id: "arvw_replay",
        parent_thread_id: "thrd_parent",
        target_model_tool_call_id: "tool_call_replay",
        target_tool_name: "Write",
        failure_kind: "runtime_failure" as const,
        message: "approval reviewer failed",
      },
    };

    await expect(transport.append(envelope)).resolves.toMatchObject({
      ok: false,
      error: { code: "unavailable", retryable: true },
    });
    await expect(transport.append(envelope)).resolves.toMatchObject({
      ok: true,
      writeId: envelope.writeId,
    });
    expect(bridge.writeEventRequests).toHaveLength(2);
    expect(bridge.writeEventRequests.map((request) => request.runtimeWriteId)).toEqual([
      envelope.writeId,
      envelope.writeId,
    ]);
    expect(bridge.writeEventRequests.map((request) => writeEventDeclarationDigest(request))).toEqual([
      writeEventDeclarationDigest(bridge.writeEventRequests[0]!),
      writeEventDeclarationDigest(bridge.writeEventRequests[0]!),
    ]);
  });

  test("rejects zero-arm and contradictory Tool settlement responses", async () => {
    const bridge = new DeclarationBridge();
    bridge.settleToolResultResponses.push({}, { committed: {}, stale: {} });
    const transport = new BridgeAPIEventWriter(options(bridge));
    const envelope = mcpSettlementEnvelope();

    await expect(transport.settleToolResult(envelope)).resolves.toMatchObject({
      ok: false, error: { code: "schema_mismatch" },
    });
    await expect(transport.settleToolResult(envelope)).resolves.toMatchObject({
      ok: false, error: { code: "schema_mismatch" },
    });
  });

  test("parses each legal Tool settlement response arm into its closed result", async () => {
    const bridge = new DeclarationBridge();
    bridge.settleToolResultResponses.push({ committed: {} }, { duplicate: {} }, { stale: {} });
    const transport = new BridgeAPIEventWriter(options(bridge));
    const envelope = mcpSettlementEnvelope();

    await expect(transport.settleToolResult(envelope)).resolves.toEqual({ ok: true, result: { type: "committed" } });
    await expect(transport.settleToolResult(envelope)).resolves.toEqual({ ok: true, result: { type: "duplicate" } });
    await expect(transport.settleToolResult(envelope)).resolves.toEqual({ ok: true, result: { type: "stale" } });
  });

  test("replays an ambiguous Tool settlement from immutable request identity", async () => {
    const bridge = new DeclarationBridge();
    bridge.settleToolResultPostCommitErrors.push(status.UNKNOWN);
    const transport = new BridgeAPIEventWriter({ ...options(bridge), sleep: async () => {} });
    const envelope = mcpSettlementEnvelope();

    await expect(transport.settleToolResult(envelope)).resolves.toEqual({ ok: true, result: { type: "duplicate" } });
    expect(bridge.settleToolResultRequests).toHaveLength(2);
    expect(bridge.settleToolResultRequests[1]).toEqual(bridge.settleToolResultRequests[0]);
  });

  test("bounds persistent Tool settlement acknowledgement loss without changing request bytes", async () => {
    const bridge = new DeclarationBridge();
    bridge.settleToolResultPostCommitErrors.push(status.UNKNOWN, status.UNAVAILABLE, status.DEADLINE_EXCEEDED);
    const delays: number[] = [];
    const transport = new BridgeAPIEventWriter({
      ...options(bridge),
      sleep: async (delayMs) => { delays.push(delayMs); },
    });
    const envelope = mcpSettlementEnvelope();

    await expect(transport.settleToolResult(envelope)).resolves.toMatchObject({
      ok: false,
      error: { code: "timeout", retryable: true },
    });
    expect(delays).toEqual([100, 300]);
    expect(bridge.settleToolResultRequests).toHaveLength(3);
    expect(bridge.settleToolResultRequests[1]).toEqual(bridge.settleToolResultRequests[0]);
    expect(bridge.settleToolResultRequests[2]).toEqual(bridge.settleToolResultRequests[0]);
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

  test("serializes idle completion create and consumes the closed termination result", async () => {
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
    };

    const idle = await writer.finishIdle(idleEnvelope);
    const terminated = await writer.commitRuntimeTermination(terminationEnvelope);

    expect(idle).toMatchObject({ ok: true, declaration: { receipt: { idleCloseout: { durableTurnId: "turn_1" } } } });
    expect(terminated).toEqual({
      ok: true,
      type: "committed",
      failureEventId: "sevt_termination_failure",
      closeoutEventId: "sevt_termination_closeout",
    });
    expect(bridge.finishIdleRequests[0]?.completionMailCreate).toBeDefined();
    expect(bridge.terminationRequests[0]).toEqual({
      scope: {
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        sessionThreadId: "thrd_1",
        binding: {
          bindingId: "bind_1",
          bindingGeneration: 3,
          targetPodUid: "pod_1",
        },
      },
      runtimeWriteId: "terminate_1",
      failureJson: JSON.stringify(terminationEnvelope.failure),
    });
  });

  test("rejects zero, multiple, and incomplete termination result variants", async () => {
    const envelope: SessionEventWriterRuntimeTerminationEnvelope = {
      ...eventScope("terminate_contract"),
      failure: {
        type: "runtime",
        code: "runtime_invalid_sequence",
        message: "terminated",
        retryable: false,
        fatal: true,
        reason: "runtime_shutdown",
      },
    };
    for (const responseValue of [
      {},
      { committed: { failureEventId: "failure", closeoutEventId: "closeout" }, stale: {} },
      { duplicate: { failureEventId: "", closeoutEventId: "closeout" } },
    ] satisfies CommitRuntimeTerminationResponse[]) {
      const bridge = new DeclarationBridge();
      bridge.terminationResponses.push(responseValue);
      const writer = new BridgeAPIEventWriter(options(bridge));
      expect(await writer.commitRuntimeTermination(envelope)).toMatchObject({ ok: false, error: { code: "schema_mismatch" } });
    }
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

function settlementScope() {
  return {
    workspaceId: "wksp_1", sessionId: "sesn_1", sessionThreadId: "thrd_1",
    bindingId: "bind_1", bindingGeneration: 3, targetPodUid: "pod_1",
  };
}

function mcpSettlementEnvelope(): SessionEventWriterToolSettlementEnvelope {
  return {
    ...settlementScope(),
    settlement: {
      toolUseEventId: "sevt_mcp_use",
      outcome: {
        type: "completed",
        output: { text: "done", truncated: false },
      },
    },
  };
}

class DeclarationBridge {
  readonly loadContextRequests: LoadContextRequest[] = [];
  readonly commitInputsRequests: CommitInputsRequest[] = [];
  readonly writeEventRequests: WriteEventRequest[] = [];
  readonly settleToolResultRequests: SettleToolResultRequest[] = [];
  readonly settleToolResultResponses: SettleToolResultResponse[] = [];
  readonly settleToolResultPostCommitErrors: number[] = [];
  readonly writeEventRejections: string[] = [];
  readonly writeEventPostCommitErrors: number[] = [];
  readonly writeRequestEndRequests: WriteRequestEndRequest[] = [];
  readonly finishIdleRequests: FinishIdleRequest[] = [];
  readonly terminationRequests: CommitRuntimeTerminationRequest[] = [];
  readonly terminationResponses: CommitRuntimeTerminationResponse[] = [];
  readonly commitTaskNotificationRequests: CommitTaskNotificationResultRequest[] = [];
  readonly repairRequests: CommitInternalToolRepairRequest[] = [];
  readonly taskNotificationErrors: number[] = [];
  readonly taskNotificationRejections: string[] = [];
  loadContextJson: string | undefined;
  corruptTaskNotificationDigest = false;
  repairOperationId = "repair";
  private eventSequence = 0;
  private messageSequence = 0;
  private readonly committedWriteEvents = new Map<string, Record<string, unknown>>();
  private readonly committedToolSettlements = new Set<string>();

  client(): AgentRuntimeBridgeServiceClient {
    return {
      loadContext: this.loadContext.bind(this),
      commitInputs: this.commitInputs.bind(this),
      writeEvent: this.writeEvent.bind(this),
      settleToolResult: this.settleToolResult.bind(this),
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
      contextJson: this.loadContextJson ?? JSON.stringify({
        messages: [],
        turnFacts: { events: [], internalRepairs: [] },
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
    const events = request.eventIds.map((eventId, index) => this.event(
      request,
      eventId,
      request.sequenceFrom + index,
      request.inputKind === "approval_review" ? "created" : "existing",
    ));
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
    const rejectionCode = this.taskNotificationRejections.shift();
    if (rejectionCode !== undefined) {
      callback(null, {
        ack: {
          status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED,
          runtimeInputId: request.runtimeInputId,
          errorCode: rejectionCode,
        },
      });
      return grpcCall();
    }
    const event = this.event(request, `sevt_${request.runtimeInputId}`);
    const operationId = taskNotificationOperationId(request.runtimeInputId, request.taskId);
    callback(null, response(request, request.runtimeInputId, [receipt({
      request,
      operationKind: "commit_task_notification_result",
      sourceKind: "task_notification",
      operationId,
      digest: this.corruptTaskNotificationDigest ? "corrupt-task-notification-digest" : taskNotificationDeclarationDigest(request),
      events: [event],
      messages: request.messageCreate === undefined ? [] : [this.message(request, request.messageCreate, event.eventId)],
    })]));
    return grpcCall();
  }

  private writeEvent(request: WriteEventRequest, _metadata: Metadata, callback: (error: Error | null, responseValue: unknown) => void): unknown {
    this.writeEventRequests.push(request);
    const rejectionCode = this.writeEventRejections.shift();
    if (rejectionCode !== undefined) {
      callback(null, {
        ack: {
          status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED,
          runtimeWriteId: request.runtimeWriteId,
          errorCode: rejectionCode,
        },
      });
      return grpcCall();
    }
    const existing = this.committedWriteEvents.get(request.runtimeWriteId);
    if (existing !== undefined) {
      callback(null, {
        ...existing,
        ack: {
          ...(existing.ack as Record<string, unknown>),
          status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE,
        },
      });
      return grpcCall();
    }
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
    const committed = {
      ...response(request, request.runtimeWriteId, [declarationReceipt]),
      eventId: event.eventId,
      sequence: event.eventSequence,
    };
    this.committedWriteEvents.set(request.runtimeWriteId, committed);
    const postCommitError = this.writeEventPostCommitErrors.shift();
    if (postCommitError !== undefined) {
      callback(Object.assign(new Error("write event response was lost"), { code: postCommitError }), {});
      return grpcCall();
    }
    callback(null, committed);
    return grpcCall();
  }

  private settleToolResult(
    request: SettleToolResultRequest,
    _metadata: Metadata,
    _options: CallOptions,
    callback: (error: Error | null, responseValue: SettleToolResultResponse) => void,
  ): unknown {
    this.settleToolResultRequests.push(request);
    const forced = this.settleToolResultResponses.shift();
    if (forced !== undefined) {
      callback(null, forced);
      return grpcCall();
    }
    const target = request.settlement?.toolUseEventId ?? "";
    const response = this.committedToolSettlements.has(target)
      ? { duplicate: {} }
      : { committed: {} };
    this.committedToolSettlements.add(target);
    const postCommitError = this.settleToolResultPostCommitErrors.shift();
    if (postCommitError !== undefined) {
      callback(Object.assign(new Error("settlement response lost"), { code: postCommitError }), {});
      return grpcCall();
    }
    callback(null, response);
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
    callback(null, this.terminationResponses.shift() ?? {
      committed: {
        failureEventId: "sevt_termination_failure",
        closeoutEventId: "sevt_termination_closeout",
      },
    });
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
    return messageStamp(request, create.parts, this.messageSequence);
  }

  private appendMessage(
    request: { readonly scope: RuntimeScope | undefined },
    parts: RuntimePartCreate[],
    eventId: string,
  ) {
    this.messageSequence += 1;
    return messageStamp(request, parts, this.messageSequence);
  }
}

function messageStamp(
  request: { readonly scope: RuntimeScope | undefined },
  parts: RuntimePartCreate[],
  messageSequence: number,
) {
  const messageId = `msg_${messageSequence}`;
  return {
    sessionThreadId: request.scope?.sessionThreadId ?? "",
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
