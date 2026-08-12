/**
 * Computes the canonical replay digest for each private declaration carrier.
 * The canonical form mirrors Bridge's semantic write shapes and deliberately
 * excludes transport-only identity from nested message and part values.
 */

import { createHash } from "node:crypto";
import { RuntimeMessageCreateKind } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  CommitInputsRequest,
  CommitInternalToolRepairRequest,
  CommitRuntimeTerminationRequest,
  CommitTaskNotificationResultRequest,
  FinishIdleRequest,
  RuntimeAssistantPartAppend,
  RuntimeMessageCreate,
  RuntimeToolSettlement,
  WriteEventRequest,
  WriteRequestEndRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
  canonicalRunToolJSON,
  canonicalRunToolJSONWithoutObjectFields,
} from "@tetral/gateway-protocol/src/run-tool-canonical-json.js";

const internalProviderPayloadFields = new Set([
  "background_task", "engine_sandbox_id", "provider_sandbox_id", "provider_session_id",
  "provider_command_id", "provider_command_metadata", "provider_command_metadata_json",
  "provider_metadata", "provider_metadata_json",
]);

export function commitInputsDeclarationDigest(request: Pick<CommitInputsRequest,
  "scope" | "runtimeInputId" | "eventIds" | "sequenceFrom" | "sequenceTo" | "inputKind" | "messageCreates"
>): string {
  return digest({
    event_ids: request.eventIds,
    input_kind: request.inputKind,
    message_creates: request.messageCreates.map(canonicalMessageCreate),
    operation_kind: "commit_inputs",
    runtime_input_id: request.runtimeInputId,
    sequence_from: request.sequenceFrom,
    sequence_to: request.sequenceTo,
    session_thread_id: request.scope?.sessionThreadId ?? "",
  });
}

export function internalToolRepairDeclarationDigest(request: Pick<CommitInternalToolRepairRequest,
  "scope" | "modelRequestId" | "modelToolCallId" | "toolName" | "messageCreate"
>, repairKey: string): string {
  return digest({
    message_create: canonicalMessageCreate(request.messageCreate),
    model_request_id: request.modelRequestId,
    model_tool_call_id: request.modelToolCallId,
    operation_kind: "commit_internal_tool_repair",
    repair_key: repairKey,
    session_thread_id: request.scope?.sessionThreadId ?? "",
    tool_name: request.toolName,
  });
}

export function taskNotificationDeclarationDigest(request: Pick<CommitTaskNotificationResultRequest,
  "scope" | "runtimeInputId" | "taskId" | "resultJson" | "messageCreate"
>): string {
  const result = canonicalRunToolJSONWithoutObjectFields(request.resultJson, internalProviderPayloadFields);
  return digestRaw({
    message_create: canonicalMessageCreate(request.messageCreate),
    operation_kind: "commit_task_notification_result",
    runtime_input_id: request.runtimeInputId,
    session_thread_id: request.scope?.sessionThreadId ?? "",
    task_id: request.taskId,
  }, "result", result);
}

export function writeEventDeclarationDigest(request: Pick<WriteEventRequest,
  "scope" | "runtimeWriteId" | "modelRequestId" | "eventType" | "payloadJson" |
  "serverToolUse" | "assistantPartAppend" | "toolSettlement" | "mcpMaterializationHandle" |
  "sandboxResultDigest" | "contextThroughMessageSequence" | "requestKind"
>): string {
  const payload = canonicalRunToolJSONWithoutObjectFields(request.payloadJson, internalProviderPayloadFields);
  const declaration: Record<string, unknown> = {
    assistant_part_append: canonicalAssistantAppend(request.assistantPartAppend),
    event_type: request.eventType,
    mcp_materialization_handle: nullableString(request.mcpMaterializationHandle),
    model_request_id: nullableString(request.modelRequestId),
    operation_kind: "write_event",
    runtime_write_id: request.runtimeWriteId,
    sandbox_result_digest: nullableString(request.sandboxResultDigest),
    server_tool_use: request.serverToolUse === undefined ? null : {
      web_fetch_requests: request.serverToolUse.webFetchRequests,
      web_search_requests: request.serverToolUse.webSearchRequests,
    },
    session_thread_id: request.scope?.sessionThreadId ?? "",
    tool_settlement: canonicalToolSettlement(request.toolSettlement),
  };
  if (request.eventType === "span.model_request_start") {
    declaration.context_through_message_sequence = request.contextThroughMessageSequence ?? null;
    declaration.request_kind = nullableString(request.requestKind);
  }
  return digestRaw(declaration, "payload", payload);
}

export function writeRequestEndDeclarationDigest(request: Pick<WriteRequestEndRequest,
  "scope" | "modelRequestId" | "finishReason" | "usageJson" | "modelRequestStartEventId" |
  "isError" | "errorKind" | "consumedAttachmentRefs" | "requestKind" | "reschedule" |
  "consumedFileAttachments" | "trailingPartAppend" | "prefixConsumption" |
  "compactedThroughMessageSequence" | "compactionEventPayloadJson" | "interruptSettlement" |
  "compactionCheckpointCreate"
>): string {
  return digest({
    compacted_through_message_sequence: request.compactedThroughMessageSequence ?? null,
    compaction_checkpoint_create: canonicalMessageCreate(request.compactionCheckpointCreate),
    compaction_event_payload: request.compactionEventPayloadJson.length === 0
      ? null : JSON.parse(canonicalRunToolJSON(request.compactionEventPayloadJson)) as unknown,
    consumed_attachment_refs: request.consumedAttachmentRefs,
    consumed_file_attachments: request.consumedFileAttachments.map((attachment) => ({
      file_id: attachment.fileId, source_event_id: attachment.sourceEventId,
    })),
    error_kind: nullableString(request.errorKind),
    finish_reason: request.finishReason,
    interrupt_settlement: request.interruptSettlement === undefined ? null : {
      event_ids: request.interruptSettlement.eventIds,
      runtime_input_id: request.interruptSettlement.runtimeInputId,
      sequence_from: request.interruptSettlement.sequenceFrom,
      sequence_to: request.interruptSettlement.sequenceTo,
    },
    is_error: request.isError,
    model_request_id: request.modelRequestId,
    model_request_start_event_id: request.modelRequestStartEventId,
    operation_kind: "write_request_end",
    prefix_consumption: request.prefixConsumption === undefined ? null : {
      child_thread_id: request.prefixConsumption.childThreadId,
      parent_boundary_event_id: request.prefixConsumption.parentBoundaryEventId,
    },
    request_kind: request.requestKind,
    reschedule: request.reschedule === undefined ? null : {
      attempt: request.reschedule.attempt,
      backoff_ms: request.reschedule.backoffMs,
      deadline: request.reschedule.deadline,
    },
    session_thread_id: request.scope?.sessionThreadId ?? "",
    trailing_part_append: canonicalAssistantAppend(request.trailingPartAppend),
    usage: JSON.parse(canonicalRunToolJSON(request.usageJson)) as unknown,
  });
}

export function finishIdleDeclarationDigest(request: Pick<FinishIdleRequest,
  "scope" | "durableTurnId" | "stopReasonJson" | "completionMailCreate"
>): string {
  return digest({
    completion_mail_create: canonicalMessageCreate(request.completionMailCreate),
    durable_turn_id: request.durableTurnId,
    operation_kind: "finish_idle",
    session_thread_id: request.scope?.sessionThreadId ?? "",
    stop_reason: JSON.parse(canonicalRunToolJSON(request.stopReasonJson)) as unknown,
  });
}

export function runtimeTerminationDeclarationDigest(request: Pick<CommitRuntimeTerminationRequest,
  "scope" | "runtimeWriteId" | "failureJson" | "toolSettlements" | "completionMailCreate"
>): string {
  return digest({
    completion_mail_create: canonicalMessageCreate(request.completionMailCreate),
    failure: JSON.parse(canonicalRunToolJSON(request.failureJson)) as unknown,
    operation_kind: "commit_runtime_termination",
    runtime_write_id: request.runtimeWriteId,
    session_thread_id: request.scope?.sessionThreadId ?? "",
    tool_settlements: request.toolSettlements.map(canonicalToolSettlement),
  });
}

export function childLifecycleDeclarationDigest(input: {
  readonly operationKind: "mark_child_thread_closed" | "mark_child_thread_active";
  readonly action: "close" | "resume";
  readonly sessionThreadId: string;
  readonly childThreadId: string;
  readonly sourceKind: "tool_use" | "approval_review";
  readonly sourceCommandId: string;
}): string {
  return digest({
    action: input.action,
    child_thread_id: input.childThreadId,
    operation_kind: input.operationKind,
    session_thread_id: input.sessionThreadId,
    source_command_id: input.sourceCommandId,
    source_kind: input.sourceKind,
  });
}

function canonicalMessageCreate(create: RuntimeMessageCreate | undefined): unknown {
  if (create === undefined) return null;
  return {
    message_info: JSON.parse(canonicalRunToolJSON(create.messageInfoJson)) as unknown,
    message_kind: runtimeMessageCreateKindName(create.messageKind),
    parts: create.parts.map(canonicalPart),
  };
}

function canonicalAssistantAppend(append: RuntimeAssistantPartAppend | undefined): unknown {
  return append === undefined ? null : { parts: append.parts.map(canonicalPart) };
}

function canonicalPart(part: { readonly partKind: string; readonly partJson: string }): unknown {
  return { part_json: JSON.parse(canonicalRunToolJSON(part.partJson)) as unknown, part_kind: part.partKind };
}

function canonicalToolSettlement(settlement: RuntimeToolSettlement | undefined): unknown {
  if (settlement === undefined) return null;
  if (settlement.completed !== undefined) {
    return { tool_use_event_id: settlement.toolUseEventId, completed: JSON.parse(canonicalRunToolJSON(settlement.completed.outputJson)) as unknown };
  }
  if (settlement.error !== undefined) {
    return { tool_use_event_id: settlement.toolUseEventId, error: JSON.parse(canonicalRunToolJSON(settlement.error.errorJson)) as unknown };
  }
  if (settlement.cancelled !== undefined) {
    return {
      tool_use_event_id: settlement.toolUseEventId,
      cancelled: settlement.cancelled.errorJson === undefined ? null : JSON.parse(canonicalRunToolJSON(settlement.cancelled.errorJson)) as unknown,
    };
  }
  throw new Error("Runtime Tool settlement has no outcome");
}

function digest(value: Readonly<Record<string, unknown>>): string {
  const canonical = canonicalRunToolJSON(JSON.stringify(value));
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

function digestRaw(value: Readonly<Record<string, unknown>>, field: string, raw: string): string {
  const encoded = JSON.stringify(value);
  return createHash("sha256")
    .update(canonicalRunToolJSON(`${encoded.slice(0, -1)},${JSON.stringify(field)}:${raw}}`), "utf8")
    .digest("hex");
}

function nullableString(value: string | undefined): string | null {
  return value === undefined || value.length === 0 ? null : value;
}

function runtimeMessageCreateKindName(kind: RuntimeMessageCreateKind): string {
  const name = RuntimeMessageCreateKind[kind];
  if (typeof name !== "string" || name === "RUNTIME_MESSAGE_CREATE_KIND_UNSPECIFIED") {
    throw new Error("Runtime message create has an invalid kind");
  }
  return name;
}
