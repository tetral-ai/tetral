/**
 * Builds the canonical digest for one CommitInputs declaration. It guards
 * transport replay identity by hashing semantic declaration fields only;
 * BridgeAPIContextLoader calls it before transport and validates the returned
 * receipt before exposing durable stamps to Runtime Core.
 */

import { createHash } from "node:crypto";
import { RuntimeDraftKind } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  CommitInputsRequest,
  CommitInternalToolRepairRequest,
  CommitRuntimeTerminationRequest,
  CommitTaskNotificationResultRequest,
  FinishIdleRequest,
  WriteEventRequest,
  WriteRequestEndRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { canonicalRunToolJSON } from "@tetral/gateway-protocol/src/run-tool-canonical-json.js";

/** Returns the SHA-256 digest Bridge must echo for this declaration. */
export function commitInputsDeclarationDigest(
  request: Pick<
    CommitInputsRequest,
    | "scope"
    | "runtimeInputId"
    | "eventIds"
    | "sequenceFrom"
    | "sequenceTo"
    | "inputKind"
    | "drafts"
    | "pendingToolCancellations"
    | "sandboxExecutionToolUseEventIds"
  >,
): string {
  const declaration = {
    drafts: request.drafts.map((draft) => ({
      draft_kind: runtimeDraftKindName(draft.draftKind),
      message_info: JSON.parse(canonicalRunToolJSON(draft.messageInfoJson)) as unknown,
      ordinal: draft.ordinal,
      parts: draft.parts.map((part) => ({
        ordinal: part.ordinal,
        part_json: JSON.parse(canonicalRunToolJSON(part.partJson)) as unknown,
        part_kind: part.partKind,
        runtime_local_part_id: part.runtimeLocalPartId,
      })),
      runtime_local_id: draft.runtimeLocalId,
      source_event_id: draft.sourceEventId.length === 0 ? null : draft.sourceEventId,
      source_id: draft.sourceId,
      source_kind: draft.sourceKind,
    })),
    event_ids: request.eventIds,
    input_kind: request.inputKind,
    operation_kind: "commit_inputs",
    pending_tool_cancellations: request.pendingToolCancellations.map((cancellation) => ({
      runtime_local_id: cancellation.runtimeLocalId,
      tool_use_event_id: cancellation.toolUseEventId,
    })),
    runtime_input_id: request.runtimeInputId,
    sandbox_execution_tool_use_event_ids: request.sandboxExecutionToolUseEventIds,
    sequence_from: request.sequenceFrom,
    sequence_to: request.sequenceTo,
    session_thread_id: request.scope?.sessionThreadId ?? "",
  };
  const canonical = canonicalRunToolJSON(JSON.stringify(declaration));
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

/** Returns the SHA-256 digest Bridge must echo for one internal tool repair. */
export function internalToolRepairDeclarationDigest(
  request: Pick<
    CommitInternalToolRepairRequest,
    "scope" | "modelRequestId" | "modelToolCallId" | "toolName" | "drafts"
  >,
): string {
  const repairKey = request.drafts[0]?.sourceId ?? "";
  const declaration = {
    drafts: canonicalDrafts(request.drafts),
    model_request_id: request.modelRequestId,
    model_tool_call_id: request.modelToolCallId,
    operation_kind: "commit_internal_tool_repair",
    repair_key: repairKey,
    session_thread_id: request.scope?.sessionThreadId ?? "",
    tool_name: request.toolName,
  };
  const canonical = canonicalRunToolJSON(JSON.stringify(declaration));
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

/** Returns the SHA-256 digest Bridge must echo for one task notification. */
export function taskNotificationDeclarationDigest(
  request: Pick<
    CommitTaskNotificationResultRequest,
    "scope" | "runtimeInputId" | "taskId" | "resultJson" | "draft"
  >,
): string {
  const declaration = {
    draft: request.draft === undefined ? null : canonicalDrafts([request.draft])[0],
    operation_kind: "commit_task_notification_result",
    result: JSON.parse(canonicalRunToolJSON(request.resultJson)) as unknown,
    runtime_input_id: request.runtimeInputId,
    session_thread_id: request.scope?.sessionThreadId ?? "",
    source_id: request.draft?.sourceId ?? "",
    task_id: request.taskId,
  };
  const canonical = canonicalRunToolJSON(JSON.stringify(declaration));
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

/** Returns the SHA-256 digest Bridge must echo for one WriteEvent declaration. */
export function writeEventDeclarationDigest(
  request: Pick<
    WriteEventRequest,
    | "scope"
    | "runtimeWriteId"
    | "modelRequestId"
    | "eventType"
    | "payloadJson"
    | "stableReasoningParts"
    | "serverToolUse"
    | "mcpMaterializationHandle"
    | "drafts"
  >,
): string {
  const declaration = {
    drafts: canonicalDrafts(request.drafts),
    event_type: request.eventType,
    model_request_id: request.modelRequestId.length === 0 ? null : request.modelRequestId,
    mcp_materialization_handle: request.mcpMaterializationHandle === undefined || request.mcpMaterializationHandle.length === 0
      ? null
      : request.mcpMaterializationHandle,
    operation_kind: "write_event",
    payload: JSON.parse(canonicalRunToolJSON(request.payloadJson)) as unknown,
    runtime_write_id: request.runtimeWriteId,
    server_tool_use: request.serverToolUse === undefined
      ? null
      : {
          web_fetch_requests: request.serverToolUse.webFetchRequests,
          web_search_requests: request.serverToolUse.webSearchRequests,
        },
    session_thread_id: request.scope?.sessionThreadId ?? "",
    stable_reasoning: request.stableReasoningParts.map((part) => ({
      metadata: JSON.parse(canonicalRunToolJSON(part.metadataJson.length === 0 ? "{}" : part.metadataJson)) as unknown,
      part_sequence: part.partSequence,
      provider_part_id: part.providerPartId,
      reasoning_part_id: part.reasoningPartId,
      text: part.text,
      truncated: part.truncated,
    })),
  };
  const canonical = canonicalRunToolJSON(JSON.stringify(declaration));
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

/** Returns the SHA-256 digest Bridge must echo for one request closeout. */
export function writeRequestEndDeclarationDigest(
  request: Pick<
    WriteRequestEndRequest,
    | "scope"
    | "modelRequestId"
    | "finishReason"
    | "usageJson"
    | "modelRequestStartEventId"
    | "isError"
    | "errorKind"
    | "consumedAttachmentRefs"
    | "requestKind"
    | "reschedule"
    | "stableReasoningParts"
    | "consumedFileAttachments"
    | "drafts"
    | "prefixConsumption"
    | "compactedThroughMessageSequence"
    | "compactionEventPayloadJson"
    | "interruptSettlement"
  >,
): string {
  const declaration = {
    compacted_through_message_sequence: request.compactedThroughMessageSequence ?? null,
    compaction_event_payload: request.compactionEventPayloadJson.length === 0
      ? null
      : JSON.parse(canonicalRunToolJSON(request.compactionEventPayloadJson)) as unknown,
    consumed_attachment_refs: request.consumedAttachmentRefs,
    consumed_file_attachments: request.consumedFileAttachments.map((attachment) => ({
      file_id: attachment.fileId,
      source_event_id: attachment.sourceEventId,
    })),
    drafts: canonicalDrafts(request.drafts),
    error_kind: request.errorKind.length === 0 ? null : request.errorKind,
    finish_reason: request.finishReason,
    interrupt_settlement: request.interruptSettlement === undefined
      ? null
      : {
          event_ids: request.interruptSettlement.eventIds,
          pending_tool_cancellations: request.interruptSettlement.pendingToolCancellations.map((cancellation) => ({
            runtime_local_id: cancellation.runtimeLocalId,
            tool_use_event_id: cancellation.toolUseEventId,
          })),
          runtime_input_id: request.interruptSettlement.runtimeInputId,
          sandbox_execution_tool_use_event_ids: request.interruptSettlement.sandboxExecutionToolUseEventIds,
          sequence_from: request.interruptSettlement.sequenceFrom,
          sequence_to: request.interruptSettlement.sequenceTo,
        },
    is_error: request.isError,
    model_request_id: request.modelRequestId,
    model_request_start_event_id: request.modelRequestStartEventId,
    operation_kind: "write_request_end",
    prefix_consumption: request.prefixConsumption === undefined
      ? null
      : {
          checkpoint_runtime_local_id: request.prefixConsumption.checkpointRuntimeLocalId,
          child_thread_id: request.prefixConsumption.childThreadId,
          parent_boundary_event_id: request.prefixConsumption.parentBoundaryEventId,
        },
    request_kind: request.requestKind,
    reschedule: request.reschedule === undefined
      ? null
      : {
          attempt: request.reschedule.attempt,
          backoff_ms: request.reschedule.backoffMs,
          deadline: request.reschedule.deadline,
        },
    session_thread_id: request.scope?.sessionThreadId ?? "",
    stable_reasoning: request.stableReasoningParts.map((part) => ({
      metadata: JSON.parse(canonicalRunToolJSON(part.metadataJson.length === 0 ? "{}" : part.metadataJson)) as unknown,
      part_sequence: part.partSequence,
      provider_part_id: part.providerPartId,
      reasoning_part_id: part.reasoningPartId,
      text: part.text,
      truncated: part.truncated,
    })),
    usage: JSON.parse(canonicalRunToolJSON(request.usageJson)) as unknown,
  };
  const canonical = canonicalRunToolJSON(JSON.stringify(declaration));
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

/** Returns the SHA-256 digest Bridge must echo for one idle closeout. */
export function finishIdleDeclarationDigest(
  request: Pick<FinishIdleRequest, "scope" | "durableTurnId" | "stopReasonJson" | "drafts">,
): string {
  const declaration = {
    drafts: canonicalDrafts(request.drafts),
    durable_turn_id: request.durableTurnId,
    operation_kind: "finish_idle",
    session_thread_id: request.scope?.sessionThreadId ?? "",
    stop_reason: JSON.parse(canonicalRunToolJSON(request.stopReasonJson)) as unknown,
  };
  const canonical = canonicalRunToolJSON(JSON.stringify(declaration));
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

/** Returns the SHA-256 digest Bridge must echo for one runtime termination. */
export function runtimeTerminationDeclarationDigest(
  request: Pick<
    CommitRuntimeTerminationRequest,
    | "scope"
    | "runtimeWriteId"
    | "failureJson"
    | "drafts"
    | "pendingToolCancellations"
    | "sandboxExecutionToolUseEventIds"
  >,
): string {
  const declaration = {
    drafts: canonicalDrafts(request.drafts),
    failure: JSON.parse(canonicalRunToolJSON(request.failureJson)) as unknown,
    operation_kind: "commit_runtime_termination",
    pending_tool_cancellations: request.pendingToolCancellations.map((cancellation) => ({
      runtime_local_id: cancellation.runtimeLocalId,
      tool_use_event_id: cancellation.toolUseEventId,
    })),
    runtime_write_id: request.runtimeWriteId,
    sandbox_execution_tool_use_event_ids: request.sandboxExecutionToolUseEventIds,
    session_thread_id: request.scope?.sessionThreadId ?? "",
  };
  const canonical = canonicalRunToolJSON(JSON.stringify(declaration));
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

/** Returns the SHA-256 digest Bridge must echo for one child lifecycle declaration. */
export function childLifecycleDeclarationDigest(input: {
  readonly operationKind: "mark_child_thread_closed" | "mark_child_thread_active";
  readonly action: "close" | "resume";
  readonly sessionThreadId: string;
  readonly childThreadId: string;
  readonly sourceKind: "tool_use" | "approval_review";
  readonly sourceCommandId: string;
  readonly requestedAt: string;
}): string {
  const declaration = {
    action: input.action,
    child_thread_id: input.childThreadId,
    operation_kind: input.operationKind,
    requested_at: input.requestedAt,
    session_thread_id: input.sessionThreadId,
    source_command_id: input.sourceCommandId,
    source_kind: input.sourceKind,
  };
  const canonical = canonicalRunToolJSON(JSON.stringify(declaration));
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

function canonicalDrafts(
  drafts: CommitInputsRequest["drafts"],
): readonly unknown[] {
  return drafts.map((draft) => ({
    draft_kind: runtimeDraftKindName(draft.draftKind),
    message_info: JSON.parse(canonicalRunToolJSON(draft.messageInfoJson)) as unknown,
    ordinal: draft.ordinal,
    parts: draft.parts.map((part) => ({
      ordinal: part.ordinal,
      part_json: JSON.parse(canonicalRunToolJSON(part.partJson)) as unknown,
      part_kind: part.partKind,
      runtime_local_part_id: part.runtimeLocalPartId,
    })),
    runtime_local_id: draft.runtimeLocalId,
    source_event_id: draft.sourceEventId.length === 0 ? null : draft.sourceEventId,
    source_id: draft.sourceId,
    source_kind: draft.sourceKind,
  }));
}

function runtimeDraftKindName(kind: RuntimeDraftKind): string {
  const name = RuntimeDraftKind[kind];
  if (typeof name !== "string" || name === "RUNTIME_DRAFT_KIND_UNSPECIFIED") {
    throw new Error("runtime declaration has an invalid draft kind");
  }
  return name;
}
