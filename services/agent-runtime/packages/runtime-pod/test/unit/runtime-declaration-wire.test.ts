import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { RuntimeDraftKind } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { RuntimeMessageDraft } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
  childLifecycleDeclarationDigest,
  commitInputsDeclarationDigest,
  finishIdleDeclarationDigest,
  internalToolRepairDeclarationDigest,
  runtimeTerminationDeclarationDigest,
  taskNotificationDeclarationDigest,
  writeEventDeclarationDigest,
  writeRequestEndDeclarationDigest,
} from "../../src/runtime-declaration-wire.js";

const podFamilies = [
  "commit_inputs",
  "write_event",
  "write_event_request_start",
  "write_request_end",
  "finish_idle",
  "runtime_termination",
  "child_lifecycle",
  "internal_tool_repair",
  "task_notification",
] as const;

type PodDeclarationFamily = typeof podFamilies[number];

interface SharedDeclarationPart {
  readonly runtime_local_part_id: string;
  readonly part_kind: string;
  readonly ordinal: number;
  readonly part_json: string;
}

interface SharedDeclarationDraft {
  readonly runtime_local_id: string;
  readonly source_kind: string;
  readonly source_id: string;
  readonly source_event_id: string;
  readonly ordinal: number;
  readonly message_info_json: string;
  readonly part: SharedDeclarationPart;
}

interface SharedDeclarationVector {
  readonly session_thread_id: string;
  readonly runtime_input_id: string;
  readonly event_ids: readonly string[];
  readonly sequence_from: number;
  readonly sequence_to: number;
  readonly input_kind: string;
  readonly runtime_write_id: string;
  readonly model_request_id: string;
  readonly model_request_start_event_id: string;
  readonly model_tool_call_id: string;
  readonly event_type: string;
  readonly payload_json: string;
  readonly finish_reason: string;
  readonly usage_json: string;
  readonly request_kind: string;
  readonly context_through_message_sequence: number;
  readonly durable_turn_id: string;
  readonly stop_reason_json: string;
  readonly failure_json: string;
  readonly child_thread_id: string;
  readonly operation_kind: "mark_child_thread_closed" | "mark_child_thread_active";
  readonly action: "close" | "resume";
  readonly source_kind: "tool_use" | "approval_review";
  readonly source_command_id: string;
  readonly requested_at: string;
  readonly tool_name: string;
  readonly repair_key: string;
  readonly task_id: string;
  readonly result_json: string;
  readonly draft: SharedDeclarationDraft;
  readonly canonical_json: string;
  readonly digest: string;
}

interface WriteEventDeclarationByteVector {
  readonly payload_json: string;
  readonly stripped_payload_json: string;
  readonly draft?: SharedDeclarationDraft;
  readonly digest: string;
}

const corpus = JSON.parse(readFileSync(
  resolve(import.meta.dir, "../../../../../bridge/testdata/runtime_declaration_vectors.json"),
  "utf8",
)) as Record<PodDeclarationFamily | "mcp_materialization", SharedDeclarationVector>;

const writeEventByteCorpus = JSON.parse(readFileSync(
  resolve(import.meta.dir, "../../../../../bridge/testdata/write_event_declaration_byte_vectors.json"),
  "utf8",
)) as Record<string, WriteEventDeclarationByteVector>;

describe("Runtime Pod shared declaration vectors", () => {
  test("contains exactly the active Pod families plus MCP materialization", () => {
    expect(Object.keys(corpus)).toEqual([...podFamilies, "mcp_materialization"]);
  });

  test.each([...podFamilies])("%s", (family: PodDeclarationFamily) => {
    const vector = corpus[family];
    expect(createHash("sha256").update(vector.canonical_json, "utf8").digest("hex")).toBe(vector.digest);
    expect(productionDigest(family, vector)).toBe(vector.digest);
  });
});

describe("WriteEvent cross-language byte identity", () => {
  test.each(Object.entries(writeEventByteCorpus))("%s", (_name, vector) => {
    expect(writeEventDeclarationDigest({
      scope: {
        requestId: "",
        workspaceId: "",
        sessionId: "",
        sessionThreadId: "thr_digest_bytes",
        binding: undefined,
      },
      runtimeWriteId: "rwrite_digest_bytes",
      modelRequestId: "mreq_digest_bytes",
      eventType: "agent.message",
      payloadJson: vector.payload_json,
      stableReasoningParts: [],
      serverToolUse: undefined,
      drafts: vector.draft === undefined
        ? []
        : [runtimeDraft(vector.draft, RuntimeDraftKind.RUNTIME_DRAFT_KIND_TOOL_USE)],
      mcpMaterializationHandle: undefined,
      sandboxResultDigest: undefined,
      contextThroughMessageSequence: undefined,
      requestKind: "",
    })).toBe(vector.digest);
  });
});

describe("Request Start declaration identity", () => {
  test("includes the private request kind and consumed message boundary", () => {
    const base = {
      scope: {
        requestId: "request_1",
        workspaceId: "workspace_1",
        sessionId: "session_1",
        sessionThreadId: "thread_1",
        binding: undefined,
      },
      runtimeWriteId: "write_1",
      modelRequestId: "model_request_1",
      eventType: "span.model_request_start",
      payloadJson: JSON.stringify({
        type: "span.model_request_start",
        model_request_id: "model_request_1",
      }),
      sessionVisible: false,
      stableReasoningParts: [],
      serverToolUse: undefined,
      drafts: [],
      mcpMaterializationHandle: undefined,
      sandboxResultDigest: undefined,
      contextThroughMessageSequence: 4,
      requestKind: "agent_provider_request",
    };

    const digest = writeEventDeclarationDigest(base);
    expect(writeEventDeclarationDigest({
      ...base,
      contextThroughMessageSequence: 5,
    })).not.toBe(digest);
    expect(writeEventDeclarationDigest({
      ...base,
      requestKind: "compaction_summary",
    })).not.toBe(digest);
  });
});

function productionDigest(family: PodDeclarationFamily, vector: SharedDeclarationVector): string {
  const scope = {
    requestId: "",
    workspaceId: "",
    sessionId: "",
    sessionThreadId: vector.session_thread_id,
    binding: undefined,
  };
  switch (family) {
    case "commit_inputs":
      return commitInputsDeclarationDigest({
        scope,
        runtimeInputId: vector.runtime_input_id,
        eventIds: [...vector.event_ids],
        sequenceFrom: vector.sequence_from,
        sequenceTo: vector.sequence_to,
        inputKind: vector.input_kind,
        drafts: [runtimeDraft(vector.draft, RuntimeDraftKind.RUNTIME_DRAFT_KIND_USER_INPUT)],
        pendingToolCancellations: [],
        sandboxExecutionToolUseEventIds: [],
      });
    case "write_event":
    case "write_event_request_start":
      return writeEventDeclarationDigest({
        scope,
        runtimeWriteId: vector.runtime_write_id,
        modelRequestId: vector.model_request_id,
        eventType: vector.event_type,
        payloadJson: vector.payload_json,
        stableReasoningParts: [],
        serverToolUse: undefined,
        drafts: [],
        mcpMaterializationHandle: undefined,
        contextThroughMessageSequence: family === "write_event_request_start"
          ? vector.context_through_message_sequence
          : undefined,
        requestKind: family === "write_event_request_start" ? vector.request_kind : "",
      });
    case "write_request_end":
      return writeRequestEndDeclarationDigest({
        scope,
        modelRequestId: vector.model_request_id,
        finishReason: vector.finish_reason,
        usageJson: vector.usage_json,
        modelRequestStartEventId: vector.model_request_start_event_id,
        isError: false,
        errorKind: "",
        consumedAttachmentRefs: [],
        requestKind: vector.request_kind,
        reschedule: undefined,
        stableReasoningParts: [],
        consumedFileAttachments: [],
        drafts: [],
        prefixConsumption: undefined,
        compactedThroughMessageSequence: undefined,
        compactionEventPayloadJson: "",
        interruptSettlement: undefined,
      });
    case "finish_idle":
      return finishIdleDeclarationDigest({
        scope,
        durableTurnId: vector.durable_turn_id,
        stopReasonJson: vector.stop_reason_json,
        drafts: [],
      });
    case "runtime_termination":
      return runtimeTerminationDeclarationDigest({
        scope,
        runtimeWriteId: vector.runtime_write_id,
        failureJson: vector.failure_json,
        drafts: [],
        pendingToolCancellations: [],
        sandboxExecutionToolUseEventIds: [],
      });
    case "child_lifecycle":
      return childLifecycleDeclarationDigest({
        operationKind: vector.operation_kind,
        action: vector.action,
        sessionThreadId: vector.session_thread_id,
        childThreadId: vector.child_thread_id,
        sourceKind: vector.source_kind,
        sourceCommandId: vector.source_command_id,
        requestedAt: vector.requested_at,
      });
    case "internal_tool_repair":
      return internalToolRepairDeclarationDigest({
        scope,
        modelRequestId: vector.model_request_id,
        modelToolCallId: vector.model_tool_call_id,
        toolName: vector.tool_name,
        drafts: [runtimeDraft(vector.draft, RuntimeDraftKind.RUNTIME_DRAFT_KIND_INTERNAL_TOOL_REPAIR)],
      });
    case "task_notification":
      return taskNotificationDeclarationDigest({
        scope,
        runtimeInputId: vector.runtime_input_id,
        taskId: vector.task_id,
        resultJson: vector.result_json,
        draft: runtimeDraft(vector.draft, RuntimeDraftKind.RUNTIME_DRAFT_KIND_TASK_NOTIFICATION),
      });
  }
}

function runtimeDraft(vector: SharedDeclarationDraft, kind: RuntimeDraftKind): RuntimeMessageDraft {
  return {
    runtimeLocalId: vector.runtime_local_id,
    sourceKind: vector.source_kind,
    sourceId: vector.source_id,
    sourceEventId: vector.source_event_id,
    draftKind: kind,
    ordinal: vector.ordinal,
    messageInfoJson: vector.message_info_json,
    parts: [{
      runtimeLocalPartId: vector.part.runtime_local_part_id,
      partKind: vector.part.part_kind,
      ordinal: vector.part.ordinal,
      partJson: vector.part.part_json,
    }],
  };
}
