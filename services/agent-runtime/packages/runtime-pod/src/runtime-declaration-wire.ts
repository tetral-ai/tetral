/**
 * Builds the canonical digest for one CommitInputs declaration. It guards
 * transport replay identity by hashing semantic declaration fields only;
 * BridgeAPIContextLoader calls it before transport and validates the returned
 * receipt before exposing durable stamps to Runtime Core.
 */

import { createHash } from "node:crypto";
import { RuntimeDraftKind } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { CommitInputsRequest } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { canonicalRunToolJSON } from "./run-tool-canonical-json.js";

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
    runtime_input_id: request.runtimeInputId,
    sequence_from: request.sequenceFrom,
    sequence_to: request.sequenceTo,
    session_thread_id: request.scope?.sessionThreadId ?? "",
  };
  const canonical = canonicalRunToolJSON(JSON.stringify(declaration));
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

function runtimeDraftKindName(kind: RuntimeDraftKind): string {
  const name = RuntimeDraftKind[kind];
  if (typeof name !== "string" || name === "RUNTIME_DRAFT_KIND_UNSPECIFIED") {
    throw new Error("runtime declaration has an invalid draft kind");
  }
  return name;
}
