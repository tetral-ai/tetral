/**
 * This module translates Runtime input rejection names between TypeScript and
 * the protobuf command response. It guards the closed in-band error vocabulary
 * and its generic fallback. The Runtime service calls it when encoding command
 * rejections and naming codes for logs, and it calls the generated protobuf enum.
 *
 * @packageDocumentation
 */

import { RuntimeInputErrorCode } from "./gen/tetral/agent_runtime/v1/agent_runtime.js";

/** Error names that Runtime may intentionally expose on a rejected command. */
export const RuntimeInputErrorCodes = [
  "selected_pod_identity_mismatch",
  "runtime_input_identity_conflict",
  "binding_identity_mismatch",
  "bridge_commit_unavailable",
  "bridge_commit_rejected",
  "bridge_token_unavailable",
  "bridge_task_notification_projection_invalid",
  "runtime_control_conflict",
  "runtime_context_load_failed",
  "runtime_control_not_accepted",
] as const;

/** Closed TypeScript name union for intentional in-band command rejections. */
export type RuntimeInputErrorCodeName = (typeof RuntimeInputErrorCodes)[number];

const RuntimeInputErrorCodeValues: Record<RuntimeInputErrorCodeName, RuntimeInputErrorCode> = {
  selected_pod_identity_mismatch: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_SELECTED_POD_IDENTITY_MISMATCH,
  runtime_input_identity_conflict: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_RUNTIME_INPUT_IDENTITY_CONFLICT,
  binding_identity_mismatch: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_BINDING_IDENTITY_MISMATCH,
  bridge_commit_unavailable: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_BRIDGE_COMMIT_UNAVAILABLE,
  bridge_commit_rejected: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_BRIDGE_COMMIT_REJECTED,
  bridge_token_unavailable: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_BRIDGE_TOKEN_UNAVAILABLE,
  bridge_task_notification_projection_invalid: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_BRIDGE_TASK_NOTIFICATION_PROJECTION_INVALID,
  runtime_control_conflict: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_RUNTIME_CONTROL_CONFLICT,
  runtime_context_load_failed: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_RUNTIME_CONTEXT_LOAD_FAILED,
  runtime_control_not_accepted: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_RUNTIME_CONTROL_NOT_ACCEPTED,
};

const RuntimeInputErrorCodeNames: Record<RuntimeInputErrorCode, RuntimeInputErrorCodeName | "runtime_rejected_input"> = {
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_SELECTED_POD_IDENTITY_MISMATCH]: "selected_pod_identity_mismatch",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_RUNTIME_INPUT_IDENTITY_CONFLICT]: "runtime_input_identity_conflict",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_BINDING_IDENTITY_MISMATCH]: "binding_identity_mismatch",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_BRIDGE_COMMIT_UNAVAILABLE]: "bridge_commit_unavailable",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_BRIDGE_COMMIT_REJECTED]: "bridge_commit_rejected",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_BRIDGE_TOKEN_UNAVAILABLE]: "bridge_token_unavailable",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_BRIDGE_TASK_NOTIFICATION_PROJECTION_INVALID]: "bridge_task_notification_projection_invalid",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_RUNTIME_CONTROL_CONFLICT]: "runtime_control_conflict",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_RUNTIME_CONTEXT_LOAD_FAILED]: "runtime_context_load_failed",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_RUNTIME_CONTROL_NOT_ACCEPTED]: "runtime_control_not_accepted",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_RUNTIME_REJECTED_INPUT]: "runtime_rejected_input",
  [RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_UNSPECIFIED]: "runtime_rejected_input",
  [RuntimeInputErrorCode.UNRECOGNIZED]: "runtime_rejected_input",
};

/** Maps a known name to protobuf and folds all unknown names into the generic rejection. */
export function runtimeInputErrorCodeOrGeneric(code: string): RuntimeInputErrorCode {
  return isRuntimeInputErrorCode(code)
    ? RuntimeInputErrorCodeValues[code]
    : RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_RUNTIME_REJECTED_INPUT;
}

/** Maps a protobuf value to its stable TypeScript name with a generic fallback. */
export function runtimeInputErrorCodeName(code: RuntimeInputErrorCode): RuntimeInputErrorCodeName | "runtime_rejected_input" {
  return RuntimeInputErrorCodeNames[code] ?? "runtime_rejected_input";
}

/** Narrows an arbitrary string to the closed Runtime input error vocabulary. */
export function isRuntimeInputErrorCode(code: string): code is RuntimeInputErrorCodeName {
  return RuntimeInputErrorCodes.includes(code as RuntimeInputErrorCodeName);
}
