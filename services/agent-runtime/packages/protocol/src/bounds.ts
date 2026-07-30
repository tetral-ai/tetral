/**
 * This module validates the bounded identity, sequence, target, and payload
 * envelope accepted by the Runtime Pod command service. It guards the pod from
 * oversized or malformed Bridge commands before dispatch, and the command
 * handler calls it before invoking Runtime Core while it calls only local byte
 * and network-address validators.
 *
 * @packageDocumentation
 */

import { isIP } from "node:net";

/** Maximum UTF-8 bytes accepted for one Runtime command identifier. */
export const MaxIdBytes = 128;
/** Maximum UTF-8 bytes accepted for one pod identity or address field. */
export const MaxPodIdentityBytes = 253;
/** Largest binding generation accepted by Runtime command-envelope validation. */
export const MaxBindingGeneration = 0xffffffff;
/** Largest exact JavaScript integer accepted for event sequence fields. */
export const MaxSequenceValue = Number.MAX_SAFE_INTEGER;
// MaxPayloadJsonBytes bounds the UTF-8 byte length of a runtime input command's
// payloadJson carrier, enforced below in validateRuntimeInputCommandRequest.
// This is a fuse sized above the largest admission-legal payload, not a gate
// below it: 2 MiB admits a 1 MiB admission-legal text block plus JSON envelope
// overhead, with headroom. This 2 MiB value lives only here in TypeScript; the
// exact literal is pinned by the guard in
// integration/static/runtime_transport_bounds_test.go.
// The fuse assumes delivery-path re-serialization leaves payload data bytes
// raw; the Go path disables HTML escaping via marshalBridgeDataJSON in
// services/bridge/bridge_api_tools.go.
// UPDATE-WITH: services/agent-runtime/packages/protocol/test/unit/bounds.test.ts;
//   integration/static/runtime_transport_bounds_test.go;
//   internal/sessionrpc/bounds.go;
//   services/bridge/runtime_delivery.go
/** Maximum UTF-8 bytes accepted for one Runtime command payload JSON carrier. */
export const MaxPayloadJsonBytes = 2 * 1024 * 1024;

// Completion-mail context loads expose only bounded durable descriptors so an
// unreceipted backlog can drain incrementally.
// UPDATE-WITH: services/bridge/completion_mail.go;
//   integration/static/runtime_transport_bounds_test.go.
export const MailFetchMaxEnvelopes = 4;

/** The complete untrusted command envelope checked before Runtime dispatch. */
export interface RuntimeInputCommandValidationInput {
  readonly requestId: string;
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodNamespace: string;
  readonly targetPodName: string;
  readonly targetPodUid: string;
  readonly targetPodIp: string;
  readonly runtimeInputId: string;
  readonly eventIds: readonly string[];
  readonly sequenceFrom: number;
  readonly sequenceTo: number;
  readonly commandKind: number;
  readonly payloadJson: string;
}

/** A deliberately non-descriptive validation result suitable for the internal wire. */
export type ValidationResult = { readonly ok: true } | { readonly ok: false; readonly message: "invalid internal request" };

/** Validates one Runtime command without exposing which boundary check failed. */
export function validateRuntimeInputCommandRequest(input: RuntimeInputCommandValidationInput): ValidationResult {
  const ids = validateIdFields(
    input.requestId,
    input.workspaceId,
    input.sessionId,
    input.sessionThreadId,
    input.bindingId,
    input.runtimeInputId,
  );
  if (!ids.ok) {
    return ids;
  }
  for (const eventId of input.eventIds) {
    const event = validateIdFields(eventId);
    if (!event.ok) {
      return event;
    }
  }
  for (const value of [
    input.targetPodNamespace,
    input.targetPodName,
    input.targetPodUid,
    input.targetPodIp,
  ]) {
    if (invalidBytes(value, MaxPodIdentityBytes)) {
      return invalidRequest();
    }
  }
  if (isIP(input.targetPodIp) === 0) {
    return invalidRequest();
  }
  if (invalidBindingGeneration(input.bindingGeneration)) {
    return invalidRequest();
  }
  if (input.commandKind < 1 || input.commandKind > 7 || !Number.isInteger(input.commandKind)) {
    return invalidRequest();
  }
  if (
    !Number.isInteger(input.sequenceFrom) ||
    !Number.isInteger(input.sequenceTo) ||
    input.sequenceFrom < 0 ||
    input.sequenceTo < input.sequenceFrom ||
    input.sequenceTo > MaxSequenceValue
  ) {
    return invalidRequest();
  }
  if (new TextEncoder().encode(input.payloadJson).byteLength > MaxPayloadJsonBytes) {
    return invalidRequest();
  }
  return { ok: true };
}

function validateIdFields(...values: readonly string[]): ValidationResult {
  for (const value of values) {
    if (invalidBytes(value, MaxIdBytes)) {
      return invalidRequest();
    }
  }
  return { ok: true };
}

function invalidBytes(value: string, limit: number): boolean {
  return value.length === 0 || new TextEncoder().encode(value).byteLength > limit;
}

function invalidBindingGeneration(value: number): boolean {
  return !Number.isInteger(value) || value <= 0 || value > MaxBindingGeneration;
}

function invalidRequest(): ValidationResult {
  return { ok: false, message: "invalid internal request" };
}
