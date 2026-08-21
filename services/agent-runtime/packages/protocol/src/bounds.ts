/**
 * Method-specific bounds for the authenticated Bridge-to-Runtime Pod ingress.
 *
 * Each validator accepts only the fields owned by its RPC. The gRPC method is
 * the command discriminator; no universal command envelope, Event range, or
 * caller-selected pod address crosses this boundary.
 *
 * @packageDocumentation
 */

import type {
  AcceptAgentMailRequest,
  AcceptInputRequest,
  AcceptTaskNotificationRequest,
  ApplyRuntimeConfigRequest,
  CleanupSessionRequest,
  InterruptRequest,
  ResolveToolConfirmationRequest,
  RecoverThreadRequest,
} from "./gen/tetral/agent_runtime/v1/agent_runtime.js";

/** Maximum UTF-8 bytes accepted for one Runtime command identifier. */
export const MaxIdBytes = 128;
/** Maximum UTF-8 bytes accepted for the selected pod UID. */
export const MaxPodIdentityBytes = 253;
/** Largest binding generation accepted by Runtime command validation. */
export const MaxBindingGeneration = 0xffffffff;
/** Largest exact JavaScript integer accepted for durable Runtime input order. */
export const MaxInputOrder = Number.MAX_SAFE_INTEGER;
/** Maximum UTF-8 bytes accepted for an existing Queue partition key. */
export const MaxQueuePartitionKeyBytes = 512;
/** Maximum UTF-8 bytes accepted for an existing Queue dedupe key. */
export const MaxQueueDedupeKeyBytes = 1280;

// This fuse is above the largest admission-legal payload and protects the
// Runtime Pod before method-specific content parsing. Keep it aligned with the
// Go sender fuse and integration/static/runtime_transport_bounds_test.go.
/** Maximum UTF-8 bytes accepted for one bounded Runtime ingress content field. */
export const MaxRuntimeIngressContentBytes = 2 * 1024 * 1024;

// Completion-mail context loads expose only bounded durable descriptors so an
// unreceipted backlog can drain incrementally.
export const MailFetchMaxEnvelopes = 4;

/** A deliberately non-descriptive validation result suitable for the internal wire. */
export type ValidationResult = { readonly ok: true } | { readonly ok: false; readonly message: "invalid internal request" };

interface ThreadBindingScope {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
}

/** Validates one durable lost-custody recovery trigger. */
export function validateRecoverThreadRequest(input: RecoverThreadRequest): ValidationResult {
  const scope = validateThreadBindingScope(input);
  if (
    !scope.ok ||
    !validId(input.sourceEventId) ||
    input.recoveryLeaseRef === undefined ||
    !validId(input.recoveryLeaseRef.jobId) ||
    !validId(input.recoveryLeaseRef.leaseToken) ||
    !validQueueKey(input.recoveryLeaseRef.partitionKey, MaxQueuePartitionKeyBytes) ||
    !validQueueKey(input.recoveryLeaseRef.dedupeKey, MaxQueueDedupeKeyBytes)
  ) {
    return invalidRequest();
  }
  return { ok: true };
}

interface SessionBindingScope {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
}

/** Validates a normal or bounded-rejection input request. */
export function validateAcceptInputRequest(input: AcceptInputRequest): ValidationResult {
  const scope = validateThreadBindingScope(input);
  if (!scope.ok || !validId(input.runtimeInputId) || !validInputOrder(input.inputOrder)) {
    return invalidRequest();
  }
  const hasMessages = input.messagesJson !== undefined;
  const hasRejection = input.rejection !== undefined;
  if (hasMessages === hasRejection) {
    return invalidRequest();
  }
  if (hasMessages && invalidContent(input.messagesJson ?? "")) {
    return invalidRequest();
  }
  if (hasRejection && (input.rejection?.reason !== 1 && input.rejection?.reason !== 2)) {
    return invalidRequest();
  }
  return { ok: true };
}

/** Validates one target-owned agent-mail delivery. */
export function validateAcceptAgentMailRequest(input: AcceptAgentMailRequest): ValidationResult {
  const scope = validateThreadBindingScope(input);
  if (
    !scope.ok ||
    !validId(input.runtimeInputId) ||
    !validId(input.deliveryId) ||
    invalidContent(input.content)
  ) {
    return invalidRequest();
  }
  return { ok: true };
}

/** Validates one canonical task-notification delivery. */
export function validateAcceptTaskNotificationRequest(input: AcceptTaskNotificationRequest): ValidationResult {
  const scope = validateThreadBindingScope(input);
  if (
    !scope.ok ||
    !validId(input.runtimeInputId) ||
    !validInputOrder(input.inputOrder) ||
    invalidContent(input.notificationJson)
  ) {
    return invalidRequest();
  }
  return { ok: true };
}

/** Validates the dedicated interrupt fence request. */
export function validateInterruptRequest(input: InterruptRequest): ValidationResult {
  const scope = validateThreadBindingScope(input);
  if (
    !scope.ok ||
    !validId(input.runtimeInputId) ||
    input.interruptLeaseRef === undefined ||
    !validId(input.interruptLeaseRef.jobId) ||
    !validId(input.interruptLeaseRef.leaseToken) ||
    !validQueueKey(input.interruptLeaseRef.partitionKey, MaxQueuePartitionKeyBytes) ||
    !validQueueKey(input.interruptLeaseRef.dedupeKey, MaxQueueDedupeKeyBytes) ||
    (input.origin !== 1 && input.origin !== 2)
  ) {
    return invalidRequest();
  }
  return { ok: true };
}

/** Validates one Bridge-derived Tool confirmation decision. */
export function validateResolveToolConfirmationRequest(input: ResolveToolConfirmationRequest): ValidationResult {
  const scope = validateThreadBindingScope(input);
  if (
    !scope.ok ||
    !validId(input.runtimeInputId) ||
    !validId(input.toolUseEventId) ||
    (input.decision !== 1 && input.decision !== 2) ||
    (input.denyMessage !== undefined && invalidContent(input.denyMessage, true))
  ) {
    return invalidRequest();
  }
  return { ok: true };
}

/** Validates one session or MCP configuration generation. */
export function validateApplyRuntimeConfigRequest(input: ApplyRuntimeConfigRequest): ValidationResult {
  const scope = validateSessionBindingScope(input);
  if (!scope.ok || (input.sessionConfig === undefined) === (input.mcpManifest === undefined)) {
    return invalidRequest();
  }
  if (input.sessionConfig !== undefined) {
    if (!validPositiveInteger(input.sessionConfig.generation) || invalidContent(input.sessionConfig.contentJson)) {
      return invalidRequest();
    }
    return { ok: true };
  }
  if (
    input.mcpManifest === undefined ||
    !validId(input.mcpManifest.mcpServerName) ||
    !validPositiveInteger(input.mcpManifest.generation) ||
    invalidContent(input.mcpManifest.contentJson)
  ) {
    return invalidRequest();
  }
  return { ok: true };
}

/** Validates one session-residency cleanup operation. */
export function validateCleanupSessionRequest(input: CleanupSessionRequest): ValidationResult {
  const scope = validateSessionBindingScope(input);
  if (
    !scope.ok ||
    !validId(input.cleanupOperationId) ||
    (input.reason !== 1 && input.reason !== 2)
  ) {
    return invalidRequest();
  }
  return { ok: true };
}

function validateThreadBindingScope(input: ThreadBindingScope): ValidationResult {
  if (!validId(input.sessionThreadId)) {
    return invalidRequest();
  }
  return validateSessionBindingScope(input);
}

function validateSessionBindingScope(input: SessionBindingScope): ValidationResult {
  if (
    !validId(input.workspaceId) ||
    !validId(input.sessionId) ||
    !validId(input.bindingId) ||
    invalidBytes(input.targetPodUid, MaxPodIdentityBytes) ||
    !validPositiveInteger(input.bindingGeneration) ||
    input.bindingGeneration > MaxBindingGeneration
  ) {
    return invalidRequest();
  }
  return { ok: true };
}

function validId(value: string): boolean {
  return !invalidBytes(value, MaxIdBytes);
}

function validQueueKey(value: string, maxBytes: number): boolean {
  return !invalidBytes(value, maxBytes);
}

function validPositiveInteger(value: number): boolean {
  return Number.isInteger(value) && value > 0;
}

function validInputOrder(value: number): boolean {
  return Number.isInteger(value) && value >= 0 && value <= MaxInputOrder;
}

function invalidContent(value: string, allowEmpty = false): boolean {
  return (!allowEmpty && value.length === 0) || new TextEncoder().encode(value).byteLength > MaxRuntimeIngressContentBytes;
}

function invalidBytes(value: string, limit: number): boolean {
  return value.length === 0 || new TextEncoder().encode(value).byteLength > limit;
}

function invalidRequest(): ValidationResult {
  return { ok: false, message: "invalid internal request" };
}
