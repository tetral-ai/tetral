import { describe, expect, test } from "bun:test";
import {
  AcceptInputRejectionReason,
  CleanupSessionReason,
  InterruptOrigin,
  ToolConfirmationDecision,
} from "../../src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import {
  MailFetchMaxEnvelopes,
  MaxBindingGeneration,
  MaxIdBytes,
  MaxInputOrder,
  MaxRuntimeIngressContentBytes,
  validateAcceptAgentMailRequest,
  validateAcceptInputRequest,
  validateAcceptTaskNotificationRequest,
  validateApplyRuntimeConfigRequest,
  validateCleanupSessionRequest,
  validateInterruptRequest,
  validateResolveToolConfirmationRequest,
} from "../../src/bounds.js";

describe("method-specific Runtime ingress bounds", () => {
  test("pins the ingress content fuse and bounded mail batch", () => {
    expect(MaxRuntimeIngressContentBytes).toBe(2 * 1024 * 1024);
    expect(MailFetchMaxEnvelopes).toBe(4);
  });

  test("validates AcceptInput without a universal command envelope", () => {
    const base = { ...threadScope(), runtimeInputId: "rin_1", inputOrder: 0, messagesJson: "{}" };
    expect(validateAcceptInputRequest(base)).toEqual({ ok: true });
    expect(validateAcceptInputRequest({ ...base, inputOrder: MaxInputOrder })).toEqual({ ok: true });
    expect(validateAcceptInputRequest({ ...base, messagesJson: undefined, rejection: { reason: AcceptInputRejectionReason.ACCEPT_INPUT_REJECTION_REASON_PAYLOAD_TOO_LARGE } })).toEqual({ ok: true });
    for (const input of [
      { ...base, runtimeInputId: "" },
      { ...base, inputOrder: -1 },
      { ...base, inputOrder: MaxInputOrder + 1 },
      { ...base, messagesJson: undefined },
      { ...base, rejection: { reason: AcceptInputRejectionReason.ACCEPT_INPUT_REJECTION_REASON_RUNTIME_REJECTED } },
      { ...base, messagesJson: oversized(MaxRuntimeIngressContentBytes) },
    ]) expectInvalid(validateAcceptInputRequest(input));
  });

  test("validates each dedicated operation's owned fields", () => {
    const scope = threadScope();
    expect(validateAcceptAgentMailRequest({ ...scope, runtimeInputId: "agent_mail:d1", deliveryId: "d1", content: "mail" })).toEqual({ ok: true });
    expectInvalid(validateAcceptAgentMailRequest({ ...scope, runtimeInputId: "agent_mail:d1", deliveryId: "", content: "mail" }));

    expect(validateAcceptTaskNotificationRequest({ ...scope, runtimeInputId: "task:t1", inputOrder: 0, notificationJson: "{}" })).toEqual({ ok: true });
    expectInvalid(validateAcceptTaskNotificationRequest({ ...scope, runtimeInputId: "task:t1", inputOrder: -1, notificationJson: "{}" }));

    const interrupt = { ...scope, runtimeInputId: "rin_interrupt", origin: InterruptOrigin.INTERRUPT_ORIGIN_USER, interruptLeaseRef: { jobId: "qjob_interrupt", leaseToken: "lease_interrupt", partitionKey: "session:wksp_1:sesn_1", dedupeKey: "runtime_input:wksp_1:sesn_1:rin_interrupt" } };
    expect(validateInterruptRequest(interrupt)).toEqual({ ok: true });
    expectInvalid(validateInterruptRequest({ ...interrupt, interruptLeaseRef: undefined }));

    expect(validateResolveToolConfirmationRequest({ ...scope, runtimeInputId: "rin_confirm", toolUseEventId: "sevt_tool", decision: ToolConfirmationDecision.TOOL_CONFIRMATION_DECISION_DENY, denyMessage: "no" })).toEqual({ ok: true });
    expectInvalid(validateResolveToolConfirmationRequest({ ...scope, runtimeInputId: "rin_confirm", toolUseEventId: "", decision: ToolConfirmationDecision.TOOL_CONFIRMATION_DECISION_ALLOW }));
  });

  test("validates session-scoped config and cleanup requests", () => {
    const scope = sessionScope();
    expect(validateApplyRuntimeConfigRequest({ ...scope, sessionConfig: { generation: 1, contentJson: "{}" } })).toEqual({ ok: true });
    expect(validateApplyRuntimeConfigRequest({ ...scope, mcpManifest: { mcpServerName: "docs", generation: 2, contentJson: "{}" } })).toEqual({ ok: true });
    expectInvalid(validateApplyRuntimeConfigRequest({ ...scope }));
    expectInvalid(validateApplyRuntimeConfigRequest({ ...scope, sessionConfig: { generation: 1, contentJson: "{}" }, mcpManifest: { mcpServerName: "docs", generation: 2, contentJson: "{}" } }));
    expect(validateCleanupSessionRequest({ ...scope, cleanupOperationId: "cleanup_1", reason: CleanupSessionReason.CLEANUP_SESSION_REASON_EXPIRED })).toEqual({ ok: true });
    expectInvalid(validateCleanupSessionRequest({ ...scope, cleanupOperationId: "", reason: CleanupSessionReason.CLEANUP_SESSION_REASON_EXPIRED }));
  });

  test("applies shared identity and binding fences to every method", () => {
    const base = { ...threadScope(), runtimeInputId: "rin_1", inputOrder: 1, messagesJson: "{}" };
    for (const input of [
      { ...base, workspaceId: oversized(MaxIdBytes) },
      { ...base, sessionId: "" },
      { ...base, sessionThreadId: "" },
      { ...base, bindingId: "" },
      { ...base, bindingGeneration: 0 },
      { ...base, bindingGeneration: MaxBindingGeneration + 1 },
      { ...base, targetPodUid: "" },
    ]) expectInvalid(validateAcceptInputRequest(input));
  });
});

function sessionScope() {
  return { workspaceId: "wksp_1", sessionId: "sesn_1", bindingId: "bind_1", bindingGeneration: 1, targetPodUid: "pod-uid" };
}

function threadScope() {
  return { ...sessionScope(), sessionThreadId: "thrd_1" };
}

function expectInvalid(result: { readonly ok: boolean; readonly message?: string }) {
  expect(result).toEqual({ ok: false, message: "invalid internal request" });
}

function oversized(limit: number): string {
  return "x".repeat(limit + 1);
}
