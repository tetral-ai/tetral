import { describe, expect, test } from "bun:test";
import {
  MailFetchMaxEnvelopes,
  MaxBindingGeneration,
  MaxIdBytes,
  MaxPayloadJsonBytes,
  MaxPodIdentityBytes,
  MaxSequenceValue,
  validateRuntimeInputCommandRequest,
} from "../../src/bounds.js";

describe("protocol bounds", () => {
  test("pins the command payload fuse above the public request cap", () => {
    expect(MaxPayloadJsonBytes).toBe(2 * 1024 * 1024);
    expect(MaxPayloadJsonBytes).toBeGreaterThan(1 * 1024 * 1024);
  });

  test("pins bounded completion-mail context batches", () => {
    expect(MailFetchMaxEnvelopes).toBe(4);
  });

  test("rejects invalid Runtime Pod command envelope fields before side effects", () => {
    const base = validRuntimeInputCommandRequest();
    for (const request of [
      { ...base, requestId: oversized(MaxIdBytes) },
      { ...base, requestId: "" },
      { ...base, workspaceId: oversized(MaxIdBytes) },
      { ...base, workspaceId: "" },
      { ...base, sessionId: oversized(MaxIdBytes) },
      { ...base, sessionId: "" },
      { ...base, sessionThreadId: oversized(MaxIdBytes) },
      { ...base, sessionThreadId: "" },
      { ...base, bindingId: oversized(MaxIdBytes) },
      { ...base, bindingId: "" },
      { ...base, runtimeInputId: oversized(MaxIdBytes) },
      { ...base, runtimeInputId: "" },
      { ...base, eventIds: ["sevt_1", oversized(MaxIdBytes)] },
      { ...base, eventIds: [""] },
      { ...base, targetPodNamespace: oversized(MaxPodIdentityBytes) },
      { ...base, targetPodNamespace: "" },
      { ...base, targetPodName: oversized(MaxPodIdentityBytes) },
      { ...base, targetPodName: "" },
      { ...base, targetPodUid: oversized(MaxPodIdentityBytes) },
      { ...base, targetPodUid: "" },
      { ...base, targetPodIp: oversized(MaxPodIdentityBytes) },
      { ...base, targetPodIp: "not-an-ip" },
      { ...base, bindingGeneration: 0 },
      { ...base, bindingGeneration: -1 },
      { ...base, bindingGeneration: 1.5 },
      { ...base, bindingGeneration: MaxBindingGeneration + 1 },
      { ...base, commandKind: 0 },
      { ...base, commandKind: 8 },
      { ...base, commandKind: 1.5 },
      { ...base, sequenceFrom: -1 },
      { ...base, sequenceFrom: 1.5 },
      { ...base, sequenceTo: 0 },
      { ...base, sequenceTo: 1.5 },
      { ...base, sequenceTo: MaxSequenceValue + 1 },
      { ...base, payloadJson: oversized(MaxPayloadJsonBytes) },
    ]) {
      expectInvalid(validateRuntimeInputCommandRequest(request));
    }
  });

  test("accepts valid Runtime Pod command envelope edge values", () => {
    expect(validateRuntimeInputCommandRequest({
      ...validRuntimeInputCommandRequest(),
      commandKind: 7,
      eventIds: [],
      sequenceFrom: 0,
      sequenceTo: 0,
      payloadJson: "",
    })).toEqual({ ok: true });
    expect(validateRuntimeInputCommandRequest({
      ...validRuntimeInputCommandRequest(),
      sequenceFrom: MaxSequenceValue,
      sequenceTo: MaxSequenceValue,
      payloadJson: "x".repeat(MaxPayloadJsonBytes),
    })).toEqual({ ok: true });
  });

});

function expectInvalid(result: { readonly ok: boolean; readonly message?: string }) {
  expect(result.ok).toBe(false);
  if (!result.ok) {
    expect(result.message).toBe("invalid internal request");
  }
}

function validRuntimeInputCommandRequest() {
  return {
    requestId: "req_1",
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 1,
    targetPodNamespace: "engine",
    targetPodName: "runtime-pod-a",
    targetPodUid: "pod-uid",
    targetPodIp: "10.0.0.1",
    runtimeInputId: "rin_1",
    eventIds: ["sevt_1"],
    sequenceFrom: 1,
    sequenceTo: 1,
    commandKind: 1,
    payloadJson: "{}",
  };
}

function oversized(limit: number): string {
  return "x".repeat(limit + 1);
}
