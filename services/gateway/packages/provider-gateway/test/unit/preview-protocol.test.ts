import { describe, expect, test } from "bun:test";
import fixture from "../../../../../../integration/testdata/public-streaming.json";
import { PreviewLimits, encodePreviewFrame, previewSubject, previewFrameEncodedSize } from "../../src/providers/preview-protocol.js";
import type { PreviewFrame } from "../../src/providers/preview-protocol.js";
import { parsePreviewNatsConfig, PreviewPublisherDefaults } from "../../src/providers/preview-config.js";

const identity = fixture.wire.identity;
function delta(text: string): Extract<PreviewFrame, { kind: "event_delta" }> {
  return { ...identity, version: 1, request_kind: "agent_provider_request", kind: "event_delta", event_type: "agent.message", event_id: fixture.wire.message_event_id, preview_sequence: 1, text };
}
describe("private preview protocol", () => {
  test("shared fixture fixes scope encoding and strict field whitelists", () => {
    const frames: PreviewFrame[] = [
      { ...identity, version: 1, request_kind: "agent_provider_request", kind: "request_open" },
      { ...identity, version: 1, request_kind: "agent_provider_request", kind: "event_start", event_type: "agent.message", event_id: fixture.wire.message_event_id, preview_sequence: 0 },
      delta(fixture.content.first_fragments[0]!),
    ];
    for (const [index, frame] of frames.entries()) {
      const decoded = JSON.parse(new TextDecoder().decode(encodePreviewFrame(frame)));
      expect(Object.keys(decoded).sort()).toEqual([fixture.wire.request_open_keys, fixture.wire.event_start_keys, fixture.wire.event_delta_keys][index]!);
      expect(decoded).toEqual(frame);
    }
    expect(previewSubject(identity.workspace_id, identity.session_id)).toBe("preview.v1.d29ya3NwYWNlIEEvzrI.c2Vzc18wMTIzNDU2Nzg5YWJjZGVm");
    for (const scope of ["", "bad\n", "\ud83d"]) expect(() => previewSubject(scope, identity.session_id)).toThrow();
  });
  test("encoded UTF-8 and escaped JSON frame boundaries use independent padding", () => {
    const overhead = new TextEncoder().encode(JSON.stringify(delta(""))).length;
    for (const size of [262143, 262144, 262145]) {
      const frame = delta("x".repeat(size - overhead));
      if (size > 262144) expect(() => encodePreviewFrame(frame)).toThrow();
      else expect(encodePreviewFrame(frame).length).toBe(size);
    }
    expect(PreviewLimits).toEqual({ maxFrameBytes: 262144, maxEventIdentities: 4096 });
    for (const text of ["β", "😀", 'quote"\n\\']) expect(JSON.parse(new TextDecoder().decode(encodePreviewFrame(delta(text)))).text).toBe(text);
  });
  test("no reasoning, unknown keys, unsafe sequence or isolated surrogate enters private wire", () => {
    const good = delta("alpha");
    for (const bad of [
      { ...good, provider_metadata: fixture.content.reasoning_signature },
      { ...good, event_type: "agent.thinking" }, { ...good, kind: "finish" },
      { ...good, preview_sequence: 0 }, { ...good, preview_sequence: Number.MAX_SAFE_INTEGER + 1 },
      { ...good, text: "\ud83d" }, { ...good, text: "\ude00" }, { ...good, event_id: "provider-part-id" },
    ]) expect(() => encodePreviewFrame(bad as PreviewFrame)).toThrow();
  });
  test("direct encoder exactly matches standard JSON UTF-8 for every control escape and scalar width", () => {
    const controls = Array.from({ length: 128 }, (_, value) => String.fromCharCode(value)).join("");
    for (const text of [controls, "\u0080\u07ff\u0800\ud7ff\ue000\uffff😀\u2028\u2029/β", fixture.content.first_complete]) {
      const frame = { ...delta(text), preview_sequence: Number.MAX_SAFE_INTEGER };
      const expected = new TextEncoder().encode(JSON.stringify(frame));
      expect(previewFrameEncodedSize(frame)).toBe(expected.length);
      expect(encodePreviewFrame(frame)).toEqual(expected);
    }
    // JSON expansion is rejected during sizing, before a serialized allocation.
    expect(() => previewFrameEncodedSize(delta("\x00".repeat(65536)))).toThrow();
  });
});
describe("preview typed configuration", () => {
  const base = { TETRAL_NATS_SERVERS: "nats://localhost:4222", TETRAL_NATS_USER_PATH: "/private/user", TETRAL_NATS_PASSWORD_PATH: "/private/password" };
  test("unset wiring and restart-only defaults are explicit", () => {
    expect(parsePreviewNatsConfig({})).toBeUndefined();
    expect(parsePreviewNatsConfig(base)?.policy).toEqual({ queueBytes: 4194304, queueFrames: 1024, batchBytes: 262144, batchFrames: 64, connectTimeoutMs: 1000, flushTimeoutMs: 1000, retryMaxMs: 5000, credentialPollMs: 250, pingIntervalMs: 1000, maxPingOut: 1 });
    expect(PreviewPublisherDefaults.queueBytes).toBe(4194304);
    expect(parsePreviewNatsConfig({ ...base, TETRAL_GATEWAY_PREVIEW_BATCH_FRAMES: "7", TETRAL_NATS_CONNECT_TIMEOUT_MS: "123" })?.policy.batchFrames).toBe(7);
  });
  test("partial credentials, embedded credentials, mixed transport and invalid combinations fail before serving", () => {
    for (const env of [
      { TETRAL_NATS_SERVERS: base.TETRAL_NATS_SERVERS },
      { ...base, TETRAL_NATS_SERVERS: "nats://user:secret@localhost:4222" },
      { ...base, TETRAL_NATS_TLS_CA_PATH: "/private/ca" },
      { ...base, TETRAL_NATS_TLS_CA_PATH: "/private/ca", TETRAL_NATS_TLS_CERT_PATH: "/private/cert", TETRAL_NATS_TLS_KEY_PATH: "/private/key" },
      { ...base, TETRAL_GATEWAY_PREVIEW_QUEUE_BYTES: "3" },
      { ...base, TETRAL_GATEWAY_PREVIEW_BATCH_FRAMES: "1025" },
      { ...base, TETRAL_NATS_CONNECT_TIMEOUT_MS: "01" },
      { ...base, TETRAL_GATEWAY_PREVIEW_BATCH_BYTES: "262145" },
    ]) expect(() => parsePreviewNatsConfig(env)).toThrow();
    expect(parsePreviewNatsConfig({ ...base, TETRAL_NATS_SERVERS: "tls://broker.test:4222", TETRAL_NATS_TLS_CA_PATH: "/private/ca", TETRAL_NATS_TLS_CERT_PATH: "/private/cert", TETRAL_NATS_TLS_KEY_PATH: "/private/key" })?.tls?.caPath).toBe("/private/ca");
  });
  test("protected addresses require DNS for both IP families while standard literal addresses remain usable", () => {
    for (const address of ["127.0.0.1", "[::1]", "[2001:db8::1]"]) {
      expect(() => parsePreviewNatsConfig({ ...base, TETRAL_NATS_SERVERS: `tls://${address}:4222`, TETRAL_NATS_TLS_CA_PATH: "/private/ca", TETRAL_NATS_TLS_CERT_PATH: "/private/cert", TETRAL_NATS_TLS_KEY_PATH: "/private/key" })).toThrow("invalid preview server");
      expect(parsePreviewNatsConfig({ ...base, TETRAL_NATS_SERVERS: `nats://${address}:4222` })?.servers).toEqual([`nats://${address}:4222`]);
    }
  });
  test("heartbeat units and outstanding-count bounds are independent typed process controls", () => {
    const selected = parsePreviewNatsConfig({ ...base, TETRAL_NATS_PING_INTERVAL_MS: "125", TETRAL_NATS_MAX_PING_OUT: "3" });
    expect(selected?.policy.pingIntervalMs).toBe(125); expect(selected?.policy.maxPingOut).toBe(3);
    expect(selected?.policy.connectTimeoutMs).toBe(1000); expect(selected?.policy.retryMaxMs).toBe(5000);
    for (const [key, values] of [["TETRAL_NATS_PING_INTERVAL_MS", ["0", "01", "60001", "1.5", "NaN"]], ["TETRAL_NATS_MAX_PING_OUT", ["0", "17", "9007199254740992"]]] as const) {
      for (const value of values) expect(() => parsePreviewNatsConfig({ ...base, [key]: value })).toThrow();
    }
  });
});
