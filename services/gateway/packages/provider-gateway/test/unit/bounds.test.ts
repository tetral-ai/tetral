import { describe, expect, test } from "bun:test";
import {
  GatewayGrpcKeepaliveTimeMs,
  GatewayGrpcKeepaliveTimeoutMs,
  grpcServerOptions,
  validateRunWebRequest,
} from "../../src/bounds.js";
import { validRunWebRequest } from "./fixtures.js";

describe("Gateway process bounds", () => {
  test("validates RunWeb envelope without performing live Web execution", () => {
    expect(validateRunWebRequest(validRunWebRequest())).toEqual({ ok: true });
    expectInvalid(validateRunWebRequest({ ...validRunWebRequest(), input: undefined }));
    expectInvalid(validateRunWebRequest({ ...validRunWebRequest(), bindingId: "" }));
    expectInvalid(validateRunWebRequest({ ...validRunWebRequest(), bindingGeneration: 0 }));
    expectInvalid(validateRunWebRequest({ ...validRunWebRequest(), runtimeBindingToken: "" }));
    expectInvalid(validateRunWebRequest({
      ...validRunWebRequest(),
      input: { searchQuery: [], open: [], find: [] },
    }));
    expectInvalid(validateRunWebRequest({
      ...validRunWebRequest(),
      input: { searchQuery: [], open: [{ url: "https://example.test", refId: "ref_1", lineno: undefined }], find: [] },
    }));
    expectInvalid(validateRunWebRequest({ ...validRunWebRequest(), toolUseEventId: "" }));
  });

  test("keeps grpc-js server options in the Gateway process package", () => {
    expect(grpcServerOptions()).toEqual({
      "grpc.max_receive_message_length": 32 * 1024 * 1024,
      "grpc.max_send_message_length": 512 * 1024,
      "grpc.max_connection_age_ms": 5 * 60 * 1000,
      "grpc.max_connection_age_grace_ms": 30 * 60 * 1000,
      "grpc.keepalive_time_ms": GatewayGrpcKeepaliveTimeMs,
      "grpc.keepalive_timeout_ms": GatewayGrpcKeepaliveTimeoutMs,
    });
    expect(GatewayGrpcKeepaliveTimeMs).toBe(30 * 1000);
    expect(GatewayGrpcKeepaliveTimeoutMs).toBe(10 * 1000);
  });
});

function expectInvalid(result: { readonly ok: boolean; readonly message?: string }) {
  expect(result.ok).toBe(false);
  if (!result.ok) {
    expect(result.message).toBe("invalid internal request");
  }
}
