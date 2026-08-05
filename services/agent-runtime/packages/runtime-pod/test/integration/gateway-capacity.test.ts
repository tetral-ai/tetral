import { describe, expect, test } from "bun:test";
import {
  capacityProofHasRequiredHeadroom,
  runGatewayCapacityProof,
  runGatewayCapacityFuseMutation,
  runGatewayReceiveFuseProof,
  runLargeToolInputMappingProof,
  runMaximumReadTransportProof,
  runPreEventGatewayFailureProof,
} from "../harness/gateway-transport-harness.js";

describe("Runtime-to-Gateway catalog capacity", () => {
  test("carries the maximum-context production vectors with transport headroom", async () => {
    const measurements = await runGatewayCapacityProof();

    expect(measurements).toHaveLength(4);
    for (const measurement of measurements) {
      expect(capacityProofHasRequiredHeadroom(measurement)).toBe(true);
      expect(Object.keys(measurement.loweredBytesByFamily).sort()).toEqual([
        "anthropic",
        "openai",
        "openai-compatible",
      ]);
      expect(Math.max(...Object.values(measurement.loweredBytesByFamily))).toBeLessThan(measurement.configuredFuseBytes);
    }
  }, 60_000);

  test("fails when the Runtime request carrier is lowered below a maximum vector", async () => {
    await expect(runGatewayCapacityFuseMutation()).rejects.toMatchObject({
      type: "gateway-client",
    });
  }, 30_000);

  test("maps a large tool input after crossing the production Gateway transport", async () => {
    const events = await runLargeToolInputMappingProof();
    expect(events[0]).toMatchObject({
      type: "tool-call",
      id: "call_large",
      toolName: "Write",
      input: { file_path: "notes/large.txt", content: "\u0001".repeat(200_000) },
    });
    expect(events[1]).toMatchObject({ type: "finish", finishReason: "tool-calls" });
  }, 30_000);

  test("carries an exact maximum Read result through formatting and provider transport", async () => {
    const measurement = await runMaximumReadTransportProof();
    expect(measurement.envelopeBytes).toBe(200_000);
    expect(measurement.projectedOutputBytes).toBeLessThanOrEqual(256 * 1024);
    expect(measurement.providerRequestBytes).toBeLessThan(32 * 1024 * 1024);
    expect(measurement.providerBodyContainsMarker).toBe(true);
  }, 30_000);

  test("classifies a real oversized Gateway event as a local receive-fuse failure", async () => {
    expect(await runGatewayReceiveFuseProof()).toMatchObject({
      type: "gateway-client",
      code: "gateway_protocol_error",
      retryable: false,
      fatal: true,
    });
  }, 30_000);

  test("normalizes real pre-event Gateway rejection and unavailability", async () => {
    const failures = await runPreEventGatewayFailureProof();
    expect(failures.invalidArgument).toMatchObject({
      type: "llm-service",
      error: { code: "gateway_protocol_error", retryable: false, fatal: true },
    });
    expect(failures.unavailable).toMatchObject({
      type: "llm-service",
      error: { code: "gateway_stream_error", retryable: true, fatal: false },
    });
  }, 30_000);
});
