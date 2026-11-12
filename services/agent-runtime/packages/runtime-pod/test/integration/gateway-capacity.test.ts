import { describe, expect, test } from "bun:test";
import {
  capacityProofHasRequiredHeadroom,
  runGatewayCapacityProof,
  runGatewayCapacityFuseMutation,
  runGatewayReceiveCapacityFuseMutation,
  runMaximumReadTransportProof,
} from "../harness/gateway-transport-harness.js";

describe("Runtime-to-Gateway catalog capacity", () => {
  test("carries the maximum-context production vectors with transport headroom", async () => {
    const measurements = await runGatewayCapacityProof();

    expect(measurements).toHaveLength(5);
    for (const measurement of measurements) {
      expect(capacityProofHasRequiredHeadroom(measurement)).toBe(true);
      expect(measurement.estimatedTokens).toBeLessThanOrEqual(measurement.modelLimitTokens);
      expect(Object.keys(measurement.loweredBytesByFamily).sort()).toEqual([
        "anthropic",
        "openai",
        "openai-compatible",
      ]);
      expect(Math.max(...Object.values(measurement.loweredBytesByFamily))).toBeLessThan(measurement.configuredFuseBytes);
    }
    const coldHistory = measurements.find((measurement) => measurement.vector === "escape_dense_output_history");
    expect(coldHistory?.loadedContextBytes).toBeGreaterThan(20 * 1024 * 1024);
    expect(coldHistory?.loadedContextBytes).toBeLessThan((64 * 1024 * 1024) * 0.8);
  }, 60_000);

  test("fails when the Runtime request carrier is lowered below a maximum vector", async () => {
    await expect(runGatewayCapacityFuseMutation()).rejects.toMatchObject({
      type: "gateway-client",
      code: "gateway_protocol_error",
      message: "Gateway request exceeded the local transport fuse.",
      retryable: false,
      fatal: true,
    });
  }, 30_000);

  test("fails when the Gateway receive carrier is lowered below a maximum vector", async () => {
    await expect(runGatewayReceiveCapacityFuseMutation()).rejects.toMatchObject({
      type: "gateway-client",
      code: "gateway_protocol_error",
      message: "Gateway rejected the request above its transport fuse.",
      retryable: false,
      fatal: true,
    });
  }, 30_000);

  test("carries an exact maximum Read result through formatting and provider transport", async () => {
    const measurement = await runMaximumReadTransportProof();
    expect(measurement.envelopeBytes).toBe(200_000);
    expect(measurement.projectedOutputBytes).toBeLessThanOrEqual(512 * 1024);
    expect(measurement.providerRequestBytes).toBeLessThan(64 * 1024 * 1024);
    expect(measurement.providerBodyContainsMarkers).toBe(true);
    expect(measurement.outputPreserved).toBe(true);
    expect(measurement.truncated).toBe(false);
  }, 30_000);
});
