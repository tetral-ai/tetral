import { describe, expect, test } from "bun:test";
import {
  capacityProofHasRequiredHeadroom,
  runGatewayCapacityProof,
  runGatewayCapacityFuseMutation,
  runGatewayAbsentRetryDelayProof,
  runGatewayReceiveCapacityFuseMutation,
  runGatewayReceiveFuseProof,
  runLargeToolInputMappingProof,
  runMaximumReadTransportProof,
  runPreEventGatewayFailureProof,
  runRecordedGLMTransportProof,
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

  test("maps a large tool input after crossing the production Gateway transport", async () => {
    const events = await runLargeToolInputMappingProof();
    expect(events[0]).toMatchObject({
      type: "tool-call",
      id: "call_large_memory_0",
      toolName: "memory",
      input: {
        action: "create",
        path: "notes/large.md",
        content: `CREATE_HEAD${"\u0001".repeat(9_000)}CREATE_TAIL`,
      },
    });
    expect(events[1]).toMatchObject({
      type: "tool-call",
      id: "call_large_memory_1",
      toolName: "memory",
      input: {
        action: "replace",
        path: "notes/large.md",
        old_text: `OLD_HEAD${"<".repeat(5_000)}OLD_TAIL`,
        new_text: `NEW_HEAD${"\\".repeat(5_000)}NEW_TAIL`,
      },
    });
    expect(events[2]).toMatchObject({ type: "finish", finishReason: "tool-calls" });
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

  test("treats a zero retry delay crossing real Gateway gRPC as absent", async () => {
    const failure = await runGatewayAbsentRetryDelayProof();
    expect(failure).toMatchObject({
      type: "provider",
      code: "provider_unavailable",
      retryable: true,
      fatal: false,
    });
    expect(failure).not.toHaveProperty("retryAfterMs");
  }, 30_000);

  test("replays recorded GLM reasoning text tool and terminal events through Gateway gRPC", async () => {
    const events = await runRecordedGLMTransportProof();
    const types = events.map((event) => event.type);

    expect(types.filter((type) => type === "reasoning-start")).toHaveLength(1);
    expect(types.filter((type) => type === "reasoning-delta")).toHaveLength(66);
    expect(types.filter((type) => type === "reasoning-end")).toHaveLength(1);
    expect(types.filter((type) => type === "text-start")).toHaveLength(1);
    expect(types.filter((type) => type === "text-delta")).toHaveLength(1);
    expect(types.filter((type) => type === "text-end")).toHaveLength(1);
    expect(types.filter((type) => type === "tool-input-start")).toHaveLength(1);
    expect(types.filter((type) => type === "tool-input-delta")).toHaveLength(1);
    expect(types.filter((type) => type === "tool-input-end")).toHaveLength(1);
    expect(events.find((event) => event.type === "tool-call")).toMatchObject({
      type: "tool-call",
      toolName: "Search",
      input: { query: "tetral" },
    });
    expect(events.at(-1)).toMatchObject({ type: "finish", finishReason: "tool-calls" });
  }, 30_000);
});
