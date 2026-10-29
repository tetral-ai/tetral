import { describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { ObservabilityLive } from "@tetral/ts-observability/effect";
import { recordingTelemetry } from "@tetral/ts-observability/test-support/recording-telemetry";
import { runtimePodDeclarations } from "../../src/logger.js";

describe("Effect observability layer", () => {
  test("an Effect log becomes a record with its annotations and the trace of its Effect span", async () => {
    const recording = recordingTelemetry({ serviceName: "agent-runtime", declarations: runtimePodDeclarations });
    try {
      await Effect.runPromise(Effect.logInfo("runtime_command_accepted").pipe(
        Effect.annotateLogs({ "session.id": "sesn_1" }),
        Effect.withSpan("build_context"),
        Effect.provide(ObservabilityLive),
      ));
      const [span] = recording.spans();
      expect(span?.name).toBe("build_context");
      expect(recording.records()).toEqual([expect.objectContaining({
        event: "runtime_command_accepted",
        msg: "runtime_command_accepted",
        level: "INFO",
        "session.id": "sesn_1",
        trace_id: span!.spanContext().traceId,
        span_id: span!.spanContext().spanId,
      })]);
    } finally { recording.restore(); }
  });

  test("an undeclared Effect log becomes diagnostic.invalid_event when the policy records it", async () => {
    const recording = recordingTelemetry({ serviceName: "agent-runtime", declarations: runtimePodDeclarations, undeclared: "record" });
    try {
      await Effect.runPromise(Effect.logWarning("unlisted_event").pipe(Effect.provide(ObservabilityLive)));
      expect(recording.records()).toEqual([expect.objectContaining({ event: "diagnostic.invalid_event", "event.original": "unlisted_event", level: "WARN" })]);
    } finally { recording.restore(); }
  });

  test("a failed Effect span keeps only the error class and code, and its failure text reaches neither span nor record", async () => {
    const recording = recordingTelemetry({ serviceName: "agent-runtime", declarations: runtimePodDeclarations });
    try {
      await Effect.runPromiseExit(Effect.fail({ errorCode: "provider_timeout", detail: "PRIVATE_FAILURE_TEXT" }).pipe(
        Effect.tapCause((cause) => Effect.logError("runtime_terminal_settlement", cause)),
        Effect.withSpan("agent_provider_request"),
        Effect.provide(ObservabilityLive),
      ));
      const [span] = recording.spans();
      expect(span?.events).toEqual([]);
      expect(span?.attributes).toEqual({ "error.class": "operation_failed", "error.code": "provider_timeout" });
      expect(span?.status.message).toBeUndefined();
      expect(recording.records()).toEqual([expect.objectContaining({ event: "runtime_terminal_settlement", level: "ERROR", "error.code": expect.any(String), trace_id: span!.spanContext().traceId })]);
      expect(JSON.stringify(recording.records())).not.toContain("PRIVATE_FAILURE_TEXT");
    } finally { recording.restore(); }
  });
});
