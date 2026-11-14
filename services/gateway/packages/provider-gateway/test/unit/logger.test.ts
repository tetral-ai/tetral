import { describe, expect, test } from "bun:test";
import { causeOf, semanticErrorFields, semanticErrorOutcome } from "@tetral/ts-observability";
import { createJsonLogger, logWorkloadStarted, startupFailureLogRecord, workloadStartedLogRecord } from "../../src/logger.js";

describe("Provider Gateway logger", () => {
  test("emits required resource fields by default", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({ write: (line) => lines.push(line) });

    logger.info({ event: "provider_request_streamed", "event.kind": "provider_request_streamed" });

    const record = JSON.parse(lines[0] ?? "{}") as Record<string, unknown>;
    expect(record).toMatchObject({
      level: "INFO",
      msg: "provider_request_streamed",
      "service.name": "provider-gateway",
      "deployment.environment": "unknown",
      "service.version": "unknown",
      event: "provider_request_streamed",
      "event.kind": "provider_request_streamed",
    });
  });

  test("inherits logger-owned resource fields and timestamp", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({
      write: (line) => lines.push(line),
      deploymentEnvironment: "prod",
      serviceVersion: "v1",
      clock: () => new Date("2026-08-09T04:28:21.000Z"),
    });

    logger.error({
      event: "provider_stream_incomplete",
      "service.name": "spoofed-service",
      "deployment.environment": "spoofed-env",
      "service.version": "spoofed-version",
      level: "info",
      time: "1900-01-01T00:00:00.000Z",
      ...semanticErrorFields({ errorCode: "provider_stream_incomplete", cause: causeOf(new Error("the stream ended inside a text fragment")) }),
    });

    const record = JSON.parse(lines[0] ?? "{}") as Record<string, unknown>;
    expect(record).toMatchObject({
      level: "ERROR",
      "service.name": "provider-gateway",
      "deployment.environment": "prod",
      "service.version": "v1",
      time: "2026-08-09T04:28:21.000000Z",
      event: "provider_stream_incomplete",
    });
  });

  test("a failure record built without semanticErrorFields fails type-check", () => {
    const logger = createJsonLogger({ write: () => undefined });
    if (false) {
      // @ts-expect-error A failure record takes its code and cause from semanticErrorFields.
      logger.info({ event: "provider_request_streamed", [semanticErrorOutcome]: true });
    }
    expect(logger).toBeDefined();
  });

  test("redacts sensitive caller fields and values through the shared wrapper", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({ write: (line) => lines.push(line) });

    logger.error({
      event: "provider_request_streamed",
      authorization: "Bearer sk-live-provider-secret",
      api_key: "sk-live-api-key",
      provider_response: "upstream rejected token=secret-value",
      session_token: "credential-value",
      engine_vault_key: "credential-value",
      message: "request failed",
      ...semanticErrorFields({ errorCode: "invalid_argument", cause: causeOf(new Error("the request names no model")) }),
    });

    const serialized = lines[0] ?? "";
    expect(serialized).not.toContain("sk-live-provider-secret");
    expect(serialized).not.toContain("sk-live-api-key");
    expect(serialized).not.toContain("token=secret-value");
    const record = JSON.parse(serialized) as Record<string, unknown>;
    expect(record.authorization).toBe("[REDACTED]");
    expect(record.api_key).toBe("[REDACTED]");
    expect(record.provider_response).toBe("[REDACTED]");
    expect(record.session_token).toBe("[REDACTED]");
    expect(record.engine_vault_key).toBe("[REDACTED]");
    expect(record["error.message_safe"]).toBe("provider request failed");
    expect(record["error.message"]).toBe("the request names no model");
  });

  test("a startup failure record carries its cause: the configuration problem, or the error", () => {
    const written = (record: ReturnType<typeof startupFailureLogRecord>): unknown => {
      const lines: string[] = [];
      createJsonLogger({ write: (line) => lines.push(line) }).error(record);
      return JSON.parse(lines[0] ?? "{}");
    };
    expect(written(startupFailureLogRecord({ kind: "config_error", message: "invalid gateway config" }))).toMatchObject({
      event: "startup_failed",
      "event.kind": "startup_failed",
      operation: "startup",
      component: "gateway",
      kind: "config_error",
      message: "invalid gateway config",
      "startup.cause_category": "configuration",
      "startup.config_problem": "invalid gateway config",
      "error.class": "config_error",
      "error.code": "config_error",
      "error.message_safe": "configuration invalid",
      "error.message": "invalid gateway config",
    });
    expect(written(startupFailureLogRecord({
      kind: "startup_error",
      message: "gateway service startup failed",
      error: new Error("listen EADDRINUSE: address already in use 127.0.0.1:8090"),
      causeCategory: "listener",
    }))).toMatchObject({
      event: "startup_failed",
      kind: "startup_error",
      message: "gateway service startup failed",
      "startup.cause_category": "listener",
      "error.class": "startup_error",
      "error.code": "startup_error",
      "error.message_safe": "gateway service startup failed",
      "error.message": "listen EADDRINUSE: address already in use 127.0.0.1:8090",
    });
    expect(startupFailureLogRecord({
      kind: "startup_error",
      message: "gateway service startup failed",
      causeCategory: "schema",
    })["startup.cause_category"]).toBe("schema");
  });

  test("started logging uses workload vocabulary and cannot change readiness", () => {
    expect(workloadStartedLogRecord()).toEqual({
      event: "workload.started",
      "event.kind": "started",
      operation: "workload.lifecycle",
      component: "workload",
      "listener.transport": "tcp",
      "readiness.state": "ready",
    });
    expect(() => logWorkloadStarted({
      info: () => { throw new Error("sink unavailable"); },
      error: () => undefined,
    })).not.toThrow();
  });
});
