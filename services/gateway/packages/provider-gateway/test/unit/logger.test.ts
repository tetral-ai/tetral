import { describe, expect, test } from "bun:test";
import { semanticErrorOutcome } from "@tetral/ts-observability";
import { createJsonLogger, startupFailureLogRecord } from "../../src/logger.js";

describe("Provider Gateway logger", () => {
  test("emits required resource fields by default", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({ write: (line) => lines.push(line) });

    logger.info({ event: "provider_request_streamed", "event.kind": "provider_request_streamed" });

    const record = JSON.parse(lines[0] ?? "{}") as Record<string, unknown>;
    expect(record).toMatchObject({
      level: "info",
      "service.name": "gateway",
      "deployment.environment": "unknown",
      "service.version": "unknown",
      event: "provider_request_streamed",
      "event.kind": "provider_request_streamed",
    });
  });

  test("does not let caller records override shared resource fields", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({ write: (line) => lines.push(line), deploymentEnvironment: "prod", serviceVersion: "v1" });

    logger.error({
      event: "provider_request_failed",
      "service.name": "spoofed-service",
      "deployment.environment": "spoofed-env",
      "service.version": "spoofed-version",
      level: "info",
    });

    const record = JSON.parse(lines[0] ?? "{}") as Record<string, unknown>;
    expect(record).toMatchObject({
      level: "error",
      "service.name": "gateway",
      "deployment.environment": "prod",
      "service.version": "v1",
      event: "provider_request_failed",
    });
  });

  test("requires the complete tuple when a record marks a semantic failure", () => {
    const logger = createJsonLogger({ write: () => undefined });

    // @ts-expect-error semantic failure records require class, code, and safe message
    logger.info({ event: "provider_request_failed", [semanticErrorOutcome]: true });
    logger.info({
      event: "provider_request_failed",
      [semanticErrorOutcome]: true,
      "error.class": "provider_error",
      "error.code": "provider_authentication_error",
      "error.message_safe": "provider returned 401",
    });
  });

  test("redacts sensitive caller fields and values through the shared wrapper", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({ write: (line) => lines.push(line) });

    logger.error({
      event: "provider_request_failed",
      authorization: "Bearer sk-live-provider-secret",
      api_key: "sk-live-api-key",
      provider_response: "upstream rejected token=secret-value",
      message: "request failed",
      [semanticErrorOutcome]: true,
      "error.class": "provider_error",
      "error.code": "provider_authentication_error",
      "error.message_safe": "provider returned 401",
    });

    const serialized = lines[0] ?? "";
    expect(serialized).not.toContain("sk-live-provider-secret");
    expect(serialized).not.toContain("sk-live-api-key");
    expect(serialized).not.toContain("token=secret-value");
    const record = JSON.parse(serialized) as Record<string, unknown>;
    expect(record.authorization).toBe("[REDACTED]");
    expect(record.api_key).toBe("[REDACTED]");
    expect(record.provider_response).toBe("[REDACTED]");
    expect(record["error.message_safe"]).toBe("provider returned 401");
  });

  test("startup failure helper emits shared safe error fields", () => {
    expect(startupFailureLogRecord({ kind: "config_error", message: "invalid gateway config" })).toMatchObject({
      event: "startup_failed",
      "event.kind": "startup_failed",
      operation: "startup",
      component: "gateway",
      kind: "config_error",
      message: "invalid gateway config",
      "error.class": "config_error",
      "error.code": "config_error",
      "error.message_safe": "invalid gateway config",
    });
    expect(startupFailureLogRecord({ kind: "startup_error", message: "raw provider body" })).toMatchObject({
      event: "startup_failed",
      "event.kind": "startup_failed",
      operation: "startup",
      component: "gateway",
      kind: "startup_error",
      message: "gateway service startup failed",
      "error.class": "startup_error",
      "error.code": "startup_error",
      "error.message_safe": "gateway service startup failed",
    });
  });
});
