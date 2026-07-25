import { describe, expect, test } from "bun:test";
import { semanticErrorOutcome } from "@tetral/ts-observability";
import { createJsonLogger, startupFailureLogRecord } from "../../src/logger.js";

describe("MCP Connector logger", () => {
  test("emits shared resource fields through the TS observability wrapper", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({
      write: (line) => lines.push(line),
      deploymentEnvironment: "test",
      serviceVersion: "unit",
    });

    logger.info({
      event: "mcp_connector_ready",
      "event.kind": "mcp_connector_ready",
      operation: "startup",
      component: "mcp-connector",
    });

    const record = JSON.parse(lines[0] ?? "{}") as Record<string, unknown>;
    expect(record).toMatchObject({
      level: "info",
      "service.name": "mcp-connector",
      "deployment.environment": "test",
      "service.version": "unit",
      event: "mcp_connector_ready",
      "event.kind": "mcp_connector_ready",
      operation: "startup",
      component: "mcp-connector",
    });
  });

  test("redacts sensitive caller fields and values through the shared wrapper", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({ write: (line) => lines.push(line) });

    logger.error({
      event: "mcp_call_failed",
      authorization: "Bearer ghp_livegithubtoken",
      refresh_token: "refresh-token-secret",
      apiKey: "standalone-api-key-secret",
      accessToken: "standalone-access-token-secret",
      detail: "connector URL included access_token=secret-value",
      camelDetail: "connector URL included refreshToken=secret-value",
      [semanticErrorOutcome]: true,
      "error.class": "mcp_connection_failed",
      "error.code": "mcp_connection_failed",
      "error.message_safe": "connector request failed",
    });

    const serialized = lines[0] ?? "";
    expect(serialized).not.toContain("ghp_livegithubtoken");
    expect(serialized).not.toContain("refresh-token-secret");
    expect(serialized).not.toContain("standalone-api-key-secret");
    expect(serialized).not.toContain("standalone-access-token-secret");
    expect(serialized).not.toContain("access_token=secret-value");
    const record = JSON.parse(serialized) as Record<string, unknown>;
    expect(record.authorization).toBe("[REDACTED]");
    expect(record.refresh_token).toBe("[REDACTED]");
    expect(record.apiKey).toBe("[REDACTED]");
    expect(record.accessToken).toBe("[REDACTED]");
    expect(record.detail).toBe("[REDACTED]");
    expect(record.camelDetail).toBe("[REDACTED]");
    expect(record["error.message_safe"]).toBe("connector request failed");
  });

  test("accepts complete semantic error records at info level", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({ write: (line) => lines.push(line) });

    logger.info({
      event: "mcp_call_failed",
      [semanticErrorOutcome]: true,
      "error.class": "runtime_error",
      "error.code": "mcp_connection_failed",
      "error.message_safe": "MCP connector call failed.",
    });

    expect(JSON.parse(lines[0] ?? "{}")).toMatchObject({
      level: "info",
      "error.class": "runtime_error",
      "error.code": "mcp_connection_failed",
      "error.message_safe": "MCP connector call failed.",
    });
  });

  test("rejects incomplete semantic error records at type-check time", () => {
    const logger = createJsonLogger({ write: () => undefined });
    if (false) {
      // @ts-expect-error Semantic error records require class, code, and safe message together.
      logger.info({ event: "mcp_call_failed", [semanticErrorOutcome]: true });
    }
    expect(logger).toBeDefined();
  });

  test("startup failure helper emits shared safe error fields", () => {
    expect(startupFailureLogRecord({ kind: "config_error", message: "invalid mcp config" })).toMatchObject({
      event: "startup_failed",
      "event.kind": "startup_failed",
      operation: "startup",
      component: "mcp-connector",
      kind: "config_error",
      message: "invalid mcp config",
      "error.class": "config_error",
      "error.code": "config_error",
      "error.message_safe": "invalid mcp config",
    });
  });
});
