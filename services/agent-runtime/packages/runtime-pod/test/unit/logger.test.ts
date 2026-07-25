import { describe, expect, test } from "bun:test";
import { createJsonLogger, runtimeCloseoutLogRecord, shutdownFailureLogRecord, startupFailureLogRecord } from "../../src/logger.js";

describe("Runtime Pod JSON logger", () => {
  test("emits service identity fields on structured records", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({
      write: (line) => lines.push(line),
      serviceName: "agent-runtime",
      deploymentEnvironment: "test",
      serviceVersion: "unit",
    });

    logger.info({
      event: "runtime_command_accepted",
      "grpc.method": "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptInput",
      "grpc.code": "OK",
      "caller.service_account": "engine/bridge",
      "duration.ms": 3,
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      "thread.id": "thrd_1",
      "runtime_input.id": "rin_1",
      "binding.id": "bind_1",
      "request.id": "req_1",
    });

    const record = JSON.parse(lines[0] ?? "{}") as Record<string, unknown>;
    expect(record).toMatchObject({
      level: "info",
      "service.name": "agent-runtime",
      "deployment.environment": "test",
      "service.version": "unit",
      "grpc.method": "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptInput",
      "grpc.code": "OK",
      "caller.service_account": "engine/bridge",
      "duration.ms": 3,
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      "thread.id": "thrd_1",
      "runtime_input.id": "rin_1",
      "binding.id": "bind_1",
      "request.id": "req_1",
    });
    expect(record).not.toHaveProperty("error.class");
  });

  test("startup and shutdown failures include shared safe error fields", () => {
    expect(startupFailureLogRecord({
      kind: "config_error",
      message: "invalid runtime pod identity",
    })).toMatchObject({
      event: "startup_failed",
      "event.kind": "startup_failed",
      operation: "startup",
      component: "agent-runtime",
      kind: "config_error",
      message: "invalid runtime pod identity",
      "error.class": "config_error",
      "error.code": "config_error",
      "error.message_safe": "invalid runtime pod identity",
    });

    expect(startupFailureLogRecord({
      kind: "startup_error",
      message: "raw dependency detail must not be used",
    })).toMatchObject({
      event: "startup_failed",
      "event.kind": "startup_failed",
      operation: "startup",
      component: "agent-runtime",
      kind: "startup_error",
      message: "runtime pod startup failed",
      "error.class": "startup_error",
      "error.code": "startup_error",
      "error.message_safe": "runtime pod startup failed",
    });

    expect(shutdownFailureLogRecord({
      event: "shutdown_drain_timeout",
      message: "runtime pod shutdown drain timed out",
    })).toMatchObject({
      event: "shutdown_drain_timeout",
      "event.kind": "shutdown_drain_timeout",
      operation: "shutdown",
      component: "agent-runtime",
      kind: "shutdown_error",
      "error.class": "shutdown_error",
      "error.code": "shutdown_drain_timeout",
      "error.message_safe": "runtime pod shutdown drain timed out",
    });
  });

  test("closeout records expose only bounded taxonomy and active counts", () => {
    expect(runtimeCloseoutLogRecord({
      event: "runtime_closeout_unrepairable",
      activeCloseouts: 1,
      errorCode: "ack_mismatch",
    })).toEqual(expect.objectContaining({
      event: "runtime_closeout_unrepairable",
      "event.kind": "runtime_closeout_unrepairable",
      operation: "runtime_closeout",
      component: "agent-runtime",
      message: "runtime_closeout_unrepairable",
      "closeout.active_count": 1,
      "closeout.error_code": "ack_mismatch",
      "error.class": "runtime_closeout",
      "error.code": "ack_mismatch",
      "error.message_safe": "runtime_closeout_unrepairable",
    }));
  });
});
