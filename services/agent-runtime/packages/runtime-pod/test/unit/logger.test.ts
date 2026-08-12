import { describe, expect, test } from "bun:test";
import { acceptedInputCommitLogRecord, createJsonLogger, logWorkloadStarted, providerRescheduleSelectedLogRecord, providerToolDeclarationRejectedLogRecord, recordRuntimeReceiptEvidence, runtimeCloseoutLogRecord, shutdownFailureLogRecord, startupFailureLogRecord, workloadStartedLogRecord } from "../../src/logger.js";

describe("Runtime Pod JSON logger", () => {
  test("accepted-input commit record contains only stable identity and bounded outcome", () => {
    const record = acceptedInputCommitLogRecord({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_1",
      requestId: "req_1",
      runtimeInputId: "rin_1",
      inputKind: "task_notification",
      attempt: 2,
      durationMs: 17,
      outcome: "retry",
      failureClass: "task_notification_payload_mismatch",
    });
    expect(record).toEqual({
      event: "runtime_accepted_input_commit",
      "event.kind": "runtime_accepted_input_commit",
      operation: "commit_accepted_input",
      component: "agent-runtime",
      message: "accepted input commit observed",
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      "thread.id": "thr_1",
      "request.id": "req_1",
      "runtime_input.id": "rin_1",
      "runtime_input.kind": "task_notification",
      "retry.attempt": 2,
      "duration.ms": 17,
      outcome: "retry",
      "failure.class": "task_notification_payload_mismatch",
    });
  });

  test("provider reschedule record pins accepted attempt and selected delay", () => {
    expect(providerRescheduleSelectedLogRecord({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_1",
      requestId: "req_1",
      modelRequestId: "mreq_1",
      attempt: 2,
      delayMs: 2_000,
      delaySource: "runtime_fallback",
      failureCode: "provider_unavailable",
    })).toEqual(expect.objectContaining({
      event: "provider_reschedule_selected",
      "request.id": "req_1",
      "model_request.id": "mreq_1",
      "retry.attempt": 2,
      "delay.ms": 2_000,
      "delay.source": "runtime_fallback",
      "provider.failure.code": "provider_unavailable",
      retryable: true,
    }));
  });

  test("provider tool declaration rejection contains only bounded identities and discriminators", () => {
    const record = providerToolDeclarationRejectedLogRecord({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_1",
      requestId: "req_1",
      modelRequestId: "mreq_1",
      declarationKind: "freeform",
      family: "claude",
      validationMember: "tool_family",
    });

    expect(record).toEqual(expect.objectContaining({
      event: "provider_tool_declaration_rejected",
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      "thread.id": "thr_1",
      "request.id": "req_1",
      "model_request.id": "mreq_1",
      "tool.declaration.kind": "freeform",
      "tool.family": "claude",
      "validation.member": "tool_family",
      "error.class": "provider_tool_declaration",
      "error.code": "invalid_tool_definition",
      "error.message_safe": "provider tool declaration rejected",
    }));
    expect(JSON.stringify(record)).not.toContain("lark");
    expect(JSON.stringify(record)).not.toContain("input_schema");
  });

  test("emits service identity fields on structured records", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({
      write: (line) => lines.push(line),
      serviceName: "agent-runtime",
      deploymentEnvironment: "test",
      serviceVersion: "unit",
      clock: () => new Date("2026-08-08T12:34:56.789Z"),
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
      time: "2026-08-08T12:34:56.789Z",
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

  test("logger owns the event time even when a caller supplies one", () => {
    const lines: string[] = [];
    const logger = createJsonLogger({
      write: (line) => lines.push(line),
      clock: () => new Date("2026-08-08T00:00:00.000Z"),
    });
    logger.info({ event: "clock_test", time: "2000-01-01T00:00:00.000Z" });
    expect(JSON.parse(lines[0] ?? "{}")).toMatchObject({ time: "2026-08-08T00:00:00.000Z" });
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
      "startup.cause_category": "configuration",
      "error.class": "config_error",
      "error.code": "config_error",
      "error.message_safe": "invalid runtime pod identity",
    });

    expect(startupFailureLogRecord({
      kind: "startup_error",
      message: "raw dependency detail must not be used",
      cause: new ReferenceError("raw dependency detail must not be used"),
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
      "startup.cause_class": "ReferenceError",
      "startup.cause_category": "unknown",
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

  test("started helper uses the shared workload lifecycle vocabulary", () => {
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

  test("startup cause class accepts only bounded constructor identifiers", () => {
    const revoked = Proxy.revocable({}, {});
    revoked.revoke();
    const longConstructorName = "A".repeat(65);
    const hostileValues: readonly [string, unknown][] = [
      ["undefined", undefined],
      ["null", null],
      ["primitive", "secret-token"],
      ["null prototype", Object.create(null) as unknown],
      ["revoked proxy", revoked.proxy],
      ["URL-shaped constructor", { constructor: { name: "postgres://user:pw@host/db" } }],
      ["overlong constructor", { constructor: { name: longConstructorName } }],
    ];
    for (const [name, cause] of hostileValues) {
      const record = startupFailureLogRecord({
        kind: "startup_error",
        message: "runtime pod startup failed",
        cause,
      });
      expect(record["startup.cause_class"], name).toBe("unknown");
    }

    const typeErrorRecord = startupFailureLogRecord({
      kind: "startup_error",
      message: "runtime pod startup failed",
      cause: new TypeError("raw request body"),
    });
    expect(typeErrorRecord["startup.cause_class"]).toBe("TypeError");
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

  test("receipt evidence emits identity-only applied and discarded records", () => {
    const lines: string[] = [];
    const outcomes: string[] = [];
    const logger = createJsonLogger({ write: (line) => lines.push(line) });
    const common = {
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thrd_1",
      operation: "write_event",
      sourceKind: "agent.message",
      operationId: "rwrite_1",
      declarationDigest: "digest_1",
      bindingId: "bind_1",
      bindingGeneration: 2,
    } as const;
    recordRuntimeReceiptEvidence(logger, {
      recordReceiptEvidence: (outcome) => outcomes.push(outcome),
      recordHotState: () => undefined,
      addActiveToolFibers: () => undefined,
      addPendingApprovals: () => undefined,
      observeProviderStreamDuration: () => undefined,
      observeEventWriteLatency: () => undefined,
      observeContextLoadLatency: () => undefined,
      recordCleanupCommandOutcome: () => undefined,
    }, {
      ...common,
      applicationDisposition: "current_custody",
      outcome: "applied",
    });
    recordRuntimeReceiptEvidence(logger, undefined, {
      ...common,
      applicationDisposition: "stale_custody",
      outcome: "stale_custody",
    });

    const records = lines.map((line) => JSON.parse(line) as Record<string, unknown>);
    expect(records[0]).toMatchObject({
      event: "runtime_receipt_applied",
      "event.kind": "runtime_receipt_applied",
      "operation.id": "rwrite_1",
      "declaration.digest": "digest_1",
      "receipt.application_disposition": "current_custody",
    });
    expect(records[1]).toMatchObject({
      event: "runtime_receipt_discarded",
      "receipt.discard_reason": "stale_custody",
    });
    expect(JSON.stringify(records)).not.toContain("prompt");
    expect(JSON.stringify(records)).not.toContain("declaration.source.id");
    expect(JSON.stringify(records)).not.toContain("session.thread.id");
    expect(outcomes).toEqual(["applied"]);
  });
});
