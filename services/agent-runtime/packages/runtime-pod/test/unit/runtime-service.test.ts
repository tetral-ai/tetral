import { describe, expect, test } from "bun:test";
import { Metadata, status } from "@grpc/grpc-js";
import {
  RuntimeCommandKind,
  RuntimeCommandStatus,
  RuntimeInputErrorCode,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import { runtimeInputErrorCodeOrGeneric } from "@tetral/agent-runtime-protocol/src/error-codes.js";
import type {
  RuntimeInputCommandRequest,
  RuntimeInputCommandResponse,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import { GrpcStatusError, RuntimeControlService } from "../../src/runtime-service.js";
import { RuntimePodMetricsRegistry } from "../../src/metrics.js";
import type {
  RuntimeAuthenticator,
  RuntimeControlInputCommitter,
  RuntimeCleanupController,
  RuntimeCommandScope,
  RuntimeCommandRunner,
  RuntimeSessionRunHost,
  RuntimeTaskNotificationCommitter,
} from "../../src/runtime-service.js";
import type { RuntimePodLogRecord } from "../../src/logger.js";
import {
  buildRuntimeServiceBridgeRuntimeMessage as bridgeRuntimeMessage,
} from "../../../core/test/unit/runtime-message-builders.js";

describe("RuntimeControlService command envelope", () => {
  test("authorizes before validation, lifecycle command runner, logs, or core effects", async () => {
    let commandRunnerCalls = 0;
    const fixture = runtimeFixture({
      auth: "deny",
      commandRunner: {
        runCommand: async () => {
          commandRunnerCalls += 1;
          throw new GrpcStatusError(status.FAILED_PRECONDITION, "runtime pod not ready");
        },
      },
    });

    await expectGrpcCode(fixture.service.acceptInput({ ...validCommand(), sessionId: "" }, authMetadata()), status.PERMISSION_DENIED);

    expect(commandRunnerCalls).toBe(0);
    expect(fixture.runHost.sessionIds).toEqual([]);
    expect(fixture.cleanupController.requests).toEqual([]);
    expect(fixture.logger.records).toEqual([]);
  });

  test("accepts message input, logs safe command fields, and wakes the run host once", async () => {
    const fixture = runtimeFixture();

    const response = await fixture.service.acceptInput(validCommand(), authMetadata("caller-token-secret"));

    expect(response).toEqual(acceptedResponse());
    expect(fixture.runHost.sessionIds).toEqual(["sesn_1"]);
    expect(fixture.cleanupController.requests).toEqual([]);
    expect(fixture.logger.records.at(-1)).toMatchObject({
      event: "runtime_command_accepted",
      "event.kind": "runtime_command_accepted",
      operation: "AcceptMessages",
      component: "runtime-control-service",
      "grpc.method": "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptInput",
      "grpc.code": "OK",
      "caller.service_account": "engine/bridge",
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      "thread.id": "thrd_1",
      "runtime_input.id": "rin_1",
      "binding.id": "bind_1",
      "request.id": "req_1",
    });
    for (const forbidden of ["error.class", "error.code", "error.message_safe", "retryable", "terminal"]) {
      expect(fixture.logger.records.at(-1)).not.toHaveProperty(forbidden);
    }
    const serializedLog = JSON.stringify(fixture.logger.records.at(-1));
    for (const forbidden of ["caller-token-secret", "bearer", "uid-a", "10.0.0.1"]) {
      expect(serializedLog).not.toContain(forbidden);
    }
  });

  test("accepts an agent-mail bare poke through its dedicated rescan effect", async () => {
    const fixture = runtimeFixture();
    const request = validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_AGENT_MAIL,
      runtimeInputId: "agent_mail:delivery_1",
      eventIds: [],
      sequenceFrom: 0,
      sequenceTo: 0,
      payloadJson: "{}",
    });

    const response = await fixture.service.acceptAgentMail(request, authMetadata());

    expect(response).toEqual(acceptedResponse(request));
    expect(fixture.runHost.agentMailCommands).toEqual([expect.objectContaining({
      sessionId: "sesn_1",
      sessionThreadId: "thrd_1",
      runtimeInputId: "agent_mail:delivery_1",
    })]);
    expect(fixture.logger.records.at(-1)).toMatchObject({
      operation: "AcceptAgentMail",
      "grpc.method": "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptAgentMail",
    });
  });

  test("returns a retryable in-band rejection when agent-mail context loading is transiently unavailable", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        agentMailResult: { ok: false, sessionId: "sesn_1", reason: "context_load_failed" },
      }),
    });
    const request = validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_AGENT_MAIL,
      runtimeInputId: "agent_mail:delivery_retry",
      eventIds: [],
      sequenceFrom: 0,
      sequenceTo: 0,
      payloadJson: "{}",
    });

    const response = await fixture.service.acceptAgentMail(request, authMetadata());

    expect(response.status).toBe(RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED);
    expect(response.retryable).toBe(true);
    expect(response.errorCode).toBe(runtimeInputErrorCodeOrGeneric("context_load_failed"));
  });

  test("deduplicates the same runtime_input_id across RPC attempt request_id changes", async () => {
    const fixture = runtimeFixture();

    const first = await fixture.service.acceptInput(validCommand(), authMetadata());
    const duplicate = await fixture.service.acceptInput({ ...validCommand(), requestId: "req_2" }, authMetadata());

    expect(first.status).toBe(RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED);
    expect(duplicate).toEqual({ ...acceptedResponse(), status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_DUPLICATE });
    expect(fixture.runHost.sessionIds).toEqual(["sesn_1"]);
  });

  test("deduplicates a config patch rebuilt under a fresh active binding by input id and payload", async () => {
    const fixture = runtimeFixture();
    const payloadJson = JSON.stringify({ config_generation: 7 });
    const first = validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
      runtimeInputId: "rin_config_rebuilt",
      payloadJson,
    });
    await expect(fixture.service.applyRuntimeConfig(first, authMetadata())).resolves.toMatchObject({
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED,
    });
    await expect(fixture.service.cleanupSession({
      ...validCommand({
        commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
        runtimeInputId: "rin_cleanup_binding_one",
        payloadJson: JSON.stringify({ reason: "expired" }),
      }),
      eventIds: [],
    }, authMetadata())).resolves.toMatchObject({
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED,
    });
    await expect(fixture.service.applyRuntimeConfig(validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
      runtimeInputId: "rin_config_binding_two",
      bindingId: "bind_2",
      bindingGeneration: 2,
      payloadJson: JSON.stringify({ config_generation: 8 }),
    }), authMetadata())).resolves.toMatchObject({
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED,
    });

    const duplicate = await fixture.service.applyRuntimeConfig({
      ...first,
      bindingId: "bind_2",
      bindingGeneration: 2,
      requestId: "req_rebuilt_retry",
    }, authMetadata());
    expect(duplicate).toMatchObject({
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_DUPLICATE,
      runtimeInputId: "rin_config_rebuilt",
    });
    expect(fixture.runHost.runtimeConfigPatches).toHaveLength(2);

    const staleBinding = await fixture.service.applyRuntimeConfig({
      ...first,
      requestId: "req_stale_binding_retry",
    }, authMetadata());
    expect(staleBinding).toMatchObject({
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED,
      retryable: true,
      errorCode: runtimeInputErrorCodeOrGeneric("binding_identity_mismatch"),
    });
    expect(fixture.runHost.runtimeConfigPatches).toHaveLength(2);
  });

  test("scopes runtime_input_id idempotency by workspace", async () => {
    const fixture = runtimeFixture();

    const first = await fixture.service.acceptInput(validCommand(), authMetadata());
    const second = await fixture.service.acceptInput(validCommand({
      workspaceId: "wksp_2",
      sessionId: "sesn_2",
      sessionThreadId: "thrd_2",
      runtimeInputId: "rin_1",
      requestId: "req_2",
    }), authMetadata());

    expect(first.status).toBe(RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED);
    expect(second.status).toBe(RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED);
    expect(fixture.runHost.sessionIds).toEqual(["sesn_1", "sesn_2"]);
  });

  test("rejects conflicting runtime_input_id identity without a second core effect", async () => {
    const fixture = runtimeFixture();

    await fixture.service.acceptInput(validCommand(), authMetadata());
    const conflict = await fixture.service.acceptInput({ ...validCommand(), bindingId: "bind_other" }, authMetadata());

    expect(conflict).toEqual({
      ...acceptedResponse({ bindingId: "bind_other" }),
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED,
      retryable: false,
      errorCode: runtimeInputErrorCodeOrGeneric("runtime_input_identity_conflict"),
    });
    expect(fixture.runHost.sessionIds).toEqual(["sesn_1"]);
  });

  test("rejects fresh commands for a hot session when binding generation mismatches", async () => {
    const fixture = runtimeFixture();

    await fixture.service.acceptInput(validCommand(), authMetadata());
    const rejected = await fixture.service.acceptInput(validCommand({
      requestId: "req_2",
      runtimeInputId: "rin_2",
      bindingGeneration: 43,
    }), authMetadata());

    expect(rejected).toEqual({
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED,
      retryable: true,
      errorCode: runtimeInputErrorCodeOrGeneric("binding_identity_mismatch"),
      sessionId: "sesn_1",
      runtimeInputId: "rin_2",
      bindingId: "bind_1",
      bindingGeneration: 43,
    });
    expect(fixture.runHost.sessionIds).toEqual(["sesn_1"]);
  });

  test("scopes active binding fences by workspace and session", async () => {
    const fixture = runtimeFixture();

    const first = await fixture.service.acceptInput(validCommand({
      sessionId: "sesn_shared",
      sessionThreadId: "thrd_a",
      runtimeInputId: "rin_workspace_a",
    }), authMetadata());
    const second = await fixture.service.acceptInput(validCommand({
      workspaceId: "wksp_2",
      sessionId: "sesn_shared",
      sessionThreadId: "thrd_b",
      requestId: "req_workspace_b",
      runtimeInputId: "rin_workspace_b",
      eventIds: ["sevt_workspace_b"],
      bindingId: "bind_workspace_b",
      bindingGeneration: 77,
      targetPodUid: "uid-a",
    }), authMetadata());

    expect(first.status).toBe(RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED);
    expect(second.status).toBe(RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED);
    expect(fixture.runHost.sessionIds).toEqual(["sesn_shared", "sesn_shared"]);
  });

  test("returns retryable rejected for selected pod identity mismatch before effects", async () => {
    for (const request of [
      { ...validCommand(), targetPodNamespace: "other" },
      { ...validCommand(), targetPodName: "other" },
      { ...validCommand(), targetPodUid: "other" },
      { ...validCommand(), targetPodIp: "10.0.0.2" },
    ]) {
      const fixture = runtimeFixture();
      const response = await fixture.service.acceptInput(request, authMetadata());

      expect(response.status).toBe(RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED);
      expect(response.retryable).toBe(true);
      expect(response.errorCode).toBe(runtimeInputErrorCodeOrGeneric("selected_pod_identity_mismatch"));
      expect(fixture.runHost.sessionIds).toEqual([]);
      expect(fixture.cleanupController.requests).toEqual([]);
      expect(fixture.logger.records).toEqual([]);
    }
  });

  test("rejects invalid envelopes and command-kind mismatches with INVALID_ARGUMENT", async () => {
    const fixture = runtimeFixture();

    await expectGrpcCode(fixture.service.acceptInput({ ...validCommand(), sessionId: "" }, authMetadata()), status.INVALID_ARGUMENT);
    await expectGrpcCode(
      fixture.service.acceptInput(
        { ...validCommand(), commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL },
        authMetadata(),
      ),
      status.INVALID_ARGUMENT,
    );

    expect(fixture.runHost.sessionIds).toEqual([]);
    expect(fixture.cleanupController.requests).toEqual([]);
    expect(fixture.logger.records).toEqual([]);
  });

  test("accepts cleanup command and routes payload reason to cleanup controller", async () => {
    const metrics = new RuntimePodMetricsRegistry();
    const fixture = runtimeFixture({ metrics });
    const request = {
      ...validCommand({
        runtimeInputId: "rin_cleanup",
        commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
        payloadJson: JSON.stringify({ reason: "expired" }),
      }),
      eventIds: [],
    };

    const response = await fixture.service.cleanupSession(request, authMetadata());

    expect(response).toEqual(acceptedResponse({ runtimeInputId: "rin_cleanup" }));
    expect(fixture.runHost.sessionIds).toEqual([]);
    expect(fixture.cleanupController.requests).toEqual([
      { scope: expect.objectContaining({ sessionId: "sesn_1", sessionThreadId: "thrd_1" }), reason: "expired" },
    ]);
    expect(metrics.snapshot().cleanupCommandOutcomes.get("accepted")).toBe(1);
    expect(metrics.snapshot().cleanupCommandOutcomes.get("completed")).toBe(1);
  });

  test("accepts non-message control commands and records durable settlement inputs", async () => {
    const events: string[] = [];
    const fixture = runtimeFixture({ events });

    const commands: Array<{
      readonly call: (request: RuntimeInputCommandRequest) => Promise<RuntimeInputCommandResponse>;
      readonly kind: RuntimeCommandKind;
      readonly runtimeInputId: string;
      readonly payloadJson: string;
    }> = [
      {
        call: (request) => fixture.service.interrupt(request, authMetadata()),
        kind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
        runtimeInputId: "rin_interrupt",
        payloadJson: "",
      },
      {
        call: (request) => fixture.service.resolveToolConfirmation(request, authMetadata()),
        kind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION,
        runtimeInputId: "rin_confirm",
        payloadJson: JSON.stringify({ source_event_id: "sevt_confirm_1", tool_use_event_id: "sevt_tool_1", decision: "allow" }),
      },
      {
        call: (request) => fixture.service.acceptTaskNotification(request, authMetadata()),
        kind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
        runtimeInputId: "rin_task",
        payloadJson: canonicalTaskNotificationPayloadJson({ taskId: "task_1", sourceToolUseEventId: "sevt_tool_1", status: "completed" }),
      },
      {
        call: (request) => fixture.service.acceptTaskNotification(request, authMetadata()),
        kind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
        runtimeInputId: "rin_task_expired",
        payloadJson: canonicalTaskNotificationPayloadJson({ taskId: "task_expired", sourceToolUseEventId: "sevt_tool_expired", status: "expired" }),
      },
      {
        call: (request) => fixture.service.applyRuntimeConfig(request, authMetadata()),
        kind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
        runtimeInputId: "rin_config",
        payloadJson: JSON.stringify({ config_generation: 3 }),
      },
      {
        call: (request) => fixture.service.applyRuntimeConfig(request, authMetadata()),
        kind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
        runtimeInputId: "rin_mcp_manifest",
        payloadJson: JSON.stringify({
          mcp_manifest: {
            mcp_server_name: "github",
            manifest_etag: "etag_1",
            manifest_generation: 1,
            tools: [{ name: "github_search", description: "Search GitHub", input_schema: { type: "object" } }],
          },
        }),
      },
    ];

    for (const command of commands) {
      const response = await command.call(
        validCommand({
          commandKind: command.kind,
          runtimeInputId: command.runtimeInputId,
          payloadJson: command.payloadJson,
        }),
      );
      expect(response).toEqual(acceptedResponse({ runtimeInputId: command.runtimeInputId }));
    }

    expect(fixture.runHost.sessionIds).toEqual([]);
    expect(fixture.runHost.interrupts).toEqual([
      { sessionId: "sesn_1", command: commandScope({ runtimeInputId: "rin_interrupt" }) },
    ]);
    expect(fixture.runHost.toolConfirmations).toEqual([
      {
        sessionId: "sesn_1",
        command: {
          ...commandScope({ runtimeInputId: "rin_confirm" }),
          sourceEventId: "sevt_confirm_1",
          toolUseEventId: "sevt_tool_1",
          decision: "allow",
        },
      },
    ]);
    expect(fixture.runHost.taskNotifications).toEqual([
      {
        sessionId: "sesn_1",
        command: {
          ...commandScope({ runtimeInputId: "rin_task" }),
          taskId: "task_1",
          sourceToolUseEventId: "sevt_tool_1",
          status: "completed",
          payloadJson: canonicalTaskNotificationPayloadJson({
            taskId: "task_1",
            sourceToolUseEventId: "sevt_tool_1",
            status: "completed",
          }),
          bridgeProjection: bridgeRuntimeMessage(),
        },
      },
      {
        sessionId: "sesn_1",
        command: {
          ...commandScope({ runtimeInputId: "rin_task_expired" }),
          taskId: "task_expired",
          sourceToolUseEventId: "sevt_tool_expired",
          status: "expired",
          payloadJson: canonicalTaskNotificationPayloadJson({
            taskId: "task_expired",
            sourceToolUseEventId: "sevt_tool_expired",
            status: "expired",
          }),
          bridgeProjection: bridgeRuntimeMessage(),
        },
      },
    ]);
    expect(fixture.taskNotificationCommitter.commits.map((commit) => commit.command)).toEqual([
      {
        runtimeInputId: "rin_task",
        taskId: "task_1",
        sourceToolUseEventId: "sevt_tool_1",
        status: "completed",
        payloadJson: canonicalTaskNotificationPayloadJson({
          taskId: "task_1",
          sourceToolUseEventId: "sevt_tool_1",
          status: "completed",
        }),
      },
      {
        runtimeInputId: "rin_task_expired",
        taskId: "task_expired",
        sourceToolUseEventId: "sevt_tool_expired",
        status: "expired",
        payloadJson: canonicalTaskNotificationPayloadJson({
          taskId: "task_expired",
          sourceToolUseEventId: "sevt_tool_expired",
          status: "expired",
        }),
      },
    ]);
    expect(fixture.controlInputCommitter.commits).toEqual([
      {
        scope: commandScope({ runtimeInputId: "rin_interrupt" }),
        inputKind: "interrupt_control",
      },
      {
        scope: commandScope({ runtimeInputId: "rin_confirm" }),
        inputKind: "tool_confirmation",
      },
    ]);
    expect(events).toContain("runHost.interrupt:start:rin_interrupt");
    expect(events.indexOf("runHost.interrupt:start:rin_interrupt")).toBeLessThan(events.indexOf("commit.interrupt_control:rin_interrupt"));
    expect(events.indexOf("commit.interrupt_control:rin_interrupt")).toBeLessThan(events.indexOf("runHost.interrupt:end:rin_interrupt"));
    expect(fixture.runHost.runtimeConfigPatches).toEqual([
      { sessionId: "sesn_1", command: { ...commandScope({ runtimeInputId: "rin_config" }), generation: 3, payloadJson: JSON.stringify({ config_generation: 3 }) } },
      {
        sessionId: "sesn_1",
        command: {
          ...commandScope({ runtimeInputId: "rin_mcp_manifest" }),
          mcpServerName: "github",
          manifestETag: "etag_1",
      manifestReadiness: "ready",
          generation: 1,
          payloadJson: JSON.stringify({
            mcp_manifest: {
              mcp_server_name: "github",
              manifest_etag: "etag_1",
              manifest_generation: 1,
              tools: [{ name: "github_search", description: "Search GitHub", input_schema: { type: "object" } }],
            },
          }),
        },
      },
    ]);
    expect(fixture.cleanupController.requests).toEqual([]);
    expect(fixture.logger.records).toHaveLength(commands.length);
  });

  test("active interrupt closeout owns the durable processed marker and retries when its ACK fails", async () => {
    const events: string[] = [];
    const fixture = runtimeFixture({
      events,
      controlInputCommitter: new RecordingControlInputCommitter([
        {
          ok: false,
          retryable: true,
          errorCode: "bridge_commit_unavailable",
          message: "control input durable commit failed",
        },
        { ok: true },
      ], events),
    });
    const command = validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
      runtimeInputId: "rin_interrupt_commit_fail",
    });

    await expect(fixture.service.interrupt(command, authMetadata())).resolves.toEqual(
      rejectedResponse({
        runtimeInputId: "rin_interrupt_commit_fail",
        retryable: true,
        errorCode: "bridge_commit_unavailable",
      }),
    );
    await expect(fixture.service.interrupt({ ...command, requestId: "req_retry" }, authMetadata())).resolves.toEqual(
      acceptedResponse({ runtimeInputId: "rin_interrupt_commit_fail" }),
    );

    expect(fixture.runHost.interrupts).toEqual([
      { sessionId: "sesn_1", command: commandScope({ runtimeInputId: "rin_interrupt_commit_fail" }) },
      {
        sessionId: "sesn_1",
        command: { ...commandScope({ runtimeInputId: "rin_interrupt_commit_fail" }), requestId: "req_retry" },
      },
    ]);
    expect(fixture.controlInputCommitter.commits).toEqual([
      {
        scope: commandScope({ runtimeInputId: "rin_interrupt_commit_fail" }),
        inputKind: "interrupt_control",
      },
      {
        scope: { ...commandScope({ runtimeInputId: "rin_interrupt_commit_fail" }), requestId: "req_retry" },
        inputKind: "interrupt_control",
      },
    ]);
    expect(events).toEqual([
      "runHost.interrupt:start:rin_interrupt_commit_fail",
      "commit.interrupt_control:rin_interrupt_commit_fail",
      "runHost.interrupt:end:rin_interrupt_commit_fail",
      "runHost.interrupt:start:rin_interrupt_commit_fail",
      "commit.interrupt_control:rin_interrupt_commit_fail",
      "runHost.interrupt:end:rin_interrupt_commit_fail",
    ]);
  });

  test("idle interrupt commits its processed marker after the hot-state no-op", async () => {
    const events: string[] = [];
    const fixture = runtimeFixture({
      events,
      runHost: new RecordingRunHost({ interruptResult: {
        ok: true,
        sessionId: "sesn_1",
        created: false,
        interrupted: false,
        idleInterrupt: true,
      } }, events),
    });
    const command = validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
      runtimeInputId: "rin_idle_interrupt",
    });

    await expect(fixture.service.interrupt(command, authMetadata())).resolves.toEqual(
      acceptedResponse({ runtimeInputId: "rin_idle_interrupt" }),
    );
    expect(events).toEqual([
      "runHost.interrupt:start:rin_idle_interrupt",
      "runHost.interrupt:end:rin_idle_interrupt",
      "commit.interrupt_control:rin_idle_interrupt",
    ]);
  });

  test("fails closed before hot-state mutation when tool confirmation commit reports a stale non-retryable rejection", async () => {
    const fixture = runtimeFixture({
      controlInputCommitter: new RecordingControlInputCommitter({
        ok: false,
        retryable: false,
        errorCode: "bridge_commit_rejected",
        message: "control input durable commit rejected",
      }),
    });
    const command = validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION,
      runtimeInputId: "rin_confirm_stale",
      payloadJson: JSON.stringify({ source_event_id: "sevt_confirm_1", tool_use_event_id: "sevt_tool_1", decision: "allow" }),
    });

    await expect(
      fixture.service.resolveToolConfirmation(
        command,
        authMetadata(),
      ),
    ).resolves.toEqual(
      rejectedResponse({
        runtimeInputId: "rin_confirm_stale",
        retryable: false,
        errorCode: "bridge_commit_rejected",
      }),
    );
    await expect(fixture.service.resolveToolConfirmation({ ...command, requestId: "req_2" }, authMetadata())).resolves.toEqual(
      rejectedResponse({
        runtimeInputId: "rin_confirm_stale",
        retryable: false,
        errorCode: "bridge_commit_rejected",
      }),
    );

    expect(fixture.runHost.toolConfirmations).toHaveLength(0);
    expect(fixture.controlInputCommitter.commits).toEqual([
      {
        scope: commandScope({ runtimeInputId: "rin_confirm_stale" }),
        inputKind: "tool_confirmation",
      },
    ]);
  });

  test("returns in-band terminal rejection for stale tool confirmations after durable ACK", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        toolConfirmationResult: {
          ok: false,
          sessionId: "sesn_1",
          reason: "control_conflict",
        },
      }),
    });

    await expect(
      fixture.service.resolveToolConfirmation(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION,
          runtimeInputId: "rin_confirm_conflict",
          payloadJson: JSON.stringify({ source_event_id: "sevt_confirm_1", tool_use_event_id: "sevt_tool_1", decision: "deny" }),
        }),
        authMetadata(),
      ),
    ).resolves.toEqual(
      rejectedResponse({
        runtimeInputId: "rin_confirm_conflict",
        retryable: false,
        errorCode: "runtime_control_conflict",
      }),
    );

    expect(fixture.runHost.toolConfirmations).toHaveLength(1);
    expect(fixture.controlInputCommitter.commits).toHaveLength(1);
  });

  test("does not update hot task-notification state when durable Bridge ACK fails", async () => {
    const fixture = runtimeFixture({
      taskNotificationCommitter: new RecordingTaskNotificationCommitter({
        ok: false,
        retryable: true,
        errorCode: "bridge_commit_unavailable",
        message: "bridge unavailable",
      }),
    });

    await expect(
      fixture.service.acceptTaskNotification(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
          runtimeInputId: "rin_task",
          payloadJson: canonicalTaskNotificationPayloadJson({ taskId: "task_1", sourceToolUseEventId: "sevt_tool_1", status: "completed" }),
        }),
        authMetadata(),
      ),
    ).resolves.toEqual(
      rejectedResponse({
        runtimeInputId: "rin_task",
        retryable: true,
        errorCode: "bridge_commit_unavailable",
      }),
    );

    expect(fixture.taskNotificationCommitter.commits).toHaveLength(1);
    expect(fixture.runHost.taskNotifications).toEqual([]);
    expect(fixture.logger.records.at(-1)).toMatchObject({
      event: "runtime_command_rejected",
      retryable: true,
      terminal: false,
      "error.class": "runtime_command_rejected",
      "error.code": "bridge_commit_unavailable",
      "error.message_safe": "runtime command rejected",
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      "thread.id": "thrd_1",
    });
  });

  test("returns in-band rejection when task-notification durable Bridge ACK fails terminally", async () => {
    const fixture = runtimeFixture({
      taskNotificationCommitter: new RecordingTaskNotificationCommitter({
        ok: false,
        retryable: false,
        errorCode: "bridge_commit_rejected",
        message: "bridge rejected task notification",
      }),
    });

    await expect(
      fixture.service.acceptTaskNotification(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
          runtimeInputId: "rin_task_terminal",
          payloadJson: canonicalTaskNotificationPayloadJson({ taskId: "task_1", sourceToolUseEventId: "sevt_tool_1", status: "completed" }),
        }),
        authMetadata(),
      ),
    ).resolves.toEqual(
      rejectedResponse({
        runtimeInputId: "rin_task_terminal",
        retryable: false,
        errorCode: "bridge_commit_rejected",
      }),
    );

    expect(fixture.taskNotificationCommitter.commits).toHaveLength(1);
    expect(fixture.runHost.taskNotifications).toEqual([]);
    expect(fixture.logger.records.at(-1)).toMatchObject({
      event: "runtime_command_rejected",
      retryable: false,
      terminal: true,
      "error.class": "runtime_command_rejected",
      "error.code": "bridge_commit_rejected",
      "error.message_safe": "runtime command rejected",
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      "thread.id": "thrd_1",
    });
  });

  test("returns in-band terminal rejection when task-notification hot apply is stale", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        taskNotificationResult: {
          ok: false,
          sessionId: "sesn_1",
          reason: "control_conflict",
        },
      }),
    });

    await expect(
      fixture.service.acceptTaskNotification(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
          runtimeInputId: "rin_task_conflict",
          payloadJson: canonicalTaskNotificationPayloadJson({ taskId: "task_1", sourceToolUseEventId: "sevt_tool_1", status: "completed" }),
        }),
        authMetadata(),
      ),
    ).resolves.toEqual(
      rejectedResponse({
        runtimeInputId: "rin_task_conflict",
        retryable: false,
        errorCode: "runtime_control_conflict",
      }),
    );

    expect(fixture.taskNotificationCommitter.commits).toHaveLength(1);
    expect(fixture.runHost.taskNotifications).toHaveLength(1);
  });

  test("returns in-band terminal rejection when runtime-config hot apply cannot load context", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        runtimeConfigPatchResult: {
          ok: false,
          sessionId: "sesn_1",
          reason: "context_load_failed",
        },
      }),
    });

    await expect(
      fixture.service.applyRuntimeConfig(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
          runtimeInputId: "rin_config_context_failed",
          payloadJson: JSON.stringify({ config_generation: 7 }),
        }),
        authMetadata(),
      ),
    ).resolves.toEqual(
      rejectedResponse({
        runtimeInputId: "rin_config_context_failed",
        retryable: false,
        errorCode: "runtime_context_load_failed",
      }),
    );

    expect(fixture.runHost.runtimeConfigPatches).toHaveLength(1);
  });

  test("rejects malformed MCP manifest runtime config patch before hot-state mutation", async () => {
    const fixture = runtimeFixture();

    await expectGrpcCode(
      fixture.service.applyRuntimeConfig(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
          runtimeInputId: "rin_mcp_manifest_bad",
          payloadJson: JSON.stringify({
            mcp_manifest: {
              mcp_server_name: "github",
              manifest_etag: "etag_bad",
              manifest_generation: 2,
              tools: [{ name: "github_search", description: "Search GitHub" }],
            },
          }),
        }),
        authMetadata(),
      ),
      status.INVALID_ARGUMENT,
    );

    expect(fixture.runHost.runtimeConfigPatches).toEqual([]);
  });

  test("accepts unready MCP patch without tools or model-visible diagnostic", async () => {
    const fixture = runtimeFixture();
    const payloadJson = JSON.stringify({
      mcp_manifest: {
        mcp_server_name: "github",
        manifest_generation: 4,
        readiness: "unready",
        diagnostic: "delivery_exhausted",
      },
    });
    await expect(fixture.service.applyRuntimeConfig(validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
      runtimeInputId: "rin_mcp_unready",
      payloadJson,
    }), authMetadata())).resolves.toMatchObject({ runtimeInputId: "rin_mcp_unready" });
    expect(fixture.runHost.runtimeConfigPatches).toEqual([{
      sessionId: "sesn_1",
      command: {
        ...commandScope({ runtimeInputId: "rin_mcp_unready" }),
        generation: 4,
        mcpServerName: "github",
        manifestReadiness: "unready",
        manifestDiagnostic: "delivery_exhausted",
        payloadJson,
      },
    }]);
  });

  test("rejects string runtime config generation before hot-state mutation", async () => {
    const fixture = runtimeFixture();

    await expectGrpcCode(
      fixture.service.applyRuntimeConfig(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
          runtimeInputId: "rin_config_string",
          payloadJson: JSON.stringify({ config_generation: "7" }),
        }),
        authMetadata(),
      ),
      status.INVALID_ARGUMENT,
    );

    expect(fixture.runHost.runtimeConfigPatches).toEqual([]);
  });

  test("accepts stale task notification ACKs without hot-state mutation", async () => {
    const fixture = runtimeFixture({
      taskNotificationCommitter: new RecordingTaskNotificationCommitter({ ok: true, stale: true }),
    });

    const response = await fixture.service.acceptTaskNotification(
      validCommand({
        commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
        runtimeInputId: "rin_task",
        payloadJson: canonicalTaskNotificationPayloadJson({ taskId: "task_1", sourceToolUseEventId: "sevt_tool_1", status: "completed" }),
      }),
      authMetadata(),
    );

    expect(response).toEqual(acceptedResponse({ runtimeInputId: "rin_task" }));
    expect(fixture.taskNotificationCommitter.commits).toHaveLength(1);
    expect(fixture.runHost.taskNotifications).toEqual([]);
  });

  test("rejects task notifications that omit either required stream snapshot", async () => {
    const fixture = runtimeFixture();

    for (const payloadJson of [
      JSON.stringify({
        task_id: "task_1",
        source_tool_use_event_id: "sevt_tool_1",
        status: "completed",
        stderr: { text: "", truncated: false },
      }),
      JSON.stringify({
        task_id: "task_1",
        source_tool_use_event_id: "sevt_tool_1",
        status: "completed",
        stdout: { text: "", truncated: false },
      }),
    ]) {
      await expectGrpcCode(
        fixture.service.acceptTaskNotification(
          validCommand({
            commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
            runtimeInputId: "rin_task_missing_stream",
            payloadJson,
          }),
          authMetadata(),
        ),
        status.INVALID_ARGUMENT,
      );
    }

    expect(fixture.taskNotificationCommitter.commits).toEqual([]);
    expect(fixture.runHost.taskNotifications).toEqual([]);
  });

  test("canonicalizes task notification payload before Bridge commit and applies Bridge runtime projection to hot state", async () => {
    const runtimeMessage = bridgeRuntimeMessage({ text: "bridge-projected runtime notification" });
    const fixture = runtimeFixture({
      taskNotificationCommitter: new RecordingTaskNotificationCommitter({ ok: true, runtimeMessage }),
    });
    const payloadJson = JSON.stringify({
      task_id: "task_1",
      source_tool_use_event_id: "sevt_tool_1",
      status: "completed",
      exit_code: 0,
      stdout: {
        text: "done",
        truncated: false,
        original_bytes: 4,
        provider_metadata: { raw: "secret" },
      },
      stderr: {
        text: "",
        truncated: false,
        original_lines: null,
        provider_command_id: "cmd_provider",
      },
      output_paths: {
        stdout: "/tmp/tetral-runtime/tasks/task_1/stdout.log",
        stderr: "/tmp/tetral-runtime/tasks/task_1/stderr.log",
        status: "/tmp/tetral-runtime/tasks/task_1/exit.json",
        provider_session_id: "sess_provider",
      },
      provider_metadata_json: "{\"raw\":\"secret\"}",
    });

    await fixture.service.acceptTaskNotification(
      validCommand({
        commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
        runtimeInputId: "rin_task",
        payloadJson,
      }),
      authMetadata(),
    );

    const expected = {
      task_id: "task_1",
      source_tool_use_event_id: "sevt_tool_1",
      status: "completed",
      exit_code: 0,
      stdout: { text: "done", truncated: false, original_bytes: 4 },
      stderr: { text: "", truncated: false, original_lines: null },
    };
    const committed = fixture.taskNotificationCommitter.commits[0]?.command.payloadJson;
    const applied = fixture.runHost.taskNotifications[0]?.command.payloadJson;
    expect(JSON.parse(committed ?? "{}")).toEqual(expected);
    expect(JSON.parse(applied ?? "{}")).toEqual(expected);
    expect(fixture.runHost.taskNotifications[0]?.command.bridgeProjection).toEqual(runtimeMessage);
    expect(fixture.runHost.taskNotifications[0]?.command.bridgeProjection.parts[0]).toMatchObject({
      type: "text",
      text: "bridge-projected runtime notification",
    });
    expect(JSON.stringify(fixture.taskNotificationCommitter.commits)).not.toContain("provider_");
    expect(JSON.stringify(fixture.runHost.taskNotifications)).not.toContain("provider_");
  });

  test("maps local capacity and cleanup busy outcomes to retryable transport failures", async () => {
    const runHost = new RecordingRunHost({ ok: false, sessionId: "sesn_1", reason: "local_session_capacity_exceeded" });
    const cleanupController = new RecordingCleanupController({ ok: false, sessionId: "sesn_1", reason: "session_busy" });
    const metrics = new RuntimePodMetricsRegistry();
    const fixture = runtimeFixture({ runHost, cleanupController, metrics });

    await expectGrpcCode(fixture.service.acceptInput(validCommand(), authMetadata()), status.RESOURCE_EXHAUSTED);
    await expectGrpcCode(
      fixture.service.cleanupSession(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
          runtimeInputId: "rin_cleanup",
        }),
        authMetadata(),
      ),
      status.FAILED_PRECONDITION,
    );
    expect(metrics.snapshot().cleanupCommandOutcomes.get("rejected")).toBe(1);
  });
});

function runtimeFixture(options: {
  readonly auth?: "allow" | "deny";
  readonly runHost?: RecordingRunHost;
  readonly cleanupController?: RecordingCleanupController;
  readonly controlInputCommitter?: RecordingControlInputCommitter;
  readonly taskNotificationCommitter?: RecordingTaskNotificationCommitter;
  readonly commandRunner?: RuntimeCommandRunner;
  readonly metrics?: RuntimePodMetricsRegistry;
  readonly events?: string[] | undefined;
} = {}) {
  const runHost = options.runHost ?? new RecordingRunHost(undefined, options.events);
  const cleanupController = options.cleanupController ?? new RecordingCleanupController();
  const controlInputCommitter = options.controlInputCommitter ?? new RecordingControlInputCommitter(undefined, options.events);
  const taskNotificationCommitter = options.taskNotificationCommitter ?? new RecordingTaskNotificationCommitter();
  const logger = new RecordingLogger();
  const service = new RuntimeControlService({
    ownPod: { namespace: "engine", name: "runtime-pod-a", uid: "uid-a", ip: "10.0.0.1" },
    allowedBridge: { namespace: "engine", name: "bridge" },
    authenticator: new FixedAuthenticator(options.auth ?? "allow"),
    runHost,
    controlInputCommitter,
    taskNotificationCommitter,
    cleanupController,
    logger,
    ready: () => true,
    ...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
    ...(options.commandRunner !== undefined ? { commandRunner: options.commandRunner } : {}),
  });
  return { service, runHost, cleanupController, controlInputCommitter, taskNotificationCommitter, logger };
}

function validCommand(overrides: Partial<RuntimeInputCommandRequest> = {}): RuntimeInputCommandRequest {
  return {
    requestId: "req_1",
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 42,
    targetPodNamespace: "engine",
    targetPodName: "runtime-pod-a",
    targetPodUid: "uid-a",
    targetPodIp: "10.0.0.1",
    runtimeInputId: "rin_1",
    eventIds: ["sevt_1"],
    sequenceFrom: 7,
    sequenceTo: 9,
    commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_MESSAGES,
    payloadJson: "",
    ...overrides,
  };
}

function commandScope(overrides: Partial<RuntimeInputCommandRequest> = {}): RuntimeCommandScope {
  const command = validCommand(overrides);
  return {
    requestId: command.requestId,
    workspaceId: command.workspaceId,
    sessionId: command.sessionId,
    sessionThreadId: command.sessionThreadId,
    bindingId: command.bindingId,
    bindingGeneration: command.bindingGeneration,
    targetPodUid: command.targetPodUid,
    runtimeInputId: command.runtimeInputId,
    eventIds: [...command.eventIds],
    sequenceFrom: command.sequenceFrom,
    sequenceTo: command.sequenceTo,
  };
}

function canonicalTaskNotificationPayloadJson(input: {
  readonly taskId: string;
  readonly sourceToolUseEventId: string;
  readonly status: "completed" | "failed" | "cancelled" | "expired";
}): string {
  return JSON.stringify({
    task_id: input.taskId,
    source_tool_use_event_id: input.sourceToolUseEventId,
    status: input.status,
    stdout: { text: "", truncated: false },
    stderr: { text: "", truncated: false },
  });
}

function acceptedResponse(overrides: {
  readonly runtimeInputId?: string;
  readonly bindingId?: string;
} = {}): RuntimeInputCommandResponse {
  return {
    status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED,
    retryable: false,
    errorCode: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_UNSPECIFIED,
    sessionId: "sesn_1",
    runtimeInputId: overrides.runtimeInputId ?? "rin_1",
    bindingId: overrides.bindingId ?? "bind_1",
    bindingGeneration: 42,
  };
}

function rejectedResponse(overrides: {
  readonly runtimeInputId?: string;
  readonly bindingId?: string;
  readonly retryable: boolean;
  readonly errorCode: string;
}): RuntimeInputCommandResponse {
  return {
    status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED,
    retryable: overrides.retryable,
    errorCode: runtimeInputErrorCodeOrGeneric(overrides.errorCode),
    sessionId: "sesn_1",
    runtimeInputId: overrides.runtimeInputId ?? "rin_1",
    bindingId: overrides.bindingId ?? "bind_1",
    bindingGeneration: 42,
  };
}

function authMetadata(token = "caller-token"): Metadata {
  const metadata = new Metadata();
  metadata.set("authorization", `bearer ${token}`);
  return metadata;
}

async function expectGrpcCode(promise: Promise<unknown>, code: status): Promise<void> {
  try {
    await promise;
    throw new Error(`expected gRPC code ${code}`);
  } catch (error) {
    if (error instanceof GrpcStatusError) {
      expect(error.code).toBe(code);
      return;
    }
    throw error;
  }
}

class FixedAuthenticator implements RuntimeAuthenticator {
  constructor(private readonly mode: "allow" | "deny") {}

  async authenticate() {
    if (this.mode === "deny") {
      return { ok: false as const, code: "PermissionDenied" as const, message: "permission denied" };
    }
    return { ok: true as const, serviceAccount: { namespace: "engine", name: "bridge" } };
  }
}

class RecordingRunHost implements RuntimeSessionRunHost {
  readonly sessionIds: string[] = [];
  readonly messageCommands: Array<Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0]> = [];
  readonly agentMailCommands: Array<Parameters<RuntimeSessionRunHost["handleAgentMail"]>[0]> = [];
  readonly interrupts: Array<{ readonly sessionId: string; readonly command: Parameters<RuntimeSessionRunHost["handleInterruptControl"]>[1] }> = [];
  readonly toolConfirmations: Array<{ readonly sessionId: string; readonly command: Parameters<RuntimeSessionRunHost["handleToolConfirmation"]>[1] }> = [];
  readonly taskNotifications: Array<{ readonly sessionId: string; readonly command: Parameters<RuntimeSessionRunHost["handleTaskNotification"]>[1] }> = [];
  readonly runtimeConfigPatches: Array<{ readonly sessionId: string; readonly command: Parameters<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>[1] }> = [];

  constructor(
    result:
      | Awaited<ReturnType<RuntimeSessionRunHost["handleAcceptInput"]>>
      | {
          readonly acceptInputResult?: Awaited<ReturnType<RuntimeSessionRunHost["handleAcceptInput"]>>;
          readonly toolConfirmationResult?: Awaited<ReturnType<RuntimeSessionRunHost["handleToolConfirmation"]>>;
          readonly taskNotificationResult?: Awaited<ReturnType<RuntimeSessionRunHost["handleTaskNotification"]>>;
          readonly runtimeConfigPatchResult?: Awaited<ReturnType<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>>;
          readonly interruptResult?: Awaited<ReturnType<RuntimeSessionRunHost["handleInterruptControl"]>>;
          readonly agentMailResult?: Awaited<ReturnType<RuntimeSessionRunHost["handleAgentMail"]>>;
        } = {},
    private readonly events?: string[] | undefined,
  ) {
    if ("ok" in result) {
      this.acceptInputResult = result;
      return;
    }
    this.acceptInputResult = result.acceptInputResult ?? this.acceptInputResult;
    this.toolConfirmationResult = result.toolConfirmationResult ?? this.toolConfirmationResult;
    this.taskNotificationResult = result.taskNotificationResult ?? this.taskNotificationResult;
    this.runtimeConfigPatchResult = result.runtimeConfigPatchResult ?? this.runtimeConfigPatchResult;
    this.interruptResult = result.interruptResult ?? this.interruptResult;
    this.agentMailResult = result.agentMailResult ?? this.agentMailResult;
  }

  private readonly acceptInputResult: Awaited<ReturnType<RuntimeSessionRunHost["handleAcceptInput"]>> = {
    ok: true,
    sessionId: "sesn_1",
    created: true,
    started: true,
    pendingWake: false,
  };
  private readonly toolConfirmationResult: Awaited<ReturnType<RuntimeSessionRunHost["handleToolConfirmation"]>> = {
    ok: true,
    sessionId: "sesn_1",
    created: false,
    applied: true,
  };
  private readonly taskNotificationResult: Awaited<ReturnType<RuntimeSessionRunHost["handleTaskNotification"]>> = {
    ok: true,
    sessionId: "sesn_1",
    created: false,
    applied: true,
  };
  private readonly runtimeConfigPatchResult: Awaited<ReturnType<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>> = {
    ok: true,
    sessionId: "sesn_1",
    created: false,
    applied: true,
  };
  private readonly interruptResult: Awaited<ReturnType<RuntimeSessionRunHost["handleInterruptControl"]>> | undefined;
  private readonly agentMailResult: Awaited<ReturnType<RuntimeSessionRunHost["handleAgentMail"]>> = {
    ok: true,
    sessionId: "sesn_1",
    applied: true,
  };

  async handleAcceptInput(command: Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0]) {
    this.messageCommands.push(command);
    this.sessionIds.push(command.sessionId);
    return this.acceptInputResult;
  }

  async handleAgentMail(command: Parameters<RuntimeSessionRunHost["handleAgentMail"]>[0]) {
    this.agentMailCommands.push(command);
    return this.agentMailResult;
  }

  async handleInterruptControl(
    sessionId: string,
    command: Parameters<RuntimeSessionRunHost["handleInterruptControl"]>[1],
    commitInterruptInput: Parameters<RuntimeSessionRunHost["handleInterruptControl"]>[2],
  ) {
    this.interrupts.push({ sessionId, command });
    this.events?.push(`runHost.interrupt:start:${command.runtimeInputId}`);
    if (this.interruptResult?.ok === true && this.interruptResult.idleInterrupt) {
      this.events?.push(`runHost.interrupt:end:${command.runtimeInputId}`);
      return this.interruptResult;
    }
    if (commitInterruptInput === undefined) {
      throw new Error("missing interrupt input committer");
    }
    const committed = await commitInterruptInput();
    this.events?.push(`runHost.interrupt:end:${command.runtimeInputId}`);
    if (!committed.ok) {
      return { ok: false as const, sessionId, reason: "context_load_failed" as const };
    }
    return this.interruptResult ?? {
      ok: true as const,
      sessionId,
      created: false,
      interrupted: true,
      idleInterrupt: false,
    };
  }

  async handleToolConfirmation(sessionId: string, command: Parameters<RuntimeSessionRunHost["handleToolConfirmation"]>[1]) {
    this.toolConfirmations.push({ sessionId, command });
    return this.toolConfirmationResult;
  }

  async handleTaskNotification(sessionId: string, command: Parameters<RuntimeSessionRunHost["handleTaskNotification"]>[1]) {
    this.taskNotifications.push({ sessionId, command });
    return this.taskNotificationResult;
  }

  async handleRuntimeConfigPatch(sessionId: string, command: Parameters<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>[1]) {
    this.runtimeConfigPatches.push({ sessionId, command });
    return this.runtimeConfigPatchResult;
  }
}

class RecordingTaskNotificationCommitter implements RuntimeTaskNotificationCommitter {
  readonly commits: Array<Parameters<RuntimeTaskNotificationCommitter["commitTaskNotification"]>[0]> = [];

  constructor(
    private readonly result: Awaited<ReturnType<RuntimeTaskNotificationCommitter["commitTaskNotification"]>> = {
      ok: true,
      runtimeMessage: bridgeRuntimeMessage(),
    },
  ) {}

  async commitTaskNotification(input: Parameters<RuntimeTaskNotificationCommitter["commitTaskNotification"]>[0]) {
    this.commits.push(input);
    return this.result;
  }
}

class RecordingControlInputCommitter implements RuntimeControlInputCommitter {
  readonly commits: Array<Parameters<RuntimeControlInputCommitter["commitControlInput"]>[0]> = [];

  constructor(
    private readonly result:
      | Awaited<ReturnType<RuntimeControlInputCommitter["commitControlInput"]>>
      | readonly Awaited<ReturnType<RuntimeControlInputCommitter["commitControlInput"]>>[] = { ok: true },
    private readonly events?: string[] | undefined,
  ) {}

  async commitControlInput(input: Parameters<RuntimeControlInputCommitter["commitControlInput"]>[0]) {
    this.commits.push(input);
    this.events?.push(`commit.${input.inputKind}:${input.scope.runtimeInputId}`);
    if (Array.isArray(this.result)) {
      return this.result[Math.min(this.commits.length - 1, this.result.length - 1)] ?? { ok: true as const };
    }
    return this.result;
  }
}

class RecordingCleanupController implements RuntimeCleanupController {
  readonly requests: Array<{ readonly scope: RuntimeCommandScope; readonly reason: "expired" | "operator_requested" }> = [];

  constructor(
    private readonly result: Awaited<ReturnType<RuntimeCleanupController["startCleanup"]>> = {
      ok: true,
      sessionId: "sesn_1",
      completion: Promise.resolve({ ok: true, sessionId: "sesn_1", cleaned: true }),
    },
  ) {}

  async startCleanup(scope: RuntimeCommandScope, reason: "expired" | "operator_requested") {
    this.requests.push({ scope, reason });
    return this.result;
  }
}

class RecordingLogger {
  readonly records: RuntimePodLogRecord[] = [];

  info(record: RuntimePodLogRecord): void {
    this.records.push(record);
  }

  error(record: RuntimePodLogRecord): void {
    this.records.push(record);
  }
}
