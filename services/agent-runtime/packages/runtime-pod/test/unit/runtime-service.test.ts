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
} from "../../src/runtime-service.js";
import type { RuntimePodLogRecord } from "../../src/logger.js";
import {
  buildRuntimeServiceBridgeRuntimeMessage as bridgeRuntimeMessage,
} from "../../../core/test/unit/runtime-message-builders.js";
import { RuntimeMessageCreateSchema } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
  RuntimeControlInputDeclaration,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";

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

  test("accepts a declared agent-mail envelope through its dedicated effect", async () => {
    const fixture = runtimeFixture();
    const request = validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_AGENT_MAIL,
      runtimeInputId: "agent_mail:delivery_1",
      eventIds: ["sevt_agent_mail_1"],
      sequenceFrom: 7,
      sequenceTo: 7,
      payloadJson: agentMailPayloadJson("delivery_1"),
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

  test("rejects agent-mail content that diverges from its authoritative Runtime parts", async () => {
    const fixture = runtimeFixture();
    const payload = JSON.parse(agentMailPayloadJson("delivery_drift")) as {
      message: { content: Array<{ type: string; text: string }> };
    };
    payload.message.content[0]!.text = "different derived text";
    await expectGrpcCode(fixture.service.acceptAgentMail(validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_AGENT_MAIL,
      runtimeInputId: "agent_mail:delivery_drift",
      eventIds: ["sevt_agent_mail_drift"],
      sequenceFrom: 7,
      sequenceTo: 7,
      payloadJson: JSON.stringify(payload),
    }), authMetadata()), status.INVALID_ARGUMENT);
    expect(fixture.runHost.agentMailCommands).toEqual([]);
  });

  test("returns a retryable in-band rejection when agent-mail context loading is transiently unavailable", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        agentMailResult: { ok: false, sessionId: "sesn_1", reason: "context_load_failed", retryable: true },
      }),
    });
    const request = validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_AGENT_MAIL,
      runtimeInputId: "agent_mail:delivery_retry",
      eventIds: ["sevt_agent_mail_retry"],
      sequenceFrom: 8,
      sequenceTo: 8,
      payloadJson: agentMailPayloadJson("delivery_retry"),
    });

    const response = await fixture.service.acceptAgentMail(request, authMetadata());

    expect(response.status).toBe(RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED);
    expect(response.retryable).toBe(true);
    expect(response.errorCode).toBe(runtimeInputErrorCodeOrGeneric("runtime_context_load_failed"));
  });

  test("deduplicates the same runtime_input_id across RPC attempt request_id changes", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        acceptInputResults: [
          { ok: true, sessionId: "sesn_1", created: true, started: true },
          { ok: true, sessionId: "sesn_1", created: false, started: false, duplicate: true },
        ],
      }),
    });

    const first = await fixture.service.acceptInput(validCommand(), authMetadata());
    const duplicate = await fixture.service.acceptInput({ ...validCommand(), requestId: "req_2" }, authMetadata());

    expect(first.status).toBe(RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED);
    expect(duplicate).toEqual({ ...acceptedResponse(), status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_DUPLICATE });
    expect(fixture.runHost.sessionIds).toEqual(["sesn_1", "sesn_1"]);
  });

  test("delivers a bounded rejection fact without the rejected input body", async () => {
    const fixture = runtimeFixture();
    const response = await fixture.service.acceptInput(validCommand({
      payloadJson: JSON.stringify({
        input_kind: "rejection",
        reason_code: "runtime_command_payload_too_large",
      }),
    }), authMetadata());

    expect(response.status).toBe(RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED);
    expect(fixture.runHost.messageCommands).toEqual([
      expect.objectContaining({
        kind: "rejection",
        reasonCode: "runtime_command_payload_too_large",
      }),
    ]);
    expect(fixture.runHost.messageCommands[0]).not.toHaveProperty("payloadJson");
  });

  test("deduplicates a config patch rebuilt under a fresh active binding by input id and payload", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        runtimeConfigPatchResults: [
          { ok: true, sessionId: "sesn_1", created: false, applied: true },
          { ok: true, sessionId: "sesn_1", created: false, applied: true },
          { ok: true, sessionId: "sesn_1", created: false, applied: false },
        ],
      }),
    });
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
    expect(fixture.runHost.runtimeConfigPatches).toHaveLength(3);

    const staleBinding = await fixture.service.applyRuntimeConfig({
      ...first,
      requestId: "req_stale_binding_retry",
    }, authMetadata());
    expect(staleBinding).toMatchObject({
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED,
      retryable: true,
      errorCode: runtimeInputErrorCodeOrGeneric("binding_identity_mismatch"),
    });
    expect(fixture.runHost.runtimeConfigPatches).toHaveLength(3);
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

  test("reports conflicting runtime_input_id identity from the thread-owned receipt", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        acceptInputResults: [
          { ok: true, sessionId: "sesn_1", created: true, started: true },
          { ok: false, sessionId: "sesn_1", reason: "control_conflict" },
        ],
      }),
    });

    await fixture.service.acceptInput(validCommand(), authMetadata());
    const conflict = await fixture.service.acceptInput(
      { ...validCommand(), payloadJson: JSON.stringify({ changed: true }) },
      authMetadata(),
    );

    expect(conflict).toEqual({
      ...acceptedResponse(),
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED,
      retryable: false,
      errorCode: runtimeInputErrorCodeOrGeneric("runtime_input_identity_conflict"),
    });
    expect(fixture.runHost.sessionIds).toEqual(["sesn_1", "sesn_1"]);
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
        payloadJson: JSON.stringify({ origin: "user" }),
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
      { sessionId: "sesn_1", command: { ...commandScope({ runtimeInputId: "rin_interrupt" }), origin: "user" } },
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
        },
      },
    ]);
    expect(fixture.controlInputCommitter.commits.map(({ scope, inputKind }) => ({ scope, inputKind }))).toEqual([
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
        testControlSuccess(commandScope({ runtimeInputId: "rin_interrupt_commit_fail" }), "interrupt_control"),
      ], events),
    });
    const command = validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
      runtimeInputId: "rin_interrupt_commit_fail",
      payloadJson: JSON.stringify({ origin: "user" }),
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
      { sessionId: "sesn_1", command: { ...commandScope({ runtimeInputId: "rin_interrupt_commit_fail" }), origin: "user" } },
      {
        sessionId: "sesn_1",
        command: { ...commandScope({ runtimeInputId: "rin_interrupt_commit_fail" }), requestId: "req_retry", origin: "user" },
      },
    ]);
    expect(fixture.controlInputCommitter.commits.map(({ scope, inputKind }) => ({ scope, inputKind }))).toEqual([
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

  test("preserves a joined interrupt declaration rejection without requesting redelivery", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        interruptResult: {
          ok: false,
          sessionId: "sesn_1",
          reason: "context_load_failed",
          retryable: false,
          errorCode: "bridge_commit_rejected",
        },
      }),
    });
    const command = validCommand({
      commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
      runtimeInputId: "rin_joined_interrupt_rejected",
      payloadJson: JSON.stringify({ origin: "user" }),
    });

    await expect(fixture.service.interrupt(command, authMetadata())).resolves.toEqual(
      rejectedResponse({
        runtimeInputId: "rin_joined_interrupt_rejected",
        retryable: false,
        errorCode: "bridge_commit_rejected",
      }),
    );
  });

  test("idle interrupt commits its admitted declaration through the host callback", async () => {
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
      payloadJson: JSON.stringify({ origin: "user" }),
    });

    await expect(fixture.service.interrupt(command, authMetadata())).resolves.toEqual(
      acceptedResponse({ runtimeInputId: "rin_idle_interrupt" }),
    );
    expect(events).toEqual([
      "runHost.interrupt:start:rin_idle_interrupt",
      "commit.interrupt_control:rin_idle_interrupt",
      "runHost.interrupt:end:rin_idle_interrupt",
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

    expect(fixture.runHost.toolConfirmations).toHaveLength(2);
    expect(fixture.runHost.toolConfirmationCommitResults).toEqual([
      expect.objectContaining({ ok: false, retryable: false }),
      expect.objectContaining({ ok: false, retryable: false }),
    ]);
    expect(fixture.controlInputCommitter.commits.map(({ scope, inputKind }) => ({ scope, inputKind }))).toEqual([
      {
        scope: commandScope({ runtimeInputId: "rin_confirm_stale" }),
        inputKind: "tool_confirmation",
      },
      {
        scope: { ...commandScope({ runtimeInputId: "rin_confirm_stale" }), requestId: "req_2" },
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

  test("task-notification transport acceptance does not perform the semantic commit", async () => {
    const fixture = runtimeFixture();

    await expect(
      fixture.service.acceptTaskNotification(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
          runtimeInputId: "rin_task",
          payloadJson: canonicalTaskNotificationPayloadJson({ taskId: "task_1", sourceToolUseEventId: "sevt_tool_1", status: "completed" }),
        }),
        authMetadata(),
      ),
    ).resolves.toEqual(acceptedResponse({ runtimeInputId: "rin_task" }));

    expect(fixture.runHost.taskNotifications).toHaveLength(1);
  });

  test("returns in-band terminal rejection when task-notification admission conflicts", async () => {
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
        retryable: true,
        errorCode: "runtime_context_load_failed",
      }),
    );

    expect(fixture.runHost.runtimeConfigPatches).toHaveLength(1);
  });

  test("returns the typed retryable control-busy disposition for deferred runtime config", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        runtimeConfigPatchResult: {
          ok: false,
          sessionId: "sesn_1",
          reason: "control_busy",
        },
      }),
    });

    await expect(
      fixture.service.applyRuntimeConfig(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
          runtimeInputId: "rin_config_busy",
          payloadJson: JSON.stringify({ config_generation: 7 }),
        }),
        authMetadata(),
      ),
    ).resolves.toMatchObject({
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED,
      runtimeInputId: "rin_config_busy",
      retryable: true,
      errorCode: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_CONTROL_BUSY,
    });
  });

  test("returns the typed retryable control-busy disposition for an overlapping interrupt", async () => {
    const fixture = runtimeFixture({
      runHost: new RecordingRunHost({
        interruptResult: {
          ok: false,
          sessionId: "sesn_1",
          reason: "control_busy",
        },
      }),
    });

    await expect(
      fixture.service.interrupt(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
          runtimeInputId: "rin_interrupt_busy",
          payloadJson: JSON.stringify({ origin: "user" }),
        }),
        authMetadata(),
      ),
    ).resolves.toMatchObject({
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED,
      runtimeInputId: "rin_interrupt_busy",
      retryable: true,
      errorCode: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_CONTROL_BUSY,
    });
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

    expect(fixture.runHost.taskNotifications).toEqual([]);
  });

  test("defers the canonicalized task notification declaration to the owning loop", async () => {
    const fixture = runtimeFixture();
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
    const applied = fixture.runHost.taskNotifications[0]?.command.payloadJson;
    expect(JSON.parse(applied ?? "{}")).toEqual(expected);
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
  readonly commandRunner?: RuntimeCommandRunner;
  readonly metrics?: RuntimePodMetricsRegistry;
  readonly events?: string[] | undefined;
} = {}) {
  const runHost = options.runHost ?? new RecordingRunHost(undefined, options.events);
  const cleanupController = options.cleanupController ?? new RecordingCleanupController();
  const controlInputCommitter = options.controlInputCommitter ?? new RecordingControlInputCommitter(undefined, options.events);
  const logger = new RecordingLogger();
  const service = new RuntimeControlService({
    ownPod: { namespace: "engine", name: "runtime-pod-a", uid: "uid-a", ip: "10.0.0.1" },
    allowedBridge: { namespace: "engine", name: "bridge" },
    authenticator: new FixedAuthenticator(options.auth ?? "allow"),
    runHost,
    controlInputCommitter,
    cleanupController,
    logger,
    ready: () => true,
    ...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
    ...(options.commandRunner !== undefined ? { commandRunner: options.commandRunner } : {}),
  });
  return { service, runHost, cleanupController, controlInputCommitter, logger };
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

function agentMailPayloadJson(deliveryId: string): string {
  const message = bridgeRuntimeMessage({
    text: "child completion",
  });
  return JSON.stringify({
    delivery_id: deliveryId,
    source_thread_id: "thrd_child",
    source_tool_use_event_id: "sevt_child_tool",
    message: {
      ...message,
      content: message.parts.map((part) => {
        if (part.type !== "text") {
          throw new Error("agent mail fixture requires text parts");
        }
        return { type: "text", text: part.text };
      }),
    },
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

function testControlDeclaration(
  command: RuntimeCommandScope,
  inputKind: "interrupt_control" | "tool_confirmation",
): RuntimeControlInputDeclaration {
  if (inputKind === "interrupt_control") {
    return { messageCreates: [] };
  }
  return {
    messageCreates: [RuntimeMessageCreateSchema.parse({
      sourceEventId: command.eventIds[0],
      messageKind: "approval_input",
      role: "user",
      origin: "user",
      status: "completed",
      parts: [{
        type: "text",
        text: "Approval allowed",
        truncated: false,
        status: "completed",
      }],
    })],
  };
}

function testControlReceipt(
  input: Parameters<RuntimeControlInputCommitter["commitControlInput"]>[0],
): Extract<Awaited<ReturnType<RuntimeControlInputCommitter["commitControlInput"]>>, { readonly ok: true; readonly receipt: object }> {
  return {
    ok: true,
    receipt: {
      sessionThreadId: input.scope.sessionThreadId,
      operationKind: "commit_inputs",
      sourceKind: input.inputKind,
      operationId: input.scope.runtimeInputId,
      declarationDigest: "digest_test",
      events: input.scope.eventIds.map((eventId, index) => ({
        sessionThreadId: input.scope.sessionThreadId,
        eventId,
        eventSequence: input.scope.sequenceFrom + index,
        disposition: "existing" as const,
      })),
      messages: input.messageCreates.map((create, index) => ({
        sessionThreadId: input.scope.sessionThreadId,
        owningEventId: create.sourceEventId!,
        messageId: `msg_${index}_${input.scope.runtimeInputId}`,
        messageSequence: input.scope.sequenceTo + index + 1,
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "",
        disposition: "created" as const,
        parts: create.parts.map((_part, partIndex) => ({
          partId: `part_${partIndex}_${input.scope.runtimeInputId}`,
          messageId: `msg_${index}_${input.scope.runtimeInputId}`,
          partSequence: partIndex,
          createdAt: "2026-01-01T00:00:00.000Z",
          updatedAt: "",
          disposition: "created" as const,
        })),
      })),
      pendingAttachmentDelta: [],
      interruptToolProjections: [],
      prefixConsumptions: [],
      childLifecycle: [],
    },
  };
}

function testControlSuccess(
  scope: RuntimeCommandScope,
  inputKind: "interrupt_control" | "tool_confirmation",
) {
  const declaration = testControlDeclaration(scope, inputKind);
  return testControlReceipt({ scope, inputKind, ...declaration });
}

class RecordingRunHost implements RuntimeSessionRunHost {
  readonly sessionIds: string[] = [];
  readonly messageCommands: Array<Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0]> = [];
  readonly agentMailCommands: Array<Parameters<RuntimeSessionRunHost["handleAgentMail"]>[0]> = [];
  readonly interrupts: Array<{ readonly sessionId: string; readonly command: Parameters<RuntimeSessionRunHost["handleInterruptControl"]>[1] }> = [];
  readonly toolConfirmations: Array<{ readonly sessionId: string; readonly command: Parameters<RuntimeSessionRunHost["handleToolConfirmation"]>[1] }> = [];
  readonly toolConfirmationCommitResults: Array<Awaited<ReturnType<Parameters<RuntimeSessionRunHost["handleToolConfirmation"]>[2]>>> = [];
  readonly taskNotifications: Array<{ readonly sessionId: string; readonly command: Parameters<RuntimeSessionRunHost["handleTaskNotification"]>[1] }> = [];
  readonly runtimeConfigPatches: Array<{ readonly sessionId: string; readonly command: Parameters<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>[1] }> = [];

  constructor(
    result:
      | Awaited<ReturnType<RuntimeSessionRunHost["handleAcceptInput"]>>
      | {
          readonly acceptInputResult?: Awaited<ReturnType<RuntimeSessionRunHost["handleAcceptInput"]>>;
          readonly acceptInputResults?: readonly Awaited<ReturnType<RuntimeSessionRunHost["handleAcceptInput"]>>[];
          readonly toolConfirmationResult?: Awaited<ReturnType<RuntimeSessionRunHost["handleToolConfirmation"]>>;
          readonly taskNotificationResult?: Awaited<ReturnType<RuntimeSessionRunHost["handleTaskNotification"]>>;
          readonly runtimeConfigPatchResult?: Awaited<ReturnType<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>>;
          readonly runtimeConfigPatchResults?: readonly Awaited<ReturnType<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>>[];
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
    this.acceptInputResults = [...(result.acceptInputResults ?? [])];
    this.toolConfirmationResult = result.toolConfirmationResult ?? this.toolConfirmationResult;
    this.taskNotificationResult = result.taskNotificationResult ?? this.taskNotificationResult;
    this.runtimeConfigPatchResult = result.runtimeConfigPatchResult ?? this.runtimeConfigPatchResult;
    this.runtimeConfigPatchResults = [...(result.runtimeConfigPatchResults ?? [])];
    this.interruptResult = result.interruptResult ?? this.interruptResult;
    this.agentMailResult = result.agentMailResult ?? this.agentMailResult;
  }

  private readonly acceptInputResult: Awaited<ReturnType<RuntimeSessionRunHost["handleAcceptInput"]>> = {
    ok: true,
    sessionId: "sesn_1",
    created: true,
    started: true,
  };
  private readonly acceptInputResults: Array<Awaited<ReturnType<RuntimeSessionRunHost["handleAcceptInput"]>>> = [];
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
  private readonly runtimeConfigPatchResults: Array<Awaited<ReturnType<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>>> = [];
  private readonly interruptResult: Awaited<ReturnType<RuntimeSessionRunHost["handleInterruptControl"]>> | undefined;
  private readonly agentMailResult: Awaited<ReturnType<RuntimeSessionRunHost["handleAgentMail"]>> = {
    ok: true,
    sessionId: "sesn_1",
    applied: true,
  };

  async handleAcceptInput(command: Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0]) {
    this.messageCommands.push(command);
    this.sessionIds.push(command.sessionId);
    return this.acceptInputResults.shift() ?? this.acceptInputResult;
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
    if (commitInterruptInput === undefined) {
      throw new Error("missing interrupt input committer");
    }
    const committed = await commitInterruptInput(testControlDeclaration(command, "interrupt_control"));
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

  async handleToolConfirmation(
    sessionId: string,
    command: Parameters<RuntimeSessionRunHost["handleToolConfirmation"]>[1],
    commit: Parameters<RuntimeSessionRunHost["handleToolConfirmation"]>[2],
  ) {
    this.toolConfirmations.push({ sessionId, command });
    const committed = await commit(testControlDeclaration(command, "tool_confirmation"));
    this.toolConfirmationCommitResults.push(committed);
    if (!committed.ok) {
      return { ok: false as const, sessionId, reason: "context_load_failed" as const };
    }
    if ("stale" in committed) {
      return { ok: true as const, sessionId, created: false, applied: false as const, stale: true as const };
    }
    return this.toolConfirmationResult;
  }

  async handleTaskNotification(
    sessionId: string,
    command: Parameters<RuntimeSessionRunHost["handleTaskNotification"]>[1],
  ) {
    this.taskNotifications.push({ sessionId, command });
    return this.taskNotificationResult;
  }

  async handleRuntimeConfigPatch(sessionId: string, command: Parameters<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>[1]) {
    this.runtimeConfigPatches.push({ sessionId, command });
    return this.runtimeConfigPatchResults.shift() ?? this.runtimeConfigPatchResult;
  }
}

class RecordingControlInputCommitter implements RuntimeControlInputCommitter {
  readonly commits: Array<Parameters<RuntimeControlInputCommitter["commitControlInput"]>[0]> = [];

  constructor(
    private readonly result:
      | Awaited<ReturnType<RuntimeControlInputCommitter["commitControlInput"]>>
      | readonly Awaited<ReturnType<RuntimeControlInputCommitter["commitControlInput"]>>[]
      | undefined = undefined,
    private readonly events?: string[] | undefined,
  ) {}

  async commitControlInput(input: Parameters<RuntimeControlInputCommitter["commitControlInput"]>[0]) {
    this.commits.push(input);
    this.events?.push(`commit.${input.inputKind}:${input.scope.runtimeInputId}`);
    if (Array.isArray(this.result)) {
      return this.result[Math.min(this.commits.length - 1, this.result.length - 1)] ?? testControlReceipt(input);
    }
    return this.result ?? testControlReceipt(input);
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
