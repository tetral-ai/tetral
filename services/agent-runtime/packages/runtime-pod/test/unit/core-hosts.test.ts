import { describe, expect, test } from "bun:test";
import { Stream } from "effect";
import { RuntimeInternalToolRepairStore } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
  SessionEvent,
  SessionEventEnvelope,
  SessionEventWriterAppendResult,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { createToolCatalog, lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import type { RuntimeCoreHostsOptions } from "../../src/core-hosts.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { runtimeModelForThread, runtimeToolPolicyForThread } from "../../src/command.js";
import {
  buildCoreHostsUserMessage as userMessage,
  buildCoreHostsAssistantRunningToolMessage as assistantRunningToolMessage,
  buildCoreHostsBridgeRuntimeMessage as bridgeRuntimeMessage,
} from "../../../core/test/unit/runtime-message-builders.js";
import { acceptedInputReceipt } from "../../../core/test/unit/runtime-declaration-fixtures.js";

const emptyColdCoverage = {
  pendingToolIds: [],
  pendingAttachmentIdentities: [],
  undeliveredMailDeliveryIds: [],
} as const;

describe("Runtime core host production assembly", () => {
  test("resident threads admit stored completion envelopes through the local command channel", async () => {
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies(),
    });
    try {
      const accepted = await hosts.commandRunHost.handleAcceptInput(acceptedInput("sesn_1"));
      expect(accepted).toMatchObject({ ok: true, sessionId: "sesn_1" });
      const mail = await hosts.commandRunHost.handleAgentMail?.({
        ...commandScope("sesn_1"),
        runtimeInputId: "agent_mail:delivery_warm_push",
        deliveryId: "delivery_warm_push",
        sourceThreadId: "thrd_child",
        sourceToolUseEventId: "sevt_child_spawn",
        message: bridgeRuntimeMessage("sesn_1", "child result"),
      });
      expect(mail).toMatchObject({ ok: true, sessionId: "sesn_1", applied: true });

      const cleanup = await waitForCleanup(hosts.cleanupRunHost, "sesn_1");
      expect(cleanup).toMatchObject({ ok: true, sessionId: "sesn_1" });
    } finally {
      await hosts.close();
    }
  });

  test("message command exposes an installing ThreadEntry during its sole cold load", async () => {
    const observations: string[] = [];
    let loadCount = 0;
    let hosts: Awaited<ReturnType<typeof buildRuntimeCoreHosts>> | undefined;
    hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async (command) => {
            loadCount += 1;
            observations.push("loadThreadContext");
            const inspected = await hosts?.subAgentRunHost.inspectThread(command);
            observations.push(`observed:${inspected?.ok === true ? inspected.observed : "unavailable"}`);
            return {
              messages: [],
              runtimeBindingToken: "runtime-binding-token-cold",
              coldCoverage: emptyColdCoverage,
            };
          },
        },
      }),
    });
    try {
      const accepted = await hosts.commandRunHost.handleAcceptInput(acceptedInput("sesn_cold"));
      const inspected = await hosts.subAgentRunHost.inspectThread(commandScope("sesn_cold"));

      expect(accepted).toMatchObject({ ok: true, sessionId: "sesn_cold", created: true });
      expect(observations).toEqual(["loadThreadContext", "observed:true"]);
      expect(inspected).toMatchObject({ ok: true, sessionId: "sesn_cold", sessionThreadId: "thrd_1", observed: true });
      expect(await hosts.commandRunHost.handleAcceptInput({
        ...acceptedInput("sesn_cold"),
        runtimeInputId: "rin_warm",
        eventIds: ["sevt_warm"],
        sequenceFrom: 2,
        sequenceTo: 2,
      })).toMatchObject({ ok: true, sessionId: "sesn_cold" });
      expect(loadCount).toBe(1);
    } finally {
      await hosts.close();
    }
  });

  test("cold interrupt stays residency-free while message config and task commands join one complete preload", async () => {
    const loadStarted = deferred<void>();
    const releaseLoad = deferred<void>();
    let loadCount = 0;
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async () => {
            loadCount += 1;
            loadStarted.resolve();
            await releaseLoad.promise;
            return {
              messages: [],
              runtimeBindingToken: "runtime-binding-token-singleflight",
              backgroundTools: [{ taskId: "task_singleflight", sourceToolUseEventId: "sevt_tool_singleflight" }],
              coldCoverage: emptyColdCoverage,
            };
          },
        },
      }),
    });
    try {
      const scope = commandScope("sesn_singleflight");
      const interrupt = await hosts.commandRunHost.handleInterruptControl("sesn_singleflight", {
        ...scope,
        runtimeInputId: "rin_interrupt_singleflight",
        eventIds: ["sevt_interrupt_singleflight"],
        sequenceFrom: 1,
        sequenceTo: 1,
      }, async () => ({ ok: true }));
      expect(interrupt).toEqual({ ok: true, sessionId: "sesn_singleflight", created: false, interrupted: false, idleInterrupt: true });
      expect(await hosts.subAgentRunHost.inspectThread(scope)).toMatchObject({ ok: true, observed: false });

      const message = hosts.commandRunHost.handleAcceptInput({
        ...acceptedInput("sesn_singleflight"),
        runtimeInputId: "rin_message_singleflight",
      });
      await loadStarted.promise;
      const config = hosts.commandRunHost.handleRuntimeConfigPatch("sesn_singleflight", {
        ...scope,
        bindingId: "bind_singleflight_replacement",
        bindingGeneration: 2,
        runtimeInputId: "rin_config_singleflight",
        generation: 2,
        payloadJson: "{\"config_generation\":2}",
      });
      const task = hosts.commandRunHost.handleTaskNotification("sesn_singleflight", {
        ...scope,
        runtimeInputId: "rin_task_singleflight",
        taskId: "task_singleflight",
        sourceToolUseEventId: "sevt_tool_singleflight",
        status: "completed",
        payloadJson: "{\"task_id\":\"task_singleflight\",\"source_tool_use_event_id\":\"sevt_tool_singleflight\",\"status\":\"completed\"}",
      }, async () => ({
        ok: true,
        committedMessage: bridgeRuntimeMessage("sesn_singleflight", "task completed"),
      }));
      await Promise.resolve();
      expect(await hosts.subAgentRunHost.inspectThread(scope)).toMatchObject({ ok: true, observed: true });

      releaseLoad.resolve();
      const [messageResult, configResult, taskResult] = await Promise.all([message, config, task]);
      expect(messageResult).toMatchObject({ ok: true, sessionId: "sesn_singleflight" });
      expect(configResult).toEqual({ ok: false, sessionId: "sesn_singleflight", reason: "control_busy" });
      expect(taskResult).toMatchObject({ ok: true, sessionId: "sesn_singleflight", applied: true });
      expect(loadCount).toBe(1);
      expect(await hosts.subAgentRunHost.inspectThread(scope)).toMatchObject({
        ok: true,
        observed: true,
        messages: expect.arrayContaining([expect.objectContaining({ id: "msg_sesn_singleflight_task_notification" })]),
      });
    } finally {
      await hosts.close();
    }
  });

  test("approved pending MCP call resumes after cold manifest install", async () => {
    const observations: string[] = [];
    const runToolCalls: string[] = [];
    const appended: SessionEvent[] = [];
    const terminalResultAppended = deferred<void>();
    const pendingInput = { query: "tetral" };
    const loadedMessages = [
      userMessage("sesn_cold_confirm", "user-cold-confirm", 0, "hello"),
      assistantRunningToolMessage(
        "sesn_cold_confirm",
        "assistant-cold-confirm",
        1,
        "tool-1",
        "github_search",
        "sevt_tool_1",
        pendingInput,
        { kind: "mcp", mcpServerName: "github" },
      ),
    ];
    const replacementScope = {
      ...commandScope("sesn_cold_confirm"),
      bindingId: "bind_2",
      bindingGeneration: 2,
      targetPodUid: "pod_2",
    };

    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async (command) => {
            observations.push(`load:${command.bindingId}:${command.bindingGeneration}`);
            return {
              messages: loadedMessages,
              runtimeBindingToken: "runtime-binding-token-cold-confirm",
              runtimeConfigPatch: {
                ...command,
                generation: 5,
                coldLoad: true,
                installedBuiltinFamily: "claude",
                payloadJson: JSON.stringify({
                  config_generation: 5,
                  runtime_config: {
                    agent: { config: { model: "openai/gpt-5.5" } },
                    installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
                  },
                  tool_policy: { mcpToolsets: [{ mcpServerName: "github" }] },
                }),
              },
              mcpManifests: [{
                ...command,
                generation: 7,
                mcpServerName: "github",
                manifestETag: "etag_7",
                payloadJson: JSON.stringify({
                  mcp_manifest: {
                    mcp_server_name: "github",
                    manifest_etag: "etag_7",
                    manifest_generation: 7,
                    tools: [{ name: "github_search", description: "Search GitHub", input_schema: { type: "object" } }],
                  },
                }),
              }],
              pendingToolUses: [
                {
                  toolUseEventId: "sevt_tool_1",
                  modelRequestId: "mrq_cold_confirm",
                  modelToolCallId: "tool-1",
                  toolName: "github_search",
                  kind: "approval",
                  input: pendingInput,
                  decision: "allow",
                  status: "resolving",
                  expiresAt: "2026-06-16T00:30:00.000Z",
                },
              ],
              coldCoverage: {
                ...emptyColdCoverage,
                pendingToolIds: ["sevt_tool_1"],
              },
            };
          },
        },
        agentLoop: {
          providerCallRuntime: {
            systemInstructions: "cold confirmation system",
          },
          runtimeModel: (session) => runtimeModelForThread(
            session.identity.threadRole,
            session.configuration.patches().map((patch) => patch.payloadJson),
            { providerId: "anthropic", modelId: "claude-opus-4-8" },
          ),
          runtimePolicy: (session) => {
            const policy = runtimeToolPolicyForThread(
              session.identity.threadRole,
              session.configuration.patches().map((patch) => patch.payloadJson),
              session.configuration.installedBuiltinFamily(),
            );
            if (lookupToolEntry(policy.toolCatalog, "github_search") !== undefined) {
              observations.push("manifest:installed");
            }
            return policy;
          },
          sessionEventWriter: {
            append: async (envelope) => {
              appended.push(envelope.event);
              if (envelope.event.type === "agent.mcp_tool_result") {
                terminalResultAppended.resolve();
              }
              return successfulEventAppend(envelope);
            },
            writeRequestEnd: async (envelope) => ({ ok: true, writeId: envelope.writeId, eventId: `evt_${envelope.writeId}`, processedAt: "2026-06-16T00:00:00.000Z" }),
            finishIdle: async (envelope) => ({
              ok: true,
              writeId: envelope.durableTurnId,
              eventId: `evt_${envelope.durableTurnId}`,
              processedAt: "2026-06-16T00:00:00.000Z",
            }),
          },
          runTool: (request) => {
            observations.push("tool:invoked");
            runToolCalls.push(`${request.modelRequestId}:${request.modelToolCallId}:${request.toolUseEventId}`);
            expect(request.input).toEqual(pendingInput);
            expect(request.currentModel).toEqual({ providerId: "openai", modelId: "gpt-5.5" });
            expect(request.entry.route).toEqual({ kind: "gateway", operation: "RunMcpTool", mcpServerName: "github" });
            expect(request.bindingId).toBe("bind_2");
            expect(request.bindingGeneration).toBe(2);
            return { type: "completed", output: { text: "approved", truncated: false } };
          },
        },
      }),
    });
    try {
      const result = await hosts.commandRunHost.handleToolConfirmation("sesn_cold_confirm", {
        ...replacementScope,
        requestId: "req_confirm_cold",
        runtimeInputId: "rin_confirm_cold",
        eventIds: ["sevt_confirm_cold"],
        sequenceFrom: 2,
        sequenceTo: 2,
        sourceEventId: "sevt_confirm_cold",
        toolUseEventId: "sevt_tool_1",
        decision: "allow",
      }, async () => ({ ok: true }));

      expect(result).toMatchObject({ ok: true, sessionId: "sesn_cold_confirm", applied: false });
      await terminalResultAppended.promise;
      expect(observations[0]).toBe("load:bind_2:2");
      const manifestInstalledAt = observations.indexOf("manifest:installed");
      const toolInvokedAt = observations.indexOf("tool:invoked");
      expect(manifestInstalledAt).toBeGreaterThan(0);
      expect(toolInvokedAt).toBeGreaterThan(manifestInstalledAt);
      expect(runToolCalls).toEqual(["mrq_cold_confirm:tool-1:sevt_tool_1"]);
      const terminalMcpResults = appended.filter((event) => event.type === "agent.mcp_tool_result");
      expect(terminalMcpResults).toEqual([
        expect.objectContaining({
          mcp_tool_use_id: "sevt_tool_1",
          content: [{ type: "text", text: "approved" }],
        }),
      ]);
      expect(terminalMcpResults[0]).not.toHaveProperty("is_error");
      expect(appended.filter((event) => event.type === "agent.tool_result")).toHaveLength(0);
      expect(JSON.stringify(appended)).not.toContain("unknown");
    } finally {
      await hosts.close();
    }
  });

  test("tool confirmation cold-loads after a residency-free idle interrupt", async () => {
    const observations: string[] = [];
    const runToolCalls: string[] = [];
    const toolExecuted = deferred<void>();
    const pendingInput = { file_path: "src/a.ts", content: "ok" };
    const loadedMessages = [
      userMessage("sesn_interrupt_confirm", "user-interrupt-confirm", 0, "hello"),
      assistantRunningToolMessage("sesn_interrupt_confirm", "assistant-interrupt-confirm", 1, "tool-1", "Write", "sevt_tool_1", pendingInput),
    ];

    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async (command) => {
            observations.push(`load:${command.runtimeInputId}`);
            return {
              messages: loadedMessages,
              runtimeBindingToken: "runtime-binding-token-interrupt-confirm",
              runtimeConfigPatch: {
                ...command,
                generation: 3,
                coldLoad: true,
                installedBuiltinFamily: "claude",
                payloadJson: JSON.stringify({
                  config_generation: 3,
                  runtime_config: {
                    agent: { config: { model: "fake/fake-chat" } },
                    installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
                  },
                }),
              },
              pendingToolUses: [
                {
                  toolUseEventId: "sevt_tool_1",
                  modelRequestId: "mrq_interrupt_confirm",
                  modelToolCallId: "tool-1",
                  toolName: "Write",
                  kind: "approval",
                  input: pendingInput,
                  decision: "allow",
                  status: "resolving",
                  expiresAt: "2026-06-16T00:30:00.000Z",
                },
              ],
              coldCoverage: {
                ...emptyColdCoverage,
                pendingToolIds: ["sevt_tool_1"],
              },
            };
          },
        },
        agentLoop: {
          providerCallRuntime: {
            systemInstructions: "cold confirmation system",
            toolCatalog: createToolCatalog({ family: "claude", configs: [{ name: "Write", enabled: true, permissionPolicy: "always_ask" }] }),
          },
          runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
          runTool: (request) => {
            runToolCalls.push(`${request.modelRequestId}:${request.modelToolCallId}:${request.toolUseEventId}`);
            expect(request.input).toEqual(pendingInput);
            toolExecuted.resolve();
            return { type: "completed", output: { text: "approved", truncated: false } };
          },
        },
      }),
    });
    try {
      const interrupt = await hosts.commandRunHost.handleInterruptControl("sesn_interrupt_confirm", {
        ...commandScope("sesn_interrupt_confirm"),
        requestId: "req_interrupt_before_confirm",
        runtimeInputId: "rin_interrupt_before_confirm",
        eventIds: ["sevt_interrupt_before_confirm"],
        sequenceFrom: 2,
        sequenceTo: 2,
      });
      const shell = await hosts.subAgentRunHost.inspectThread(commandScope("sesn_interrupt_confirm"));

      expect(interrupt).toEqual({ ok: true, sessionId: "sesn_interrupt_confirm", created: false, interrupted: false, idleInterrupt: true });
      expect(shell).toMatchObject({ ok: true, observed: false });

      const result = await hosts.commandRunHost.handleToolConfirmation("sesn_interrupt_confirm", {
        ...commandScope("sesn_interrupt_confirm"),
        requestId: "req_confirm_after_interrupt",
        runtimeInputId: "rin_confirm_after_interrupt",
        eventIds: ["sevt_confirm_after_interrupt"],
        sequenceFrom: 3,
        sequenceTo: 3,
        sourceEventId: "sevt_confirm_after_interrupt",
        toolUseEventId: "sevt_tool_1",
        decision: "allow",
      }, async () => ({ ok: true }));

      expect(result).toMatchObject({ ok: true, sessionId: "sesn_interrupt_confirm", applied: false });
      expect(observations).toEqual(["load:rin_confirm_after_interrupt"]);
      await toolExecuted.promise;
      expect(runToolCalls).toEqual(["mrq_interrupt_confirm:tool-1:sevt_tool_1"]);
    } finally {
      await hosts.close();
    }
  });

  test("runtime config acknowledges without creating thread residency", async () => {
    const observations: string[] = [];
    let hosts: Awaited<ReturnType<typeof buildRuntimeCoreHosts>> | undefined;
    hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async (command) => {
            observations.push(`load:${command.runtimeInputId}`);
            const inspected = await hosts?.subAgentRunHost.inspectThread(command);
            observations.push(`observed:${inspected?.ok === true ? inspected.observed : "unavailable"}`);
            return {
              messages: [],
              runtimeBindingToken: "runtime-binding-token-config",
              coldCoverage: emptyColdCoverage,
            };
          },
        },
      }),
    });
    try {
      const result = await hosts.commandRunHost.handleRuntimeConfigPatch("sesn_cold_config", {
        ...commandScope("sesn_cold_config"),
        requestId: "req_config_cold",
        runtimeInputId: "rin_config_cold",
        generation: 6,
        payloadJson: "{\"config_generation\":6}",
      });
      const inspected = await hosts.subAgentRunHost.inspectThread(commandScope("sesn_cold_config"));

      expect(result).toEqual({
        ok: true,
        sessionId: "sesn_cold_config",
        created: false,
        applied: false,
        noResidency: true,
      });
      expect(observations).toEqual([]);
      expect(inspected).toMatchObject({ ok: true, observed: false });
    } finally {
      await hosts?.close();
    }
  });

  test("task notification cold-loads thread context before hot settlement", async () => {
    const observations: string[] = [];
    const committedMessage = bridgeRuntimeMessage("sesn_cold_task", "task completed");
    let hosts: Awaited<ReturnType<typeof buildRuntimeCoreHosts>> | undefined;
    hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async (command) => {
            observations.push(`load:${command.runtimeInputId}`);
            const inspected = await hosts?.subAgentRunHost.inspectThread(command);
            observations.push(`observed:${inspected?.ok === true ? inspected.observed : "unavailable"}`);
            return {
              messages: [committedMessage],
              runtimeBindingToken: "runtime-binding-token-task",
              coldCoverage: emptyColdCoverage,
            };
          },
        },
      }),
    });
    try {
      const result = await hosts.commandRunHost.handleTaskNotification("sesn_cold_task", {
        ...commandScope("sesn_cold_task"),
        requestId: "req_task_cold",
        runtimeInputId: "rin_task_cold",
        taskId: "task_1",
        sourceToolUseEventId: "sevt_tool_1",
        status: "completed",
        payloadJson: "{\"task_id\":\"task_1\",\"source_tool_use_event_id\":\"sevt_tool_1\",\"status\":\"completed\"}",
      }, async () => ({ ok: true, committedMessage }));
      const inspected = await hosts.subAgentRunHost.inspectThread(commandScope("sesn_cold_task"));

      expect(result).toEqual({ ok: true, sessionId: "sesn_cold_task", created: true, applied: true });
      expect(observations).toEqual(["load:rin_task_cold", "observed:true"]);
      expect(inspected).toMatchObject({ ok: true, observed: true, messages: [committedMessage] });
    } finally {
      await hosts?.close();
    }
  });
});

async function waitForCleanup(
  cleanupRunHost: Awaited<ReturnType<typeof buildRuntimeCoreHosts>>["cleanupRunHost"],
  sessionId: string,
) {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const result = await cleanupRunHost.handleCleanupSession(commandScope(sessionId));
    if (result.ok) {
      return result;
    }
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 5);
    });
  }
  return await cleanupRunHost.handleCleanupSession(commandScope(sessionId));
}

function commandScope(sessionId: string) {
  return {
    requestId: "req_1",
    workspaceId: "wksp_1",
    sessionId,
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 1,
    targetPodUid: "pod_1",
    runtimeInputId: "rin_cleanup",
    eventIds: [],
    sequenceFrom: 0,
    sequenceTo: 0,
  };
}

function acceptedInput(sessionId: string) {
  return {
    kind: "messages" as const,
    requestId: "req_1",
    workspaceId: "wksp_1",
    sessionId,
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 1,
    targetPodUid: "pod_1",
    runtimeInputId: "rin_1",
    eventIds: ["sevt_1"],
    sequenceFrom: 1,
    sequenceTo: 1,
    payloadJson: JSON.stringify({
      messages: [userMessage(sessionId, "msg_rin_1", 1, "test input")],
    }),
  };
}

function deferred<T>(): { readonly promise: Promise<T>; readonly resolve: (value: T) => void } {
  let resolve: (value: T) => void = () => undefined;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function testCoreDependencies(
  overrides: {
    readonly contextLoader?: Partial<RuntimeCoreHostsOptions["contextLoader"]>;
    readonly agentLoop?: Partial<RuntimeCoreHostsOptions["agentLoop"]>;
  } = {},
): Pick<RuntimeCoreHostsOptions, "contextLoader" | "agentLoop"> {
  return {
    contextLoader: {
      loadThreadContext: async () => ({
        messages: [],
        runtimeBindingToken: "runtime-binding-token-test",
        coldCoverage: emptyColdCoverage,
      }),
      commitAcceptedInput: async (input) => acceptedInputReceipt(input),
      ...overrides.contextLoader,
    },
    agentLoop: {
      internalToolRepairStore: new RecordingMessageStore(),
      sessionEventWriter: {
        append: async (envelope) => successfulEventAppend(envelope),
        writeRequestEnd: async (envelope) => ({
          ok: true,
          writeId: envelope.writeId,
          eventId: `evt_${envelope.writeId}`,
          processedAt: "2026-06-16T00:00:00.000Z",
        }),
        finishIdle: async (envelope) => ({
          ok: true,
          writeId: envelope.durableTurnId,
          eventId: `evt_${envelope.durableTurnId}`,
          processedAt: "2026-06-16T00:00:00.000Z",
        }),
      },
      runtime: {
        now: () => "2026-06-16T00:00:00.000Z",
        monotonicMs: () => 0,
        createId: (prefix) => `${prefix}_1`,
        sleep: async () => true,
      },
      llmService: {
        stream: () => Stream.empty,
      },
      storeOperationTimeoutMs: 100,
      runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
      runtimePolicy: () => ({ toolCatalog: createToolCatalog({ family: "claude" }) }),
      ...overrides.agentLoop,
    },
  };
}

class RecordingMessageStore extends RuntimeInternalToolRepairStore {
  protected async commitInternalToolRepairRecord(): Promise<never> {
    throw new Error("internal tool repair is not exercised by this host test");
  }
}

function successfulEventAppend(envelope: SessionEventEnvelope): SessionEventWriterAppendResult {
  const committedAt = "2026-06-16T00:00:00.000Z";
  const eventId = `evt_${envelope.writeId}`;
  return {
    ok: true,
    writeId: envelope.writeId,
    eventId,
    processedAt: committedAt,
    declaration: {
      applicationDisposition: "current_custody",
      observedBindingId: envelope.bindingId,
      observedBindingGeneration: envelope.bindingGeneration,
      receipt: {
        sessionThreadId: envelope.sessionThreadId,
        operationKind: "write_event",
        sourceKind: envelope.event.type,
        sourceId: envelope.writeId,
        declarationDigest: `digest_${envelope.writeId}`,
        pendingAttachmentDelta: [],
        pendingToolDelta: [],
        prefixConsumptions: [],

        childLifecycle: [],
        events: [{
          sessionThreadId: envelope.sessionThreadId,
          sourceEventId: envelope.writeId,
          eventId,
          eventSequence: 1,
          disposition: "created",
        }],
        messages: envelope.drafts.map((draft, messageIndex) => ({
          runtimeLocalId: draft.runtimeLocalId,
          sessionThreadId: envelope.sessionThreadId,
          owningEventId: eventId,
          messageId: `msg_${draft.runtimeLocalId}`,
          messageSequence: messageIndex + 1,
          createdAt: committedAt,
          updatedAt: committedAt,
          disposition: "created",
          parts: draft.parts.map((part, partIndex) => ({
            runtimeLocalPartId: part.runtimeLocalPartId,
            partId: `part_${part.runtimeLocalPartId}`,
            messageId: `msg_${draft.runtimeLocalId}`,
            partSequence: partIndex,
            createdAt: committedAt,
            updatedAt: committedAt,
            disposition: "created",
          })),
        })),
      },
    },
  };
}
