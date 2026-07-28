import { describe, expect, test } from "bun:test";
import { Stream } from "effect";
import { RuntimeMessageStore } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
  RuntimeMessageInfo,
  RuntimeMessageStoreOperationControls,
  RuntimePart,
  SessionEvent,
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

describe("Runtime core host production assembly", () => {
  test("SessionRunHost and SessionManager keep command wakeup and cleanup local to Runtime Pod hot state", async () => {
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies(),
    });
    try {
      const accepted = await hosts.commandRunHost.handleAcceptInput(acceptedInput("sesn_1"));
      expect(accepted).toMatchObject({ ok: true, sessionId: "sesn_1" });

      const cleanup = await waitForCleanup(hosts.cleanupRunHost, "sesn_1");
      expect(cleanup).toEqual({ ok: true, sessionId: "sesn_1", cleaned: true });
    } finally {
      await hosts.close();
    }
  });

  test("concurrent cold completion pulls both return the same durable envelope", async () => {
    let commitAttempts = 0;
    let durableReceipt = false;
    let durableReceiptWrites = 0;
    const loadFilters: Array<string | undefined> = [];
    const sessionId = "sesn_cold_pull_race";
    const childId = "thrd_cold_pull_child";
    const message = bridgeRuntimeMessage(
      sessionId,
      "Message Type: FINAL_ANSWER\nTask name: main\nSender: child\nPayload:\nchild result",
    );
    const mail = {
      deliveryId: "delivery_cold_pull",
      sourceThreadId: childId,
      sourceToolUseEventId: "sevt_cold_pull_spawn",
      message,
    };
    const context = {
      messages: [],
      runtimeBindingToken: "runtime-binding-token-pull",
    };
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async (_command, options) => {
            loadFilters.push(options?.agentMailSourceThreadId);
            return {
              ...context,
              pendingAgentMail: options?.agentMailSourceThreadId !== undefined || !durableReceipt ? [mail] : [],
            };
          },
          commitAcceptedInput: async (_input, options) => {
            commitAttempts += 1;
            expect(options).toEqual({ registerScope: false, hydrateContext: false });
            const inputDisposition = durableReceipt ? "duplicate" as const : "committed" as const;
            if (!durableReceipt) {
              durableReceipt = true;
              durableReceiptWrites += 1;
            }
            return {
              type: "receipt" as const,
              inputDisposition,
            };
          },
        },
      }),
    });
    try {
      const scope = commandScope(sessionId);
      const [first, second] = await Promise.all([
        hosts.subAgentRunHost.pullAgentMail?.(scope, childId),
        hosts.subAgentRunHost.pullAgentMail?.(scope, childId),
      ]);
      expect([first, second]).toEqual([
        { deliveryId: "delivery_cold_pull", finalMessage: "Message Type: FINAL_ANSWER\nTask name: main\nSender: child\nPayload:\nchild result" },
        { deliveryId: "delivery_cold_pull", finalMessage: "Message Type: FINAL_ANSWER\nTask name: main\nSender: child\nPayload:\nchild result" },
      ]);
      expect(commitAttempts).toBe(2);
      expect(durableReceiptWrites).toBe(1);

      expect(await hosts.commandRunHost.handleAgentMail?.({
        ...scope,
        runtimeInputId: "agent_mail:delivery_cold_pull",
      })).toEqual({ ok: true, sessionId, applied: false });
      expect(loadFilters).toEqual([childId, childId, undefined]);
      expect(commitAttempts).toBe(2);
    } finally {
      await hosts.close();
    }
  });

  test("pull retry re-reads and re-presents after the durable receipt response is lost", async () => {
    let commitAttempts = 0;
    let durableReceipt = false;
    let durableReceiptWrites = 0;
    const loadFilters: Array<string | undefined> = [];
    const sessionId = "sesn_cold_pull_response_lost";
    const childId = "thrd_cold_pull_response_lost_child";
    const finalMessage = "Message Type: FINAL_ANSWER\nTask name: main\nSender: child\nPayload:\nresponse survived";
    const mail = {
      deliveryId: "delivery_cold_pull_response_lost",
      sourceThreadId: childId,
      sourceToolUseEventId: "sevt_cold_pull_response_lost_spawn",
      message: bridgeRuntimeMessage(sessionId, finalMessage),
    };
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async (_command, options) => {
            loadFilters.push(options?.agentMailSourceThreadId);
            return {
              messages: [],
              runtimeBindingToken: "runtime-binding-token-pull-response-lost",
              pendingAgentMail: options?.agentMailSourceThreadId === childId || !durableReceipt ? [mail] : [],
            };
          },
          commitAcceptedInput: async (_input, options) => {
            commitAttempts += 1;
            expect(options).toEqual({ registerScope: false, hydrateContext: false });
            if (!durableReceipt) {
              durableReceipt = true;
              durableReceiptWrites += 1;
              throw new Error("receipt response lost");
            }
            return {
              type: "receipt" as const,
              inputDisposition: "duplicate" as const,
            };
          },
        },
      }),
    });
    try {
      const scope = commandScope(sessionId);
      await expect(hosts.subAgentRunHost.pullAgentMail?.(scope, childId)).rejects.toThrow("receipt response lost");

      expect(await hosts.subAgentRunHost.pullAgentMail?.(scope, childId)).toEqual({
        deliveryId: "delivery_cold_pull_response_lost",
        finalMessage,
      });
      expect(loadFilters).toEqual([childId, childId]);
      expect(commitAttempts).toBe(2);
      expect(durableReceiptWrites).toBe(1);
    } finally {
      await hosts.close();
    }
  });

  test("cold resume retry installs pending completion mail while an ordinary cold child stays idle", async () => {
    const sessionId = "sesn_resume_cold_mail";
    const parentId = "thrd_resume_cold_parent";
    const childId = "thrd_resume_cold_child";
    const idleChildId = "thrd_resume_cold_idle_child";
    let childLoadAttempts = 0;
    const mail = {
      deliveryId: "delivery_resume_cold_mail",
      sourceThreadId: "thrd_resume_cold_grandchild",
      sourceToolUseEventId: "sevt_resume_cold_spawn",
      message: bridgeRuntimeMessage(
        sessionId,
        "Message Type: FINAL_ANSWER\nTask name: child\nSender: grandchild\nPayload:\nfinished",
      ),
    };
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async (command) => {
            if (command.sessionThreadId === childId) {
              childLoadAttempts += 1;
              if (childLoadAttempts === 1) {
                throw new Error("injected first resume preload failure");
              }
              return {
                messages: [],
                runtimeBindingToken: "runtime-binding-token-resume-mail",
                pendingAgentMail: [mail],
              };
            }
            return {
              messages: [],
              runtimeBindingToken: "runtime-binding-token-resume-idle",
              pendingAgentMail: [],
            };
          },
        },
      }),
    });
    const preload = (sessionThreadId: string) => hosts.subAgentRunHost.preloadThread({
      ...commandScope(sessionId),
      sessionThreadId,
      runtimeInputId: `rin_resume_${sessionThreadId}`,
      thread: {
        parentThreadId: parentId,
        role: "subagent" as const,
        visibility: "public" as const,
        taskName: sessionThreadId,
        agentType: "general",
        status: "idle" as const,
      },
    });
    try {
      await expect(preload(childId)).rejects.toThrow("injected first resume preload failure");
      expect(await hosts.subAgentRunHost.inspectThread({
        ...commandScope(sessionId),
        sessionThreadId: childId,
      })).toMatchObject({ ok: true, observed: false });

      expect(await preload(childId)).toMatchObject({ ok: true, applied: true });
      expect(await hosts.subAgentRunHost.inspectThread({
        ...commandScope(sessionId),
        sessionThreadId: childId,
      })).toMatchObject({ ok: true, observed: true, status: "running" });

      expect(await preload(idleChildId)).toMatchObject({ ok: true, applied: true });
      await new Promise<void>((resolve) => setTimeout(resolve, 5));
      expect(await hosts.subAgentRunHost.inspectThread({
        ...commandScope(sessionId),
        sessionThreadId: idleChildId,
      })).toMatchObject({ ok: true, observed: true, status: "idle" });
    } finally {
      await hosts.close();
    }
  });

  test("mail-head commit failure releases hot state and cold preload redelivers the same delivery", async () => {
    const sessionId = "sesn_mail_commit_failure";
    const threadId = "thrd_mail_commit_failure_main";
    let commitAttempts = 0;
    const mail = {
      deliveryId: "delivery_mail_commit_failure",
      sourceThreadId: "thrd_mail_commit_failure_child",
      sourceToolUseEventId: "sevt_mail_commit_failure_spawn",
      message: bridgeRuntimeMessage(
        sessionId,
        "Message Type: FINAL_ANSWER\nTask name: main\nSender: child\nPayload:\nretry me",
      ),
    };
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async () => ({
            messages: [],
            runtimeBindingToken: "runtime-binding-token-mail-failure",
            pendingAgentMail: [mail],
          }),
          commitAcceptedInput: async (input) => {
            commitAttempts += 1;
            if (commitAttempts === 1) {
              throw new Error("injected mail receipt persistence failure");
            }
            return {
              type: "context" as const,
              messages: [input.kind === "inter_agent_message" ? input.message : mail.message],
              runtimeBindingToken: "runtime-binding-token-mail-failure",
              inputDisposition: "committed" as const,
            };
          },
        },
      }),
    });
    const preload = () => hosts.subAgentRunHost.preloadThread({
      ...commandScope(sessionId),
      sessionThreadId: threadId,
      runtimeInputId: `rin_mail_commit_failure_${commitAttempts}`,
      thread: {
        role: "main" as const,
        visibility: "public" as const,
        agentType: "general",
        status: "idle" as const,
      },
    });
    try {
      expect(await preload()).toMatchObject({ ok: true, applied: true });
      for (let attempt = 0; attempt < 100; attempt += 1) {
        const inspected = await hosts.subAgentRunHost.inspectThread({
          ...commandScope(sessionId),
          sessionThreadId: threadId,
        });
        if (inspected.ok && !inspected.observed) {
          break;
        }
        await new Promise<void>((resolve) => setTimeout(resolve, 1));
      }
      expect(await hosts.subAgentRunHost.inspectThread({
        ...commandScope(sessionId),
        sessionThreadId: threadId,
      })).toMatchObject({ ok: true, observed: false });
      expect(commitAttempts).toBe(1);

      expect(await preload()).toMatchObject({ ok: true, applied: true });
      for (let attempt = 0; attempt < 100 && commitAttempts < 2; attempt += 1) {
        await new Promise<void>((resolve) => setTimeout(resolve, 1));
      }
      expect(commitAttempts).toBe(2);
      expect(await hosts.subAgentRunHost.inspectThread({
        ...commandScope(sessionId),
        sessionThreadId: threadId,
      })).toMatchObject({ ok: true, observed: true });
    } finally {
      await hosts.close();
    }
  });

  test("agent mail retries when its inspected hot thread is released during context load", async () => {
    const sessionId = "sesn_agent_mail_release_race";
    const childId = "thrd_agent_mail_release_child";
    let loadCalls = 0;
    let continueAgentMailLoad: (() => void) | undefined;
    let markAgentMailLoadStarted: (() => void) | undefined;
    const agentMailLoadStarted = new Promise<void>((resolve) => {
      markAgentMailLoadStarted = resolve;
    });
    const context = {
      messages: [],
      runtimeBindingToken: "runtime-binding-token-release-race",
      pendingAgentMail: [{
        deliveryId: "delivery_agent_mail_release_race",
        sourceThreadId: childId,
        sourceToolUseEventId: "sevt_agent_mail_release_spawn",
        message: bridgeRuntimeMessage(
          sessionId,
          "Message Type: FINAL_ANSWER\nTask name: main\nSender: child\nPayload:\nchild result",
        ),
      }],
    };
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async () => {
            loadCalls += 1;
            if (loadCalls === 1) {
              return { messages: [], runtimeBindingToken: context.runtimeBindingToken };
            }
            markAgentMailLoadStarted?.();
            await new Promise<void>((resolve) => {
              continueAgentMailLoad = resolve;
            });
            return context;
          },
        },
      }),
    });
    try {
      expect(await hosts.commandRunHost.handleAcceptInput(acceptedInput(sessionId))).toMatchObject({
        ok: true,
        sessionId,
      });

      const mailDelivery = hosts.commandRunHost.handleAgentMail?.({
        ...commandScope(sessionId),
        runtimeInputId: "agent_mail:delivery_agent_mail_release_race",
      });
      await agentMailLoadStarted;
      expect(await waitForCleanup(hosts.cleanupRunHost, sessionId)).toEqual({
        ok: true,
        sessionId,
        cleaned: true,
      });
      continueAgentMailLoad?.();

      expect(await mailDelivery).toEqual({
        ok: false,
        sessionId,
        reason: "context_load_failed",
      });
      expect(await hosts.subAgentRunHost.inspectThread(commandScope(sessionId))).toMatchObject({
        ok: true,
        observed: false,
      });
    } finally {
      continueAgentMailLoad?.();
      await hosts.close();
    }
  });

  test("message command cold-load completes before ThreadEntry is inserted into hot state", async () => {
    const observations: string[] = [];
    let hosts: Awaited<ReturnType<typeof buildRuntimeCoreHosts>> | undefined;
    hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async (command) => {
            observations.push("loadThreadContext");
            const inspected = await hosts?.subAgentRunHost.inspectThread(command);
            observations.push(`observed:${inspected?.ok === true ? inspected.observed : "unavailable"}`);
            return {
              messages: [],
              runtimeBindingToken: "runtime-binding-token-cold",
            };
          },
        },
      }),
    });
    try {
      const accepted = await hosts.commandRunHost.handleAcceptInput(acceptedInput("sesn_cold"));
      const inspected = await hosts.subAgentRunHost.inspectThread(commandScope("sesn_cold"));

      expect(accepted).toMatchObject({ ok: true, sessionId: "sesn_cold", created: false });
      expect(observations).toEqual(["loadThreadContext", "observed:false"]);
      expect(inspected).toMatchObject({ ok: true, sessionId: "sesn_cold", sessionThreadId: "thrd_1", observed: true });
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
      });
      expect(interrupt).toEqual({ ok: true, sessionId: "sesn_singleflight", created: false, interrupted: false, idleInterrupt: true });
      expect(await hosts.subAgentRunHost.inspectThread(scope)).toMatchObject({ ok: true, observed: false });

      const message = hosts.commandRunHost.handleAcceptInput({
        ...acceptedInput("sesn_singleflight"),
        runtimeInputId: "rin_message_singleflight",
      });
      await loadStarted.promise;
      const config = hosts.commandRunHost.handleRuntimeConfigPatch("sesn_singleflight", {
        ...scope,
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
        bridgeProjection: bridgeRuntimeMessage("sesn_singleflight", "task completed"),
      });
      await Promise.resolve();
      expect(await hosts.subAgentRunHost.inspectThread(scope)).toMatchObject({ ok: true, observed: false });

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
    const {
      providerId: _providerId,
      modelId: _modelId,
      ...loadedMessage
    } = userMessage("sesn_cold_confirm", "user-cold-confirm", 0, "hello", "receipt-only", "receipt-only");
    const loadedMessages = [loadedMessage];
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
            };
          },
        },
        agentLoop: {
          providerCallRuntime: {
            systemInstructions: "cold confirmation system",
          },
          runtimeModel: (session) => runtimeModelForThread(
            session.identity.threadRole,
            session.state.runtimeConfigPatches().map((patch) => patch.payloadJson),
            { providerId: "anthropic", modelId: "claude-opus-4-8" },
          ),
          runtimePolicy: (session) => {
            const policy = runtimeToolPolicyForThread(
              session.identity.threadRole,
              session.state.runtimeConfigPatches().map((patch) => patch.payloadJson),
              session.state.installedBuiltinFamily(),
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
              return { ok: true, writeId: envelope.writeId, eventId: `evt_${envelope.writeId}`, processedAt: "2026-06-16T00:00:00.000Z" };
            },
            writeRequestEnd: async (envelope) => ({ ok: true, writeId: envelope.writeId, eventId: `evt_${envelope.writeId}`, processedAt: "2026-06-16T00:00:00.000Z" }),
            finishIdle: async (envelope) => ({ ok: true, writeId: envelope.writeId, eventId: `evt_${envelope.writeId}`, processedAt: "2026-06-16T00:00:00.000Z" }),
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
      });

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
      userMessage("sesn_interrupt_confirm", "user-interrupt-confirm", 0, "hello", "fake", "fake-chat"),
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
      });

      expect(result).toMatchObject({ ok: true, sessionId: "sesn_interrupt_confirm", applied: false });
      expect(observations).toEqual(["load:rin_confirm_after_interrupt"]);
      await toolExecuted.promise;
      expect(runToolCalls).toEqual(["mrq_interrupt_confirm:tool-1:sevt_tool_1"]);
    } finally {
      await hosts.close();
    }
  });

  test("runtime config command cold-loads before acknowledging hot install", async () => {
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
            };
          },
          refreshRuntimeBindingToken: async (identity, options) => {
            observations.push(`refresh:${identity.sessionThreadId}:${options?.force === true}`);
            return "runtime-binding-token-after-config";
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

      expect(result).toEqual({ ok: true, sessionId: "sesn_cold_config", created: false, applied: true });
      expect(observations).toEqual(["load:rin_config_cold", "observed:false", "refresh:thrd_1:true"]);
      expect(inspected).toMatchObject({ ok: true, observed: true });
    } finally {
      await hosts?.close();
    }
  });

  test("task notification cold-loads thread context before hot settlement", async () => {
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
              runtimeBindingToken: "runtime-binding-token-task",
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
        bridgeProjection: bridgeRuntimeMessage("sesn_cold_task", "task completed"),
      });
      const inspected = await hosts.subAgentRunHost.inspectThread(commandScope("sesn_cold_task"));

      expect(result).toEqual({ ok: true, sessionId: "sesn_cold_task", created: false, applied: true });
      expect(observations).toEqual(["load:rin_task_cold", "observed:false"]);
      expect(inspected).toMatchObject({ ok: true, observed: true });
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
    payloadJson: "{}",
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
      buildContext: async () => [],
      loadPendingInput: async () => ({ type: "empty" }),
      loadThreadContext: async () => ({
        messages: [],
        runtimeBindingToken: "runtime-binding-token-test",
      }),
      ...overrides.contextLoader,
    },
    agentLoop: {
      messageStore: new RecordingMessageStore(),
      sessionEventWriter: {
        append: async (envelope) => ({
          ok: true,
          writeId: envelope.writeId,
          eventId: `evt_${envelope.writeId}`,
          processedAt: "2026-06-16T00:00:00.000Z",
        }),
        writeRequestEnd: async (envelope) => ({
          ok: true,
          writeId: envelope.writeId,
          eventId: `evt_${envelope.writeId}`,
          processedAt: "2026-06-16T00:00:00.000Z",
        }),
        finishIdle: async (envelope) => ({
          ok: true,
          writeId: envelope.writeId,
          eventId: `evt_${envelope.writeId}`,
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

class RecordingMessageStore extends RuntimeMessageStore {
  protected async writeMessageRecord(message: RuntimeMessageInfo, _controls: RuntimeMessageStoreOperationControls): Promise<unknown> {
    return { ok: true, messageId: message.id, operation: "writeMessage" };
  }

  protected async writePartRecord(part: RuntimePart, _controls: RuntimeMessageStoreOperationControls): Promise<unknown> {
    return { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" };
  }
}
