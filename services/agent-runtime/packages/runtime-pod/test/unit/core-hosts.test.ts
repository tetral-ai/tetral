import { describe, expect, test } from "bun:test";
import { status } from "@grpc/grpc-js";
import { Stream } from "effect";
import { RuntimeInternalToolRepairStore } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
  DurableRuntimeMessage,
  SessionEvent,
  SessionEventEnvelope,
  SessionEventWriterAppendResult,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { ThreadTurnLoadFacts } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import type { RuntimeControlInputDeclaration } from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import { createToolCatalog, lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import type { RuntimeCoreHostsOptions } from "../../src/core-hosts.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { runtimeModelForThread, runtimeToolPolicyForThread } from "../../src/command.js";
import {
  buildCoreHostsUserMessage as userMessage,
  buildCoreHostsAssistantRunningToolMessage as assistantRunningToolMessage,
  buildCoreHostsBridgeRuntimeMessage as bridgeRuntimeMessage,
  buildCoreHostsDurableBridgeRuntimeMessage as durableBridgeRuntimeMessage,
  buildRuntimeControlCommitResult,
} from "../../../core/test/unit/runtime-message-builders.js";
import { acceptedInputReceipt } from "../../../core/test/unit/runtime-declaration-fixtures.js";

const emptyColdCoverage = {
  pendingToolIds: [],
  pendingSandboxExecutionIds: [],
  pendingAttachmentIdentities: [],
  undeliveredMailDeliveryIds: [],
} as const;

const emptyTurnFacts: ThreadTurnLoadFacts = { events: [], messageLineage: [] };

function turnFactsFor(messages: readonly DurableRuntimeMessage[]): ThreadTurnLoadFacts {
  return {
    events: [],
    messageLineage: messages.map((message) => ({
      messageId: message.id,
      messageSequence: message.sequence,
      owningEventId: message.owningEventId,
      entries: [{
        lineageKind: "declaration_receipt",
        operationKind: message.origin === "agent" ? "write_event" : "commit_inputs",
        sourceKind: message.origin === "agent" ? "runtime_event" : message.origin === "runtime" ? "agent_mail" : "messages",
        eventId: message.owningEventId,
        eventSequence: message.eventSequence,
        disposition: "created",
      }],
    })),
  };
}

function turnFactsForPendingTool(input: {
  readonly messages: readonly DurableRuntimeMessage[];
  readonly modelRequestId: string;
  readonly toolUseEventId: string;
  readonly modelToolCallId: string;
  readonly toolName: string;
  readonly family: "agent.tool_use" | "agent.mcp_tool_use";
}): ThreadTurnLoadFacts {
  const toolMessage = input.messages.find((message) => message.owningEventId === input.toolUseEventId);
  if (toolMessage === undefined || toolMessage.eventSequence < 2) {
    throw new Error("pending Tool Use fixture requires an ordered assistant projection");
  }
  const startSequence = toolMessage.eventSequence - 1;
  return {
    events: [
      {
        eventId: `start_${input.modelRequestId}`,
        eventSequence: startSequence,
        type: "span.model_request_start",
        modelRequestId: input.modelRequestId,
        requestStart: {
          requestKind: "agent_provider_request",
          contextThroughMessageSequence: 1,
        },
      },
      {
        eventId: input.toolUseEventId,
        eventSequence: toolMessage.eventSequence,
        type: input.family,
        modelRequestId: input.modelRequestId,
        toolUse: {
          modelToolCallId: input.modelToolCallId,
          toolName: input.toolName,
        },
      },
      {
        eventId: `end_${input.modelRequestId}`,
        eventSequence: toolMessage.eventSequence + 1,
        type: "span.model_request_end",
        modelRequestId: input.modelRequestId,
        requestEnd: {
          requestStartEventId: `start_${input.modelRequestId}`,
          isError: false,
          rescheduled: false,
        },
      },
    ],
    messageLineage: turnFactsFor(input.messages).messageLineage.map((lineage) =>
      lineage.messageId === toolMessage.id
        ? {
            ...lineage,
            modelRequestId: input.modelRequestId,
            entries: lineage.entries.map((entry) => ({
              ...entry,
              sourceKind: input.family,
            })),
          }
        : lineage
    ),
  };
}

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
        eventIds: ["sevt_received_warm_push"],
        sequenceFrom: 2,
        sequenceTo: 2,
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

  test("cold installation resolves pending mail in sent order before the triggering input", async () => {
    const observations: string[] = [];
    const descriptors = [
      { deliveryId: "delivery_cold_1", sourceThreadId: "thrd_child_1", sourceToolUseEventId: "sevt_child_1" },
      { deliveryId: "delivery_cold_2", sourceThreadId: "thrd_child_2", sourceToolUseEventId: "sevt_child_2" },
    ] as const;
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async () => {
            observations.push("load");
            return {
              messages: [],
              turnFacts: emptyTurnFacts,
              thread: {
                role: "main",
                visibility: "public",
                agentType: "general",
                status: "idle",
              },
              runtimeBindingToken: "runtime-binding-token-cold-mail",
              pendingAgentMail: descriptors,
              coldCoverage: {
                ...emptyColdCoverage,
                undeliveredMailDeliveryIds: descriptors.map((mail) => mail.deliveryId),
              },
            };
          },
          resolveAgentMail: async (command, childThreadId, deliveryId) => {
            observations.push(`resolve:${deliveryId}`);
            const index = descriptors.findIndex((mail) =>
              mail.deliveryId === deliveryId && mail.sourceThreadId === childThreadId
            );
            if (index < 0) {
              throw new Error("unexpected cold agent-mail descriptor");
            }
            const descriptor = descriptors[index]!;
            return {
              ...descriptor,
              targetThreadId: command.sessionThreadId,
              receivedEventId: `sevt_received_${index + 1}`,
              receivedSequence: index + 10,
              message: bridgeRuntimeMessage(command.sessionId, `child result ${index + 1}`),
              publicMessageJson: publicAgentMailJSON(
                bridgeRuntimeMessage(command.sessionId, `child result ${index + 1}`),
              ),
            };
          },
          commitAcceptedInput: async (input) => {
            observations.push(`commit:${input.runtimeInputId}`);
            return acceptedInputReceipt(input);
          },
        },
      }),
    });
    try {
      expect(await hosts.commandRunHost.handleAcceptInput(acceptedInput("sesn_cold_mail"))).toMatchObject({
        ok: true,
        sessionId: "sesn_cold_mail",
      });
      expect(observations.slice(0, 3)).toEqual([
        "load",
        "resolve:delivery_cold_1",
        "resolve:delivery_cold_2",
      ]);
    } finally {
      await hosts.close();
    }
  });

  test("cold child installation resolves a direct instruction under its parent scope", async () => {
    const observations: string[] = [];
    const deliveryId = "delivery_cold_direct";
    const parentThreadId = "thrd_cold_direct_parent";
    const childThreadId = "thrd_1";
    const message = bridgeRuntimeMessage("sesn_cold_direct", "parent instruction");
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async () => ({
            messages: [],
            turnFacts: emptyTurnFacts,
            thread: {
              parentThreadId,
              role: "subagent",
              visibility: "public",
              taskName: "child",
              agentType: "general",
              status: "idle",
            },
            runtimeBindingToken: "runtime-binding-token-cold-direct",
            pendingAgentMail: [{
              deliveryId,
              sourceThreadId: parentThreadId,
              sourceToolUseEventId: "sevt_cold_direct_source",
            }],
            coldCoverage: {
              ...emptyColdCoverage,
              undeliveredMailDeliveryIds: [deliveryId],
            },
          }),
          resolveAgentMail: async (command, peerThreadId, requestedDeliveryId) => {
            observations.push(`resolve:${command.sessionThreadId}:${peerThreadId}:${requestedDeliveryId}`);
            if (
              command.sessionThreadId !== parentThreadId ||
              peerThreadId !== childThreadId ||
              requestedDeliveryId !== deliveryId
            ) {
              throw new Error("cold direct resolver orientation is invalid");
            }
            return {
              deliveryId,
              sourceThreadId: parentThreadId,
              sourceToolUseEventId: "sevt_cold_direct_source",
              targetThreadId: childThreadId,
              receivedEventId: "sevt_cold_direct_received",
              receivedSequence: 7,
              message,
              publicMessageJson: publicAgentMailJSON(message),
            };
          },
          commitAcceptedInput: async (input) => acceptedInputReceipt(input),
        },
      }),
    });
    try {
      expect(await hosts.commandRunHost.handleAcceptInput(acceptedInput("sesn_cold_direct"))).toMatchObject({
        ok: true,
        sessionId: "sesn_cold_direct",
      });
      expect(observations).toEqual([
        `resolve:${parentThreadId}:${childThreadId}:${deliveryId}`,
      ]);
    } finally {
      await hosts.close();
    }
  });

  test("cold mail fails before resolution when durable thread lineage is absent", async () => {
    let resolveCalls = 0;
    const message = bridgeRuntimeMessage("sesn_cold_mail_no_lineage", "parent instruction");
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async () => ({
            messages: [],
            turnFacts: emptyTurnFacts,
            runtimeBindingToken: "runtime-binding-token-cold-no-lineage",
            pendingAgentMail: [{
              deliveryId: "delivery_cold_no_lineage",
              sourceThreadId: "thrd_parent",
              sourceToolUseEventId: "sevt_cold_no_lineage_source",
            }],
            coldCoverage: {
              ...emptyColdCoverage,
              undeliveredMailDeliveryIds: ["delivery_cold_no_lineage"],
            },
          }),
          resolveAgentMail: async (command) => {
            resolveCalls += 1;
            return {
              deliveryId: "delivery_cold_no_lineage",
              sourceThreadId: "thrd_parent",
              sourceToolUseEventId: "sevt_cold_no_lineage_source",
              targetThreadId: command.sessionThreadId,
              receivedEventId: "sevt_cold_no_lineage_received",
              receivedSequence: 7,
              message,
              publicMessageJson: publicAgentMailJSON(message),
            };
          },
        },
      }),
    });
    try {
      await expect(
        hosts.commandRunHost.handleAcceptInput(acceptedInput("sesn_cold_mail_no_lineage")),
      ).resolves.toMatchObject({ ok: false });
      expect(resolveCalls).toBe(0);
    } finally {
      await hosts.close();
    }
  });

  test("cold agent-mail trigger starts the resolver-seeded input instead of ACKing it idle", async () => {
    const deliveryId = "delivery_cold_trigger";
    const sourceThreadId = "thrd_cold_trigger_child";
    const message = bridgeRuntimeMessage("sesn_cold_trigger", "stored completion");
    const committed = deferred<void>();
    const commits: string[] = [];
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async () => ({
            messages: [],
            turnFacts: emptyTurnFacts,
            thread: {
              role: "main",
              visibility: "public",
              agentType: "general",
              status: "idle",
            },
            runtimeBindingToken: "runtime-binding-token-cold-trigger",
            pendingAgentMail: [{
              deliveryId,
              sourceThreadId,
              sourceToolUseEventId: "sevt_cold_trigger_source",
            }],
            coldCoverage: {
              ...emptyColdCoverage,
              undeliveredMailDeliveryIds: [deliveryId],
            },
          }),
          resolveAgentMail: async (command) => ({
            deliveryId,
            sourceThreadId,
            sourceToolUseEventId: "sevt_cold_trigger_source",
            targetThreadId: command.sessionThreadId,
            receivedEventId: "sevt_cold_trigger_received",
            receivedSequence: 9,
            message,
            publicMessageJson: publicAgentMailJSON(message),
          }),
          commitAcceptedInput: async (input) => {
            commits.push(input.runtimeInputId);
            committed.resolve();
            return acceptedInputReceipt(input);
          },
        },
      }),
    });
    try {
      expect(await hosts.commandRunHost.handleAgentMail?.({
        ...commandScope("sesn_cold_trigger"),
        runtimeInputId: `agent_mail:${deliveryId}`,
        eventIds: ["sevt_cold_trigger_received"],
        sequenceFrom: 9,
        sequenceTo: 9,
        deliveryId,
        sourceThreadId,
        sourceToolUseEventId: "sevt_cold_trigger_source",
        message,
      })).toMatchObject({ ok: true, applied: true });
      await committed.promise;
      expect(commits).toEqual([`agent_mail:${deliveryId}`]);
    } finally {
      await hosts.close();
    }
  });

  test("completion pull resolves the stored envelope without loading context", async () => {
    const observations: string[] = [];
    const message = bridgeRuntimeMessage("sesn_pull", "stored completion");
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async () => {
            observations.push("load");
            return {
              messages: [],
              turnFacts: emptyTurnFacts,
              runtimeBindingToken: "runtime-binding-token-pull",
              coldCoverage: emptyColdCoverage,
            };
          },
          resolveAgentMail: async (command, childThreadId, deliveryId) => {
            observations.push(`resolve:${childThreadId}:${deliveryId ?? "oldest"}`);
            return {
              deliveryId: "delivery_pull",
              sourceThreadId: childThreadId,
              sourceToolUseEventId: "sevt_pull",
              targetThreadId: command.sessionThreadId,
              receivedEventId: "sevt_received_pull",
              receivedSequence: 12,
              message,
              publicMessageJson: publicAgentMailJSON(message),
            };
          },
        },
      }),
    });
    try {
      expect(await hosts.subAgentRunHost.pullAgentMail?.(commandScope("sesn_pull"), "thrd_child")).toEqual({
        deliveryId: "delivery_pull",
        finalMessage: "stored completion",
      });
      expect(observations).toEqual(["resolve:thrd_child:oldest"]);
    } finally {
      await hosts.close();
    }
  });

  test("completion pull returns no message when the durable resolver has no uncommitted mail", async () => {
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          resolveAgentMail: async () => {
            throw Object.assign(new Error("not found"), { code: status.NOT_FOUND });
          },
        },
      }),
    });
    try {
      await expect(
        hosts.subAgentRunHost.pullAgentMail?.(commandScope("sesn_pull_empty"), "thrd_child"),
      ).resolves.toBeUndefined();
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
              turnFacts: emptyTurnFacts,
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

  test("cold interrupt and sibling commands join one complete preload", async () => {
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
              turnFacts: emptyTurnFacts,
              runtimeBindingToken: "runtime-binding-token-singleflight",
              backgroundTools: [{ taskId: "task_singleflight", sourceToolUseEventId: "sevt_tool_singleflight" }],
              coldCoverage: emptyColdCoverage,
            };
          },
        },
      }),
    });
    try {
      let taskCommitCalls = 0;
      const scope = commandScope("sesn_singleflight");
      const interruptCommand = {
        ...scope,
        runtimeInputId: "rin_interrupt_singleflight",
        eventIds: ["sevt_interrupt_singleflight"],
        sequenceFrom: 1,
        sequenceTo: 1,
      };
      const interrupt = hosts.commandRunHost.handleInterruptControl(
        "sesn_singleflight",
        interruptCommand,
        async (declaration) => buildRuntimeControlCommitResult(interruptCommand, "interrupt_control", declaration),
      );
      await loadStarted.promise;
      const message = hosts.commandRunHost.handleAcceptInput({
        ...acceptedInput("sesn_singleflight"),
        runtimeInputId: "rin_message_singleflight",
      });
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
      }, async () => {
        taskCommitCalls += 1;
        return {
          ok: true,
          committedMessage: durableBridgeRuntimeMessage("sesn_singleflight", "task completed"),
        };
      });
      await Promise.resolve();
      expect(await hosts.subAgentRunHost.inspectThread(scope)).toMatchObject({ ok: true, observed: true });

      releaseLoad.resolve();
      const [interruptResult, messageResult, configResult, taskResult] = await Promise.all([interrupt, message, config, task]);
      expect(interruptResult).toEqual({ ok: true, sessionId: "sesn_singleflight", created: false, interrupted: false, idleInterrupt: true });
      expect(messageResult).toMatchObject({ ok: true, sessionId: "sesn_singleflight" });
      expect(configResult).toEqual({ ok: false, sessionId: "sesn_singleflight", reason: "control_busy" });
      expect(taskResult).toMatchObject({ ok: true, sessionId: "sesn_singleflight", applied: true });
      expect(loadCount).toBe(1);
      expect(taskCommitCalls).toBe(0);
      expect(await hosts.subAgentRunHost.inspectThread(scope)).toMatchObject({
        ok: true,
        observed: true,
        messages: [],
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
      userMessage("sesn_cold_confirm", "user-cold-confirm", 1, "hello"),
      {
        ...assistantRunningToolMessage(
          "sesn_cold_confirm",
          "assistant-cold-confirm",
          2,
          "tool-1",
          "github_search",
          "sevt_tool_1",
          pendingInput,
          { kind: "mcp", mcpServerName: "github" },
        ),
        eventSequence: 3,
      },
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
              turnFacts: turnFactsForPendingTool({
                messages: loadedMessages,
                modelRequestId: "mrq_cold_confirm",
                toolUseEventId: "sevt_tool_1",
                modelToolCallId: "tool-1",
                toolName: "github_search",
                family: "agent.mcp_tool_use",
              }),
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
                  input: pendingInput,
                  decision: "allow",
                  status: "resolving",
                },
              ],
              coldCoverage: {
                ...emptyColdCoverage,
                pendingToolIds: ["sevt_tool_1"],
              },
            };
          },
        },
        threadLoop: {
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
      const confirmationCommand = {
        ...replacementScope,
        requestId: "req_confirm_cold",
        runtimeInputId: "rin_confirm_cold",
        eventIds: ["sevt_confirm_cold"],
        sequenceFrom: 2,
        sequenceTo: 2,
        sourceEventId: "sevt_confirm_cold",
        toolUseEventId: "sevt_tool_1",
        decision: "allow",
      } as const;
      const result = await hosts.commandRunHost.handleToolConfirmation(
        "sesn_cold_confirm",
        confirmationCommand,
        async (declaration) => buildRuntimeControlCommitResult(confirmationCommand, "tool_confirmation", declaration),
      );

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

  test("cold interrupt installs pending tools before committing their cancellation", async () => {
    const observations: string[] = [];
    const pendingInput = { file_path: "src/a.ts", content: "ok" };
    const loadedMessages = [
      userMessage("sesn_interrupt_confirm", "user-interrupt-confirm", 1, "hello"),
      {
        ...assistantRunningToolMessage("sesn_interrupt_confirm", "assistant-interrupt-confirm", 2, "tool-1", "Write", "sevt_tool_1", pendingInput),
        eventSequence: 3,
      },
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
              turnFacts: turnFactsForPendingTool({
                messages: loadedMessages,
                modelRequestId: "mrq_interrupt_confirm",
                toolUseEventId: "sevt_tool_1",
                modelToolCallId: "tool-1",
                toolName: "Write",
                family: "agent.tool_use",
              }),
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
                  input: pendingInput,
                  decision: "allow",
                  status: "resolving",
                },
              ],
              coldCoverage: {
                ...emptyColdCoverage,
                pendingToolIds: ["sevt_tool_1"],
              },
            };
          },
        },
      }),
    });
    try {
      const interruptCommand = {
        ...commandScope("sesn_interrupt_confirm"),
        requestId: "req_interrupt_before_confirm",
        runtimeInputId: "rin_interrupt_before_confirm",
        eventIds: ["sevt_interrupt_before_confirm"],
        sequenceFrom: 2,
        sequenceTo: 2,
      };
      let committedDeclaration: RuntimeControlInputDeclaration | undefined;
      const interrupt = await hosts.commandRunHost.handleInterruptControl(
        "sesn_interrupt_confirm",
        interruptCommand,
        async (declaration) => {
          committedDeclaration = declaration;
          const result = buildRuntimeControlCommitResult(interruptCommand, "interrupt_control", declaration);
          if (!result.ok || !("receipt" in result)) return result;
          return {
            ...result,
            receipt: {
              ...result.receipt,
              interruptToolProjections: [{
                toolUseEventId: "sevt_tool_1",
                resultEvent: {
                  sessionThreadId: interruptCommand.sessionThreadId,
                  eventId: "sevt_interrupt_tool_result",
                  eventSequence: 5,
                  disposition: "created" as const,
                },
                terminalState: { type: "cancelled" as const },
              }],
            },
          };
        },
      );
      const shell = await hosts.subAgentRunHost.inspectThread(commandScope("sesn_interrupt_confirm"));

      expect(interrupt).toEqual({ ok: true, sessionId: "sesn_interrupt_confirm", created: false, interrupted: false, idleInterrupt: true });
      expect(observations).toEqual(["load:rin_interrupt_before_confirm"]);
		expect(committedDeclaration).toMatchObject({
			messageCreates: [],
      });
      expect(shell).toMatchObject({
        ok: true,
        observed: true,
        messages: expect.arrayContaining([
          expect.objectContaining({
            parts: [expect.objectContaining({
              type: "tool",
              toolUseEventId: "sevt_tool_1",
              state: expect.objectContaining({ status: "cancelled" }),
            })],
          }),
        ]),
      });
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
              turnFacts: emptyTurnFacts,
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

  test("cold installation rejects conflicting durable run identities", async () => {
    let appendCalls = 0;
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async () => ({
            messages: [],
            durableTurnId: "sevt_running_sibling",
            turnFacts: {
              events: [{
                eventId: "sevt_running_checkpoint",
                eventSequence: 1,
                type: "session.status_running",
              }],
              messageLineage: [],
            },
            runtimeBindingToken: "runtime-binding-token-conflicting-run",
            coldCoverage: emptyColdCoverage,
          }),
        },
        threadLoop: {
          sessionEventWriter: {
            append: async (envelope) => {
              appendCalls += 1;
              return successfulEventAppend(envelope);
            },
            writeRequestEnd: async (envelope) => ({
              ok: true,
              writeId: envelope.writeId,
              eventId: `evt_${envelope.writeId}`,
              processedAt: "2026-06-16T00:00:00.000Z",
            }),
          },
        },
      }),
    });
    try {
      expect(await hosts.subAgentRunHost.preloadThread(commandScope("sesn_conflicting_run"))).toMatchObject({
        ok: false,
        sessionId: "sesn_conflicting_run",
        reason: "context_load_failed",
      });
      expect(appendCalls).toBe(0);
      expect(await hosts.subAgentRunHost.inspectThread(commandScope("sesn_conflicting_run"))).toMatchObject({
        ok: true,
        observed: false,
      });
    } finally {
      await hosts.close();
    }
  });

  test("cold Request Start without Request End remains passive until durable repair", async () => {
    let providerCalls = 0;
    let appendCalls = 0;
    const hosts = await buildRuntimeCoreHosts({
      maxLocalSessions: 4,
      now: () => "2026-06-16T00:00:00.000Z",
      ...testCoreDependencies({
        contextLoader: {
          loadThreadContext: async () => ({
            messages: [],
            durableTurnId: "sevt_open_running",
            turnFacts: {
              events: [
                { eventId: "sevt_open_running", eventSequence: 1, type: "session.status_running" },
                {
                  eventId: "sevt_open_start",
                  eventSequence: 2,
                  type: "span.model_request_start",
                  modelRequestId: "mreq_open_cold",
                  requestStart: {
                    requestKind: "agent_provider_request",
                    contextThroughMessageSequence: 0,
                  },
                },
              ],
              messageLineage: [],
            },
            runtimeBindingToken: "runtime-binding-token-open-cold",
            coldCoverage: emptyColdCoverage,
          }),
        },
        threadLoop: {
          llmService: {
            stream: () => {
              providerCalls += 1;
              return Stream.empty;
            },
          },
          sessionEventWriter: {
            append: async (envelope) => {
              appendCalls += 1;
              return successfulEventAppend(envelope);
            },
            writeRequestEnd: async (envelope) => ({
              ok: true,
              writeId: envelope.writeId,
              eventId: `evt_${envelope.writeId}`,
              processedAt: "2026-06-16T00:00:00.000Z",
            }),
          },
        },
      }),
    });
    try {
      expect(await hosts.subAgentRunHost.preloadThread(commandScope("sesn_open_cold"))).toMatchObject({
        ok: true,
        applied: true,
      });
      await Promise.resolve();
      expect(providerCalls).toBe(0);
      expect(appendCalls).toBe(0);
    } finally {
      await hosts.close();
    }
  });

  test("task notification cold-loads thread context before hot settlement", async () => {
    const observations: string[] = [];
    const committedMessage = durableBridgeRuntimeMessage("sesn_cold_task", "task completed");
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
              turnFacts: turnFactsFor([committedMessage]),
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
		origin: "user" as const,
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

function publicAgentMailJSON(message: ReturnType<typeof bridgeRuntimeMessage>): string {
  return JSON.stringify({
    ...message,
    content: message.parts.map((part) => {
      if (part.type !== "text") {
        throw new Error("agent-mail fixture requires text parts");
      }
      return { type: "text", text: part.text };
    }),
  });
}

function testCoreDependencies(
  overrides: {
    readonly contextLoader?: Partial<RuntimeCoreHostsOptions["contextLoader"]>;
    readonly threadLoop?: Partial<RuntimeCoreHostsOptions["threadLoop"]>;
  } = {},
): Pick<RuntimeCoreHostsOptions, "contextLoader" | "threadLoop"> {
  return {
    contextLoader: {
      loadThreadContext: async () => ({
        messages: [],
        turnFacts: emptyTurnFacts,
        runtimeBindingToken: "runtime-binding-token-test",
        coldCoverage: emptyColdCoverage,
      }),
      commitAcceptedInput: async (input) => acceptedInputReceipt(input),
      ...overrides.contextLoader,
    },
    threadLoop: {
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
      ...overrides.threadLoop,
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
        operationId: envelope.writeId,
        declarationDigest: `digest_${envelope.writeId}`,
        pendingAttachmentDelta: [],
		interruptToolProjections: [],
        prefixConsumptions: [],

        childLifecycle: [],
        events: [{
          sessionThreadId: envelope.sessionThreadId,
          eventId,
          eventSequence: 1,
          disposition: "created",
        }],
		messages: envelope.assistantPartAppend === undefined ? [] : [{
          sessionThreadId: envelope.sessionThreadId,
          owningEventId: eventId,
			messageId: `msg_${envelope.writeId}`,
			messageSequence: 1,
          createdAt: committedAt,
          updatedAt: committedAt,
          disposition: "created",
			parts: envelope.assistantPartAppend.parts.map((_part, partIndex) => ({
			partId: `part_${envelope.writeId}_${partIndex}`,
			messageId: `msg_${envelope.writeId}`,
            partSequence: partIndex,
            createdAt: committedAt,
            updatedAt: committedAt,
            disposition: "created",
          })),
		}],
      },
    },
  };
}
