import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { Metadata, status } from "@grpc/grpc-js";
import type { CallOptions } from "@grpc/grpc-js";
import {
  BridgeWriteStatus,
  ChildLifecycleDisposition,
  DurableEventDisposition,
  DurableProjectionDisposition,
  ReceiptApplicationDisposition,
  RequestRescheduleDisposition,
  RuntimeDraftKind,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  AgentRuntimeBridgeServiceClient,
  CommitInputsRequest,
  CommitInternalToolRepairRequest,
  CommitRuntimeTerminationRequest,
  CreateChildThreadRequest,
  FinishIdleRequest,
  LoadContextRequest,
  MarkChildThreadClosedRequest,
  RefreshRuntimeBindingTokenRequest,
  ResolveInterAgentDeliveryRequest,
  WriteEventRequest,
  WriteRequestEndRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { RuntimeApprovalReviewRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { ProviderMetadataSchema } from "@tetral/agent-runtime-core/src/contracts/provider.js";
import {
  RuntimeMessageDraftSchema,
  RuntimeMessageSchema,
  SessionEventWriterRetryPolicy,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
  RuntimeMessage,
  RuntimeMessageDraft,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { AutoApprovalReviewerManager } from "@tetral/agent-runtime-core/src/session/approval-reviewer-manager.js";
import { stableRuntimeID } from "@tetral/agent-runtime-core/src/runtime/runtime-identity.js";
import type { RuntimeAcceptedInputState, RuntimeThreadControlState } from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import type { RuntimeThreadIdentity } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import {
  acceptedInputDrafts,
  runtimeOutputDraft,
  runtimeWorkingDraftFromDurable,
} from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import { BridgeAPIApprovalReviewerThreadCreator, BridgeAPIContextLoader, BridgeAPIControlInputCommitter, BridgeAPIEventWriter, BridgeAPIInternalToolRepairCommitter } from "../../src/bridge-client.js";
import {
  childLifecycleDeclarationDigest,
  commitInputsDeclarationDigest,
  finishIdleDeclarationDigest,
  internalToolRepairDeclarationDigest,
  runtimeTerminationDeclarationDigest,
  writeEventDeclarationDigest,
  writeRequestEndDeclarationDigest,
} from "../../src/runtime-declaration-wire.js";
import {
  buildBridgeClientDurableRuntimeMessage as durableRuntimeMessage,
  buildBridgeClientRuntimeMessage as runtimeMessage,
  buildBridgeClientRuntimeRepairMessage as runtimeRepairMessage,
} from "../../../core/test/unit/runtime-message-builders.js";
import { createJsonLogger } from "../../src/logger.js";
import { RuntimePodMetricsRegistry } from "../../src/metrics.js";

describe("BridgeAPIContextLoader", () => {
  test("rejects missing message arrays and accepts an explicit empty context", async () => {
    for (const contextJson of ["", "{}", JSON.stringify({ messages: "not-an-array" })]) {
      const bridge = new RecordingBridgeClient();
      bridge.loadContextJSON = contextJson;
      const loader = new BridgeAPIContextLoader({
        address: "bridge.test:9090",
        tokenPath: "/var/run/token",
        client: bridge.client(),
        metadataFactory: async () => new Metadata(),
      });

      await expect(loader.loadThreadContext(control("thr_main", "rin_malformed_context", 1))).rejects.toMatchObject({
        type: "context-loader",
        code: "schema_mismatch",
        fatal: true,
      });
    }

    const bridge = new RecordingBridgeClient();
    bridge.loadContextJSON = JSON.stringify({
      messages: [],
      turnFacts: { events: [], messageLineage: [] },
      coldCoverage: {
        pendingToolIds: [],
        pendingSandboxExecutionIds: [],
        pendingAttachmentIdentities: [],
        undeliveredMailDeliveryIds: [],
      },
      thread: {
        parentThreadId: null,
        role: "main",
        visibility: "public",
        taskName: null,
        agentType: "general",
        status: "idle",
      },
    });
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    await expect(loader.loadThreadContext(control("thr_main", "rin_empty_context", 1))).resolves.toMatchObject({ messages: [] });
  });

  test("refreshes binding tokens on expiry margin with per-thread single-flight", async () => {
    const bridge = new RecordingBridgeClient();
    let releaseMetadata: (() => void) | undefined;
    const metadataReady = new Promise<void>((resolve) => {
      releaseMetadata = resolve;
    });
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      nowEpochMs: () => 1_000_000,
      refreshMarginMs: 60_000,
      metadataFactory: async () => {
        await metadataReady;
        return new Metadata();
      },
    });
    const farIdentity = bindingIdentity(jwtWithExpiry(2_000));
    await expect(loader.refreshRuntimeBindingToken(farIdentity)).resolves.toBe(farIdentity.runtimeBindingToken);
    expect(bridge.refreshRuntimeBindingTokenRequests).toEqual([]);

    const nearIdentity = bindingIdentity(jwtWithExpiry(1_050));
    const first = loader.refreshRuntimeBindingToken(nearIdentity);
    const second = loader.refreshRuntimeBindingToken(nearIdentity);
    releaseMetadata?.();

    await expect(Promise.all([first, second])).resolves.toEqual(["runtime-binding-token-refreshed", "runtime-binding-token-refreshed"]);
    expect(bridge.refreshRuntimeBindingTokenRequests).toHaveLength(1);
    expect(bridge.refreshRuntimeBindingTokenRequests[0]?.scope).toMatchObject({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      binding: { bindingId: "bind_1", bindingGeneration: 7, targetPodUid: "pod_1" },
    });
  });

  test("classifies a stale-generation refresh as lost Runtime custody", async () => {
    const bridge = new RecordingBridgeClient();
    bridge.refreshErrors.push(Object.assign(new Error("stale binding"), { code: status.FAILED_PRECONDITION }));
    const sleeps: number[] = [];
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      sleep: async (durationMs) => {
        sleeps.push(durationMs);
      },
    });

    await expect(loader.refreshRuntimeBindingToken(bindingIdentity("opaque-token"), { force: true })).rejects.toMatchObject({
      type: "context-loader",
      code: "superseded",
      retryable: false,
    });
    expect(bridge.refreshRuntimeBindingTokenRequests).toHaveLength(1);
    expect(sleeps).toEqual([]);
  });

  test("resolves the exact public agent-mail envelope and rejects derived-content drift", async () => {
    const bridge = new RecordingBridgeClient();
    const storedMessage = runtimeMessage("msg_agent_mail_resolved", "stored completion");
    bridge.resolveInterAgentDeliveryMessageJSON = JSON.stringify({
      ...storedMessage,
      content: [{ type: "text", text: "stored completion" }],
    });
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    await expect(loader.resolveAgentMail(
      control("thr_parent", "rin_agent_mail_resolve", 4),
      "thr_child",
      "delivery_resolved",
    )).resolves.toEqual({
      deliveryId: "delivery_resolved",
      sourceThreadId: "thr_parent",
      targetThreadId: "thr_child",
      sourceToolUseEventId: "sevt_agent_mail_source",
      receivedEventId: "sevt_agent_mail_received",
      receivedSequence: 8,
      message: storedMessage,
      publicMessageJson: bridge.resolveInterAgentDeliveryMessageJSON,
    });
    expect(bridge.resolveInterAgentDeliveryRequests).toEqual([
      expect.objectContaining({
        childThreadId: "thr_child",
        deliveryId: "delivery_resolved",
      }),
    ]);

    bridge.resolveInterAgentDeliveryMessageJSON = JSON.stringify({
      ...storedMessage,
      content: [{ type: "text", text: "different derived text" }],
    });
    await expect(loader.resolveAgentMail(
      control("thr_parent", "rin_agent_mail_resolve_bad", 5),
      "thr_child",
      "delivery_resolved",
    )).rejects.toMatchObject({
      type: "context-loader",
      code: "schema_mismatch",
      fatal: true,
    });

    bridge.resolveInterAgentDeliveryMessageJSON = JSON.stringify({
      ...storedMessage,
      content: [{ type: "text", text: "stored completion", unsupported: "silently dropped" }],
    });
    await expect(loader.resolveAgentMail(
      control("thr_parent", "rin_agent_mail_resolve_extra", 6),
      "thr_child",
      "delivery_resolved",
    )).rejects.toMatchObject({
      type: "context-loader",
      code: "schema_mismatch",
      fatal: true,
    });
  });

  test("commits accepted input by thread and preserves inter-agent delivery payloads", async () => {
    const bridge = new RecordingBridgeClient();
    const lifecycleLogs: string[] = [];
    const metrics = new RuntimePodMetricsRegistry();
    bridge.pendingAttachmentDeltaJson = [JSON.stringify({
      origin: {
        fileBacked: {
          sourceEventId: "sevt_1",
          fileId: "file_first_turn",
        },
      },
      mime: "image/png",
      filename: "first-turn.png",
    })];
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      logger: createJsonLogger({ write: (line) => lifecycleLogs.push(line) }),
      metrics,
    });
    const mainInput: RuntimeAcceptedInputState = {
      ...control("thr_main", "rin_main", 1),
      kind: "messages",
      payloadJson: JSON.stringify({ messages: [runtimeMessage("msg_main_input", "main input")] }),
    };
    const childInput: RuntimeAcceptedInputState = {
      ...control("thr_child", "rin_child", 2),
      kind: "inter_agent_message",
      deliveryId: "iam_thr_main_sevt_spawn_thr_child",
      sourceThreadId: "thr_main",
      sourceToolUseEventId: "sevt_spawn",
      message: runtimeMessage("msg_child_input", "thr_child input"),
      thread: {
        parentThreadId: "thr_main",
        role: "subagent",
        visibility: "public",
        taskName: "worker",
        agentType: "worker",
        status: "idle",
      },
    };

    const loadCountBeforeCommit = bridge.loadContextRequests.length;
    const mainCommit = await loader.commitAcceptedInput(mainInput, { drafts: acceptedInputDrafts(mainInput) });
    expect(mainCommit).toMatchObject({
      type: "receipt",
      inputDisposition: "committed",
      applicationDisposition: "current_custody",
    });
    expect(mainCommit.receipt.pendingAttachmentDelta).toEqual([{
      transient: undefined,
      fileBacked: {
        sourceEventId: "sevt_1",
        fileId: "file_first_turn",
      },
      mime: "image/png",
      filename: "first-turn.png",
    }]);
    expect(await loader.commitAcceptedInput(childInput, { drafts: acceptedInputDrafts(childInput) })).toMatchObject({
      type: "receipt",
      inputDisposition: "committed",
      applicationDisposition: "current_custody",
    });
    expect(bridge.loadContextRequests).toHaveLength(loadCountBeforeCommit);
    expect(lifecycleLogs.map((line) => JSON.parse(line))).toEqual(expect.arrayContaining([
      expect.objectContaining({
        event: "runtime_receipt_applied",
        "declaration.source.kind": "messages",
        "declaration.source.id": "rin_main",
      }),
      expect.objectContaining({
        event: "runtime_receipt_applied",
        "declaration.source.kind": "agent_mail",
        "declaration.source.id": "rin_child",
      }),
    ]));
    expect(metrics.snapshot().receiptEvidence.get("applied")).toBe(2);

    expect(bridge.commitInputsRequests.find((request) => request.runtimeInputId === "rin_main")).toMatchObject({
      inputKind: "messages",
      drafts: [expect.objectContaining({
        sourceKind: "messages",
        sourceId: "rin_main",
      })],
    });

    const childCommit = bridge.commitInputsRequests.find((request) => request.runtimeInputId === "rin_child");
    if (childCommit === undefined) {
      throw new Error("missing child commit request");
    }
    expect(childCommit).toMatchObject({
      inputKind: "agent_mail",
      scope: {
        sessionId: "sesn_1",
        sessionThreadId: "thr_child",
      },
      runtimeInputId: "rin_child",
      eventIds: ["sevt_2"],
      sequenceFrom: 2,
      sequenceTo: 2,
      drafts: [expect.objectContaining({
        sourceKind: "agent_mail",
        sourceId: "rin_child",
      })],
    });
    const loadedContext = await loader.loadThreadContext(control("thr_child", "rin_child_reload", 3));
    expect(loadedContext.messages.map((message) => message.id)).toEqual(["msg_context_thr_child"]);
    expect(loadedContext.runtimeConfigPatch?.generation).toBe(11);
    expect(loadedContext.runtimeConfigPatch?.installedBuiltinFamily).toBe("claude");
    expect(loadedContext.mcpManifests).toEqual([{
      ...control("thr_child", "rin_child_reload", 3),
      runtimeInputId: "runtime_config_update:mcp_manifest:sesn_1:github:7",
      generation: 7,
      mcpServerName: "github",
      manifestETag: "etag_7",
      manifestReadiness: "ready",
      payloadJson: JSON.stringify({
        mcp_manifest: {
          mcp_server_name: "github",
          manifest_generation: 7,
          readiness: "ready",
          diagnostic: null,
          manifest_etag: "etag_7",
          tools: [{ name: "github_search", description: "Search GitHub", input_schema: { type: "object" } }],
        },
      }),
    }, {
      ...control("thr_child", "rin_child_reload", 3),
      runtimeInputId: "runtime_config_update:mcp_manifest:sesn_1:gitlab:8",
      generation: 8,
      mcpServerName: "gitlab",
      manifestReadiness: "unready",
      manifestDiagnostic: "delivery_exhausted",
      payloadJson: JSON.stringify({
        mcp_manifest: {
          mcp_server_name: "gitlab",
          manifest_generation: 8,
          readiness: "unready",
          diagnostic: "delivery_exhausted",
        },
      }),
    }]);
    expect(JSON.parse(loadedContext.runtimeConfigPatch?.payloadJson ?? "{}")).toMatchObject({
      config_generation: 11,
      approval_mode: "approve_for_me",
      tool_policy: {
        approvalMode: "approve_for_me",
      },
      runtime_config: {
        system: "Operate as the session specialist.",
        memoryStores: [{
          memoryStoreId: "memstore_notes",
          name: "Project notes",
          access: "read_write",
          instructions: null,
        }],
        skills: [{ skillId: "sk_docs", version: "latest" }],
        skillsIndex: [{
          skill_id: "sk_docs",
          skill_version_id: "skv_docs_3",
          version: "3.0.0",
        }],
      },
    });
    expect(loadedContext.pendingToolUses).toEqual([{
      toolUseEventId: "evt_pending_tool",
      modelRequestId: "mrq_pending_tool",
      modelToolCallId: "toolu_pending",
      toolName: "Write",
      input: { path: "/workspace/file.txt" },
      decision: "deny",
      denyMessage: "not safe",
      status: "resolving",
    }]);
    expect(loadedContext.pendingSandboxExecutions).toEqual([{
      toolUseEventId: "evt_pending_sandbox",
      modelRequestId: "mrq_pending_sandbox",
      modelToolCallId: "toolu_pending_sandbox",
      toolName: "Bash",
      input: { command: "pwd" },
      executionState: "running",
    }]);
    expect(loadedContext.backgroundTools).toEqual([{
      taskId: "task_loaded",
      sourceToolUseEventId: "evt_background_tool",
    }]);
    expect(loadedContext.pendingAttachments).toEqual([{
      transient: {
        attachmentRef: "att_loaded",
        sourceToolUseEventId: "evt_loaded_tool",
        sourcePath: "mcp:github/plot.png",
        pageRange: "1-2",
        detail: "auto",
      },
      fileBacked: undefined,
      mime: "image/png",
      filename: "plot.png",
    }, {
      transient: undefined,
      fileBacked: {
        sourceEventId: "evt_loaded_user",
        fileId: "file_loaded_pdf",
      },
      mime: "application/pdf",
      filename: "brief.pdf",
    }]);
    expect(bridge.loadContextRequests.map((request) => request.scope?.sessionThreadId)).toEqual([
      "thr_child",
    ]);
  });

  test("classifies an unknown CommitInputs transport result as retryable", async () => {
    const bridge = new RecordingBridgeClient();
    bridge.commitErrors.push(Object.assign(new Error("response lost"), { code: status.UNAVAILABLE }));
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const input: RuntimeAcceptedInputState = {
      ...control("thr_main", "rin_lost_commit_response", 1),
      kind: "messages",
      payloadJson: JSON.stringify({ messages: [runtimeMessage("msg_lost_commit_response", "hello")] }),
    };

    await expect(loader.commitAcceptedInput(input, { drafts: acceptedInputDrafts(input) })).rejects.toMatchObject({
      type: "context-loader",
      code: "unavailable",
      retryable: true,
      fatal: false,
    });
  });

  test("rejects incomplete receipts and current-custody responses for another binding", async () => {
    const input: RuntimeAcceptedInputState = {
      ...control("thr_main", "rin_receipt_fence", 1),
      kind: "messages",
      payloadJson: JSON.stringify({ messages: [runtimeMessage("msg_receipt_fence", "hello")] }),
    };
    const bridge = new RecordingBridgeClient();
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    bridge.commitInputsResponseMutator = (response) => {
      response.declaration.receipts.push(response.declaration.receipts[0]);
    };
    await expect(loader.commitAcceptedInput(input, { drafts: acceptedInputDrafts(input) })).rejects.toMatchObject({
      type: "context-loader",
      code: "schema_mismatch",
    });

    bridge.commitInputsResponseMutator = (response) => {
      response.declaration.observedBindingId = "bind_other";
    };
    await expect(loader.commitAcceptedInput(input, { drafts: acceptedInputDrafts(input) })).rejects.toMatchObject({
      type: "context-loader",
      code: "schema_mismatch",
    });
  });
});

describe("BridgeAPIControlInputCommitter", () => {
  test("commits frozen interrupt and tool-confirmation declarations", async () => {
    const bridge = new RecordingBridgeClient(BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE);
    const committer = new BridgeAPIControlInputCommitter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const scope = control("thr_main", "rin_interrupt", 7);
    const cancellationDraft = RuntimeMessageDraftSchema.parse({
      runtimeLocalId: "stid_interrupt_message",
      sourceKind: "interrupt_control",
      sourceId: "rin_interrupt",
      sourceEventId: "sevt_7",
      draftKind: "cancellation",
      ordinal: 0,
      role: "assistant",
      origin: "agent",
      status: "completed",
      parts: [{
        runtimeLocalPartId: "stid_interrupt_part",
        type: "tool",
        ordinal: 0,
        toolCallId: "call_interrupt",
        toolName: "Bash",
        toolUseEventId: "sevt_tool",
        state: {
          status: "cancelled",
          error: { type: "runtime", message: "The tool was cancelled.", retryable: false },
        },
        completedAt: "2026-01-01T00:00:00.000Z",
      }],
    });
    const approvalDraft = RuntimeMessageDraftSchema.parse({
      runtimeLocalId: "stid_approval_message",
      sourceKind: "tool_confirmation",
      sourceId: "rin_confirm",
      sourceEventId: "sevt_confirm",
      draftKind: "approval_input",
      ordinal: 0,
      role: "user",
      origin: "user",
      status: "completed",
      parts: [{
        runtimeLocalPartId: "stid_approval_part",
        type: "text",
        ordinal: 0,
        text: "Approval allowed",
        truncated: false,
        status: "completed",
      }],
    });

    const interrupt = await committer.commitControlInput({
      scope,
      inputKind: "interrupt_control",
      drafts: [cancellationDraft],
      pendingToolCancellations: [{
        toolUseEventId: "sevt_tool",
        runtimeLocalId: cancellationDraft.runtimeLocalId,
      }],
      sandboxExecutionToolUseEventIds: [],
    });
    const confirmation = await committer.commitControlInput({
      scope: { ...scope, runtimeInputId: "rin_confirm", eventIds: ["sevt_confirm"] },
      inputKind: "tool_confirmation",
      drafts: [approvalDraft],
      pendingToolCancellations: [],
      sandboxExecutionToolUseEventIds: [],
    });

    expect(interrupt).toEqual({ ok: true, receipt: expect.objectContaining({ messages: [expect.any(Object)] }) });
    expect(confirmation).toEqual({ ok: true, receipt: expect.objectContaining({ messages: [expect.any(Object)] }) });
    expect(bridge.commitInputsRequests).toEqual([
      expect.objectContaining({
        runtimeInputId: "rin_interrupt",
        inputKind: "interrupt_control",
        eventIds: ["sevt_7"],
        sequenceFrom: 7,
        sequenceTo: 7,
        drafts: [expect.objectContaining({ runtimeLocalId: cancellationDraft.runtimeLocalId })],
        pendingToolCancellations: [{
          toolUseEventId: "sevt_tool",
          runtimeLocalId: cancellationDraft.runtimeLocalId,
        }],
      }),
      expect.objectContaining({
        runtimeInputId: "rin_confirm",
        inputKind: "tool_confirmation",
        eventIds: ["sevt_confirm"],
        sequenceFrom: 7,
        sequenceTo: 7,
        drafts: [expect.objectContaining({ runtimeLocalId: approvalDraft.runtimeLocalId })],
        pendingToolCancellations: [],
      }),
    ]);
  });

  test("keeps stale Bridge CommitInputs errors retryable for binding refresh", async () => {
    const staleError = Object.assign(new Error("stale"), { code: status.FAILED_PRECONDITION });
    const bridge = new RecordingBridgeClient(BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED, staleError);
    const committer = new BridgeAPIControlInputCommitter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    const result = await committer.commitControlInput({
      scope: control("thr_main", "rin_stale", 9),
      inputKind: "tool_confirmation",
      drafts: [],
      pendingToolCancellations: [],
      sandboxExecutionToolUseEventIds: [],
    });

    expect(result).toMatchObject({
      ok: false,
      retryable: true,
      errorCode: "bridge_commit_unavailable",
    });
  });

  test("replays the identical frozen control declaration after an unknown transport result", async () => {
    const bridge = new RecordingBridgeClient(BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE);
    bridge.commitErrors.push(Object.assign(new Error("response lost"), { code: status.INTERNAL }));
    const committer = new BridgeAPIControlInputCommitter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      sleep: async () => {},
    });
    const scope = control("thr_main", "rin_replay_control", 11);

    await expect(committer.commitControlInput({
      scope,
      inputKind: "interrupt_control",
      drafts: [],
      pendingToolCancellations: [],
      sandboxExecutionToolUseEventIds: [],
    })).resolves.toEqual({ ok: true, receipt: expect.any(Object) });

    expect(bridge.commitInputsRequests).toHaveLength(2);
    expect(bridge.commitInputsRequests[1]).toEqual(bridge.commitInputsRequests[0]);
  });

  test("returns retryable after the bounded exact-declaration transport replay window", async () => {
    const bridge = new RecordingBridgeClient(BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE);
    bridge.commitErrors.push(
      Object.assign(new Error("response lost one"), { code: status.UNAVAILABLE }),
      Object.assign(new Error("response lost two"), { code: status.DEADLINE_EXCEEDED }),
      Object.assign(new Error("response lost three"), { code: status.INTERNAL }),
    );
    const sleeps: number[] = [];
    const committer = new BridgeAPIControlInputCommitter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      sleep: async (durationMs) => {
        sleeps.push(durationMs);
      },
    });
    const scope = control("thr_main", "rin_bounded_replay_control", 11);

    const startedAt = Date.now();
    await expect(committer.commitControlInput({
      scope,
      inputKind: "interrupt_control",
      drafts: [],
      pendingToolCancellations: [],
      sandboxExecutionToolUseEventIds: [],
    })).resolves.toMatchObject({
      ok: false,
      retryable: true,
      errorCode: "bridge_commit_unavailable",
    });
    const finishedAt = Date.now();

    expect(bridge.commitInputsRequests).toHaveLength(3);
    expect(bridge.commitInputsRequests[1]).toEqual(bridge.commitInputsRequests[0]);
    expect(bridge.commitInputsRequests[2]).toEqual(bridge.commitInputsRequests[0]);
    expect(bridge.commitInputsCallOptions).toHaveLength(3);
    for (const options of bridge.commitInputsCallOptions) {
      expect(options.deadline).toBeNumber();
      expect(options.deadline as number).toBeGreaterThanOrEqual(
        startedAt + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
      );
      expect(options.deadline as number).toBeLessThanOrEqual(
        finishedAt + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
      );
    }
    expect(sleeps).toEqual([100, 300]);
  });

  test("returns a typed stale-custody result without applying a replayed control receipt", async () => {
    const bridge = new RecordingBridgeClient(BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE);
    bridge.commitInputsResponseMutator = (response) => {
      response.declaration.applicationDisposition =
        ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY;
      response.declaration.observedBindingId = "bind_replacement";
      response.declaration.observedBindingGeneration = 10;
    };
    const committer = new BridgeAPIControlInputCommitter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    await expect(committer.commitControlInput({
      scope: control("thr_main", "rin_stale_receipt", 9),
      inputKind: "tool_confirmation",
      drafts: [],
      pendingToolCancellations: [],
      sandboxExecutionToolUseEventIds: [],
    })).resolves.toEqual({ ok: true, stale: true });
  });
});

describe("BridgeAPIEventWriter", () => {
  test("returns the original Tool Use receipt when Bridge reports a duplicate write", async () => {
    const bridge = new RecordingBridgeClient();
    bridge.eventWriterAckStatus = BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE;
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    const result = await writer.append({
      ...writerScope(),
      writeId: "rwrite_duplicate_tool_use",
      event: {
        type: "agent.tool_use",
        name: "Read",
        input: { path: "a.txt" },
        evaluated_permission: "allow",
      },
      drafts: [outputDraft(
        "rwrite_duplicate_tool_use",
        "agent.tool_use",
        "tool_use",
        assistantToolMessage("running", { kind: "tool" }),
      )],
      modelRequestId: "req_duplicate_tool_use",
    });

    expect(result).toMatchObject({
      ok: true,
      writeId: "rwrite_duplicate_tool_use",
      declaration: {
        applicationDisposition: "current_custody",
        receipt: {
          operationKind: "write_event",
          sourceKind: "agent.tool_use",
          events: [{ disposition: "created" }],
        },
      },
    });
    expect(bridge.writeEventRequests).toHaveLength(1);
  });

  test("rejects a compaction boundary on an ordinary WriteEvent receipt", async () => {
    const bridge = new RecordingBridgeClient();
    bridge.eventWriterCompactedThroughMessageSequence = 0;
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    await expect(writer.append({
      ...writerScope(),
      writeId: "rwrite_unsolicited_compaction",
      event: {
        type: "session.error",
        error: {
          type: "provider",
          code: "provider_rate_limited",
          message: "provider unavailable",
          retryable: true,
          fatal: false,
          retryStatus: { type: "retrying", attempt: 1 },
        },
      },
    })).resolves.toMatchObject({
      ok: false,
      error: {
        code: "schema_mismatch",
      },
    });
  });

  test("maps internal provider failures to fork-SDK session errors before WriteEvent", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    await writer.append({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_error",
      event: {
        type: "session.error",
        error: {
          type: "provider",
          code: "provider_rate_limited",
          message: "raw provider response",
          retryable: true,
          fatal: false,
          retryStatus: { type: "retrying", attempt: 1 },
        },
      },
    });

    expect(JSON.parse(bridge.writeEventRequests[0]?.payloadJson ?? "{}")).toEqual({
      type: "session.error",
      error: {
        type: "model_rate_limited_error",
        message: "raw provider response",
        retry_status: { type: "retrying" },
      },
    });
  });

  test("passes loop-authored drafts to Bridge WriteEvent without changing public payload", async () => {
    const fixture = JSON.parse(readFileSync(resolve(import.meta.dir, "../../../../../../testdata/stable-reasoning-anchor-vector.json"), "utf8")) as {
      readonly model_request_id: string;
      readonly event: {
        readonly type: "agent.tool_use";
        readonly name: string;
        readonly input: Record<string, string>;
        readonly evaluated_permission: "allow";
      };
      readonly stable_reasoning_parts: readonly {
        readonly reasoning_part_id: string;
        readonly provider_part_id: string;
        readonly part_sequence: number;
        readonly text: string;
        readonly metadata_json: string;
        readonly truncated: boolean;
      }[];
    };
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    const result = await writer.append({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_tool",
      event: fixture.event,
      drafts: [outputDraft(
        "rwrite_tool",
        "agent.tool_use",
        "tool_use",
        assistantToolMessage("running", { kind: "tool" }),
      )],
      modelRequestId: fixture.model_request_id,
      stableReasoningParts: fixture.stable_reasoning_parts.map((part) => ({
        reasoningPartId: part.reasoning_part_id,
        providerPartId: part.provider_part_id,
        partSequence: part.part_sequence,
        text: part.text,
        providerMetadata: ProviderMetadataSchema.parse(JSON.parse(part.metadata_json)),
        truncated: part.truncated,
      })),
    });

    expect(result).toMatchObject({ ok: true, writeId: "rwrite_tool" });
    expect(bridge.writeEventRequests).toHaveLength(1);
    expect(JSON.parse(bridge.writeEventRequests[0]?.payloadJson ?? "{}")).toEqual({
      type: "agent.tool_use",
      name: "Read",
      input: { path: "a.txt" },
      evaluated_permission: "allow",
    });
    expect(bridge.writeEventRequests[0]?.drafts).toHaveLength(1);
    expect(JSON.parse(bridge.writeEventRequests[0]?.drafts[0]?.messageInfoJson ?? "{}")).toMatchObject({
      role: "assistant",
      origin: "agent",
    });
    expect(bridge.writeEventRequests[0]?.modelRequestId).toBe(fixture.model_request_id);
    expect(bridge.writeEventRequests[0]?.stableReasoningParts).toEqual(fixture.stable_reasoning_parts.map((part) => ({
      reasoningPartId: part.reasoning_part_id,
      providerPartId: part.provider_part_id,
      partSequence: part.part_sequence,
      text: part.text,
      metadataJson: JSON.stringify(ProviderMetadataSchema.parse(JSON.parse(part.metadata_json))),
      truncated: part.truncated,
    })));

    await writer.append({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_text_projection",
      event: { type: "agent.message", content: [{ type: "text", text: "answer" }] },
      drafts: [outputDraft(
        "rwrite_text_projection",
        "agent.message",
        "assistant_text",
        assistantTextMessage("answer"),
      )],
      modelRequestId: fixture.model_request_id,
    });
    expect(bridge.writeEventRequests[1]?.modelRequestId).toBe(fixture.model_request_id);

    await writer.append({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_tool_result",
      event: {
        type: "agent.tool_result",
        tool_use_id: "sevt_tool_use_1",
        content: [{ type: "text", text: "done" }],
      },
      drafts: [outputDraft(
        "rwrite_tool_result",
        "agent.tool_result",
        "tool_result",
        assistantToolMessage("completed", { kind: "tool" }),
      )],
      modelRequestId: fixture.model_request_id,
    });
    expect(bridge.writeEventRequests[2]?.modelRequestId).toBe(fixture.model_request_id);

    await writer.append({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_mcp_tool_result",
      event: {
        type: "agent.mcp_tool_result",
        mcp_tool_use_id: "evt_mcp_use",
        content: [{ type: "text", text: "mcp result" }],
      },
      drafts: [outputDraft(
        "rwrite_mcp_tool_result",
        "agent.mcp_tool_result",
        "tool_result",
        assistantToolMessage("completed", { kind: "mcp", mcpServerName: "github" }, "github_search", "call_mcp"),
      )],
      modelRequestId: fixture.model_request_id,
      mcpMaterializationHandle: "evt_mcp_use",
    });
    expect(bridge.writeEventRequests[3]?.mcpMaterializationHandle).toBe("evt_mcp_use");
  });

  test("attaches web server-tool usage only on the durable tool-result request", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    await writer.append({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_web_result",
      event: {
        type: "agent.tool_result",
        tool_use_id: "evt_web_use",
        content: [{ type: "text", text: "web result" }],
      },
      drafts: [outputDraft(
        "rwrite_web_result",
        "agent.tool_result",
        "tool_result",
        assistantToolMessage("completed", { kind: "tool" }, "web", "call_web"),
      )],
      modelRequestId: "mreq_web",
      serverToolUse: { webSearchRequests: 2, webFetchRequests: 1 },
    });

    expect(bridge.writeEventRequests).toHaveLength(1);
    expect(bridge.writeEventRequests[0]?.serverToolUse).toEqual({ webSearchRequests: 2, webFetchRequests: 1 });
    expect(JSON.parse(bridge.writeEventRequests[0]?.payloadJson ?? "{}")).toEqual({
      type: "agent.tool_result",
      tool_use_id: "evt_web_use",
      content: [{ type: "text", text: "web result" }],
    });
  });

  test("sends approval reviewer decisions under internal reviewer scope", async () => {
    const bridge = new RecordingBridgeClient();
    const reviewerInput: RuntimeAcceptedInputState = {
      ...control("thr_reviewer", "rin_review", 2),
      kind: "approval_review",
      reviewId: "arvw_1",
      parentThreadId: "thr_main",
      targetModelToolCallId: "tool_call_1",
      targetToolName: "Write",
      promptItems: [runtimeMessage("msg_review", "review the action")],
      outputSchemaJson: "{}",
      thread: {
        parentThreadId: "thr_main",
        role: "approval_reviewer",
        visibility: "internal",
        agentType: "approval_reviewer",
        status: "idle",
      },
    };
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    const result = await writer.append({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_reviewer",
      writeId: "rwrite_arvw_1_decision",
      event: {
        type: "approval_review.decision",
        review_id: "arvw_1",
        parent_thread_id: "thr_main",
        target_model_tool_call_id: "tool_call_1",
        target_tool_name: "Write",
        risk_level: "low",
        user_authorization: "high",
        outcome: "allow",
        rationale: "low risk",
      },
    });

    expect(result).toMatchObject({ ok: true, writeId: "rwrite_arvw_1_decision" });
    expect(bridge.writeEventRequests).toHaveLength(1);
    expect(bridge.writeEventRequests[0]).toMatchObject({
      eventType: "approval_review.decision",
      sessionVisible: false,
      scope: { sessionThreadId: "thr_reviewer" },
    });
    expect(JSON.parse(bridge.writeEventRequests[0]?.payloadJson ?? "{}")).toEqual({
      type: "approval_review.decision",
      review_id: "arvw_1",
      parent_thread_id: "thr_main",
      target_model_tool_call_id: "tool_call_1",
      target_tool_name: "Write",
      risk_level: "low",
      user_authorization: "high",
      outcome: "allow",
      rationale: "low risk",
    });
  });

  test("sends approval reviewer failures under internal reviewer scope", async () => {
    const bridge = new RecordingBridgeClient();
    const reviewerInput: RuntimeAcceptedInputState = {
      ...control("thr_reviewer", "rin_review", 2),
      kind: "approval_review",
      reviewId: "arvw_1",
      parentThreadId: "thr_main",
      targetModelToolCallId: "tool_call_1",
      targetToolName: "Write",
      promptItems: [runtimeMessage("msg_review", "review the action")],
      outputSchemaJson: "{}",
      thread: {
        parentThreadId: "thr_main",
        role: "approval_reviewer",
        visibility: "internal",
        agentType: "approval_reviewer",
        status: "idle",
      },
    };
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    const result = await writer.append({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_reviewer",
      writeId: "rwrite_arvw_1_failure",
      event: {
        type: "approval_review.failure",
        review_id: "arvw_1",
        parent_thread_id: "thr_main",
        target_model_tool_call_id: "tool_call_1",
        target_tool_name: "Write",
        failure_kind: "parse_failure",
        message: "approval reviewer decision is not JSON",
      },
    });

    expect(result).toMatchObject({ ok: true, writeId: "rwrite_arvw_1_failure" });
    expect(bridge.writeEventRequests).toHaveLength(1);
    expect(bridge.writeEventRequests[0]).toMatchObject({
      eventType: "approval_review.failure",
      sessionVisible: false,
      scope: { sessionThreadId: "thr_reviewer" },
    });
    expect(JSON.parse(bridge.writeEventRequests[0]?.payloadJson ?? "{}")).toEqual({
      type: "approval_review.failure",
      review_id: "arvw_1",
      parent_thread_id: "thr_main",
      target_model_tool_call_id: "tool_call_1",
      target_tool_name: "Write",
      failure_kind: "parse_failure",
      message: "approval reviewer decision is not JSON",
    });
  });

  test("ensures approval reviewer trunk through durable child-thread creation", async () => {
    const bridge = new RecordingBridgeClient();
    const creator = new BridgeAPIApprovalReviewerThreadCreator({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const reviewRequest: RuntimeApprovalReviewRequest = {
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      bindingId: "bind_1",
      bindingGeneration: 7,
      targetPodUid: "pod_1",
      runtimeBindingToken: "runtime-binding-token",
      modelRequestId: "req_2",
      targetModelToolCallId: "tool_call_1",
      targetToolName: "Write",
      actionJson: {},
      approvalReviewerManager: new AutoApprovalReviewerManager(),
      parentTranscript: { generation: 1, messages: [] },
      currentProviderRequestMessages: [],
      siblingToolCalls: [],
      policyContext: {},
    };

    await expect(creator.createApprovalReviewerThread({
      request: reviewRequest,
      reviewId: "arvw_1",
      reviewerThreadId: "thr_reviewer",
      isTrunk: true,
      threadContextPrefixJson: "",
    })).resolves.toEqual({ ok: true });

    expect(bridge.createChildThreadRequests).toHaveLength(1);
    expect(bridge.createChildThreadRequests[0]).toMatchObject({
      scope: {
        requestId: "req_2",
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        binding: {
          bindingId: "bind_1",
          bindingGeneration: 7,
          targetPodUid: "pod_1",
        },
      },
      parentThreadId: "thr_main",
      childThreadId: "thr_reviewer",
      role: "approval_reviewer",
      agentType: "approval_reviewer",
      forkTurns: "none",
      threadContextPrefixJson: "",
      isTrunk: true,
      reviewerReviewId: "",
    });

    const threadContextPrefixJson = JSON.stringify({
      source_parent_thread_id: "thr_main",
      review_id: "arvw_sidecar",
      fork_turns: "all",
      runtime_messages_snapshot: [runtimeMessage("msg_seed", "seed")],
    });
    await expect(creator.createApprovalReviewerThread({
      request: reviewRequest,
      reviewId: "arvw_sidecar",
      reviewerThreadId: "thr_reviewer_sidecar",
      isTrunk: false,
      threadContextPrefixJson,
    })).resolves.toEqual({ ok: true });
    expect(bridge.createChildThreadRequests[1]).toMatchObject({
      childThreadId: "thr_reviewer_sidecar",
      role: "approval_reviewer",
      forkTurns: "all",
      threadContextPrefixJson,
      isTrunk: false,
      reviewerReviewId: "arvw_sidecar",
    });
    await expect(creator.closeApprovalReviewerThread({
      request: reviewRequest,
      reviewId: "arvw_sidecar",
      reviewerThreadId: "thr_reviewer_sidecar",
      isTrunk: false,
      threadContextPrefixJson,
    })).resolves.toEqual({ ok: true });
    expect(bridge.markChildThreadClosedRequests).toEqual([
      expect.objectContaining({
        childThreadId: "thr_reviewer_sidecar",
        scope: expect.objectContaining({ sessionThreadId: "thr_main" }),
      }),
    ]);
  });

  test("serializes request-end usage with total input tokens for Bridge normalization", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    const result = await writer.writeRequestEnd({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_end",
      modelRequestId: "mreq_1",
      modelRequestStartEventId: "sevt_start",
      isError: false,
      finishReason: "stop",
      drafts: [outputDraft("mreq_1", "model_request", "assistant_text", assistantTextMessage("sealed answer"))],
      consumedAttachmentRefs: ["att_1"],
      consumedFileAttachments: [{
        sourceEventId: "sevt_user_file",
        fileId: "file_1",
      }],
      usage: {
        inputTokens: 2,
        outputTokens: 3,
        reasoningTokens: 1,
        cacheReadTokens: 1,
        cacheWriteTokens: 2,
        totalTokens: 8,
        providerUsageJson: "{\"provider\":\"openai\"}",
      },
    });

    expect(result).toMatchObject({
      ok: true,
      writeId: "rwrite_end",
      declaration: {
        receipt: {
          messages: [{
            messageId: "msg_request_end_0",
            parts: [{ partId: "part_request_end_0_0" }],
          }],
        },
      },
    });
    const usage = JSON.parse(bridge.writeRequestEndRequests[0]?.usageJson ?? "{}");
    expect(usage).toEqual({
      input_tokens: 5,
      cache_read_input_tokens: 1,
      cache_creation_input_tokens: 2,
      output_tokens: 3,
      reasoning_output_tokens: 1,
      total_tokens: 8,
      provider_usage_json: "{\"provider\":\"openai\"}",
    });
    expect(bridge.writeRequestEndRequests[0]?.errorKind).toBe("");
    expect(bridge.writeRequestEndRequests[0]?.consumedAttachmentRefs).toEqual(["att_1"]);
    expect(bridge.writeRequestEndRequests[0]?.consumedFileAttachments).toEqual([{
      sourceEventId: "sevt_user_file",
      fileId: "file_1",
    }]);
    expect(bridge.writeRequestEndRequests[0]?.requestKind).toBe("agent_provider_request");

    const errorResult = await writer.writeRequestEnd({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_error_end",
      modelRequestId: "mreq_error",
      modelRequestStartEventId: "sevt_error_start",
      isError: true,
      errorKind: "gateway_stream_error",
      finishReason: "error",
      reschedule: {
        attempt: 1,
        deadline: "2026-01-01T00:00:30.000Z",
        backoffMs: 30_000,
      },
    });

    expect(errorResult).toMatchObject({
      ok: true,
      writeId: "rwrite_error_end",
      rescheduleDisposition: {
        status: "accepted",
        attempt: 1,
        effectiveDeadline: "2026-01-01T00:00:30.000Z",
      },
    });
    expect(bridge.writeRequestEndRequests[1]?.errorKind).toBe("gateway_stream_error");
    expect(bridge.writeRequestEndRequests[1]?.reschedule).toEqual({
      attempt: 1,
      deadline: "2026-01-01T00:00:30.000Z",
      backoffMs: 30_000,
    });

    bridge.eventWriterTransportError = Object.assign(new Error("divergent close"), { code: status.ALREADY_EXISTS });
    expect(await writer.writeRequestEnd({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_divergent_end",
      modelRequestId: "mreq_error",
      modelRequestStartEventId: "sevt_error_start",
      isError: true,
      errorKind: "gateway_stream_error",
      finishReason: "error",
    })).toMatchObject({
      ok: false,
      error: { code: "superseded", retryable: false },
    });
  });

  test("carries closeout sentinel acknowledgements across every closeout writer", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const operations = [
      () => writer.append({
        ...writerScope(),
        workspaceId: "wksp_1",
      sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_error",
        event: { type: "session.status_running" as const },
      }),
      () => writer.finishIdle!({
        ...writerScope(),
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        durableTurnId: "evt_turn_idle",
        stopReason: { type: "end_turn" as const },
      }),
      () => writer.writeRequestEnd!({
        ...writerScope(),
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_request_end",
        modelRequestId: "mreq_request_end",
        modelRequestStartEventId: "sevt_request_end",
        isError: true,
        errorKind: "runtime_persistence_error",
        finishReason: "error",
      }),
      () => writer.commitRuntimeTermination!({
        ...writerScope(),
        workspaceId: "wksp_1",
      sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_termination",
        failure: {
          type: "runtime" as const,
          code: "runtime_invalid_sequence" as const,
          message: "Runtime operation failed.",
          retryable: false,
          fatal: true,
          reason: "runtime_contract_validation" as const,
        },
        pendingToolCancellations: [],
        sandboxExecutionToolUseEventIds: [],
      }),
    ];

    for (const [bridgeCode, podCode] of [
      ["scope_superseded", "superseded"],
      ["closeout_unrepairable", "unrepairable"],
      ["unsentineled_rejection", "unavailable"],
    ] as const) {
      bridge.eventWriterAckStatus = BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED;
      bridge.eventWriterErrorCode = bridgeCode;
      for (const operation of operations) {
        expect(await operation()).toMatchObject({
          ok: false,
          error: {
            code: podCode,
            retryable: podCode === "unavailable",
          },
        });
      }
    }
  });

  test("maps a superseded runtime termination without retrying the Bridge call", async () => {
    const bridge = new RecordingBridgeClient();
    bridge.eventWriterAckStatus = BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED;
    bridge.eventWriterErrorCode = "scope_superseded";
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    const result = await writer.commitRuntimeTermination!({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_termination_superseded",
      failure: {
        type: "runtime",
        code: "runtime_invalid_sequence",
        message: "Runtime operation failed.",
        retryable: false,
        fatal: true,
        reason: "runtime_contract_validation",
      },
      pendingToolCancellations: [],
      sandboxExecutionToolUseEventIds: [],
    });

    expect(result).toMatchObject({
      ok: false,
      error: {
        code: "superseded",
        retryable: false,
      },
    });
    expect(bridge.commitRuntimeTerminationRequests).toHaveLength(1);
  });

  test("returns the Bridge acknowledgement identity from every event writer", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const operations = [
      () => writer.append({
        ...writerScope(),
        workspaceId: "wksp_1",
      sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_requested_append",
        event: { type: "session.status_running" as const },
      }),
      () => writer.finishIdle!({
        ...writerScope(),
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        durableTurnId: "evt_turn_requested_idle",
        stopReason: { type: "end_turn" as const },
      }),
      () => writer.writeRequestEnd!({
        ...writerScope(),
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_requested_end",
        modelRequestId: "mreq_requested_end",
        modelRequestStartEventId: "sevt_requested_end",
        isError: false,
        finishReason: "stop",
      }),
      () => writer.commitRuntimeTermination!({
        ...writerScope(),
        workspaceId: "wksp_1",
      sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_requested_termination",
        failure: {
          type: "runtime" as const,
          code: "runtime_invalid_sequence" as const,
          message: "Runtime operation failed.",
          retryable: false,
          fatal: true,
          reason: "runtime_contract_validation" as const,
        },
        pendingToolCancellations: [],
        sandboxExecutionToolUseEventIds: [],
      }),
    ];

    bridge.eventWriterAckWriteId = "rwrite_bridge_ack";
    for (const operation of operations) {
      expect(await operation()).toMatchObject({ ok: true, writeId: "rwrite_bridge_ack" });
    }
    bridge.eventWriterAckWriteId = "";
    for (const operation of operations) {
      expect(await operation()).toMatchObject({ ok: true, writeId: "" });
    }
  });

  test("FinishIdle awaits the Bridge callback without a client-side abandonment timer", async () => {
    let release: (() => void) | undefined;
    const client = {
      finishIdle(
        request: FinishIdleRequest,
        _metadata: Metadata,
        callback: (error: Error | null, response: unknown) => void,
      ) {
        release = () => {
          const now = "2026-01-01T00:00:00.000Z";
          const idleEventId = `evt_idle_${request.durableTurnId}`;
          callback(null, {
            ack: {
              status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED,
              runtimeInputId: "",
              runtimeWriteId: request.durableTurnId,
              errorCode: "",
            },
            declaration: {
              observedBindingId: request.scope?.binding?.bindingId ?? "",
              observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
              applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
              receipts: [{
                sessionThreadId: request.scope?.sessionThreadId ?? "",
                operationKind: "finish_idle",
                sourceKind: "turn_closeout",
                sourceId: request.durableTurnId,
                declarationDigest: finishIdleDeclarationDigest(request),
                pendingAttachmentDeltaJson: [],
                pendingToolDeltaJson: [],
                prefixConsumptions: [],
                childLifecycle: [],
                events: [{
                  sessionThreadId: request.scope?.sessionThreadId ?? "",
                  sourceEventId: request.durableTurnId,
                  eventId: idleEventId,
                  eventSequence: 1,
                  disposition: 2,
                }],
                messages: [],
                requestReschedule: undefined,
                idleCloseout: {
                  durableTurnId: request.durableTurnId,
                  idleEventId,
                  idleEventSequence: 1,
                  committedIdleAt: now,
                },
                compactedThroughMessageSequence: undefined,
              }],
            },
          });
        };
        return grpcCall();
      },
    } as unknown as AgentRuntimeBridgeServiceClient;
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client,
      metadataFactory: async () => new Metadata(),
    });
    let settled = false;
    const result = writer.finishIdle!({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      durableTurnId: "evt_turn_direct_await",
      stopReason: { type: "end_turn" },
    }).finally(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);
    expect(release).toBeDefined();

    release?.();
    expect(await result).toMatchObject({ ok: true, writeId: "evt_turn_direct_await" });
  });

  test("normalizes retryable closeout transport statuses without collapsing unknown failures", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    for (const [grpcCode, writerCode] of [
      [status.UNAVAILABLE, "unavailable"],
      [status.DEADLINE_EXCEEDED, "timeout"],
      [status.ABORTED, "unavailable"],
      [status.RESOURCE_EXHAUSTED, "unavailable"],
    ] as const) {
      bridge.eventWriterTransportError = Object.assign(new Error("transport failed"), { code: grpcCode });
      expect(await writer.append({
        ...writerScope(),
        workspaceId: "wksp_1",
      sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: `rwrite_transport_${grpcCode}`,
        event: { type: "session.status_running" },
      })).toMatchObject({
        ok: false,
        error: { code: writerCode, retryable: true },
      });
    }
    bridge.eventWriterTransportError = new Error("unclassified transport failure");
    expect(await writer.append({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_transport_unknown",
      event: { type: "session.status_running" },
    })).toMatchObject({
      ok: false,
      error: { code: "unknown", retryable: false },
    });
  });

  test("settles ordered stable reasoning metadata through WriteRequestEnd", async () => {
    const bridge = new RecordingBridgeClient();
    const input: RuntimeAcceptedInputState = {
      ...control("thr_main", "rin_reasoning", 2),
      kind: "messages",
      payloadJson: "{}",
    };
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });

    const result = await writer.writeRequestEnd({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_reasoning_1",
      modelRequestId: "mreq_1",
      modelRequestStartEventId: "sevt_reasoning_start_1",
      isError: false,
      finishReason: "stop",
      stableReasoningParts: [{
        reasoningPartId: "part_reasoning_1",
        providerPartId: "provider_reasoning_1",
        partSequence: 0,
        text: "thinking",
        providerMetadata: { anthropic: { signature: "sig_round_trip" } },
        truncated: false,
      }],
    });

    expect(result).toMatchObject({ ok: true, writeId: "rwrite_reasoning_1" });
    expect(bridge.writeRequestEndRequests).toEqual([expect.objectContaining({
      modelRequestId: "mreq_1",
      stableReasoningParts: [expect.objectContaining({
        reasoningPartId: "part_reasoning_1",
        providerPartId: "provider_reasoning_1",
        partSequence: 0,
        text: "thinking",
        metadataJson: JSON.stringify({ anthropic: { signature: "sig_round_trip" } }),
      })],
    })]);
  });

  test("matches joined request-end and interrupt receipts by operation identity", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const cancellationDraft = RuntimeMessageDraftSchema.parse({
      runtimeLocalId: "stid_interrupt_message",
      sourceKind: "interrupt_control",
      sourceId: "rin_interrupt_1",
      sourceEventId: "sevt_interrupt_1",
      draftKind: "cancellation",
      ordinal: 0,
      role: "assistant",
      origin: "agent",
      status: "completed",
      parts: [{
        runtimeLocalPartId: "stid_interrupt_part",
        type: "tool",
        ordinal: 0,
        toolCallId: "call_interrupt_1",
        toolName: "Bash",
        toolUseEventId: "sevt_tool_use_1",
        state: {
          status: "cancelled",
          error: {
            type: "runtime",
            message: "The tool was cancelled.",
            retryable: false,
          },
        },
        completedAt: "2026-01-01T00:00:00.000Z",
      }],
    });

    const result = await writer.writeRequestEnd({
      ...writerScope(),
      writeId: "rwrite_interrupt_1",
      modelRequestId: "mreq_interrupt_1",
      modelRequestStartEventId: "sevt_request_start_1",
      isError: true,
      errorKind: "runtime_interrupted",
      finishReason: "cancelled",
      drafts: [cancellationDraft],
      interruptSettlement: {
        runtimeInputId: "rin_interrupt_1",
        eventIds: ["sevt_interrupt_1"],
        sequenceFrom: 9,
        sequenceTo: 9,
        pendingToolCancellations: [{
          toolUseEventId: "sevt_tool_use_1",
          runtimeLocalId: cancellationDraft.runtimeLocalId,
        }],
        sandboxExecutionToolUseEventIds: [],
      },
    });

    expect(result).toMatchObject({
      ok: true,
      declaration: {
        receipt: {
          operationKind: "write_request_end",
          sourceKind: "model_request",
          sourceId: "mreq_interrupt_1",
        },
        relatedReceipts: [{
          operationKind: "commit_inputs",
          sourceKind: "interrupt_control",
          sourceId: "rin_interrupt_1",
        }],
      },
    });
    expect(bridge.writeRequestEndRequests[0]).toMatchObject({
      interruptSettlement: {
        runtimeInputId: "rin_interrupt_1",
        eventIds: ["sevt_interrupt_1"],
        sequenceFrom: 9,
        sequenceTo: 9,
        pendingToolCancellations: [{
          toolUseEventId: "sevt_tool_use_1",
          runtimeLocalId: cancellationDraft.runtimeLocalId,
        }],
      },
    });
  });

  test("commits normalized terminal failure through the atomic Bridge closeout", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const failure = {
      type: "provider",
      code: "provider_invalid_request",
      message: "Provider request is terminal.",
      retryable: false,
      fatal: true,
      retryStatus: { type: "terminal" },
    } as const;

    const result = await writer.commitRuntimeTermination?.({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_terminate",
      failure,
      pendingToolCancellations: [],
      sandboxExecutionToolUseEventIds: [],
    });

    expect(result).toMatchObject({ ok: true, writeId: "rwrite_terminate" });
    expect(bridge.commitRuntimeTerminationRequests).toEqual([expect.objectContaining({
      runtimeWriteId: "rwrite_terminate",
      failureJson: JSON.stringify(failure),
      drafts: [],
      pendingToolCancellations: [],
      scope: expect.objectContaining({ sessionId: "sesn_1", sessionThreadId: "thr_main" }),
    })]);
  });
});

describe("BridgeAPIInternalToolRepairCommitter", () => {
  test("commits an unstamped internal repair and returns only database-assigned projection identity", async () => {
    const bridge = new RecordingBridgeClient();
    const committer = new BridgeAPIInternalToolRepairCommitter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const repairMessage = runtimeRepairMessage();
    const repairKey = "internal_invalid_tool:mreq_1:call_1:unknown_tool";
    const draft = runtimeOutputDraft({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      runtimeWriteId: repairKey,
      eventType: "internal_tool_repair",
      draftKind: "internal_tool_repair",
      message: runtimeWorkingDraftFromDurable({
        workspaceId: "wksp_1",
        sessionThreadId: "thr_main",
        modelRequestId: "mreq_1",
        message: {
          ...repairMessage,
          owningEventId: "evt_repair_seed",
          eventSequence: 1,
        },
      }),
    });

    const result = await committer.commitInternalToolRepair({
      ...writerScope(),
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      modelRequestId: "mreq_1",
      modelToolCallId: "call_1",
      toolName: "unknown_tool",
      repairKey,
      draft,
    });

    expect(result).toMatchObject({
      ok: true,
      eventId: "evt_internal_repair",
      declaration: {
        receipt: {
          operationKind: "commit_internal_tool_repair",
          sourceKind: "internal_tool_repair",
          sourceId: repairKey,
          messages: [{
            runtimeLocalId: draft.runtimeLocalId,
            messageId: "msg_durable_repair",
            parts: [{
              runtimeLocalPartId: draft.parts[0]?.runtimeLocalPartId,
              partId: "part_durable_repair",
            }],
          }],
        },
        applicationDisposition: "current_custody",
      },
    });
    expect(bridge.commitInternalToolRepairRequests).toHaveLength(1);
    expect(bridge.commitInternalToolRepairRequests[0]).toMatchObject({
      modelRequestId: "mreq_1",
      modelToolCallId: "call_1",
      toolName: "unknown_tool",
      scope: {
        requestId: "req_1",
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
      },
      drafts: [expect.objectContaining({
        runtimeLocalId: draft.runtimeLocalId,
        sourceKind: "internal_tool_repair",
        sourceId: repairKey,
        draftKind: RuntimeDraftKind.RUNTIME_DRAFT_KIND_INTERNAL_TOOL_REPAIR,
      })],
    });
    expect(bridge.commitInternalToolRepairRequests[0]).not.toHaveProperty("dataJson");
    expect(bridge.commitInputsRequests).toEqual([]);
    expect(bridge.writeEventRequests).toEqual([]);
  });
});

function control(
  sessionThreadId: string,
  runtimeInputId: string,
  sequence: number,
): RuntimeThreadControlState {
  return {
    requestId: `req_${sequence}`,
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId,
    bindingId: "bind_1",
    bindingGeneration: 7,
    targetPodUid: "pod_1",
    runtimeInputId,
    eventIds: [`sevt_${sequence}`],
    sequenceFrom: sequence,
    sequenceTo: sequence,
  };
}

function writerScope(): Pick<
  RuntimeThreadControlState,
  "requestId" | "workspaceId" | "sessionId" | "sessionThreadId" | "bindingId" | "bindingGeneration" | "targetPodUid"
> & { readonly drafts: RuntimeMessageDraft[] } {
  const scope = control("thr_main", "rin_main", 1);
  return {
    requestId: scope.requestId,
    workspaceId: scope.workspaceId,
    sessionId: scope.sessionId,
    sessionThreadId: scope.sessionThreadId,
    bindingId: scope.bindingId,
    bindingGeneration: scope.bindingGeneration,
    targetPodUid: scope.targetPodUid,
    drafts: [],
  };
}

function outputDraft(
  runtimeWriteId: string,
  eventType: string,
  draftKind: RuntimeMessageDraft["draftKind"],
  message: RuntimeMessage,
): RuntimeMessageDraft {
  return runtimeOutputDraft({
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thr_main",
    runtimeWriteId,
    eventType,
    draftKind,
    message: runtimeWorkingDraftFromDurable({
      workspaceId: "wksp_1",
      sessionThreadId: "thr_main",
      modelRequestId: "mreq_output",
      message: {
        ...message,
        owningEventId: "evt_output_seed",
        eventSequence: 1,
      },
    }),
  });
}

function assistantTextMessage(text: string): RuntimeMessage {
  return RuntimeMessageSchema.parse({
    id: "msg_output",
    sessionId: "sesn_1",
    role: "assistant",
    origin: "agent",
    sequence: 1,
    status: "completed",
    createdAt: "2026-01-01T00:00:00.000Z",
    parts: [{
      id: "part_output_text",
      sessionId: "sesn_1",
      messageId: "msg_output",
      sequence: 0,
      type: "text",
      text,
      truncated: false,
      status: "completed",
      createdAt: "2026-01-01T00:00:00.000Z",
      completedAt: "2026-01-01T00:00:00.000Z",
    }],
  });
}

function assistantToolMessage(
  statusValue: "running" | "completed",
  toolEvent: { readonly kind: "tool" } | { readonly kind: "mcp"; readonly mcpServerName: string },
  toolName = "Read",
  toolCallId = "call_1",
): RuntimeMessage {
  const input = {
    value: toolName === "web" ? { search_query: [{ q: "tetral" }] } : { path: "a.txt" },
    preview: toolName === "web"
      ? "{\"search_query\":[{\"q\":\"tetral\"}]}"
      : "{\"path\":\"a.txt\"}",
    truncated: false,
  };
  return RuntimeMessageSchema.parse({
    id: "msg_output",
    sessionId: "sesn_1",
    role: "assistant",
    origin: "agent",
    sequence: 1,
    status: "completed",
    createdAt: "2026-01-01T00:00:00.000Z",
    parts: [{
      id: "part_output_tool",
      sessionId: "sesn_1",
      messageId: "msg_output",
      sequence: 0,
      type: "tool",
      toolCallId,
      toolName,
      toolEvent,
      state: statusValue === "running"
        ? { status: "running", input }
        : {
            status: "completed",
            input,
            output: { text: toolName === "web" ? "web result" : "done", truncated: false },
          },
      createdAt: "2026-01-01T00:00:00.000Z",
      completedAt: statusValue === "completed" ? "2026-01-01T00:00:00.000Z" : undefined,
    }],
  });
}

function bindingIdentity(runtimeBindingToken: string): RuntimeThreadIdentity {
  return {
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thr_main",
    bindingId: "bind_1",
    bindingGeneration: 7,
    targetPodUid: "pod_1",
    runtimeBindingToken,
  };
}

function jwtWithExpiry(exp: number): string {
  const encode = (value: object): string => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode({ alg: "none" })}.${encode({ exp })}.signature`;
}

class RecordingBridgeClient {
  readonly commitInputsRequests: CommitInputsRequest[] = [];
  readonly commitInputsCallOptions: CallOptions[] = [];
  readonly commitInternalToolRepairRequests: CommitInternalToolRepairRequest[] = [];
  readonly commitRuntimeTerminationRequests: CommitRuntimeTerminationRequest[] = [];
  readonly createChildThreadRequests: CreateChildThreadRequest[] = [];
  readonly finishIdleRequests: FinishIdleRequest[] = [];
  readonly loadContextRequests: LoadContextRequest[] = [];
  readonly markChildThreadClosedRequests: MarkChildThreadClosedRequest[] = [];
  readonly refreshRuntimeBindingTokenRequests: RefreshRuntimeBindingTokenRequest[] = [];
  readonly resolveInterAgentDeliveryRequests: ResolveInterAgentDeliveryRequest[] = [];
  readonly refreshErrors: Error[] = [];
  readonly writeEventRequests: WriteEventRequest[] = [];
  readonly writeRequestEndRequests: WriteRequestEndRequest[] = [];
  loadContextJSON: string | undefined;
  resolveInterAgentDeliveryMessageJSON = "";
  eventWriterAckStatus = BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED;
  eventWriterErrorCode = "";
  eventWriterAckWriteId: string | undefined;
  eventWriterCompactedThroughMessageSequence: number | undefined;
  eventWriterTransportError: Error | null = null;
  pendingAttachmentDeltaJson: string[] = [];
  readonly commitErrors: Error[] = [];
  commitInputsResponseMutator:
    | ((response: {
      declaration: {
        receipts: unknown[];
        observedBindingId: string;
        observedBindingGeneration: number;
        applicationDisposition: ReceiptApplicationDisposition;
      };
    }) => void)
    | undefined;

  constructor(
    private readonly commitStatus = BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED,
    private readonly commitError: Error | null = null,
  ) {}

  client(): AgentRuntimeBridgeServiceClient {
    return {
      commitInputs: this.commitInputs.bind(this),
      commitInternalToolRepair: this.commitInternalToolRepair.bind(this),
      commitRuntimeTermination: this.commitRuntimeTermination.bind(this),
      createChildThread: this.createChildThread.bind(this),
      finishIdle: this.finishIdle.bind(this),
      loadContext: this.loadContext.bind(this),
      markChildThreadClosed: this.markChildThreadClosed.bind(this),
      refreshRuntimeBindingToken: this.refreshRuntimeBindingToken.bind(this),
      resolveInterAgentDelivery: this.resolveInterAgentDelivery.bind(this),
      writeEvent: this.writeEvent.bind(this),
      writeRequestEnd: this.writeRequestEnd.bind(this),
    } as unknown as AgentRuntimeBridgeServiceClient;
  }

  private resolveInterAgentDelivery(
    request: ResolveInterAgentDeliveryRequest,
    _metadata: Metadata,
    callback: (error: Error | null, response: unknown) => void,
  ): unknown {
    this.resolveInterAgentDeliveryRequests.push(request);
    const completionPull = request.deliveryId.length === 0;
    callback(null, {
      ack: {
        status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED,
        runtimeInputId: "",
        runtimeWriteId: "",
        errorCode: "",
      },
      deliveryId: completionPull ? "delivery_resolved" : request.deliveryId,
      sourceThreadId: completionPull ? request.childThreadId : request.scope?.sessionThreadId ?? "",
      targetThreadId: completionPull ? request.scope?.sessionThreadId ?? "" : request.childThreadId,
      sourceToolUseEventId: "sevt_agent_mail_source",
      receivedEventId: "sevt_agent_mail_received",
      receivedSequence: 8,
      messageJson: this.resolveInterAgentDeliveryMessageJSON,
    });
    return grpcCall();
  }

  private commitInputs(
    request: CommitInputsRequest,
    _metadata: Metadata,
    options: CallOptions,
    callback: (error: Error | null, response: unknown) => void,
  ): unknown {
    this.commitInputsRequests.push(request);
    this.commitInputsCallOptions.push(options);
    const queuedError = this.commitErrors.shift();
    if (queuedError !== undefined) {
      callback(queuedError, null);
      return grpcCall();
    }
    if (this.commitError !== null) {
      callback(this.commitError, null);
      return grpcCall();
    }
    const response = {
      ack: {
        status: this.commitStatus,
        runtimeInputId: request.runtimeInputId,
        runtimeWriteId: "",
        errorCode: "",
      },
      declaration: {
        receipts: [{
          sessionThreadId: request.scope?.sessionThreadId ?? "",
          operationKind: "commit_inputs",
          sourceKind: request.inputKind,
          sourceId: request.runtimeInputId,
          events: request.eventIds.map((eventId, index) => ({
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            sourceEventId: eventId,
            eventId,
            eventSequence: request.sequenceFrom + index,
            disposition: 1,
          })),
          messages: request.drafts.map((draft, messageIndex) => ({
            runtimeLocalId: draft.runtimeLocalId,
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            owningEventId: draft.sourceEventId,
            messageId: `msg_${draft.runtimeLocalId}`,
            messageSequence: request.sequenceFrom + messageIndex,
            createdAt: "2026-07-28T00:00:00.000Z",
            updatedAt: "2026-07-28T00:00:00.000Z",
            disposition: 1,
            parts: draft.parts.map((part, partIndex) => ({
              runtimeLocalPartId: part.runtimeLocalPartId,
              partId: `part_${part.runtimeLocalPartId}`,
              messageId: `msg_${draft.runtimeLocalId}`,
              partSequence: partIndex,
              createdAt: "2026-07-28T00:00:00.000Z",
              updatedAt: "2026-07-28T00:00:00.000Z",
              disposition: 1,
            })),
          })),
          pendingAttachmentDeltaJson: this.pendingAttachmentDeltaJson,
          pendingToolDeltaJson: request.pendingToolCancellations.map((cancellation) => JSON.stringify({
            runtime_local_id: cancellation.runtimeLocalId,
            tool_use_event_id: cancellation.toolUseEventId,
            status: "cancelled",
            result_event_id: request.eventIds[0],
          })),
          prefixConsumptions: [],
          declarationDigest: commitInputsDeclarationDigest(request),
          requestReschedule: undefined,
          childLifecycle: [],
          idleCloseout: undefined,
          compactedThroughMessageSequence: undefined,
        }],
        observedBindingId: request.scope?.binding?.bindingId ?? "",
        observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
        applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
      },
    };
    this.commitInputsResponseMutator?.(response);
    callback(null, response);
    return grpcCall();
  }

  private commitInternalToolRepair(request: CommitInternalToolRepairRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.commitInternalToolRepairRequests.push(request);
    const draft = request.drafts[0];
    const part = draft?.parts[0];
    callback(null, {
      ack: {
        status: this.commitStatus,
        runtimeInputId: "",
        runtimeWriteId: request.modelToolCallId,
        errorCode: "",
      },
      declaration: {
        receipts: [{
          sessionThreadId: request.scope?.sessionThreadId ?? "",
          operationKind: "commit_internal_tool_repair",
          sourceKind: "internal_tool_repair",
          sourceId: draft?.sourceId ?? "",
          events: [{
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            sourceEventId: draft?.sourceId ?? "",
            eventId: "evt_internal_repair",
            eventSequence: 12,
            disposition: 2,
          }],
          messages: [{
            runtimeLocalId: draft?.runtimeLocalId ?? "",
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            owningEventId: "evt_internal_repair",
            messageId: "msg_durable_repair",
            messageSequence: 3,
            createdAt: "2026-01-01T00:00:00Z",
            updatedAt: "2026-01-01T00:00:00Z",
            disposition: 1,
            parts: [{
              runtimeLocalPartId: part?.runtimeLocalPartId ?? "",
              partId: "part_durable_repair",
              messageId: "msg_durable_repair",
              partSequence: 0,
              createdAt: "2026-01-01T00:00:00Z",
              updatedAt: "2026-01-01T00:00:00Z",
              disposition: 1,
            }],
          }],
          pendingAttachmentDeltaJson: [],
          pendingToolDeltaJson: [],
          prefixConsumptions: [],
          declarationDigest: internalToolRepairDeclarationDigest(request),
          requestReschedule: undefined,
          childLifecycle: [],
          idleCloseout: undefined,
          compactedThroughMessageSequence: undefined,
        }],
        observedBindingId: request.scope?.binding?.bindingId ?? "",
        observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
        applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
      },
    });
    return grpcCall();
  }

  private commitRuntimeTermination(request: CommitRuntimeTerminationRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.commitRuntimeTerminationRequests.push(request);
    if (this.eventWriterTransportError !== null) {
      callback(this.eventWriterTransportError, null);
      return grpcCall();
    }
    const now = "2026-01-01T00:00:00.000Z";
    const events = request.drafts.map((draft, index) => ({
      sessionThreadId: request.scope?.sessionThreadId ?? "",
      sourceEventId: request.runtimeWriteId,
      eventId: `evt_termination_${request.runtimeWriteId}_${index}`,
      eventSequence: index + 1,
      disposition: DurableEventDisposition.DURABLE_EVENT_DISPOSITION_CREATED,
    }));
    const messages = request.drafts.map((draft, index) => ({
      runtimeLocalId: draft.runtimeLocalId,
      sessionThreadId: request.scope?.sessionThreadId ?? "",
      owningEventId: events[index]!.eventId,
      messageId: `msg_termination_${request.runtimeWriteId}_${index}`,
      messageSequence: index + 1,
      createdAt: now,
      updatedAt: now,
      disposition: DurableProjectionDisposition.DURABLE_PROJECTION_DISPOSITION_CREATED,
      parts: draft.parts.map((part, partIndex) => ({
        runtimeLocalPartId: part.runtimeLocalPartId,
        partId: `part_termination_${request.runtimeWriteId}_${index}_${partIndex}`,
        messageId: `msg_termination_${request.runtimeWriteId}_${index}`,
        partSequence: partIndex,
        createdAt: now,
        updatedAt: now,
        disposition: DurableProjectionDisposition.DURABLE_PROJECTION_DISPOSITION_CREATED,
      })),
    }));
    callback(null, {
      ack: {
        status: this.eventWriterAckStatus,
        runtimeInputId: "",
        runtimeWriteId: this.eventWriterAckWriteId ?? request.runtimeWriteId,
        errorCode: this.eventWriterErrorCode,
      },
      declaration: {
        observedBindingId: request.scope?.binding?.bindingId ?? "",
        observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
        applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
        receipts: [{
          sessionThreadId: request.scope?.sessionThreadId ?? "",
          operationKind: "commit_runtime_termination",
          sourceKind: "runtime_termination",
          sourceId: request.runtimeWriteId,
          events,
          messages,
          pendingAttachmentDeltaJson: [],
          pendingToolDeltaJson: request.pendingToolCancellations.map((pending) => JSON.stringify({
            status: "cancelled",
            tool_use_event_id: pending.toolUseEventId,
          })),
          prefixConsumptions: [],
          declarationDigest: runtimeTerminationDeclarationDigest(request),
          requestReschedule: undefined,
          childLifecycle: [],
          idleCloseout: undefined,
          compactedThroughMessageSequence: undefined,
        }],
      },
    });
    return grpcCall();
  }

  private finishIdle(request: FinishIdleRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.finishIdleRequests.push(request);
    if (this.eventWriterTransportError !== null) {
      callback(this.eventWriterTransportError, null);
      return grpcCall();
    }
    const now = "2026-01-01T00:00:00.000Z";
    const idleEventId = `evt_idle_${request.durableTurnId}`;
    const completionEventId = `evt_mail_${request.durableTurnId}`;
    callback(null, {
      ack: {
        status: this.eventWriterAckStatus,
        runtimeInputId: "",
        runtimeWriteId: this.eventWriterAckWriteId ?? request.durableTurnId,
        errorCode: this.eventWriterErrorCode,
      },
      declaration: {
        observedBindingId: request.scope?.binding?.bindingId ?? "",
        observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
        applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
        receipts: [{
          sessionThreadId: request.scope?.sessionThreadId ?? "",
          operationKind: "finish_idle",
          sourceKind: "turn_closeout",
          sourceId: request.durableTurnId,
          declarationDigest: finishIdleDeclarationDigest(request),
          pendingAttachmentDeltaJson: [],
          pendingToolDeltaJson: [],
          prefixConsumptions: [],
          childLifecycle: [],
          events: [
            {
              sessionThreadId: request.scope?.sessionThreadId ?? "",
              sourceEventId: request.durableTurnId,
              eventId: idleEventId,
              eventSequence: 1,
              disposition: 2,
            },
            ...request.drafts.map((draft) => ({
              sessionThreadId: request.scope?.sessionThreadId ?? "",
              sourceEventId: request.durableTurnId,
              eventId: completionEventId,
              eventSequence: 2,
              disposition: 2,
            })),
          ],
          messages: request.drafts.map((draft, messageIndex) => ({
            runtimeLocalId: draft.runtimeLocalId,
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            owningEventId: completionEventId,
            messageId: `msg_finish_idle_${messageIndex}`,
            messageSequence: messageIndex + 1,
            createdAt: now,
            updatedAt: now,
            disposition: 1,
            parts: draft.parts.map((part, partIndex) => ({
              runtimeLocalPartId: part.runtimeLocalPartId,
              partId: `part_finish_idle_${messageIndex}_${partIndex}`,
              messageId: `msg_finish_idle_${messageIndex}`,
              partSequence: partIndex,
              createdAt: now,
              updatedAt: now,
              disposition: 1,
            })),
          })),
          requestReschedule: undefined,
          idleCloseout: {
            durableTurnId: request.durableTurnId,
            idleEventId,
            idleEventSequence: 1,
            committedIdleAt: now,
          },
          compactedThroughMessageSequence: undefined,
        }],
      },
    });
    return grpcCall();
  }

  private createChildThread(request: CreateChildThreadRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.createChildThreadRequests.push(request);
    callback(null, {
      ack: {
        status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED,
        runtimeInputId: "",
        runtimeWriteId: "",
        errorCode: "",
      },
      childThreadId: request.childThreadId,
    });
    return grpcCall();
  }

  private loadContext(request: LoadContextRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.loadContextRequests.push(request);
    callback(null, {
      ack: {
        status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED,
        runtimeInputId: request.runtimeInputId,
        runtimeWriteId: "",
        errorCode: "",
      },
      contextJson: this.loadContextJSON ?? JSON.stringify({
        messages: [durableRuntimeMessage(`msg_context_${request.scope?.sessionThreadId ?? "unknown"}`, "context")],
        turnFacts: {
          events: [],
          messageLineage: [{
            messageId: `msg_context_${request.scope?.sessionThreadId ?? "unknown"}`,
            messageSequence: 1,
            owningEventId: `evt_msg_context_${request.scope?.sessionThreadId ?? "unknown"}`,
            entries: [{
              lineageKind: "declaration_receipt",
              operationKind: "commit_inputs",
              sourceKind: "messages",
              sourceId: `rin_context_${request.scope?.sessionThreadId ?? "unknown"}`,
              eventId: `evt_msg_context_${request.scope?.sessionThreadId ?? "unknown"}`,
              eventSequence: 1,
              disposition: "created",
            }],
          }],
        },
        thread: request.scope?.sessionThreadId === "thr_child"
          ? {
              parentThreadId: "thr_main",
              role: "subagent",
              visibility: "public",
              taskName: "worker",
              agentType: "worker",
              status: "idle",
            }
          : {
              parentThreadId: null,
              role: "main",
              visibility: "public",
              taskName: null,
              agentType: "general",
              status: "idle",
            },
        runtimeConfig: {
          configGeneration: 11,
          approvalMode: "approve_for_me",
          system: "Operate as the session specialist.",
          memoryStores: [{
            memoryStoreId: "memstore_notes",
            name: "Project notes",
            access: "read_write",
            instructions: null,
          }],
          toolPolicy: {
            approvalMode: "approve_for_me",
            tools: { configs: [{ name: "Write", enabled: true, permissionPolicy: "ask" }] },
          },
          skills: [{ skillId: "sk_docs", version: "latest" }],
          skillsIndex: [{
            skill_id: "sk_docs",
            skill_version_id: "skv_docs_3",
            version: "3.0.0",
            name: "Docs",
            description: "Read project documentation.",
            directory: "docs",
          }],
          installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
        },
        mcpManifests: [{
          mcpServerName: "github",
          manifestETag: "etag_7",
          manifestGeneration: 7,
          tools: [{ name: "github_search", description: "Search GitHub", inputSchema: { type: "object" } }],
        }, {
          mcpServerName: "gitlab",
          manifestGeneration: 8,
          readiness: "unready",
          diagnostic: "delivery_exhausted",
          tools: [],
        }],
        pendingToolUses: [{
          toolUseEventId: "evt_pending_tool",
          modelRequestId: "mrq_pending_tool",
          modelToolCallId: "toolu_pending",
          toolName: "Write",
          input: { path: "/workspace/file.txt" },
          decision: "deny",
          denyMessage: "not safe",
          status: "resolving",
        }],
        pendingSandboxExecutions: [{
          toolUseEventId: "evt_pending_sandbox",
          modelRequestId: "mrq_pending_sandbox",
          modelToolCallId: "toolu_pending_sandbox",
          toolName: "Bash",
          input: { command: "pwd" },
          executionState: "running",
        }],
        backgroundTools: [{
          taskId: "task_loaded",
          sourceToolUseEventId: "evt_background_tool",
        }],
        pendingAttachments: [{
          origin: {
            transient: {
              attachmentRef: "att_loaded",
              sourceToolUseEventId: "evt_loaded_tool",
              sourcePath: "mcp:github/plot.png",
              pageRange: "1-2",
              detail: "auto",
            },
          },
          mime: "image/png",
          filename: "plot.png",
        }, {
          origin: {
            fileBacked: {
              sourceEventId: "evt_loaded_user",
              fileId: "file_loaded_pdf",
            },
          },
          mime: "application/pdf",
          filename: "brief.pdf",
        }],
        coldCoverage: {
          pendingToolIds: ["evt_pending_tool"],
          pendingSandboxExecutionIds: ["evt_pending_sandbox"],
          pendingAttachmentIdentities: [
            "transient:evt_loaded_tool:att_loaded",
            "file:evt_loaded_user:file_loaded_pdf",
          ],
          undeliveredMailDeliveryIds: [],
        },
      }),
      runtimeBindingToken: `token_${request.scope?.sessionThreadId ?? "unknown"}`,
    });
    return grpcCall();
  }

  private markChildThreadClosed(request: MarkChildThreadClosedRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.markChildThreadClosedRequests.push(request);
    const sourceKind = request.source?.reviewerReviewId === undefined ? "tool_use" : "approval_review";
    const sourceCommandId = request.source?.reviewerReviewId ?? request.source?.sourceToolUseEventId ?? "";
    const operationId = stableRuntimeID("child_tree_close", sourceCommandId, request.childThreadId);
    callback(null, {
      ack: {
        status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED,
        runtimeInputId: "",
        runtimeWriteId: operationId,
        errorCode: "",
      },
      declaration: {
        receipts: [{
          sessionThreadId: request.childThreadId,
          operationKind: "mark_child_thread_closed",
          sourceKind: "child_close_command",
          sourceId: operationId,
          events: [],
          messages: [],
          pendingAttachmentDeltaJson: [],
          pendingToolDeltaJson: [],
          prefixConsumptions: [],
          declarationDigest: childLifecycleDeclarationDigest({
            operationKind: "mark_child_thread_closed",
            action: "close",
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            childThreadId: request.childThreadId,
            sourceKind,
            sourceCommandId,
            requestedAt: request.closedAt,
          }),
          childLifecycle: [{
            childThreadId: request.childThreadId,
            disposition: ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_CLOSED,
            effectiveAt: request.closedAt,
          }],
        }],
        applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
        observedBindingId: request.scope?.binding?.bindingId ?? "",
        observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
      },
    });
    return grpcCall();
  }

  private refreshRuntimeBindingToken(request: RefreshRuntimeBindingTokenRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.refreshRuntimeBindingTokenRequests.push(request);
    const error = this.refreshErrors.shift();
    if (error !== undefined) {
      callback(error, null);
      return grpcCall();
    }
    callback(null, { runtimeBindingToken: "runtime-binding-token-refreshed" });
    return grpcCall();
  }

  private writeEvent(request: WriteEventRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.writeEventRequests.push(request);
    if (this.eventWriterTransportError !== null) {
      callback(this.eventWriterTransportError, null);
      return grpcCall();
    }
    const eventId = `evt_${request.runtimeWriteId}`;
    const now = "2026-01-01T00:00:00.000Z";
    callback(null, {
      ack: {
        status: this.eventWriterAckStatus,
        runtimeInputId: "",
        runtimeWriteId: this.eventWriterAckWriteId ?? request.runtimeWriteId,
        errorCode: this.eventWriterErrorCode,
      },
      eventId,
      sequence: 1,
      declaration: {
        observedBindingId: request.scope?.binding?.bindingId ?? "",
        observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
        applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
        receipts: [{
          sessionThreadId: request.scope?.sessionThreadId ?? "",
          operationKind: "write_event",
          sourceKind: request.eventType,
          sourceId: request.runtimeWriteId,
          declarationDigest: writeEventDeclarationDigest(request),
          pendingAttachmentDeltaJson: [],
          pendingToolDeltaJson: [],
          prefixConsumptions: [],
          compactedThroughMessageSequence: this.eventWriterCompactedThroughMessageSequence,
          childLifecycle: [],
          events: [{
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            sourceEventId: request.runtimeWriteId,
            eventId,
            eventSequence: 1,
            disposition: 2,
          }],
          messages: request.drafts.map((draft, messageIndex) => ({
            runtimeLocalId: draft.runtimeLocalId,
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            owningEventId: eventId,
            messageId: `msg_committed_${messageIndex}`,
            messageSequence: messageIndex + 1,
            createdAt: now,
            updatedAt: now,
            disposition: 1,
            parts: draft.parts.map((part, partIndex) => ({
              runtimeLocalPartId: part.runtimeLocalPartId,
              partId: `part_committed_${messageIndex}_${partIndex}`,
              messageId: `msg_committed_${messageIndex}`,
              partSequence: partIndex,
              createdAt: now,
              updatedAt: now,
              disposition: 1,
            })),
          })),
        }],
      },
    });
    return grpcCall();
  }

  private writeRequestEnd(request: WriteRequestEndRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.writeRequestEndRequests.push(request);
    if (this.eventWriterTransportError !== null) {
      callback(this.eventWriterTransportError, null);
      return grpcCall();
    }
    const now = "2026-01-01T00:00:00.000Z";
    const eventId = `evt_${request.runtimeWriteId}`;
    const interruptRequest = request.interruptSettlement === undefined
      ? undefined
      : {
          scope: request.scope,
          runtimeInputId: request.interruptSettlement.runtimeInputId,
          eventIds: request.interruptSettlement.eventIds,
          sequenceFrom: request.interruptSettlement.sequenceFrom,
          sequenceTo: request.interruptSettlement.sequenceTo,
          inputKind: "interrupt_control",
          drafts: request.drafts.filter(
            (draft) => draft.draftKind === RuntimeDraftKind.RUNTIME_DRAFT_KIND_CANCELLATION,
          ),
          pendingToolCancellations: request.interruptSettlement.pendingToolCancellations,
          sandboxExecutionToolUseEventIds: request.interruptSettlement.sandboxExecutionToolUseEventIds,
        };
    const interruptReceipt = interruptRequest === undefined
      ? undefined
      : {
          sessionThreadId: request.scope?.sessionThreadId ?? "",
          operationKind: "commit_inputs",
          sourceKind: "interrupt_control",
          sourceId: interruptRequest.runtimeInputId,
          declarationDigest: commitInputsDeclarationDigest(interruptRequest),
          pendingAttachmentDeltaJson: [],
          pendingToolDeltaJson: [],
          prefixConsumptions: [],
          childLifecycle: [],
          events: [{
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            sourceEventId: interruptRequest.runtimeInputId,
            eventId: interruptRequest.eventIds[0] ?? "",
            eventSequence: interruptRequest.sequenceFrom,
            disposition: 1,
          }],
          messages: interruptRequest.drafts.map((draft, messageIndex) => ({
            runtimeLocalId: draft.runtimeLocalId,
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            owningEventId: draft.sourceEventId,
            messageId: `msg_interrupt_${messageIndex}`,
            messageSequence: messageIndex + 1,
            createdAt: now,
            updatedAt: now,
            disposition: 1,
            parts: draft.parts.map((part, partIndex) => ({
              runtimeLocalPartId: part.runtimeLocalPartId,
              partId: `part_interrupt_${messageIndex}_${partIndex}`,
              messageId: `msg_interrupt_${messageIndex}`,
              partSequence: partIndex,
              createdAt: now,
              updatedAt: now,
              disposition: 1,
            })),
          })),
          requestReschedule: undefined,
          idleCloseout: undefined,
          compactedThroughMessageSequence: undefined,
        };
    callback(null, {
      ack: {
        status: this.eventWriterAckStatus,
        runtimeInputId: "",
        runtimeWriteId: this.eventWriterAckWriteId ?? request.runtimeWriteId,
        errorCode: this.eventWriterErrorCode,
      },
      rescheduleDisposition: request.reschedule === undefined ? undefined : {
        status: "accepted",
        denialReason: "",
        attempt: request.reschedule.attempt,
        effectiveDeadline: request.reschedule.deadline,
      },
      declaration: {
        observedBindingId: request.scope?.binding?.bindingId ?? "",
        observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
        applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
        receipts: [
          ...(interruptReceipt === undefined ? [] : [interruptReceipt]),
          {
          sessionThreadId: request.scope?.sessionThreadId ?? "",
          operationKind: "write_request_end",
          sourceKind: "model_request",
          sourceId: request.modelRequestId,
          declarationDigest: writeRequestEndDeclarationDigest(request),
          pendingAttachmentDeltaJson: [],
          pendingToolDeltaJson: [],
          prefixConsumptions: [],
          childLifecycle: [],
          events: [{
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            sourceEventId: request.modelRequestId,
            eventId,
            eventSequence: 1,
            disposition: 2,
          }],
          messages: request.drafts.map((draft, messageIndex) => ({
            runtimeLocalId: draft.runtimeLocalId,
            sessionThreadId: request.scope?.sessionThreadId ?? "",
            owningEventId: eventId,
            messageId: `msg_request_end_${messageIndex}`,
            messageSequence: messageIndex + 1,
            createdAt: now,
            updatedAt: now,
            disposition: 1,
            parts: draft.parts.map((part, partIndex) => ({
              runtimeLocalPartId: part.runtimeLocalPartId,
              partId: `part_request_end_${messageIndex}_${partIndex}`,
              messageId: `msg_request_end_${messageIndex}`,
              partSequence: partIndex,
              createdAt: now,
              updatedAt: now,
              disposition: 1,
            })),
          })),
          requestReschedule: request.reschedule === undefined
            ? undefined
            : {
                disposition: RequestRescheduleDisposition.REQUEST_RESCHEDULE_DISPOSITION_ACCEPTED,
                requestKind: request.requestKind,
                attempt: request.reschedule.attempt,
                effectiveDeadline: request.reschedule.deadline,
              },
          idleCloseout: undefined,
          compactedThroughMessageSequence: request.compactedThroughMessageSequence,
          },
        ],
      },
    });
    return grpcCall();
  }
}

function grpcCall(): { readonly cancel: () => void } {
  return { cancel: () => undefined };
}
