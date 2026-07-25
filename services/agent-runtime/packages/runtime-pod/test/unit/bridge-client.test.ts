import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { Metadata, status } from "@grpc/grpc-js";
import {
  BridgeWriteStatus,
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
  WriteEventRequest,
  WriteRequestEndRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { RuntimeApprovalReviewRequest } from "@tetral/agent-runtime-core/src/agent-loop/agent-loop.js";
import { ProviderMetadataSchema } from "@tetral/agent-runtime-core/src/contracts/provider.js";
import { AutoApprovalReviewerManager } from "@tetral/agent-runtime-core/src/session/approval-reviewer-manager.js";
import type { RuntimeAcceptedInputState, RuntimeThreadControlState } from "@tetral/agent-runtime-core/src/session/session-state.js";
import type { RuntimeSessionIdentity } from "@tetral/agent-runtime-core/src/session/session.js";
import { BridgeAPIApprovalReviewerThreadCreator, BridgeAPIContextLoader, BridgeAPIControlInputCommitter, BridgeAPIEventWriter, BridgeAPIInternalToolRepairCommitter } from "../../src/bridge-client.js";
import {
  buildBridgeClientRuntimeMessage as runtimeMessage,
  buildBridgeClientRuntimeRepairMessage as runtimeRepairMessage,
} from "../../../core/test/unit/runtime-message-builders.js";

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

  test("retries stale-generation refresh failures with a bounded policy", async () => {
    const bridge = new RecordingBridgeClient();
    bridge.refreshErrors.push(
      Object.assign(new Error("stale binding"), { code: status.FAILED_PRECONDITION }),
      Object.assign(new Error("stale binding"), { code: status.FAILED_PRECONDITION }),
    );
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

    await expect(loader.refreshRuntimeBindingToken(bindingIdentity("opaque-token"), { force: true })).resolves.toBe("runtime-binding-token-refreshed");
    expect(bridge.refreshRuntimeBindingTokenRequests).toHaveLength(3);
    expect(sleeps).toEqual([100, 300]);
  });

  test("stacks short-lived reviewer commit scopes without replacing the durable active input", () => {
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: new RecordingBridgeClient().client(),
      metadataFactory: async () => new Metadata(),
    });
    const durable: RuntimeAcceptedInputState = {
      ...control("thr_reviewer", "rin_durable", 1),
      kind: "messages",
      payloadJson: "{}",
    };
    const first: RuntimeAcceptedInputState = {
      ...control("thr_reviewer", "rin_first_review", 2),
      kind: "messages",
      payloadJson: "{}",
    };
    const second: RuntimeAcceptedInputState = {
      ...control("thr_reviewer", "rin_second_review", 3),
      kind: "messages",
      payloadJson: "{}",
    };
    loader.registerAcceptedInput(durable);
    const unregisterFirst = loader.registerScopedAcceptedInput(first);
    const unregisterSecond = loader.registerScopedAcceptedInput(second);

    expect(loader.acceptedInputForThread("sesn_1", "thr_reviewer")?.runtimeInputId).toBe("rin_second_review");
    unregisterFirst();
    expect(loader.acceptedInputForThread("sesn_1", "thr_reviewer")?.runtimeInputId).toBe("rin_second_review");
    unregisterSecond();
    expect(loader.acceptedInputForThread("sesn_1", "thr_reviewer")?.runtimeInputId).toBe("rin_durable");
  });

  test("commits accepted input by thread and preserves inter-agent delivery payloads", async () => {
    const bridge = new RecordingBridgeClient();
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const mainInput: RuntimeAcceptedInputState = {
      ...control("thr_main", "rin_main", 1),
      kind: "messages",
      payloadJson: '{"messages":["user"]}',
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

    await loader.commitAcceptedInput(mainInput);
    await loader.commitAcceptedInput(childInput);

    expect(bridge.commitInputsRequests.find((request) => request.runtimeInputId === "rin_main")).toMatchObject({
      inputKind: "messages",
      hotContextPatchJson: mainInput.payloadJson,
    });

    const childCommit = bridge.commitInputsRequests.find((request) => request.runtimeInputId === "rin_child");
    if (childCommit === undefined) {
      throw new Error("missing child commit request");
    }
    expect(childCommit).toMatchObject({
      inputKind: "inter_agent_message",
      scope: {
        sessionId: "sesn_1",
        sessionThreadId: "thr_child",
      },
      runtimeInputId: "rin_child",
      eventIds: ["sevt_2"],
      sequenceFrom: 2,
      sequenceTo: 2,
      hotContextPatchJson: "{}",
      approvalReviewJson: "",
    });
    expect(JSON.parse(childCommit.interAgentMessageJson)).toMatchObject({
      delivery_id: "iam_thr_main_sevt_spawn_thr_child",
      source_thread_id: "thr_main",
      source_tool_use_event_id: "sevt_spawn",
      message: {
        id: "msg_child_input",
        origin: "runtime",
        role: "user",
      },
    });
    expect(loader.acceptedInputForSession("sesn_1")).toBeUndefined();
    expect(loader.acceptedInputForThread("sesn_1", "thr_main")?.runtimeInputId).toBe("rin_main");
    expect(loader.acceptedInputForThread("sesn_1", "thr_child")?.runtimeInputId).toBe("rin_child");

    const childContext = await loader.buildThreadContext({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thr_child",
      bindingId: "bind_1",
      bindingGeneration: 7,
      runtimeBindingToken: "token_thr_child",
      targetPodUid: "pod_thr_child",
    });

    expect(childContext.map((message) => message.id)).toEqual(["msg_context_thr_child"]);
    const loadedContext = await loader.loadThreadContext(control("thr_child", "rin_child_reload", 3));
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
      kind: "approval",
      input: { path: "/workspace/file.txt" },
      decision: "deny",
      denyMessage: "not safe",
      status: "resolving",
      expiresAt: "2026-01-01T00:30:00Z",
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
      "thr_main",
      "thr_child",
      "thr_child",
      "thr_child",
    ]);
  });

  test("pull receipt commits without replacing the parent run's active scope", async () => {
    const bridge = new RecordingBridgeClient();
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const active: RuntimeAcceptedInputState = {
      ...control("thr_main", "rin_active_parent", 1),
      kind: "messages",
      payloadJson: '{"messages":["active"]}',
    };
    const pulled: RuntimeAcceptedInputState = {
      ...control("thr_main", "agent_mail:delivery_pull", 0),
      eventIds: [],
      sequenceFrom: 0,
      sequenceTo: 0,
      kind: "inter_agent_message",
      deliveryId: "delivery_pull",
      sourceThreadId: "thr_child",
      sourceToolUseEventId: "sevt_spawn_pull",
      message: runtimeMessage("msg_pull", "completion"),
      presentation: "pull",
    };
    loader.registerAcceptedInput(active);

    const loadCountBeforeReceipt = bridge.loadContextRequests.length;
    const result = await loader.commitAcceptedInput(pulled, { registerScope: false, hydrateContext: false });

    expect(result).toEqual({ type: "receipt", inputDisposition: "committed" });
    expect(bridge.loadContextRequests).toHaveLength(loadCountBeforeReceipt);
    expect(loader.acceptedInputForThread("sesn_1", "thr_main")?.runtimeInputId).toBe("rin_active_parent");
    expect(JSON.parse(bridge.commitInputsRequests.at(-1)?.interAgentMessageJson ?? "{}")).toMatchObject({
      delivery_id: "delivery_pull",
      presentation: "pull",
    });
  });
});

describe("BridgeAPIControlInputCommitter", () => {
  test("commits interrupt and tool-confirmation control inputs without message projection payloads", async () => {
    const bridge = new RecordingBridgeClient(BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE);
    const committer = new BridgeAPIControlInputCommitter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const scope = control("thr_main", "rin_interrupt", 7);

    const interrupt = await committer.commitControlInput({ scope, inputKind: "interrupt_control" });
    const confirmation = await committer.commitControlInput({
      scope: { ...scope, runtimeInputId: "rin_confirm", eventIds: ["sevt_confirm"] },
      inputKind: "tool_confirmation",
    });

    expect(interrupt).toEqual({ ok: true });
    expect(confirmation).toEqual({ ok: true });
    expect(bridge.commitInputsRequests).toEqual([
      expect.objectContaining({
        runtimeInputId: "rin_interrupt",
        inputKind: "interrupt_control",
        eventIds: ["sevt_7"],
        sequenceFrom: 7,
        sequenceTo: 7,
        hotContextPatchJson: "{}",
        interAgentMessageJson: "",
        approvalReviewJson: "",
      }),
      expect.objectContaining({
        runtimeInputId: "rin_confirm",
        inputKind: "tool_confirmation",
        eventIds: ["sevt_confirm"],
        sequenceFrom: 7,
        sequenceTo: 7,
        hotContextPatchJson: "{}",
        interAgentMessageJson: "",
        approvalReviewJson: "",
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
    });

    expect(result).toMatchObject({
      ok: false,
      retryable: true,
      errorCode: "bridge_commit_unavailable",
    });
  });
});

describe("BridgeAPIEventWriter", () => {
  test("maps internal provider failures to fork-SDK session errors before WriteEvent", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      scopeForThread: () => ({
        ...control("thr_main", "rin_main", 1),
        kind: "messages",
        payloadJson: "{}",
      }),
    });

    await writer.append({
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

  test("writes pre-commit running status with a registered accepted input scope", async () => {
    const bridge = new RecordingBridgeClient();
    const loader = new BridgeAPIContextLoader({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
    });
    const input: RuntimeAcceptedInputState = {
      ...control("thr_main", "rin_main", 1),
      kind: "messages",
      payloadJson: "{}",
    };
    const unregister = loader.registerAcceptedInput(input);
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      scopeForThread: (sessionId, sessionThreadId) => loader.acceptedInputForThread(sessionId, sessionThreadId),
    });

    const result = await writer.append({
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_running",
      event: { type: "session.status_running" },
    });

    expect(result).toMatchObject({ ok: true, writeId: "rwrite_running" });
    expect(bridge.commitInputsRequests).toEqual([]);
    expect(bridge.writeEventRequests[0]).toMatchObject({
      runtimeWriteId: "rwrite_running",
      scope: {
        requestId: "req_1",
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        binding: {
          bindingId: "bind_1",
          bindingGeneration: 7,
          targetPodUid: "pod_1",
        },
      },
    });
    unregister();
    expect(loader.acceptedInputForThread("sesn_1", "thr_main")).toBeUndefined();
  });

  test("passes internal projection JSON to Bridge WriteEvent without changing public payload", async () => {
    const fixture = JSON.parse(readFileSync(resolve(import.meta.dir, "../../../../../../testdata/stable-reasoning-anchor-vector.json"), "utf8")) as {
      readonly model_request_id: string;
      readonly event: {
        readonly type: "agent.tool_use";
        readonly name: string;
        readonly input: Record<string, string>;
        readonly evaluated_permission: "allow";
      };
      readonly projection_json: string;
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
      scopeForThread: () => ({
        ...control("thr_main", "rin_main", 1),
        kind: "messages",
        payloadJson: "{}",
      }),
    });

    const result = await writer.append({
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_tool",
      event: fixture.event,
      projectionJson: fixture.projection_json,
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
    expect(bridge.writeEventRequests[0]?.projectionJson).toBe(fixture.projection_json);
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
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_text_projection",
      event: { type: "agent.message", content: [{ type: "text", text: "answer" }] },
      projectionJson: JSON.stringify({
        type: "runtime_text_projection",
        message_id: "msg_1",
        part_id: "part_text_1",
        part_sequence: 2,
        truncated: false,
      }),
      modelRequestId: fixture.model_request_id,
    });
    expect(bridge.writeEventRequests[1]?.modelRequestId).toBe(fixture.model_request_id);

    await writer.append({
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_tool_result",
      event: {
        type: "agent.tool_result",
        tool_use_id: "sevt_tool_use_1",
        content: [{ type: "text", text: "done" }],
      },
      projectionJson: JSON.stringify({
        type: "runtime_tool_projection",
        message_id: "msg_1",
        part_id: "part_tool_1",
        part_sequence: 3,
        model_tool_call_id: "call_1",
        tool_name: "Read",
        input: { path: "a.txt" },
        state: "completed",
        output: { text: "done", truncated: false },
      }),
      modelRequestId: fixture.model_request_id,
    });
    expect(bridge.writeEventRequests[2]?.modelRequestId).toBe(fixture.model_request_id);

    await writer.append({
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_tool_without_anchor",
      event: fixture.event,
      projectionJson: fixture.projection_json,
    });
    expect(bridge.writeEventRequests[3]?.modelRequestId).toBe("");
    expect(bridge.writeEventRequests[3]?.stableReasoningParts).toEqual([]);
  });

  test("attaches web server-tool usage only on the durable tool-result request", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      scopeForThread: () => ({
        ...control("thr_main", "rin_main", 1),
        kind: "messages",
        payloadJson: "{}",
      }),
    });

    await writer.append({
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_web_result",
      event: {
        type: "agent.tool_result",
        tool_use_id: "evt_web_use",
        content: [{ type: "text", text: "web result" }],
      },
      projectionJson: JSON.stringify({
        type: "runtime_tool_projection",
        message_id: "msg_web",
        part_id: "part_web",
        part_sequence: 0,
        model_tool_call_id: "call_web",
        tool_name: "web",
        input: { search_query: [{ q: "tetral" }] },
        state: "completed",
        output: { text: "web result", truncated: false },
      }),
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
      scopeForThread: (sessionId, sessionThreadId) =>
        sessionId === "sesn_1" && sessionThreadId === "thr_reviewer" ? reviewerInput : undefined,
    });

    const result = await writer.append({
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
      scopeForThread: (sessionId, sessionThreadId) =>
        sessionId === "sesn_1" && sessionThreadId === "thr_reviewer" ? reviewerInput : undefined,
    });

    const result = await writer.append({
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
    const releasedScopes: string[] = [];
    const creator = new BridgeAPIApprovalReviewerThreadCreator({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      releaseThreadScope: (sessionId, sessionThreadId) => releasedScopes.push(`${sessionId}/${sessionThreadId}`),
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
      currentRequestTurnMessages: [],
      siblingToolCalls: [],
      policyContext: {},
    };

    await expect(creator.createApprovalReviewerThread({
      request: reviewRequest,
      reviewId: "arvw_1",
      reviewerThreadId: "thr_reviewer",
      isTrunk: true,
      forkSeedJson: "",
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
      forkSeedJson: "",
      isTrunk: true,
      reviewerReviewId: "",
    });

    const forkSeedJson = JSON.stringify({
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
      forkSeedJson,
    })).resolves.toEqual({ ok: true });
    expect(bridge.createChildThreadRequests[1]).toMatchObject({
      childThreadId: "thr_reviewer_sidecar",
      role: "approval_reviewer",
      forkTurns: "all",
      forkSeedJson,
      isTrunk: false,
      reviewerReviewId: "arvw_sidecar",
    });
    expect(releasedScopes).toEqual([]);

    await expect(creator.closeApprovalReviewerThread({
      request: reviewRequest,
      reviewId: "arvw_sidecar",
      reviewerThreadId: "thr_reviewer_sidecar",
      isTrunk: false,
      forkSeedJson,
    })).resolves.toEqual({ ok: true });
    expect(bridge.markChildThreadClosedRequests).toEqual([
      expect.objectContaining({
        childThreadId: "thr_reviewer_sidecar",
        scope: expect.objectContaining({ sessionThreadId: "thr_main" }),
      }),
    ]);
    expect(releasedScopes).toEqual(["sesn_1/thr_reviewer_sidecar"]);
  });

  test("serializes request-end usage with total input tokens for Bridge normalization", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      scopeForThread: () => ({
        ...control("thr_main", "rin_main", 1),
        kind: "messages",
        payloadJson: "{}",
      }),
    });

    const result = await writer.writeRequestEnd({
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_end",
      modelRequestId: "mreq_1",
      modelRequestStartEventId: "sevt_start",
      isError: false,
      finishReason: "stop",
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

    expect(result).toMatchObject({ ok: true, writeId: "rwrite_end" });
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
    expect(bridge.writeRequestEndRequests[0]?.requestKind).toBe("");

    const errorResult = await writer.writeRequestEnd({
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

    const staleTerminalResult = await writer.writeRequestEnd({
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_stale_terminal",
      modelRequestId: "mreq_stale_terminal",
      modelRequestStartEventId: "sevt_stale_terminal_start",
      isError: true,
      errorKind: "gateway_stream_error",
      finishReason: "error",
    });
    expect(staleTerminalResult).toMatchObject({
      ok: true,
      rescheduleDisposition: { status: "denied", reason: "stale_terminal", attempt: 0 },
    });
  });

  test("carries closeout sentinel acknowledgements across every closeout writer", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      scopeForThread: () => ({
        ...control("thr_main", "rin_main", 1),
        kind: "messages",
        payloadJson: "{}",
      }),
    });
    const operations = [
      () => writer.append({
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_error",
        event: { type: "session.status_running" as const },
      }),
      () => writer.finishIdle!({
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_idle",
        idleSince: "2026-01-01T00:00:00.000Z",
        stopReason: { type: "end_turn" as const },
      }),
      () => writer.writeRequestEnd!({
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
      scopeForThread: () => ({
        ...control("thr_main", "rin_main", 1),
        kind: "messages",
        payloadJson: "{}",
      }),
    });

    const result = await writer.commitRuntimeTermination!({
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
      scopeForThread: () => ({
        ...control("thr_main", "rin_main", 1),
        kind: "messages",
        payloadJson: "{}",
      }),
    });
    const operations = [
      () => writer.append({
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_requested_append",
        event: { type: "session.status_running" as const },
      }),
      () => writer.finishIdle!({
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_requested_idle",
        idleSince: "2026-01-01T00:00:00.000Z",
        stopReason: { type: "end_turn" as const },
      }),
      () => writer.writeRequestEnd!({
        sessionId: "sesn_1",
        sessionThreadId: "thr_main",
        writeId: "rwrite_requested_end",
        modelRequestId: "mreq_requested_end",
        modelRequestStartEventId: "sevt_requested_end",
        isError: false,
        finishReason: "stop",
      }),
      () => writer.commitRuntimeTermination!({
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
        release = () => callback(null, {
          ack: {
            status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED,
            runtimeInputId: "",
            runtimeWriteId: request.runtimeWriteId,
            errorCode: "",
          },
        });
        return grpcCall();
      },
    } as unknown as AgentRuntimeBridgeServiceClient;
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client,
      metadataFactory: async () => new Metadata(),
      scopeForThread: () => ({
        ...control("thr_main", "rin_main", 1),
        kind: "messages",
        payloadJson: "{}",
      }),
    });
    let settled = false;
    const result = writer.finishIdle!({
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_direct_await",
      idleSince: "2026-01-01T00:00:00.000Z",
      stopReason: { type: "end_turn" },
    }).finally(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);
    expect(release).toBeDefined();

    release?.();
    expect(await result).toMatchObject({ ok: true, writeId: "rwrite_direct_await" });
  });

  test("normalizes retryable closeout transport statuses without collapsing unknown failures", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      scopeForThread: () => ({
        ...control("thr_main", "rin_main", 1),
        kind: "messages",
        payloadJson: "{}",
      }),
    });

    for (const [grpcCode, writerCode] of [
      [status.UNAVAILABLE, "unavailable"],
      [status.DEADLINE_EXCEEDED, "timeout"],
      [status.ABORTED, "unavailable"],
      [status.RESOURCE_EXHAUSTED, "unavailable"],
    ] as const) {
      bridge.eventWriterTransportError = Object.assign(new Error("transport failed"), { code: grpcCode });
      expect(await writer.append({
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
      scopeForThread: () => input,
    });

    const result = await writer.writeRequestEnd({
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

  test("commits normalized terminal failure through the atomic Bridge closeout", async () => {
    const bridge = new RecordingBridgeClient();
    const writer = new BridgeAPIEventWriter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      scopeForThread: () => ({ ...control("thr_main", "rin_terminate", 3), kind: "messages", payloadJson: "{}" }),
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
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      writeId: "rwrite_terminate",
      failure,
    });

    expect(result).toMatchObject({ ok: true, writeId: "rwrite_terminate" });
    expect(bridge.commitRuntimeTerminationRequests).toEqual([expect.objectContaining({
      runtimeWriteId: "rwrite_terminate",
      failureJson: JSON.stringify(failure),
      scope: expect.objectContaining({ sessionId: "sesn_1", sessionThreadId: "thr_main" }),
    })]);
  });
});

describe("BridgeAPIInternalToolRepairCommitter", () => {
  test("commits event-less internal invalid-tool repair through the Bridge scope fence", async () => {
    const bridge = new RecordingBridgeClient();
    const committer = new BridgeAPIInternalToolRepairCommitter({
      address: "bridge.test:9090",
      tokenPath: "/var/run/token",
      client: bridge.client(),
      metadataFactory: async () => new Metadata(),
      scopeForThread: () => ({
        ...control("thr_main", "rin_main", 1),
        kind: "messages",
        payloadJson: "{}",
      }),
    });
    const repairMessage = runtimeRepairMessage();

    const result = await committer.commitInternalToolRepair({
      sessionId: "sesn_1",
      sessionThreadId: "thr_main",
      modelRequestId: "mreq_1",
      modelToolCallId: "call_1",
      toolName: "unknown_tool",
      repairKey: "internal_invalid_tool:mreq_1:call_1:unknown_tool",
      message: repairMessage,
    });

    expect(result).toEqual({
      ok: true,
      messageId: "msg_repair",
      partId: "part_repair",
      operation: "writePart",
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
    });
    expect(JSON.parse(bridge.commitInternalToolRepairRequests[0]?.dataJson ?? "{}")).toEqual(repairMessage);
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

function bindingIdentity(runtimeBindingToken: string): RuntimeSessionIdentity {
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
  readonly commitInternalToolRepairRequests: CommitInternalToolRepairRequest[] = [];
  readonly commitRuntimeTerminationRequests: CommitRuntimeTerminationRequest[] = [];
  readonly createChildThreadRequests: CreateChildThreadRequest[] = [];
  readonly finishIdleRequests: FinishIdleRequest[] = [];
  readonly loadContextRequests: LoadContextRequest[] = [];
  readonly markChildThreadClosedRequests: MarkChildThreadClosedRequest[] = [];
  readonly refreshRuntimeBindingTokenRequests: RefreshRuntimeBindingTokenRequest[] = [];
  readonly refreshErrors: Error[] = [];
  readonly writeEventRequests: WriteEventRequest[] = [];
  readonly writeRequestEndRequests: WriteRequestEndRequest[] = [];
  loadContextJSON: string | undefined;
  eventWriterAckStatus = BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED;
  eventWriterErrorCode = "";
  eventWriterAckWriteId: string | undefined;
  eventWriterTransportError: Error | null = null;

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
      writeEvent: this.writeEvent.bind(this),
      writeRequestEnd: this.writeRequestEnd.bind(this),
    } as unknown as AgentRuntimeBridgeServiceClient;
  }

  private commitInputs(request: CommitInputsRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.commitInputsRequests.push(request);
    if (this.commitError !== null) {
      callback(this.commitError, null);
      return grpcCall();
    }
    callback(null, {
      ack: {
        status: this.commitStatus,
        runtimeInputId: request.runtimeInputId,
        runtimeWriteId: "",
        errorCode: "",
      },
    });
    return grpcCall();
  }

  private commitInternalToolRepair(request: CommitInternalToolRepairRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.commitInternalToolRepairRequests.push(request);
    callback(null, {
      ack: {
        status: this.commitStatus,
        runtimeInputId: "",
        runtimeWriteId: request.modelToolCallId,
        errorCode: "",
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
    callback(null, {
      ack: {
        status: this.eventWriterAckStatus,
        runtimeInputId: "",
        runtimeWriteId: this.eventWriterAckWriteId ?? request.runtimeWriteId,
        errorCode: this.eventWriterErrorCode,
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
    callback(null, {
      ack: {
        status: this.eventWriterAckStatus,
        runtimeInputId: "",
        runtimeWriteId: this.eventWriterAckWriteId ?? request.runtimeWriteId,
        errorCode: this.eventWriterErrorCode,
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
        messages: [runtimeMessage(`msg_context_${request.scope?.sessionThreadId ?? "unknown"}`, "context")],
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
          kind: "approval",
          input: { path: "/workspace/file.txt" },
          decision: "deny",
          denyMessage: "not safe",
          status: "resolving",
          expiresAt: "2026-01-01T00:30:00Z",
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
      }),
      runtimeBindingToken: `token_${request.scope?.sessionThreadId ?? "unknown"}`,
    });
    return grpcCall();
  }

  private markChildThreadClosed(request: MarkChildThreadClosedRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.markChildThreadClosedRequests.push(request);
    callback(null, {
      ack: {
        status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED,
        runtimeInputId: "",
        runtimeWriteId: "",
        errorCode: "",
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
    callback(null, {
      ack: {
        status: this.eventWriterAckStatus,
        runtimeInputId: "",
        runtimeWriteId: this.eventWriterAckWriteId ?? request.runtimeWriteId,
        errorCode: this.eventWriterErrorCode,
      },
      eventId: `evt_${request.runtimeWriteId}`,
      sequence: 1,
    });
    return grpcCall();
  }

  private writeRequestEnd(request: WriteRequestEndRequest, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void): unknown {
    this.writeRequestEndRequests.push(request);
    if (this.eventWriterTransportError !== null) {
      callback(this.eventWriterTransportError, null);
      return grpcCall();
    }
    const staleTerminal = request.modelRequestId === "mreq_stale_terminal";
    callback(null, {
      ack: {
        status: this.eventWriterAckStatus,
        runtimeInputId: "",
        runtimeWriteId: this.eventWriterAckWriteId ?? request.runtimeWriteId,
        errorCode: this.eventWriterErrorCode,
      },
      rescheduleDisposition: staleTerminal ? {
        status: "denied",
        denialReason: "stale_terminal",
        attempt: 0,
        effectiveDeadline: "",
      } : request.reschedule === undefined ? undefined : {
        status: "accepted",
        denialReason: "",
        attempt: request.reschedule.attempt,
        effectiveDeadline: request.reschedule.deadline,
      },
    });
    return grpcCall();
  }
}

function grpcCall(): { readonly cancel: () => void } {
  return { cancel: () => undefined };
}
