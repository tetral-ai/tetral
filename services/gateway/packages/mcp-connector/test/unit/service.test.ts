import { describe, expect, test } from "bun:test";
import { createHash, createHmac } from "node:crypto";
import { Metadata, status } from "@grpc/grpc-js";
import type { CallOptions, ServiceError } from "@grpc/grpc-js";
import { McpErrorKind, McpRetryStatus, RunMcpToolStatus } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import { BridgeAPIMcpToolResultIdempotencyStore } from "../../src/bridge-client.js";
import { McpConnectorError } from "../../src/errors.js";
import { McpConnectorMetricsRegistry } from "../../src/metrics.js";
import { MCP_FAILURE_KIND_METADATA_KEY, MCP_MANIFEST_NOTIFY_RETRY_DELAYS_MS, McpConnectorServiceShell, GrpcStatusError } from "../../src/service.js";
import type { ClaimMcpToolResultRequest, ClaimMcpToolResultResponse, CommitMcpToolResultRequest, CommitMcpToolResultResponse } from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { BridgeWriteStatus, ReceiptApplicationDisposition } from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { McpAuthenticator, McpClient, McpClientTool, McpConnectorLogger } from "../../src/service.js";
import { InMemoryMcpIdempotencyStore } from "../../src/idempotency.js";
import { canonicalRunToolJSON } from "@tetral/gateway-protocol/src/run-tool-canonical-json.js";
import { canonicalJson } from "../../src/idempotency.js";
import type { McpIdempotencyStore } from "../../src/idempotency.js";
import type { RuntimeBindingRequestIdentity } from "@tetral/gateway-protocol/src/binding-token.js";

const RuntimePodUid = "pod_uid_mcp_connector";
const BindingTokenKey = "gateway-runtime-binding-token-test-key-32";

describe("McpConnectorServiceShell", () => {
  test("refuses non-catalog server names before client I/O", async () => {
    const client = new RecordingMcpClient();
    const service = createService(client);

    await expect(service.listMcpTools({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "not-github",
    }, new Metadata())).rejects.toMatchObject({ code: status.INVALID_ARGUMENT });

    expect(client.calls).toEqual([]);
  });

  test("omits platform collisions while leaving family collisions to Bridge", async () => {
    const client = new RecordingMcpClient();
    const platformNames = [
      "web",
      "memory",
      "spawn_agent",
      "send_message",
      "wait_agent",
      "interrupt_agent",
      "close_agent",
      "resume_agent",
      "list_agents",
    ];
    const familyNames = ["Bash", "Read", "Write", "Edit", "Glob", "Grep", "exec_command", "write_stdin", "view_image", "apply_patch"];
    const passingTools = [
      { name: "create_issue", description: "Create an issue.", inputSchema: { required: ["title"], type: "object" } },
      ...familyNames.map((name) => ({ name, description: `${name} family collision.`, inputSchema: { type: "object" } })),
    ];
    client.tools = [
      ...passingTools,
      { name: "disabled_tool", description: "Hidden.", inputSchema: { type: "object" }, enabled: false },
      ...platformNames.map((name) => ({ name, description: `${name} platform collision.`, inputSchema: { type: "object" } })),
    ];
    const logger = new MemoryLogger();
    const service = createService(client, new RecordingManifestChangeNotifier(), logger);

    const response = await service.listMcpTools(validListRequest(), new Metadata());

    expect(response.tools.map((tool) => tool.name)).toEqual(passingTools.map((tool) => tool.name));
    expect(response.omittedTools).toEqual(platformNames.map((name) => ({ name, reason: "builtin_name_collision" })));
    const canonicalManifest = response.tools.map((tool) => ({
      name: tool.name,
      description: tool.description,
      input_schema: JSON.parse(tool.inputSchemaJson) as unknown,
    }));
    expect(response.manifestEtag).toBe(createHash("sha256").update(canonicalJson(canonicalManifest)).digest("hex"));
    const warningRecords = (logger.records as Array<Record<string, unknown>>).filter((record) => record["event.kind"] === "mcp_manifest_tool_omitted");
    expect(warningRecords).toHaveLength(platformNames.length);
    expect(warningRecords.map((record) => record["mcp.tool.name"])).toEqual(platformNames);
    for (const record of warningRecords) {
      expect(record).toMatchObject({
        severity: "warning",
        operation: "mcp_manifest_list",
        component: "mcp-connector",
        "workspace.id": "wksp_1",
        "session.id": "sesn_1",
        "mcp.server.name": "github",
        "mcp.omission.reason": "builtin_name_collision",
      });
    }
    expect(logger.records).toContainEqual(expect.objectContaining({
      event: "mcp_manifest_listed",
      "event.kind": "mcp_manifest_listed",
      operation: "mcp_manifest_list",
      component: "mcp-connector",
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      "mcp.server.name": "github",
      "mcp.tool_count": passingTools.length,
    }));
    expect(JSON.stringify(logger.records)).not.toContain("Authorization");
    expect(JSON.stringify(logger.records)).not.toContain("Bearer");
  });

  test("keeps discovery, notification, and Tool settlement independent of logger failure", async () => {
    const client = new RecordingMcpClient();
    const notifier = new RecordingManifestChangeNotifier();
    const throwingLogger: McpConnectorLogger = {
      info: () => { throw new Error("telemetry sink unavailable"); },
      error: () => { throw new Error("telemetry sink unavailable"); },
    };
    const service = createService(client, notifier, throwingLogger, undefined, undefined, async () => undefined);

    const listed = await service.listMcpTools(validListRequest(), new Metadata());
    expect(listed.tools.map((tool) => tool.name)).toEqual(["create_issue"]);

    await expect(service.handleToolsListChangedNotification(validListRequest())).resolves.toMatchObject({ status: "notified" });
    notifier.result = { ok: false, retryable: true, code: "bridge_unavailable", message: "private bridge detail" };
    await expect(service.handleToolsListChangedNotification(validListRequest())).resolves.toMatchObject({ status: "exhausted" });

    await expect(service.runMcpTool(validRunRequest(), authorizationMetadata())).resolves.toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      resultText: "ok",
    });
    await expect(service.runMcpTool(validRunRequest({
      requestId: "req_rejected",
      toolUseEventId: "sevt_tool_rejected",
      runtimeBindingToken: "invalid-binding-token",
    }), authorizationMetadata())).rejects.toMatchObject({ code: status.PERMISSION_DENIED });
  });

  test("classifies only bounded initial discovery failures with private metadata", async () => {
    const cases = [
      [new McpConnectorError("mcp_credential_required", "secret credential", "terminal"), status.FAILED_PRECONDITION, "credential_unavailable"],
      [new McpConnectorError("mcp_authentication_failed", "secret token response", "terminal"), status.FAILED_PRECONDITION, "credential_unavailable"],
      [new McpConnectorError("mcp_connection_failed", "upstream body", "exhausted"), status.UNAVAILABLE, "server_unavailable"],
      [new McpConnectorError("mcp_timeout", "upstream timeout detail"), status.DEADLINE_EXCEEDED, "discovery_timeout"],
    ] as const;
    for (const [failure, code, kind] of cases) {
      const client = new RecordingMcpClient();
      const logger = new MemoryLogger();
      client.listToolsError = failure;

      const error = await createService(client, new RecordingManifestChangeNotifier(), logger)
        .listMcpTools(validListRequest(), new Metadata())
        .then(() => undefined, (caught: unknown) => caught);

      expect(error).toBeInstanceOf(GrpcStatusError);
      expect(error).toMatchObject({ code, message: "mcp manifest discovery failed" });
      expect((error as GrpcStatusError).metadata.get(MCP_FAILURE_KIND_METADATA_KEY)).toEqual([kind]);
      expect(logger.records).toEqual([expect.objectContaining({
        event: "mcp_manifest_discovery_failed",
        "workspace.id": "wksp_1",
        "session.id": "sesn_1",
        "mcp.server.name": "github",
        "mcp.failure.kind": kind,
        "error.code": kind,
      })]);
      expect(JSON.stringify(logger.records)).not.toContain(failure.message);
    }

    const retrying = new RecordingMcpClient();
    retrying.listToolsError = new McpConnectorError("mcp_connection_failed", "still reconnecting", "retrying");
    const error = await createService(retrying).listMcpTools(validListRequest(), new Metadata())
      .then(() => undefined, (caught: unknown) => caught);
    expect(error).toBe(retrying.listToolsError);
  });

  test("initial discovery and pre-dispatch notify failures leave later real notifications eligible", async () => {
    const client = new RecordingMcpClient();
    const notifier = new RecordingManifestChangeNotifier();
    const service = createService(client, notifier);
    client.listToolsError = new Error("upstream discovery unavailable");
    await expect(service.listMcpTools(validListRequest(), new Metadata())).rejects.toThrow("upstream discovery unavailable");
    expect(notifier.notifications).toEqual([]);
    await expect(service.handleToolsListChangedNotification(validListRequest())).rejects.toThrow("upstream discovery unavailable");
    expect(notifier.notifications).toEqual([]);
    client.listToolsError = undefined;
    notifier.result = { ok: false, retryable: false, code: "token_unavailable", message: "Bridge auth unavailable." };
    await expect(service.handleToolsListChangedNotification(validListRequest())).rejects.toThrow("Bridge auth unavailable.");
    expect(notifier.notifications).toHaveLength(1);
    notifier.result = { ok: true, duplicate: true };
    await expect(service.handleToolsListChangedNotification(validListRequest())).resolves.toMatchObject({ status: "notified" });
    expect(notifier.notifications).toHaveLength(2);
    expect(notifier.notifications[1]?.manifestEtag).toBe(notifier.notifications[0]?.manifestEtag);
  });

  test("manifest notify retries exactly four times, logs exhaustion only, and a later event retriggers", async () => {
    const client = new RecordingMcpClient();
    const notifier = new RecordingManifestChangeNotifier();
    const logger = new MemoryLogger();
    const sleeps: number[] = [];
    const service = createService(client, notifier, logger, undefined, new InMemoryMcpIdempotencyStore(), async (delayMs) => {
      sleeps.push(delayMs);
    });
    await service.listMcpTools(validListRequest(), new Metadata());

    client.tools = [
      ...client.tools,
      { name: "create_pull_request", description: "Create a pull request.", inputSchema: { type: "object" } },
    ];
    notifier.result = { ok: false, retryable: true, code: `grpc_${status.DEADLINE_EXCEEDED}`, message: "Bridge manifest notify timed out." };
    const exhausted = await service.handleToolsListChangedNotification(validListRequest());
    expect(exhausted.status).toBe("exhausted");
    expect(notifier.notifications).toHaveLength(4);
    expect(sleeps).toEqual([...MCP_MANIFEST_NOTIFY_RETRY_DELAYS_MS]);
    expect((logger.records as Array<Record<string, unknown>>).filter((record) => record["event.kind"] === "mcp_manifest_notify_exhausted")).toEqual([
      expect.objectContaining({
        "mcp.notification_attempts": 4,
        "error.class": "mcp_manifest_notify_failed",
        "error.code": `grpc_${status.DEADLINE_EXCEEDED}`,
        "error.message_safe": "mcp manifest Bridge notification retries exhausted",
      }),
    ]);
    expect((logger.records as Array<Record<string, unknown>>).filter((record) => record["event.kind"] === "mcp_manifest_changed_notified")).toHaveLength(0);

    notifier.result = { ok: true, duplicate: false };
    const retried = await service.handleToolsListChangedNotification(validListRequest());

    expect(retried.status).toBe("notified");
    expect(notifier.notifications).toHaveLength(5);
    expect(notifier.notifications[4]?.manifestEtag).toBe(notifier.notifications[0]?.manifestEtag);
  });

  test("manifest notify retry wait is cancellation-aware", async () => {
    const client = new RecordingMcpClient();
    const notifier = new RecordingManifestChangeNotifier();
    const service = createService(client, notifier);
    await service.listMcpTools(validListRequest(), new Metadata());
    client.tools = [...client.tools, { name: "create_pull_request", description: "Create a pull request.", inputSchema: { type: "object" } }];
    notifier.result = { ok: false, retryable: true, code: "bridge_unavailable", message: "Bridge manifest notify failed." };
    const controller = new AbortController();
    controller.abort();
    await expect(service.handleToolsListChangedNotification(validListRequest(), controller.signal)).rejects.toMatchObject({ code: status.CANCELLED });
    expect(notifier.notifications).toHaveLength(1);
  });

  test("runs tools through formatter and replays same normalized input without re-calling the client", async () => {
    const client = new RecordingMcpClient();
    client.resultText = "created";
    const service = createService(client);
    const request = validRunRequest({ inputJson: `{"b":2,"a":1}` });

    const first = await service.runMcpTool(request, new Metadata());
    const second = await service.runMcpTool({ ...request, inputJson: `{"a":1,"b":2}` }, new Metadata());

    expect(first).toEqual({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      resultText: "created",
      attachments: [],
      errorKind: undefined,
      retryStatus: undefined,
    });
    expect(second).toEqual(first);
    expect(client.calls).toEqual(["callTool"]);
  });

  test("cold MCP manifest restores approved pending call after binding replacement", async () => {
    const client = new RecordingMcpClient();
    client.resultText = "cold approved result";
    const service = createService(client);
    const replacementIdentity: RuntimeBindingRequestIdentity = {
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      sessionThreadId: "thrd_1",
      bindingId: "bind_2",
      bindingGeneration: 43,
    };

    const response = await service.runMcpTool(validRunRequest({
      requestId: "req_cold_approved",
      bindingId: replacementIdentity.bindingId,
      bindingGeneration: replacementIdentity.bindingGeneration,
      runtimeBindingToken: signedRuntimeBindingToken(replacementIdentity, RuntimePodUid),
    }), new Metadata());

    expect(response).toEqual({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      resultText: "cold approved result",
      attachments: [],
      errorKind: undefined,
      retryStatus: undefined,
    });
    expect(client.calls.filter((call) => call === "callTool")).toHaveLength(1);
    expect(client.calls.filter((call) => call === "listTools")).toHaveLength(0);
  });

  test("waits for an in-flight same-input retry and replays the result", async () => {
    const client = new RecordingMcpClient();
    client.resultText = "created";
    const gate = deferred<void>();
    client.callToolDelay = gate.promise;
    const service = createService(client);
    const request = validRunRequest({ inputJson: `{"b":2,"a":1}` });

    const first = service.runMcpTool(request, new Metadata());
    await waitFor(() => client.calls.length === 1);
    const second = service.runMcpTool({ ...request, inputJson: `{"a":1,"b":2}` }, new Metadata());
    await Promise.resolve();
    expect(client.calls).toEqual(["callTool"]);

    gate.resolve();
    const [firstResponse, secondResponse] = await Promise.all([first, second]);

    expect(secondResponse).toEqual(firstResponse);
    expect(client.calls).toEqual(["callTool"]);
  });

  test("rejects an in-flight different-input retry before second client I/O", async () => {
    const client = new RecordingMcpClient();
    const gate = deferred<void>();
    client.callToolDelay = gate.promise;
    const service = createService(client);
    const request = validRunRequest({ inputJson: `{"a":1}` });

    const first = service.runMcpTool(request, new Metadata());
    await waitFor(() => client.calls.length === 1);
    await expect(service.runMcpTool({ ...request, inputJson: `{"a":2}` }, new Metadata())).resolves.toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      errorKind: McpErrorKind.MCP_ERROR_KIND_CLAIM_CONFLICT,
    });
    expect(client.calls).toEqual(["callTool"]);

    gate.resolve();
    await first;
  });

  test("replays completed MCP results through Bridge durable idempotency and rejects hash mismatch", async () => {
    const bridge = new RecordingMcpToolResultBridgeClient();
    const firstClient = new RecordingMcpClient();
    firstClient.resultText = "created";
    const first = createService(firstClient, new RecordingManifestChangeNotifier(), new MemoryLogger(), undefined, bridgeBackedStore(bridge));
    const request = validRunRequest({ inputJson: `{"title":"Bug","body":"Details"}` });

    const firstResponse = await first.runMcpTool(request, authorizationMetadata());

    const secondClient = new RecordingMcpClient();
    secondClient.resultText = "would execute if replay failed";
    const second = createService(secondClient, new RecordingManifestChangeNotifier(), new MemoryLogger(), undefined, bridgeBackedStore(bridge));
    const replayResponse = await second.runMcpTool(request, authorizationMetadata());

    expect(replayResponse).toEqual(firstResponse);
    expect(firstClient.calls).toEqual(["callTool"]);
    expect(secondClient.calls).toEqual([]);
    expect(bridge.commitCalls).toHaveLength(1);
    expect(bridge.claimCalls).toHaveLength(2);

    const thirdClient = new RecordingMcpClient();
    const third = createService(thirdClient, new RecordingManifestChangeNotifier(), new MemoryLogger(), undefined, bridgeBackedStore(bridge));
    await expect(third.runMcpTool({ ...request, inputJson: `{"title":"Different"}` }, authorizationMetadata())).resolves.toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      errorKind: McpErrorKind.MCP_ERROR_KIND_CLAIM_CONFLICT,
    });
    expect(thirdClient.calls).toEqual([]);
    expect(bridge.claimCalls).toHaveLength(3);
    expect(bridge.commitCalls).toHaveLength(1);
  });

  test("retries a lost commit acknowledgement without repeating the external MCP call", async () => {
    const bridge = new RecordingMcpToolResultBridgeClient();
    bridge.commitTransportFailuresRemaining = 1;
    const client = new RecordingMcpClient();
    client.resultText = "created";
    const service = createService(
      client,
      new RecordingManifestChangeNotifier(),
      new MemoryLogger(),
      undefined,
      bridgeBackedStore(bridge),
    );

    await expect(service.runMcpTool(validRunRequest(), authorizationMetadata())).resolves.toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      resultText: "created",
    });
    expect(client.calls).toEqual(["callTool"]);
    expect(bridge.commitCalls).toHaveLength(2);
    expect(bridge.commitCalls[0]).toBe(bridge.commitCalls[1]);
  });

  test("commits inline media through Bridge and returns refs only", async () => {
    const bridge = new RecordingMcpToolResultBridgeClient();
    const client = new RecordingMcpClient();
    client.callToolResult = {
      content: [{ type: "image", data: Buffer.from("image-bytes").toString("base64"), mimeType: "image/png" }],
    };
    const service = createService(client, new RecordingManifestChangeNotifier(), new MemoryLogger(), undefined, bridgeBackedStore(bridge));

    const response = await service.runMcpTool(validRunRequest(), authorizationMetadata());

    expect(response.attachments).toEqual([{
      attachmentRef: "att_bridge_1",
      mime: "image/png",
      sizeBytes: 11,
      suggestedFilename: "mcp-image-1.png",
    }]);
    expect(JSON.stringify(response)).not.toContain("image-bytes");
    expect(bridge.commitCalls).toHaveLength(1);
    expect(bridge.commitCalls[0]?.inlineMedia).toEqual([{
      data: new Uint8Array(Buffer.from("image-bytes")),
      mime: "image/png",
      suggestedFilename: "mcp-image-1.png",
    }]);
    expect(bridge.commitCalls[0]?.resultJson).not.toContain("data_base64");
    expect(bridge.commitCalls[0]?.resultJson).not.toContain(Buffer.from("image-bytes").toString("base64"));
  });

  test("maps a live Bridge MCP claim to retryable runtime_error without client I/O", async () => {
    const bridge = new RecordingMcpToolResultBridgeClient();
    bridge.inFlight = true;
    const client = new RecordingMcpClient();
    const service = createService(client, new RecordingManifestChangeNotifier(), new MemoryLogger(), undefined, bridgeBackedStore(bridge));

    const response = await service.runMcpTool(validRunRequest(), authorizationMetadata());

    expect(response).toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP tool result is already in flight.",
      errorKind: McpErrorKind.MCP_ERROR_KIND_IN_FLIGHT,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_RETRYING,
    });
    expect(client.calls).toEqual([]);
    expect(bridge.claimCalls).toHaveLength(1);
    expect(bridge.commitCalls).toHaveLength(0);
  });

  test("stops stale-custody MCP claims before external tool execution", async () => {
    const client = new RecordingMcpClient();
    const service = createService(
      client,
      new RecordingManifestChangeNotifier(),
      new MemoryLogger(),
      undefined,
      new StaleCustodyIdempotencyStore(),
    );

    const response = await service.runMcpTool(validRunRequest(), authorizationMetadata());

    expect(response).toEqual({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP tool execution lost runtime custody.",
      attachments: [],
      errorKind: McpErrorKind.MCP_ERROR_KIND_CUSTODY_LOST,
      retryStatus: undefined,
    });
    expect(client.calls).toEqual([]);
  });

  test("fences same-replica concurrent Bridge-backed delivery before second MCP call", async () => {
    const bridge = new RecordingMcpToolResultBridgeClient();
    const client = new RecordingMcpClient();
    const gate = deferred<void>();
    client.callToolDelay = gate.promise;
    const service = createService(client, new RecordingManifestChangeNotifier(), new MemoryLogger(), undefined, bridgeBackedStore(bridge));
    const request = validRunRequest({ inputJson: `{"title":"Bug"}` });

    const first = service.runMcpTool(request, authorizationMetadata());
    await waitFor(() => client.calls.length === 1);
    const second = await service.runMcpTool({ ...request, requestId: "req_2" }, authorizationMetadata());

    expect(second).toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP tool result is already in flight.",
      errorKind: McpErrorKind.MCP_ERROR_KIND_IN_FLIGHT,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_RETRYING,
    });
    expect(client.calls).toEqual(["callTool"]);
    expect(bridge.claimCalls).toHaveLength(2);
    expect(bridge.commitCalls).toHaveLength(0);

    gate.resolve();
    await first;
    expect(bridge.commitCalls).toHaveLength(1);
  });

  test("logs exactly one mcp_commit_failed terminal outcome when durable commit fails", async () => {
    const client = new RecordingMcpClient();
    const logger = new MemoryLogger();
    const metrics = new McpConnectorMetricsRegistry();
    const service = createService(client, new RecordingManifestChangeNotifier(), logger, metrics, new RejectingCommitIdempotencyStore());

    const response = await service.runMcpTool(validRunRequest(), authorizationMetadata());

    expect(client.calls).toEqual(["callTool"]);
    expect(response).toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      errorKind: McpErrorKind.MCP_ERROR_KIND_COMMIT_FAILED,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_RETRYING,
    });
    expect(logger.records).toHaveLength(1);
    expect(logger.records[0]).toMatchObject({ status: "runtime_error", error_kind: "mcp_commit_failed" });
    expect(metrics.render()).toContain('mcpconnector_calls_total{tool="create_issue",status="runtime_error",error_kind="mcp_commit_failed"} 1');
  });

  test("records exactly one terminal RunMcpTool outcome for every early exit", async () => {
    const cases: Array<{
      readonly name: string;
      readonly request?: ReturnType<typeof validRunRequest>;
      readonly authenticator?: McpAuthenticator;
      readonly ready?: boolean;
      readonly idempotencyStore?: McpIdempotencyStore;
      readonly errorKind: string;
    }> = [
      {
        name: "authentication",
        authenticator: {
          authenticate: async () => ({ ok: false, code: "Unauthenticated", message: "unauthenticated" }),
        },
        errorKind: "mcp_unauthenticated",
      },
      { name: "readiness", ready: false, errorKind: "mcp_not_ready" },
      { name: "validation", request: validRunRequest({ requestId: "" }), errorKind: "mcp_invalid_input" },
      { name: "claim transport", idempotencyStore: new ThrowingClaimIdempotencyStore(), errorKind: "mcp_connection_failed" },
    ];

    for (const testCase of cases) {
      const logger = new MemoryLogger();
      const client = new RecordingMcpClient();
      const service = new McpConnectorServiceShell({
        authenticator: testCase.authenticator ?? new AllowingAuthenticator(),
        client,
        logger,
        runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({
          hmacKey: BindingTokenKey,
          now: () => new Date("2026-01-01T00:00:00Z"),
        }),
        ready: () => testCase.ready ?? true,
        idempotencyStore: testCase.idempotencyStore ?? new InMemoryMcpIdempotencyStore(),
      });

      await expect(service.runMcpTool(testCase.request ?? validRunRequest(), authorizationMetadata()), testCase.name).rejects.toBeDefined();
      expect(logger.records, testCase.name).toHaveLength(1);
      expect(logger.records[0], testCase.name).toMatchObject({
        operation: "run_mcp_tool",
        "event.kind": "mcpconnector.call",
        status: "runtime_error",
        error_kind: testCase.errorKind,
      });
    }
  });

  test("does not classify an invalid scoped discovery request as external degradation", async () => {
    const client = new RecordingMcpClient();
    const logger = new MemoryLogger();
    const invalid = new McpConnectorError("mcp_invalid_input", "invalid upstream schema");
    client.listToolsError = invalid;

    const error = await createService(client, new RecordingManifestChangeNotifier(), logger)
      .listMcpTools(validListRequest(), new Metadata())
      .then(() => undefined, (caught: unknown) => caught);

    expect(error).toBe(invalid);
    expect(error).not.toBeInstanceOf(GrpcStatusError);
    expect(logger.records).toEqual([expect.objectContaining({
      event: "mcp_manifest_discovery_failed",
      "mcp.failure.kind": "internal",
      "error.code": "internal",
    })]);
  });

  test("does not execute the MCP tool when the durable Claim reaches its deadline", async () => {
    const client = new RecordingMcpClient();
    const bridge = new DeadlineClaimBridgeClient();
    const store = new BridgeAPIMcpToolResultIdempotencyStore({
      address: "bridge.tetral-system.svc.cluster.local:9090",
      tokenPath: "/token",
      client: bridge,
      metadataFactory: async () => new Metadata(),
      claimTimeoutMs: 1_234,
      now: () => 1_000,
    });
    const service = createService(client, new RecordingManifestChangeNotifier(), new MemoryLogger(), undefined, store);

    await expect(service.runMcpTool(validRunRequest(), new Metadata())).rejects.toMatchObject({
      code: status.DEADLINE_EXCEEDED,
    });

    expect(bridge.claimOptions).toEqual([{ deadline: new Date(2_234) }]);
    expect(client.calls).toEqual([]);
  });

  test("returns the one durable terminal failure when Bridge rejects the provider result shape", async () => {
    const client = new RecordingMcpClient();
    const bridge = new RecordingMcpToolResultBridgeClient();
    bridge.commitPayloadRejectionsRemaining = 1;
    const service = createService(
      client,
      new RecordingManifestChangeNotifier(),
      new MemoryLogger(),
      undefined,
      bridgeBackedStore(bridge),
    );

    await expect(service.runMcpTool(validRunRequest(), authorizationMetadata())).resolves.toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP tool result could not be committed.",
      errorKind: McpErrorKind.MCP_ERROR_KIND_COMMIT_FAILED,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
      materializationHandle: "sevt_tool_1",
    });
    expect(client.calls).toHaveLength(1);
    expect(bridge.commitCalls).toHaveLength(2);
    expect(bridge.stored.size).toBe(1);
  });

  test("logs RunMcpTool without credential material", async () => {
    const client = new RecordingMcpClient();
    client.callToolResult = {
      refreshTriggered: true,
      content: [
        { type: "text", text: "created" },
        { type: "image", data: Buffer.from("image-bytes").toString("base64"), mimeType: "image/png" },
      ],
    };
    const logger = new MemoryLogger();
    const service = createService(
      client,
      new RecordingManifestChangeNotifier(),
      logger,
      undefined,
      bridgeBackedStore(new RecordingMcpToolResultBridgeClient()),
    );
    const request = validRunRequest({
      runtimeBindingToken: signedRuntimeBindingToken(validRunIdentity(), RuntimePodUid),
    });

    const response = await service.runMcpTool(request, authorizationMetadata());
    await service.runMcpTool({ ...request, inputJson: `{\n}` }, authorizationMetadata());

    expect(response.status).toBe(RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED);
    expect(logger.records).toHaveLength(2);
    for (const record of logger.records as Array<Record<string, unknown>>) {
      expect(Object.keys(record).sort()).toEqual([
        "attachment_count",
        "component",
        "content_items",
        "duration.ms",
        "error_kind",
        "event.kind",
        "mcp_server_name",
        "operation",
        "refresh_triggered",
        "request.id",
        "session.id",
        "status",
        "thread.id",
        "tool.use.event.id",
        "tool_name",
        "workspace.id",
      ].sort());
      expect(record).toMatchObject({
        "request.id": "req_1",
        "workspace.id": "wksp_1",
        "session.id": "sesn_1",
        "thread.id": "thrd_1",
        "tool.use.event.id": "sevt_tool_1",
        operation: "run_mcp_tool",
        "event.kind": "mcpconnector.call",
        component: "mcp-connector",
        mcp_server_name: "github",
        tool_name: "create_issue",
        status: "completed",
        error_kind: "",
        refresh_triggered: true,
        content_items: 2,
        attachment_count: 1,
      });
      expect(typeof record["duration.ms"]).toBe("number");
      const serialized = JSON.stringify(record);
      expect(serialized).not.toContain("Authorization");
      expect(serialized).not.toContain("Bearer");
      expect(serialized).not.toContain("rtbt_v1");
    }
  });

  test("maps invalid MCP arguments to model-visible tool errors", async () => {
    const client = new RecordingMcpClient();
    client.callToolError = new McpConnectorError("mcp_invalid_input", "MCP server rejected the arguments.");
    const logger = new MemoryLogger();
    const service = createService(client, new RecordingManifestChangeNotifier(), logger);

    const response = await service.runMcpTool(validRunRequest(), new Metadata());

    expect(response).toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR,
      resultText: "MCP server rejected the arguments.",
      errorKind: McpErrorKind.MCP_ERROR_KIND_INVALID_INPUT,
    });
    expect(logger.records).toHaveLength(1);
    expect(logger.records[0]).toMatchObject({
      "request.id": "req_1",
      "workspace.id": "wksp_1",
      "session.id": "sesn_1",
      "thread.id": "thrd_1",
      operation: "run_mcp_tool",
      "event.kind": "mcpconnector.call",
      component: "mcp-connector",
      mcp_server_name: "github",
      tool_name: "create_issue",
      status: "tool_error",
      error_kind: "mcp_invalid_input",
      "error.class": "mcp_invalid_input",
      "error.code": "mcp_invalid_input",
      "error.message_safe": "MCP connector call failed.",
    });
    expect(Object.keys(logger.records[0] as Record<string, unknown>).sort()).toEqual([
      "attachment_count",
      "component",
      "content_items",
      "duration.ms",
      "error.class",
      "error.code",
      "error_kind",
      "error.message_safe",
      "event.kind",
      "mcp_server_name",
      "operation",
      "refresh_triggered",
      "request.id",
      "session.id",
      "status",
      "thread.id",
      "tool.use.event.id",
      "tool_name",
      "workspace.id",
    ].sort());
    expect(JSON.stringify(logger.records[0])).not.toContain("MCP server rejected the arguments.");
  });

  test("maps terminal MCP authentication failure into the event envelope fields", async () => {
    const client = new RecordingMcpClient();
    client.callToolError = new McpConnectorError("mcp_authentication_failed", "MCP authentication failed after refresh.", "terminal");
    const service = createService(client);

    const response = await service.runMcpTool(validRunRequest(), new Metadata());

    expect(response).toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP authentication failed after refresh.",
      errorKind: McpErrorKind.MCP_ERROR_KIND_AUTHENTICATION_FAILED,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
    });
  });

  test("surfaces credential_required MCP delivery failures without client success", async () => {
    const client = new RecordingMcpClient();
    client.callToolError = new McpConnectorError("mcp_credential_required", "MCP server github requires a configured credential.", "terminal");
    const service = createService(client);

    const response = await service.runMcpTool(validRunRequest(), new Metadata());

    expect(response).toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP server github requires a configured credential.",
      errorKind: McpErrorKind.MCP_ERROR_KIND_CREDENTIAL_REQUIRED,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
    });
  });

  test("maps reconnect-in-progress failures into retrying runtime status", async () => {
    const client = new RecordingMcpClient();
    client.callToolError = new McpConnectorError("mcp_connection_failed", "MCP connection retrying.", "retrying");
    const service = createService(client);

    const response = await service.runMcpTool(validRunRequest(), new Metadata());

    expect(response).toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP connection retrying.",
      errorKind: McpErrorKind.MCP_ERROR_KIND_CONNECTION_FAILED,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_RETRYING,
    });
  });

  test("commits, replays, logs, and meters unclassified failures as internal without retry status", async () => {
    const bridge = new RecordingMcpToolResultBridgeClient();
    const logger = new MemoryLogger();
    const metrics = new McpConnectorMetricsRegistry();
    const firstClient = new RecordingMcpClient();
    firstClient.callToolError = new Error("unclassified connector exception");
    const first = createService(firstClient, new RecordingManifestChangeNotifier(), logger, metrics, bridgeBackedStore(bridge));
    const request = validRunRequest();

    const firstResponse = await first.runMcpTool(request, authorizationMetadata());

    expect(firstResponse).toEqual({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP connector failed.",
      attachments: [],
      errorKind: McpErrorKind.MCP_ERROR_KIND_INTERNAL,
      retryStatus: undefined,
      materializationHandle: "sevt_tool_1",
    });
    expect(bridge.commitCalls).toHaveLength(1);
    expect(logger.records).toContainEqual(expect.objectContaining({
      status: "runtime_error",
      error_kind: "mcp_internal_error",
      "error.class": "mcp_internal_error",
    }));
    expect(metrics.render()).toContain('mcpconnector_calls_total{tool="create_issue",status="runtime_error",error_kind="mcp_internal_error"} 1');

    const replayClient = new RecordingMcpClient();
    const replay = createService(replayClient, new RecordingManifestChangeNotifier(), new MemoryLogger(), undefined, bridgeBackedStore(bridge));
    const replayResponse = await replay.runMcpTool(request, authorizationMetadata());

    expect(replayResponse).toEqual(firstResponse);
    expect(replayClient.calls).toEqual([]);
    expect(bridge.commitCalls).toHaveLength(1);
  });

  test("keeps genuine reconnect exhaustion distinct from unclassified failures", async () => {
    const client = new RecordingMcpClient();
    client.callToolError = new McpConnectorError("mcp_connection_failed", "MCP connection failed.", "exhausted");
    const service = createService(client);

    const response = await service.runMcpTool(validRunRequest(), new Metadata());

    expect(response).toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP connection failed.",
      errorKind: McpErrorKind.MCP_ERROR_KIND_CONNECTION_FAILED,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_EXHAUSTED,
    });
  });

  test("maps call timeout into a model-visible tool error", async () => {
    const client = new RecordingMcpClient();
    client.callToolError = new McpConnectorError("mcp_timeout", "MCP tool call timed out.");
    const service = createService(client);

    const response = await service.runMcpTool(validRunRequest(), new Metadata());

    expect(response).toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR,
      resultText: "MCP tool call timed out.",
      errorKind: McpErrorKind.MCP_ERROR_KIND_TIMEOUT,
      retryStatus: undefined,
    });
  });

  test("rejects idempotency conflicts for the same tool_use_event_id", async () => {
    const service = createService(new RecordingMcpClient());
    const request = validRunRequest({ inputJson: `{"a":1}` });

    await service.runMcpTool(request, new Metadata());
    await expect(service.runMcpTool({ ...request, inputJson: `{"a":2}` }, new Metadata())).resolves.toMatchObject({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      errorKind: McpErrorKind.MCP_ERROR_KIND_CLAIM_CONFLICT,
    });
  });

  test("exports MCP connector metrics for calls, latency, sessions, manifests, and refresh attempts", async () => {
    const client = new RecordingMcpClient();
    client.connectionCountValue = 2;
    client.callToolResult = {
      refreshTriggered: true,
      content: [{ type: "text", text: "created" }],
    };
    const metrics = new McpConnectorMetricsRegistry();
    const service = createService(client, new RecordingManifestChangeNotifier(), new MemoryLogger(), metrics);

    await service.listMcpTools(validListRequest(), new Metadata());
    client.tools = [
      ...client.tools,
      { name: "create_pull_request", description: "Create a pull request.", inputSchema: { type: "object" } },
    ];
    await service.handleToolsListChangedNotification(validListRequest());
    await service.runMcpTool(validRunRequest(), new Metadata());
    metrics.recordRefreshAttempt("success");

    const rendered = service.metricsText();
    expect(rendered).toContain('mcpconnector_calls_total{tool="create_issue",status="completed",error_kind=""} 1');
    expect(rendered).toContain('mcpconnector_call_latency_seconds_count{tool="create_issue",status="completed",error_kind=""} 1');
    expect(rendered).toContain("mcpconnector_sessions_active 2");
    expect(rendered).toContain("mcpconnector_manifest_refreshes_total 2");
    expect(rendered).toContain('mcpconnector_refresh_attempts_total{outcome="success"} 1');
    expect(rendered).toContain('mcpconnector_refresh_attempts_total{outcome="failed"} 0');
  });

  test("rejects RunMcpTool before idempotency or client I/O when the binding token is stale", async () => {
    for (const request of [
      validRunRequest({
        runtimeBindingToken: signedRuntimeBindingToken({ ...validRunIdentity(), sessionId: "sesn_other" }, RuntimePodUid),
      }),
      validRunRequest({
        runtimeBindingToken: signedRuntimeBindingToken(validRunIdentity(), "pod_uid_other"),
      }),
      validRunRequest({
        runtimeBindingToken: signedRuntimeBindingToken(validRunIdentity(), RuntimePodUid, "2025-12-31T23:59:59Z"),
      }),
    ]) {
      const client = new RecordingMcpClient();
      const logger = new MemoryLogger();
      const service = createService(client, new RecordingManifestChangeNotifier(), logger);

      await expect(service.runMcpTool(request, new Metadata())).rejects.toMatchObject({
        code: status.PERMISSION_DENIED,
      });
      expect(client.calls).toEqual([]);
      expect(logger.records).toHaveLength(1);
      expect(logger.records[0]).toMatchObject({
        "request.id": "req_1",
        "workspace.id": "wksp_1",
        "session.id": "sesn_1",
        "thread.id": "thrd_1",
        operation: "run_mcp_tool",
        "event.kind": "mcpconnector.call",
        component: "mcp-connector",
        mcp_server_name: "github",
        tool_name: "create_issue",
        status: "runtime_error",
        error_kind: "runtime_binding_token_rejected",
        refresh_triggered: false,
        content_items: 0,
        attachment_count: 0,
        "error.class": "runtime_binding_token_rejected",
        "error.code": "runtime_binding_token_rejected",
        "error.message_safe": "MCP connector call failed.",
      });
      expect(Object.keys(logger.records[0] as Record<string, unknown>).sort()).toEqual([
        "attachment_count",
        "component",
        "content_items",
        "duration.ms",
        "error.class",
        "error.code",
        "error_kind",
        "error.message_safe",
        "event.kind",
        "mcp_server_name",
        "operation",
        "refresh_triggered",
        "request.id",
        "session.id",
        "status",
      "thread.id",
      "tool.use.event.id",
      "tool_name",
        "workspace.id",
      ].sort());
      expect(JSON.stringify(logger.records[0])).not.toContain("rtbt_v1");
    }
  });
});

function createService(
  client: RecordingMcpClient,
  manifestChangeNotifier = new RecordingManifestChangeNotifier(),
  logger: McpConnectorLogger = new MemoryLogger(),
  metrics?: McpConnectorMetricsRegistry,
  idempotencyStore: McpIdempotencyStore = new InMemoryMcpIdempotencyStore(),
  manifestNotifySleep?: (delayMs: number, signal?: AbortSignal) => Promise<void>,
): McpConnectorServiceShell {
  return new McpConnectorServiceShell({
    authenticator: new AllowingAuthenticator(),
    client,
    logger,
    runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({
      hmacKey: BindingTokenKey,
      now: () => new Date("2026-01-01T00:00:00Z"),
    }),
    ready: () => true,
    manifestChangeNotifier,
    manifestNotifySleep,
    metrics,
    idempotencyStore,
    activeSessionCount: () => client.connectionCount(),
  });
}

function bridgeBackedStore(client: RecordingMcpToolResultBridgeClient): BridgeAPIMcpToolResultIdempotencyStore {
  return new BridgeAPIMcpToolResultIdempotencyStore({
    address: "bridge.tetral-system.svc.cluster.local:9090",
    tokenPath: "/token",
    client,
    metadataFactory: async () => {
      const metadata = new Metadata();
      metadata.set("authorization", "bearer projected-token");
      return metadata;
    },
    sleep: async () => undefined,
  });
}

function validListRequest(overrides = {}) {
  return {
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    mcpServerName: "github",
    ...overrides,
  };
}

function validRunRequest(overrides = {}) {
  return {
    requestId: "req_1",
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    toolUseEventId: "sevt_tool_1",
    mcpServerName: "github",
    toolName: "create_issue",
    inputJson: "{}",
    bindingId: "bind_1",
    bindingGeneration: 42,
    runtimeBindingToken: signedRuntimeBindingToken(validRunIdentity(), RuntimePodUid),
    ...overrides,
  };
}

function authorizationMetadata(): Metadata {
  const metadata = new Metadata();
  metadata.set("authorization", "Bearer bearer-secret-must-not-log");
  return metadata;
}

function validRunIdentity(): RuntimeBindingRequestIdentity {
  return {
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 42,
  };
}

function signedRuntimeBindingToken(request: RuntimeBindingRequestIdentity, runtimePodUid: string, expiresAt = "2026-01-01T00:05:00Z"): string {
  const payload = {
    v: 1,
    workspace_id: request.workspaceId,
    session_id: request.sessionId,
    session_thread_id: request.sessionThreadId,
    binding_id: request.bindingId,
    binding_generation: request.bindingGeneration,
    runtime_pod_uid: runtimePodUid,
    exp: Math.floor(new Date(expiresAt).getTime() / 1000),
  };
  const payloadPart = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signaturePart = createHmac("sha256", BindingTokenKey).update(payloadPart).digest("base64url");
  return `rtbt_v1.${payloadPart}.${signaturePart}`;
}

class RecordingMcpClient implements McpClient {
  calls: string[] = [];
  resultText = "ok";
  connectionCountValue = 0;
  callToolDelay: Promise<void> | undefined;
  callToolResult: Awaited<ReturnType<McpClient["callTool"]>> | undefined;
  callToolError: unknown;
  listToolsError: unknown;
  tools: McpClientTool[] = [{ name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" } }];

  async listTools() {
    this.calls.push("listTools");
    if (this.listToolsError !== undefined) throw this.listToolsError;
    return this.tools;
  }

  async callTool() {
    this.calls.push("callTool");
    await this.callToolDelay;
    if (this.callToolError !== undefined) {
      throw this.callToolError;
    }
    if (this.callToolResult !== undefined) {
      return this.callToolResult;
    }
    return { content: [{ type: "text", text: this.resultText }] };
  }

  connectionCount(): number {
    return this.connectionCountValue;
  }
}

class RecordingManifestChangeNotifier {
  result:
    | { readonly ok: true; readonly duplicate: boolean }
    | { readonly ok: false; readonly retryable: boolean; readonly code: string; readonly message: string } = { ok: true, duplicate: false };

  readonly notifications: Array<{
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
    readonly manifestEtag: string;
  }> = [];

  async notify(input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
    readonly manifestEtag: string;
  }) {
    this.notifications.push(input);
    return this.result;
  }
}

class RejectingCommitIdempotencyStore implements McpIdempotencyStore {
  claim(): ReturnType<McpIdempotencyStore["claim"]> {
    return { status: "new" };
  }

  store(): ReturnType<McpIdempotencyStore["store"]> {
    throw new Error("mcp tool idempotency commit rejected");
  }

  fail(): ReturnType<McpIdempotencyStore["fail"]> {
    return undefined;
  }
}

class ThrowingClaimIdempotencyStore implements McpIdempotencyStore {
  claim(): ReturnType<McpIdempotencyStore["claim"]> {
    throw new Error("claim transport unavailable");
  }

  store(): ReturnType<McpIdempotencyStore["store"]> {
    throw new Error("unexpected store");
  }

  fail(): ReturnType<McpIdempotencyStore["fail"]> {
    return undefined;
  }
}

class StaleCustodyIdempotencyStore implements McpIdempotencyStore {
  claim(): ReturnType<McpIdempotencyStore["claim"]> {
    return { status: "stale_custody" };
  }

  store(): ReturnType<McpIdempotencyStore["store"]> {
    throw new Error("stale custody must not execute or store");
  }

  fail(): ReturnType<McpIdempotencyStore["fail"]> {
    return undefined;
  }
}

class RecordingMcpToolResultBridgeClient {
  readonly claimCalls: ClaimMcpToolResultRequest[] = [];
  readonly commitCalls: CommitMcpToolResultRequest[] = [];
  readonly stored = new Map<string, CommitMcpToolResultRequest>();
  readonly declarations = new Map<string, ReturnType<typeof fakeMcpMaterializationDeclaration>>();
  readonly claims = new Map<string, ClaimMcpToolResultRequest>();
  inFlight = false;
  commitTransportFailuresRemaining = 0;
  commitPayloadRejectionsRemaining = 0;

  claimMcpToolResult(
    request: ClaimMcpToolResultRequest,
    _metadata: Metadata,
    _options: CallOptions,
    callback: (error: ServiceError | null, response: ClaimMcpToolResultResponse) => void,
  ) {
    this.claimCalls.push(request);
    if (this.inFlight) {
      callback(null, { ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "mcp_claim_in_flight" }, resultJson: "", declaration: undefined });
      return;
    }
    const existing = this.stored.get(bridgeResultKey(request));
    if (existing === undefined) {
      const claim = this.claims.get(bridgeResultKey(request));
      if (claim !== undefined) {
        if (!sameBridgeMcpResult(claim, request)) {
          callback(grpcServiceError(status.ALREADY_EXISTS), { ack: undefined, resultJson: "", declaration: undefined });
          return;
        }
        callback(null, { ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "mcp_claim_in_flight" }, resultJson: "", declaration: undefined });
        return;
      }
      this.claims.set(bridgeResultKey(request), request);
      callback(null, { ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "" }, resultJson: "", declaration: undefined });
      return;
    }
    if (!sameBridgeMcpResult(existing, request)) {
      callback(grpcServiceError(status.ALREADY_EXISTS), { ack: undefined, resultJson: "", declaration: undefined });
      return;
    }
    callback(null, {
      ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE, runtimeInputId: "", runtimeWriteId: "", errorCode: "" },
      resultJson: existing.resultJson,
      materializationHandle: request.toolUseEventId,
      declaration: this.declarations.get(bridgeResultKey(request)),
    });
  }

  commitMcpToolResult(
    request: CommitMcpToolResultRequest,
    _metadata: Metadata,
    _options: { readonly deadline?: Date | number },
    callback: (error: ServiceError | null, response: CommitMcpToolResultResponse) => void,
  ) {
    this.commitCalls.push(request);
    if (this.commitPayloadRejectionsRemaining > 0) {
      this.commitPayloadRejectionsRemaining -= 1;
      callback(grpcServiceError(status.INVALID_ARGUMENT), {
        ack: undefined,
        refsOnlyResultJson: "",
        declaration: undefined,
      });
      return;
    }
    if (this.commitTransportFailuresRemaining > 0) {
      this.commitTransportFailuresRemaining -= 1;
      callback(grpcServiceError(status.UNAVAILABLE), {
        ack: undefined,
        refsOnlyResultJson: "",
        declaration: undefined,
      });
      return;
    }
    const key = bridgeResultKey(request);
    const existing = this.stored.get(key);
    if (existing !== undefined) {
      if (!sameBridgeMcpResult(existing, request)) {
        callback(grpcServiceError(status.ALREADY_EXISTS), { ack: undefined, refsOnlyResultJson: "", declaration: undefined });
        return;
      }
      callback(null, {
        ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_DUPLICATE, runtimeInputId: "", runtimeWriteId: "", errorCode: "" },
        refsOnlyResultJson: existing.resultJson,
        materializationHandle: request.toolUseEventId,
        declaration: this.declarations.get(key),
      });
      return;
    }
    const claim = this.claims.get(key);
    if (claim === undefined || claim.scope?.requestId !== request.scope?.requestId || !sameBridgeMcpResult(claim, request)) {
      callback(null, {
        ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "mcp_claim_not_owned" },
        refsOnlyResultJson: "",
        declaration: undefined,
      });
      return;
    }
    this.claims.delete(key);
    const refsOnlyResultJson = fakeBridgeCompleteMcpAttachmentRefs(request);
    const declaration = fakeMcpMaterializationDeclaration(request, refsOnlyResultJson);
    this.stored.set(key, { ...request, resultJson: refsOnlyResultJson, inlineMedia: [] });
    this.declarations.set(key, declaration);
    callback(null, {
      ack: { status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED, runtimeInputId: "", runtimeWriteId: "", errorCode: "" },
      refsOnlyResultJson,
      materializationHandle: request.toolUseEventId,
      declaration,
    });
  }
}

function fakeMcpMaterializationDeclaration(request: CommitMcpToolResultRequest, refsOnlyResultJson: string) {
  const parsed = JSON.parse(refsOnlyResultJson) as {
    response: {
      attachments: Array<{
        attachment_ref: string;
        mime: string;
        suggested_filename: string;
      }>;
    };
  };
  return {
    receipts: [{
      sessionThreadId: request.scope?.sessionThreadId ?? "",
      operationKind: "commit_mcp_tool_result",
      sourceKind: "mcp_tool_execution",
      operationId: fakeMcpMaterializationOperationId(request),
      events: [],
      messages: [],
      pendingAttachmentDeltaJson: parsed.response.attachments.map((attachment) => JSON.stringify({
        origin: {
          transient: {
            attachmentRef: attachment.attachment_ref,
            sourceToolUseEventId: request.toolUseEventId,
            sourcePath: `mcp:${request.mcpServerName}/${attachment.suggested_filename}`,
            detail: "auto",
          },
        },
        mime: attachment.mime,
        filename: attachment.suggested_filename,
      })),
      interruptToolProjections: [],
      prefixConsumptions: [],
      declarationDigest: fakeMcpMaterializationDeclarationDigest(request),
      requestReschedule: undefined,
      childLifecycle: [],
      requestStart: undefined,
      idleCloseout: undefined,
      compactedThroughMessageSequence: undefined,
    }],
    observedBindingId: request.scope?.binding?.bindingId ?? "",
    observedBindingGeneration: request.scope?.binding?.bindingGeneration ?? 0,
    applicationDisposition: ReceiptApplicationDisposition.RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY,
  };
}

function fakeMcpMaterializationOperationId(request: CommitMcpToolResultRequest): string {
  const encoder = new TextEncoder();
  const parts = [
    "mcp_tool_execution",
    request.toolUseEventId,
    request.normalizedInputHash,
  ].map((part) => encoder.encode(part));
  const framed = new Uint8Array(parts.reduce((total, part) => total + 4 + part.byteLength, 0));
  const view = new DataView(framed.buffer);
  let offset = 0;
  for (const part of parts) {
    view.setUint32(offset, part.byteLength, false);
    offset += 4;
    framed.set(part, offset);
    offset += part.byteLength;
  }
  return `stid_${createHash("sha256").update(framed).digest("hex")}`;
}

function fakeMcpMaterializationDeclarationDigest(request: CommitMcpToolResultRequest): string {
  const inlineMedia = request.inlineMedia.map((media) => ({
      content_sha256: createHash("sha256").update(media.data).digest("hex"),
      mime: media.mime,
      suggested_filename: media.suggestedFilename,
    }));
  const declaration = `{
    "inline_media":${JSON.stringify(inlineMedia)},
    "input":${canonicalRunToolJSON(request.inputJson)},
    "mcp_server_name":${JSON.stringify(request.mcpServerName)},
    "normalized_input_hash":${JSON.stringify(request.normalizedInputHash)},
    "operation_kind":"commit_mcp_tool_result",
    "result":${canonicalRunToolJSON(request.resultJson)},
    "session_thread_id":${JSON.stringify(request.scope?.sessionThreadId ?? "")},
    "tool_name":${JSON.stringify(request.toolName)},
    "tool_use_event_id":${JSON.stringify(request.toolUseEventId)}
  }`;
  return createHash("sha256").update(canonicalRunToolJSON(declaration), "utf8").digest("hex");
}

class DeadlineClaimBridgeClient {
  readonly claimOptions: CallOptions[] = [];

  claimMcpToolResult(
    _request: ClaimMcpToolResultRequest,
    _metadata: Metadata,
    options: CallOptions,
    callback: (error: ServiceError | null, response: ClaimMcpToolResultResponse) => void,
  ) {
    this.claimOptions.push(options);
    callback(grpcServiceError(status.DEADLINE_EXCEEDED), { ack: undefined, resultJson: "", declaration: undefined });
  }

  commitMcpToolResult() {
    throw new Error("commit must not run after a failed Claim");
  }
}

function fakeBridgeCompleteMcpAttachmentRefs(request: CommitMcpToolResultRequest): string {
  const body = JSON.parse(request.resultJson) as {
    response: { attachments: Array<Record<string, unknown>> };
  };
  body.response.attachments = body.response.attachments.map((attachment, index) => ({
    ...attachment,
    attachment_ref: `att_bridge_${index + 1}`,
  }));
  return JSON.stringify(body);
}

function bridgeResultKey(request: ClaimMcpToolResultRequest | CommitMcpToolResultRequest): string {
  return `${request.scope?.workspaceId}/${request.scope?.sessionId}/${request.scope?.sessionThreadId}/${request.toolUseEventId}`;
}

function sameBridgeMcpResult(left: ClaimMcpToolResultRequest | CommitMcpToolResultRequest, right: ClaimMcpToolResultRequest | CommitMcpToolResultRequest): boolean {
  return left.normalizedInputHash === right.normalizedInputHash &&
    left.mcpServerName === right.mcpServerName &&
    left.toolName === right.toolName;
}

function grpcServiceError(code: status): ServiceError {
  const error = new Error("grpc error") as ServiceError;
  error.code = code;
  error.details = "grpc error";
  error.metadata = new Metadata();
  return error;
}

class AllowingAuthenticator implements McpAuthenticator {
  async authenticate() {
    return { ok: true as const, serviceAccount: { namespace: "tetral-agent-runtime", name: "agent-runtime", podUid: RuntimePodUid } };
  }
}

class MemoryLogger implements McpConnectorLogger {
  readonly records: unknown[] = [];

  info(record: unknown): void {
    this.records.push(record);
  }

  error(record: unknown): void {
    this.records.push(record);
  }
}

function deferred<T>(): { readonly promise: Promise<T>; readonly resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("condition was not met");
}

void GrpcStatusError;
