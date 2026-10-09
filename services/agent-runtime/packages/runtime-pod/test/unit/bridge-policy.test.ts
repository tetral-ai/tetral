import { expect, test } from "bun:test";
import { credentials, Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceClient,
  AgentRuntimeBridgeServiceService,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import projection from "../../src/bridge-method-policy.json";
import { BridgeUnaryCalls } from "../../src/bridge-calls.js";
import {
  bridgeMethodDeadline,
  DefaultBridgeMethodPolicies,
  parseBridgeMethodPolicies,
} from "../../src/bridge-policy.js";

test("Bridge deadline policy and proxy projection cover the exact descriptor", () => {
  const methods = Object.keys(AgentRuntimeBridgeServiceService).sort();
  expect(Object.keys(DefaultBridgeMethodPolicies).sort()).toEqual(methods);
  expect(
    projection.map((entry) => entry.method[0]!.toLowerCase() + entry.method.slice(1)).sort(),
  ).toEqual(methods);
  for (const entry of projection) {
    const method = (entry.method[0]!.toLowerCase() +
      entry.method.slice(1)) as keyof typeof DefaultBridgeMethodPolicies;
    const policy = DefaultBridgeMethodPolicies[method];
    expect(entry.path).toBe(AgentRuntimeBridgeServiceService[method].path);
    expect(entry.kind).toBe(policy.kind);
    expect(entry.timeoutMs).toBe(policy.kind === "fixed" ? policy.timeoutMs : null);
  }
  expect(
    parseBridgeMethodPolicies({ TETRAL_BRIDGE_WRITE_EVENT_TIMEOUT_MS: "7000" })?.writeEvent,
  ).toEqual({ kind: "fixed", timeoutMs: 7000 });
  expect(parseBridgeMethodPolicies({ TETRAL_BRIDGE_WRITE_EVENT_TIMEOUT_MS: "07" })).toBeUndefined();
  expect(
    parseBridgeMethodPolicies({ TETRAL_BRIDGE_READ_FILE_ATTACHMENT_CHUNK_TIMEOUT_MS: "7000" }),
  ).toBeUndefined();
  expect(bridgeMethodDeadline(DefaultBridgeMethodPolicies, "finishIdle", 1000, 2000)).toBe(2000);
});

test("configured unary deadline cancels the real Bridge handler and joins its client", async () => {
  const server = new Server();
  let cancelled!: () => void;
  const stopped = new Promise<void>((resolve) => {
    cancelled = resolve;
  });
  server.addService(AgentRuntimeBridgeServiceService, {
    loadContext: (call: { on(name: string, callback: () => void): void }) => {
      call.on("cancelled", cancelled);
    },
  });
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) =>
      error ? reject(error) : resolve(port),
    ),
  );
  const client = new AgentRuntimeBridgeServiceClient(
    `127.0.0.1:${port}`,
    credentials.createInsecure(),
  );
  const policies = parseBridgeMethodPolicies({ TETRAL_BRIDGE_LOAD_CONTEXT_TIMEOUT_MS: "80" })!;
  const owner = new BridgeUnaryCalls(client, policies);
  try {
    await expect(
      owner.call(
        "loadContext",
        {
          scope: {
            workspaceId: "wksp_test",
            sessionId: "sesn_test",
            sessionThreadId: "thrd_test",
            binding: {
              bindingId: "binding_test",
              bindingGeneration: 1,
              targetPodUid: "pod_test",
              runtimeProcessId: "process_test",
            },
          },
          sourceEventId: "",
          handoffId: "",
        },
        new Metadata(),
      ),
    ).rejects.toMatchObject({ code: status.DEADLINE_EXCEEDED });
    await stopped;
    const parent = new AbortController();
    const attempt = owner.call(
      "loadContext",
      {
        scope: {
          workspaceId: "wksp_test",
          sessionId: "sesn_test",
          sessionThreadId: "thrd_test",
          binding: {
            bindingId: "binding_test",
            bindingGeneration: 1,
            targetPodUid: "pod_test",
            runtimeProcessId: "process_test",
          },
        },
        sourceEventId: "",
        handoffId: "",
      },
      new Metadata(),
      { signal: parent.signal },
    );
    setTimeout(() => parent.abort(), 10);
    await expect(attempt).rejects.toMatchObject({ code: status.CANCELLED });
    await owner.close();
    await owner.close();
  } finally {
    await owner.close();
    server.forceShutdown();
  }
}, 2000);
