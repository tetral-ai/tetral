import { createHmac } from "node:crypto";
import { access, readFile, rename, writeFile } from "node:fs/promises";
import { credentials, Metadata } from "@grpc/grpc-js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { AgentRuntimeBridgeServiceClient } from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { McpConnectorServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RunMcpToolRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import { BridgeAPIMcpToolResultIdempotencyStore } from "../../src/bridge-client.js";
import { McpSDKClient } from "../../src/client.js";
import { createMcpConnectorGrpcServer } from "../../src/server.js";
import { McpConnectorServiceShell } from "../../src/service.js";

// The Go side polls these handoff files, so each appears complete or not at all.
let handoffWrites = 0;
const writeHandoffFile = async (path: string, contents: string) => {
  const temporary = `${path}.${process.pid}.${++handoffWrites}.tmp`;
  await writeFile(temporary, contents);
  await rename(temporary, path);
};

const input = JSON.parse(await readFile(process.argv[2]!, "utf8")) as {
  directory: string;
  bridges: string[];
  externalURL: string;
  claimID: string;
  bootID: string;
  retryReceiver: boolean;
  request: RunMcpToolRequest;
  podUID: string;
};
const exists = async (path: string) => {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
};
const wait = async (name: string) => {
  while (!(await exists(`${input.directory}/${name}`))) await Bun.sleep(10);
};
const metadata = new Metadata();
metadata.set("authorization", "Bearer mcp");
const receivers = input.bridges.map(
  (address) =>
    new AgentRuntimeBridgeServiceClient(address, credentials.createInsecure()),
);
let commitAttempts = 0;
const commitRequests: unknown[] = [];
const bridgeClient = {
  claimMcpToolResult: receivers[0]!.claimMcpToolResult.bind(receivers[0]),
  relinquishMcpToolResult: receivers[0]!.relinquishMcpToolResult.bind(
    receivers[0],
  ),
  commitMcpToolResult: (
    ...args: Parameters<AgentRuntimeBridgeServiceClient["commitMcpToolResult"]>
  ) => {
    const receiver = input.retryReceiver && commitAttempts > 0 ? 1 : 0;
    commitAttempts++;
    commitRequests.push(args[0]);
    void writeFile(
      `${input.directory}/commit-attempts.json`,
      JSON.stringify({ commitRequests, commitAttempts, receiver }),
    );
    return receivers[receiver]!.commitMcpToolResult(...args);
  },
  close: () => {
    for (const receiver of receivers) receiver.close();
  },
};
const store = new BridgeAPIMcpToolResultIdempotencyStore({
  address: input.bridges[0]!,
  tokenPath: "unused",
  client: bridgeClient,
  metadataFactory: async () => metadata,
  claimTimeoutMs: 2000,
  commitTimeoutMs: 500,
  relinquishTimeoutMs: 500,
});
const sdk = new McpSDKClient({
  credentialResolver: {
    resolve: async () => ({
      ok: true,
      mode: "bearer",
      token: "fixture",
      tokenHash: "fixture",
      vaultId: "fixture",
      credentialId: "fixture",
    }),
    refresh: async () => ({ ok: false, error: "refresh_unavailable" }),
  },
  onToolsListChanged: async () => undefined,
  createTransport: () =>
    new StreamableHTTPClientTransport(new URL(input.externalURL)),
  callTimeoutMs: 2000,
  connectTimeoutMs: 1000,
  idleTimeoutMs: 1000,
});
const key = "replica-handoff-shared-token-signing-key";
const claims = {
  v: 1,
  workspace_id: input.request.workspaceId,
  session_id: input.request.sessionId,
  session_thread_id: input.request.sessionThreadId,
  binding_id: input.request.bindingId,
  binding_generation: input.request.bindingGeneration,
  runtime_pod_uid: input.podUID,
  runtime_process_id: input.request.runtimeProcessId,
  exp: Math.floor(Date.now() / 1000) + 300,
};
const payload = Buffer.from(JSON.stringify(claims)).toString("base64url");
const request = {
  ...input.request,
  runtimeBindingToken: `rtbt_v1.${payload}.${createHmac("sha256", key).update(payload).digest("base64url")}`,
};
const service = new McpConnectorServiceShell({
  ready: () => true,
  logger: { info: () => undefined, error: () => undefined },
  authenticator: {
    authenticate: async ({ metadata }) => ({
      ok: true,
      serviceAccount: {
        namespace: "tetral-agent-runtime",
        name: "agent-runtime",
        podUid:
          metadata.get("authorization")[0] === "Bearer new"
            ? "pod_new"
            : input.podUID,
      },
    }),
  },
  runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({
    hmacKey: key,
  }),
  client: sdk,
  idempotencyStore: store,
  claimIdFactory: () => input.claimID,
});
const server = createMcpConnectorGrpcServer(service),
  port = await server.bind("127.0.0.1:0");
const caller = new McpConnectorServiceClient(
  `127.0.0.1:${port}`,
  credentials.createInsecure(),
);
await writeHandoffFile(
  `${input.directory}/ready.json`,
  JSON.stringify({ port, pid: process.pid, bootID: input.bootID }),
);
let shutdown: Promise<void> | undefined;
const close = () =>
  (shutdown ??= (async () => {
    const drainDeadline = new Date(Date.now() + 200),
      deadline = new Date(drainDeadline.getTime() + 1000);
    store.beginDrain(drainDeadline);
    await service.shutdown(deadline, drainDeadline);
    await sdk.closeAll(deadline);
    await store.close();
    await server.shutdown(deadline);
    caller.close();
  })());
try {
  await wait("run");
  const started = performance.now();
  const call = new Promise<unknown>((resolve) =>
    caller.runMcpTool(
      request,
      new Metadata(),
      { deadline: new Date(Date.now() + 2000) },
      (error, response) =>
        resolve(error === null ? { response } : { code: error.code }),
    ),
  );
  // This watcher belongs to the fixture scope and is stopped and joined below.
  const stopped = new AbortController();
  const watcher = (async () => {
    while (!stopped.signal.aborted) {
      if (await exists(`${input.directory}/shutdown`)) {
        await writeHandoffFile(
          `${input.directory}/draining.json`,
          JSON.stringify({ bootID: input.bootID }),
        );
        await close();
        return;
      }
      await Bun.sleep(10);
    }
  })();
  const result = await call;
  await writeHandoffFile(
    `${input.directory}/result.json`,
    JSON.stringify({
      result,
      durationMs: performance.now() - started,
      commitAttempts,
      commitRequests,
      bootID: input.bootID,
    }),
  );
  stopped.abort();
  await watcher;
  await wait("shutdown");
  await close();
  await writeHandoffFile(
    `${input.directory}/closed.json`,
    JSON.stringify({
      bootID: input.bootID,
      connections: sdk.connectionCount(),
    }),
  );
} finally {
  await close();
}
