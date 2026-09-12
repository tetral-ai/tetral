import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { McpSDKClient } from "../../src/client.js";
import { Metadata } from "@grpc/grpc-js";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import { BridgeAPIManifestChangeNotifier } from "../../src/bridge-client.js";
import { InMemoryMcpIdempotencyStore } from "../../src/idempotency.js";
import { McpConnectorServiceShell } from "../../src/service.js";

const bridgeAddress = process.argv[2];
const tokenPath = process.argv[3];
if (bridgeAddress === undefined || tokenPath === undefined) {
  throw new Error("bridge address and token path are required");
}

let listCalls = 0;
const sdkServer = new Server({ name: "manifest-fixture", version: "1" }, { capabilities: { tools: { listChanged: true } } });
sdkServer.setRequestHandler(ListToolsRequestSchema, async () => {
  listCalls += 1;
  return { tools: [{ name: "github_search", description: "Search GitHub", inputSchema: { type: "object" as const } }] };
});
const sleeps: number[] = [];
const notificationResults: Array<Awaited<ReturnType<McpConnectorServiceShell["handleToolsListChangedNotification"]>>> = [];
let service: McpConnectorServiceShell;
const client = new McpSDKClient({
  createTransport: () => {
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    void sdkServer.connect(serverTransport);
    return clientTransport;
  },
  credentialResolver: {
    resolve: async () => ({
      ok: true as const,
      mode: "bearer" as const,
      token: "mcp-token",
      tokenHash: "mcp-token-hash",
      vaultId: "vlt_1",
      credentialId: "cred_1",
    }),
    refresh: async () => ({
      ok: true as const,
      mode: "bearer" as const,
      token: "mcp-token",
      tokenHash: "mcp-token-hash",
      vaultId: "vlt_1",
      credentialId: "cred_1",
    }),
  },
  onToolsListChanged: async (input) => {
    notificationResults.push(await service.handleToolsListChangedNotification(input));
  },
});
service = new McpConnectorServiceShell({
  authenticator: {
    authenticate: async () => ({
      ok: true as const,
      serviceAccount: { namespace: "tetral", name: "bridge", podUid: "pod_bridge" },
    }),
  },
  runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({
    hmacKey: "manifest-ack-loss-composition-key",
  }),
  logger: { info: () => undefined, error: () => undefined },
  ready: () => true,
  client,
  idempotencyStore: new InMemoryMcpIdempotencyStore({ mcpServerName: "github", toolName: "create_issue", inputJson: "{}" }),
  manifestChangeNotifier: new BridgeAPIManifestChangeNotifier({
    address: bridgeAddress,
    tokenPath,
  }),
  manifestNotifySleep: async (delayMs) => {
    sleeps.push(delayMs);
  },
});
const input = { workspaceId: "default", sessionId: "sesn_mcp_ack_loss", mcpServerName: "github" };
const initial = await service.listMcpTools(input, new Metadata());
await sdkServer.sendToolListChanged();
await until(() => notificationResults.length === 1);
await sdkServer.sendToolListChanged();
await until(() => notificationResults.length === 2);

await client.closeAll();
await sdkServer.close();

process.stdout.write(`${JSON.stringify({
  initialManifestEtag: initial.manifestEtag,
  recovered: notificationResults[0],
  laterReplay: notificationResults[1],
  listCalls,
  sleeps,
})}\n`);

async function until(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("manifest notification did not settle");
    await Bun.sleep(1);
  }
}
