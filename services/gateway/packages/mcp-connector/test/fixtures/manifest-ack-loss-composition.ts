import { mock } from "bun:test";
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

type ToolsChanged = (error?: unknown) => void;

const sdkClients: FakeSDKClient[] = [];

class FakeSDKClient {
  onerror: ((error: Error) => void) | undefined;
  listCalls = 0;
  readonly #toolsChanged: ToolsChanged;

  constructor(
    _identity: unknown,
    options: { readonly listChanged: { readonly tools: { readonly onChanged: ToolsChanged } } },
  ) {
    this.#toolsChanged = options.listChanged.tools.onChanged;
    sdkClients.push(this);
  }

  async connect(): Promise<void> {}

  async listTools() {
    this.listCalls += 1;
    return {
      tools: [{ name: "github_search", description: "Search GitHub", inputSchema: { type: "object" } }],
    };
  }

  async callTool() {
    return { content: [] };
  }

  async close(): Promise<void> {}

  emitToolsChanged(): void {
    this.#toolsChanged(undefined);
  }
}

mock.module("@modelcontextprotocol/sdk/client/index.js", () => ({ Client: FakeSDKClient }));
mock.module("@modelcontextprotocol/sdk/client/streamableHttp.js", () => ({
  StreamableHTTPClientTransport: class FakeTransport {},
}));

const { McpSDKClient } = await import("../../src/client.js");
const sleeps: number[] = [];
const notificationResults: Array<Awaited<ReturnType<McpConnectorServiceShell["handleToolsListChangedNotification"]>>> = [];
let service: McpConnectorServiceShell;
const client = new McpSDKClient({
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
const sdk = sdkClients[0];
if (sdk === undefined) throw new Error("production MCP SDK client was not constructed");
sdk.emitToolsChanged();
await until(() => notificationResults.length === 1);
sdk.emitToolsChanged();
await until(() => notificationResults.length === 2);

process.stdout.write(`${JSON.stringify({
  initialManifestEtag: initial.manifestEtag,
  recovered: notificationResults[0],
  laterReplay: notificationResults[1],
  listCalls: sdk.listCalls,
  sleeps,
})}\n`);

async function until(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("manifest notification did not settle");
    await Bun.sleep(1);
  }
}
