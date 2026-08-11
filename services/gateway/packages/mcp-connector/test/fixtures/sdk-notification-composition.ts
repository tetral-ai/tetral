import { mock } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Metadata, Server, ServerCredentials } from "@grpc/grpc-js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { Server as McpServer } from "@modelcontextprotocol/sdk/server/index.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import {
  AgentRuntimeBridgeServiceService,
  BridgeWriteStatus,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  AgentRuntimeBridgeServiceServer,
  McpManifestChangedRequest,
} from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { SchemaSQL } from "../../../schema/src/verify.js";

const [sdkClientTransport, sdkServerTransport] = InMemoryTransport.createLinkedPair();
const sdkServer = new McpServer({ name: "notification-proof", version: "1.0.0" }, {
  capabilities: { tools: { listChanged: true } },
});
let sdkListCalls = 0;
sdkServer.setRequestHandler(ListToolsRequestSchema, async () => {
  sdkListCalls += 1;
  return {
    tools: [{ name: "create_issue", description: "Create an issue.", inputSchema: { type: "object" } }],
  };
});
await sdkServer.connect(sdkServerTransport);

class ProductionSDKTransport {
  set onclose(handler: NonNullable<Transport["onclose"]>) { sdkClientTransport.onclose = handler; }
  set onerror(handler: NonNullable<Transport["onerror"]>) { sdkClientTransport.onerror = handler; }
  set onmessage(handler: NonNullable<Transport["onmessage"]>) { sdkClientTransport.onmessage = handler; }
  get sessionId() { return sdkClientTransport.sessionId; }
  async start() { await sdkClientTransport.start(); }
  async close() { await sdkClientTransport.close(); }
  async send(message: Parameters<Transport["send"]>[0], options?: Parameters<Transport["send"]>[1]) {
    await sdkClientTransport.send(message, options);
  }
}

mock.module("@modelcontextprotocol/sdk/client/streamableHttp.js", () => ({
  StreamableHTTPClientTransport: ProductionSDKTransport,
}));
mock.module("../../src/credential.js", () => ({
  SQLGitHubMcpCredentialResolver: class FakeCredentialResolver {
    async resolve() {
      return {
        ok: true as const,
        mode: "bearer" as const,
        token: "mcp-token",
        tokenHash: "mcp-token-hash",
        vaultId: "vlt_1",
        credentialId: "cred_1",
      };
    }

    async refresh() {
      return await this.resolve();
    }
  },
}));
mock.module("../../src/auth.js", () => ({
  authenticateMcpCaller: async () => ({
    ok: true as const,
    serviceAccount: { namespace: "tetral", name: "bridge", podUid: "pod_bridge" },
  }),
  buildOutboundBearerMetadata: async ({ tokenPath }: { readonly tokenPath: string }) => {
    const metadata = new Metadata();
    metadata.set("authorization", `bearer ${(await Bun.file(tokenPath).text()).trim()}`);
    return metadata;
  },
  KubernetesTokenReviewClient: class FakeTokenReviewClient {},
  validateKubernetesTokenReviewReviewerMaterial: async () => undefined,
}));

const fixtureDir = await mkdtemp(join(tmpdir(), "mcp-notification-composition-"));
const tokenPath = join(fixtureDir, "bridge-token");
const bridgeRequests: McpManifestChangedRequest[] = [];
let serviceFromCommand: import("../../src/service.js").McpConnectorServiceShell | undefined;
const bridgeServer = new Server();
bridgeServer.addService(AgentRuntimeBridgeServiceService, {
  mcpManifestChanged: (call, callback) => {
    bridgeRequests.push(call.request);
    callback(null, {
      ack: {
        status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_COMMITTED,
        runtimeInputId: "runtime_config_update:mcp_manifest:sesn_1:github:1",
        runtimeWriteId: "",
        errorCode: "",
      },
    });
  },
} as AgentRuntimeBridgeServiceServer);

try {
  await writeFile(tokenPath, "projected-token\n", { mode: 0o600 });
  const bridgePort = await bindLocalServer(bridgeServer);
  Object.assign(process.env, validEnv(bridgePort, tokenPath));

  const { runMcpConnectorCommand } = await import("../../src/command.js");
  const sql = ((<T>(_strings: TemplateStringsArray): PromiseLike<T> => Promise.resolve([] as T)) as SchemaSQL & {
    close: () => Promise<void>;
  });
  sql.close = async () => undefined;
  let initialListCalls = 0;
  let notificationListCalls = 0;
  await runMcpConnectorCommand({
    sql,
    schemaVerifier: async () => undefined,
    reviewerMaterialValidator: async () => undefined,
    logger: { info: () => undefined, error: () => undefined },
    serverFactory: (service) => {
      serviceFromCommand = service;
      return {
        server: undefined as never,
        bind: async () => 9091,
        shutdown: async () => undefined,
      };
    },
    httpServerFactory: () => ({
      url: new URL("http://127.0.0.1:8081"),
      stop: async () => undefined,
    }),
    registerSignalHandlers: () => undefined,
    waitForever: async () => {
      const service = capturedService();
      await service.listMcpTools({ workspaceId: "wksp_1", sessionId: "sesn_1", mcpServerName: "github" }, new Metadata());
      initialListCalls = sdkListCalls;

      await sdkServer.sendToolListChanged();
      await sdkServer.sendToolListChanged();
      await until(() => bridgeRequests.length === 2);
      notificationListCalls = sdkListCalls - initialListCalls;
      return undefined as never;
    },
  });

  process.stdout.write(`${JSON.stringify({ initialListCalls, notificationListCalls, bridgeRequests })}\n`);
} finally {
  await sdkServer.close();
  bridgeServer.forceShutdown();
  await rm(fixtureDir, { recursive: true, force: true });
}

process.exit(0);

function capturedService(): import("../../src/service.js").McpConnectorServiceShell {
  if (serviceFromCommand === undefined) throw new Error("command service was not composed");
  return serviceFromCommand;
}

function validEnv(bridgePort: number, outboundTokenPath: string): Record<string, string> {
  return {
    TETRAL_MCP_CONNECTOR_GRPC_ADDR: "127.0.0.1:9091",
    TETRAL_MCP_CONNECTOR_HTTP_ADDR: "127.0.0.1:8081",
    TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
    TETRAL_SERVICE_VERSION: "composition",
    TETRAL_INTERNAL_GRPC_AUDIENCE: "tetral-internal-grpc",
    TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "tetral/runtime",
    TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS: "tetral/bridge",
    TETRAL_BRIDGE_API_GRPC_ADDR: `127.0.0.1:${bridgePort}`,
    TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH: outboundTokenPath,
    TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY: "x".repeat(32),
    TETRAL_DATABASE_URL: "postgres://unused",
    ENGINE_VAULT_KEY: "01".repeat(32),
    KUBERNETES_API_SERVER_URL: "https://kubernetes.invalid",
    KUBERNETES_API_CA_CERT_PATH: "/unused-ca",
    KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: "/unused-token",
  };
}

async function bindLocalServer(server: Server): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) => {
      if (error !== null) reject(error);
      else resolve(port);
    });
  });
}

async function until(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("notification composition did not settle");
    await Bun.sleep(1);
  }
}
