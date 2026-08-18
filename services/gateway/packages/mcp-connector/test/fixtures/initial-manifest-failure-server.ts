import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import { McpConnectorError } from "../../src/errors.js";
import { InMemoryMcpIdempotencyStore } from "../../src/idempotency.js";
import { createMcpConnectorGrpcServer } from "../../src/server.js";
import { McpConnectorServiceShell } from "../../src/service.js";
import type { McpClient, McpConnectorLogger } from "../../src/service.js";

let listCalls = 0;
const records: unknown[] = [];
const logger: McpConnectorLogger = {
  info: (record) => records.push(record),
  error: (record) => records.push(record),
};
const client: McpClient = {
  listTools: async ({ sessionId }) => {
    listCalls += 1;
    if (sessionId.includes("credential")) {
      throw new McpConnectorError("mcp_credential_required", "CREDENTIAL_BODY_CANARY", "terminal");
    }
    if (sessionId.includes("server")) {
      throw new McpConnectorError("mcp_connection_failed", "SERVER_BODY_CANARY", "exhausted");
    }
    if (sessionId.includes("timeout")) {
      throw new McpConnectorError("mcp_timeout", "TIMEOUT_BODY_CANARY");
    }
    if (sessionId.includes("invalid")) {
      return [{
        name: "invalid_manifest_tool",
        description: "x".repeat(100_000),
        inputSchema: { type: "object" },
      }];
    }
    throw new Error("UNTYPED_BODY_CANARY");
  },
  callTool: async () => {
    throw new Error("tool execution is outside this fixture");
  },
};
const service = new McpConnectorServiceShell({
  ready: () => true,
  client,
  logger,
  authenticator: {
    authenticate: async () => ({
      ok: true,
      serviceAccount: { namespace: "tetral-system", name: "bridge", podUid: "bridge-pod" },
    }),
  },
  runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({
    hmacKey: "manifest-failure-binding-key-32bytes",
  }),
  idempotencyStore: new InMemoryMcpIdempotencyStore({ mcpServerName: "github", toolName: "create_issue", inputJson: "{}" }),
  manifestChangeNotifier: {
    notify: async () => ({ ok: true, duplicate: false }),
  },
});
const server = createMcpConnectorGrpcServer(service);
const port = await server.bind("127.0.0.1:0");
process.stdout.write(`${JSON.stringify({ address: `127.0.0.1:${port}` })}\n`);
await new Promise<void>((resolve) => process.stdin.once("data", () => resolve()));
const serialized = JSON.stringify(records);
const finalLine = `${JSON.stringify({
  listCalls,
  logsRedacted: !serialized.includes("BODY_CANARY"),
})}\n`;
await new Promise<void>((resolve, reject) => {
  process.stdout.write(finalLine, (error) => error === null ? resolve() : reject(error));
});
process.exit(0);
