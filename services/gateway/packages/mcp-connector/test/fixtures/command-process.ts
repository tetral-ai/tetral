import { fixtureServerResolver } from "./registered-server.js";
// Test-owned process fixture exercises acquired MCP listeners, SDK client and SQL.
import { runMcpConnectorCommand } from "../../src/command.js";
import { McpSDKClient } from "../../src/client.js";
import type { SchemaSQL } from "../../../schema/src/verify.js";
import { runProcessEntry } from "@tetral/ts-observability";
export const failureSentinel = "PRIVATE_COMMAND_FAILURE_NON_TOKEN_SENTINEL";
export function commandEnv(): Record<string, string> {
  return {
    TETRAL_MCP_CONNECTOR_GRPC_ADDR: "127.0.0.1:9091",
    TETRAL_MCP_CONNECTOR_HTTP_ADDR: "127.0.0.1:8081",
    TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
    TETRAL_SERVICE_VERSION: "unit",
    TETRAL_INTERNAL_GRPC_AUDIENCE: "tetral-internal-grpc",
    TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "tetral/runtime",
    TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS: "tetral/bridge",
    TETRAL_BRIDGE_API_GRPC_ADDR: "bridge:9090",
    TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH: "/bridge-token",
    TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY: "x".repeat(32),
    TETRAL_DATABASE_URL: "postgres://gateway",
    ENGINE_VAULT_KEY: "01".repeat(32),
    KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc",
    KUBERNETES_API_CA_CERT_PATH: "/ca",
    KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: "/token",
  };
}
export function commandFixture(mode: string, event: (name: string) => void = () => undefined) {
  const failure = new Error(failureSentinel);
  const laterFailure = new Error("PRIVATE_LATER_CLOSE_NON_TOKEN_SENTINEL");
  const close = async (phase: string) => {
    event(phase + ".close");
    if (mode === phase || mode.endsWith("both_cleanup") || (phase === "http" && mode.endsWith("_failure"))) throw phase === "http" ? failure : laterFailure;
  };
  const sql = ((<T>(_strings: TemplateStringsArray): PromiseLike<T> => Promise.resolve([] as T)) as SchemaSQL & { close: () => Promise<void> });
  sql.close = async () => close("database");
  const client = new McpSDKClient({serverResolver: fixtureServerResolver,
    credentialResolver: { resolve: async () => { throw new Error("unused resolve"); }, refresh: async () => { throw new Error("unused refresh"); } },
    onToolsListChanged: async () => undefined,
  });
  const originalClose = client.closeAll.bind(client);
  client.closeAll = async () => { await originalClose(); await close("mcp_clients"); };
  let ready: (() => { readonly ready: boolean }) | undefined;
  const options: Parameters<typeof runMcpConnectorCommand>[0] = {
    sql, client,
    schemaVerifier: async () => { event("dependency.build"); if (mode === "dependency") throw failure; },
    reviewerMaterialValidator: async () => { if (mode === "reviewer") throw failure; },
    manifestChangeNotifier: { notify: async () => ({ ok: true, duplicate: false }) },
    serverFactory: () => ({ server: undefined as never, bind: async () => { event("listener.start"); if (mode === "listener") throw failure; return 1; }, shutdown: async () => close("grpc") }),
    httpServerFactory: (_address, state) => { ready = state.ready; if (mode === "http_bind") throw failure; return { url: new URL("http://127.0.0.1:1"), stop: async () => { event("http.ready:" + ready?.().ready); await close("http"); } }; },
    waitForever: async () => {
      event("wait.ready:" + ready?.().ready); event("wait");
      if (mode.startsWith("wait")) throw failure;
      if (mode.startsWith("SIG")) { setTimeout(() => process.kill(process.pid, mode.startsWith("SIGINT") ? "SIGINT" : "SIGTERM"), 0); return await new Promise<never>(() => undefined); }
      return undefined as never;
    },
  };
  return { options, failure, laterFailure };
}
if (import.meta.main) {
  Object.assign(process.env, commandEnv());
  const fixture = commandFixture(process.argv[2] ?? "none", (event) => process.stdout.write(event + "\n"));
  await runProcessEntry((processBoundary) => runMcpConnectorCommand({ ...fixture.options, processBoundary }));
}
