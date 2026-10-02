// Actual MCP service owns an SDK listing through cancellation; command retains SQL custody.
import { Metadata } from "@grpc/grpc-js";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  runProcessEntry,
  registerProcessSignalHandlers,
} from "@tetral/ts-observability";
import { runMcpConnectorCommand } from "../../src/command.js";
import { createMcpConnectorGrpcServer } from "../../src/server.js";
import type { McpConnectorServiceShell } from "../../src/service.js";
import type { McpCredentialSQL } from "../../src/credential.js";
import { commandEnv, commandFixture } from "./command-process.js";
const [sink = "normal", trigger = "SIGTERM", mode = "held"] =
  process.argv.slice(2);
const credentialMode = mode.startsWith("credential-");
const directory =
  process.argv[5] ?? (await mkdtemp(join(tmpdir(), "mcp-shutdown-")));
await writeFile(join(directory, "token"), "synthetic-reviewer");
await writeFile(join(directory, "ca"), "synthetic-http-ca");
const review = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  fetch: () =>
    Response.json({
      status: {
        authenticated: true,
        audiences: ["tetral-internal-grpc"],
        user: {
          username: "system:serviceaccount:tetral:bridge",
          extra: { "authentication.kubernetes.io/pod-uid": ["fixture-pod"] },
        },
      },
    }),
});
Object.assign(process.env, commandEnv(), {
  TETRAL_MCP_CONNECTOR_GRPC_ADDR: "127.0.0.1:0",
  TETRAL_MCP_CONNECTOR_HTTP_ADDR: "127.0.0.1:0",
  TETRAL_SERVICE_DRAIN_TIMEOUT_MS: credentialMode ? "200" : "2000",
  TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS: credentialMode ? "1000" : "3000",
  ...(credentialMode ? { TETRAL_MCP_CREDENTIAL_TIMEOUT_MS: "10" } : {}),
  KUBERNETES_API_SERVER_URL: review.url.toString(),
  KUBERNETES_API_CA_CERT_PATH: join(directory, "ca"),
  KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: join(directory, "token"),
});
const emit = (event: string) =>
  process.stdout.write(JSON.stringify({ event, at: Date.now() }) + "\n");
const observe = (record: Record<string, unknown>) => {
  if (sink === "throw") throw new Error("synthetic diagnostic sink failure");
  if (sink === "normal") process.stderr.write(JSON.stringify(record) + "\n");
};
let release!: () => void, entered!: () => void;
const held = new Promise<void>((resolve) => {
  release = resolve;
});
const admission = new Promise<void>((resolve) => {
  entered = resolve;
});
let service: McpConnectorServiceShell;
// The credential variant omits the injected SDK stub: command assembly creates
// its production client and SQL resolver, whose transaction ignores caller abort.
const credentialSQL: McpCredentialSQL = async <T = unknown>(
  strings: TemplateStringsArray,
): Promise<T> => {
  if (strings.join("").includes("WITH session_vaults")) {
    emit("credential.held");
    entered();
    await held;
    emit("credential.joined");
  }
  return [] as T;
};
credentialSQL.begin = async (operation) => await operation(credentialSQL);
const sql = credentialMode
  ? credentialSQL
  : commandFixture("none").options.sql!;
Object.assign(sql, {
  close: async () => {
    emit("database.close");
  },
});
await runProcessEntry((processBoundary) =>
  runMcpConnectorCommand({
    processBoundary,
    logger: { info: observe, error: observe },
    sql,
    schemaVerifier: async () => undefined,
    reviewerMaterialValidator: async () => undefined,
    manifestChangeNotifier: {
      notify: async () => ({ ok: true, duplicate: false }),
    },
    ...(credentialMode
      ? {}
      : {
          client: {
            listTools: async () => {
              emit("worker.held");
              entered();
              await held;
              emit("worker.joined");
              return [];
            },
            callTool: async () => {
              throw new Error("unexpected dispatch");
            },
          },
        }),
    serverFactory: (owned) => {
      service = owned;
      return createMcpConnectorGrpcServer(owned);
    },
    registerSignalHandlers: (shutdown) =>
      registerProcessSignalHandlers(() => {
        emit("shutdown.begin");
        if (mode === "cooperative" || mode === "credential-cooperative")
          setTimeout(release, 100);
        return shutdown();
      }),
    waitForever: async () => {
      const metadata = new Metadata();
      metadata.set("authorization", "bearer synthetic");
      const worker = service.listMcpTools(
        {
          workspaceId: "default",
          sessionId: "fixture-session",
          mcpServerName: "github",
        },
        metadata,
      );
      void worker.catch(() => emit("worker.returned"));
      await Promise.race([
        admission,
        worker.then(() => {
          throw new Error("worker did not enter held boundary");
        }),
      ]);
      if (trigger === "finally") {
        emit("shutdown.begin");
        return undefined as never;
      }
      process.kill(process.pid, trigger as NodeJS.Signals);
      return await new Promise<never>(() => undefined);
    },
  }),
);
