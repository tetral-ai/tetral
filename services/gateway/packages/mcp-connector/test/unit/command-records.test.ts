import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SchemaSQL } from "../../../schema/src/verify.js";
import { runMcpConnectorCommand } from "../../src/command.js";
import { createJsonLogger } from "../../src/logger.js";

const fixtureCA = new URL("../../../provider-gateway/test/fixtures/tls/cert.pem", import.meta.url);
const savedEnv = { ...process.env };

afterEach(() => {
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, savedEnv);
});

// The failure records the command writes itself, read through the product's writer.
describe("mcp-connector command records", () => {
  test("a rotated trust bundle whose candidate pool fails verification is recorded with the verifier's error", async () => {
    const directory = await mkdtemp(join(tmpdir(), "mcp-reload-"));
    try {
      const caPath = join(directory, "ca.pem");
      const ca = await readFile(fixtureCA, "utf8");
      await writeFile(caPath, ca);
      Object.assign(process.env, env(), { TETRAL_DATABASE_TLS_CA_PATH: caPath, TETRAL_DATABASE_TLS_SERVER_NAME: "db.internal" });
      const records: Record<string, unknown>[] = [];
      const sql = ((<T>(_strings: TemplateStringsArray): PromiseLike<T> => Promise.resolve([] as T)) as SchemaSQL & { close: () => Promise<void> });
      sql.close = async () => undefined;
      let verifications = 0;
      const reloadFailed = () => records.filter((record) => record.event === "transport.credential_reload_failed");
      await runMcpConnectorCommand({
        sql,
        schemaVerifier: async () => {
          verifications++;
          if (verifications > 1) throw new Error("connect ECONNREFUSED 10.0.0.7:5432");
        },
        reviewerMaterialValidator: async () => undefined,
        client: { connectionCount: () => 0, listTools: async () => [], callTool: async () => ({ content: [] }) },
        manifestChangeNotifier: { notify: async () => ({ ok: true, duplicate: false }) },
        logger: createJsonLogger({ write: (line) => { records.push(JSON.parse(line) as Record<string, unknown>); } }),
        serverFactory: () => ({ server: undefined as never, bind: async () => 9091, shutdown: async () => undefined }),
        httpServerFactory: () => ({ url: new URL("http://127.0.0.1:8081"), stop: async () => undefined }),
        registerSignalHandlers: () => undefined,
        // While the command runs: the same anchor twice, a new bundle that removes no trust.
        waitForever: async () => {
          await writeFile(caPath, `${ca.trim()}\n${ca.trim()}\n`);
          const deadline = Date.now() + 5_000;
          while (reloadFailed().length === 0 && Date.now() < deadline) await Bun.sleep(25);
          return undefined as never;
        },
      });
      expect(reloadFailed().map((record) => [record["transport.outcome"], record["error.code"], record["error.message"]])).toEqual([
        ["candidate_verification_failed", "operation_failed", "database pool generation verification failed: connect ECONNREFUSED 10.0.0.7:5432"],
      ]);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});

function env(): Record<string, string> {
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
