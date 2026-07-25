import { afterEach, describe, expect, test } from "bun:test";
import { runMcpConnectorCommand } from "../../src/command.js";
import { SchemaVerificationError } from "../../../schema/src/verify.js";
import type { SchemaSQL } from "../../../schema/src/verify.js";

const savedEnv = { ...process.env };

afterEach(() => {
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, savedEnv);
});

describe("mcp-connector schema startup", () => {
  test("schema-behind fails before client creation or listener bind", async () => {
    Object.assign(process.env, validEnv());
    const events: string[] = [];
    const sql = ((<T>(_strings: TemplateStringsArray): PromiseLike<T> => Promise.resolve([] as T)) as SchemaSQL & { close: () => Promise<void> });
    sql.close = async () => { events.push("close"); };
    const behind = new SchemaVerificationError("schema_behind");

    await expect(runMcpConnectorCommand({
      sql,
      schemaVerifier: async () => {
        events.push("schema_verify");
        throw behind;
      },
      client: {
        connectionCount: () => 0,
        listTools: async () => { events.push("client_list"); return []; },
        callTool: async () => { events.push("client_call"); throw new Error("must not call"); },
      },
    })).rejects.toBe(behind);
    expect(events).toEqual(["schema_verify", "close"]);
  });

  test("constructs SQL with the validated pool and statement bounds", async () => {
    Object.assign(process.env, validEnv());
    const behind = new SchemaVerificationError("schema_behind");
    let sqlOptions: Bun.SQL.PostgresOrMySQLOptions | undefined;
    const sql = ((<T>(_strings: TemplateStringsArray): PromiseLike<T> => Promise.resolve([] as T)) as SchemaSQL & { close: () => Promise<void> });
    sql.close = async () => undefined;

    await expect(runMcpConnectorCommand({
      sqlFactory: (options) => {
        sqlOptions = options;
        return sql;
      },
      schemaVerifier: async () => { throw behind; },
    })).rejects.toBe(behind);

    expect(sqlOptions).toEqual({
      url: "postgres://gateway",
      max: 10,
      idleTimeout: 30,
      maxLifetime: 1_800,
      connectionTimeout: 30,
      connection: { statement_timeout: 30_000 },
    });
  });
});

function validEnv(): Record<string, string> {
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
