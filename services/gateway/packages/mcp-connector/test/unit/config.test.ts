import { describe, expect, test } from "bun:test";
import { loadMcpConnectorConfigFromEnv } from "../../src/config.js";

describe("MCP connector config", () => {
  test("projects only connector-owned environment", () => {
    const config = loadMcpConnectorConfigFromEnv({
      ...validEnv(),
      DATABASE_URL: "postgres://must-not-be-read",
      OPENAI_API_KEY: "sk-must-not-be-read",
    });

    expect(config.ok).toBe(true);
    if (config.ok) {
      expect(JSON.stringify(config.config)).not.toContain("DATABASE_URL");
      expect(JSON.stringify(config.config)).not.toContain("OPENAI_API_KEY");
      expect(config.config.grpcBindAddress).toBe("127.0.0.1:0");
      expect(config.config.httpBindAddress).toBe("127.0.0.1:0");
      expect(config.config.allowedRuntimePod).toEqual({
        namespace: "tetral-agent-runtime",
        serviceAccount: "agent-runtime",
      });
      expect(config.config.allowedBridge).toEqual({
        namespace: "tetral-system",
        serviceAccount: "bridge",
      });
      expect(config.config.bridgeApiGrpcAddress).toBe("bridge.tetral-system.svc.cluster.local:9090");
      expect(config.config.bridgeTokenPath).toBe("/var/run/secrets/tetral-internal-grpc/bridge/token");
      expect(config.config.runtimeBindingTokenHMACKey).toBe("gateway-runtime-binding-token-test-key-32");
      expect(config.config.databaseUrl).toBe("postgres://gateway-db");
      expect(config.config.vaultKeyHex).toMatch(/^[0-9a-f]{64}$/);
      expect(config.config.databasePool).toEqual({
        max: 10,
        idleTimeout: 30,
        maxLifetime: 1_800,
        connectionTimeout: 30,
        statementTimeoutMs: 30_000,
      });
    }
  });

  test("accepts positive SQL pool bounds and rejects zero or negative values", () => {
    const configured = loadMcpConnectorConfigFromEnv({
      ...validEnv(),
      TETRAL_DATABASE_POOL_MAX: "7",
      TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS: "11",
      TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS: "601",
      TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS: "13",
      TETRAL_DATABASE_STATEMENT_TIMEOUT_MS: "17000",
    });
    expect(configured.ok).toBe(true);
    if (configured.ok) {
      expect(configured.config.databasePool).toEqual({
        max: 7,
        idleTimeout: 11,
        maxLifetime: 601,
        connectionTimeout: 13,
        statementTimeoutMs: 17_000,
      });
    }

    for (const key of [
      "TETRAL_DATABASE_POOL_MAX",
      "TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS",
      "TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS",
      "TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS",
      "TETRAL_DATABASE_STATEMENT_TIMEOUT_MS",
    ] as const) {
      for (const value of ["0", "-1"]) {
        expect(loadMcpConnectorConfigFromEnv({
          ...validEnv(),
          [key]: value,
        }).ok).toBe(false);
      }
    }
  });

  test("rejects wildcard or malformed method caller authorization", () => {
    for (const allowed of ["*", "tetral-agent-runtime/agent-runtime,tetral-system/api"]) {
      expect(loadMcpConnectorConfigFromEnv({
        ...validEnv(),
        TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: allowed,
      }).ok).toBe(false);
      expect(loadMcpConnectorConfigFromEnv({
        ...validEnv(),
        TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS: allowed,
      }).ok).toBe(false);
    }
  });

  test("requires database URL and vault key for production credential resolution", () => {
    expect(loadMcpConnectorConfigFromEnv({
      ...validEnv(),
      TETRAL_DATABASE_URL: "",
    }).ok).toBe(false);
    expect(loadMcpConnectorConfigFromEnv({
      ...validEnv(),
      ENGINE_VAULT_KEY: "not-a-key",
    }).ok).toBe(false);
  });
});

function validEnv(): Record<string, string> {
  return {
    TETRAL_MCP_CONNECTOR_GRPC_ADDR: "127.0.0.1:0",
    TETRAL_MCP_CONNECTOR_HTTP_ADDR: "127.0.0.1:0",
    TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
    TETRAL_SERVICE_VERSION: "test",
    TETRAL_INTERNAL_GRPC_AUDIENCE: "tetral-internal-grpc",
    TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "tetral-agent-runtime/agent-runtime",
    TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS: "tetral-system/bridge",
    TETRAL_BRIDGE_API_GRPC_ADDR: "bridge.tetral-system.svc.cluster.local:9090",
    TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH: "/var/run/secrets/tetral-internal-grpc/bridge/token",
    TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY: "gateway-runtime-binding-token-test-key-32",
    TETRAL_DATABASE_URL: "postgres://gateway-db",
    ENGINE_VAULT_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc",
    KUBERNETES_API_CA_CERT_PATH: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: "/var/run/secrets/kubernetes.io/serviceaccount/token",
  };
}
