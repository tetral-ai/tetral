import { describe, expect, test } from "bun:test";
import { loadProviderGatewayConfigFromEnv } from "../../src/config.js";

describe("Gateway config", () => {
  test("projects only Gateway-owned environment", () => {
    const config = loadProviderGatewayConfigFromEnv({
      ...validEnv(),
      DATABASE_URL: "postgres://must-not-be-read",
      OPENAI_API_KEY: "sk-must-not-be-read",
    });

    expect(config.ok).toBe(true);
    if (config.ok) {
      expect(JSON.stringify(config.config)).not.toContain("DATABASE_URL");
      expect(JSON.stringify(config.config)).not.toContain("OPENAI_API_KEY");
      expect(config.config.allowedRuntimePod).toEqual({
        namespace: "tetral-agent-runtime",
        serviceAccount: "agent-runtime",
      });
      expect(config.config.databaseUrl).toBe("postgres://gateway-readonly.example/tetral");
      expect(config.config.vaultKeyHex).toBe("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
      expect(config.config.bridgeApiGrpcAddress).toBe("bridge.tetral-system.svc.cluster.local:9090");
      expect(config.config.bridgeTokenPath).toBe("/var/run/secrets/tetral-internal-grpc/bridge/token");
      expect(config.config.maxConcurrentTurns).toBe(8);
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
    const configured = loadProviderGatewayConfigFromEnv({
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
        expect(loadProviderGatewayConfigFromEnv({
          ...validEnv(),
          [key]: value,
        }).ok).toBe(false);
      }
    }
  });

  test("accepts bounded concurrent-turn admission cap and rejects invalid values", () => {
    const configured = loadProviderGatewayConfigFromEnv({
      ...validEnv(),
      TETRAL_GATEWAY_MAX_CONCURRENT_TURNS: "3",
    });
    expect(configured.ok).toBe(true);
    if (configured.ok) {
      expect(configured.config.maxConcurrentTurns).toBe(3);
    }

    for (const value of ["0", "-1", "1.5", "many"]) {
      expect(loadProviderGatewayConfigFromEnv({
        ...validEnv(),
        TETRAL_GATEWAY_MAX_CONCURRENT_TURNS: value,
      }).ok).toBe(false);
    }
  });

  test("rejects wildcard or multi-service caller authorization", () => {
    for (const allowed of ["*", "tetral-agent-runtime/agent-runtime,tetral-system/api"]) {
      const config = loadProviderGatewayConfigFromEnv({
        ...validEnv(),
        TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: allowed,
      });
      expect(config.ok).toBe(false);
    }
  });

  test("rejects malformed credential store root config before startup", () => {
    expect(loadProviderGatewayConfigFromEnv({
      ...validEnv(),
      ENGINE_VAULT_KEY: "short",
    }).ok).toBe(false);
    expect(loadProviderGatewayConfigFromEnv({
      ...validEnv(),
      ENGINE_VAULT_KEY: "z".repeat(64),
    }).ok).toBe(false);
    expect(loadProviderGatewayConfigFromEnv({
      ...validEnv(),
      TETRAL_DATABASE_URL: "",
    }).ok).toBe(false);
  });
});

function validEnv(): Record<string, string> {
  return {
    TETRAL_PROVIDER_GATEWAY_GRPC_ADDR: "127.0.0.1:0",
    TETRAL_PROVIDER_GATEWAY_HTTP_ADDR: "127.0.0.1:0",
    TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
    TETRAL_SERVICE_VERSION: "test",
    TETRAL_INTERNAL_GRPC_AUDIENCE: "tetral-internal-grpc",
    TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "tetral-agent-runtime/agent-runtime",
    TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY: "gateway-runtime-binding-token-test-key-32",
    TETRAL_DATABASE_URL: "postgres://gateway-readonly.example/tetral",
    ENGINE_VAULT_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    TETRAL_BRIDGE_API_GRPC_ADDR: "bridge.tetral-system.svc.cluster.local:9090",
    TETRAL_PROVIDER_GATEWAY_BRIDGE_TOKEN_PATH: "/var/run/secrets/tetral-internal-grpc/bridge/token",
    KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc",
    KUBERNETES_API_CA_CERT_PATH: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: "/var/run/secrets/kubernetes.io/serviceaccount/token",
  };
}
