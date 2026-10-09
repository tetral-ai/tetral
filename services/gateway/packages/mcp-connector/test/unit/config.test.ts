import { describe, expect, test } from "bun:test";
import { McpBridgePolicyDefaults, McpClientPolicyDefaults, loadMcpConnectorConfigFromEnv } from "../../src/config.js";

describe("MCP connector config", () => {
  test("preserves configured business drain and separate join within Pod grace",()=>{
    const result=loadMcpConnectorConfigFromEnv({...validEnv(),TETRAL_DRAIN_TIMEOUT_MS:"200",TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS:"1000"});
    expect(result.ok).toBe(true);if(result.ok){expect(result.config.drainTimeoutMs).toBe(200);expect(result.config.cancelJoinTimeoutMs).toBe(1000);}
    for(const join of ["0","-1","55000","bad"])expect(loadMcpConnectorConfigFromEnv({...validEnv(),TETRAL_DRAIN_TIMEOUT_MS:"200",TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS:join}).ok).toBe(false);
  });

  test("reads the application drain only from the shared drain key",()=>{
    const result=loadMcpConnectorConfigFromEnv({...validEnv(),TETRAL_SERVICE_DRAIN_TIMEOUT_MS:"200"});
    expect(result.ok).toBe(true);if(result.ok)expect(result.config.drainTimeoutMs).toBe(30000);
  });

  test("Bridge policy defaults match the owning Runtime descriptor and preserve discovery", async () => {
    const descriptor = await Bun.file(new URL("../../../../../agent-runtime/packages/runtime-pod/src/bridge-method-policy.json", import.meta.url)).json() as Array<{method:string;timeoutMs:number}>;
    for (const [method, timeout] of Object.entries(McpBridgePolicyDefaults)) {
      expect(descriptor.find(entry => entry.method === method[0]!.toUpperCase()+method.slice(1))?.timeoutMs).toBe(timeout);
    }
    expect(McpClientPolicyDefaults.discoveryTimeoutMs).toBe(120000);
    const configured = loadMcpConnectorConfigFromEnv({...validEnv(), TETRAL_BRIDGE_CLAIM_MCP_TOOL_RESULT_TIMEOUT_MS:"7000", TETRAL_MCP_CONNECT_TIMEOUT_MS:"8000", TETRAL_MCP_DISCOVERY_TIMEOUT_MS:"45000"});
    expect(configured.ok).toBe(true);
    if (configured.ok) {
      expect(configured.config.bridgePolicies.claimMcpToolResult).toBe(7000);
      expect(configured.config.clientPolicies.connectTimeoutMs).toBe(8000);
      expect(configured.config.clientPolicies.discoveryTimeoutMs).toBe(45000);
    }
  });

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
      expect(config.config.allowedDiscoveryCallers).toEqual([
 {namespace:"tetral-system",serviceAccount:"bridge"},
 {namespace:"tetral-system",serviceAccount:"job-runner"},
 ]);
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
      for (const value of ["0","-1","1.5"," 1","01","9007199254740992",""]) {
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

    }
  });

  test("discovery preserves explicit namespace overrides and rejects malformed or duplicate lists", () => {
    const configured = loadMcpConnectorConfigFromEnv({...validEnv(),TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS:"custom-system/bridge,custom-system/job-runner"});
    expect(configured.ok).toBe(true);
    if(configured.ok) expect(configured.config.allowedDiscoveryCallers).toEqual([{namespace:"custom-system",serviceAccount:"bridge"},{namespace:"custom-system",serviceAccount:"job-runner"}]);
    for(const value of ["","*","tetral-system/*","tetral-system/bridge,","tetral-system/bridge,tetral-system/bridge","wrong namespace/bridge","tetral-system/ bridge",",tetral-system/bridge"]){
      expect(loadMcpConnectorConfigFromEnv({...validEnv(),TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS:value}).ok).toBe(false);
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

test("execution total clips larger phase ceilings and rejects invalid phase/lease policies",()=>{
 const shorter=loadMcpConnectorConfigFromEnv({...validEnv(),TETRAL_MCP_EXECUTION_TIMEOUT_MS:"60000"});expect(shorter.ok).toBe(true);if(shorter.ok){expect(shorter.config.clientPolicies.executionTimeoutMs).toBe(60000);expect(shorter.config.clientPolicies.callTimeoutMs).toBe(120000);}
 for(const key of ["TETRAL_MCP_EXECUTION_TIMEOUT_MS","TETRAL_MCP_CALL_TIMEOUT_MS","TETRAL_MCP_CREDENTIAL_TIMEOUT_MS","TETRAL_MCP_CONNECT_TIMEOUT_MS","TETRAL_MCP_DISCOVERY_TIMEOUT_MS","TETRAL_BRIDGE_CLAIM_MCP_TOOL_RESULT_TIMEOUT_MS","TETRAL_BRIDGE_COMMIT_MCP_TOOL_RESULT_TIMEOUT_MS"])for(const invalid of ["0","-1","1.5","2147483648","bad"])expect(loadMcpConnectorConfigFromEnv({...validEnv(),[key]:invalid}).ok).toBe(false);
 expect(loadMcpConnectorConfigFromEnv({...validEnv(),TETRAL_MCP_EXECUTION_TIMEOUT_MS:"170001"}).ok).toBe(false);expect(loadMcpConnectorConfigFromEnv({...validEnv(),TETRAL_BRIDGE_COMMIT_MCP_TOOL_RESULT_TIMEOUT_MS:"10001"}).ok).toBe(false);
});

function validEnv(): Record<string, string> {
  return {
    TETRAL_MCP_CONNECTOR_GRPC_ADDR: "127.0.0.1:0",
    TETRAL_MCP_CONNECTOR_HTTP_ADDR: "127.0.0.1:0",
    TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
    TETRAL_SERVICE_VERSION: "test",
    TETRAL_INTERNAL_GRPC_AUDIENCE: "tetral-internal-grpc",
    TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "tetral-agent-runtime/agent-runtime",
    TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS: "tetral-system/bridge,tetral-system/job-runner",
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
