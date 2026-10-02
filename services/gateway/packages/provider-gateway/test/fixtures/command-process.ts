// Test-owned process fixture uses only existing command composition seams.
import { runProviderGatewayCommand } from "../../src/command.js";
import type { ProviderGatewayCommandOptions, ProviderGatewayCommandDependencies } from "../../src/command.js";
import { runProcessEntry } from "@tetral/ts-observability";

export const failureSentinel = "PRIVATE_COMMAND_FAILURE_NON_TOKEN_SENTINEL";
export function commandEnv(): Record<string, string> {
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
export function commandFixture(mode: string, event: (name: string) => void = () => undefined) {
  const failure = new Error(failureSentinel);
  const laterFailure = new Error("PRIVATE_LATER_CLOSE_NON_TOKEN_SENTINEL");
  const close = async (phase: string) => {
    event(phase + ".close");
    if (mode === phase || mode.endsWith("both_cleanup") || (phase === "app" && mode.endsWith("_failure"))) throw phase === "app" ? failure : laterFailure;
  };
  const dependencies: ProviderGatewayCommandDependencies = {
    tokenReviewClient: { createTokenReview: async () => ({ authenticated: true, audiences: [], username: "", podUid: "" }) },
    credentialResolver: undefined as never, close: async () => close("database"),
    app: {
      service: undefined as never, health: () => ({ ok: true }), ready: () => ({ ready: true }),
      start: async () => { event("listener.start"); if (mode === "listener") throw failure; return { grpcPort: 1, httpUrl: new URL("http://127.0.0.1:1") }; },
      shutdown: async () => close("app"),
    },
  };
  const options: ProviderGatewayCommandOptions = {
    dependencyBuilder: async () => { event("dependency.build"); if (mode === "dependency") throw failure; return dependencies; },
    waitForever: async () => {
      event("wait");
      if (mode.startsWith("wait")) throw failure;
      if (mode.startsWith("SIG")) {
        setTimeout(() => process.kill(process.pid, mode.startsWith("SIGINT") ? "SIGINT" : "SIGTERM"), 0);
        return await new Promise<never>(() => undefined);
      }
      return undefined as never;
    },
  };
  return { options, failure, laterFailure };
}
if (import.meta.main) {
  Object.assign(process.env, commandEnv());
  const fixture = commandFixture(process.argv[2] ?? "none", (event) => process.stdout.write(event + "\n"));
  await runProcessEntry((processBoundary) => runProviderGatewayCommand({ ...fixture.options, processBoundary }));
}
