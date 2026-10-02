// Test-owned process fixture uses only existing command composition seams.
import { runRuntimePodCommand } from "../../src/command.js";
import type { RuntimePodCommandOptions, RuntimePodCommandDependencies } from "../../src/command.js";
import { registerProcessSignalHandlers, runProcessEntry } from "@tetral/ts-observability";
import { RuntimePodMetricsRegistry } from "../../src/metrics.js";

export const failureSentinel = "PRIVATE_COMMAND_FAILURE_NON_TOKEN_SENTINEL";
export function commandEnv(): Record<string, string> {
	return {
		TETRAL_RUNTIME_POD_NAMESPACE: "engine",
		TETRAL_RUNTIME_POD_NAME: "runtime-pod-a",
		TETRAL_RUNTIME_POD_UID: "uid-a",
		TETRAL_RUNTIME_POD_IP: "10.0.0.1",
		TETRAL_RUNTIME_POD_GRPC_PORT: "19090",
		TETRAL_RUNTIME_POD_HTTP_ADDR: "127.0.0.1:0",
		TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
		TETRAL_SERVICE_VERSION: "test",
		TETRAL_RUNTIME_POD_GRPC_AUDIENCE: "tetral-internal-grpc",
		TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "engine/job-runner",
		KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc",
		KUBERNETES_API_CA_CERT_PATH:
			"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH:
			"/var/run/secrets/kubernetes.io/serviceaccount/token",
		TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH:
			"/var/run/secrets/tetral-internal-grpc/runtime-pod/token",
		TETRAL_BRIDGE_API_GRPC_ADDR: "bridge.engine.svc:9090",
		TETRAL_GATEWAY_GRPC_ADDR: "gateway.engine.svc:9090",
		TETRAL_MCP_CONNECTOR_GRPC_ADDR: "gateway.engine.svc:9091",
		TETRAL_WEB_CONNECTOR_GRPC_ADDR: "gateway.engine.svc:9092",
		TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL: "anthropic/claude-opus-4-8",
		TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES: "32768",
	};
}
export function commandFixture(mode: string, event: (name: string) => void = () => undefined) {
	const failure = new Error(failureSentinel);
	const laterFailure = new Error("PRIVATE_LATER_CLOSE_NON_TOKEN_SENTINEL");
	const close = async (phase: string) => {
		event(phase + ".close");
		if (mode === phase || mode.endsWith("both_cleanup") || (phase === "app" && mode.endsWith("_failure"))) throw phase === "app" ? failure : laterFailure;
	};
	const dependencies: RuntimePodCommandDependencies = {
		tokenReviewClient: { createTokenReview: async () => ({ authenticated: true, audiences: [], username: "" }) },
		coreHosts: { close: async () => close("runtime_core") } as RuntimePodCommandDependencies["coreHosts"], metrics: new RuntimePodMetricsRegistry(),
		app: {
			service: undefined as never, lifecycle: undefined as never,
			start: async () => { event("listener.start"); if (mode === "listener") throw failure; return { grpcPort: 1, httpUrl: new URL("http://127.0.0.1:1") }; },
			shutdown: async () => close("app"),
		},
	};
	const options: RuntimePodCommandOptions = {
		dependencyBuilder: async () => { event("dependency.build"); if (mode === "dependency") throw failure; return dependencies; },
		waitForever: async () => {
			event("wait");
			if (mode.startsWith("wait")) throw failure;
			if (mode.endsWith("_immediate")) {
				process.emit(mode.startsWith("SIGINT") ? "SIGINT" : "SIGTERM");
				event("signal.returned");
				return await new Promise<never>(() => undefined);
			}
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
	if (process.argv[2] === "signal_sync_throw") {
		registerProcessSignalHandlers(() => {
			process.stdout.write("shutdown.entered\n");
			throw new Error(failureSentinel);
		});
		process.emit("SIGTERM");
	}
	Object.assign(process.env, commandEnv());
	const fixture = commandFixture(process.argv[2] ?? "none", (event) => process.stdout.write(event + "\n"));
	await runProcessEntry(() => runRuntimePodCommand(fixture.options));
}
