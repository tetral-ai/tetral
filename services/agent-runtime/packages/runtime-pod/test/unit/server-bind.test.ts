import { describe, expect, test } from "bun:test";
import { Writable } from "node:stream";
import { createDiagnosticStreamSink, createTetralJsonLogger } from "@tetral/ts-observability";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import { createRuntimeHttpServer } from "../../src/http-server.js";
import { RuntimePodLifecycle } from "../../src/lifecycle.js";
import { RuntimePodMetricsRegistry } from "../../src/metrics.js";
import { RuntimeControlService } from "../../src/runtime-service.js";

describe("Runtime Pod server bind addresses", () => {
	test("HTTP metrics reports actual diagnostic emissions and rejected sink writes", async () => {
		let acceptSink = true;
		const logger = createTetralJsonLogger({ serviceName: "agent-runtime", write: () => acceptSink });
		logger.info({ event: "http_diagnostic_probe" });
		acceptSink = false;
		logger.error({ event: "http_diagnostic_drop" });
		const httpServer = createRuntimeHttpServer("127.0.0.1:0", fakeLifecycle(), undefined, logger);
		try {
			const response = await fetch(new URL("/metrics", httpServer.url));
			expect(response.status).toBe(200);
			const body = await response.text();
			expect(body).toContain("tetral_diagnostic_emitted_total 1\n");
			expect(body).toContain("tetral_diagnostic_dropped_total 1\n");
			expect(body).toContain("tetral_diagnostic_sink_failures_total 0\n");
		} finally {
			httpServer.stop();
			logger.close();
		}
	});
	test("HTTP metrics includes an asynchronous production stream failure", async () => {
		const stream = new Writable({ write(_chunk, _encoding, done) { done(); } });
		const sink = createDiagnosticStreamSink(stream);
		const logger = createTetralJsonLogger({ serviceName: "agent-runtime", write: sink.write, sinkFailures: () => sink.stats().failures });
		logger.info({ event: "http_diagnostic_probe" });
		stream.emit("error", new Error("asynchronous stderr failure"));
		logger.info({ event: "http_diagnostic_after_error" });
		const httpServer = createRuntimeHttpServer("127.0.0.1:0", fakeLifecycle(), undefined, logger);
		try {
			const response = await fetch(new URL("/metrics", httpServer.url));
			const body = await response.text();
			expect(body).toContain("tetral_diagnostic_emitted_total 1\n");
			expect(body).toContain("tetral_diagnostic_dropped_total 1\n");
			expect(body).toContain("tetral_diagnostic_sink_failures_total 1\n");
		} finally { httpServer.stop(); logger.close(); sink.close(); stream.destroy(); }
	});
	test("HTTP server accepts explicit production host addresses, serves metrics, and rejects hostless addresses", async () => {
		const metricsRegistry = new RuntimePodMetricsRegistry();
		metricsRegistry.recordHotState({
			activeSessions: 2,
			activeThreads: 3,
			activeFibers: 1,
			pendingApprovals: 1,
		});
		metricsRegistry.addActiveToolFibers(2);
		metricsRegistry.observeProviderStreamDuration(
			"agent_provider_request",
			42,
			"success",
		);
		metricsRegistry.observeEventWriteLatency("append", 7, "success");
		metricsRegistry.observeContextLoadLatency(
			"commit_accepted_input",
			11,
			"success",
		);
		metricsRegistry.recordCleanupCommandOutcome("completed");
		metricsRegistry.recordCloseoutEvent({
			event: "runtime_closeout_stalled",
			activeCloseouts: 2,
		});
		metricsRegistry.recordCloseoutEvent({
			event: "runtime_closeout_recovered",
			activeCloseouts: 0,
		});
		const httpServer = createRuntimeHttpServer(
			"0.0.0.0:0",
			fakeLifecycle(),
			metricsRegistry,
		);
		try {
			expect(httpServer.url.port).not.toBe("");
			const metrics = await fetch(new URL("/metrics", httpServer.url));
			expect(metrics.status).toBe(200);
			expect(metrics.headers.get("content-type")).toContain("text/plain");
			const body = await metrics.text();
			expect(body).toContain("runtimepod_ready 0");
			expect(body).toContain("runtimepod_commands_in_flight");
			expect(body).toContain("runtimepod_active_sessions 2");
			expect(body).toContain("runtimepod_active_threads 3");
			expect(body).toContain("runtimepod_active_fibers 1");
			expect(body).toContain("runtimepod_active_tool_fibers 2");
			expect(body).toContain("runtimepod_pending_approvals 1");
			expect(body).toContain(
				'runtimepod_provider_stream_duration_ms_count{kind="agent_provider_request",outcome="success"} 1',
			);
			expect(body).toContain(
				'runtimepod_event_write_latency_ms_count{operation="append",outcome="success"} 1',
			);
			expect(body).toContain(
				'runtimepod_context_load_latency_ms_count{operation="commit_accepted_input",outcome="success"} 1',
			);
			expect(body).toContain(
				'runtimepod_cleanup_command_outcomes_total{outcome="completed"} 1',
			);
			expect(body).toContain(
				'runtimepod_closeout_events_total{event="runtime_closeout_stalled"} 2',
			);
			expect(body).toContain(
				'runtimepod_closeout_events_total{event="runtime_closeout_recovered"} 1',
			);
		} finally {
			httpServer.stop();
		}

		expect(() => createRuntimeHttpServer(":0", fakeLifecycle())).toThrow(
			"invalid http bind address",
		);
	});

	test("gRPC server binds explicit host addresses for ephemeral ports", async () => {
		const grpcServer = createRuntimeGrpcServer(fakeRuntimeControlService());
		try {
			const port = await grpcServer.bind("127.0.0.1:0");
			expect(port).toBeGreaterThan(0);
		} finally {
			await grpcServer.shutdown();
		}
	});
});

function fakeLifecycle() {
	return new RuntimePodLifecycle({
		config: {
			ok: true,
			config: {
				ownPod: {
					namespace: "engine",
					name: "runtime-pod-a",
					uid: "uid-a",
					ip: "10.0.0.1",
				},
				deploymentEnvironment: "test",
 diagnostics: {level:"info",maxRecordBytes:16384,summaryIntervalMs:30000,burst:1},
				serviceVersion: "test",
				jobRunner: { namespace: "engine", serviceAccount: "job-runner" },
				grpcBindAddress: "127.0.0.1:0",
				httpBindAddress: "127.0.0.1:0",
				kubernetesApiServerUrl: "https://kubernetes.default.svc",
				kubernetesApiCaCertPath:
					"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
				tokenReviewReviewerTokenPath:
					"/var/run/secrets/kubernetes.io/serviceaccount/token",
				outboundInternalGrpcTokenPath:
					"/var/run/secrets/tetral-internal-grpc/runtime-pod/token",
				bridgeApiGrpcAddress: "bridge.engine.svc:9090",
				gatewayGrpcAddress: "gateway.engine.svc:9090",
				mcpConnectorGrpcAddress: "gateway.engine.svc:9091",
				webConnectorGrpcAddress: "gateway.engine.svc:9092",
				providerStreamTimeoutMs: 1_800_000,
				platformModels: {
					approvalReviewer: {
						providerId: "anthropic",
						modelId: "claude-opus-4-8",
					},
				},
				skillGuidance: {
					descriptionBudgetBytes: 32_768,
				},
			},
		},
		logger: { info: () => undefined, error: () => undefined },
		bootstrap: {
			runtime: async () => undefined,
			core: async () => undefined,
			grpc: async () => undefined,
			authClient: async () => undefined,
		},
	});
}

function fakeRuntimeControlService(): RuntimeControlService {
	return new RuntimeControlService({
		ownPod: {
			namespace: "engine",
			name: "runtime-pod-a",
			uid: "uid-a",
			ip: "10.0.0.1",
		},
		allowedJobRunner: { namespace: "engine", name: "job-runner" },
		authenticator: {
			authenticate: async () => ({
				ok: true,
				serviceAccount: { namespace: "engine", name: "job-runner" },
			}),
		},
		runHost: {
			handleAcceptInput: async (command) => ({
				ok: true,
				sessionId: command.sessionId,
				created: false,
				started: false,
			}),
			handleAgentMail: async (command) => ({
				ok: true,
				sessionId: command.sessionId,
				applied: true,
			}),
			handleInterruptControl: async (sessionId) => ({
				ok: true,
				sessionId,
				created: false,
				interrupted: true,
				idleInterrupt: false,
			}),
			handleToolConfirmation: async (sessionId) => ({
				ok: true,
				sessionId,
				created: false,
				applied: true,
			}),
			handleTaskNotification: async (sessionId) => ({
				ok: true,
				sessionId,
				created: false,
				applied: true,
			}),
			handleRuntimeConfigPatch: async (sessionId) => ({
				ok: true,
				sessionId,
				created: false,
				applied: true,
			}),
		},
		cleanupController: {
			startCleanup: async (scope) => ({
				ok: true,
				sessionId: scope.sessionId,
				cleaned: true,
			}),
		},
		logger: { info: () => undefined, error: () => undefined },
		ready: () => true,
	});
}
