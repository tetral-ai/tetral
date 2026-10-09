import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { Socket } from "node:net";
import { createRuntimePodApp } from "../../src/app.js";
import { Writable } from "node:stream";
import { createDiagnosticStreamSink } from "@tetral/ts-observability";
import { runRuntimePodCommand } from "../../src/command.js";
import { createJsonLogger } from "../../src/logger.js";
import type { RuntimeProcessPort } from "../../src/runtime-process.js";
import { commandEnv, commandFixture, failureSentinel } from "../fixtures/command-process.js";

describe("RuntimePod command failure ownership", () => {
	test("listener, wait and cleanup failures preserve errors and attempt each later close once", async () => {
		for (const mode of ["listener", "wait", "wait_both_cleanup", "app", "runtime_core", "both_cleanup"]) {
			const events: string[] = [], lines: string[] = [];
			const fixture = commandFixture(mode, (event) => events.push(event));
			let releases = 0;
			const base = createJsonLogger({ write: (line) => { lines.push(line); } });
			const logger = { ...base, flush: () => { releases++; base.flush(); } };
			await withEnv(async () => {
				try { await runRuntimePodCommand({ ...fixture.options, logger, registerSignalHandlers: () => undefined }); throw new Error("expected failure"); }
				catch (error) {
					if (mode !== "runtime_core") expect(error).toBe(fixture.failure);
					else expect(error).toBe(fixture.laterFailure);
				}
			});
			expect(events.filter((event) => event === "app.close")).toHaveLength(1);
			expect(events.filter((event) => event === "runtime_core.close")).toHaveLength(1);
			expect(events.indexOf("app.close")).toBeLessThan(events.indexOf("runtime_core.close"));
			expect(releases).toBe(1);
			const records = lines.map((line) => JSON.parse(line));
			expect(records).toContainEqual(expect.objectContaining({
				event: mode === "listener" || mode.startsWith("wait") ? "workload.command_failed" : "workload.cleanup_failed",
				phase: mode === "listener" || mode.startsWith("wait") ? mode.startsWith("wait") ? "wait" : mode : mode === "runtime_core" ? "runtime_core" : "app",
				"error.class": mode === "listener" || mode.startsWith("wait") ? "process_error" : "cleanup_error",
			}));
			expect(lines.join("")).not.toContain(failureSentinel);
			expect(lines.join("")).not.toContain("PRIVATE_LATER_CLOSE_NON_TOKEN_SENTINEL");
		}
	});

	test("repeated shutdown reuses its failed result and still releases later resources", async () => {
		const events: string[] = [];
		let reentered: Promise<void> | undefined;
		const fixture = commandFixture("app", (event) => {
			events.push(event);
			if (event === "app.close") reentered = shutdown!();
		});
		let shutdown: (() => Promise<void>) | undefined;
		await withEnv(async () => {
			await expect(runRuntimePodCommand({
				...fixture.options, logger: { info: () => undefined, error: () => { throw new Error(failureSentinel); } },
				registerSignalHandlers: (close) => { shutdown = close; },
				waitForever: async () => {
					const first = shutdown!();
					expect(events).toContain("app.close");
					expect(reentered).toBe(first);
					const second = shutdown!();
					expect(second).toBe(first);
					await first;
					return undefined as never;
				},
			})).rejects.toBe(fixture.failure);
		});
		expect(events.filter((event) => event.endsWith(".close"))).toEqual(["app.close", "runtime_core.close"]);
	});

	test("disabled, throwing and real backpressured diagnostics do not extend prompt business cleanup", async () => {
		for (const mode of ["disabled", "throw", "backpressure"]) {
			const events: string[] = [];
			let complete: (() => void) | undefined;
			const stream = new Writable({ highWaterMark: 1, write(_chunk, _encoding, done) { complete = done; } });
			const sink = createDiagnosticStreamSink(stream);
			const logger = mode === "disabled" ? { info: () => undefined, error: () => undefined }
				: mode === "throw" ? { info: () => { throw new Error(failureSentinel); }, error: () => { throw new Error(failureSentinel); }, flush: () => { throw new Error(failureSentinel); } }
					: createJsonLogger({ write: sink.write });
			if (mode === "backpressure") logger.error({ event: "command_backpressure_probe" });
			const fixture = commandFixture("wait", (event) => events.push(event));
			const before = performance.now();
			try {
				await withEnv(async () => { await expect(runRuntimePodCommand({ ...fixture.options, logger, registerSignalHandlers: () => undefined })).rejects.toBe(fixture.failure); });
				expect(performance.now() - before).toBeLessThan(1_000);
				expect(events.filter((event) => event.endsWith(".close"))).toEqual(["app.close", "runtime_core.close"]);
				if (mode === "backpressure") { expect(sink.stats().blocked).toBe(true); expect(sink.stats().dropped).toBeGreaterThan(0); }
			} finally { complete?.(); sink.close(); stream.destroy(); }
		}
	});

	test("actual application HTTP-stop failure still joins gRPC and releases the next command resource", async () => {
		const failure = new Error(failureSentinel), events: string[] = [], lines: string[] = [];
		const originalServe = Bun.serve;
		let closeActualHttp: (() => Promise<void>) | undefined;
		let port: number | undefined;
		Bun.serve = ((...args: Parameters<typeof Bun.serve>) => {
			const server = originalServe(...args);
			closeActualHttp = async () => { await server.stop(true); };
			return new Proxy(server, {
				get(target, key) {
					if (key === "stop") return () => {
						events.push("http.stop");
						void closeActualHttp?.().catch(() => undefined);
						throw failure;
					};
					return Reflect.get(target, key, target);
				}
			});
		}) as typeof Bun.serve;
		try {
			await withEnv(async () => {
				await expect(runRuntimePodCommand({
					logger: createJsonLogger({ write: (line) => { lines.push(line); } }),
					dependencyBuilder: async (input) => {
						const fixture = commandFixture("none");
						const dependencies = await fixture.options.dependencyBuilder!(input);
						const app = createRuntimePodApp({ config: { ...input.config, grpcBindAddress: "127.0.0.1:0" }, logger: input.logger, tokenReviewClient: dependencies.tokenReviewClient, commandRunHost: {} as never, cleanupRunHost: {} as never, runtimeProcess: fixtureRuntimeProcess(), quiesce: async () => { events.push("drain"); }, });
						return {
							...dependencies,
							coreHosts: { ...dependencies.coreHosts, close: async () => { events.push("next.close"); } },
							app: {
								...app,
								start: async () => { const started = await app.start(); port = started.grpcPort; return started; },
								shutdown: () => {
									const closing = app.shutdown();
									expect(app.lifecycle.ready()).toEqual({ ready: false });
									expect(app.lifecycle.metricsSnapshot().accepting).toBe(false);
									return closing;
								},
							},
						};
					},
					registerSignalHandlers: () => undefined,
					waitForever: async () => undefined as never,
				})).rejects.toBe(failure);
			});
			expect(events.filter((event) => event === "http.stop")).toHaveLength(1);
			expect(events.filter((event) => event === "next.close")).toHaveLength(1);
			expect(events).toContain("drain");
			expect(port).toBeGreaterThan(0);
			await expect(new Promise<void>((resolve, reject) => {
				const socket = new Socket();
				const fail = (error: Error): void => { socket.destroy(); reject(error); };
				socket.setTimeout(250, () => fail(new Error("refused-port probe timed out")));
				socket.once("connect", () => { socket.destroy(); resolve(); });
				socket.once("error", fail);
				socket.connect({ host: "127.0.0.1", port: port! });
			})).rejects.toMatchObject({ code: "ECONNREFUSED" });
			expect(lines.join("")).not.toContain(failureSentinel);
			expect(lines.map((line) => JSON.parse(line))).toContainEqual(expect.objectContaining({ event: "workload.cleanup_failed", phase: "app" }));
		} finally { Bun.serve = originalServe; await closeActualHttp?.(); }
	});

	test("actual application bootstrap failure remains unready when an injected diagnostic callback throws", async () => {
		let app: ReturnType<typeof createRuntimePodApp> | undefined, nextCloses = 0;
		await withEnv(async () => {
			await expect(runRuntimePodCommand({
				logger: { info: () => { throw new Error(failureSentinel); }, error: () => { throw new Error(failureSentinel); } },
				dependencyBuilder: async (input) => {
					const dependencies = await commandFixture("none").options.dependencyBuilder!(input);
					app = createRuntimePodApp({
						config: input.config, logger: input.logger, tokenReviewClient: dependencies.tokenReviewClient,
						commandRunHost: {} as never, cleanupRunHost: {} as never, bootstrap: { core: async () => { throw new Error(failureSentinel); } },
						runtimeProcess: fixtureRuntimeProcess(), quiesce: async () => undefined,
					});
					return {
						...dependencies, app,
						coreHosts: { ...dependencies.coreHosts, close: async () => { nextCloses++; } },
					};
				}, registerSignalHandlers: () => undefined,
			})).rejects.toThrow("runtime pod startup failed");
		});
		expect(app?.lifecycle.ready()).toEqual({ ready: false });
		expect(nextCloses).toBe(1);
	});

	test("the production executable config-failure boundary exits with safe JSON instead of a native stack", () => {
		const path = new URL("../../src/command.ts", import.meta.url).pathname;
		const result = spawnSync(process.execPath, [path], {
			encoding: "utf8", timeout: 10_000,
			env: { ...process.env, ...commandEnv(), TETRAL_LOG_LEVEL: failureSentinel },
		});
		expect(result.error).toBeUndefined();
		expect(result.signal).toBeNull();
		expect(result.status).toBe(1);
		expect(result.stderr).not.toContain(failureSentinel);
		expect(result.stderr).not.toContain("Bun v");
		const records = result.stderr.trim().split("\n").map((line) => JSON.parse(line));
		expect(records).toHaveLength(1);
		expect(records[0]).toMatchObject({ level: "error", kind: "config_error" });
	});

	test("the shared signal entry contains a synchronous callback failure before returning", () => {
		const path = new URL("../fixtures/command-process.ts", import.meta.url).pathname;
		const result = spawnSync(process.execPath, [path, "signal_sync_throw"], { encoding: "utf8", timeout: 10_000 });
		expect(result.error).toBeUndefined();
		expect(result.signal).toBeNull();
		expect(result.status).toBe(1);
		expect(result.stdout).toBe("shutdown.entered\n");
		expect(result.stderr).toBe("");
	});

	test("actual Bun entry and both signal exits contain raw failures and report nonzero status", () => {
		const path = new URL("../fixtures/command-process.ts", import.meta.url).pathname;
		for (const mode of ["dependency", "listener", "wait", "both_cleanup", "SIGTERM_failure", "SIGINT_failure", "SIGTERM", "SIGINT", "SIGTERM_immediate", "SIGINT_immediate"]) {
			const result = spawnSync(process.execPath, [path, mode], { encoding: "utf8", timeout: 10_000 });
			expect(result.error).toBeUndefined();
			expect(result.signal).toBeNull();
			expect(result.status).toBe(mode.startsWith("SIG") && !mode.endsWith("_failure") ? 0 : 1);
			expect(result.stderr).not.toContain(failureSentinel);
			expect(result.stderr).not.toContain("PRIVATE_LATER_CLOSE_NON_TOKEN_SENTINEL");
			expect(result.stderr).not.toContain("Bun v");
			for (const line of result.stderr.trim().split("\n").filter(Boolean)) expect(() => JSON.parse(line)).not.toThrow();
			const events = result.stdout.trim().split("\n");
			if (mode !== "dependency") {
				expect(events.filter((event) => event === "app.close")).toHaveLength(1);
				expect(events.filter((event) => event === "runtime_core.close")).toHaveLength(1);
			}
			if (mode.endsWith("_immediate")) {
				expect(events.indexOf("app.close")).toBeLessThan(events.indexOf("signal.returned"));
			}
			if (mode.endsWith("_failure")) expect(JSON.parse(result.stderr.trim().split("\n").at(-1)!)).toMatchObject({ event: "workload.cleanup_failed", phase: "app", "error.class": "cleanup_error" });
		}
	}, 30_000);
});

async function withEnv(run: () => Promise<void>): Promise<void> {
	const saved = { ...process.env };
	Object.assign(process.env, commandEnv());
	try { await run(); } finally {
		for (const key of Object.keys(process.env)) delete process.env[key];
		Object.assign(process.env, saved);
	}
}

function fixtureRuntimeProcess(): RuntimeProcessPort {
	return {
		runtimeProcessId: "command-failure-process",
		register: async () => undefined,
		report: async () => undefined,
		release: async () => {
			throw new Error("command fixture owns no Session binding");
		},
		close: async () => undefined,
	};
}
