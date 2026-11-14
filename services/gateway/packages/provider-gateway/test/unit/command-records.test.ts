import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildProviderGatewayCommandDependencies } from "../../src/command.js";
import type { ProviderGatewayConfig } from "../../src/config.js";
import { encryptAES256GCM } from "../../src/providers/crypto.js";
import { validProviderRequest, writtenLogs } from "./fixtures.js";

const fixtureCA = new URL("../fixtures/tls/cert.pem", import.meta.url);

// The failure records the command composition writes itself, read through the
// product's writer.
describe("provider-gateway command records", () => {
	test("a rotated trust bundle whose candidate pool fails verification is recorded with the verifier's error", async () => {
		const directory = await mkdtemp(join(tmpdir(), "gateway-reload-"));
		try {
			const caPath = join(directory, "ca.pem");
			const ca = await readFile(fixtureCA, "utf8");
			await writeFile(caPath, ca);
			const records: Record<string, unknown>[] = [];
			let verifications = 0;
			const dependencies = await buildProviderGatewayCommandDependencies({
				config: { ...config(), databaseTLS: { caPath, serverName: "db.internal" } },
				logger: writtenLogs(records),
				builderOptions: {
					tokenReviewClientFactory: () => ({
						createTokenReview: async () => ({ authenticated: false, username: "", audiences: [], podUid: "" }),
					}),
					sqlFactory: () => fakeSQL(() => []) as never,
					schemaVerifier: async () => {
						verifications++;
						if (verifications > 1) throw new Error("connect ECONNREFUSED 10.0.0.7:5432");
					},
				},
			});
			try {
				// The same anchor twice: a new bundle that removes no trust.
				await writeFile(caPath, `${ca.trim()}\n${ca.trim()}\n`);
				const deadline = Date.now() + 5_000;
				while (!records.some((record) => record.event === "transport.credential_reload_failed") && Date.now() < deadline)
					await Bun.sleep(25);
				const failed = records.filter((record) => record.event === "transport.credential_reload_failed");
				expect(failed.map((record) => [record["transport.outcome"], record["error.code"], record["error.message"]])).toEqual([
					["candidate_verification_failed", "operation_failed", "database pool generation verification failed: connect ECONNREFUSED 10.0.0.7:5432"],
				]);
			} finally {
				await dependencies.close?.();
			}
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	test("a platform key the model backend rejects is quarantined and recorded through the returned resolver", async () => {
		const records: Record<string, unknown>[] = [];
		const encryptedKey = await encryptAES256GCM(new TextEncoder().encode("sk-platform"), config().vaultKeyHex, () => new Uint8Array(12).fill(5));
		const dependencies = await buildProviderGatewayCommandDependencies({
			config: config(),
			logger: writtenLogs(records),
			builderOptions: {
				tokenReviewClientFactory: () => ({
					createTokenReview: async () => ({ authenticated: false, username: "", audiences: [], podUid: "" }),
				}),
				sqlFactory: () => fakeSQL((query) => query.includes("platform_provider_keys")
					? [{
						key_id: "pfk_openai_1", provider_id: "openai", encrypted_key: encryptedKey, weight: 1, priority: 0,
						cache_scope: "openai-workspace", status: "active", disabled_reason: null, updated_at: "2026-07-01T00:00:00Z",
					}]
					: []) as never,
				schemaVerifier: async () => undefined,
			},
		});
		try {
			const resolved = await dependencies.credentialResolver.resolve(validProviderRequest({ model: { providerId: "openai", modelId: "gpt-5.5", variant: "" } }));
			expect(resolved).toMatchObject({ ok: true, credential: { source: "platform", platformKey: { keyId: "pfk_openai_1" } } });
			dependencies.credentialResolver.recordPlatformFailure("pfk_openai_1", {
				action: "quarantine",
				providerError: { code: "provider_key_unavailable", message: "Incorrect API key provided: sk-pla***", statusCode: 401 },
			});
			const quarantined = records.filter((record) => record.event === "platform_provider_key_quarantined");
			expect(quarantined.map((record) => [record["error.code"], record["error.message"]])).toEqual([
				["provider_key_unavailable", "the model backend rejected a platform key with HTTP 401"],
			]);
			expect(JSON.stringify(records)).not.toContain("Incorrect API key");
		} finally {
			await dependencies.close?.();
		}
	});
});

/** A database whose every query answers with `rows(query)`. */
function fakeSQL(rows: (query: string) => unknown[]) {
	const sql = Object.assign(
		async (strings: TemplateStringsArray) => rows(strings.join("?")),
		{
			begin: async <T>(operation: (tx: typeof sql) => Promise<T>) => await operation(sql),
			close: async () => undefined,
		},
	);
	return sql;
}

function config(): ProviderGatewayConfig {
	return {
		deploymentEnvironment: "test",
		diagnostics: { level: "info", maxRecordBytes: 16384, summaryIntervalMs: 30000, burst: 1, traceSampleRatio: 1, dbSlowQueryMs: 250 },
		serviceVersion: "unit",
		grpcBindAddress: "127.0.0.1:9090",
		httpBindAddress: "127.0.0.1:8080",
		allowedRuntimePod: { namespace: "tetral", serviceAccount: "runtime" },
		runtimeBindingTokenHMACKey: "x".repeat(32),
		databaseUrl: "postgres://gateway",
		drainTimeoutMs: 30000,
		cancelJoinTimeoutMs: 5000,
		databasePool: { max: 10, idleTimeout: 30, maxLifetime: 1_800, connectionTimeout: 30, statementTimeoutMs: 30_000 },
		vaultKeyHex: "01".repeat(32),
		kubernetesApiServerUrl: "https://kubernetes.default.svc",
		kubernetesApiCaCertPath: "/ca",
		tokenReviewReviewerTokenPath: "/token",
		bridgeApiGrpcAddress: "bridge:9090",
		bridgeTokenPath: "/bridge-token",
		maxConcurrentTurns: 1,
	};
}
