import { describe, expect, test } from "bun:test";
import type { SchemaSQL } from "../../../schema/src/verify.js";
import {
	SchemaVerificationError,
	verifyPostgreSQLSchema,
} from "../../../schema/src/verify.js";
import { buildProviderGatewayCommandDependencies } from "../../src/command.js";
import type { ProviderGatewayConfig } from "../../src/config.js";

describe("provider-gateway schema verification", () => {
	test("accepts the exact current registry through SELECT-only SQL", async () => {
		const queries: string[] = [];
		const sql = schemaSQL(queries, [
			[{ exists: true }],
			[
				{
					version: 1,
					checksum:
						"a74c18aaa979dd2f94e175f53cddb2addc26bd320343412932648a88c6ebaee2",
				},
			],
		]);

		await expect(verifyPostgreSQLSchema(sql)).resolves.toBeUndefined();
		expect(queries).toHaveLength(2);
		expect(
			queries.every((query) => query.trimStart().startsWith("SELECT")),
		).toBe(true);
	});

	for (const testCase of [
		{
			name: "missing",
			responses: [[{ exists: false }]],
			kind: "schema_missing",
		},
		{
			name: "behind",
			responses: [[{ exists: true }], []],
			kind: "schema_behind",
		},
		{
			name: "gap",
			responses: [
				[{ exists: true }],
				[{ version: 2, checksum: "a".repeat(64) }],
			],
			kind: "schema_history_gap",
		},
		{
			name: "duplicate",
			responses: [
				[{ exists: true }],
				[
					{
						version: 1,
						checksum:
							"a74c18aaa979dd2f94e175f53cddb2addc26bd320343412932648a88c6ebaee2",
					},
					{
						version: 1,
						checksum:
							"a74c18aaa979dd2f94e175f53cddb2addc26bd320343412932648a88c6ebaee2",
					},
				],
			],
			kind: "schema_history_duplicate",
		},
		{
			name: "drift",
			responses: [
				[{ exists: true }],
				[{ version: 1, checksum: "a".repeat(64) }],
			],
			kind: "schema_checksum_drift",
		},
		{
			name: "ahead",
			responses: [
				[{ exists: true }],
				[
					{
						version: 1,
						checksum:
							"a74c18aaa979dd2f94e175f53cddb2addc26bd320343412932648a88c6ebaee2",
					},
					{ version: 2, checksum: "a".repeat(64) },
				],
			],
			kind: "schema_ahead",
		},
	] as const) {
		test(`fails closed for ${testCase.name} history`, async () => {
			const sql = schemaSQL(
				[],
				testCase.responses.map((response) => [...response]),
			);
			try {
				await verifyPostgreSQLSchema(sql);
				throw new Error("expected schema verification failure");
			} catch (error) {
				expect(error).toBeInstanceOf(SchemaVerificationError);
				expect((error as SchemaVerificationError).kind).toBe(testCase.kind);
				expect(String(error)).not.toContain("postgres://");
				expect(String(error)).not.toContain("SELECT");
			}
		});
	}

	test("turns malformed driver results into a typed public-safe error", async () => {
		let calls = 0;
		const sql = (<T>(_strings: TemplateStringsArray): PromiseLike<T> => {
			calls += 1;
			if (calls === 1) return Promise.resolve([{ exists: true }] as T);
			return Promise.resolve(null as T);
		}) as SchemaSQL;
		try {
			await verifyPostgreSQLSchema(sql);
			throw new Error("expected malformed schema verification failure");
		} catch (error) {
			expect(error).toBeInstanceOf(SchemaVerificationError);
			expect((error as SchemaVerificationError).kind).toBe(
				"schema_history_malformed",
			);
			expect(String(error)).not.toContain("SELECT");
		}
	});

	test("redacts a secret-bearing driver error", async () => {
		const secret = "postgres://schema-user:do-not-leak@db.internal/tetral";
		const sql = (<T>(_strings: TemplateStringsArray): PromiseLike<T> =>
			Promise.reject(new Error(secret))) as SchemaSQL;
		try {
			await verifyPostgreSQLSchema(sql);
			throw new Error("expected schema verification failure");
		} catch (error) {
			expect(error).toBeInstanceOf(SchemaVerificationError);
			expect((error as SchemaVerificationError).kind).toBe(
				"schema_history_malformed",
			);
			expect(String(error)).not.toContain(secret);
			expect(String(error)).not.toContain("postgres://");
		}
	});

	test("schema-behind aborts dependency construction before stores or bootstrap", async () => {
		const events: string[] = [];
		const behind = new SchemaVerificationError("schema_behind");
		let sqlOptions: Bun.SQL.PostgresOrMySQLOptions | undefined;
		await expect(
			buildProviderGatewayCommandDependencies({
				config: providerConfig(),
				logger: { info: () => undefined, error: () => undefined },
				builderOptions: {
					tokenReviewClientFactory: () => ({
						createTokenReview: async () => ({
							authenticated: false,
							username: "",
							audiences: [],
							podUid: "",
						}),
					}),
					sqlFactory: (options) => {
						sqlOptions = options;
						return schemaSQL(events, []) as never;
					},
					schemaVerifier: async () => {
						events.push("schema_verify");
						throw behind;
					},
				},
			}),
		).rejects.toBe(behind);
		expect(sqlOptions).toEqual({
			url: "postgres://gateway",
			max: 10,
			idleTimeout: 30,
			maxLifetime: 1_800,
			connectionTimeout: 30,
			connection: { statement_timeout: 30_000 },
		});
		expect(events).toEqual(["schema_verify", "close"]);
	});
});

function schemaSQL(
	queries: string[],
	responses: unknown[][],
): SchemaSQL & { close: () => Promise<void> } {
	const query = (<T>(strings: TemplateStringsArray): PromiseLike<T> => {
		queries.push(strings.join(""));
		return Promise.resolve((responses.shift() ?? []) as T);
	}) as SchemaSQL & { close: () => Promise<void> };
	query.close = async () => {
		queries.push("close");
	};
	return query;
}

function providerConfig(): ProviderGatewayConfig {
	return {
		deploymentEnvironment: "test",
		serviceVersion: "unit",
		grpcBindAddress: "127.0.0.1:9090",
		httpBindAddress: "127.0.0.1:8080",
		allowedRuntimePod: { namespace: "tetral", serviceAccount: "runtime" },
		runtimeBindingTokenHMACKey: "x".repeat(32),
		databaseUrl: "postgres://gateway",
		databasePool: {
			max: 10,
			idleTimeout: 30,
			maxLifetime: 1_800,
			connectionTimeout: 30,
			statementTimeoutMs: 30_000,
		},
		vaultKeyHex: "01".repeat(32),
		kubernetesApiServerUrl: "https://kubernetes.default.svc",
		kubernetesApiCaCertPath: "/ca",
		tokenReviewReviewerTokenPath: "/token",
		bridgeApiGrpcAddress: "bridge:9090",
		bridgeTokenPath: "/bridge-token",
		maxConcurrentTurns: 1,
	};
}
