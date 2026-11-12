import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { Metadata } from "@grpc/grpc-js";
import * as SessionManager from "@tetral/agent-runtime-core/src/session/session-manager.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/turn/load.js";
import type { ToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { ApplyRuntimeConfigRequest } from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import type { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { McpConnectorServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { RunMcpToolStatus } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Context, Effect, Exit, Layer, Scope } from "effect";
import {
	RecordingContextLoader,
	runtimeThreadLoopLayer,
	testRunCustody,
} from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";
import {
	runtimeMCPManifestEligibilityFromPatchPayloads,
	runtimeToolPolicyForThread,
} from "../../src/command.js";
import type { RuntimeSessionRunHost } from "../../src/runtime-service.js";
import { RuntimeControlService } from "../../src/runtime-service.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

// Written by the integration tests: around one MCP manifest change, what the
// Runtime has installed, the command the Job Runner sends, and the context
// Bridge serves a replacement Runtime.
const vectors = new URL("../../../protocol/testdata/mcp-manifest-commands/", import.meta.url);

interface ManifestCommandVector {
	readonly runtimeConfigContentJson: string;
	readonly installedManifest: { readonly generation: number; readonly contentJson: string };
	readonly command: unknown;
	readonly coldContextJson: string;
}

// Whether the MCP tool is in the catalog before and after the command.
const rows: Record<string, { readonly installedOffers: boolean; readonly commandOffers: boolean }> = {
	"unready-generation-removes-tool": { installedOffers: true, commandOffers: false },
	"ready-generation-installs-tool": { installedOffers: false, commandOffers: true },
};
const toolName = "github_search";

describe("MCP manifest command vectors", () => {
	test("every vector has a row and every row a vector", () => {
		const names = readdirSync(vectors)
			.filter((name) => name.endsWith(".json"))
			.map((name) => name.slice(0, -".json".length))
			.sort();
		expect(names).toEqual(Object.keys(rows).sort());
	});

	for (const [name, row] of Object.entries(rows)) {
		test(`${name}: the command sets the catalog warm and cold, and the next request follows it`, async () => {
			const vector = JSON.parse(readFileSync(new URL(`${name}.json`, vectors), "utf8")) as ManifestCommandVector;
			const command = ApplyRuntimeConfigRequest.fromJSON(vector.command);
			const commandGeneration = command.mcpManifest?.generation ?? 0;

			const warm = await applyWarm(vector, command);
			expect(warm.commandResponse.applied).toBeDefined();
			expect(warm.staleResponse.duplicate).toBeDefined();
			expect(warm.events.map((event) => [event.source, event.disposition, event.currentGeneration, event.toolCatalogEligible])).toEqual([
				["cold_load", "applied", vector.installedManifest.generation, row.installedOffers],
				["runtime_config_update", "applied", commandGeneration, row.commandOffers],
				["runtime_config_update", "stale", commandGeneration, row.commandOffers],
			]);
			expect(warm.catalogs.map(offers)).toEqual([row.installedOffers, row.commandOffers, row.commandOffers]);

			const cold = await loadReplacement(vector, command);
			expect(cold.events.map((event) => [event.source, event.currentGeneration, event.toolCatalogEligible])).toEqual([
				["cold_load", commandGeneration, row.commandOffers],
			]);
			expect(cold.catalogs.map(offers)).toEqual([row.commandOffers]);

			for (const catalog of [warm.catalogs[2]!, cold.catalogs[0]!]) {
				const next = await nextRequest(command, catalog);
				expect(next.providerToolNames.length).toBeGreaterThan(0);
				expect(next.providerToolNames.every((names) => names.includes(toolName) === row.commandOffers)).toBe(true);
				expect(next.mcpConnectorCalls).toBe(row.commandOffers ? 1 : 0);
			}
		});
	}
});

// installedManifestState is the readiness and etag Bridge's context carries
// beside an installed manifest payload.
function installedManifestState(contentJson: string): { manifestReadiness: "unready" } | { manifestETag: string } {
	const manifest = (JSON.parse(contentJson) as { mcp_manifest: { readiness: string; manifest_etag?: string } }).mcp_manifest;
	return manifest.readiness === "unready" ? { manifestReadiness: "unready" } : { manifestETag: manifest.manifest_etag ?? "" };
}

function offers(catalog: ToolCatalog): boolean {
	return lookupToolEntry(catalog, toolName) !== undefined;
}

async function applyWarm(vector: ManifestCommandVector, command: ApplyRuntimeConfigRequest) {
	const { manager, scope, events, catalogs } = await buildManager();
	try {
		const preload = await Effect.runPromise(
			manager.preloadThread({
				...controlState(command, "rin_preload"),
				runtimeBindingToken: "runtime-binding-token",
				currentRequestMessage: null,
				messages: [],
				runtimeConfigPatch: {
					...addressState(command),
					configIdentity: "session:1",
					generation: 1,
					coldLoad: true,
					installedBuiltinFamily: "claude",
					contentJson: vector.runtimeConfigContentJson,
				},
				mcpManifests: [
					{
						...addressState(command),
						configIdentity: `mcp:github:${vector.installedManifest.generation}`,
						generation: vector.installedManifest.generation,
						mcpServerName: "github",
						...installedManifestState(vector.installedManifest.contentJson),
						contentJson: vector.installedManifest.contentJson,
					},
				],
			}),
		);
		expect(preload).toMatchObject({ ok: true, applied: true });
		const service = runtimeControlService(command, manager);
		const commandResponse = await service.applyRuntimeConfig(command, new Metadata());
		// The installed generation delivered again after the command.
		const staleResponse = await service.applyRuntimeConfig(
			{
				...command,
				sessionConfig: undefined,
				mcpManifest: {
					mcpServerName: "github",
					generation: vector.installedManifest.generation,
					contentJson: vector.installedManifest.contentJson,
				},
			},
			new Metadata(),
		);
		return { events, catalogs, commandResponse, staleResponse };
	} finally {
		await Effect.runPromise(Scope.close(scope, Exit.void));
	}
}

async function loadReplacement(vector: ManifestCommandVector, command: ApplyRuntimeConfigRequest) {
	const { manager, scope, events, catalogs } = await buildManager();
	try {
		const loader = new BridgeAPIContextLoader({
			address: "unused.test",
			tokenPath: "/unused/token",
			metadataFactory: async () => new Metadata(),
			client: {
				loadContext: (
					_request: unknown,
					_metadata: Metadata,
					_options: unknown,
					callback: (error: Error | null, response: unknown) => void,
				) => {
					callback(null, { contextJson: vector.coldContextJson, runtimeBindingToken: "runtime-binding-token" });
				},
			} as unknown as AgentRuntimeBridgeServiceClient,
		});
		const loaded = await loader.loadThreadContext(controlState(command, "rin_manifest_cold_load"));
		const turnCheckpoint = extractThreadTurnCheckpoint({ messages: loaded.messages, facts: loaded.turnFacts });
		const preload = await Effect.runPromise(
			manager.preloadThread({
				...controlState(command, "rin_manifest_cold_load"),
				runtimeBindingToken: loaded.runtimeBindingToken,
				currentRequestMessage: loaded.currentRequestMessage,
				messages: loaded.messages,
				turnCheckpoint,
				turnToolRouteView: extractColdThreadToolRouteView({
					checkpoint: turnCheckpoint,
					pendingToolUses: loaded.pendingToolUses ?? [],
					pendingSandboxExecutions: loaded.pendingSandboxExecutions ?? [],
				}),
				thread: loaded.thread,
				...(loaded.runtimeConfigPatch !== undefined ? { runtimeConfigPatch: loaded.runtimeConfigPatch } : {}),
				...(loaded.mcpManifests !== undefined ? { mcpManifests: loaded.mcpManifests } : {}),
			}),
		);
		expect(preload).toMatchObject({ ok: true, applied: true });
		return { events, catalogs };
	} finally {
		await Effect.runPromise(Scope.close(scope, Exit.void));
	}
}

// nextRequest runs one provider request with this catalog in which the model
// calls the MCP tool, and counts what reaches the Connector.
async function nextRequest(command: ApplyRuntimeConfigRequest, toolCatalog: ToolCatalog) {
	let mcpConnectorCalls = 0;
	const providerToolNames: string[][] = [];
	const runner = new RuntimePodToolRunner({
		bridgeAddress: "unused.test:9090",
		webAddress: "unused.test:9090",
		mcpConnectorAddress: "unused.test:9091",
		tokenPath: "/unused/token",
		metadataFactory: async () => new Metadata(),
		mcpConnectorClient: {
			runMcpTool: (_request: unknown, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void) => {
				mcpConnectorCalls += 1;
				callback(null, { status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED, resultText: "tool completed", attachments: [], errorKind: 0, retryStatus: 0 });
				return { cancel() {} };
			},
		} as unknown as Pick<McpConnectorServiceClient, "runMcpTool">,
	});
	const loader = new RecordingContextLoader([], {
		type: "context",
		entries: [{ messageSequence: 1, contextKind: "user", parts: [{ type: "text", text: "use the MCP tool" }] }],
	});
	const thread = new ThreadRuntime({
		threadRole: "main",
		threadVisibility: "public",
		...addressState(command),
		runtimeBindingToken: "runtime-binding-token",
	});
	const result = await Effect.runPromise(
		Effect.gen(function* () {
			const threadLoop = yield* ThreadLoop.Service;
			return yield* threadLoop.run(thread, testRunCustody());
		}).pipe(
			Effect.provide(
				runtimeThreadLoopLayer(loader, {
					events: [
						{
							type: "tool-call-complete",
							id: "manifest-tool-call",
							toolName,
							input: { query: "tetral" },
							inputPreview: { preview: "{}", truncated: false },
						},
						{ type: "finish", finishReason: "tool-calls" },
					],
					providerCallRuntime: {
						systemInstructions: "manifest command provider request",
						toolCatalog,
						toolsetFamily: "claude",
					},
					approvalMode: "full_access",
					onStream: (request) => providerToolNames.push(request.tools.map((tool) => tool.name)),
					runTool: (request) => runner.runTool(request),
				}),
			),
		),
	);
	expect(result.type).toBe("completed");
	return { mcpConnectorCalls, providerToolNames };
}

async function buildManager() {
	const events: SessionManager.RuntimeMCPManifestUpdateEvent[] = [];
	const catalogs: ToolCatalog[] = [];
	const threadLoopLayer = Layer.succeed(
		ThreadLoop.Service,
		ThreadLoop.Service.of({
			run: () => Effect.succeed({ type: "completed", modelMessageCount: 0 }),
			closeFailedRun: () => Effect.succeed({ type: "landed", disposition: "continuation" }),
			closeRecoveredOpenRequestForInterrupt: () => Effect.succeed({ type: "interrupted" }),
			settleIdleInterrupt: () => Effect.succeed({ type: "applied", wakeThread: false }),
			settleToolConfirmation: () => Effect.succeed({ type: "duplicate" }),
			threadNeedsRun: () => false,
			seedRuntimeModel: () => undefined,
			installLoadedPendingToolUses: () => Effect.succeed({ ok: true }),
			installLoadedSandboxExecutions: () => Effect.succeed({ ok: true }),
		}),
	);
	const managerLayer = SessionManager.layer({
		maxLocalSessions: 1,
		now: () => "2026-08-10T12:00:00.000Z",
		recordMCPManifestUpdate: (event) => events.push(event),
		resolveMCPManifestEligibility: (patches, serverName) => {
			const payloads = patches.map((patch) => patch.contentJson);
			catalogs.push(runtimeToolPolicyForThread(undefined, payloads, "claude").toolCatalog);
			return runtimeMCPManifestEligibilityFromPatchPayloads(payloads, serverName);
		},
	}).pipe(Layer.provide(threadLoopLayer));
	return await Effect.runPromise(
		Effect.gen(function* () {
			const scope = yield* Scope.make();
			const context = yield* Layer.buildWithScope(managerLayer, scope);
			return { manager: Context.get(context, SessionManager.Service), scope, events, catalogs };
		}),
	);
}

function runtimeControlService(command: ApplyRuntimeConfigRequest, manager: SessionManager.Interface): RuntimeControlService {
	const runHost = {
		handleRuntimeConfigPatch: async (
			sessionId: string,
			patch: Parameters<SessionManager.Interface["applyRuntimeConfigPatch"]>[1],
		) => await Effect.runPromise(manager.applyRuntimeConfigPatch(sessionId, patch)),
	} as unknown as RuntimeSessionRunHost;
	return new RuntimeControlService({
		runtimeProcessId: command.runtimeProcessId,
		ownPod: { namespace: "engine", name: "runtime-pod", uid: command.targetPodUid, ip: "127.0.0.1" },
		allowedJobRunner: { namespace: "tetral-system", name: "job-runner" },
		authenticator: {
			authenticate: async () => ({
				ok: true as const,
				serviceAccount: { namespace: "tetral-system", name: "job-runner" },
			}),
		},
		runHost,
		cleanupController: {
			startCleanup: async (scope) => ({ ok: true as const, sessionId: scope.sessionId, cleaned: true }),
		},
		logger: { info: () => undefined, error: () => undefined },
		ready: () => true,
	});
}

function addressState(command: ApplyRuntimeConfigRequest) {
	return {
		workspaceId: command.workspaceId,
		sessionId: command.sessionId,
		sessionThreadId: `thrd_${command.sessionId}`,
		bindingId: command.bindingId,
		bindingGeneration: command.bindingGeneration,
		targetPodUid: command.targetPodUid,
		runtimeProcessId: command.runtimeProcessId,
	};
}

function controlState(command: ApplyRuntimeConfigRequest, runtimeInputId: string) {
	return {
		...addressState(command),
		requestId: `req_${runtimeInputId}`,
		runtimeInputId,
		eventIds: [],
		sequenceFrom: 0,
		sequenceTo: 0,
	};
}
