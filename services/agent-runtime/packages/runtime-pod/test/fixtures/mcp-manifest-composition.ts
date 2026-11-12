import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import type { LLMRequest } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { Metadata } from "@grpc/grpc-js";
import * as SessionManager from "@tetral/agent-runtime-core/src/session/session-manager.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import type { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/turn/load.js";
import type { ToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import type { ApplyRuntimeConfigRequest } from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import { Cause, Context, Effect, Exit, Layer, Scope, Stream } from "effect";
import { runtimeThreadLoopLayer } from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import { BridgeAPIEventWriter, BridgeAPIContextLoader } from "../../src/bridge-client.js";
import {
	runtimeMCPManifestEligibilityFromPatchPayloads,
	runtimeToolPolicyForThread,
} from "../../src/command.js";
import type { RuntimeSessionRunHost } from "../../src/runtime-service.js";
import { RuntimeControlService } from "../../src/runtime-service.js";

interface CompositionInput {
	readonly durableMode: { readonly bridgeAddress:string; readonly tokenPath:string; readonly threadId:string; readonly bindingId:string; readonly podUid:string; readonly runtimeProcessId:string; readonly serverName:string };
	readonly workspaceId: string;
	readonly sessionId: string;
}

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("composition input path is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as CompositionInput;
await serveDurableRuntime(input);

async function buildManager(ownedThreadLoopLayer?: Layer.Layer<ThreadLoop.Service>): Promise<{
	readonly manager: SessionManager.Interface;
	readonly scope: Scope.Scope;
	readonly events: SessionManager.RuntimeMCPManifestUpdateEvent[];
	readonly catalogs: ToolCatalog[];
}> {
	const events: SessionManager.RuntimeMCPManifestUpdateEvent[] = [];
	const catalogs: ToolCatalog[] = [];
	const threadLoopLayer = Layer.succeed(
		ThreadLoop.Service,
		ThreadLoop.Service.of({
			run: () => Effect.succeed({ type: "completed", modelMessageCount: 0 }),
			closeFailedRun: () =>
				Effect.succeed({ type: "landed", disposition: "continuation" }),
				closeRecoveredOpenRequestForInterrupt: () =>
					Effect.succeed({ type: "interrupted" }),
				settleIdleInterrupt: () =>
					Effect.succeed({ type: "applied", wakeThread: false }),
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
			catalogs.push(
				runtimeToolPolicyForThread(undefined, payloads, "claude").toolCatalog,
			);
			return runtimeMCPManifestEligibilityFromPatchPayloads(
				payloads,
				serverName,
			);
		},
	}).pipe(Layer.provide(ownedThreadLoopLayer ?? threadLoopLayer));
	return await Effect.runPromise(
		Effect.gen(function* () {
			const scope = yield* Scope.make();
			const context = yield* Layer.buildWithScope(managerLayer, scope);
			return {
				manager: Context.get(context, SessionManager.Service),
				scope,
				events,
				catalogs,
			};
		}),
	);
}

function runtimeControlService(
	request: ApplyRuntimeConfigRequest,
	manager: SessionManager.Interface,
): RuntimeControlService {
	const runHost = {
		handleRuntimeConfigPatch: async (
			sessionId: string,
			command: Parameters<
				SessionManager.Interface["applyRuntimeConfigPatch"]
			>[1],
		) =>
			await Effect.runPromise(
				manager.applyRuntimeConfigPatch(sessionId, command),
			),
	} as unknown as RuntimeSessionRunHost;
	return new RuntimeControlService({
	runtimeProcessId: request.runtimeProcessId,
		ownPod: {
			namespace: "engine",
			name: "runtime-pod-composition",
			uid: request.targetPodUid,
			ip: "127.0.0.1",
		},
		allowedJobRunner: { namespace: "tetral-system", name: "job-runner" },
		authenticator: {
			authenticate: async () => ({
				ok: true as const,
				serviceAccount: { namespace: "tetral-system", name: "job-runner" },
			}),
		},
		runHost,
		cleanupController: {
			startCleanup: async (scope) => ({
				ok: true as const,
				sessionId: scope.sessionId,
				cleaned: true,
			}),
		},
		logger: { info: () => undefined, error: () => undefined },
		ready: () => true,
	});
}

function addressState(input: CompositionInput) {
	return {workspaceId:input.workspaceId,sessionId:input.sessionId,sessionThreadId:input.durableMode.threadId,bindingId:input.durableMode.bindingId,bindingGeneration:1,targetPodUid:input.durableMode.podUid,runtimeProcessId:input.durableMode.runtimeProcessId};
}

function controlState(input: CompositionInput, runtimeInputId: string) {
	return {
		...addressState(input),
		requestId: `req_${runtimeInputId}`,
		runtimeInputId,
		eventIds: [],
		sequenceFrom: 0,
		sequenceTo: 0,
	};
}

/** Actual Runtime command listener and Core owner for cross-service composition. */
async function serveDurableRuntime(input: CompositionInput): Promise<void> {
    const mode = input.durableMode;
    const loader = new BridgeAPIContextLoader({ address: mode.bridgeAddress, tokenPath: mode.tokenPath });
    const writer = new BridgeAPIEventWriter({ address: mode.bridgeAddress, tokenPath: mode.tokenPath });
    const providerRequests: {
        toolNames: string[];
        modelRequestId: string;
        generation: number;
    }[] = [];
    const runtimeRecords: unknown[] = [];
    let holdProvider = false, releaseProvider: () => void = () => { }, activeProviders = 0;
    const provider = { stream(request: LLMRequest) {
            providerRequests.push({ toolNames: request.tools.map(tool => tool.name), modelRequestId: request.modelRequestId, generation: currentThread?.configuration.manifestGeneration(mode.serverName) ?? 0 });
            const blocked = holdProvider ? new Promise<void>(resolve => { releaseProvider = resolve; }) : Promise.resolve();
            activeProviders++;
            return Stream.fromEffect(Effect.promise(() => blocked)).pipe(Stream.flatMap(() => { activeProviders--; runtimeRecords.push({ phase: "provider-stream-released" }); return Stream.fromIterable([  {type:"text-complete" as const,providerPartId:"manifest-text",eventId:`evt_${createHash("sha256").update(JSON.stringify(["mcp-manifest-composition.ts", request.sessionId, request.modelRequestId, "manifest-text"])).digest("hex").slice(0,32)}`,text:("manifest applied")}, { type: "finish" as const, finishReason: "stop" as const }]); }));
        } };
    let currentThread: ThreadRuntime | undefined;
    // Legacy helper-only context APIs must never substitute a durable read.
    const actualLoader = Object.assign(loader, { buildContext: async () => { throw new Error("legacy context read reached"); }, loadPendingInput: async () => { throw new Error("legacy pending read reached"); } });
    const coreLayer = runtimeThreadLoopLayer(actualLoader, { writer, llmService: provider, installLoaderState: false, runtime: { now: () => new Date().toISOString(), monotonicMs: () => performance.now(), createId: prefix => `${prefix}_${crypto.randomUUID().replaceAll("-", "")}`, sleep: async (milliseconds, signal) => { await new Promise<void>(resolve => { const timer = setTimeout(resolve, milliseconds); signal?.addEventListener("abort", () => { clearTimeout(timer); resolve(); }, { once: true }); }); return !signal?.aborted; } }, recordAcceptedInputCommit: event => runtimeRecords.push({ phase: "accepted-input-commit", ...event }), recordRuntimeTerminalSettlement: event => runtimeRecords.push({ phase: "terminal-settlement", ...event }), runtimePolicy: thread => runtimeToolPolicyForThread(thread.identity.threadRole, thread.configuration.patches().map(patch => patch.contentJson), thread.configuration.installedBuiltinFamily()), runtimeModel: () => ({ providerId: "anthropic", modelId: "claude-opus-4-8" }) });
    const capturingLayer = Layer.effect(ThreadLoop.Service, Effect.gen(function* () { const owned = yield* ThreadLoop.Service; return ThreadLoop.Service.of({ ...owned, run: (thread, custody) => { currentThread = thread; return owned.run(thread, custody).pipe(Effect.onExit(exit => Effect.sync(() => runtimeRecords.push(Exit.isFailure(exit) ? { phase: "core-run-failed", cause: Cause.pretty(exit.cause).slice(0, 160) } : { phase: "core-run-completed", result: exit.value })))); } }); }).pipe(Effect.provide(coreLayer)));
    const { manager, scope, events, catalogs } = await buildManager(capturingLayer);
    async function coldLoad() {
        const loaded = await loader.loadThreadContext(controlState(input, "rin_actual_cold_load"));
        const turnCheckpoint = extractThreadTurnCheckpoint({ messages: loaded.messages, facts: loaded.turnFacts });
        const turnToolRouteView = extractColdThreadToolRouteView({ checkpoint: turnCheckpoint, pendingToolUses: loaded.pendingToolUses ?? [], pendingSandboxExecutions: loaded.pendingSandboxExecutions ?? [] });
        const response = await Effect.runPromise(manager.preloadThread({ ...controlState(input, "rin_actual_cold_load"), runtimeBindingToken: loaded.runtimeBindingToken, currentRequestMessage:loaded.currentRequestMessage,messages: loaded.messages, turnCheckpoint, turnToolRouteView, thread: loaded.thread, ...(loaded.runtimeConfigPatch === undefined ? {} : { runtimeConfigPatch: loaded.runtimeConfigPatch }), ...(loaded.mcpManifests === undefined ? {} : { mcpManifests: loaded.mcpManifests }) }));
        if (!response.ok || !response.applied)
            throw new Error("actual Bridge cold preload failed");
        return { response, manifestGenerations: loaded.mcpManifests?.map(patch => ({ server: patch.mcpServerName, generation: patch.generation })) };
    }
    const cold = await coldLoad();
    const request: ApplyRuntimeConfigRequest = { workspaceId: input.workspaceId, sessionId: input.sessionId, bindingId: mode.bindingId, bindingGeneration: 1, targetPodUid: mode.podUid, runtimeProcessId: mode.runtimeProcessId };
    const service = runtimeControlService(request, manager);
    const server = createRuntimeGrpcServer(service);
    const port = await server.bind("127.0.0.1:0");
    const control = Bun.serve({ hostname: "127.0.0.1", port: 0, idleTimeout: 0, fetch: async (incoming) => {
            try {
                const action = await incoming.json() as {
                    kind: string;
                    runtimeInputId?: string;
                    inputOrder?: number;
                    hold?: boolean;
                };
                if (action.kind === "observe")
                    return Response.json({ cold, events, catalogNames: catalogs.map(catalog => catalog.entries.map(entry => entry.name)), catalogRoutes: currentThread === undefined ? [] : [runtimeToolPolicyForThread(currentThread.identity.threadRole, currentThread.configuration.patches().map(patch => patch.contentJson), currentThread.configuration.installedBuiltinFamily()).toolCatalog.entries.map(entry => ({ name: entry.name, route: entry.route }))], providerRequests, activeProviders, runtimeRecords, currentGeneration: currentThread?.configuration.manifestGeneration(mode.serverName) ?? events.findLast(event => event.mcpServerName === mode.serverName)?.currentGeneration });
                if (action.kind === "start-provider") {
                    holdProvider = action.hold === true;
                    const result = await Effect.runPromise(manager.acceptInput({ ...addressState(input), runtimeInputId: action.runtimeInputId!, inputOrder: action.inputOrder!, kind: "messages", contentJson: JSON.stringify({ messages: [{ parts: [{ type: "text", text: "observe manifest catalog" }] }] }) }));
                    return Response.json({ result });
                }
                if (action.kind === "release-provider") {
                    holdProvider = false;
                    releaseProvider();
                    return Response.json({ released: true });
                }
                if (action.kind === "wait-idle") {
                    const deadline = Date.now() + 5000;
                    let snapshot;
                    do {
                        snapshot = await Effect.runPromise(manager.inspectThread(addressState(input)));
                        if (snapshot.ok && snapshot.status === "idle")
                            break;
                        if (Date.now() >= deadline)
                            return Response.json({ failed: true, snapshot: { status: snapshot.ok ? snapshot.status : "unavailable" }, runtimeRecords: runtimeRecords.slice(-8), activeProviders }, { status: 500 });
                        await Bun.sleep(1);
                    } while (true);
                    return Response.json({ snapshot });
                }
                if (action.kind === "shutdown") {
                    setTimeout(() => { void close(); }, 10);
                    return Response.json({ shutdown: true });
                }
                throw new Error("unknown owned Runtime control action");
            }
            catch {
                return Response.json({ failed: true }, { status: 500 });
            }
        } });
    process.stdout.write(`${JSON.stringify({ runtimePort: port, controlAddress: control.url.origin, versions: { bun: Bun.version }, cold })}\n`);
    let finish!: () => void;
    const stopped = new Promise<void>(resolve => { finish = resolve; });
    async function close() { holdProvider = false; releaseProvider(); await server.shutdown(); await Effect.runPromise(Scope.close(scope, Exit.void)); await Promise.all([writer.close(), loader.close()]); await control.stop(); finish(); }
    await stopped;
}
