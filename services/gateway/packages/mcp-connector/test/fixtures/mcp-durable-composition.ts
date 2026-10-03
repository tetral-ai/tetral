import { readFile } from "node:fs/promises";
import { createHmac } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import { credentials, Metadata } from "@grpc/grpc-js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import { AgentRuntimeBridgeServiceClient } from "@tetral/gateway-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { bridgeMcpCommitGrpcChannelOptions } from "../../src/bridge-client.js";
import { McpConnectorServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimeToolExecutionRequest } from "../../../../../agent-runtime/packages/core/src/thread-loop/tool-execution.js";
import { runtimeToolSettlement } from "../../../../../agent-runtime/packages/core/src/thread-loop/tool-execution.js";
import type { ToolEntry } from "../../../../../agent-runtime/packages/core/src/tools/tool-catalog.js";
import { BridgeAPIEventWriter } from "../../../../../agent-runtime/packages/runtime-pod/src/bridge-client.js";
import { RuntimePodToolRunner } from "../../../../../agent-runtime/packages/runtime-pod/src/tool-runner.js";
import { BridgeAPIManifestChangeNotifier, BridgeAPIMcpToolResultIdempotencyStore } from "../../src/bridge-client.js";
import { McpSDKClient, streamableHTTPTransportOptions } from "../../src/client.js";
import { SQLMcpCredentialResolver } from "../../src/credential.js";
import type { McpCredentialSQL } from "../../src/credential.js";
import { SQLMcpServerResolver } from "../../src/server-resolver.js";
import { createMcpConnectorGrpcServer } from "../../src/server.js";
import { McpConnectorServiceShell } from "../../src/service.js";
import { fixtureTools, McpHTTPProtocolFixture, until, barrier } from "./mcp-http-protocol.js";
import type { FixtureAdapter, FixtureResult } from "./mcp-http-protocol.js";

interface Input { bridgeAddress: string; gatewayTokenPath: string; runtimeTokenPath: string; bridgeTokenPath: string; workspaceId: string; sessionId: string; threadId: string; bindingId: string; podUid: string; masterKeyHex: string; bindingKey: string; executionTimeoutMs?: number; bridgeRecoveryAddress?: string }
interface Action { kind: "execute" | "list" | "discover" | "configure" | "reset" | "recreate" | "notify" | "resolve" | "observe" | "issuer" | "oauth-list" | "hold" | "wait-held" | "wait-issuer-cancelled" | "wait-waiters" | "wait-resources-closed" | "cancel" | "release" | "shutdown"; replica?: number; adapter?: FixtureAdapter; serverName?: string; toolName?: string; eventId?: string; callId?: string; nonce?: string; result?: FixtureResult; version?: Parameters<typeof fixtureTools>[0]; faultMethod?: string; faultOrigin?: string; faults?: number[]; workspaceId?: string; sessionId?: string; settle?: boolean; bindingToken?: string; credentialToken?: string; credentialLabel?: string; expectedStreams?: number; extraToolName?: string; issuerFailures?: string[]; twoPages?: boolean; holdPhase?: string; holdCursor?: string; expectedWaiters?: number; threadId?: string; bindingId?: string; callerDeadlineMs?: number }
const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("durable composition input path required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as Input;
const sdkPackage = await Bun.file("node_modules/@modelcontextprotocol/sdk/package.json").json() as { version: string };
const databaseURL = process.env.TETRAL_TEST_GATEWAY_DATABASE_URL;
if (databaseURL === undefined) throw new Error("installed Gateway role DSN required");
const sql = new Bun.SQL({ url: databaseURL, max: 4 });
const peers = { github: new McpHTTPProtocolFixture("github"), slack: new McpHTTPProtocolFixture("slack") };
for (const adapter of ["github", "slack"] as const) { peers[adapter].credentials.set(`Bearer fixture-${adapter}-token`, `${adapter}-original`); peers[adapter].rejectUnknownTools = true; }
const records: unknown[] = [];
const origins = new AsyncLocalStorage<string>();
const serverResolver = new SQLMcpServerResolver(sql as unknown as McpCredentialSQL);
const executions = new Map<string, AbortController>();
const controls = new Map<string,{entered:Promise<void>;release:()=>void}>();
const issuerHolds = new Map<FixtureAdapter,{entered:ReturnType<typeof barrier>;release:ReturnType<typeof barrier>}>();
const issuerRecords: unknown[] = [];
const issuerState = { github: { calls: 0, cancelled:0, rotation: 0, failures: [] as string[] }, slack: { calls: 0, cancelled:0, rotation: 0, failures: [] as string[] } };
const issuer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: async (request) => {
 const adapter = new URL(request.url).pathname.slice(1) as FixtureAdapter;
 if (adapter !== "github" && adapter !== "slack") return new Response(null, {status:404});
 const state = issuerState[adapter]; state.calls += 1;
 const form = new URLSearchParams(await request.text());
 const basic = request.headers.get("authorization") === `Basic ${btoa("fixture-client:fixture-client-secret")}`;
 const refresh = form.get("refresh_token");
 const expectedRefresh = state.rotation === 0 ? `fixture-${adapter}-refresh` : `fixture-${adapter}-refresh-${state.rotation}`;
 const refreshValid = refresh === expectedRefresh;
 const valid = request.method === "POST" && request.headers.get("content-type")?.includes("application/x-www-form-urlencoded") === true && form.get("grant_type") === "refresh_token" && refreshValid && (adapter === "slack" ? basic && !form.has("client_id") && !form.has("client_secret") : !basic && form.get("client_id") === "fixture-client") && form.get("scope") === "fixture-scope" && form.get("resource") === (adapter === "github" ? "https://api.githubcopilot.com/mcp/" : "https://mcp.slack.com/mcp");
 issuerRecords.push({adapter, validForm: valid, clientSecretBasic: basic, credentialLabel: refresh === `fixture-${adapter}-refresh` ? `${adapter}-original-refresh` : `${adapter}-rotated-refresh`});
 if (!valid) return Response.json({error:"controlled OAuth form rejected"},{status:400});

 const held=issuerHolds.get(adapter);
 if (held !== undefined) {
  held.entered.resolve();
  let abort!: () => void;
  try { await Promise.race([held.release.promise, new Promise<void>((resolve) => {
   abort = () => { state.cancelled += 1; resolve(); };
   if (request.signal.aborted) { abort(); return; }
   request.signal.addEventListener("abort", abort, { once: true });
  })]); } finally { request.signal.removeEventListener("abort", abort); }
  if (request.signal.aborted) return new Response(null, { status: 499 });
 }
 const failure = state.failures.shift();
 if (failure === "ok-false") return Response.json({ok:false,error:"invalid_grant"});
 if (failure === "http-503") return Response.json({error:"controlled issuer failure"},{status:503});
 if (failure === "malformed") return new Response("{malformed",{headers:{"content-type":"application/json"}});
 state.rotation += 1;
 const token = `fixture-${adapter}-rotated-${state.rotation}`;
 peers[adapter].credentials.set(`Bearer ${token}`, `${adapter}-rotation-${state.rotation}`);
 return Response.json({... (adapter === "slack" ? {ok:true} : {}), access_token: token, refresh_token: `fixture-${adapter}-refresh-${state.rotation}`, expires_in:3600});
} });
const issuerFetch = (endpoint: Parameters<typeof fetch>[0], options?: Parameters<typeof fetch>[1]): ReturnType<typeof fetch> => {
 const logical = new URL(String(endpoint));
 const adapter = logical.href === "https://github.com/login/oauth/access_token" ? "github" : logical.href === "https://slack.com/api/oauth.v2.user.access" ? "slack" : undefined;
 if (adapter === undefined) throw new Error("unregistered controlled OAuth endpoint");
 return fetch(new URL(`/${adapter}`,issuer.url),options);
};
const credentialResolver = new SQLMcpCredentialResolver(sql as unknown as McpCredentialSQL, input.masterKeyHex, () => new Date(), issuerFetch);
const notifier = new BridgeAPIManifestChangeNotifier({ address: input.bridgeAddress, tokenPath: input.gatewayTokenPath });
const commitClients = input.bridgeRecoveryAddress === undefined ? [] : [input.bridgeAddress,input.bridgeRecoveryAddress].map(address=>new AgentRuntimeBridgeServiceClient(address,credentials.createInsecure(),bridgeMcpCommitGrpcChannelOptions()));
let commitAttempt = 0;
const routedCommitClient = commitClients.length===0 ? undefined : {
 claimMcpToolResult: commitClients[0]!.claimMcpToolResult.bind(commitClients[0]),
 relinquishMcpToolResult: commitClients[0]!.relinquishMcpToolResult.bind(commitClients[0]),
 commitMcpToolResult: ((...args:Parameters<AgentRuntimeBridgeServiceClient["commitMcpToolResult"]>)=>commitClients[commitAttempt++===0?0:1]!.commitMcpToolResult(...args)) as AgentRuntimeBridgeServiceClient["commitMcpToolResult"],
};
const idempotencyStore = new BridgeAPIMcpToolResultIdempotencyStore({ address: input.bridgeAddress, tokenPath: input.gatewayTokenPath,...(routedCommitClient===undefined?{}:{client:routedCommitClient}),logger:{info:record=>records.push(record)} });
const replicas: ReturnType<typeof makeReplica>[] = [];
const writer = new BridgeAPIEventWriter({ address: input.bridgeAddress, tokenPath: input.runtimeTokenPath });
const metadata = new Metadata(); metadata.set("authorization", "Bearer mcp-production-runtime-token");

function makeReplica() {
	let service!: McpConnectorServiceShell;
	const ownedCredentialResolver = new SQLMcpCredentialResolver(sql as unknown as McpCredentialSQL, input.masterKeyHex, () => new Date(), issuerFetch, undefined, undefined, event => records.push({event:"oauth_refresh_completed",...event}));
	const createClient = () => new McpSDKClient({
		serverResolver, credentialResolver: ownedCredentialResolver, logger:{info:record=>records.push(record),error:record=>records.push(record)},
		onToolsListChanged: async (identity) => { await origins.run("sdk-notification", () => service.handleToolsListChangedNotification(identity)); },
		onConnectionReady: async (identity, tools, options) => { await service.handleConnectionReady(identity, tools, options); },
		createTransport: ({ url, token, requestHeaders }) => {
			const adapter = url.hostname === "api.githubcopilot.com" ? "github" : url.hostname === "mcp.slack.com" ? "slack" : undefined;
			if (adapter === undefined) throw new Error("unregistered logical endpoint reached transport");
			return new StreamableHTTPClientTransport(peers[adapter].url, { ...streamableHTTPTransportOptions({ token, requestHeaders }), fetch: (url, options) => { const headers = new Headers(options?.headers); headers.set("x-fixture-origin", origins.getStore() ?? "sdk-background"); return fetch(url, { ...options, headers }); } });
		},
	});
	let client = createClient();
	let ownerGeneration = 1;
	service = new McpConnectorServiceShell({
		client: { listTools: (identity, options) => origins.run(origins.getStore() ?? "bridge-verification", () => client.listTools(identity, options)), callTool: (identity, options) => origins.run("origin-execution", () => client.callTool(identity, options)), connectionCount: () => client.connectionCount() }, idempotencyStore, manifestChangeNotifier: notifier,
		...(input.executionTimeoutMs===undefined?{}:{executionTimeoutMs:input.executionTimeoutMs}),
		logger: { info: (record) => records.push(record), error: (record) => records.push(record) }, ready: () => true,
		authenticator: { authenticate: async ({ metadata, method }) => {
			const authorization = metadata.get("authorization")[0];
			const token = typeof authorization === "string" ? /^bearer (.+)$/i.exec(authorization)?.[1] : undefined;
			if (method === "/tetral.provider_gateway.v1.McpConnectorService/RunMcpTool" && token === "mcp-production-runtime-token") return { ok: true, serviceAccount: { namespace: "tetral-agent-runtime", name: "agent-runtime", podUid: input.podUid } };
			if (method === "/tetral.provider_gateway.v1.McpConnectorService/ListMcpTools" && token === "mcp-durable-bridge-token") return { ok: true, serviceAccount: { namespace: "tetral-system", name: "bridge", podUid: "bridge-fixture" } };
			return { ok: false, code: "Unauthenticated", message: "controlled workload identity rejected" };
		} },
		runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({ hmacKey: input.bindingKey }),
	});
	return { get client() { return client; }, service, server: createMcpConnectorGrpcServer(service), port: 0, recreate: async () => { const old = client; const oldConnections = old.connectionCount(); await until(() => Object.values(peers).reduce((sum, value) => sum + value.pendingRequests, 0) >= oldConnections); const pendingBefore = Object.values(peers).reduce((sum, value) => sum + value.pendingRequests, 0); await old.closeAll(); await until(() => Object.values(peers).reduce((sum, value) => sum + value.pendingRequests, 0) <= pendingBefore - oldConnections); const pendingAfterDisconnect = Object.values(peers).reduce((sum, value) => sum + value.pendingRequests, 0); if (old.connectionCount() !== 0) throw new Error("old SDK owner retained a connection after join"); client = createClient(); ownerGeneration += 1; return { recreated: true, ownerGeneration, oldConnections: old.connectionCount(), newConnections: client.connectionCount(), retiredConnections: oldConnections, pendingBefore, pendingAfterDisconnect }; } };
}

for (let index = 0; index < 2; index += 1) { const replica = makeReplica(); replica.port = await replica.server.bind("127.0.0.1:0"); replicas.push(replica); }
const control = Bun.serve({ hostname: "127.0.0.1", port: 0, idleTimeout: 0, fetch: async (request) => {
	try { return Response.json(await act(await request.json() as Action)); }
	catch (error) { return Response.json({ failed: true, code: typeof error === "object" && error !== null && "code" in error ? String(error.code) : "fixture_action_failed" }, { status: 500 }); }
} });
process.stdout.write(`${JSON.stringify({ controlAddress: control.url.origin, connectorAddress: `127.0.0.1:${replicas[0]!.port}`, versions: { bun: Bun.version, mcpSDK: sdkPackage.version } })}\n`);

async function act(action: Action): Promise<unknown> {
	const replica = replicas[action.replica ?? 0]!;
	const peer = peers[action.adapter ?? "github"];
	const identity = { workspaceId: action.workspaceId ?? input.workspaceId, sessionId: action.sessionId ?? input.sessionId, mcpServerName: action.serverName ?? `work-${action.adapter ?? "github"}` };
	if (action.kind === "hold" || action.kind === "wait-held" || action.kind === "release") {
 const phase=action.holdPhase??"tools/list",adapter=action.adapter??"github",key=`${adapter}:${phase}`;
 if(action.kind==="hold"){let held:{entered:Promise<void>;release:()=>void};if(phase==="issuer"){const entered=barrier(),release=barrier();issuerHolds.set(adapter,{entered,release});held={entered:entered.promise,release:()=>{issuerHolds.delete(adapter);release.resolve()}}}else held=peer.hold(phase,action.holdCursor);controls.set(key,held);return {held:true,phase}}
 const held=controls.get(key);if(held===undefined)throw new Error("phase barrier not installed");
 if(action.kind==="wait-held"){let timer:ReturnType<typeof setTimeout>|undefined;try{await Promise.race([held.entered,new Promise<void>((_,reject)=>{timer=setTimeout(()=>reject(new Error("owned phase not entered")),5000)})]);return {entered:true,phase}}finally{if(timer!==undefined)clearTimeout(timer)}}
 held.release();controls.delete(key);return {released:true,phase};
 }
 if(action.kind === "wait-resources-closed"){await until(()=>replica.client.pendingWaiterCount()===0&&replica.client.connectionCount()===0&&peer.notificationStreamCount===0);return {waiters:replica.client.pendingWaiterCount(),connections:replica.client.connectionCount(),notificationStreams:peer.notificationStreamCount}}
 if(action.kind === "wait-waiters"){await until(()=>replica.client.pendingWaiterCount()===(action.expectedWaiters??2));return {waiters:replica.client.pendingWaiterCount(),connections:replica.client.connectionCount()}}
 if(action.kind === "cancel"){const controller=executions.get(action.eventId??"");if(controller===undefined)throw new Error("execution not active");controller.abort(new Error("controlled caller cancellation"));return {cancelled:true}}
 if(action.kind === "wait-issuer-cancelled"){await until(()=>issuerState[action.adapter??"github"].cancelled===1);return {cancelled:true}}
 if (action.kind === "issuer") { issuerState[action.adapter ?? "github"].failures = action.issuerFailures ?? []; return {configured:true}; }
	if (action.kind === "observe") return { remainingFaults: Object.fromEntries([...peer.faults].map(([key,value]) => [key,value.length])), counts: peer.counts, requests: peer.requests, records, issuerCalls: issuerState[action.adapter ?? "github"].calls, issuerCancelled: issuerState[action.adapter ?? "github"].cancelled, issuerRecords, connections: replicas.map(value => value.client.connectionCount()) };
	if (action.kind === "configure") { if (action.twoPages !== undefined) peer.pages = action.twoPages ? [{tools:fixtureTools(),nextCursor:"page-two"},{tools:[{...fixtureTools()[0]!,name:"read_extra"}]}] : undefined; if (action.version !== undefined) peer.tools = fixtureTools(action.version); if (action.extraToolName !== undefined) peer.tools = [...peer.tools,{...fixtureTools()[0]!,name:action.extraToolName,description:"Retained other-server control."}]; if (action.toolName !== undefined) peer.tools = peer.tools.map((tool) => ({ ...tool, name: action.toolName! })); if (action.result !== undefined) peer.result = action.result; if (action.faultMethod !== undefined) peer.faults.set(action.faultOrigin === undefined ? action.faultMethod : `${action.faultMethod}\0${action.faultOrigin}`, action.faults ?? []); if (action.credentialToken !== undefined && action.credentialLabel !== undefined) peer.credentials.set(`Bearer ${action.credentialToken}`, action.credentialLabel); return { configured: true }; }
	if (action.kind === "reset") { for (const value of Object.values(peers)) value.resetCounts(); records.length = 0; issuerRecords.length=0; for (const state of Object.values(issuerState)) {state.calls=0;state.cancelled=0;} return { reset: true }; }
	if (action.kind === "oauth-list") { try { const tools=await origins.run("origin-discovery",()=>replica.client.listTools(identity)); return {ok:true,tools,counts:peer.counts,requests:peer.requests,issuerCalls:issuerState[action.adapter??"github"].calls,issuerRecords,records}; } catch(error) {return {ok:false,code:typeof error==="object"&&error!==null&&"code"in error?String(error.code):"sdk-failure",counts:peer.counts,requests:peer.requests,issuerCalls:issuerState[action.adapter??"github"].calls,issuerRecords,records};} }
	if (action.kind === "list") { const tools = await origins.run("origin-discovery", () => replica.client.listTools(identity)); return { tools, counts: peer.counts, requests: peer.requests }; }
	if (action.kind === "discover") {
		const client = new McpConnectorServiceClient(`127.0.0.1:${replica.port}`, credentials.createInsecure());
		const metadata = new Metadata(); metadata.set("authorization", "Bearer mcp-durable-bridge-token");
		try { const response = await new Promise((resolve, reject) => { client.listMcpTools(identity, metadata, { deadline: new Date(Date.now() + 5_000) }, (error, response) => error === null ? resolve(response) : reject(error)); }); return { ok: true, response, counts: Object.fromEntries(Object.entries(peers).map(([name, value]) => [name, value.counts])), requests: peer.requests }; }
		catch (error) { return { ok: false, code: typeof error === "object" && error !== null && "code" in error ? Number(error.code) : -1, counts: Object.fromEntries(Object.entries(peers).map(([name, value]) => [name, value.counts])) }; }
		finally { client.close(); }
	}
	if (action.kind === "resolve") { try { const resolvedServer = await serverResolver.resolve(identity); const resolved = await credentialResolver.resolve({ ...identity, resolvedServer }); return resolved.ok ? { ok: true, credentialId: resolved.credentialId, vaultId: resolved.vaultId } : resolved; } catch { return { ok: false, error: "server_unavailable" }; } }
	if (action.kind === "recreate") return await replica.recreate();
	if (action.kind === "notify") { await peer.waitForNotificationStream(); await until(() => peer.notificationStreamCount >= (action.expectedStreams ?? 1)); await peer.notify(); return { notified: true }; }
	if (action.kind === "shutdown") { setTimeout(() => { void cleanup(); }, 10); return { shutdown: true }; }
	if (action.eventId === undefined || action.callId === undefined || action.nonce === undefined) throw new Error("exact durable tool identity required");
	const deadlineClient = action.callerDeadlineMs === undefined ? undefined : new McpConnectorServiceClient(`127.0.0.1:${replica.port}`,credentials.createInsecure());
 if (deadlineClient !== undefined) {
  const realRun = deadlineClient.runMcpTool.bind(deadlineClient);
  deadlineClient.runMcpTool = ((request, metadata, callback) => {
   records.push({fixturePhase:"deadline-grpc-invoke",toolUseEventId:action.eventId,deadlineMs:action.callerDeadlineMs,callbackType:typeof callback});
   if (typeof callback !== "function") throw new Error("deadline fixture expects Runtime unary callback");
   const ownedMetadata = new Metadata(); for(const key of Object.keys(metadata.getMap()))for(const value of metadata.get(key))ownedMetadata.add(key,value);
   let call;try{call=realRun(request,ownedMetadata,{deadline:new Date(Date.now()+action.callerDeadlineMs!)},(error,response)=>{records.push({fixturePhase:"deadline-grpc-raw-callback",toolUseEventId:action.eventId,grpcCode:error?.code??0});callback(error,response)});}catch(error){records.push({fixturePhase:"deadline-grpc-synchronous-failure",toolUseEventId:action.eventId,name:error instanceof Error?error.name:"unknown",incorrectArguments:error instanceof Error&&error.message==="Incorrect arguments passed"});throw error}
   records.push({fixturePhase:"deadline-grpc-dispatched",toolUseEventId:action.eventId});return call;
  }) as typeof deadlineClient.runMcpTool;
 }
 const runner = new RuntimePodToolRunner({ bridgeAddress: input.bridgeAddress, webAddress: "127.0.0.1:1", mcpConnectorAddress: `127.0.0.1:${replica.port}`, tokenPath: input.runtimeTokenPath,...(deadlineClient===undefined?{}:{mcpConnectorClient:deadlineClient}) });
	const abortController = new AbortController(); executions.set(action.eventId,abortController);
	const runtimeRequest: RuntimeToolExecutionRequest = { ...identity, sessionThreadId: action.threadId ?? input.threadId, bindingId: action.bindingId ?? input.bindingId, bindingGeneration: 1, runtimeProcessId: `process_${input.podUid}`, runtimeBindingToken: action.bindingToken ?? signedBindingToken(identity, action.threadId ?? input.threadId, action.bindingId ?? input.bindingId), targetPodUid: input.podUid, modelRequestId: "mreq_mcp_durable", modelToolCallId: action.callId, modelOrder: 0, toolUseEventId: action.eventId, entry: toolEntry(identity.mcpServerName, action.toolName), input: { nonce: action.nonce }, retainedContextEntries: [], abortSignal: abortController.signal };
	try {
		const result = await runner.runTool(runtimeRequest);
		records.push({fixturePhase:"runtime-run-result",toolUseEventId:action.eventId,resultType:result.type});
		let settlement: unknown;
		if (action.settle !== false && result.type !== "stale_custody") settlement = await writer.settleToolResult({ workspaceId: runtimeRequest.workspaceId, sessionId: runtimeRequest.sessionId, sessionThreadId: runtimeRequest.sessionThreadId, bindingId: runtimeRequest.bindingId, bindingGeneration: runtimeRequest.bindingGeneration, runtimeProcessId: runtimeRequest.runtimeProcessId, targetPodUid: runtimeRequest.targetPodUid, settlement: { toolUseEventId: action.eventId, outcome: runtimeToolSettlement(result) } });
		return { result, settlement, counts: peer.counts, requests: peer.requests, records, issuerCalls: issuerState[action.adapter ?? "github"].calls, issuerCancelled: issuerState[action.adapter ?? "github"].cancelled, issuerRecords };
	} finally { executions.delete(action.eventId);records.push({fixturePhase:"runtime-run-close-enter",toolUseEventId:action.eventId}); await runner.close();records.push({fixturePhase:"runtime-run-close-joined",toolUseEventId:action.eventId}); }
}
function toolEntry(serverName: string, name = "read_echo"): ToolEntry { return { name, definition: { kind: "function", name, description: "Echo a nonce.", inputSchema: { type: "object" } }, inputContract: { kind: "json_object" }, route: { kind: "gateway", operation: "RunMcpTool", mcpServerName: serverName }, formatter: { successShape: "MCP text.", errorShape: "MCP error.", forbiddenFields: ["authorization", "token", "credential"] }, defaultPermissionPolicy: "always_allow", required: false }; }
function signedBindingToken(identity:{workspaceId:string;sessionId:string},threadId:string,bindingId:string): string { const payload = Buffer.from(JSON.stringify({ v: 1, workspace_id: identity.workspaceId, session_id: identity.sessionId, session_thread_id: threadId, binding_id: bindingId, binding_generation: 1, runtime_pod_uid: input.podUid, runtime_process_id: `process_${input.podUid}`, exp: Math.floor(Date.now() / 1_000) + 600 })).toString("base64url"); return `rtbt_v1.${payload}.${createHmac("sha256", input.bindingKey).update(payload).digest("base64url")}`; }
async function cleanup(): Promise<void> {
	for(const held of controls.values()) held.release(); controls.clear();
	const deadline = new Date(Date.now() + 5_000);
	const phase = (name: string) => process.stderr.write(`${JSON.stringify({ phase: name, connections: replicas.map((replica) => replica.client.connectionCount()), waiters: replicas.map((replica) => replica.client.pendingWaiterCount()) })}\n`);
	phase("service-workers");
	await Promise.all(replicas.map((replica) => replica.service.shutdown(deadline)));
	phase("grpc-listeners");
	await Promise.all(replicas.map((replica) => replica.server.shutdown(deadline)));
	phase("sdk-clients");
	await Promise.all(replicas.map((replica) => replica.client.closeAll(deadline)));
	phase("bridge-channels");
	await Promise.all([writer.close(), idempotencyStore.close(), notifier.close()]);
	for(const client of commitClients)client.close();
	phase("protocol-peers");
	for (const peer of Object.values(peers)) await peer.close();
	phase("sql");
	await issuer.stop(true); await sql.close(); await control.stop(); phase("complete"); process.exit(0);
}
