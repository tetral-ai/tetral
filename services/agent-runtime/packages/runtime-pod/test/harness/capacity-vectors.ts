import type { ClientUnaryCall } from "@grpc/grpc-js";
import type { CallOptions } from "@grpc/grpc-js";
import { Metadata } from "@grpc/grpc-js";
import type {
	AcceptSandboxExecutionRequest,
	AgentRuntimeBridgeServiceClient,
	AwaitSandboxExecutionRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { ProviderRequestKind } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimeContextEntry, RuntimeContextPart } from "../../../core/src/contracts/runtime.js";
import { finalizeRuntimeToolOutput } from "../../../core/src/contracts/runtime.js";
import { toGatewayProviderContext } from "../../../core/src/runtime/context-projection.js";
import { estimatedRuntimeContextTokens } from "../../../core/src/thread-loop/compaction.js";
import { assembleProviderCallRequest } from "../../../core/src/thread-loop/provider-request.js";
import type { RuntimeToolExecutionRequest } from "../../../core/src/thread-loop/tool-execution.js";
import type { ToolCatalog } from "../../../core/src/tools/tool-catalog.js";
import { createToolCatalog, lookupToolEntry } from "../../../core/src/tools/tool-catalog.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

// The largest requests the Runtime builds: production projection and request
// assembly at the catalog's context capacity (layer 1; no Gateway).

/** The catalog's largest context window, in estimated tokens. */
export const CapacityTargetTokens = 1_050_000;
/** Share of the request bound the largest request leaves free. */
export const RequiredHeadroomRatio = 0.2;
export const SystemWireMarkers = [
	"TETRAL_CAPACITY_BASE",
	"TETRAL_CAPACITY_AGENT",
	"TETRAL_CAPACITY_MEMORY",
	"TETRAL_CAPACITY_SKILL",
] as const;

/** Carries an exact maximum Read envelope through the real formatter, projection and request assembly. */
export async function maximumReadRequest() {
	const resultJson = maximumReadEnvelope();
	const bridgeClient = {
		acceptSandboxExecution(
			_request: AcceptSandboxExecutionRequest,
			_metadata: Metadata,
			_options: CallOptions,
			callback: (error: Error | null, response?: unknown) => void,
		) {
			callback(null, { committed: {} });
			return noopUnaryCall();
		},
		awaitSandboxExecution(
			_request: AwaitSandboxExecutionRequest,
			_metadata: Metadata,
			_options: CallOptions,
			callback: (error: Error | null, response?: unknown) => void,
		) {
			callback(null, { completed: { resultJson, taskId: "" } });
			return noopUnaryCall();
		},
	} as unknown as AgentRuntimeBridgeServiceClient;
	const runner = new RuntimePodToolRunner({
		bridgeAddress: "unused",
		webAddress: "unused",
		mcpConnectorAddress: "unused",
		tokenPath: "/unused",
		bridgeClient,
		webClient: {} as never,
		mcpConnectorClient: {} as never,
		metadataFactory: async () => new Metadata(),
	});
	const catalog = createToolCatalog({ family: "claude" });
	const entry = lookupToolEntry(catalog, "Read");
	if (entry === undefined) {
		throw new Error("maximum Read proof could not resolve the Read tool");
	}
	const input = {
		file_path: "notes/maximum.txt",
		offset: Number.MAX_SAFE_INTEGER,
		limit: 2000,
	};
	const toolRequest: RuntimeToolExecutionRequest = {
		workspaceId: "wksp_capacity",
		sessionId: "sesn_capacity",
		sessionThreadId: "thr_capacity",
		bindingId: "bind_capacity",
		bindingGeneration: 1,
		runtimeBindingToken: "binding-token",
		targetPodUid: "pod_capacity",
		runtimeProcessId: "process-test",
		modelRequestId: "mreq_maximum_read",
		modelToolCallId: "call_maximum_read",
		modelOrder: 0,
		toolUseEventId: "event_maximum_read",
		entry,
		input,
		retainedContextEntries: [],
		currentModel: { providerId: "openai", modelId: "gpt-5.5" },
		abortSignal: new AbortController().signal,
	};
	const result = await runner.runTool(toolRequest);
	if (result.type !== "completed") {
		throw new Error(`maximum Read proof did not complete: ${result.type}`);
	}
	const providerVisibleOutput = finalizeRuntimeToolOutput(result.output);
	const runtimeParts: RuntimeContextPart[] = [
		{
			type: "tool_call",
			modelToolCallId: "call_maximum_read",
			toolName: "Read",
			canonicalInput: input,
		},
		{
			type: "tool_result",
			modelToolCallId: "call_maximum_read",
			result: {
				type: "completed",
				output: { text: providerVisibleOutput.text },
			},
		},
	];
	const assembled = assembledVector(
		"maximum_read",
		[contextEntry(1, "assistant", runtimeParts)],
		undefined,
		["TETRAL_MAXIMUM_READ_HEAD", "TETRAL_MAXIMUM_READ_TAIL"],
	);
	const projected = toGatewayProviderContext([
		contextEntry(1, "assistant", runtimeParts),
	]);
	if (!projected.ok) {
		throw new Error("maximum Read proof did not project");
	}
	return {
		envelopeBytes: Buffer.byteLength(resultJson, "utf8"),
		request: assembled.request,
		projectedOutputBytes: Buffer.byteLength(
			projected.context[0]?.content.find(
				(item) => item.toolResult !== undefined,
			)?.toolResult?.completed?.outputJson ?? "",
			"utf8",
		),
		outputPreserved:
			projected.context[0]?.content.find(
				(item) => item.toolResult !== undefined,
			)?.toolResult?.completed?.outputJson ===
			JSON.stringify({ text: result.output.text }),
		truncated: result.output.truncated,
	};
}

export async function capacityVectors(): Promise<
	readonly {
		readonly name: string;
		readonly estimatedTokens: number;
		readonly request: ProviderRequest;
		readonly wireMarkers: readonly string[];
		readonly loadedContextBytes?: number;
	}[]
> {
	const ordinaryMarkers = [
		"TETRAL_ORDINARY_FIRST",
		"TETRAL_ORDINARY_LAST",
	] as const;
	const ordinary = fitMessagesToTokenTarget((budget) => [
		textMessage(
			"ordinary_ascii",
			1,
			markedText(Math.floor(budget / 2), ordinaryMarkers[0], "", "x"),
		),
		textMessage(
			"ordinary_utf8",
			2,
			markedText(budget - Math.floor(budget / 2), "", ordinaryMarkers[1], "界"),
		),
	]);
	const toolParts = completedToolParts();
	const toolMarkers = [
		"TETRAL_TOOL_INPUT_FIRST",
		"TETRAL_TOOL_INPUT_LAST",
		"TETRAL_TOOL_OUTPUT_FIRST",
		"TETRAL_TOOL_OUTPUT_LAST",
	] as const;
	const withTools = fitMessagesToTokenTarget((budget) => [
		textMessage("tool_context", 1, "x".repeat(budget)),
		contextEntry(2, "assistant", toolParts),
	]);
	const mcpMarkers = ["TETRAL_MCP_FIRST", "TETRAL_MCP_LAST"] as const;
	const withCatalog = fitMessagesToTokenTarget((budget) => [
		textMessage("catalog_context", 1, "x".repeat(budget)),
	]);
	const fragmentMarkers = [
		"TETRAL_FRAGMENT_FIRST",
		"TETRAL_FRAGMENT_LAST",
	] as const;
	const fragmented = fitMessagesToTokenTarget((budget) => {
		const count = 512;
		const each = Math.floor(budget / count);
		return Array.from({ length: count }, (_, index) => {
			const length = each + (index === count - 1 ? budget - each * count : 0);
			return textMessage(
				`fragment_${index}`,
				index + 1,
				markedText(
					length,
					index === 0 ? fragmentMarkers[0] : "",
					index === count - 1 ? fragmentMarkers[1] : "",
					"x",
				),
			);
		});
	});
	const escapeDenseMarkers = [
		"TETRAL_ESCAPE_OUTPUT_FIRST",
		"TETRAL_ESCAPE_OUTPUT_LAST",
	] as const;
	const escapeDenseToolMessages = Array.from({ length: 48 }, (_, index) =>
		contextEntry(
			index + 1,
			"assistant",
			escapeDenseToolParts(index, 48, escapeDenseMarkers),
		),
	);
	const escapeDenseOutputs = fitMessagesToTokenTarget((budget) => [
		...escapeDenseToolMessages,
		textMessage(
			"escape_dense_filler",
			escapeDenseToolMessages.length + 1,
			"x".repeat(budget),
		),
	]);
	const loadedEscapeDense = await loadCapacityContext(escapeDenseOutputs);

	return [
		assembledVector(
			"ordinary_ascii_and_utf8",
			ordinary,
			undefined,
			ordinaryMarkers,
		),
		assembledVector(
			"escaped_tool_input_and_max_output",
			withTools,
			undefined,
			toolMarkers,
		),
		assembledVector(
			"maximum_mcp_catalog",
			withCatalog,
			maximumMcpCatalog(),
			mcpMarkers,
		),
		assembledVector(
			"fragmented_history",
			fragmented,
			undefined,
			fragmentMarkers,
		),
		{
			...assembledVector(
				"escape_dense_output_history",
				loadedEscapeDense.messages,
				undefined,
				escapeDenseMarkers,
			),
			loadedContextBytes: loadedEscapeDense.contextBytes,
		},
	];
}

async function loadCapacityContext(
	contextEntries: readonly RuntimeContextEntry[],
) {
	const contextJson = JSON.stringify({
		messages: contextEntries,
        currentRequestMessage: null,
		turnFacts: { events: [], internalRepairs: [] },
		thread: {
			parentThreadId: null,
			role: "main",
			visibility: "public",
			taskName: null,
			agentType: "general",
			status: "idle",
		},
	});
	const client = {
		loadContext(
			_request: unknown,
			_metadata: Metadata,
			_options: unknown,
			callback: (error: Error | null, response?: unknown) => void,
		) {
			callback(null, {
				contextJson,
				runtimeBindingToken: "binding-token",
			});
			return noopUnaryCall();
		},
	} as unknown as AgentRuntimeBridgeServiceClient;
	const loader = new BridgeAPIContextLoader({
		address: "unused",
		tokenPath: "/unused",
		client,
		metadataFactory: async () => new Metadata(),
	});
	const loaded = await loader.loadThreadContext({
		workspaceId: "wksp_capacity",
		sessionId: "sesn_capacity",
		sessionThreadId: "thr_capacity",
		bindingId: "bind_capacity",
		bindingGeneration: 1,
		targetPodUid: "pod_capacity",
		runtimeProcessId: "process-test",
	});
	return {
		messages: loaded.messages,
		contextBytes: Buffer.byteLength(contextJson, "utf8"),
	};
}

function assembledVector(
	name: string,
	runtimeContext: readonly RuntimeContextEntry[],
	toolCatalog: ToolCatalog | undefined = undefined,
	wireMarkers: readonly string[] = [],
) {
	const projected = toGatewayProviderContext(runtimeContext);
	if (!projected.ok) {
		throw new Error(`capacity vector ${name} did not project`);
	}
	const assembled = assembleProviderCallRequest({
		identity: {
            threadRole: "main",
            threadVisibility: "public",
			workspaceId: "wksp_capacity",
			sessionId: "sesn_capacity",
			sessionThreadId: "thr_capacity",
			bindingId: "bind_capacity",
			bindingGeneration: 1,
			targetPodUid: "pod_capacity",
			runtimeProcessId: "process-test",
			runtimeBindingToken: "binding-token",
		},
		requestId: `req_${name}`,
		modelRequestId: `mreq_${name}`,
		currentModel: { providerId: "openai", modelId: "gpt-5.5" },
		providerContext: projected.context,
		runtime: {
			requestKind:
				ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
			systemInstructions: `${SystemWireMarkers[0]} Operate within the declared tool and context contract.`,
			agentSystem: `${SystemWireMarkers[1]} Operate as the capacity-proof agent.`,
			memoryStores: [
				{
					memoryStoreId: "mem_capacity",
					name: "capacity-memory",
					access: "read_only",
					instructions: `${SystemWireMarkers[2]} retain the capacity proof marker`,
				},
			],
			skillsIndex: [
				{
					skillId: "skill_capacity",
					skillVersionId: "skv_capacity",
					version: "1",
					name: "capacity-skill",
					description: `${SystemWireMarkers[3]} retain the capacity proof marker`,
					directory: "capacity-skill",
				},
			],
			skillGuidanceDescriptionBudgetBytes: 8 * 1024,
			...(toolCatalog === undefined ? {} : { toolCatalog }),
			maxOutputTokens: 128_000,
			timeoutMs: 30_000,
		},
	});
	if (!assembled.ok) {
		throw new Error(
			`capacity vector ${name} did not assemble: ${assembled.error.code}: ${assembled.error.message}`,
		);
	}
	return {
		name,
		estimatedTokens: estimatedRuntimeContextTokens(runtimeContext),
		request: { ...assembled.request, modelRequestStartEventId: "evt_00000000000000000000000000000001" },
		wireMarkers: [...SystemWireMarkers, ...wireMarkers],
	};
}

function fitMessagesToTokenTarget(
	build: (characterBudget: number) => readonly RuntimeContextEntry[],
): readonly RuntimeContextEntry[] {
	let budget = CapacityTargetTokens * 4;
	let messages = build(budget);
	for (let attempt = 0; attempt < 4; attempt += 1) {
		const estimated = estimatedRuntimeContextTokens(messages);
		const delta = CapacityTargetTokens - estimated;
		if (delta >= 0 && delta <= 2) {
			return messages;
		}
		budget = Math.max(1, budget + delta * 4);
		messages = build(budget);
	}
	return messages;
}

function maximumMcpCatalog() {
	return createToolCatalog({
		family: "gpt",
		mcpManifests: Array.from({ length: 20 }, (_, serverIndex) => ({
			mcpServerName: `capacity-server-${serverIndex}`,
			manifestETag: `etag-${serverIndex}`,
			manifestGeneration: 1,
			tools: Array.from({ length: 4 }, (_, toolIndex) => ({
				name: `capacity_${serverIndex}_${toolIndex}`,
				description: markedText(
					60 * 1024,
					serverIndex === 0 && toolIndex === 0 ? "TETRAL_MCP_FIRST" : "",
					serverIndex === 19 && toolIndex === 3 ? "TETRAL_MCP_LAST" : "",
					"d",
				),
				inputSchema: {
					type: "object",
					properties: { value: { type: "string" } },
				},
			})),
		})),
	});
}

function completedToolParts(): readonly RuntimeContextPart[] {
	const inputLength = 102_400;
	const input = {
		old_text: markedText(inputLength, "TETRAL_TOOL_INPUT_FIRST", "", "\u0001"),
		new_text: markedText(inputLength, "", "TETRAL_TOOL_INPUT_LAST", "\u0001"),
	};
	const outputLength =
		512 * 1024 - Buffer.byteLength(JSON.stringify({ text: "" }), "utf8");
	const output = markedText(
		outputLength,
		"TETRAL_TOOL_OUTPUT_FIRST",
		"TETRAL_TOOL_OUTPUT_LAST",
		"x",
	);
	return [
		{
			type: "tool_call",
			modelToolCallId: "call_capacity",
			toolName: "replace",
			canonicalInput: input,
		},
		{
			type: "tool_result",
			modelToolCallId: "call_capacity",
			result: { type: "completed", output: { text: output } },
		},
	];
}

function escapeDenseToolParts(
	index: number,
	count: number,
	markers: readonly [string, string],
): readonly RuntimeContextPart[] {
	const output = markedText(
		80 * 1024,
		index === 0 ? markers[0] : "",
		index === count - 1 ? markers[1] : "",
		"\u0001",
	);
	return [
		{
			type: "tool_call",
			modelToolCallId: `call_escape_${index}`,
			toolName: "Read",
			canonicalInput: { index },
		},
		{
			type: "tool_result",
			modelToolCallId: `call_escape_${index}`,
			result: { type: "completed", output: { text: output } },
		},
	];
}

function markedText(
	length: number,
	prefix: string,
	suffix: string,
	fill: string,
): string {
	const remaining = length - prefix.length - suffix.length;
	if (remaining < 0) {
		throw new Error("capacity marker exceeds its fixture budget");
	}
	return `${prefix}${fill.repeat(remaining)}${suffix}`;
}

function maximumReadEnvelope(): string {
	const headMarker = "TETRAL_MAXIMUM_READ_HEAD";
	const tailMarker = "TETRAL_MAXIMUM_READ_TAIL";
	const lineBreaks = "\n".repeat(1999);
	const envelope = (content: string): string =>
		JSON.stringify({
			status: "success",
			result: {
				content,
				start_line: Number.MAX_SAFE_INTEGER,
				returned_lines: 2000,
				truncated: false,
				line_truncations: 0,
			},
		});
	const empty = envelope(headMarker + tailMarker + lineBreaks);
	let remaining = 200_000 - Buffer.byteLength(empty, "utf8");
	if (remaining < 0) {
		throw new Error("maximum Read envelope metadata exceeds its byte budget");
	}
	let escapeDense = '\\"'.repeat(Math.floor(remaining / 4));
	remaining %= 4;
	if (remaining >= 2) {
		escapeDense += "\\";
		remaining -= 2;
	}
	if (remaining === 1) {
		escapeDense += "x";
	}
	const result = envelope(headMarker + escapeDense + tailMarker + lineBreaks);
	if (Buffer.byteLength(result, "utf8") !== 200_000) {
		throw new Error(
			"maximum Read envelope does not match the 200000-byte contract",
		);
	}
	return result;
}

function noopUnaryCall(): ClientUnaryCall {
	return { cancel() {} } as ClientUnaryCall;
}

function textMessage(
	_id: string,
	sequence: number,
	text: string,
): RuntimeContextEntry {
	return contextEntry(sequence, "user", [{ type: "text", text }]);
}

function contextEntry(
	messageSequence: number,
	contextKind: "user" | "assistant",
	parts: readonly RuntimeContextPart[],
): RuntimeContextEntry {
	return { messageSequence, contextKind, parts: [...parts] };
}
