/**
 * Routes Runtime Core tool requests to the process or in-process boundary that
 * owns each tool kind. The runner preserves the accepted thread scope and
 * binding on outbound calls, attaches service-account metadata, propagates
 * cancellation to gRPC calls, canonicalizes inputs used for idempotency, and
 * exposes only normalized tool outcomes to Runtime Core. Durable Bridge ACKs
 * precede dependent hot-state changes. A parent/task-name queue serializes child
 * lookup, sends, and lifecycle controls; after resolution a child-id lock guards
 * delivery and lifecycle mutation. Model order sorts entries that are still
 * pending, while an already-running first arrival is not preempted.
 *
 * `buildRuntimePodCommandDependencies` constructs this module and installs its
 * general tool callback plus the separate Sandbox acceptance and result-wait
 * callbacks into ThreadLoop. The runner calls Agent Runtime Bridge for durable
 * sandbox handoff, memory, command, event, and child-thread operations;
 * Provider Gateway for web tools; MCP Connector for MCP tools; and
 * `RuntimeSubAgentRunHost` for local child-thread execution.
 */
import { createHash } from "node:crypto";
import type {
	CallOptions,
	ClientUnaryCall,
	Metadata,
	ServiceError,
} from "@grpc/grpc-js";
import { credentials, status } from "@grpc/grpc-js";
import type {
	RuntimeBoundedText,
	RuntimeJsonValue,
	RuntimeProviderAttachment,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import {
	RuntimeBoundedTextSchema,
	RuntimeFailureSchema,
	SessionEventWriterRetryPolicy,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeThreadAddressState,
	RuntimeThreadStatusState,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import type {
	RuntimeSandboxExecutionAcceptanceResult,
	RuntimeSandboxExecutionRequest,
	RuntimeToolExecutionRequest,
	RuntimeToolExecutionResult,
} from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { selectRecentUserLedTurns } from "@tetral/agent-runtime-core/src/runtime/conversation-turns.js";
import type {
	AcceptSandboxExecutionRequest,
	AcceptSandboxExecutionResponse,
	AdmitChildInterruptRequest,
	AdmitChildInterruptResponse,
	AwaitChildInterruptRequest,
	AwaitChildInterruptResponse,
	AwaitSandboxExecutionRequest,
	AwaitSandboxExecutionResponse,
	AuthorizeWebToolExecutionRequest,
	AuthorizeWebToolExecutionResponse,
	CancelCommandRequest,
	CancelCommandResponse,
	ChildThreadFact,
	CloseChildControlRequest,
	CloseChildControlResponse,
	CreateSubagentThreadRequest,
	CreateSubagentThreadResponse,
	DeliverInterAgentMailRequest,
	DeliverInterAgentMailResponse,
	ListChildThreadsRequest,
	ListChildThreadsResponse,
	MarkChildThreadActiveRequest,
	MarkChildThreadActiveResponse,
	ReadCommandResultRequest,
	ReadCommandResultResponse,
	ResolveChildThreadRequest,
	ResolveChildThreadResponse,
	RunMemoryRequest,
	RunMemoryResponse,
	RuntimeScope,
	SendCommandInputRequest,
	SendCommandInputResponse,
	TransientAttachmentRef,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
	AgentRuntimeBridgeServiceClient,
	ChildControlAction,
	ChildInterruptOutcome,
	ChildLifecycleDisposition,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
	MaxIdBytes,
	MaxTextBytes,
} from "@tetral/gateway-protocol/src/bounds.js";
import type {
	McpAttachmentRef,
	RunMcpToolRequest,
	RunMcpToolResponse,
	RunWebRequest,
	RunWebResponse,
	WebToolInput,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
	McpConnectorServiceClient,
	McpErrorKind,
	McpRetryStatus,
	ProviderGatewayServiceClient,
	RunMcpToolStatus,
	RunWebStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { canonicalRunToolJSON } from "@tetral/gateway-protocol/src/run-tool-canonical-json.js";
import type { ServiceAccountTokenConfig } from "./auth.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import {
	bridgeAttachmentGrpcChannelOptions,
	grpcClientChannelOptions,
	webGrpcChannelOptions,
} from "./bounds.js";
import type { RuntimeSubAgentRunHost } from "./core-hosts.js";

// Durable tool-route transport retries rejoin one accepted identity. This
// delay paces only transport uncertainty; it is not an execution-attempt
// budget and never authorizes the provider operation a second time.
const DURABLE_TOOL_REJOIN_DELAY_MS = 300;
// WEB_SEARCH_REQUESTS_MAX / WEB_FETCH_REQUESTS_MAX bound the per-call
// server_tool_use counters accepted from a RunWebResponse usage block before that
// block is attached to the durable web tool result (webServerToolUse below). They
// mirror the web-connector's own per-call request clamps, so a malformed or
// over-counting usage block is rejected here too rather than inflating
// sessions.usage. Derivation: 32 = 8 operations x 4 search domains (a web search
// fans out at most one backend call per honored domain per operation); 8 = 8
// operations x 1 (a web fetch issues one reader call per operation, no domain
// fan-out). A count above the bound drops the whole usage block (webServerToolUse
// returns undefined) so nothing is attached.
// UPDATE-WITH: webServerToolUse (usage-block validation); services/web-connector/
//   types.go (maxOperations=8, maxSearchDomains=4 — the caps these mirror).
const WEB_SEARCH_REQUESTS_MAX = 32;
const WEB_FETCH_REQUESTS_MAX = 8;
// UPDATE-WITH: services/web-connector/types.go; services/gateway/packages/
// provider-gateway/src/bounds.ts.
const WEB_OPERATIONS_MAX = 8;
const WEB_SEARCH_DOMAINS_MAX = 4;
const WEB_DOMAIN_MAX_BYTES = 253;
const SUBAGENT_TASK_NAME_MAX_BYTES = 128;
const SUBAGENT_FORK_TURNS_MAX = 1_000;
const SUBAGENT_PROMPT_MAX_BYTES = 2 * 1024 * 1024;
const TOOL_RESULT_BOUND_FAILURE =
	"Tool result exceeds the 512 KiB model-visible output limit.";

class ToolResultContractError extends Error {
	constructor() {
		super(TOOL_RESULT_BOUND_FAILURE);
		this.name = "ToolResultContractError";
	}
}

class BridgeToolResultContractError extends Error {
	constructor(operation: string) {
		super(`${operation} returned an invalid result variant`);
		this.name = "BridgeToolResultContractError";
	}
}

type AcceptSandboxExecutionResult =
	| { readonly type: "committed" }
	| { readonly type: "duplicate" }
	| { readonly type: "stale" };

type AwaitSandboxExecutionResult =
	| {
			readonly type: "completed";
			readonly resultJson: string;
			readonly taskId: string;
	  }
	| { readonly type: "stale" };

type ReadCommandResult =
	| { readonly type: "completed"; readonly resultJson: string }
	| { readonly type: "stale" };

type SendCommandInputResult =
	| { readonly type: "committed"; readonly resultJson: string }
	| { readonly type: "duplicate"; readonly resultJson: string }
	| { readonly type: "stale" };

type CancelCommandResult =
	| { readonly type: "committed" }
	| { readonly type: "duplicate" }
	| { readonly type: "stale" };

type AuthorizeWebToolExecutionResult =
	| { readonly type: "authorized" }
	| { readonly type: "stale" };

type RunMemoryResult =
	| { readonly type: "committed"; readonly resultJson: string }
	| { readonly type: "duplicate"; readonly resultJson: string }
	| { readonly type: "stale" };

/**
 * Supplies the service endpoints, current accepted-input scope, and injectable
 * boundary adapters used by {@link RuntimePodToolRunner}.
 *
 * Production construction provides addresses and a service-account token path.
 * Tests may replace clients, metadata creation, and cancellation-aware sleep;
 * `subAgentRunHost` is late-bound because Runtime Core creates that host after
 * the runner is assembled.
 */
export interface RuntimePodToolRunnerOptions {
	readonly bridgeAddress: string;
	readonly webAddress: string;
	readonly mcpConnectorAddress: string;
	readonly tokenPath: string;
	readonly subAgentRunHost?: () => RuntimeSubAgentRunHost | undefined;
	readonly metadataFactory?: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	readonly sleep?: (delayMs: number, abortSignal: AbortSignal) => Promise<void>;
	readonly bridgeClient?: Pick<
		AgentRuntimeBridgeServiceClient,
		| "acceptSandboxExecution"
		| "awaitSandboxExecution"
		| "runMemory"
		| "sendCommandInput"
		| "readCommandResult"
		| "cancelCommand"
		| "authorizeWebToolExecution"
		| "createSubagentThread"
		| "resolveChildThread"
		| "listChildThreads"
		| "deliverInterAgentMail"
		| "admitChildInterrupt"
		| "awaitChildInterrupt"
		| "closeChildControl"
		| "markChildThreadActive"
	>;
	readonly webClient?: Pick<ProviderGatewayServiceClient, "runWeb">;
	readonly mcpConnectorClient?: Pick<McpConnectorServiceClient, "runMcpTool">;
}

interface ChildTaskOperationQueueEntry {
	readonly modelOrder: number;
	readonly sequence: number;
	readonly abortSignal: AbortSignal;
	readonly operation: () => Promise<unknown>;
	readonly resolve: (value: unknown) => void;
	readonly reject: (reason: unknown) => void;
	started: boolean;
	removeAbortListener: () => void;
}

interface ChildTaskOperationQueueState {
	running: boolean;
	readonly entries: ChildTaskOperationQueueEntry[];
}

/**
 * Implements the ThreadLoop tool callback across Bridge, Gateway Pod, and
 * in-process child-thread boundaries.
 *
 * Each instance owns its outbound clients and the ephemeral ordering state for
 * same-task child-agent sends and lifecycle controls. Durable idempotency
 * remains owned by the receiving services and is addressed with stable tool,
 * request, and delivery identities.
 */
export class RuntimePodToolRunner {
	private readonly bridgeClient: Pick<
		AgentRuntimeBridgeServiceClient,
		| "acceptSandboxExecution"
		| "awaitSandboxExecution"
		| "runMemory"
		| "sendCommandInput"
		| "readCommandResult"
		| "cancelCommand"
		| "authorizeWebToolExecution"
		| "createSubagentThread"
		| "resolveChildThread"
		| "listChildThreads"
		| "deliverInterAgentMail"
		| "admitChildInterrupt"
		| "awaitChildInterrupt"
		| "closeChildControl"
		| "markChildThreadActive"
	>;
	private readonly webClient: Pick<ProviderGatewayServiceClient, "runWeb">;
	private readonly mcpConnectorClient: Pick<
		McpConnectorServiceClient,
		"runMcpTool"
	>;
	private readonly metadataFactory: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	private readonly sleep: (
		delayMs: number,
		abortSignal: AbortSignal,
	) => Promise<void>;
	private readonly childOperationLocks = new Map<string, Promise<void>>();
	private readonly childTaskOperationQueues = new Map<
		string,
		ChildTaskOperationQueueState
	>();
	private nextChildTaskOperationSequence = 0;

	/**
	 * Creates a runner with injected adapters when present and dedicated gRPC
	 * clients for the remaining Bridge, web-connector, and MCP boundaries.
	 */
	constructor(private readonly options: RuntimePodToolRunnerOptions) {
		this.bridgeClient =
			options.bridgeClient ??
			new AgentRuntimeBridgeServiceClient(
				options.bridgeAddress,
				credentials.createInsecure(),
				bridgeAttachmentGrpcChannelOptions(),
			);
		this.webClient =
			options.webClient ??
			new ProviderGatewayServiceClient(
				options.webAddress,
				credentials.createInsecure(),
				webGrpcChannelOptions(),
			);
		this.mcpConnectorClient =
			options.mcpConnectorClient ??
			new McpConnectorServiceClient(
				options.mcpConnectorAddress,
				credentials.createInsecure(),
				grpcClientChannelOptions(),
			);
		this.metadataFactory =
			options.metadataFactory ?? buildOutboundBearerMetadata;
		this.sleep = options.sleep ?? sleepWithAbort;
	}

	/**
	 * Executes one ThreadLoop tool request through the route declared by its tool
	 * entry and returns the normalized completed, error, or cancelled outcome.
	 *
	 * Sandbox and memory routes call Bridge, web and MCP routes call Gateway Pod
	 * services, and sub-agent routes coordinate durable Bridge state with the
	 * local child-thread host.
	 */
	async runTool(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		switch (request.entry.route.kind) {
			case "sandbox":
				if (request.entry.route.helperSubcommand === "stdin") {
					return await this.runCommandInput(request);
				}
				return await this.runSandboxTool(request);
			case "bridge":
				return await this.runBridgeTool(request);
			case "gateway":
				return await this.runGatewayTool(request);
			case "subagent":
				return await this.runSubAgentTool(request);
		}
	}

	private async runSandboxTool(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		const acceptance = await this.acceptSandboxExecution(request);
		if (acceptance.type !== "accepted") {
			return acceptance;
		}
		return await this.awaitSandboxExecution(request);
	}

	/** Durably registers one Sandbox execution before any result wait begins. */
	async acceptSandboxExecution(
		request: RuntimeSandboxExecutionRequest,
	): Promise<RuntimeSandboxExecutionAcceptanceResult> {
		const scope = this.scope(request);
		const durableRequest: AcceptSandboxExecutionRequest = {
			scope,
			toolUseEventId: request.toolUseEventId,
		};
		for (;;) {
			try {
				const response = await acceptSandboxExecution(
					this.bridgeClient,
					durableRequest,
					await this.metadata(),
				);
				const result = parseAcceptSandboxExecutionResult(response);
				if (result.type === "stale") {
					return { type: "stale_custody" };
				}
				return { type: "accepted" };
			} catch (error) {
				if (error instanceof BridgeToolResultContractError) {
					return toolFailure(
						request,
						"Bridge returned a malformed sandbox acceptance result.",
						true,
					);
				}
				if (isDurableBridgeRejection(error)) {
					return toolFailure(
						request,
						"Bridge rejected the sandbox tool call.",
						false,
					);
				}
				await this.sleep(
					DURABLE_TOOL_REJOIN_DELAY_MS,
					new AbortController().signal,
				);
			}
		}
	}

	/** Waits for one already-accepted Sandbox execution without re-registering it. */
	async awaitSandboxExecution(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		const scope = this.scope(request);
		const durableRequest: AwaitSandboxExecutionRequest = {
			scope,
			toolUseEventId: request.toolUseEventId,
		};
		for (;;) {
			try {
				const response = await awaitSandboxExecution(
					this.bridgeClient,
					durableRequest,
					await this.metadata(),
					request.abortSignal,
				);
				const result = parseAwaitSandboxExecutionResult(response);
				if (result.type === "stale") {
					return { type: "stale_custody" };
				}
				if (
					request.entry.route.kind === "sandbox" &&
					mediaAttachmentHelper(request.entry.route.helperSubcommand)
				) {
					return await this.mediaResultToAttachment(request, result.resultJson);
				}
				return resultJsonToExecutionResult(
					request,
					withBackgroundTask(result.resultJson, result.taskId),
				);
			} catch (error) {
				if (isToolRouteAborted(error) || request.abortSignal.aborted) {
					return toolCancelled(
						request,
						"Sandbox tool execution was cancelled.",
					);
				}
				if (error instanceof ToolResultContractError) {
					return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
				}
				if (error instanceof BridgeToolResultContractError) {
					return toolFailure(
						request,
						"Bridge returned a malformed sandbox wait result.",
						true,
					);
				}
				if (isDurableBridgeRejection(error)) {
					return { type: "stale_custody" };
				}
				await this.sleep(DURABLE_TOOL_REJOIN_DELAY_MS, request.abortSignal);
			}
		}
	}

	private async mediaResultToAttachment(
		request: RuntimeToolExecutionRequest,
		resultJson: string,
	): Promise<RuntimeToolExecutionResult> {
		const parsed = parseResultJson(resultJson);
		if (!isRecord(parsed) || stringField(parsed, "status") !== "success") {
			return resultJsonToExecutionResult(request, resultJson);
		}
		const result = recordField(parsed, "result");
		const source = isRecord(result) ? result : parsed;
		const mime = stringField(source, "mime");
		const attachmentRef = stringField(source, "attachment_ref");
		if (attachmentRef !== undefined) {
			if (mime === undefined) {
				return toolFailure(
					request,
					`${request.entry.name} returned malformed media attachment metadata.`,
					false,
				);
			}
			const sourcePath =
				stringField(source, "source_path") ??
				stringField(request.input, "path") ??
				stringField(request.input, "file_path") ??
				"image";
			const filename =
				stringField(source, "filename") ?? filenameFromPath(sourcePath);
			const pageRange = stringField(source, "page_range") ?? "";
			const sizeBytes = numberField(source, "size_bytes");
			const lines = [
				"status: success",
				`mime: ${mime}`,
				...(sizeBytes !== undefined ? [`size_bytes: ${sizeBytes}`] : []),
				...(pageRange.length > 0 ? [`page_range: ${pageRange}`] : []),
				`attachment: ${filename}`,
			];
			return {
				type: "completed",
				output: capturedToolText(lines.join("\n")),
				attachments: [
					providerAttachmentFromBridge({
						attachmentRef,
						mime,
						filename,
						sourcePath,
						pageRange,
						detail: stringField(source, "detail") ?? "auto",
					}),
				],
			};
		}
		if (stringField(source, "data_base64") !== undefined) {
			return toolFailure(
				request,
				`${request.entry.name} returned raw media payload after Bridge attachment boundary.`,
				true,
			);
		}
		if (mime === undefined) {
			if (
				request.entry.route.kind === "sandbox" &&
				request.entry.route.helperSubcommand === "read"
			) {
				return resultJsonToExecutionResult(request, resultJson);
			}
			return toolFailure(
				request,
				`${request.entry.name} returned malformed media payload.`,
				false,
			);
		}
		return toolFailure(
			request,
			`${request.entry.name} returned media metadata without an attachment ref.`,
			true,
		);
	}

	private async runCommandInput(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		const scope = this.scope(request);
		const taskId = taskIdFromInput(request.input);
		if (taskId === undefined) {
			return toolFailure(
				request,
				"write_stdin requires a session_id task handle.",
				false,
			);
		}
		const chars = stringField(request.input, "chars");
		const maxOutputTokens =
			positiveIntegerField(request.input, "max_output_tokens") ?? 0;
		const operationId = stableId(
			"req",
			`command-followup:${request.toolUseEventId}`,
		);
		for (;;) {
			try {
				const metadata = await this.metadata();
				if (chars === undefined || chars.length === 0) {
					const response = await readCommandResult(
						this.bridgeClient,
						{
							scope,
							taskId,
							operationId,
							maxOutputTokens,
							toolUseEventId: request.toolUseEventId,
						},
						metadata,
						request.abortSignal,
					);
					const result = parseReadCommandResult(response);
					if (result.type === "stale") {
						return { type: "stale_custody" };
					}
					return resultJsonToExecutionResult(request, result.resultJson);
				}
				const response = await sendCommandInput(
					this.bridgeClient,
					{
						scope,
						taskId,
							operationId,
							maxOutputTokens,
							toolUseEventId: request.toolUseEventId,
					},
					metadata,
					request.abortSignal,
				);
				const result = parseSendCommandInputResult(response);
				if (result.type === "stale") {
					return { type: "stale_custody" };
				}
				return resultJsonToExecutionResult(request, result.resultJson);
			} catch (error) {
				if (isToolRouteAborted(error) || request.abortSignal.aborted) {
					return await this.cancelJoinedBackgroundCommand(
						request,
						scope,
						taskId,
					);
				}
				if (isDurableBridgeRejection(error)) {
					return toolFailure(
						request,
						"Bridge rejected the command operation.",
						false,
					);
				}
				if (error instanceof BridgeToolResultContractError) {
					return toolFailure(
						request,
						"Bridge returned a malformed command result.",
						true,
					);
				}
				await this.sleep(DURABLE_TOOL_REJOIN_DELAY_MS, request.abortSignal);
			}
		}
	}

	private async cancelJoinedBackgroundCommand(
		request: RuntimeToolExecutionRequest,
		scope: RuntimeScope,
		taskId: string,
	): Promise<RuntimeToolExecutionResult> {
		const durableRequest: CancelCommandRequest = {
			scope,
			taskId,
			reason: "runtime_interrupted",
			toolUseEventId: request.toolUseEventId,
			operationId: stableId(
				"req",
				`command-cancel:${request.toolUseEventId}:${taskId}`,
			),
		};
		const cancellationSignal = new AbortController().signal;
		for (
			let attempt = 0;
			attempt < SessionEventWriterRetryPolicy.attempts;
			attempt++
		) {
			try {
				const result = parseCancelCommandResult(
					await cancelCommand(
						this.bridgeClient,
						durableRequest,
						await this.metadata(),
					),
				);
				if (result.type === "stale") {
					return { type: "stale_custody" };
				}
				return toolCancelled(request, "Command task was cancelled.");
			} catch (error) {
				if (error instanceof BridgeToolResultContractError) {
					return toolFailure(
						request,
						"Bridge returned a malformed command cancellation result.",
						true,
					);
				}
				if (isDurableBridgeRejection(error)) {
					return { type: "stale_custody" };
				}
				if (attempt + 1 >= SessionEventWriterRetryPolicy.attempts) {
					return toolFailure(
						request,
						"Command cancellation result is unavailable.",
						true,
					);
				}
				await this.sleep(
					SessionEventWriterRetryPolicy.backoffMs[attempt] ?? 0,
					cancellationSignal,
				);
			}
		}
		return toolFailure(
			request,
			"Command cancellation result is unavailable.",
			true,
		);
	}

	private async runBridgeTool(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		if (request.entry.route.operation !== "RunMemory") {
			return toolFailure(
				request,
				`Bridge tool route ${request.entry.route.operation} is not installed.`,
				false,
			);
		}
		const scope = this.scope(request);
		const runMemoryRequest: RunMemoryRequest = {
			scope,
			toolUseEventId: request.toolUseEventId,
		};
		while (true) {
			try {
				throwIfToolRouteAborted(request.abortSignal);
				const metadata = await this.metadata();
				throwIfToolRouteAborted(request.abortSignal);
				const response = await runMemory(
					this.bridgeClient,
					runMemoryRequest,
					metadata,
					request.abortSignal,
				);
				const result = parseRunMemoryResult(response);
				if (result.type === "stale") {
					return { type: "stale_custody" };
				}
				return resultJsonToExecutionResult(request, result.resultJson);
			} catch (error) {
				if (isToolRouteAborted(error) || request.abortSignal.aborted) {
					return toolCancelled(request, "Memory tool execution was cancelled.");
				}
				if (isDurableBridgeRejection(error)) {
					return toolFailure(
						request,
						"Bridge rejected the memory tool call.",
						false,
					);
				}
				if (error instanceof BridgeToolResultContractError) {
					return toolFailure(
						request,
						"Bridge returned a malformed memory result.",
						true,
					);
				}
				await this.sleep(DURABLE_TOOL_REJOIN_DELAY_MS, request.abortSignal);
			}
		}
	}

	private async runGatewayTool(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		switch (request.entry.route.operation) {
			case "RunWeb":
				return await this.runWebTool(request);
			case "RunMcpTool":
				return await this.runMcpTool(
					request,
					request.entry.route.mcpServerName,
				);
		}
		return toolFailure(
			request,
			`Gateway tool route ${request.entry.route.operation} is not installed.`,
			false,
		);
	}

	private async runWebTool(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		const validatedInput = webInputFromRuntime(request.input);
		if (!validatedInput.ok) {
			return toolFailure(request, validatedInput.reason, false);
		}
		try {
			const authorization = parseAuthorizeWebToolExecutionResult(
				await authorizeWebToolExecution(
					this.bridgeClient,
					{
						scope: this.scope(request),
						toolUseEventId: request.toolUseEventId,
					},
					await this.metadata(),
					request.abortSignal,
				),
			);
			if (authorization.type === "stale") {
				return { type: "stale_custody" };
			}
			throwIfToolRouteAborted(request.abortSignal);
			const response = await runWeb(
				this.webClient,
				{
					workspaceId: request.workspaceId,
					sessionId: request.sessionId,
					sessionThreadId: request.sessionThreadId,
					toolUseEventId: request.toolUseEventId,
					bindingId: request.bindingId,
					bindingGeneration: request.bindingGeneration,
					runtimeBindingToken: request.runtimeBindingToken,
					input: validatedInput.input,
				},
				await this.metadata(),
				request.abortSignal,
			);
			const serverToolUse = webServerToolUse(response);
			if (serverToolUse === undefined) {
				return toolFailure(
					request,
					"Gateway web execution returned malformed usage.",
					true,
				);
			}
			if (response.status === RunWebStatus.RUN_WEB_STATUS_COMPLETED) {
				return {
					type: "completed",
					output: capturedToolText(response.resultText),
					serverToolUse,
				};
			}
			return toolFailure(
				request,
				response.resultText || "Gateway web execution failed.",
				response.status === RunWebStatus.RUN_WEB_STATUS_RUNTIME_ERROR,
				undefined,
				undefined,
				serverToolUse,
			);
		} catch (error) {
			if (isToolRouteAborted(error) || request.abortSignal.aborted) {
				return toolCancelled(request, "Gateway web execution was cancelled.");
			}
			if (error instanceof ToolResultContractError) {
				return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
			}
			if (error instanceof BridgeToolResultContractError) {
				return toolFailure(
					request,
					"Bridge returned a malformed Web authorization result.",
					true,
				);
			}
			if (isDurableBridgeRejection(error)) {
				return { type: "stale_custody" };
			}
			return toolFailure(
				request,
				"Gateway web execution is unavailable.",
				true,
			);
		}
	}

	private async runMcpTool(
		request: RuntimeToolExecutionRequest,
		mcpServerName: string,
	): Promise<RuntimeToolExecutionResult> {
		if (mcpServerName.length === 0) {
			return toolFailure(
				request,
				"MCP tool route is missing mcp_server_name.",
				false,
			);
		}
		try {
			const response = await runMcpTool(
				this.mcpConnectorClient,
				{
					workspaceId: request.workspaceId,
					sessionId: request.sessionId,
					sessionThreadId: request.sessionThreadId,
					toolUseEventId: request.toolUseEventId,
					bindingId: request.bindingId,
					bindingGeneration: request.bindingGeneration,
					runtimeBindingToken: request.runtimeBindingToken,
				},
				await this.metadata(),
				request.abortSignal,
			);
			if (response.errorKind === McpErrorKind.MCP_ERROR_KIND_CUSTODY_LOST) {
				return { type: "stale_custody" };
			}
			if (response.status === RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED) {
				const attachments = response.attachments.map((attachment) =>
					providerAttachmentFromMcp(request, mcpServerName, attachment),
				);
				const completed = completedText(response.resultText);
				if (completed.type !== "completed") {
					return completed;
				}
				return {
					...completed,
					...(attachments.length > 0 ? { attachments } : {}),
				};
			}
			const attachments =
				response.status === RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR
					? response.attachments.map((attachment) =>
							providerAttachmentFromMcp(request, mcpServerName, attachment),
						)
					: [];
			const toolScoped =
				response.status === RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR;
			const message =
				toolScoped && response.resultText.length > 0
					? response.resultText
					: modelVisibleMcpRuntimeFailure(response.errorKind);
			return toolFailure(
				request,
				message,
				false,
				undefined,
				attachments,
				undefined,
			);
		} catch (error) {
			if (isToolRouteAborted(error) || request.abortSignal.aborted) {
				return toolCancelled(request, "MCP tool execution was cancelled.");
			}
			if (error instanceof ToolResultContractError) {
				return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
			}
			return toolFailure(
				request,
				"The MCP tool outcome could not be confirmed. Check the external service before retrying.",
				false,
			);
		}
	}

	private async runSubAgentTool(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		switch (request.entry.route.operation) {
			case "spawn_agent":
				return await this.spawnAgent(request);
			case "send_message":
				return await this.sendMessage(request);
			case "wait_agent":
				return await this.waitAgent(request);
			case "interrupt_agent":
				return await this.interruptAgent(request);
			case "close_agent":
				return await this.closeAgent(request);
			case "resume_agent":
				return await this.resumeAgent(request);
			case "list_agents":
				return await this.listAgents(request);
		}
		return toolFailure(
			request,
			`Sub-agent tool route ${request.entry.route.operation} is not installed.`,
			false,
		);
	}

	private async spawnAgent(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		const taskName = taskNameValue(request.input);
		const prompt = boundedPromptValue(request.input);
		if (taskName === undefined || prompt === undefined) {
			return toolFailure(
				request,
				"spawn_agent requires a task_name of at most 128 UTF-8 bytes and a prompt of at most 2 MiB.",
				false,
			);
		}
		const agentType = subAgentType(request.input);
		if (agentType === undefined) {
			return toolFailure(
				request,
				"spawn_agent agent_type must be general, research, or worker.",
				false,
			);
		}
		const forkTurns = forkTurnsValue(request.input);
		if (forkTurns === undefined) {
			return toolFailure(
				request,
				"spawn_agent fork_turns must be none, all, or an integer string from 1 through 1000.",
				false,
			);
		}
		const currentModel = request.currentModel;
		if (currentModel === undefined) {
			return toolFailure(
				request,
				"spawn_agent requires an inherited current model.",
				true,
			);
		}
		const parentScope = this.scope(request);
		const host = this.options.subAgentRunHost?.();
		if (host === undefined) {
			return toolFailure(
				request,
				"Sub-agent runtime host is unavailable.",
				true,
			);
		}
		let durablyDeliveredThreadId: string | undefined;
		try {
			const metadata = await this.metadata();
			const parentMessageSequences = selectSubagentParentMessageSequences(
				request,
				forkTurns,
			);
			const createRequest: CreateSubagentThreadRequest = {
				scope: parentScope,
				sourceToolUseEventId: request.toolUseEventId,
				taskName,
				agentType,
				initialPrompt: prompt,
				parentMessageSequences,
			};
			const createResponse = await this.replayActorTransport(
				request.abortSignal,
				async () =>
					await createSubagentThread(
						this.bridgeClient,
						createRequest,
						metadata,
						request.abortSignal,
					),
			);
			if (
				!exactlyOneDefined(createResponse.committed, createResponse.duplicate)
			) {
				throw new BridgeToolResultContractError("CreateSubagentThread");
			}
			const childThreadId = (
				createResponse.committed ?? createResponse.duplicate
			)?.childThreadId;
			if (childThreadId === undefined || childThreadId.length === 0) {
				throw new BridgeToolResultContractError("CreateSubagentThread");
			}
			durablyDeliveredThreadId = childThreadId;
			throwIfToolRouteAborted(request.abortSignal);
			await preloadChildThread(
				host,
				request,
				parentScope,
				childThreadId,
				{
					parentThreadId: request.sessionThreadId,
					role: "subagent",
					visibility: "public",
					taskName,
					agentType,
					status: "idle",
				},
			);
			// Creation already committed the opening input. Preload is a hot-path
			// optimization; Queue custody remains the durable delivery owner when
			// this pod cannot host the child immediately.
			return completedText(
				`task_name: ${taskName}\nsession_thread_id: ${childThreadId}\nstatus: delivered`,
			);
		} catch (error) {
			if (durablyDeliveredThreadId !== undefined) {
				return completedText(
					`task_name: ${taskName}\nsession_thread_id: ${durablyDeliveredThreadId}\nstatus: delivered`,
				);
			}
			if (isToolRouteAborted(error) || request.abortSignal.aborted) {
				return toolCancelled(request, "Sub-agent spawn was cancelled.");
			}
			if (error instanceof ToolResultContractError) {
				return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
			}
			if (error instanceof BridgeToolResultContractError) {
				return toolFailure(
					request,
					"Bridge returned a malformed sub-agent spawn result.",
					true,
				);
			}
			if (isGrpcStatus(error, status.ALREADY_EXISTS)) {
				return toolFailure(
					request,
					`Sub-agent task_name ${taskName} already exists under this parent thread.`,
					false,
				);
			}
			return toolFailure(
				request,
				"Sub-agent spawn route is unavailable.",
				true,
			);
		}
	}

	private async sendMessage(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		const taskName = taskNameValue(request.input);
		const messageText = requiredString(request.input, "message");
		if (taskName === undefined || messageText === undefined) {
			return toolFailure(
				request,
				"send_message requires a task_name of at most 128 UTF-8 bytes and message.",
				false,
			);
		}
		const currentModel = request.currentModel;
		if (currentModel === undefined) {
			return toolFailure(
				request,
				"send_message requires an inherited current model.",
				true,
			);
		}
		const parentScope = this.scope(request);
		const host = this.options.subAgentRunHost?.();
		if (host === undefined) {
			return toolFailure(
				request,
				"Sub-agent runtime host is unavailable.",
				true,
			);
		}
		let durablyDeliveredThreadId: string | undefined;
		try {
			return await this.withChildTaskOperationQueue(
				request,
				taskName,
				async () => {
					const metadata = await this.metadata();
					const child = await this.resolveChildByTaskName(
						request,
						parentScope,
						taskName,
						metadata,
					);
					if (child === undefined) {
						return toolFailure(
							request,
							`No sub-agent named ${taskName} exists under this thread.`,
							false,
						);
					}
					return await this.withChildOperationLock(
						request.sessionId,
						child.sessionThreadId,
						request.abortSignal,
						async () => {
							const currentChild =
								(await this.resolveChildById(
									parentScope,
									child.sessionThreadId,
									metadata,
									request.abortSignal,
								)) ?? child;
							if (!childReceivable(currentChild)) {
								return toolFailure(
									request,
									`Sub-agent ${taskName} is not receivable in status ${currentChild.status}.`,
									false,
								);
							}
							throwIfToolRouteAborted(request.abortSignal);
							const preloaded = await preloadChildThread(
								host,
								request,
								parentScope,
								currentChild.sessionThreadId,
								{
									parentThreadId: request.sessionThreadId,
									role: "subagent",
									visibility: "public",
									taskName: currentChild.taskName,
									agentType: currentChild.agentType,
									status: currentChild.status,
								},
							);
							if (!preloaded.ok && preloaded.reason !== "thread_busy") {
								return toolFailure(
									request,
									`Sub-agent context preload failed: ${preloaded.reason}.`,
									preloaded.reason === "local_session_capacity_exceeded",
								);
							}
							const delivery = deliveryIdentity(
								request.toolUseEventId,
								currentChild.sessionThreadId,
								0,
							);
							const deliveryRequest: DeliverInterAgentMailRequest = {
								scope: parentScope,
								deliveryId: delivery.deliveryId,
								targetThreadId: currentChild.sessionThreadId,
								sourceToolUseEventId: request.toolUseEventId,
								content: messageText,
							};
							const delivered = await this.replayActorTransport(
								request.abortSignal,
								async () =>
									await deliverInterAgentMail(
										this.bridgeClient,
										deliveryRequest,
										metadata,
										request.abortSignal,
									),
							);
							if (
								!exactlyOneDefined(delivered.committed, delivered.duplicate)
							) {
								throw new BridgeToolResultContractError(
									"DeliverInterAgentMail",
								);
							}
							durablyDeliveredThreadId = currentChild.sessionThreadId;
							return completedText(
								`task_name: ${taskName}\nsession_thread_id: ${currentChild.sessionThreadId}\nstatus: delivered`,
							);
						},
					);
				},
			);
		} catch (error) {
			if (durablyDeliveredThreadId !== undefined) {
				return completedText(
					`task_name: ${taskName}\nsession_thread_id: ${durablyDeliveredThreadId}\nstatus: delivered`,
				);
			}
			if (isToolRouteAborted(error) || request.abortSignal.aborted) {
				return toolCancelled(request, "Sub-agent send was cancelled.");
			}
			if (error instanceof ToolResultContractError) {
				return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
			}
			if (error instanceof BridgeToolResultContractError) {
				return toolFailure(
					request,
					"Bridge returned a malformed sub-agent delivery result.",
					true,
				);
			}
			return toolFailure(request, "Sub-agent send route is unavailable.", true);
		}
	}

	private async waitAgent(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		const taskName = taskNameValue(request.input);
		if (taskName === undefined) {
			return toolFailure(
				request,
				"wait_agent requires a task_name of at most 128 UTF-8 bytes.",
				false,
			);
		}
		const parentScope = this.scope(request);
		try {
			const child = await this.resolveChildByTaskName(
				request,
				parentScope,
				taskName,
				await this.metadata(),
			);
			if (child === undefined) {
				return toolFailure(
					request,
					`No sub-agent named ${taskName} exists under this thread.`,
					false,
				);
			}
			const timeoutMs = numberField(recordInput(request.input), "timeout_ms");
			const host = this.options.subAgentRunHost?.();
			const hotWait = await host?.waitThread(
				threadControlFromRequest(request, parentScope, child.sessionThreadId),
				timeoutMs,
				request.abortSignal,
			);
			const timedOut =
				hotWait !== undefined &&
				hotWait.ok &&
				hotWait.observed &&
				hotWait.timedOut;
			const settled =
				hotWait !== undefined &&
				hotWait.ok &&
				!timedOut &&
				(hotWait.observed
					? hotWait.status !== undefined &&
						settledSubAgentStatus(hotWait.status)
					: settledSubAgentStatus(child.status));
			const pulled = settled
				? await host?.pullAgentMail?.(
						threadControlFromRequest(
							request,
							parentScope,
							request.sessionThreadId,
						),
						child.sessionThreadId,
					)
				: undefined;
			return completedText(
				[
					`task_name: ${taskName}`,
					`session_thread_id: ${child.sessionThreadId}`,
					`status: ${hotWait !== undefined && hotWait.ok ? (hotWait.status ?? child.status) : child.status}`,
					`timed_out: ${timedOut}`,
					...(pulled === undefined
						? []
						: [`final_message:\n${pulled.finalMessage}`]),
				].join("\n"),
			);
		} catch (error) {
			if (isToolRouteAborted(error) || request.abortSignal.aborted) {
				return toolCancelled(request, "Sub-agent wait was cancelled.");
			}
			if (error instanceof ToolResultContractError) {
				return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
			}
			if (error instanceof BridgeToolResultContractError) {
				return toolFailure(
					request,
					"Bridge returned malformed child-thread facts.",
					true,
				);
			}
			return toolFailure(request, "Sub-agent wait route is unavailable.", true);
		}
	}

	private async interruptAgent(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		const result = await this.withResolvedChild(
			request,
			"interrupt_agent",
			async (parentScope, child, metadata) => {
				return await this.withChildOperationLock(
					request.sessionId,
					child.sessionThreadId,
					request.abortSignal,
					async () => {
						throwIfToolRouteAborted(request.abortSignal);
						const control = await this.admitAndAwaitChildInterrupt(
							request,
							parentScope,
							child.sessionThreadId,
							ChildControlAction.CHILD_CONTROL_ACTION_INTERRUPT,
							metadata,
						);
						if (!control.ok) {
							return toolFailure(request, control.message, control.retryable);
						}
						const rootOutcome = control.targets.find(
							(entry) => entry.childThreadId === child.sessionThreadId,
						)?.outcome;
						const interrupted =
							rootOutcome ===
								ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_COMPLETED ||
							rootOutcome ===
								ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_DUPLICATE;
						const terminalStatus =
							rootOutcome ===
							ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_PRESERVED_FAILED
								? "failed"
								: rootOutcome ===
										ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_PRESERVED_TERMINATED
									? "terminated"
									: undefined;
						return completedText(
							[
								`task_name: ${child.taskName}`,
								`session_thread_id: ${child.sessionThreadId}`,
								`interrupted: ${interrupted}`,
								...(terminalStatus === undefined
									? []
									: [`status: ${terminalStatus}`]),
							].join("\n"),
						);
					},
				);
			},
		);
		return result;
	}

	private async closeAgent(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		return await this.withResolvedChild(
			request,
			"close_agent",
			async (parentScope, child, metadata) => {
				const host = this.options.subAgentRunHost?.();
				if (host === undefined) {
					return toolFailure(
						request,
						"Sub-agent runtime host is unavailable.",
						true,
					);
				}
				return await this.withChildOperationLock(
					request.sessionId,
					child.sessionThreadId,
					request.abortSignal,
					async () => {
						const control = await this.admitAndAwaitChildInterrupt(
							request,
							parentScope,
							child.sessionThreadId,
							ChildControlAction.CHILD_CONTROL_ACTION_CLOSE,
							metadata,
						);
						if (!control.ok) {
							return toolFailure(request, control.message, control.retryable);
						}
						const closeRequest: CloseChildControlRequest = {
							scope: parentScope,
							controlOperationId: control.controlOperationId,
						};
						const response = await this.replayActorTransport(
							request.abortSignal,
							async () =>
								await closeChildControl(
									this.bridgeClient,
									closeRequest,
									metadata,
									request.abortSignal,
								),
						);
						if (
							!exactlyOneDefined(
								response.committed,
								response.duplicate,
								response.stale,
							)
						) {
							return toolFailure(
								request,
								"Sub-agent close result was malformed.",
								true,
							);
						}
						if (response.stale !== undefined) {
							return { type: "stale_custody" };
						}
						const closed = response.committed ?? response.duplicate;
						const children = closed?.children;
						if (children === undefined || children.length === 0) {
							return toolFailure(
								request,
								"Sub-agent close result was malformed.",
								true,
							);
						}
						const rootDisposition = children.find(
							(stamp) => stamp.childThreadId === child.sessionThreadId,
						)?.disposition;
						let rootRunExitOutcome: string | undefined;
						for (const stamp of children) {
							const lifecycle = await host.markThreadClosed(
								threadControlFromRequest(
									request,
									parentScope,
									stamp.childThreadId,
								),
							);
							if (!lifecycle.ok) {
								return toolFailure(
									request,
									`Sub-agent close was not accepted: ${lifecycle.reason}.`,
									true,
								);
							}
							if (stamp.childThreadId === child.sessionThreadId) {
								rootRunExitOutcome = lifecycle.runExitOutcome;
							}
						}
						const rootStatus =
							rootDisposition ===
							ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED
								? "failed"
								: rootDisposition ===
										ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED
									? "terminated"
									: "closed_for_runtime";
						return completedText(
							[
								`task_name: ${child.taskName}`,
								`session_thread_id: ${child.sessionThreadId}`,
								`status: ${rootStatus}`,
								...(rootRunExitOutcome === undefined
									? []
									: [`run_outcome: ${rootRunExitOutcome}`]),
							].join("\n"),
						);
					},
				);
			},
		);
	}

	private async admitAndAwaitChildInterrupt(
		request: RuntimeToolExecutionRequest,
		parentScope: RuntimeScope,
		targetChildThreadId: string,
		action: ChildControlAction,
		metadata: Metadata,
	): Promise<
		| {
				readonly ok: true;
				readonly controlOperationId: string;
				readonly targets: NonNullable<
					AwaitChildInterruptResponse["completed"]
				>["targets"];
		  }
		| {
				readonly ok: false;
				readonly retryable: boolean;
				readonly message: string;
		  }
	> {
		let admitted: AdmitChildInterruptResponse;
		try {
			const admissionRequest: AdmitChildInterruptRequest = {
				scope: parentScope,
				sourceToolUseEventId: request.toolUseEventId,
				targetChildThreadId,
				action,
			};
			admitted = await this.replayActorTransport(
				request.abortSignal,
				async () =>
					await admitChildInterrupt(
						this.bridgeClient,
						admissionRequest,
						metadata,
						request.abortSignal,
					),
			);
		} catch (error) {
			return {
				ok: false,
				retryable: !isDurableBridgeRejection(error),
				message: "Sub-agent interrupt admission failed.",
			};
		}
		if (!exactlyOneDefined(admitted.committed, admitted.duplicate)) {
			return {
				ok: false,
				retryable: true,
				message: "Sub-agent interrupt admission result was malformed.",
			};
		}
		const controlOperationId = (admitted.committed ?? admitted.duplicate)
			?.controlOperationId;
		if (controlOperationId === undefined || controlOperationId.length === 0) {
			return {
				ok: false,
				retryable: true,
				message: "Sub-agent interrupt admission result was malformed.",
			};
		}
		const awaitRequest: AwaitChildInterruptRequest = {
			scope: parentScope,
			controlOperationId,
		};
		while (true) {
			throwIfToolRouteAborted(request.abortSignal);
			try {
				const response = await this.replayActorTransport(
					request.abortSignal,
					async () =>
						await awaitChildInterrupt(
							this.bridgeClient,
							awaitRequest,
							metadata,
							request.abortSignal,
						),
				);
				if (!exactlyOneDefined(response.completed)) {
					return {
						ok: false,
						retryable: true,
						message: "Sub-agent interrupt completion was malformed.",
					};
				}
				const targets = response.completed?.targets;
				if (targets === undefined || targets.length === 0) {
					return {
						ok: false,
						retryable: false,
						message: "Sub-agent interrupt completion was malformed.",
					};
				}
				const failed = targets.find(
					(entry) =>
						entry.outcome ===
						ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_DELIVERY_FAILED,
				);
				if (failed !== undefined) {
					return {
						ok: false,
						retryable: false,
						message: failed.errorCode ?? "Sub-agent interrupt delivery failed.",
					};
				}
				return { ok: true, controlOperationId, targets };
			} catch (error) {
				if (!isGrpcStatus(error, status.DEADLINE_EXCEEDED)) {
					return {
						ok: false,
						retryable: !isDurableBridgeRejection(error),
						message: "Sub-agent interrupt completion is unavailable.",
					};
				}
				await this.sleep(DURABLE_TOOL_REJOIN_DELAY_MS, request.abortSignal);
			}
		}
	}

	// A durable terminal result completes without installing hot state; reopened children require hot residency.
	private async resumeAgent(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		return await this.withResolvedChild(
			request,
			"resume_agent",
			async (parentScope, child, metadata) => {
				const host = this.options.subAgentRunHost?.();
				if (host === undefined) {
					return toolFailure(
						request,
						"Sub-agent runtime host is unavailable.",
						true,
					);
				}
				return await this.withChildOperationLock(
					request.sessionId,
					child.sessionThreadId,
					request.abortSignal,
					async () => {
						const control = threadControlFromRequest(
							request,
							parentScope,
							child.sessionThreadId,
						);
						if (child.status === "failed" || child.status === "terminated") {
							return completedText(
								[
									`task_name: ${child.taskName}`,
									`session_thread_id: ${child.sessionThreadId}`,
									`status: ${child.status}`,
								].join("\n"),
							);
						}
						let preloadedClosed = false;
						if (child.status === "closed_for_runtime") {
							const preloaded = await preloadChildThread(
								host,
								request,
								parentScope,
								child.sessionThreadId,
								{
									parentThreadId: request.sessionThreadId,
									role: "subagent",
									visibility: "public",
									taskName: child.taskName,
									agentType: child.agentType,
									status: "closed_for_runtime",
								},
							);
							if (!preloaded.ok) {
								return toolFailure(
									request,
									`Sub-agent resume context preload failed: ${preloaded.reason}.`,
									true,
								);
							}
							preloadedClosed = true;
						}
						let response: MarkChildThreadActiveResponse;
						try {
							const resumeRequest: MarkChildThreadActiveRequest = {
								scope: parentScope,
								sourceToolUseEventId: request.toolUseEventId,
								targetChildThreadId: child.sessionThreadId,
							};
							response = await this.replayActorTransport(
								request.abortSignal,
								async () =>
									await markChildThreadActive(
										this.bridgeClient,
										resumeRequest,
										metadata,
										request.abortSignal,
									),
							);
						} catch (error) {
							if (preloadedClosed) {
								await host.markThreadClosed(control);
							}
							throw error;
						}
						if (
							!exactlyOneDefined(
								response.committed,
								response.duplicate,
								response.stale,
							)
						) {
							return toolFailure(
								request,
								"Sub-agent resume result was malformed.",
								true,
							);
						}
						if (response.stale !== undefined) {
							if (preloadedClosed) {
								await host.markThreadClosed(control);
							}
							return { type: "stale_custody" };
						}
						const activation = response.committed ?? response.duplicate;
						const disposition = activation?.disposition;
						if (
							disposition !==
								ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_RESUMED &&
							disposition !==
								ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_ALREADY_ACTIVE &&
							disposition !==
								ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED &&
							disposition !==
								ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED
						) {
							if (preloadedClosed) {
								await host.markThreadClosed(control);
							}
							return toolFailure(
								request,
								"Sub-agent resume result was malformed.",
								true,
							);
						}
						if (
							disposition ===
								ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED ||
							disposition ===
								ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED
						) {
							const discarded = await host.markThreadClosed(control);
							if (!discarded.ok) {
								return toolFailure(
									request,
									`Sub-agent terminal resume state could not discard stale residency: ${discarded.reason}.`,
									true,
								);
							}
							const terminalStatus =
								disposition ===
								ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED
									? "failed"
									: "terminated";
							return completedText(
								`task_name: ${child.taskName}\nsession_thread_id: ${child.sessionThreadId}\nstatus: ${terminalStatus}`,
							);
						}
						const resumed =
							disposition ===
							ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_RESUMED;
						// MarkChildThreadActive is the durable resume boundary. A resident
						// quiescent copy follows that result, while a notification that has
						// already created a run slot is further ahead and must not be stopped.
						// Missing or otherwise non-applicable hot state is disposable; the
						// next access reconstructs the durable idle Thread.
						if (resumed) {
							await host.markThreadActive(control).catch(() => undefined);
						}
						const inspected = await host
							.inspectThread(control)
							.catch(() => undefined);
						const activeStatus =
							inspected?.ok === true && inspected.observed
								? (inspected.status ?? "idle")
								: resumed
									? "idle"
									: child.status;
						return completedText(
							`task_name: ${child.taskName}\nsession_thread_id: ${child.sessionThreadId}\nstatus: ${activeStatus}`,
						);
					},
				);
			},
		);
	}

	private async listAgents(
		request: RuntimeToolExecutionRequest,
	): Promise<RuntimeToolExecutionResult> {
		const parentScope = this.scope(request);
		try {
			const response = await listChildThreads(
				this.bridgeClient,
				{
					scope: parentScope,
					parentThreadId: request.sessionThreadId,
				},
				await this.metadata(),
				request.abortSignal,
			);
			const children = parseChildThreadList(response);
			return completedText(
				JSON.stringify(
					{
						agents: children.map((child) => ({
							task_name: child.taskName,
							session_thread_id: child.sessionThreadId,
							status: child.status,
							agent_type: child.agentType,
						})),
					},
					null,
					2,
				),
			);
		} catch (error) {
			if (error instanceof BridgeToolResultContractError) {
				return toolFailure(
					request,
					"Bridge returned malformed child-thread facts.",
					true,
				);
			}
			return toolFailure(request, "Sub-agent list route is unavailable.", true);
		}
	}

	private async withResolvedChild(
		request: RuntimeToolExecutionRequest,
		toolName: string,
		action: (
			parentScope: RuntimeScope,
			child: ChildThreadRecord,
			metadata: Metadata,
		) => Promise<RuntimeToolExecutionResult>,
	): Promise<RuntimeToolExecutionResult> {
		const taskName = taskNameValue(request.input);
		if (taskName === undefined) {
			return toolFailure(
				request,
				`${toolName} requires a task_name of at most 128 UTF-8 bytes.`,
				false,
			);
		}
		const parentScope = this.scope(request);
		try {
			return await this.withChildTaskOperationQueue(
				request,
				taskName,
				async () => {
					const metadata = await this.metadata();
					const child = await this.resolveChildByTaskName(
						request,
						parentScope,
						taskName,
						metadata,
					);
					if (child === undefined) {
						return toolFailure(
							request,
							`No sub-agent named ${taskName} exists under this thread.`,
							false,
						);
					}
					return await action(parentScope, child, metadata);
				},
			);
		} catch (error) {
			if (isToolRouteAborted(error) || request.abortSignal.aborted) {
				return toolCancelled(request, `${toolName} was cancelled.`);
			}
			if (error instanceof BridgeToolResultContractError) {
				return toolFailure(
					request,
					"Bridge returned malformed child-thread facts.",
					true,
				);
			}
			return toolFailure(request, `${toolName} route is unavailable.`, true);
		}
	}

	private async resolveChildByTaskName(
		request: RuntimeToolExecutionRequest,
		parentScope: RuntimeScope,
		taskName: string,
		metadata: Metadata,
	): Promise<ChildThreadRecord | undefined> {
		const listed = await listChildThreads(
			this.bridgeClient,
			{
				scope: parentScope,
				parentThreadId: request.sessionThreadId,
			},
			metadata,
			request.abortSignal,
		);
		const child = parseChildThreadList(listed).find(
			(candidate) =>
				candidate.role === "subagent" && candidate.taskName === taskName,
		);
		if (child === undefined) {
			return undefined;
		}
		const resolved = await resolveChildThread(
			this.bridgeClient,
			{
				scope: parentScope,
				childThreadId: child.sessionThreadId,
			},
			metadata,
			request.abortSignal,
		);
		if (!exactlyOneDefined(resolved.resolved)) {
			throw new BridgeToolResultContractError("ResolveChildThread");
		}
		const resolvedChild = parseChildThread(resolved.resolved?.child);
		if (
			resolvedChild === undefined ||
			resolvedChild.sessionThreadId !== child.sessionThreadId
		) {
			throw new BridgeToolResultContractError("ResolveChildThread");
		}
		return resolvedChild;
	}

	private async resolveChildById(
		parentScope: RuntimeScope,
		childThreadId: string,
		metadata: Metadata,
		abortSignal: AbortSignal,
	): Promise<ChildThreadRecord | undefined> {
		const resolved = await resolveChildThread(
			this.bridgeClient,
			{
				scope: parentScope,
				childThreadId,
			},
			metadata,
			abortSignal,
		);
		if (!exactlyOneDefined(resolved.resolved)) {
			throw new BridgeToolResultContractError("ResolveChildThread");
		}
		const child = parseChildThread(resolved.resolved?.child);
		if (child === undefined) {
			throw new BridgeToolResultContractError("ResolveChildThread");
		}
		return child;
	}

	private async withChildTaskOperationQueue<T>(
		request: RuntimeToolExecutionRequest,
		taskName: string,
		operation: () => Promise<T>,
	): Promise<T> {
		const key = `${request.sessionId}\x1f${request.sessionThreadId}\x1ftask:${taskName}`;
		return await this.withOrderedChildTaskOperation(
			key,
			request.modelOrder,
			request.abortSignal,
			operation,
		);
	}

	private async withOrderedChildTaskOperation<T>(
		key: string,
		modelOrder: number,
		abortSignal: AbortSignal,
		operation: () => Promise<T>,
	): Promise<T> {
		return await new Promise<T>((resolve, reject) => {
			if (abortSignal.aborted) {
				reject(new ToolRouteAborted());
				return;
			}
			let queue = this.childTaskOperationQueues.get(key);
			if (queue === undefined) {
				queue = { running: false, entries: [] };
				this.childTaskOperationQueues.set(key, queue);
			}
			const entry: ChildTaskOperationQueueEntry = {
				modelOrder,
				sequence: this.nextChildTaskOperationSequence,
				abortSignal,
				operation,
				resolve: (value) => resolve(value as T),
				reject,
				started: false,
				removeAbortListener: () => undefined,
			};
			const abort = (): void => {
				if (entry.started) {
					return;
				}
				const currentQueue = this.childTaskOperationQueues.get(key);
				const index = currentQueue?.entries.indexOf(entry) ?? -1;
				if (currentQueue === undefined || index < 0) {
					return;
				}
				currentQueue.entries.splice(index, 1);
				entry.removeAbortListener();
				reject(new ToolRouteAborted());
				if (!currentQueue.running && currentQueue.entries.length === 0) {
					this.childTaskOperationQueues.delete(key);
				}
			};
			abortSignal.addEventListener("abort", abort, { once: true });
			entry.removeAbortListener = () =>
				abortSignal.removeEventListener("abort", abort);
			queue.entries.push(entry);
			this.nextChildTaskOperationSequence += 1;
			this.pumpChildTaskOperationQueue(key);
		});
	}

	private pumpChildTaskOperationQueue(key: string): void {
		const queue = this.childTaskOperationQueues.get(key);
		if (queue === undefined || queue.running) {
			return;
		}
		queue.entries.sort(compareChildTaskOperationEntries);
		const entry = queue.entries.shift();
		if (entry === undefined) {
			this.childTaskOperationQueues.delete(key);
			return;
		}
		queue.running = true;
		entry.started = true;
		void Promise.resolve()
			.then(() => {
				throwIfToolRouteAborted(entry.abortSignal);
				return entry.operation();
			})
			.then(entry.resolve, entry.reject)
			.finally(() => {
				entry.removeAbortListener();
				const currentQueue = this.childTaskOperationQueues.get(key);
				if (currentQueue === undefined) {
					return;
				}
				currentQueue.running = false;
				if (currentQueue.entries.length === 0) {
					this.childTaskOperationQueues.delete(key);
					return;
				}
				this.pumpChildTaskOperationQueue(key);
			});
	}

	private async withChildOperationLock<T>(
		sessionId: string,
		childThreadId: string,
		abortSignal: AbortSignal,
		operation: () => Promise<T>,
	): Promise<T> {
		const key = `${sessionId}\x1f${childThreadId}`;
		const previous = this.childOperationLocks.get(key) ?? Promise.resolve();
		let releaseGate = (): void => undefined;
		const gate = new Promise<void>((resolve) => {
			releaseGate = resolve;
		});
		const next = previous.catch(() => undefined).then(() => gate);
		this.childOperationLocks.set(key, next);
		try {
			await waitForPromiseOrAbort(
				previous.catch(() => undefined),
				abortSignal,
			);
		} catch (error) {
			releaseGate();
			void next.finally(() => {
				if (this.childOperationLocks.get(key) === next) {
					this.childOperationLocks.delete(key);
				}
			});
			throw error;
		}
		try {
			throwIfToolRouteAborted(abortSignal);
			return await operation();
		} finally {
			releaseGate();
			if (this.childOperationLocks.get(key) === next) {
				this.childOperationLocks.delete(key);
			}
		}
	}

	private async replayActorTransport<Response>(
		abortSignal: AbortSignal,
		operation: () => Promise<Response>,
	): Promise<Response> {
		for (;;) {
			throwIfToolRouteAborted(abortSignal);
			try {
				return await operation();
			} catch (error) {
				if (
					isToolRouteAborted(error) ||
					abortSignal.aborted ||
					!isAmbiguousActorTransportFailure(error)
				) {
					throw error;
				}
				await this.sleep(DURABLE_TOOL_REJOIN_DELAY_MS, abortSignal);
			}
		}
	}

	private scope(request: RuntimeSandboxExecutionRequest): RuntimeScope {
		return {
			workspaceId: request.workspaceId,
			sessionId: request.sessionId,
			sessionThreadId: request.sessionThreadId,
			binding: {
				bindingId: request.bindingId,
				bindingGeneration: request.bindingGeneration,
				targetPodUid: request.targetPodUid,
			},
		};
	}

	private async metadata(): Promise<Metadata> {
		return await this.metadataFactory({ tokenPath: this.options.tokenPath });
	}
}

function modelVisibleMcpRuntimeFailure(
	errorKind: McpErrorKind | undefined,
): string {
	switch (errorKind) {
		case McpErrorKind.MCP_ERROR_KIND_IN_FLIGHT:
			return "The MCP tool execution is still in progress. Check the external service before retrying.";
		case McpErrorKind.MCP_ERROR_KIND_COMMIT_FAILED:
			return "The MCP tool may have completed, but its result could not be confirmed. Check the external service before retrying.";
		case McpErrorKind.MCP_ERROR_KIND_CLAIM_CONFLICT:
			return "The MCP tool request conflicts with an existing execution. Check the external service before retrying.";
		case McpErrorKind.MCP_ERROR_KIND_INTERNAL:
		case McpErrorKind.MCP_ERROR_KIND_UNSPECIFIED:
		case McpErrorKind.UNRECOGNIZED:
		case undefined:
			return "The MCP tool outcome could not be confirmed. Check the external service before retrying.";
		default:
			return "MCP tool execution is unavailable.";
	}
}

interface ChildThreadRecord {
	readonly sessionThreadId: string;
	readonly parentThreadId?: string | undefined;
	readonly role: string;
	readonly status: RuntimeThreadStatusState;
	readonly taskName?: string | undefined;
	readonly agentType: "general" | "research" | "worker" | "approval_reviewer";
}

interface DeliveryIdentity {
	readonly deliveryId: string;
	readonly sourceToolUseEventId: string;
}

function completedText(text: string): RuntimeToolExecutionResult {
	return { type: "completed", output: capturedToolText(text) };
}

function capturedToolText(text: string) {
	const parsed = RuntimeBoundedTextSchema.safeParse({ text, truncated: false });
	if (!parsed.success) {
		throw new ToolResultContractError();
	}
	return parsed.data;
}

function stableId(prefix: string, seed: string): string {
	return `${prefix}_${createHash("sha256").update(seed).digest("hex").slice(0, 32)}`;
}

function deliveryIdentity(
	sourceToolUseEventId: string,
	childThreadId: string,
	deliveryIndex: number,
): DeliveryIdentity {
	return {
		deliveryId: stableId(
			"delivery",
			`${sourceToolUseEventId}:${childThreadId}:${deliveryIndex}`,
		),
		sourceToolUseEventId,
	};
}

function compareChildTaskOperationEntries(
	left: ChildTaskOperationQueueEntry,
	right: ChildTaskOperationQueueEntry,
): number {
	if (left.modelOrder !== right.modelOrder) {
		return left.modelOrder - right.modelOrder;
	}
	return left.sequence - right.sequence;
}

async function preloadChildThread(
	host: RuntimeSubAgentRunHost,
	request: RuntimeToolExecutionRequest,
	parentScope: RuntimeScope,
	childThreadId: string,
	thread: Extract<
		RuntimeAcceptedInputState,
		{ readonly kind: "inter_agent_message" }
	>["thread"],
): Promise<Awaited<ReturnType<RuntimeSubAgentRunHost["preloadThread"]>>> {
	return await host.preloadThread({
		...threadControlFromRequest(request, parentScope, childThreadId),
		thread,
	});
}

function requiredString(
	input: RuntimeJsonValue,
	field: string,
): string | undefined {
	const value = stringField(input, field);
	return value !== undefined && value.trim().length > 0
		? value.trim()
		: undefined;
}

function taskNameValue(input: RuntimeJsonValue): string | undefined {
	const value = requiredString(input, "task_name");
	return value !== undefined &&
		new TextEncoder().encode(value).byteLength <= SUBAGENT_TASK_NAME_MAX_BYTES
		? value
		: undefined;
}

function boundedPromptValue(input: RuntimeJsonValue): string | undefined {
	const value = requiredString(input, "prompt");
	return value !== undefined &&
		new TextEncoder().encode(value).byteLength <= SUBAGENT_PROMPT_MAX_BYTES
		? value
		: undefined;
}

function subAgentType(
	input: RuntimeJsonValue,
): "general" | "research" | "worker" | undefined {
	const value = stringField(input, "agent_type") ?? "general";
	return value === "general" || value === "research" || value === "worker"
		? value
		: undefined;
}

function forkTurnsValue(input: RuntimeJsonValue): string | undefined {
	const value = stringField(input, "fork_turns") ?? "all";
	if (value === "none" || value === "all") {
		return value;
	}
	if (value.length === 0 || value[0] === "0") {
		return undefined;
	}
	if (![...value].every((char) => char >= "0" && char <= "9")) {
		return undefined;
	}
	const count = Number(value);
	return Number.isSafeInteger(count) && count <= SUBAGENT_FORK_TURNS_MAX
		? value
		: undefined;
}

function selectSubagentParentMessageSequences(
	request: RuntimeToolExecutionRequest,
	forkTurns: string,
): number[] {
	if (forkTurns === "none") {
		return [];
	}
	const selected =
		forkTurns === "all"
			? request.retainedContextEntries
			: selectRecentUserLedTurns(
					request.retainedContextEntries,
					Number(forkTurns),
				);
	return selected.map((entry) => entry.messageSequence);
}

function parseChildThread(
	child: ChildThreadFact | undefined,
): ChildThreadRecord | undefined {
	if (
		child === undefined ||
		child.childThreadId.length === 0 ||
		(child.role !== "main" &&
			child.role !== "subagent" &&
			child.role !== "approval_reviewer") ||
		(child.visibility !== "public" && child.visibility !== "internal") ||
		!isThreadStatus(child.status) ||
		(child.taskName.length > 0 &&
			new TextEncoder().encode(child.taskName).byteLength >
				SUBAGENT_TASK_NAME_MAX_BYTES) ||
		(child.agentType !== "general" &&
			child.agentType !== "research" &&
			child.agentType !== "worker" &&
			child.agentType !== "approval_reviewer")
	) {
		return undefined;
	}
	return {
		sessionThreadId: child.childThreadId,
		...(child.parentThreadId.length > 0
			? { parentThreadId: child.parentThreadId }
			: {}),
		role: child.role,
		status: child.status,
		...(child.taskName.length > 0 ? { taskName: child.taskName } : {}),
		agentType: child.agentType,
	};
}

function parseChildThreadList(
	response: ListChildThreadsResponse,
): readonly ChildThreadRecord[] {
	if (
		!exactlyOneDefined(response.completed) ||
		response.completed === undefined
	) {
		throw new BridgeToolResultContractError("ListChildThreads");
	}
	const children = response.completed.children.map(parseChildThread);
	if (children.some((child) => child === undefined)) {
		throw new BridgeToolResultContractError("ListChildThreads");
	}
	return children.filter(
		(child): child is ChildThreadRecord => child !== undefined,
	);
}

function childReceivable(child: ChildThreadRecord): boolean {
	return (
		child.status !== "closed_for_runtime" &&
		child.status !== "terminated" &&
		child.status !== "failed"
	);
}

function settledSubAgentStatus(statusValue: RuntimeThreadStatusState): boolean {
	return (
		statusValue === "idle" ||
		statusValue === "failed" ||
		statusValue === "terminated" ||
		statusValue === "closed_for_runtime"
	);
}

function isThreadStatus(value: string): value is RuntimeThreadStatusState {
	return (
		value === "idle" ||
		value === "running" ||
		value === "requires_action" ||
		value === "closed_for_runtime" ||
		value === "rescheduling" ||
		value === "terminated" ||
		value === "failed"
	);
}

function exactlyOneDefined(...values: readonly unknown[]): boolean {
	return values.filter((value) => value !== undefined).length === 1;
}

function threadControlFromRequest(
	request: RuntimeToolExecutionRequest,
	parentScope: RuntimeScope,
	childThreadId: string,
): RuntimeThreadAddressState {
	return {
		workspaceId: parentScope.workspaceId,
		sessionId: parentScope.sessionId,
		sessionThreadId: childThreadId,
		bindingId: parentScope.binding?.bindingId ?? request.bindingId,
		bindingGeneration:
			parentScope.binding?.bindingGeneration ?? request.bindingGeneration,
		targetPodUid: parentScope.binding?.targetPodUid ?? "",
	};
}

function recordInput(
	input: RuntimeJsonValue,
): Record<string, RuntimeJsonValue> {
	return isRecord(input) ? input : {};
}

function parseAcceptSandboxExecutionResult(
	response: AcceptSandboxExecutionResponse,
): AcceptSandboxExecutionResult {
	const variantCount =
		Number(response.committed !== undefined) +
		Number(response.duplicate !== undefined) +
		Number(response.stale !== undefined);
	if (variantCount !== 1) {
		throw new BridgeToolResultContractError("AcceptSandboxExecution");
	}
	if (response.committed !== undefined) {
		return { type: "committed" };
	}
	if (response.duplicate !== undefined) {
		return { type: "duplicate" };
	}
	return { type: "stale" };
}

function parseAwaitSandboxExecutionResult(
	response: AwaitSandboxExecutionResponse,
): AwaitSandboxExecutionResult {
	const variantCount =
		Number(response.completed !== undefined) +
		Number(response.stale !== undefined);
	if (variantCount !== 1) {
		throw new BridgeToolResultContractError("AwaitSandboxExecution");
	}
	if (response.completed !== undefined) {
		return {
			type: "completed",
			resultJson: response.completed.resultJson,
			taskId: response.completed.taskId,
		};
	}
	return { type: "stale" };
}

function parseReadCommandResult(
	response: ReadCommandResultResponse,
): ReadCommandResult {
	const variantCount =
		Number(response.completed !== undefined) +
		Number(response.stale !== undefined);
	if (variantCount !== 1) {
		throw new BridgeToolResultContractError("ReadCommandResult");
	}
	if (response.completed !== undefined) {
		return { type: "completed", resultJson: response.completed.resultJson };
	}
	return { type: "stale" };
}

function parseSendCommandInputResult(
	response: SendCommandInputResponse,
): SendCommandInputResult {
	const variantCount =
		Number(response.committed !== undefined) +
		Number(response.duplicate !== undefined) +
		Number(response.stale !== undefined);
	if (variantCount !== 1) {
		throw new BridgeToolResultContractError("SendCommandInput");
	}
	if (response.committed !== undefined) {
		return { type: "committed", resultJson: response.committed.resultJson };
	}
	if (response.duplicate !== undefined) {
		return { type: "duplicate", resultJson: response.duplicate.resultJson };
	}
	return { type: "stale" };
}

function parseCancelCommandResult(
	response: CancelCommandResponse,
): CancelCommandResult {
	const variantCount =
		Number(response.committed !== undefined) +
		Number(response.duplicate !== undefined) +
		Number(response.stale !== undefined);
	if (variantCount !== 1) {
		throw new BridgeToolResultContractError("CancelCommand");
	}
	if (response.committed !== undefined) {
		return { type: "committed" };
	}
	if (response.duplicate !== undefined) {
		return { type: "duplicate" };
	}
	return { type: "stale" };
}

function parseAuthorizeWebToolExecutionResult(
	response: AuthorizeWebToolExecutionResponse,
): AuthorizeWebToolExecutionResult {
	const variantCount =
		Number(response.authorized !== undefined) +
		Number(response.stale !== undefined);
	if (variantCount !== 1) {
		throw new BridgeToolResultContractError("AuthorizeWebToolExecution");
	}
	return response.authorized !== undefined
		? { type: "authorized" }
		: { type: "stale" };
}

function parseRunMemoryResult(response: RunMemoryResponse): RunMemoryResult {
	const variantCount =
		Number(response.committed !== undefined) +
		Number(response.duplicate !== undefined) +
		Number(response.stale !== undefined);
	if (variantCount !== 1) {
		throw new BridgeToolResultContractError("RunMemory");
	}
	if (response.committed !== undefined) {
		return { type: "committed", resultJson: response.committed.resultJson };
	}
	if (response.duplicate !== undefined) {
		return { type: "duplicate", resultJson: response.duplicate.resultJson };
	}
	return { type: "stale" };
}

function acceptSandboxExecution(
	client: Pick<AgentRuntimeBridgeServiceClient, "acceptSandboxExecution">,
	request: AcceptSandboxExecutionRequest,
	metadata: Metadata,
): Promise<AcceptSandboxExecutionResponse> {
	const options: CallOptions = {
		deadline: Date.now() + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
	};
	return new Promise((resolve, reject) => {
		try {
			client.acceptSandboxExecution(
				request,
				metadata,
				options,
				(error, response) => {
					if (error !== null) {
						reject(error);
						return;
					}
					resolve(response);
				},
			);
		} catch (error) {
			reject(error);
		}
	});
}

function awaitSandboxExecution(
	client: Pick<AgentRuntimeBridgeServiceClient, "awaitSandboxExecution">,
	request: AwaitSandboxExecutionRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<AwaitSandboxExecutionResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.awaitSandboxExecution(unaryRequest, unaryMetadata, callback),
	);
}

function runMemory(
	client: Pick<AgentRuntimeBridgeServiceClient, "runMemory">,
	request: RunMemoryRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<RunMemoryResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.runMemory(unaryRequest, unaryMetadata, callback),
	);
}

function sendCommandInput(
	client: Pick<AgentRuntimeBridgeServiceClient, "sendCommandInput">,
	request: SendCommandInputRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<SendCommandInputResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.sendCommandInput(unaryRequest, unaryMetadata, callback),
	);
}

function readCommandResult(
	client: Pick<AgentRuntimeBridgeServiceClient, "readCommandResult">,
	request: ReadCommandResultRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<ReadCommandResultResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.readCommandResult(unaryRequest, unaryMetadata, callback),
	);
}

function cancelCommand(
	client: Pick<AgentRuntimeBridgeServiceClient, "cancelCommand">,
	request: CancelCommandRequest,
	metadata: Metadata,
): Promise<CancelCommandResponse> {
	const options: CallOptions = {
		deadline: Date.now() + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
	};
	return new Promise((resolve, reject) => {
		try {
			client.cancelCommand(request, metadata, options, (error, response) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			});
		} catch (error) {
			reject(error);
		}
	});
}

function authorizeWebToolExecution(
	client: Pick<AgentRuntimeBridgeServiceClient, "authorizeWebToolExecution">,
	request: AuthorizeWebToolExecutionRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<AuthorizeWebToolExecutionResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.authorizeWebToolExecution(
				unaryRequest,
				unaryMetadata,
				callback,
			),
	);
}

function createSubagentThread(
	client: Pick<AgentRuntimeBridgeServiceClient, "createSubagentThread">,
	request: CreateSubagentThreadRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<CreateSubagentThreadResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.createSubagentThread(unaryRequest, unaryMetadata, callback),
	);
}

function resolveChildThread(
	client: Pick<AgentRuntimeBridgeServiceClient, "resolveChildThread">,
	request: ResolveChildThreadRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<ResolveChildThreadResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.resolveChildThread(unaryRequest, unaryMetadata, callback),
	);
}

function listChildThreads(
	client: Pick<AgentRuntimeBridgeServiceClient, "listChildThreads">,
	request: ListChildThreadsRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<ListChildThreadsResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.listChildThreads(unaryRequest, unaryMetadata, callback),
	);
}

function deliverInterAgentMail(
	client: Pick<AgentRuntimeBridgeServiceClient, "deliverInterAgentMail">,
	request: DeliverInterAgentMailRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<DeliverInterAgentMailResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.deliverInterAgentMail(unaryRequest, unaryMetadata, callback),
	);
}

function admitChildInterrupt(
	client: Pick<AgentRuntimeBridgeServiceClient, "admitChildInterrupt">,
	request: AdmitChildInterruptRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<AdmitChildInterruptResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.admitChildInterrupt(unaryRequest, unaryMetadata, callback),
	);
}

function awaitChildInterrupt(
	client: Pick<AgentRuntimeBridgeServiceClient, "awaitChildInterrupt">,
	request: AwaitChildInterruptRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<AwaitChildInterruptResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.awaitChildInterrupt(unaryRequest, unaryMetadata, callback),
	);
}

function closeChildControl(
	client: Pick<AgentRuntimeBridgeServiceClient, "closeChildControl">,
	request: CloseChildControlRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<CloseChildControlResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.closeChildControl(unaryRequest, unaryMetadata, callback),
	);
}

function markChildThreadActive(
	client: Pick<AgentRuntimeBridgeServiceClient, "markChildThreadActive">,
	request: MarkChildThreadActiveRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<MarkChildThreadActiveResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.markChildThreadActive(unaryRequest, unaryMetadata, callback),
	);
}

function runWeb(
	client: Pick<ProviderGatewayServiceClient, "runWeb">,
	request: RunWebRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<RunWebResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.runWeb(unaryRequest, unaryMetadata, callback),
	);
}

function runMcpTool(
	client: Pick<McpConnectorServiceClient, "runMcpTool">,
	request: RunMcpToolRequest,
	metadata: Metadata,
	abortSignal: AbortSignal,
): Promise<RunMcpToolResponse> {
	return cancellableUnaryCall(
		request,
		metadata,
		abortSignal,
		(unaryRequest, unaryMetadata, callback) =>
			client.runMcpTool(unaryRequest, unaryMetadata, callback),
	);
}

type UnaryCallback<Response> = (
	error: ServiceError | null,
	response: Response,
) => void;
type UnaryInvoker<Request, Response> = (
	request: Request,
	metadata: Metadata,
	callback: UnaryCallback<Response>,
) => ClientUnaryCall;

function unaryCall<Request, Response>(
	request: Request,
	metadata: Metadata,
	invoke: UnaryInvoker<Request, Response>,
): Promise<Response> {
	return new Promise((resolve, reject) => {
		try {
			invoke(
				request,
				metadata,
				(error: ServiceError | null, response: Response) => {
					if (error !== null) {
						reject(error);
						return;
					}
					resolve(response);
				},
			);
		} catch (error) {
			reject(error);
		}
	});
}

class ToolRouteAborted extends Error {
	constructor() {
		super("tool route aborted");
	}
}

function throwIfToolRouteAborted(abortSignal: AbortSignal): void {
	if (abortSignal.aborted) {
		throw new ToolRouteAborted();
	}
}

function sleepWithAbort(
	delayMs: number,
	abortSignal: AbortSignal,
): Promise<void> {
	return new Promise<void>((resolve, reject) => {
		if (abortSignal.aborted) {
			reject(new ToolRouteAborted());
			return;
		}
		let timer: ReturnType<typeof setTimeout> | undefined;
		let settled = false;
		const cleanup = (): void => {
			if (timer !== undefined) {
				clearTimeout(timer);
				timer = undefined;
			}
			abortSignal.removeEventListener("abort", abort);
		};
		const settle = (settlement: () => void): void => {
			if (settled) {
				return;
			}
			settled = true;
			cleanup();
			settlement();
		};
		const abort = (): void => {
			settle(() => reject(new ToolRouteAborted()));
		};
		abortSignal.addEventListener("abort", abort, { once: true });
		timer = setTimeout(() => settle(resolve), delayMs);
	});
}

function waitForPromiseOrAbort<T>(
	promise: Promise<T>,
	abortSignal: AbortSignal,
): Promise<T> {
	return new Promise<T>((resolve, reject) => {
		if (abortSignal.aborted) {
			reject(new ToolRouteAborted());
			return;
		}
		const abort = (): void => {
			reject(new ToolRouteAborted());
		};
		abortSignal.addEventListener("abort", abort, { once: true });
		promise.then(
			(value) => {
				abortSignal.removeEventListener("abort", abort);
				resolve(value);
			},
			(error) => {
				abortSignal.removeEventListener("abort", abort);
				reject(error);
			},
		);
	});
}

function cancellableUnaryCall<Request, Response>(
	request: Request,
	metadata: Metadata,
	abortSignal: AbortSignal,
	invoke: UnaryInvoker<Request, Response>,
): Promise<Response> {
	return new Promise((resolve, reject) => {
		if (abortSignal.aborted) {
			reject(new ToolRouteAborted());
			return;
		}
		let settled = false;
		let call: ClientUnaryCall | undefined;
		const cleanup = (): void => {
			abortSignal.removeEventListener("abort", abort);
		};
		const settle = (settlement: () => void): void => {
			if (settled) {
				return;
			}
			settled = true;
			cleanup();
			settlement();
		};
		const abort = (): void => {
			call?.cancel();
			settle(() => reject(new ToolRouteAborted()));
		};
		abortSignal.addEventListener("abort", abort, { once: true });
		try {
			call = invoke(
				request,
				metadata,
				(error: ServiceError | null, response: Response) => {
					if (error !== null) {
						settle(() => reject(error));
						return;
					}
					settle(() => resolve(response));
				},
			);
		} catch (error) {
			settle(() => reject(error));
		}
	});
}

function isToolRouteAborted(error: unknown): boolean {
	return (
		error instanceof ToolRouteAborted ||
		(typeof error === "object" &&
			error !== null &&
			(error as Partial<ServiceError>).code === status.CANCELLED)
	);
}

function isGrpcStatus(error: unknown, code: status): boolean {
	return (
		typeof error === "object" &&
		error !== null &&
		(error as Partial<ServiceError>).code === code
	);
}

function isDurableBridgeRejection(error: unknown): boolean {
	if (
		typeof error !== "object" ||
		error === null ||
		typeof (error as Partial<ServiceError>).code !== "number"
	) {
		return false;
	}
	switch ((error as Partial<ServiceError>).code) {
		case status.INVALID_ARGUMENT:
		case status.NOT_FOUND:
		case status.ALREADY_EXISTS:
		case status.FAILED_PRECONDITION:
			return true;
		default:
			return false;
	}
}

function isAmbiguousActorTransportFailure(error: unknown): boolean {
	if (
		typeof error !== "object" ||
		error === null ||
		typeof (error as Partial<ServiceError>).code !== "number"
	) {
		return false;
	}
	switch ((error as Partial<ServiceError>).code) {
		case status.UNKNOWN:
		case status.DEADLINE_EXCEEDED:
		case status.ABORTED:
		case status.INTERNAL:
		case status.UNAVAILABLE:
			return true;
		default:
			return false;
	}
}

function resultJsonToExecutionResult(
	request: RuntimeToolExecutionRequest,
	resultJson: string,
): RuntimeToolExecutionResult {
	const parsed = parseResultJson(resultJson);
	if (parsed === undefined) {
		return toolFailure(
			request,
			"Tool route returned malformed result JSON.",
			false,
		);
	}
	const visible = filterVisibleToolResult(request, parsed);
	const activationFailure = sandboxActivationExhaustionResult(request, visible);
	if (activationFailure !== undefined) {
		return activationFailure;
	}
	const status = isRecord(visible) ? stringField(visible, "status") : undefined;
	const retryableValue =
		isRecord(visible) && Object.hasOwn(visible, "retryable")
			? visible.retryable
			: undefined;
	if (retryableValue !== undefined && typeof retryableValue !== "boolean") {
		return toolFailure(
			request,
			"Tool route returned malformed retryability.",
			false,
		);
	}
	if (
		status === "success" ||
		status === "completed" ||
		status === "running" ||
		status === "accepted"
	) {
		const backgroundTask =
			status === "running" ? backgroundTaskFromResult(visible) : undefined;
		let output: RuntimeBoundedText;
		try {
			output = RuntimeBoundedTextSchema.parse({
				...capturedToolText(formatToolResult(request, visible)),
				truncated: resultIsTruncated(parsed),
			});
		} catch (error) {
			if (error instanceof ToolResultContractError) {
				return toolFailure(request, TOOL_RESULT_BOUND_FAILURE, false);
			}
			throw error;
		}
		return {
			type: "completed",
			output,
			...(backgroundTask !== undefined ? { backgroundTask } : {}),
		};
	}
	if (status === "cancelled" || status === "expired") {
		return {
			type: "cancelled",
			error: runtimeFailure(
				request,
				resultErrorMessage(request, visible, `Tool route ${status}.`),
				false,
			),
		};
	}
	return {
		type: "error",
		error: runtimeFailure(
			request,
			resultErrorMessage(request, visible, "Tool route failed."),
			typeof retryableValue === "boolean"
				? retryableValue
				: status === "runtime_error" || status === "failed",
		),
	};
}

// Activation exhaustion is a private lifecycle settlement. Every Sandbox Tool
// family converges here before Runtime failure and public Tool Result creation,
// where Runtime replaces Sandbox, route, attempt, and provider detail with one
// provider-neutral model-visible failure text.
function sandboxActivationExhaustionResult(
	request: RuntimeToolExecutionRequest,
	parsed: RuntimeJsonValue,
): RuntimeToolExecutionResult | undefined {
	if (request.entry.route.kind !== "sandbox" || !isRecord(parsed)) {
		return undefined;
	}
	const error = recordField(parsed, "error");
	if (
		!isRecord(error) ||
		stringField(error, "kind") !== "sandbox_activation_attempts_exhausted"
	) {
		return undefined;
	}
	return toolFailure(
		request,
		"The requested operation could not be completed.",
		false,
	);
}

function backgroundTaskFromResult(
	parsed: RuntimeJsonValue,
): { readonly taskId: string } | undefined {
	if (!isRecord(parsed)) {
		return undefined;
	}
	const result = recordField(parsed, "result");
	const source = isRecord(result) ? result : parsed;
	const taskId =
		stringField(source, "task_id") ?? stringField(parsed, "task_id");
	if (taskId === undefined || taskId.length === 0) {
		return undefined;
	}
	return { taskId };
}

function parseResultJson(resultJson: string): RuntimeJsonValue | undefined {
	try {
		const parsed = JSON.parse(
			resultJson.length === 0 ? "{}" : resultJson,
		) as unknown;
		return isRuntimeJsonValue(parsed) ? parsed : undefined;
	} catch {
		return undefined;
	}
}

function withBackgroundTask(resultJson: string, taskId: string): string {
	if (taskId.length === 0) {
		return resultJson;
	}
	const parsed = parseResultJson(resultJson);
	if (!isRecord(parsed) || stringField(parsed, "task_id") !== undefined) {
		return resultJson;
	}
	return stableJsonStringify({ ...parsed, task_id: taskId });
}

function resultErrorMessage(
	request: RuntimeToolExecutionRequest,
	parsed: RuntimeJsonValue,
	fallback: string,
): string {
	let message = fallback;
	if (isRecord(parsed)) {
		const topLevelMessage = stringField(parsed, "message");
		if (topLevelMessage !== undefined && topLevelMessage.length > 0) {
			message = topLevelMessage;
		}
		const error = parsed.error;
		if (isRecord(error)) {
			const errorMessage = stringField(error, "message");
			if (errorMessage !== undefined && errorMessage.length > 0) {
				message = errorMessage;
			}
			const kind = stringField(error, "kind");
			if (message === fallback && kind !== undefined && kind.length > 0) {
				message = `Tool route failed with ${kind}.`;
			}
		}
		const errorCode = stringField(parsed, "error_code");
		if (
			message === fallback &&
			errorCode !== undefined &&
			errorCode.length > 0
		) {
			message = `Tool route failed with ${errorCode}.`;
		}
	}
	const partial = formatToolResult(request, parsed);
	return partial.length > 0
		? `${message}\n\nPartial result:\n${partial}`
		: message;
}

function formatToolResult(
	request: RuntimeToolExecutionRequest,
	parsed: RuntimeJsonValue,
): string {
	if (isRecord(parsed)) {
		const resultText =
			stringField(parsed, "result_text") ?? stringField(parsed, "resultText");
		if (resultText !== undefined) {
			return resultText;
		}
		if (
			request.entry.route.kind === "sandbox" &&
			(request.entry.route.helperSubcommand === "exec" ||
				request.entry.route.helperSubcommand === "stdin")
		) {
			return formatCommandEnvelope(parsed);
		}
		if (
			request.entry.route.kind === "bridge" &&
			request.entry.route.operation === "RunMemory"
		) {
			return formatMemoryEnvelope(parsed);
		}
		if (request.entry.route.kind === "sandbox") {
			return formatSandboxEnvelope(
				parsed,
				request.entry.route.helperSubcommand === "read",
			);
		}
	}
	return visibleJsonText(parsed);
}

function formatCommandEnvelope(
	parsed: Record<string, RuntimeJsonValue>,
): string {
	const result = recordField(parsed, "result");
	const source = isRecord(result) ? result : parsed;
	const lines = [`status: ${stringField(parsed, "status") ?? "unknown"}`];
	const taskId =
		stringField(source, "task_id") ?? stringField(parsed, "task_id");
	if (taskId !== undefined) {
		lines.push(`session_id: ${taskId}`);
	}
	const exitCode = numberField(source, "exit_code");
	if (exitCode !== undefined) {
		lines.push(`exit_code: ${exitCode}`);
	}
	const signal = stringField(source, "signal");
	if (signal !== undefined) {
		lines.push(`signal: ${signal}`);
	}
	appendStream(lines, "stdout", recordField(source, "stdout"));
	appendStream(lines, "stderr", recordField(source, "stderr"));
	const text = stringField(source, "text");
	if (text !== undefined) {
		lines.push("output:");
		lines.push(text);
	}
	return lines.join("\n");
}

function formatMemoryEnvelope(
	parsed: Record<string, RuntimeJsonValue>,
): string {
	const lines = [`status: ${stringField(parsed, "status") ?? "unknown"}`];
	for (const field of [
		"action",
		"path",
		"new_path",
		"summary",
		"message",
	] as const) {
		const value = stringField(parsed, field);
		if (value !== undefined && value.length > 0) {
			lines.push(`${field}: ${value}`);
		}
	}
	for (const field of [
		"error_code",
		"reread_required",
		"projection_refreshed",
		"retryable",
	] as const) {
		const value = parsed[field];
		if (typeof value === "string" || typeof value === "boolean") {
			lines.push(`${field}: ${String(value)}`);
		}
	}
	const conflictingPaths = parsed.conflicting_paths;
	if (Array.isArray(conflictingPaths)) {
		lines.push(`conflicting_paths: ${JSON.stringify(conflictingPaths)}`);
	}
	return lines.length > 1 ? lines.join("\n") : visibleJsonText(parsed);
}

function formatSandboxEnvelope(
	parsed: Record<string, RuntimeJsonValue>,
	numberReadContent: boolean,
): string {
	const result = recordField(parsed, "result");
	const source = isRecord(result) ? result : parsed;
	const lines = [`status: ${stringField(parsed, "status") ?? "unknown"}`];
	if (numberReadContent) {
		appendReadContent(lines, source);
	}
	for (const field of [
		"path",
		"file_path",
		"content",
		"message",
		"summary",
		"next_offset",
		"total_lines",
	] as const) {
		if (numberReadContent && field === "content") {
			continue;
		}
		const value = source[field];
		if (typeof value === "string" && value.length > 0) {
			lines.push(`${field}: ${value}`);
		} else if (typeof value === "number") {
			lines.push(`${field}: ${value}`);
		}
	}
	const rendered =
		lines.length > 1 ? lines.join("\n") : visibleJsonText(source);
	const truncationLines: string[] = [];
	if (resultIsTruncated(parsed)) {
		truncationLines.push("truncated: true");
	}
	const lineTruncations = numberField(source, "line_truncations");
	if (lineTruncations !== undefined && lineTruncations > 0) {
		truncationLines.push(`line_truncations: ${lineTruncations}`);
	}
	return truncationLines.length > 0
		? `${rendered}\n${truncationLines.join("\n")}`
		: rendered;
}

function appendReadContent(
	lines: string[],
	source: Record<string, RuntimeJsonValue>,
): void {
	const content = stringField(source, "content");
	if (content === undefined) {
		return;
	}
	lines.push("content:");
	const withoutTrailingNewline = content.endsWith("\n")
		? content.slice(0, -1)
		: content;
	const returnedLines = numberField(source, "returned_lines") ?? 0;
	let contentLines: string[];
	if (withoutTrailingNewline.length > 0) {
		contentLines = withoutTrailingNewline.split("\n");
	} else if (returnedLines > 0) {
		contentLines = [""];
	} else {
		contentLines = [];
	}
	if (contentLines.length === 0) {
		return;
	}
	const startLine = numberField(source, "start_line");
	if (startLine === undefined) {
		lines.push(...contentLines);
		return;
	}
	const firstLine = BigInt(startLine);
	for (let index = 0; index < contentLines.length; index++) {
		const lineNumber = (firstLine + BigInt(index)).toString().padStart(6, " ");
		lines.push(`${lineNumber}\t${contentLines[index] ?? ""}`);
	}
}

function resultIsTruncated(value: RuntimeJsonValue): boolean {
	if (!isRecord(value)) {
		return false;
	}
	const result = recordField(value, "result");
	return (
		value.truncated === true || (isRecord(result) && result.truncated === true)
	);
}

function appendStream(
	lines: string[],
	label: "stdout" | "stderr",
	value: unknown,
): void {
	if (!isRecord(value)) {
		return;
	}
	const text = stringField(value, "text");
	if (text === undefined || text.length === 0) {
		return;
	}
	lines.push(`${label}:`);
	lines.push(text);
	if (value.truncated === true) {
		lines.push(`${label}_truncated: true`);
	}
}

function visibleJsonText(value: RuntimeJsonValue): string {
	return JSON.stringify(filterVisibleJson(value), null, 2);
}

function filterVisibleToolResult(
	request: RuntimeToolExecutionRequest,
	value: RuntimeJsonValue,
): RuntimeJsonValue {
	const forbidden = new Set(
		request.entry.formatter.forbiddenFields.map(normalizedResultField),
	);
	return filterVisibleJson(value, forbidden);
}

function filterVisibleJson(
	value: RuntimeJsonValue,
	declaredForbidden: ReadonlySet<string> = new Set(),
): RuntimeJsonValue {
	if (Array.isArray(value)) {
		return value.map((item) => filterVisibleJson(item, declaredForbidden));
	}
	if (!isRecord(value)) {
		return value;
	}
	return Object.fromEntries(
		Object.entries(value)
			.filter(
				([key]) =>
					!forbiddenResultField(key) &&
					!declaredForbidden.has(normalizedResultField(key)),
			)
			.map(([key, child]) => [
				key,
				filterVisibleJson(child, declaredForbidden),
			]),
	);
}

function normalizedResultField(key: string): string {
	return key.replace(/[^a-z0-9]/giu, "").toLowerCase();
}

function forbiddenResultField(key: string): boolean {
	const normalized = key.toLowerCase();
	return (
		normalized.includes("payload") ||
		normalized.includes("provider") ||
		normalized.includes("daytona") ||
		normalized.includes("sandbox_process") ||
		normalized.includes("sandbox_driver") ||
		normalized.includes("credential") ||
		normalized === "data_base64" ||
		normalized.includes("base64") ||
		normalized === "binding_id" ||
		normalized === "bindingid" ||
		normalized === "runtimebindingtoken"
	);
}

function webServerToolUse(
	response: RunWebResponse,
):
	| { readonly webSearchRequests: number; readonly webFetchRequests: number }
	| undefined {
	const usage = response.usage;
	if (
		usage === undefined ||
		!Number.isSafeInteger(usage.webSearchRequests) ||
		usage.webSearchRequests < 0 ||
		usage.webSearchRequests > WEB_SEARCH_REQUESTS_MAX ||
		!Number.isSafeInteger(usage.webFetchRequests) ||
		usage.webFetchRequests < 0 ||
		usage.webFetchRequests > WEB_FETCH_REQUESTS_MAX
	) {
		return undefined;
	}
	return {
		webSearchRequests: usage.webSearchRequests,
		webFetchRequests: usage.webFetchRequests,
	};
}

function toolFailure(
	request: RuntimeSandboxExecutionRequest,
	message: string,
	retryable: boolean,
	retryStatus?: ReturnType<typeof runtimeRetryStatusFromMcp>,
	attachments?: readonly RuntimeProviderAttachment[],
	serverToolUse?: {
		readonly webSearchRequests: number;
		readonly webFetchRequests: number;
	},
): Extract<RuntimeToolExecutionResult, { readonly type: "error" }> {
	return {
		type: "error",
		error: runtimeFailure(request, message, retryable, retryStatus),
		...(attachments !== undefined && attachments.length > 0
			? { attachments }
			: {}),
		...(serverToolUse !== undefined ? { serverToolUse } : {}),
	};
}

function toolCancelled(
	request: RuntimeToolExecutionRequest,
	message: string,
): RuntimeToolExecutionResult {
	return {
		type: "cancelled",
		error: runtimeFailure(request, message, false),
	};
}

function runtimeFailure(
	request: RuntimeSandboxExecutionRequest,
	message: string,
	retryable: boolean,
	retryStatus?: ReturnType<typeof runtimeRetryStatusFromMcp>,
) {
	return RuntimeFailureSchema.parse({
		type: "runtime",
		code: "runtime_invalid_sequence",
		message,
		retryable,
		fatal: false,
		sessionId: request.sessionId,
		...(retryStatus !== undefined ? { retryStatus } : {}),
	});
}

function runtimeRetryStatusFromMcp(retryStatus: McpRetryStatus | undefined) {
	switch (retryStatus) {
		case McpRetryStatus.MCP_RETRY_STATUS_RETRYING:
			return { type: "retrying" as const, attempt: 1 };
		case McpRetryStatus.MCP_RETRY_STATUS_EXHAUSTED:
			return { type: "exhausted" as const };
		case McpRetryStatus.MCP_RETRY_STATUS_TERMINAL:
			return { type: "terminal" as const };
		case McpRetryStatus.MCP_RETRY_STATUS_UNSPECIFIED:
		case undefined:
			return undefined;
		case McpRetryStatus.UNRECOGNIZED:
			return undefined;
	}
}

function providerAttachmentFromBridge(
	attachment: TransientAttachmentRef,
): RuntimeProviderAttachment {
	return {
		transient: {
			attachmentRef: attachment.attachmentRef,
			sourcePath: attachment.sourcePath,
			pageRange: attachment.pageRange,
			detail: attachment.detail,
		},
		fileBacked: undefined,
		mime: attachment.mime,
		filename: attachment.filename,
	};
}

function providerAttachmentFromMcp(
	request: RuntimeToolExecutionRequest,
	mcpServerName: string,
	attachment: McpAttachmentRef,
): RuntimeProviderAttachment {
	return {
		transient: {
			attachmentRef: attachment.attachmentRef,
			sourcePath: `mcp:${mcpServerName}/${attachment.suggestedFilename}`,
			pageRange: "",
			detail: "auto",
		},
		fileBacked: undefined,
		mime: attachment.mime,
		filename: attachment.suggestedFilename,
	};
}

function mediaAttachmentHelper(helperSubcommand: string): boolean {
	return helperSubcommand === "view_image" || helperSubcommand === "read";
}

function filenameFromPath(value: string): string {
	const parts = value.split(/[\\/]/).filter((part) => part.length > 0);
	return parts.at(-1) ?? "image";
}

function stableJsonStringify(value: RuntimeJsonValue): string {
	return canonicalRunToolJSON(JSON.stringify(value));
}

function taskIdFromInput(input: RuntimeJsonValue): string | undefined {
	return stringField(input, "session_id");
}

function stringField(
	input: RuntimeJsonValue,
	field: string,
): string | undefined {
	return isRecord(input) && typeof input[field] === "string"
		? input[field]
		: undefined;
}

function positiveIntegerField(
	input: RuntimeJsonValue,
	field: string,
): number | undefined {
	const value = isRecord(input) ? input[field] : undefined;
	return typeof value === "number" && Number.isSafeInteger(value) && value > 0
		? value
		: undefined;
}

function recordField(
	input: RuntimeJsonValue,
	field: string,
): RuntimeJsonValue | undefined {
	return isRecord(input) ? input[field] : undefined;
}

type WebInputValidation =
	| { readonly ok: true; readonly input: WebToolInput }
	| { readonly ok: false; readonly reason: string };

function webInputFromRuntime(input: RuntimeJsonValue): WebInputValidation {
	if (!isRecord(input)) {
		return { ok: false, reason: "web input must be an object." };
	}
	const rawSearchQuery = optionalArrayField(input, "search_query");
	if (rawSearchQuery === undefined) {
		return { ok: false, reason: "web search_query must be an array." };
	}
	const rawOpen = optionalArrayField(input, "open");
	if (rawOpen === undefined) {
		return { ok: false, reason: "web open must be an array." };
	}
	const rawFind = optionalArrayField(input, "find");
	if (rawFind === undefined) {
		return { ok: false, reason: "web find must be an array." };
	}
	const operationCount =
		rawSearchQuery.length + rawOpen.length + rawFind.length;
	if (operationCount === 0) {
		return {
			ok: false,
			reason: "web requires at least one search, open, or find operation.",
		};
	}
	if (operationCount > WEB_OPERATIONS_MAX) {
		return {
			ok: false,
			reason: `web accepts at most ${WEB_OPERATIONS_MAX} operations.`,
		};
	}

	const searchQuery: Array<{ readonly q: string; readonly domains: string[] }> =
		[];
	for (const item of rawSearchQuery) {
		if (!isRecord(item)) {
			return {
				ok: false,
				reason: "web search_query contains an invalid operation.",
			};
		}
		const q = stringField(item, "q");
		const rawDomains = optionalArrayField(item, "domains");
		if (
			rawDomains === undefined ||
			q === undefined ||
			invalidUtf8Bytes(q, MaxTextBytes) ||
			rawDomains.length > WEB_SEARCH_DOMAINS_MAX ||
			rawDomains.some(
				(domain) =>
					typeof domain !== "string" ||
					invalidUtf8Bytes(domain, WEB_DOMAIN_MAX_BYTES),
			)
		) {
			return {
				ok: false,
				reason: "web search_query contains an invalid operation.",
			};
		}
		const domains = rawDomains.filter(
			(domain): domain is string => typeof domain === "string",
		);
		searchQuery.push({ q, domains });
	}

	const open: Array<{
		readonly url?: string;
		readonly refId?: string;
		readonly lineno?: number;
	}> = [];
	for (const item of rawOpen) {
		if (!isRecord(item)) {
			return { ok: false, reason: "web open contains an invalid operation." };
		}
		const url = stringField(item, "url");
		const refId = stringField(item, "ref_id");
		const lineno = numberField(item, "lineno");
		const hasUrl = url !== undefined && url !== "";
		const hasRef = refId !== undefined && refId !== "";
		if (
			hasUrl === hasRef ||
			(url !== undefined && invalidUtf8Bytes(url, MaxTextBytes)) ||
			(refId !== undefined && invalidUtf8Bytes(refId, MaxIdBytes)) ||
			(lineno !== undefined && lineno < 1)
		) {
			return { ok: false, reason: "web open contains an invalid operation." };
		}
		open.push({
			...(url !== undefined ? { url } : {}),
			...(refId !== undefined ? { refId } : {}),
			...(lineno !== undefined ? { lineno } : {}),
		});
	}

	const find: Array<{ readonly refId: string; readonly pattern: string }> = [];
	for (const item of rawFind) {
		if (!isRecord(item)) {
			return { ok: false, reason: "web find contains an invalid operation." };
		}
		const refId = stringField(item, "ref_id");
		const pattern = stringField(item, "pattern");
		if (
			refId === undefined ||
			pattern === undefined ||
			invalidUtf8Bytes(refId, MaxIdBytes) ||
			encodedBytes(pattern) > MaxTextBytes
		) {
			return { ok: false, reason: "web find contains an invalid operation." };
		}
		find.push({ refId, pattern });
	}
	return { ok: true, input: { searchQuery, open, find } };
}

function invalidUtf8Bytes(value: string, maxBytes: number): boolean {
	return value.length === 0 || encodedBytes(value) > maxBytes;
}

function encodedBytes(value: string): number {
	return new TextEncoder().encode(value).byteLength;
}

function optionalArrayField(
	input: Record<string, RuntimeJsonValue>,
	field: string,
): readonly RuntimeJsonValue[] | undefined {
	const value = input[field];
	return value === undefined || Array.isArray(value)
		? (value ?? [])
		: undefined;
}

function numberField(
	input: Record<string, RuntimeJsonValue>,
	field: string,
): number | undefined {
	const value = input[field];
	return typeof value === "number" && Number.isSafeInteger(value)
		? value
		: undefined;
}

function isRuntimeJsonValue(value: unknown): value is RuntimeJsonValue {
	if (
		value === null ||
		typeof value === "boolean" ||
		typeof value === "string"
	) {
		return true;
	}
	if (typeof value === "number") {
		return Number.isFinite(value);
	}
	if (Array.isArray(value)) {
		return value.every(isRuntimeJsonValue);
	}
	if (!isRecord(value)) {
		return false;
	}
	return Object.values(value).every(isRuntimeJsonValue);
}

function isRecord(value: unknown): value is Record<string, RuntimeJsonValue> {
	return (
		typeof value === "object" &&
		value !== null &&
		!Array.isArray(value) &&
		Object.getPrototypeOf(value) === Object.prototype
	);
}
