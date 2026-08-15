import { describe, expect, test } from "bun:test";
import {
	MaxProviderErrorMessageBytes,
	MaxProviderRequestToolOutputJsonBytes,
	MaxProviderToolCallInputJsonBytes,
} from "@tetral/gateway-protocol/src/bounds.js";
import { normalizeProviderError } from "../../src/contracts/provider.js";
import type {
	RuntimeDeclarationOperationControls,
	RuntimeInternalToolRepairCommit,
	RuntimeInternalToolRepairCommitResult,
	SessionEvent,
} from "../../src/contracts/runtime.js";
import {
	boundRuntimeJson,
	boundRuntimeText,
	boundRuntimeToolError,
	ContextLoaderErrorSchema,
	createRuntimeId,
	LLMEventSchema,
	normalizeContextLoaderError,
	normalizeRuntimeFailure,
	normalizeRuntimeInternalToolRepairStoreError,
	normalizeSessionEventWriterError,
	PendingInputResultSchema,
	RuntimeBoundedJsonSchema,
	RuntimeBoundedTextSchema,
	RuntimeContextEntrySchema,
	RuntimeDeclarationOperationControlsSchema,
	RuntimeFailureSchema,
	RuntimeInternalToolRepairCommitSchema,
	RuntimeInternalToolRepairStore,
	RuntimeInternalToolRepairStoreErrorSchema,
	RuntimeToolErrorSchema,
	runtimeNow,
	runtimeSleep,
	SessionEventEnvelopeSchema,
	SessionEventSchema,
	SessionEventWriterAppendEventSchema,
	SessionEventWriterAppendResultSchema,
	SessionEventWriterErrorSchema,
	SessionEventWriterRequestEndEnvelopeSchema,
	SessionEventWriterToolSettlementEnvelopeSchema,
} from "../../src/contracts/runtime.js";
import { RuntimePreviewTextMaxBytes } from "../../src/llm/llm-event.js";
import { sessionEventForDurableWrite } from "../../src/runtime/session-event-writer.js";

const signal = new AbortController().signal;
const createdAt = "2026-05-22T00:00:00.000Z";
const canary = "DUMMY_TOKEN_CANARY";
const rawSql = "select * from secrets";
const connectionString = "postgres://user:pass@db.internal/app";
const rawHeaders = "authorization: bearer raw-header-secret";
const rawPrompt = "system prompt raw backend payload marker";
const rawProviderPayload = "raw provider payload marker";

function writerBinding() {
	return {
		bindingId: "binding-1",
		bindingGeneration: 1,
		targetPodUid: "pod-1",
	} as const;
}

test("attachment_unavailable defaults to a non-fatal provider failure", () => {
	expect(
		normalizeProviderError({ code: "attachment_unavailable" }),
	).toMatchObject({
		code: "attachment_unavailable",
		message: "Attachment bytes are no longer available.",
		retryable: false,
		fatal: false,
	});
});

test("runtime provider errors preserve valid UTF-8 at the exact message byte boundary", () => {
	const exact = `${"a".repeat(MaxProviderErrorMessageBytes - 3)}€`;
	const atLimit = normalizeProviderError({
		code: "provider_stream_error",
		message: exact,
	});
	const overLimit = normalizeProviderError({
		code: "provider_stream_error",
		message: `${exact}b`,
	});

	expect(Buffer.byteLength(atLimit.message, "utf8")).toBe(
		MaxProviderErrorMessageBytes,
	);
	expect(atLimit.message).toBe(exact);
	expect(overLimit.message).toBe(exact);
	expect(() =>
		new TextDecoder("utf-8", { fatal: true }).decode(
			new TextEncoder().encode(overLimit.message),
		),
	).not.toThrow();
});

type ForkSDKRetryStatus =
	| { readonly type: "retrying" }
	| { readonly type: "exhausted" }
	| { readonly type: "terminal" };

type ForkSDKTextBlock = {
	readonly type: "text";
	readonly text: string;
};

type ForkSDKAgentMCPToolUseEvent = {
	readonly id: string;
	readonly processed_at: string;
	readonly type: "agent.mcp_tool_use";
	readonly name: string;
	readonly input: { readonly [key: string]: unknown };
	readonly mcp_server_name: string;
	readonly evaluated_permission?: "allow" | "ask" | "deny" | undefined;
};

type ForkSDKAgentMCPToolResultEvent = {
	readonly id: string;
	readonly processed_at: string;
	readonly type: "agent.mcp_tool_result";
	readonly mcp_tool_use_id: string;
	readonly content?: readonly ForkSDKTextBlock[] | undefined;
	readonly is_error?: boolean | null | undefined;
};

type ForkSDKMCPAuthenticationFailedError = {
	readonly type: "mcp_authentication_failed_error";
	readonly mcp_server_name: string;
	readonly message: string;
	readonly retry_status: ForkSDKRetryStatus;
};

type ForkSDKMCPConnectionFailedError = {
	readonly type: "mcp_connection_failed_error";
	readonly mcp_server_name: string;
	readonly message: string;
	readonly retry_status: ForkSDKRetryStatus;
};

type ForkSDKSessionMCPErrorEvent = {
	readonly id: string;
	readonly processed_at: string;
	readonly type: "session.error";
	readonly error:
		| ForkSDKMCPAuthenticationFailedError
		| ForkSDKMCPConnectionFailedError;
};

type AssertType<T extends true> = T;
type RuntimeMCPToolUseSDKCompatibility = AssertType<
	Extract<SessionEvent, { readonly type: "agent.mcp_tool_use" }> & {
		readonly id: string;
		readonly processed_at: string;
	} extends ForkSDKAgentMCPToolUseEvent
		? true
		: false
>;
type RuntimeMCPToolResultSDKCompatibility = AssertType<
	Extract<SessionEvent, { readonly type: "agent.mcp_tool_result" }> & {
		readonly id: string;
		readonly processed_at: string;
	} extends ForkSDKAgentMCPToolResultEvent
		? true
		: false
>;
type RuntimeSessionErrorEvent = Extract<
	SessionEvent,
	{ readonly type: "session.error" }
>;
type RuntimeMCPAuthErrorSDKCompatibility = AssertType<
	{
		readonly type: "session.error";
		readonly error: Extract<
			RuntimeSessionErrorEvent["error"],
			{ readonly type: "mcp_authentication_failed_error" }
		>;
	} & {
		readonly id: string;
		readonly processed_at: string;
	} extends ForkSDKSessionMCPErrorEvent
		? true
		: false
>;
type RuntimeMCPConnectionErrorSDKCompatibility = AssertType<
	{
		readonly type: "session.error";
		readonly error: Extract<
			RuntimeSessionErrorEvent["error"],
			{ readonly type: "mcp_connection_failed_error" }
		>;
	} & {
		readonly id: string;
		readonly processed_at: string;
	} extends ForkSDKSessionMCPErrorEvent
		? true
		: false
>;

void (undefined as unknown as RuntimeMCPToolUseSDKCompatibility);
void (undefined as unknown as RuntimeMCPToolResultSDKCompatibility);
void (undefined as unknown as RuntimeMCPAuthErrorSDKCompatibility);
void (undefined as unknown as RuntimeMCPConnectionErrorSDKCompatibility);

async function sleepUntilAborted(
	_durationMs: number,
	sleepSignal: AbortSignal,
): Promise<boolean> {
	if (sleepSignal.aborted) {
		return false;
	}
	await new Promise<void>((resolve) =>
		sleepSignal.addEventListener("abort", () => resolve(), { once: true }),
	);
	return false;
}

function operationControls(
	overrides: Partial<RuntimeDeclarationOperationControls> = {},
): RuntimeDeclarationOperationControls {
	return {
		signal,
		timeoutMs: 100,
		sleep: sleepUntilAborted,
		...overrides,
	};
}

function parseExactSessionEvent<T extends SessionEvent>(event: T): T {
	return SessionEventSchema.parse(event) as T;
}

function withSDKEventEnvelope<T extends SessionEvent>(
	event: T,
): T & { readonly id: string; readonly processed_at: string } {
	return {
		id: "sevt_sdk_shape_1",
		processed_at: createdAt,
		...event,
	};
}

function internalToolRepairCommit(): RuntimeInternalToolRepairCommit {
	const modelToolCallId = "tool-call-1";
	const toolName = "tool";
	return RuntimeInternalToolRepairCommitSchema.parse({
		...writerBinding(),
		workspaceId: "workspace-1",
		sessionId: "session-1",
		sessionThreadId: "thread-1",
		modelRequestId: "model-request-1",
		modelToolCallId,
		toolName,
		repairKey: "repair-key-1",
		canonicalInput: {},
		error: {
			type: "provider_tool_protocol_error",
			message: "model emitted an unavailable tool",
			retryable: false,
		},
	});
}

function redactionNeedles(): readonly string[] {
	return [
		canary,
		rawSql,
		connectionString,
		rawHeaders,
		rawPrompt,
		rawProviderPayload,
	];
}

function expectSanitized(value: unknown): void {
	const serialized = JSON.stringify(value);
	for (const needle of redactionNeedles()) {
		expect(serialized).not.toContain(needle);
	}
}

class UnitRuntimeInternalToolRepairStore extends RuntimeInternalToolRepairStore {
	protected override async commitInternalToolRepairRecord(
		repair: RuntimeInternalToolRepairCommit,
	): Promise<RuntimeInternalToolRepairCommitResult> {
		return internalToolRepairResult(repair);
	}
}

function internalToolRepairResult(
	_repair: RuntimeInternalToolRepairCommit,
): RuntimeInternalToolRepairCommitResult {
	return {
		ok: true,
		type: "committed",
		repairEventId: "event-repair-1",
		assignedMessageSequence: 2,
	};
}

describe("runtime boundary contracts", () => {
	test("assistant projection events carry model request identity under the closed association law", () => {
		const base = {
			...writerBinding(),
			workspaceId: "workspace-1",
			sessionId: "sesn_1",
			sessionThreadId: "thr_1",
			writeId: "rwrite_1",
			modelRequestId: "mreq_anchor_1",
			assistantContextAppend: {
				parts: [
					{
						type: "tool" as const,
						modelToolCallId: "call_1",
						toolName: "bash",
						state: {
							status: "running" as const,
							input: { value: {}, preview: "{}", truncated: false },
						},
					},
				],
			},
		};
		for (const event of [
			{
				type: "agent.tool_use" as const,
				name: "bash",
				input: {},
				evaluated_permission: "allow" as const,
			},
			{
				type: "agent.mcp_tool_use" as const,
				name: "search",
				input: {},
				evaluated_permission: "allow" as const,
				mcp_server_name: "github",
			},
		]) {
			expect(
				SessionEventEnvelopeSchema.safeParse({ ...base, event }).success,
			).toBe(true);
		}
		expect(
			SessionEventEnvelopeSchema.safeParse({
				...base,
				event: { type: "session.status_running" },
			}).success,
		).toBe(false);
		expect(
			SessionEventEnvelopeSchema.safeParse({
				...base,
				modelRequestId: undefined,
				event: {
					type: "agent.tool_use",
					name: "bash",
					input: {},
					evaluated_permission: "allow",
				},
			}).success,
		).toBe(false);
		expect(
			SessionEventEnvelopeSchema.safeParse({
				...writerBinding(),
				workspaceId: "workspace-1",
				sessionId: "sesn_1",
				sessionThreadId: "thr_1",
				writeId: "rwrite_2",
				assistantContextAppend: {
					parts: [
						{
							type: "tool",
							modelToolCallId: "call_1",
							toolName: "bash",
							state: {
								status: "running",
								input: { value: {}, preview: "{}", truncated: false },
							},
						},
					],
				},
				event: {
					type: "agent.tool_use",
					name: "bash",
					input: {},
					evaluated_permission: "allow",
				},
				modelRequestId: "mreq_without_anchor",
			}).success,
		).toBe(true);

		const projectionBase = {
			...writerBinding(),
			workspaceId: "workspace-1",
			sessionId: "sesn_1",
			sessionThreadId: "thr_1",
			writeId: "rwrite_projection",
			modelRequestId: "mreq_projection",
		};
		const messageEnvelope = {
			...projectionBase,
			event: {
				type: "agent.message" as const,
				content: [{ type: "text" as const, text: "answer" }],
			},
			assistantContextAppend: {
				parts: [{ type: "text" as const, text: "answer", truncated: false }],
			},
		};
		expect(SessionEventEnvelopeSchema.safeParse(messageEnvelope).success).toBe(
			true,
		);
		expect(
			SessionEventEnvelopeSchema.safeParse({
				...messageEnvelope,
				modelRequestId: undefined,
			}).success,
		).toBe(false);
		for (const event of [
			{
				type: "agent.tool_result",
				tool_use_id: "sevt_tool",
				content: [{ type: "text", text: "done" }],
			},
			{
				type: "agent.mcp_tool_result",
				mcp_tool_use_id: "sevt_mcp",
				content: [{ type: "text", text: "done" }],
			},
		]) {
			expect(
				SessionEventEnvelopeSchema.safeParse({ ...projectionBase, event })
					.success,
			).toBe(false);
		}
		const settlementBase = {
			workspaceId: "workspace-1",
			sessionId: "sesn_1",
			sessionThreadId: "thr_1",
			bindingId: "binding-1",
			bindingGeneration: 1,
			targetPodUid: "pod-1",
		};
		expect(
			SessionEventWriterToolSettlementEnvelopeSchema.safeParse({
				...settlementBase,
				settlement: {
					toolUseEventId: "sevt_tool",
					outcome: {
						type: "completed",
						output: { text: "done", truncated: false },
					},
				},
			}).success,
		).toBe(true);
		expect(
			SessionEventWriterToolSettlementEnvelopeSchema.safeParse({
				...settlementBase,
				settlement: {
					toolUseEventId: "sevt_mcp_uncertain",
					outcome: {
						type: "error",
						error: {
							type: "runtime",
							code: "runtime_invalid_sequence",
							message: "Result is uncertain.",
							retryable: false,
							fatal: false,
						},
					},
				},
			}).success,
		).toBe(true);
		expect(
			SessionEventEnvelopeSchema.safeParse({
				...projectionBase,
				event: {
					type: "span.model_request_start",
					model_request_id: "mreq_projection",
				},
			}).success,
		).toBe(false);
	});

	test("request-end carries only the trailing incremental Assistant append on successful completion", () => {
		const base = {
			...writerBinding(),
			workspaceId: "workspace-1",
			sessionId: "sesn_1",
			sessionThreadId: "thr_1",
			writeId: "rwrite_1",
			modelRequestId: "mreq_1",
			isError: false,
			finishReason: "stop" as const,
		};
		const trailingContextAppend = {
			parts: [
				{
					type: "reasoning" as const,
					providerPartId: "provider_reasoning",
					text: "thinking",
					providerMetadata: {},
					truncated: false,
				},
			],
		};
		expect(
			SessionEventWriterRequestEndEnvelopeSchema.safeParse({
				...base,
				trailingContextAppend,
			}).success,
		).toBe(true);
		expect(
			SessionEventWriterRequestEndEnvelopeSchema.safeParse({
				...base,
				isError: true,
				errorKind: "provider_error",
				trailingContextAppend,
			}).success,
		).toBe(false);
		expect(
			SessionEventWriterRequestEndEnvelopeSchema.safeParse({
				...base,
				stableReasoningParts: [],
			}).success,
		).toBe(false);
	});

	test("request-end attachment settlement is combined-bounded and absent on reschedule", () => {
		const base = {
			...writerBinding(),
			workspaceId: "workspace-1",
			sessionId: "sesn_1",
			sessionThreadId: "thr_1",
			writeId: "rwrite_attachments",
			modelRequestId: "mreq_attachments",
			isError: false,
			finishReason: "stop" as const,
		};
		const fileAttachments = Array.from({ length: 16 }, (_, index) => ({
			sourceEventId: `sevt_file_${index}`,
			fileId: `file_${index}`,
		}));
		expect(
			SessionEventWriterRequestEndEnvelopeSchema.safeParse({
				...base,
				consumedAttachmentRefs: Array.from(
					{ length: 16 },
					(_, index) => `att_${index}`,
				),
				consumedFileAttachments: fileAttachments,
			}).success,
		).toBe(true);
		expect(
			SessionEventWriterRequestEndEnvelopeSchema.safeParse({
				...base,
				consumedAttachmentRefs: Array.from(
					{ length: 17 },
					(_, index) => `att_${index}`,
				),
				consumedFileAttachments: fileAttachments,
			}).success,
		).toBe(false);
		expect(
			SessionEventWriterRequestEndEnvelopeSchema.safeParse({
				...base,
				isError: true,
				errorKind: "provider_error",
				reschedule: { attempt: 1, deadline: createdAt, backoffMs: 1 },
				consumedFileAttachments: [fileAttachments[0]!],
			}).success,
		).toBe(false);
	});

	test("matches reasoning stream admission to the shared 64 KiB text and 16 KiB metadata bounds", () => {
		const metadataAtLimit = { x: "m".repeat(16 * 1024 - 8) };
		expect(
			LLMEventSchema.safeParse({
				type: "reasoning-delta",
				id: "reasoning_1",
				text_delta: "x".repeat(64 * 1024),
				providerMetadata: metadataAtLimit,
			}).success,
		).toBe(true);
		expect(
			LLMEventSchema.safeParse({
				type: "reasoning-delta",
				id: "reasoning_1",
				text_delta: "x".repeat(64 * 1024 + 1),
			}).success,
		).toBe(false);
		expect(
			LLMEventSchema.safeParse({
				type: "reasoning-delta",
				id: "reasoning_1",
				text_delta: "x",
				providerMetadata: { x: "m".repeat(16 * 1024 - 7) },
			}).success,
		).toBe(false);
	});
	test("maps internal RuntimeFailure session errors to fork-SDK durable payloads", () => {
		const failures = [
			normalizeRuntimeFailure({
				type: "provider",
				code: "provider_rate_limited",
				retryable: true,
				fatal: false,
				retryStatus: { type: "retrying", attempt: 1 },
			}),
			normalizeRuntimeFailure({
				type: "provider",
				code: "provider_unavailable",
				retryable: true,
				fatal: false,
				retryStatus: { type: "exhausted" },
			}),
			normalizeRuntimeFailure({
				type: "provider",
				code: "provider_invalid_request",
				retryable: false,
				fatal: true,
				retryStatus: { type: "terminal" },
			}),
		];

		expect(
			failures.map((error) =>
				sessionEventForDurableWrite(
					SessionEventWriterAppendEventSchema.parse({
						type: "session.error",
						error,
					}),
				),
			),
		).toEqual([
			{
				type: "session.error",
				error: {
					type: "model_rate_limited_error",
					message: "Runtime operation failed.",
					retry_status: { type: "retrying" },
				},
			},
			{
				type: "session.error",
				error: {
					type: "model_overloaded_error",
					message: "Runtime operation failed.",
					retry_status: { type: "exhausted" },
				},
			},
			{
				type: "session.error",
				error: {
					type: "model_request_failed_error",
					message: "Runtime operation failed.",
					retry_status: { type: "terminal" },
				},
			},
		]);
	});

	test("emits MCP runtime events compatible with fork SDK event types", () => {
		const mcpToolUse = withSDKEventEnvelope(
			parseExactSessionEvent({
				type: "agent.mcp_tool_use",
				name: "create_issue",
				input: { title: "Bug" },
				mcp_server_name: "github",
				evaluated_permission: "ask",
			}),
		) satisfies ForkSDKAgentMCPToolUseEvent;
		const mcpToolResult = withSDKEventEnvelope(
			parseExactSessionEvent({
				type: "agent.mcp_tool_result",
				mcp_tool_use_id: "sevt_mcp_tool_use_1",
				content: [{ type: "text", text: "created" }],
				is_error: false,
			}),
		) satisfies ForkSDKAgentMCPToolResultEvent;
		const authError = withSDKEventEnvelope(
			parseExactSessionEvent({
				type: "session.error",
				error: {
					type: "mcp_authentication_failed_error",
					mcp_server_name: "github",
					message: "MCP authentication failed after refresh.",
					retry_status: { type: "terminal" },
				},
			}),
		) satisfies ForkSDKSessionMCPErrorEvent;
		const connectionError = withSDKEventEnvelope(
			parseExactSessionEvent({
				type: "session.error",
				error: {
					type: "mcp_connection_failed_error",
					mcp_server_name: "github",
					message: "MCP connection failed.",
					retry_status: { type: "exhausted" },
				},
			}),
		) satisfies ForkSDKSessionMCPErrorEvent;

		expect([
			mcpToolUse.type,
			mcpToolResult.type,
			authError.error.type,
			connectionError.error.type,
		]).toEqual([
			"agent.mcp_tool_use",
			"agent.mcp_tool_result",
			"mcp_authentication_failed_error",
			"mcp_connection_failed_error",
		]);
		expect(
			SessionEventSchema.safeParse({
				type: "agent.mcp_tool_use",
				name: "create_issue",
				input: "not-an-sdk-tool-input-object",
				mcp_server_name: "github",
				evaluated_permission: "ask",
			}).success,
		).toBe(false);
	});

	test("accepts the required Anthropic-shaped outbound event subset and rejects legacy events", () => {
		const runtimeFailure = RuntimeFailureSchema.parse({
			type: "provider",
			code: "provider_timeout",
			message: "Provider timed out.",
			retryable: true,
			fatal: false,
			providerId: "openai",
			modelId: "gpt-5.5",
			retryStatus: { type: "exhausted" },
		});
		const events = [
			{ type: "agent.message", content: [{ type: "text", text: "hello" }] },
			{
				type: "agent.tool_use",
				name: "search",
				input: { q: "runtime" },
				evaluated_permission: "allow",
			},
			{
				type: "agent.tool_result",
				tool_use_id: "sevt_tool_use_1",
				content: [{ type: "text", text: "done" }],
			},
			{
				type: "agent.tool_result",
				tool_use_id: "sevt_tool_use_1",
				is_error: true,
			},
			{
				type: "agent.mcp_tool_use",
				name: "create_issue",
				input: { title: "Bug" },
				mcp_server_name: "github",
				evaluated_permission: "allow",
			},
			{
				type: "agent.mcp_tool_result",
				mcp_tool_use_id: "sevt_mcp_tool_use_1",
				content: [{ type: "text", text: "created" }],
			},
			{
				type: "agent.mcp_tool_result",
				mcp_tool_use_id: "sevt_mcp_tool_use_1",
				is_error: true,
			},
			{
				type: "session.error",
				error: {
					type: "mcp_authentication_failed_error",
					mcp_server_name: "github",
					message: "MCP authentication failed after refresh.",
					retry_status: { type: "terminal" },
				},
			},
			{
				type: "session.error",
				error: {
					type: "mcp_connection_failed_error",
					mcp_server_name: "github",
					message: "MCP connection failed.",
					retry_status: { type: "exhausted" },
				},
			},
			{ type: "session.status_running" },
			{ type: "session.status_idle", stop_reason: { type: "end_turn" } },
			{
				type: "session.status_idle",
				stop_reason: {
					type: "requires_action",
					event_ids: ["sevt_blocking_1"],
				},
			},
			{
				type: "session.status_idle",
				stop_reason: { type: "retries_exhausted" },
			},
			{ type: "session.status_terminated" },
			{
				type: "session.thread_status_terminated",
				session_thread_id: "thr_child",
			},
			{ type: "session.error", error: runtimeFailure },
			{ type: "span.model_request_start", model_request_id: "mreq_1" },
			{
				type: "span.model_request_end",
				model_request_start_id: "sevt_model_start_1",
				is_error: false,
				model_usage: {
					input_tokens: 1,
					output_tokens: 2,
					cache_creation_input_tokens: 0,
					cache_read_input_tokens: 0,
					speed: null,
				},
			},
		];

		expect(events.map((event) => SessionEventSchema.parse(event).type)).toEqual(
			[
				"agent.message",
				"agent.tool_use",
				"agent.tool_result",
				"agent.tool_result",
				"agent.mcp_tool_use",
				"agent.mcp_tool_result",
				"agent.mcp_tool_result",
				"session.error",
				"session.error",
				"session.status_running",
				"session.status_idle",
				"session.status_idle",
				"session.status_idle",
				"session.status_terminated",
				"session.thread_status_terminated",
				"session.error",
				"span.model_request_start",
				"span.model_request_end",
			],
		);

		for (const legacyEvent of [
			{ type: "session.status", status: { type: "idle" } },
			{
				type: "session.status_idle",
				stop_reason: {
					type: "requires_action",
					blocking_event_ids: ["sevt_blocking_1"],
				},
			},
			{ type: "agent.message", info: { status: "completed" } },
			{ type: "agent.message", part: { type: "text", text: "hello" }, time: 0 },
			{
				type: "agent.message",
				messageId: "message-1",
				partId: "part-1",
				field: "text",
				delta: "hello",
			},
		]) {
			expect(SessionEventSchema.safeParse(legacyEvent).success).toBe(false);
		}
	});

	test("defines strict pending input, loader, LLM event, event writer, and event-writer boundary schemas", () => {
		const userEntry = RuntimeContextEntrySchema.parse({
			messageSequence: 1,
			contextKind: "user",
			parts: [{ type: "text", text: "hello" }],
		});

		expect(
			PendingInputResultSchema.parse({ type: "context", entries: [userEntry] })
				.type,
		).toBe("context");
		expect(PendingInputResultSchema.parse({ type: "empty" }).type).toBe(
			"empty",
		);
		expect(
			PendingInputResultSchema.safeParse({ type: "interrupt" }).success,
		).toBe(false);
		expect(
			PendingInputResultSchema.safeParse({
				type: "context",
				entries: [{ ...userEntry, contextKind: "assistant" }],
			}).success,
		).toBe(false);
		expect(
			ContextLoaderErrorSchema.parse(
				normalizeContextLoaderError({
					code: "unavailable",
					rawError: new Error(canary),
				}),
			).message,
		).toBe("Context loader operation failed.");
		expect(
			LLMEventSchema.parse({
				type: "text-delta",
				id: "text-1",
				text_delta: "hello",
			}).type,
		).toBe("text-delta");
		expect(
			LLMEventSchema.safeParse({ type: "raw-provider-event", raw: canary })
				.success,
		).toBe(false);
		expect(
			LLMEventSchema.safeParse({
				type: "tool-result",
				id: "tool-1",
				output: { text: "done", truncated: false },
			}).success,
		).toBe(false);
		expect(
			LLMEventSchema.safeParse({
				type: "tool-error",
				id: "tool-1",
				error: normalizeRuntimeFailure({
					type: "runtime",
					code: "runtime_invalid_sequence",
					rawError: "tool failed",
				}),
			}).success,
		).toBe(false);
		expect(
			SessionEventEnvelopeSchema.parse({
				...writerBinding(),
				workspaceId: "workspace-1",
				sessionId: "session-1",
				sessionThreadId: "thread-1",
				writeId: "write-1",
				event: { type: "session.status_running" },
			}).writeId,
		).toBe("write-1");
		const appendResult = SessionEventWriterAppendResultSchema.parse({
			ok: true,
			type: "committed",
			eventId: "sevt_1",
		});
		expect(appendResult.ok).toBe(true);
		if (!appendResult.ok || appendResult.type === "stale") {
			throw new Error("expected session event append success");
		}
		expect(appendResult.eventId).toBe("sevt_1");
		expect(
			SessionEventWriterErrorSchema.parse(
				normalizeSessionEventWriterError({
					code: "ack_mismatch",
					rawError: canary,
				}),
			).message,
		).toBe("Session event writer operation failed.");
		expect(
			normalizeSessionEventWriterError({
				code: "superseded",
				rawError: canary,
			}),
		).toMatchObject({
			code: "superseded",
			retryable: false,
			fatal: false,
		});
		expect(
			normalizeSessionEventWriterError({
				code: "unrepairable",
				rawError: canary,
			}),
		).toMatchObject({
			code: "unrepairable",
			retryable: false,
			fatal: true,
		});
	});

	test("RuntimeInternalToolRepairStore exposes only the durable repair declaration and validates payloads", async () => {
		class HostileFailureStore extends UnitRuntimeInternalToolRepairStore {
			protected override async commitInternalToolRepairRecord(): Promise<RuntimeInternalToolRepairCommitResult> {
				const hostileText = `${canary} ${rawSql} ${connectionString} ${rawHeaders} ${rawPrompt} ${rawProviderPayload}`;
				return {
					ok: false,
					error: {
						type: "message-store",
						code: "schema_mismatch",
						operation: "commitInternalToolRepair",
						message: hostileText,
						retryable: false,
						fatal: true,
						constraint: rawSql,
						sessionId: rawHeaders,
					},
				};
			}
		}

		const store = new UnitRuntimeInternalToolRepairStore();
		const repair = internalToolRepairCommit();
		const repairResult = await store.commitInternalToolRepair(
			repair,
			operationControls(),
		);
		const hostileFailure =
			await new HostileFailureStore().commitInternalToolRepair(
				repair,
				operationControls(),
			);

		expect(
			Object.getOwnPropertyNames(
				RuntimeInternalToolRepairStore.prototype,
			).sort(),
		).toEqual(["commitInternalToolRepair", "constructor"]);
		expect("listMessages" in store).toBe(false);
		expect("readMessages" in store).toBe(false);
		expect(repairResult).toEqual(internalToolRepairResult(repair));
		expect(
			await store.commitInternalToolRepair(
				{ ...repair, rawDriver: "pg" },
				operationControls(),
			),
		).toMatchObject({
			ok: false,
			error: { code: "schema_mismatch", operation: "commitInternalToolRepair" },
		});
		expectSanitized(hostileFailure);
		expect(
			RuntimeDeclarationOperationControlsSchema.safeParse({
				...operationControls(),
				sessionId: "session-1",
			}).success,
		).toBe(false);
	});

	test("preserves executable runtime payloads while sanitizing failure channels", () => {
		const executableText = `${canary} ${rawSql} ${connectionString} ${rawHeaders} ${rawPrompt} ${rawProviderPayload}`;
		const boundedJson = RuntimeBoundedJsonSchema.parse({
			value: { [rawHeaders]: executableText },
			preview: executableText,
			truncated: false,
		});
		const contextEntry = RuntimeContextEntrySchema.parse({
			messageSequence: 1,
			contextKind: "user",
			parts: [{ type: "text", text: executableText }],
		});
		const toolCall = LLMEventSchema.parse({
			type: "tool-call",
			id: canary,
			toolName: "search",
			input: boundedJson.value,
			inputPreview: {
				preview: boundedJson.preview,
				truncated: boundedJson.truncated,
			},
		});
		const failure = RuntimeFailureSchema.parse({
			type: "provider",
			code: "provider_stream_error",
			message: executableText,
			retryable: false,
			fatal: true,
		});

		expect(boundedJson).toEqual({
			value: { [rawHeaders]: executableText },
			preview: executableText,
			truncated: false,
		});
		expect(contextEntry.parts[0]).toMatchObject({ text: executableText });
		expect(toolCall).toMatchObject({
			input: { [rawHeaders]: executableText },
			inputPreview: { preview: executableText, truncated: false },
		});
		expectSanitized(failure);
	});

	test("accepts exact canonical tool-output JSON and rejects one byte over", () => {
		const envelopeBytes = new TextEncoder().encode(
			JSON.stringify({ text: "" }),
		).byteLength;
		const exactText = "x".repeat(
			MaxProviderRequestToolOutputJsonBytes - envelopeBytes,
		);

		expect(
			RuntimeBoundedTextSchema.safeParse({ text: exactText, truncated: false })
				.success,
		).toBe(true);
		expect(
			RuntimeBoundedTextSchema.safeParse({
				text: `${exactText}x`,
				truncated: false,
			}).success,
		).toBe(false);
	});

	test("accepts exact Tool input and error JSON envelopes and rejects one byte over", () => {
		const inputEnvelopeBytes = new TextEncoder().encode(
			JSON.stringify({ value: "" }),
		).byteLength;
		const exactInput = "x".repeat(
			MaxProviderToolCallInputJsonBytes - inputEnvelopeBytes,
		);
		expect(
			RuntimeBoundedJsonSchema.safeParse({
				value: { value: exactInput },
				preview: "",
				truncated: true,
			}).success,
		).toBe(true);
		expect(
			RuntimeBoundedJsonSchema.safeParse({
				value: { value: `${exactInput}x` },
				preview: "",
				truncated: true,
			}).success,
		).toBe(false);

		const emptyError = {
			error: { type: "tool_failure", message: "", retryable: false },
		};
		const errorEnvelopeBytes = new TextEncoder().encode(
			JSON.stringify(emptyError),
		).byteLength;
		const exactMessage = "x".repeat(
			MaxProviderRequestToolOutputJsonBytes - errorEnvelopeBytes,
		);
		expect(
			RuntimeToolErrorSchema.safeParse({
				...emptyError.error,
				message: exactMessage,
			}).success,
		).toBe(true);
		expect(
			RuntimeToolErrorSchema.safeParse({
				...emptyError.error,
				message: `${exactMessage}x`,
			}).success,
		).toBe(false);
	});

	test("rejects lone UTF-16 surrogates before durable or provider projection", () => {
		const high = "\ud800";
		const low = "\udc00";
		expect(
			LLMEventSchema.safeParse({
				type: "reasoning-delta",
				id: "reasoning_1",
				text_delta: "safe",
				providerMetadata: { value: high },
			}).success,
		).toBe(false);
		expect(
			RuntimeBoundedJsonSchema.safeParse({
				value: { [low]: "safe" },
				preview: "",
				truncated: false,
			}).success,
		).toBe(false);
		expect(
			RuntimeBoundedTextSchema.safeParse({ text: high, truncated: false })
				.success,
		).toBe(false);
		expect(
			RuntimeToolErrorSchema.safeParse({
				type: "tool_failure",
				message: low,
				retryable: false,
			}).success,
		).toBe(false);
		expect(
			RuntimeBoundedJsonSchema.safeParse({
				value: { value: "😀" },
				preview: "",
				truncated: false,
			}).success,
		).toBe(true);
	});

	test("bounds durable tool-input previews independently from the complete value", () => {
		const value = { content: "v".repeat(16 * 1024) };
		expect(
			RuntimeBoundedJsonSchema.safeParse({
				value,
				preview: "p".repeat(RuntimePreviewTextMaxBytes),
				truncated: true,
			}).success,
		).toBe(true);
		expect(
			RuntimeBoundedJsonSchema.safeParse({
				value,
				preview: "p".repeat(RuntimePreviewTextMaxBytes + 1),
				truncated: true,
			}).success,
		).toBe(false);
	});

	test("sanitizes failure and log-safe boundary outputs", async () => {
		const hostileText = `${canary} ${rawSql} ${connectionString} ${rawHeaders} ${rawPrompt} ${rawProviderPayload}`;
		const outputs = [
			normalizeContextLoaderError({
				code: "schema_mismatch",
				rawError: hostileText,
			}),
			ContextLoaderErrorSchema.parse({
				type: "context-loader",
				code: "schema_mismatch",
				message: hostileText,
				retryable: false,
				fatal: true,
				sessionId: rawHeaders,
				reason: hostileText,
			}),
			normalizeRuntimeInternalToolRepairStoreError({
				code: "schema_mismatch",
				operation: "commitInternalToolRepair",
				rawError: hostileText,
			}),
			normalizeSessionEventWriterError({
				code: "unavailable",
				rawError: hostileText,
			}),
			normalizeRuntimeFailure({
				type: "provider",
				code: "provider_stream_error",
				rawError: hostileText,
			}),
			RuntimeFailureSchema.parse({
				type: "provider",
				code: "provider_stream_error",
				message: hostileText,
				retryable: false,
				fatal: true,
			}),
			normalizeRuntimeInternalToolRepairStoreError({
				code: "schema_mismatch",
				operation: "commitInternalToolRepair",
				constraint: rawSql,
				sessionId: rawHeaders,
			}),
			RuntimeInternalToolRepairStoreErrorSchema.parse({
				type: "message-store",
				code: "schema_mismatch",
				operation: "commitInternalToolRepair",
				message: hostileText,
				retryable: false,
				fatal: true,
				constraint: rawSql,
				sessionId: rawHeaders,
			}),
			SessionEventSchema.parse({
				type: "session.error",
				error: normalizeRuntimeFailure({
					type: "runtime",
					code: "runtime_invalid_sequence",
					rawError: hostileText,
				}),
			}),
			SessionEventSchema.parse({
				type: "session.error",
				error: RuntimeFailureSchema.parse({
					type: "message-store",
					code: "schema_mismatch",
					message: hostileText,
					retryable: false,
					fatal: true,
					operation: "commitInternalToolRepair",
					constraint: rawSql,
				}),
			}),
			RuntimeToolErrorSchema.parse({
				type: hostileText,
				message: hostileText,
				retryable: false,
			}),
			boundRuntimeToolError(
				{ type: "tool_failed", message: hostileText, retryable: true },
				1_000,
			),
		];

		for (const output of outputs) {
			expectSanitized(output);
		}
	});

	test("runtime dependencies control ids, timestamps, and sleeps", async () => {
		const deps = {
			createId: (prefix: string) => `${prefix}-1`,
			now: () => createdAt,
			monotonicMs: () => 12,
			sleep: async (_durationMs: number, sleepSignal: AbortSignal) =>
				!sleepSignal.aborted,
		};
		const id = createRuntimeId(deps, "message");
		expect(id).toBe("message-1");
		expect(runtimeNow(deps)).toBe(createdAt);
		expect(runtimeNow(deps)).toBe(createdAt);

		const controller = new AbortController();
		controller.abort();
		expect(await runtimeSleep(deps, 10_000, controller.signal)).toBe(false);
		expect(
			RuntimeInternalToolRepairStoreErrorSchema.parse(
				normalizeRuntimeInternalToolRepairStoreError({
					code: "timeout",
					operation: "commitInternalToolRepair",
				}),
			).operation,
		).toBe("commitInternalToolRepair");
	});
});
