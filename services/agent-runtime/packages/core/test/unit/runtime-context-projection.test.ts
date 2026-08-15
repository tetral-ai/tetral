import { describe, expect, test } from "bun:test";
import { ProviderContextRole } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { lowerProviderRequest } from "../../../../../gateway/packages/lowering/src/request.js";
import { OpenAIGPT55Rules } from "../../../../../gateway/packages/lowering/src/rules/openai.js";
import { validateProviderRequest } from "../../../../../gateway/packages/protocol/src/bounds.js";
import type {
	RuntimeContextEntry,
	RuntimeContextKind,
	RuntimeContextPart,
	RuntimeToolError,
} from "../../src/contracts/runtime.js";
import {
	toGatewayProviderContext,
	toGatewayProviderContextSegments,
} from "../../src/runtime/context-projection.js";
import { assembleProviderCallRequest } from "../../src/thread-loop/provider-request.js";

function entry(
	messageSequence: number,
	contextKind: RuntimeContextKind,
	parts: readonly RuntimeContextPart[],
): RuntimeContextEntry {
	return { messageSequence, contextKind, parts: [...parts] };
}

function toolCall(modelToolCallId = "call-1"): RuntimeContextPart {
	return {
		type: "tool_call",
		modelToolCallId,
		toolName: "lookup",
		canonicalInput: { q: "hi" },
	};
}

function toolResult(
	result:
		| {
				readonly type: "completed";
				readonly output: { readonly text: string };
		  }
		| { readonly type: "error"; readonly error: RuntimeToolError }
		| { readonly type: "cancelled" },
	modelToolCallId = "call-1",
): RuntimeContextPart {
	return { type: "tool_result", modelToolCallId, result };
}

describe("Runtime provider-context projection", () => {
	test("projects sealed provider-visible context without database or lifecycle metadata", () => {
		const result = toGatewayProviderContext([
			entry(1, "user", [{ type: "text", text: "hello" }]),
			entry(2, "assistant", [
				{ type: "text", text: "answer" },
				{
					type: "reasoning",
					text: "thinking",
					providerMetadata: { anthropic: { signature: "sig_1" } },
				},
				toolCall(),
				toolResult({
					type: "completed",
					output: { text: "result" },
				}),
			]),
		]);

		expect(result).toEqual({
			ok: true,
			context: [
				{
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
					content: [{ text: { text: "hello" } }],
				},
				{
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
					content: [
						{ text: { text: "answer" } },
						{
							reasoning: {
								text: "thinking",
								metadataJson: '{"anthropic":{"signature":"sig_1"}}',
							},
						},
						{
							toolCall: {
								modelToolCallId: "call-1",
								name: "lookup",
								inputJson: '{"q":"hi"}',
							},
						},
						{
							toolResult: {
								modelToolCallId: "call-1",
								completed: { outputJson: '{"text":"result"}' },
								error: undefined,
								cancelled: undefined,
							},
						},
					],
				},
			],
		});
	});

	test("treats parent prefix and child history as independent sequence domains", () => {
		const result = toGatewayProviderContextSegments([
			[entry(1, "user", [{ type: "text", text: "parent sequence one" }])],
			[
				entry(1, "assistant", [toolCall("call-child")]),
				entry(2, "assistant", [
					toolResult(
						{
							type: "completed",
							output: { text: "child result" },
						},
						"call-child",
					),
				]),
			],
		]);
		expect(result).toMatchObject({ ok: true });
		if (!result.ok) return;
		expect(result.context.flatMap((message) => message.content)).toMatchObject([
			{ text: { text: "parent sequence one" } },
			{ toolCall: { modelToolCallId: "call-child" } },
			{ toolResult: { modelToolCallId: "call-child" } },
		]);
	});

	test("preserves signed empty reasoning for provider replay", () => {
		expect(
			toGatewayProviderContext([
				entry(1, "assistant", [
					{
						type: "reasoning",
						text: "",
						providerMetadata: { anthropic: { signature: "sig_empty" } },
					},
				]),
			]),
		).toEqual({
			ok: true,
			context: [
				{
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
					content: [
						{
							reasoning: {
								text: "",
								metadataJson: '{"anthropic":{"signature":"sig_empty"}}',
							},
						},
					],
				},
			],
		});
	});

	test("keeps a pending Tool Call and adds only its independently settled result", () => {
		const pending = entry(1, "assistant", [toolCall()]);
		const terminal = entry(2, "assistant", [
			toolResult({
				type: "completed",
				output: { text: "result" },
			}),
		]);

		expect(toGatewayProviderContext([pending])).toEqual({
			ok: true,
			context: [
				{
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
					content: [
						{
							toolCall: {
								modelToolCallId: "call-1",
								name: "lookup",
								inputJson: '{"q":"hi"}',
							},
						},
					],
				},
			],
		});
		expect(toGatewayProviderContext([pending, terminal])).toEqual({
			ok: true,
			context: [
				{
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
					content: [
						{
							toolCall: {
								modelToolCallId: "call-1",
								name: "lookup",
								inputJson: '{"q":"hi"}',
							},
						},
					],
				},
				{
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
					content: [
						{
							toolResult: {
								modelToolCallId: "call-1",
								completed: { outputJson: '{"text":"result"}' },
								error: undefined,
								cancelled: undefined,
							},
						},
					],
				},
			],
		});
	});

	test("rejects malformed context at the projection boundary", () => {
		expect(
			toGatewayProviderContext([
				{
					messageSequence: 1,
					contextKind: "assistant",
					parts: [
						{
							type: "tool_result",
							result: {
								type: "completed",
								output: { text: "result" },
							},
						},
					],
				} as unknown as RuntimeContextEntry,
			]),
		).toMatchObject({ ok: false, error: { code: "provider_invalid_request" } });
	});

	test("projects internal invalid-tool repair only as its model Tool Call/Error pair", () => {
		const error: RuntimeToolError = {
			type: "provider_tool_protocol_error",
			message: "invalid tool",
			retryable: false,
		};
		const result = toGatewayProviderContext([
			entry(1, "assistant", [toolCall()]),
			entry(2, "assistant", [toolResult({ type: "error", error })]),
		]);

		expect(result).toMatchObject({ ok: true });
		if (result.ok) {
			expect(result.context.flatMap((item) => item.content)).toMatchObject([
				{ toolCall: { modelToolCallId: "call-1", name: "lookup" } },
				{
					toolResult: {
						modelToolCallId: "call-1",
						error: { errorJson: expect.any(String) },
					},
				},
			]);
		}
	});

	test("keeps completed, errored, and cancelled Tool wire lowering unchanged", () => {
		const cases = [
			{
				status: "completed",
				result: {
					type: "completed" as const,
					output: { text: "result" },
				},
				output: { text: "result" },
				isError: undefined,
			},
			{
				status: "error",
				result: {
					type: "error" as const,
					error: {
						type: "provider_tool_protocol_error" as const,
						message: "failed",
						retryable: false,
					},
				},
				output: {
					error: {
						type: "provider_tool_protocol_error",
						message: "failed",
						retryable: false,
					},
				},
				isError: true as const,
			},
			{
				status: "cancelled",
				result: { type: "cancelled" as const },
				output: { type: "text", text: "[tool execution cancelled]" },
				isError: true as const,
			},
		];

		for (const testCase of cases) {
			const projected = toGatewayProviderContext([
				entry(1, "assistant", [toolCall(), toolResult(testCase.result)]),
			]);
			expect(projected.ok).toBe(true);
			if (!projected.ok) continue;

			const assembled = assembleProviderCallRequest({
				identity: {
					workspaceId: "default",
					sessionId: "session-a",
					sessionThreadId: "thread-a",
					bindingId: "binding-a",
					bindingGeneration: 1,
					targetPodUid: "pod-a",
					runtimeBindingToken: "binding-token",
				},
				requestId: `request-${testCase.status}`,
				modelRequestId: `model-request-${testCase.status}`,
				currentModel: {
					providerId: OpenAIGPT55Rules.providerId,
					modelId: OpenAIGPT55Rules.modelId,
				},
				providerContext: projected.context,
				runtime: {
					systemInstructions: "ordinary Tool regression",
					timeoutMs: 30_000,
				},
			});
			expect(assembled.ok).toBe(true);
			if (!assembled.ok) continue;

			expect(validateProviderRequest(assembled.request)).toEqual({ ok: true });
			const lowered = lowerProviderRequest(
				assembled.request,
				OpenAIGPT55Rules,
				{ modelOutputTokenLimit: 32_000 },
			);
			const toolEntries = lowered.messages.filter(
				(item) => item.role === "assistant" || item.role === "tool",
			);
			expect(toolEntries).toEqual([
				{
					role: "assistant",
					content: [
						{
							type: "tool-call",
							toolCallId: "call-1",
							toolName: "lookup",
							input: { q: "hi" },
						},
					],
				},
				{
					role: "tool",
					content: [
						{
							type: "tool-result",
							toolCallId: "call-1",
							toolName: "lookup",
							output: testCase.output,
							...(testCase.isError === undefined
								? {}
								: { isError: testCase.isError }),
						},
					],
				},
			]);
		}
	});
});
