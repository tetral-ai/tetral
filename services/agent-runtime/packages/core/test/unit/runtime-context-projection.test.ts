import { describe, expect, test } from "bun:test";
import { ProviderContextRole } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { validateProviderRequest } from "@tetral/gateway-protocol/src/bounds.js";
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

	test("removes an unresolved parent Tool Call from a child prefix without dropping safe siblings", () => {
		const result = toGatewayProviderContextSegments([
			[
				entry(1, "assistant", [
					{ type: "text", text: "safe parent text" },
					{
						type: "reasoning",
						text: "safe parent reasoning",
						providerMetadata: { anthropic: { signature: "sig_parent" } },
					},
					toolCall("call-unresolved-spawn"),
				]),
			],
			[entry(1, "user", [{ type: "text", text: "child task" }])],
		]);

		expect(result).toMatchObject({ ok: true });
		if (!result.ok) return;
		expect(result.context).toEqual([
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
				content: [
					{ text: { text: "safe parent text" } },
					{
						reasoning: {
							text: "safe parent reasoning",
							metadataJson: '{"anthropic":{"signature":"sig_parent"}}',
						},
					},
				],
			},
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [{ text: { text: "child task" } }],
			},
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

	// The Gateway's tests lower each of these forms (testdata/provider-requests).
	test("sends completed, errored, and cancelled Tool results in their ProviderRequest forms", () => {
		const error = { type: "provider_tool_protocol_error" as const, message: "failed", retryable: false };
		const cases = [
			{
				result: { type: "completed" as const, output: { text: "result" } },
				wire: { completed: { outputJson: JSON.stringify({ text: "result" }) } },
			},
			{
				result: { type: "error" as const, error },
				wire: { error: { errorJson: JSON.stringify({ error }) } },
			},
			{ result: { type: "cancelled" as const }, wire: { cancelled: {} } },
		];

		for (const testCase of cases) {
			const projected = toGatewayProviderContext([
				entry(1, "assistant", [toolCall(), toolResult(testCase.result)]),
			]);
			if (!projected.ok) throw new Error(`${testCase.result.type} failed provider projection`);
			const assembled = assembleProviderCallRequest({
				identity: {
					threadRole: "main",
					threadVisibility: "public",
					workspaceId: "default",
					sessionId: "session-a",
					sessionThreadId: "thread-a",
					bindingId: "binding-a",
					bindingGeneration: 1,
					targetPodUid: "pod-a",
					runtimeProcessId: "process-test",
					runtimeBindingToken: "binding-token",
				},
				requestId: `request-${testCase.result.type}`,
				modelRequestId: `model-request-${testCase.result.type}`,
				currentModel: { providerId: "openai", modelId: "gpt-5.5" },
				providerContext: projected.context,
				runtime: { systemInstructions: "ordinary Tool regression", timeoutMs: 30_000 },
			});
			if (!assembled.ok) throw new Error(`${testCase.result.type} failed request assembly`);
			expect(
				validateProviderRequest({ ...assembled.request, modelRequestStartEventId: "evt_0000000000000001" }),
			).toEqual({ ok: true });
			expect(assembled.request.context.flatMap((item) => item.content)).toEqual([
				{ toolCall: { modelToolCallId: "call-1", name: "lookup", inputJson: JSON.stringify({ q: "hi" }) } },
				{ toolResult: { modelToolCallId: "call-1", ...testCase.wire } },
			]);
		}
	});
});
