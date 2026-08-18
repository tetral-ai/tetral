import { describe, expect, test } from "bun:test";
import { ProviderContextRole } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { lowerProviderRequest } from "../../../../../gateway/packages/lowering/src/request.js";
import { GatewayProviderRules } from "../../../../../gateway/packages/lowering/src/rules/index.js";
import { validateProviderRequest } from "../../../../../gateway/packages/protocol/src/bounds.js";
import type {
	RuntimeAssistantContextAppend,
	RuntimeContextEntry,
} from "../../src/contracts/runtime.js";
import { ProviderStreamAccumulator } from "../../src/runtime/accumulator.js";
import { toGatewayProviderContext } from "../../src/runtime/context-projection.js";
import {
	applyAssistantAppendResult,
	applyToolSettlementToContext,
} from "../../src/runtime/runtime-declaration.js";
import { ContextManager } from "../../src/session/context-manager.js";
import { assembleProviderCallRequest } from "../../src/thread-loop/provider-request.js";

describe("Runtime context lifetimes", () => {
	test("a request accumulator observes its ContextManager owner instead of a construction snapshot", () => {
		const context = new ContextManager("session", [
			{
				messageSequence: 1,
				contextKind: "user",
				parts: [{ type: "text", text: "before request" }],
			},
		]);
		const accumulator = new ProviderStreamAccumulator({
			modelRequestId: "request_owner",
			requestId: "provider_request_owner",
			workspaceId: "default",
			sessionId: "session",
			sessionThreadId: "thread",
			bindingId: "binding",
			bindingGeneration: 1,
			targetPodUid: "pod",
			contextOwner: context,
			writer: {} as never,
		});
		context.appendEntry({
			messageSequence: 2,
			contextKind: "runtime_notification",
			parts: [{ type: "text", text: "committed concurrently" }],
		});

		expect(accumulator.contextEntries()).toEqual(context.entries());
		expect(accumulator.applyRequestEndAppend(undefined, {})).toBe(true);
		expect(context.entries().map((entry) => entry.messageSequence)).toEqual([
			1, 2,
		]);
	});

	test("an open Assistant draft stays outside provider history until it is sealed", () => {
		const userEntry: RuntimeContextEntry = {
			messageSequence: 1,
			contextKind: "user",
			parts: [{ type: "text", text: "run both tools" }],
		};
		const append: RuntimeAssistantContextAppend = {
			parts: [
				{
					type: "reasoning",
					text: "checking",
					truncated: false,
					providerMetadata: { anthropic: { signature: "sig" } },
				},
				{
					type: "tool",
					modelToolCallId: "call_a",
					toolName: "Read",
					state: {
						status: "running",
						input: {
							value: { path: "a" },
							preview: '{"path":"a"}',
							truncated: false,
						},
					},
				},
				{
					type: "tool",
					modelToolCallId: "call_b",
					toolName: "Grep",
					state: {
						status: "running",
						input: {
							value: { pattern: "b" },
							preview: '{"pattern":"b"}',
							truncated: false,
						},
					},
				},
			],
		};
		const applied = applyAssistantAppendResult({
			modelRequestId: "request_open",
			append,
			result: {
				messageSequence: 2,
				createdToolUseEventIds: ["tool_a", "tool_b"],
			},
		});
		const context = new ContextManager("session", [userEntry]);
		context.installOpenRequestDraft(applied.draft);

		expect(context.providerEntries()).toEqual([userEntry]);
		expect(toGatewayProviderContext(context.providerEntries())).toEqual({
			ok: true,
			context: [
				{
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
					content: [{ text: { text: "run both tools" } }],
				},
			],
		});

		const sealed = context.sealOpenRequestDraft();
		expect(sealed).toEqual({
			messageSequence: 2,
			contextKind: "assistant",
			parts: applied.draft.parts,
		});
		expect(context.openRequestDraft()).toBeUndefined();
		const projected = toGatewayProviderContext(context.providerEntries());
		expect(projected).toMatchObject({
			ok: true,
			context: [
				{ role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER },
				{
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
					content: [
						{ reasoning: { text: "checking" } },
						{
							toolCall: {
								modelToolCallId: "call_a",
								name: "Read",
								inputJson: '{"path":"a"}',
							},
						},
						{
							toolCall: {
								modelToolCallId: "call_b",
								name: "Grep",
								inputJson: '{"pattern":"b"}',
							},
						},
					],
				},
			],
		});
	});

	test("independent out-of-order Tool Results pair only through modelToolCallId", () => {
		const sealed: RuntimeContextEntry = {
			messageSequence: 2,
			contextKind: "assistant",
			parts: [
				{
					type: "tool_call",
					modelToolCallId: "call_a",
					toolName: "Read",
					canonicalInput: { path: "a" },
				},
				{
					type: "tool_call",
					modelToolCallId: "call_b",
					toolName: "Grep",
					canonicalInput: { pattern: "b" },
				},
			],
		};
		const context = new ContextManager("session", [sealed]);

		context.replaceEntries(
			applyToolSettlementToContext({
				entries: context.entries(),
				assistantMessageSequence: 2,
				modelToolCallId: "call_b",
				settlement: {
					type: "error",
					error: {
						type: "runtime",
						code: "provider_tool_protocol_error",
						message: "b failed",
						retryable: false,
						fatal: true,
						reason: "runtime_contract_validation",
					},
				},
			}),
		);
		const onePending = toGatewayProviderContext(context.providerEntries());
		expect(onePending).toMatchObject({
			ok: true,
			context: [
				{
					content: [
						{ toolCall: { modelToolCallId: "call_a" } },
						{ toolCall: { modelToolCallId: "call_b" } },
						{
							toolResult: {
								modelToolCallId: "call_b",
								error: { errorJson: expect.any(String) },
							},
						},
					],
				},
			],
		});

		context.replaceEntries(
			applyToolSettlementToContext({
				entries: context.entries(),
				assistantMessageSequence: 2,
				modelToolCallId: "call_a",
				settlement: {
					type: "completed",
					output: { text: "a complete", truncated: false },
				},
			}),
		);
		const fullySettled = toGatewayProviderContext(context.providerEntries());
		expect(fullySettled).toMatchObject({
			ok: true,
			context: [
				{
					content: [
						{ toolCall: { modelToolCallId: "call_a" } },
						{ toolCall: { modelToolCallId: "call_b" } },
						{ toolResult: { modelToolCallId: "call_b" } },
						{ toolResult: { modelToolCallId: "call_a" } },
					],
				},
			],
		});

		const cold = new ContextManager(
			"session",
			JSON.parse(
				JSON.stringify(context.entries()),
			) as readonly RuntimeContextEntry[],
		);
		expect(cold.entries()).toEqual(context.entries());
		expect(toGatewayProviderContext(cold.providerEntries())).toEqual(
			fullySettled,
		);

		if (!fullySettled.ok)
			throw new Error("fully settled context failed provider projection");
		for (const rules of GatewayProviderRules) {
			const assembled = assembleProviderCallRequest({
				identity: {
					workspaceId: "default",
					sessionId: "session",
					sessionThreadId: "thread",
					bindingId: "binding",
					bindingGeneration: 1,
					targetPodUid: "pod",
					runtimeBindingToken: "runtime-binding-token",
				},
				requestId: `provider_${rules.providerFamily}`,
				modelRequestId: "request_next",
				currentModel: { providerId: rules.providerId, modelId: rules.modelId },
				providerContext: fullySettled.context,
				runtime: {
					systemInstructions: "context lifetime composition",
					timeoutMs: 30_000,
				},
			});
			expect(assembled.ok, rules.providerFamily).toBe(true);
			if (!assembled.ok) continue;
			expect(
				validateProviderRequest(assembled.request),
				rules.providerFamily,
			).toEqual({ ok: true });

			const lowered = lowerProviderRequest(assembled.request, rules, {
				modelOutputTokenLimit: 32_000,
			});
			const toolItems = lowered.messages.flatMap((message) =>
				Array.isArray(message.content)
					? message.content.filter(
							(item) =>
								item.type === "tool-call" || item.type === "tool-result",
						)
					: [],
			);
			expect(
				toolItems.map((item) => [item.type, item.toolCallId]),
				rules.providerFamily,
			).toEqual([
				["tool-call", "call_a"],
				["tool-call", "call_b"],
				["tool-result", "call_b"],
				["tool-result", "call_a"],
			]);
		}
	});
});
