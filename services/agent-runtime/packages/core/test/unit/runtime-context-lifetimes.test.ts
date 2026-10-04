import { describe, expect, test } from "bun:test";
import { ProviderContextRole } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { lowerProviderRequest } from "../../../../../gateway/packages/lowering/src/request.js";
import { GatewayProviderRules } from "../../../../../gateway/packages/lowering/src/rules/index.js";
import { validateProviderRequest } from "../../../../../gateway/packages/protocol/src/bounds.js";
import type {
	RuntimeAssistantContextAppend,
	RuntimeContextEntry,
} from "../../src/contracts/runtime.js";
import { RequestContentProcessor } from "../../src/runtime/accumulator.js";
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
		const accumulator = new RequestContentProcessor({
			modelRequestId: "request_owner",
			requestId: "provider_request_owner",
			workspaceId: "default",
			sessionId: "session",
			sessionThreadId: "thread",
			bindingId: "binding",
			bindingGeneration: 1,
			targetPodUid: "pod",
			runtimeProcessId: "process-test",
			contextOwner: context,activeToolReferences:()=>[],onCommittedApplicationFailure:()=>{},onAssistantMessageCommitted:()=>{},
			writer: {} as never,
		});
		context.appendEntry({
			messageSequence: 2,
			contextKind: "runtime_notification",
			parts: [{ type: "text", text: "committed concurrently" }],
		});

		expect(accumulator.messages()).toEqual(context.historyMessages());
		expect(context.historyMessages().map((entry) => entry.messageSequence)).toEqual([
			1, 2,
		]);
	});

	test("a committed current Assistant stays outside history until checkpoint eligibility changes", () => {
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
		let eligible=false;
 const context=new ContextManager("session",[userEntry],()=>({assistantMessageSequence:2}),message=>message.messageSequence!==2||eligible);
 context.installAssistantMessage(applied.draft);

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

		eligible=true;context.invalidateHistory();
 const sealed=context.currentAssistantMessage();
		expect(sealed).toEqual({
			messageSequence: 2,
			contextKind: "assistant",
			parts: applied.draft.parts,
		});
		expect(context.currentAssistantMessage()).toEqual(applied.draft);
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

		context.replaceMessages(
			applyToolSettlementToContext({
				entries: context.historyMessages(),
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

		context.replaceMessages(
			applyToolSettlementToContext({
				entries: context.historyMessages(),
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
				JSON.stringify(context.historyMessages()),
			) as readonly RuntimeContextEntry[],
		);
		expect(cold.historyMessages()).toEqual(context.historyMessages());
		expect(toGatewayProviderContext(cold.providerEntries())).toEqual(
			fullySettled,
		);

		if (!fullySettled.ok)
			throw new Error("fully settled context failed provider projection");
		for (const rules of GatewayProviderRules) {
			const assembled = assembleProviderCallRequest({
				identity: {
					threadRole: "main", threadVisibility: "public",
					workspaceId: "default",
					sessionId: "session",
					sessionThreadId: "thread",
					bindingId: "binding",
					bindingGeneration: 1,
					targetPodUid: "pod",
					runtimeProcessId: "process-test",
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
				validateProviderRequest({...assembled.request,modelRequestStartEventId:"evt_0000000000000001"}),
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
