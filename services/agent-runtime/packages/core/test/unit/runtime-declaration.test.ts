import { describe, expect, test } from "bun:test";
import { MaxProviderRequestToolOutputJsonBytes } from "@tetral/gateway-protocol/src/bounds.js";
import type {
	RuntimeContextEntry,
	RuntimeToolError,
} from "../../src/contracts/runtime.js";
import {
	finalizeRuntimeToolOutput,
	RuntimeBoundedTextSchema,
	RuntimeToolOutputTruncationNotice,
} from "../../src/contracts/runtime.js";
import {
	acceptedInputContextDrafts,
	applyAcceptedInputResult,
	applyAssistantAppendResult,
	applyInternalToolRepairResult,
	applyInterruptToolResults,
	applyToolSettlementToContext,
	assistantAppendFromDraftParts,
	internalToolRepairContext,
	sealAssistantDraft,
} from "../../src/runtime/runtime-declaration.js";
import type { RuntimeAcceptedInputState } from "../../src/thread-loop/thread-state.js";

function messageInput(
	contentJson: string,
): Extract<RuntimeAcceptedInputState, { readonly kind: "messages" }> {
	return {
		workspaceId: "wksp_1",
		sessionId: "sesn_1",
		sessionThreadId: "thr_1",
		bindingId: "bind_1",
		bindingGeneration: 1,
		targetPodUid: "pod_1",
		runtimeInputId: "rin_1",
		inputOrder: 1,
		kind: "messages",
		contentJson,
	};
}

const toolError: RuntimeToolError = {
	type: "runtime_shutdown",
	message: "cancelled by Runtime shutdown",
	retryable: false,
};

describe("Runtime context declaration applicators", () => {
	test("normal input consumes only the canonical Bridge messages payload", () => {
		const drafts = acceptedInputContextDrafts(
			messageInput(
				JSON.stringify({
					messages: [
						{
							id: "db identity is deliberately ignored",
							role: "user",
							origin: "user",
							parts: [
								{
									id: "part identity is deliberately ignored",
									type: "text",
									text: "hello",
								},
								{ type: "unsupported", value: "ignored" },
							],
						},
					],
				}),
			),
		);

		expect(drafts).toEqual([
			{ contextKind: "user", parts: [{ type: "text", text: "hello" }] },
		]);
		expect(() =>
			acceptedInputContextDrafts(
				messageInput(JSON.stringify({ text: "legacy fallback" })),
			),
		).toThrow("accepted input payload has no content");
	});

	test("accepted input result adds only Bridge-assigned context sequences", () => {
		const drafts = [
			{
				contextKind: "user" as const,
				parts: [{ type: "text" as const, text: "hello" }],
			},
		];
		expect(applyAcceptedInputResult(drafts, [7])).toEqual([
			{
				messageSequence: 7,
				contextKind: "user",
				parts: [{ type: "text", text: "hello" }],
			},
		]);
		expect(() => applyAcceptedInputResult(drafts, [])).toThrow(
			"assigned sequence count",
		);
	});

	test("attachment-only accepted input declares no text context", () => {
		const input = messageInput(JSON.stringify({
			messages: [{
				parts: [
					{ type: "image", source: { type: "file", file_id: "file_image" } },
					{ type: "document", source: { type: "file", file_id: "file_document" } },
				],
			}],
		}));
		const drafts = acceptedInputContextDrafts(input);
		expect(drafts).toEqual([]);
		expect(applyAcceptedInputResult(drafts, [])).toEqual([]);
	});

	test("Assistant append remains an open draft until Request End seals it", () => {
		const append = assistantAppendFromDraftParts([
			{ type: "text", text: "working", truncated: false },
			{
				type: "tool",
				modelToolCallId: "call_1",
				toolName: "read_file",
				state: {
					status: "running",
					input: {
						value: { path: "/tmp/a" },
						preview: "{path}",
						truncated: false,
					},
				},
			},
		]);
		const applied = applyAssistantAppendResult({
			modelRequestId: "req_1",
			append,
			result: { messageSequence: 8, createdToolUseEventIds: ["evt_tool_1"] },
		});

		expect(applied.draft).toEqual({
			modelRequestId: "req_1",
			messageSequence: 8,
			parts: [
				{ type: "text", text: "working" },
				{
					type: "tool_call",
					modelToolCallId: "call_1",
					toolName: "read_file",
					canonicalInput: { path: "/tmp/a" },
				},
			],
		});
		expect(applied.activeToolParts[0]?.toolUseEventId).toBe("evt_tool_1");
		expect(sealAssistantDraft(applied.draft)).toEqual({
			messageSequence: 8,
			contextKind: "assistant",
			parts: applied.draft.parts,
		});
	});

	test("Tool settlement pairs by modelToolCallId without rewriting the call", () => {
		const entries: readonly RuntimeContextEntry[] = [
			{
				messageSequence: 8,
				contextKind: "assistant",
				parts: [
					{
						type: "tool_call",
						modelToolCallId: "call_1",
						toolName: "read_file",
						canonicalInput: {},
					},
				],
			},
		];
		const settled = applyToolSettlementToContext({
			entries,
			assistantMessageSequence: 8,
			modelToolCallId: "call_1",
			settlement: {
				type: "completed",
				output: { text: "done", truncated: false },
			},
		});

		expect(settled[0]?.parts).toEqual([
			{
				type: "tool_call",
				modelToolCallId: "call_1",
				toolName: "read_file",
				canonicalInput: {},
			},
			{
				type: "tool_result",
				modelToolCallId: "call_1",
				result: {
					type: "completed",
					output: { text: "done" },
				},
			},
		]);
		expect(() =>
			applyToolSettlementToContext({
				entries: settled,
				assistantMessageSequence: 8,
				modelToolCallId: "call_1",
				settlement: { type: "cancelled" },
			}),
		).toThrow("missing or already terminal");

		const cancelled = applyToolSettlementToContext({
			entries,
			assistantMessageSequence: 8,
			modelToolCallId: "call_1",
			settlement: {
				type: "cancelled",
				error: {
					type: "runtime",
					code: "runtime_invalid_sequence",
					message: "internal cancellation detail",
					retryable: false,
					fatal: true,
				},
			},
		});
		expect(cancelled[0]?.parts[1]).toEqual({
			type: "tool_result",
			modelToolCallId: "call_1",
			result: { type: "cancelled" },
		});
	});

	test("truncated Tool output stores one bounded final provider-visible text", () => {
		const emptyEnvelopeBytes = Buffer.byteLength(
			JSON.stringify({ text: "", truncated: true }),
			"utf8",
		);
		const bounded = RuntimeBoundedTextSchema.parse({
			text: "x".repeat(
				MaxProviderRequestToolOutputJsonBytes - emptyEnvelopeBytes,
			),
			truncated: true,
		});
		const finalOutput = finalizeRuntimeToolOutput(bounded);

		expect(finalOutput.truncated).toBe(true);
		expect(finalOutput.text.endsWith(RuntimeToolOutputTruncationNotice)).toBe(
			true,
		);
		expect(Buffer.byteLength(JSON.stringify(finalOutput), "utf8")).toBeLessThanOrEqual(
			MaxProviderRequestToolOutputJsonBytes,
		);

		const settled = applyToolSettlementToContext({
			entries: [
				{
					messageSequence: 8,
					contextKind: "assistant",
					parts: [
						{
							type: "tool_call",
							modelToolCallId: "call_truncated",
							toolName: "read_file",
							canonicalInput: {},
						},
					],
				},
			],
			assistantMessageSequence: 8,
			modelToolCallId: "call_truncated",
			settlement: { type: "completed", output: bounded },
		});
		expect(settled[0]?.parts[1]).toEqual({
			type: "tool_result",
			modelToolCallId: "call_truncated",
			result: { type: "completed", output: { text: finalOutput.text } },
		});
		expect(settled[0]?.parts[1]).not.toHaveProperty("result.output.truncated");
	});

	test("interrupt settlement preserves the exact typed Tool error", () => {
		const entries: readonly RuntimeContextEntry[] = [
			{
				messageSequence: 8,
				contextKind: "assistant",
				parts: [
					{
						type: "tool_call",
						modelToolCallId: "call_1",
						toolName: "read_file",
						canonicalInput: {},
					},
				],
			},
		];
		const updated = applyInterruptToolResults({
			entries,
			routes: [
				{
					toolUseEventId: "evt_tool_1",
					assistantMessageSequence: 8,
					modelToolCallId: "call_1",
				},
			],
			results: [
				{
					toolUseEventId: "evt_tool_1",
					result: { type: "error", error: toolError },
				},
			],
		});
		expect(updated[0]?.parts[1]).toEqual({
			type: "tool_result",
			modelToolCallId: "call_1",
			result: { type: "error", error: toolError },
		});
	});

	test("internal repair emits one paired Assistant context draft", () => {
		const repair = internalToolRepairContext({
			modelToolCallId: "call_invalid",
			toolName: "missing_tool",
			canonicalInput: {},
			error: {
				type: "runtime",
				code: "runtime_invalid_sequence",
				message: "invalid",
				retryable: false,
				fatal: false,
			},
		});
		expect(repair.contextKind).toBe("assistant");
		expect(repair.parts.map((part) => part.type)).toEqual([
			"tool_call",
			"tool_result",
		]);
	});

	test("internal repair joins the request-owned open Assistant draft", () => {
		const repair = internalToolRepairContext({
			modelToolCallId: "call_invalid",
			toolName: "missing_tool",
			canonicalInput: {},
			error: {
				type: "runtime",
				code: "runtime_invalid_sequence",
				message: "invalid",
				retryable: false,
				fatal: false,
			},
		});
		const repairOnly = applyInternalToolRepairResult({
			modelRequestId: "mreq_1",
			assignedMessageSequence: 8,
			context: repair,
		});
		expect(repairOnly).toMatchObject({
			modelRequestId: "mreq_1",
			messageSequence: 8,
			parts: [
				{ type: "tool_call", modelToolCallId: "call_invalid" },
				{ type: "tool_result", modelToolCallId: "call_invalid" },
			],
		});

		const mixed = applyInternalToolRepairResult({
			modelRequestId: "mreq_1",
			assignedMessageSequence: 8,
			existingDraft: {
				modelRequestId: "mreq_1",
				messageSequence: 8,
				parts: [{ type: "text", text: "before repair" }],
			},
			context: repair,
		});
		expect(mixed.parts.map((part) => part.type)).toEqual([
			"text",
			"tool_call",
			"tool_result",
		]);
		expect(() =>
			applyInternalToolRepairResult({
				modelRequestId: "mreq_1",
				assignedMessageSequence: 9,
				existingDraft: repairOnly,
				context: repair,
			}),
		).toThrow("changed the open Request draft identity");
	});
});
