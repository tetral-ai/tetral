import { afterAll, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import type { LLMRequest } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { internalToolRepairKey } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { providerAttachmentsForTest } from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import {
	bridgeContext,
	bridgeContextVectors,
	coldRequests,
	liveHotContexts,
	pinnedProviderRequests,
	pinProviderRequest,
	preloadOwnership,
	providerContext,
	providerRequest,
	providerRequestVectors,
	restoreCold,
	settleHot,
	vectorNames,
} from "./bridge-context-support.js";

// Each row states what the Runtime must restore from one envelope Bridge
// produced for a stored state (see protocol/testdata/README.md).
// Rows compare restored values with literals written from the claim.
type Restored = Omit<
	Awaited<ReturnType<typeof restoreCold>>,
	"entries" | "activeToolReferences" | "nextStep"
> & {
	readonly entries: unknown;
	readonly activeToolReferences: unknown;
	readonly nextStep: { readonly action: string };
};
type Row = {
	readonly check: (restored: Restored, contextJson: string) => void;
	// The provider requests a fresh Thread starts from this envelope.
	readonly requests?: (requests: readonly LLMRequest[]) => void;
	// The vector's ProviderRequest is pinned for the Gateway's tests.
	readonly providerRequest?: true;
	// Settling the Tool hot from the envelope Bridge sent before the
	// settlement reaches the same provider context as this cold envelope.
	readonly hot?: {
		readonly base: string;
		readonly modelToolCallId: string;
		readonly outcome: unknown;
	};
	// A live Runtime process sent this context hot before Bridge closed the
	// request out (runtime-hot-context/, written by the integration tests).
	readonly liveHotContext?: true;
	// What the cold provider context must hold.
	readonly context?: (context: unknown[]) => void;
};

// An interrupted request keeps only its signed reasoning and its one Tool
// call and result, in that order; its failed text is gone.
function interruptedContext(
	failedText: string,
	reasoning: string,
	signature: string,
	modelToolCallId: string,
) {
	return (context: unknown[]) => {
		const parts = (context as { content: Record<string, { text?: string; metadataJson?: string; modelToolCallId?: string }>[] }[]).flatMap(
			(entry) => entry.content,
		);
		expect(JSON.stringify(parts)).not.toContain(failedText);
		const kinds = parts.flatMap((part) =>
			part.reasoning ? ["reasoning"] : part.toolCall ? ["call"] : part.toolResult ? ["result"] : [],
		);
		expect(kinds).toEqual(["reasoning", "call", "result"]);
		const reasoningPart = parts.find((part) => part.reasoning)?.reasoning;
		expect(reasoningPart?.text).toBe(reasoning);
		expect(JSON.parse(reasoningPart?.metadataJson ?? "null")).toEqual({ anthropic: { signature } });
		expect(parts.find((part) => part.toolCall)?.toolCall?.modelToolCallId).toBe(modelToolCallId);
		expect(parts.find((part) => part.toolResult)?.toolResult?.modelToolCallId).toBe(modelToolCallId);
	};
}

const priorEntries = [
	{ messageSequence: 1, contextKind: "user", parts: [{ type: "text", text: "prior-user" }] },
	{ messageSequence: 2, contextKind: "assistant", parts: [{ type: "text", text: "prior-answer" }] },
];

// A failed Request keeps nothing of its Assistant until its retained Tool
// settles; then only the signed reasoning, the call and its result remain.
function failedEndAwaitingAcceptedTool(restored: Restored, contextJson: string) {
	const selected = toolUseEventId(contextJson, "call-read-note");
	expect(restored.entries).toEqual(priorEntries);
	expect(restored.currentRequestMessage).toEqual({
		modelRequestId: "request",
		assistantMessageSequence: 3,
	});
	expect(restored.nextStep).toMatchObject({
		action: "resume_tool_routes",
		toolUseEventIds: [selected],
	});
	expect(restored.toolRouteView.routes).toMatchObject([
		{ toolUseEventId: selected, disposition: "resume_sandbox_execution" },
	]);
	expect(restored.activeToolReferences).toEqual([
		{
			toolUseEventId: selected,
			modelRequestId: "request",
			modelToolCallId: "call-read-note",
			assistantMessageSequence: 3,
			disposition: "resume_sandbox_execution",
		},
	]);
}

// A pending ask Tool of the Request at message 1 waits for its decision; a
// decision resumes its route.
function pendingAskTool(nextStep: string, disposition: string) {
	return (restored: Restored, contextJson: string) => {
		expect(restored.nextStep.action).toBe(nextStep);
		expect(restored.activeToolReferences).toEqual([
			{
				toolUseEventId: toolUseEventId(contextJson, "call-a"),
				modelRequestId: "request",
				modelToolCallId: "call-a",
				assistantMessageSequence: 1,
				disposition,
			},
		]);
	};
}

// An accepted Sandbox execution whose result the Runtime has not consumed is
// resumed, not re-run.
function awaitingSandboxResult(modelToolCallId: string) {
	return (restored: Restored, contextJson: string) => {
		const pending = toolUseEventId(contextJson, modelToolCallId);
		expect(restored.nextStep).toMatchObject({
			action: "resume_tool_routes",
			toolUseEventIds: [pending],
		});
		expect(restored.toolRouteView.routes).toMatchObject([
			{ toolUseEventId: pending, disposition: "resume_sandbox_execution" },
		]);
	};
}

function nextStepOnly(action: string) {
	return (restored: Restored) => expect(restored.nextStep.action).toBe(action);
}

const rows: Record<string, Row> = {
	// Turn facts start after the compaction floor; the terminal failure below
	// it is not needed to restore the Thread.
	"compaction-floor-after-terminal-closeout": {
		check: (restored) =>
			expect(restored.entries).toEqual([
				{ messageSequence: 2, contextKind: "compaction", parts: [{ type: "text", text: "summary" }] },
			]),
	},
	"content-open": { check: nextStepOnly("await_request_end") },
	"content-latest-no-content": { check: nextStepOnly("await_request_end") },
	"content-closed": { check: nextStepOnly("await_input") },
	"content-exhausted": { check: nextStepOnly("await_input") },
	"content-success-end-pending-tool": {
		check: pendingAskTool("finish_idle", "requires_user_action"),
	},
	"content-abnormal-end-retained-pending-tool": {
		check: pendingAskTool("finish_idle", "requires_user_action"),
	},
	"content-decided-allow": {
		check: pendingAskTool("resume_tool_routes", "resume_approval_settlement"),
	},
	"content-decided-deny": {
		check: pendingAskTool("resume_tool_routes", "resume_approval_settlement"),
	},
	"content-no-content-end": {
		check: (restored) => {
			expect(restored.nextStep.action).toBe("finish_idle");
			expect(restored.entries).toEqual([]);
			expect(restored.currentRequestMessage).toBeNull();
			expect(restored.toolRouteView.routes).toEqual([]);
		},
	},
	"retention-failed-end-accepted-tool": { check: failedEndAwaitingAcceptedTool },
	// A terminal result the Runtime has not consumed changes nothing it restores.
	"retention-failed-end-terminal-unconsumed": { check: failedEndAwaitingAcceptedTool },
	"retention-failed-end-settled": {
		check: (restored) => {
			expect(restored.entries).toEqual([
				...priorEntries,
				{
					messageSequence: 3,
					contextKind: "assistant",
					parts: [
						{
							type: "reasoning",
							text: "reason-before-tool",
							providerMetadata: { anthropic: { signature: "fixture-signature-tool" } },
						},
						{
							type: "tool_call",
							modelToolCallId: "call-read-note",
							toolName: "Read",
							canonicalInput: { file_path: "/workspace/note.txt" },
						},
						{
							type: "tool_result",
							modelToolCallId: "call-read-note",
							result: { type: "completed", output: { text: "fixture-note" } },
						},
					],
				},
			]);
			expect(restored.toolRouteView.routes).toEqual([]);
			expect(restored.activeToolReferences).toEqual([]);
		},
	},
	// Results stored in the reverse of call order restore and reach the
	// provider request; the Gateway's tests hold each call before its result.
	"multi-tool-settled-out-of-order": {
		check: nextStepOnly("prepare_next_request"),
		providerRequest: true,
	},
	"durable-tool-cancelled-awaiting-result": { check: awaitingSandboxResult("call_durable_cancel") },
	// The internal cancellation error stays out of the conversation.
	"durable-tool-cancelled-settled": {
		check: nextStepOnly("prepare_next_request"),
		providerRequest: true,
		hot: {
			base: "durable-tool-cancelled-awaiting-result",
			modelToolCallId: "call_durable_cancel",
			outcome: {
				type: "cancelled",
				error: {
					type: "runtime",
					code: "unknown",
					message: "internal cancellation detail",
					retryable: false,
					fatal: false,
				},
			},
		},
	},
	"durable-tool-error-awaiting-result": { check: awaitingSandboxResult("call_durable_error") },
	"durable-tool-error-settled": {
		check: nextStepOnly("prepare_next_request"),
		providerRequest: true,
		hot: {
			base: "durable-tool-error-awaiting-result",
			modelToolCallId: "call_durable_error",
			outcome: {
				type: "error",
				error: {
					type: "runtime",
					code: "runtime_invalid_sequence",
					message: "path does not exist\n\nPartial result:\n{}",
					retryable: false,
					fatal: false,
				},
			},
		},
	},
	"durable-tool-truncated-awaiting-result": {
		check: awaitingSandboxResult("call_durable_truncation"),
	},
	// The Runtime appends its truncation notice hot; Bridge stores only the
	// final text.
	"durable-tool-truncated-settled": {
		check: nextStepOnly("prepare_next_request"),
		providerRequest: true,
		hot: {
			base: "durable-tool-truncated-awaiting-result",
			modelToolCallId: "call_durable_truncation",
			outcome: { type: "completed", output: { text: "partial output", truncated: true } },
		},
	},
	// The repair the Runtime declared is the one it derives again from the
	// stored facts.
	"invalid-tool-repair-continued": {
		check: (restored) => {
			expect(restored.cold.turnFacts.internalRepairs).toMatchObject([
				{
					repairKey: internalToolRepairKey(
						"model_request-2",
						"call_invalid_tool_repair_fixture",
						"exec_command",
					),
				},
			]);
			expect(restored.nextStep.action).toBe("finish_idle");
		},
		providerRequest: true,
	},
	// An attachment-only input starts one request that carries both files and
	// no context, the same request the hot path starts from the Runtime input
	// vector (attachment-input-composition.test.ts).
	"attachment-only-input": {
		check: nextStepOnly("prepare_next_request"),
		requests: (requests) => {
			expect(requests).toHaveLength(1);
			expect(requests[0]?.context).toEqual([]);
			expect(requests[0]?.attachments).toEqual(
				providerAttachmentsForTest([
					{
						transient: undefined,
						fileBacked: { sourceEventId: "sevt_0000000000000001", fileId: "file_first_queued_image" },
						mime: "image/png",
						filename: "image.png",
					},
					{
						transient: undefined,
						fileBacked: { sourceEventId: "sevt_0000000000000001", fileId: "file_first_queued_document" },
						mime: "text/plain",
						filename: "notes.txt",
					},
				]),
			);
		},
	},
	// Once a request consumed the attachments, nothing is left to send and the
	// Thread goes idle.
	"attachment-consumed": {
		check: nextStepOnly("finish_idle"),
		requests: (requests) => expect(requests).toEqual([]),
	},
	"mcp-five-tools-settled": { check: nextStepOnly("prepare_next_request"), providerRequest: true },
	// A child's first request carries its parent's prefix once and no parent
	// Tool call.
	"subagent-first-request-with-parent-prefix": {
		check: (restored) => {
			const context = JSON.stringify(
				providerContext(
					restored.entries as Parameters<typeof providerContext>[0],
					restored.cold.threadContextPrefix?.entries,
				),
			);
			expect(context.split("safe parent prefix text")).toHaveLength(2);
			expect(context).not.toContain("toolCall");
		},
		providerRequest: true,
	},
	// Mail delivered to the child is restored as its own pending input.
	"subagent-mail-delivered": {
		check: (restored) =>
			expect(restored.cold.pendingAgentMail).toMatchObject([
				{ content: "inspect the target-owned durable envelope" },
			]),
	},
	// Cleanup settles the expired Sandbox Tool of an idle Thread as a
	// non-retryable error that waits in context for the next input.
	"cleanup-expired-tool-error": { check: nextStepOnly("await_input"), providerRequest: true },
	"interrupt-closed-out": {
		check: () => undefined,
		liveHotContext: true,
		context: interruptedContext(
			"failed interrupt partial text",
			"interrupt Tool reasoning",
			"sig_interrupt_composition",
			"call_interrupt_composition",
		),
	},
	"interrupt-joined-replay-closed-out": {
		check: () => undefined,
		context: interruptedContext(
			"failed partial text",
			"provider-declared reasoning",
			"sig_provider_context",
			"call_interrupt_joined_replay",
		),
	},
	// Held by the ownership cases below.
	"ownership-pending-ask-tool": { check: () => undefined },
};

describe("Bridge context vectors", () => {
	test("every vector has a row and every row a vector", () => {
		expect(vectorNames(bridgeContextVectors)).toEqual(Object.keys(rows).sort());
		expect(vectorNames(liveHotContexts)).toEqual(
			Object.entries(rows)
				.filter(([, row]) => row.liveHotContext)
				.map(([name]) => name)
				.sort(),
		);
		expect(vectorNames(providerRequestVectors)).toEqual(
			Object.entries(rows)
				.filter(([, row]) => row.providerRequest)
				.map(([name]) => name)
				.sort(),
		);
	});

	// The completeness test above cannot see a declared ProviderRequest this
	// file stopped pinning; its stale vector would keep the Gateway green.
	const ran = new Set<string>();
	afterAll(() => {
		const unpinned = [...ran].filter(
			(name) => rows[name]?.providerRequest && !pinnedProviderRequests.has(name),
		);
		if (unpinned.length > 0) {
			throw new Error(`declared ProviderRequests not pinned: ${unpinned.join(", ")}`);
		}
	});

	for (const [name, row] of Object.entries(rows)) {
		test(`${name} restores what its stored state holds`, async () => {
			ran.add(name);
			const contextJson = bridgeContext(name);
			const restored = await restoreCold(contextJson);
			row.check(restored, contextJson);
			row.requests?.(await coldRequests(contextJson));
			if (row.hot === undefined && !row.providerRequest && row.context === undefined) return;
			const cold = providerContext(restored.entries, restored.cold.threadContextPrefix?.entries);
			row.context?.(cold);
			if (row.liveHotContext) {
				expect(JSON.parse(JSON.stringify(cold))).toEqual(
					JSON.parse(readFileSync(new URL(`${name}.json`, liveHotContexts), "utf8")),
				);
			}
			if (row.hot !== undefined) {
				const base = bridgeContext(row.hot.base);
				expect(
					await settleHot(base, toolUseEventId(base, row.hot.modelToolCallId), row.hot.outcome),
				).toEqual(cold);
			}
			if (row.providerRequest) {
				pinProviderRequest(name, providerRequest(cold, restored.cold.thread));
			}
		});
	}

	// Each corruption breaks one ownership fact of a legal envelope. Preload
	// rejects it before any work and says why without disclosing content.
	const corruptions: Record<string, (wire: Record<string, unknown>) => void> = {
		"missing-message": (wire) => {
			wire.messages = [];
		},
		"missing-call": (wire) => {
			const [message] = wire.messages as { parts: { type: string }[] }[];
			if (message === undefined) throw new Error("ownership vector has no message");
			message.parts = message.parts.filter((part) => part.type !== "tool_call");
		},
		"wrong-request-pair": (wire) => {
			(wire.currentRequestMessage as { modelRequestId: string }).modelRequestId = "other-request";
		},
		"competing-route-owners": (wire) => {
			const [pending] = wire.pendingToolUses as { toolUseEventId: string }[];
			wire.pendingSandboxExecutions = [
				{
					toolUseEventId: pending?.toolUseEventId,
					modelRequestId: "request",
					modelToolCallId: "call-ownership",
					toolName: "Read",
					input: { file_path: "fixture-sensitive-content" },
					executionState: "running",
				},
			];
		},
		"sealed-without-route": (wire) => {
			wire.pendingToolUses = [];
		},
	};
	const ownershipCases: [string, ((wire: Record<string, unknown>) => void) | undefined][] = [
		["valid", undefined],
		...Object.entries(corruptions),
	];
	test.each(ownershipCases)("ownership %s", async (_name, corrupt) => {
		const wire = JSON.parse(bridgeContext("ownership-pending-ask-tool")) as Record<string, unknown>;
		corrupt?.(wire);
		const observation = await preloadOwnership(JSON.stringify(wire));
		const reported = JSON.stringify(observation);
		expect(reported).not.toContain("fixture-sensitive-content");
		expect(reported).not.toContain("fixture-secret-sentinel");
		expect(reported.length).toBeLessThanOrEqual(4096);
		expect(observation.calls).toEqual({ inputs: 0, tools: 0, provider: 0 });
		if (corrupt === undefined) {
			expect(observation.preload.ok).toBe(true);
			expect(observation.observed).toBe(true);
		} else {
			expect(observation.preload.ok).toBe(false);
			expect(observation.observed).toBe(false);
			expect(observation.preload.ok ? "" : observation.preload.reason).not.toBe("");
		}
	});
});

function toolUseEventId(contextJson: string, modelToolCallId: string): string {
	const envelope = JSON.parse(contextJson) as {
		pendingToolUses: { toolUseEventId: string; modelToolCallId: string }[];
		pendingSandboxExecutions: { toolUseEventId: string; modelToolCallId: string }[];
	};
	const owner = [...envelope.pendingToolUses, ...envelope.pendingSandboxExecutions].find(
		(pending) => pending.modelToolCallId === modelToolCallId,
	);
	if (owner === undefined) throw new Error(`no pending Tool ${modelToolCallId}`);
	return owner.toolUseEventId;
}
