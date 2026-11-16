// Layer 1: the Runtime's pinned provider requests, received by the Gateway in
// process.
import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import type { FetchFunction } from "@ai-sdk/provider-utils";
import { lowerProviderRequest } from "@tetral/gateway-lowering/src/request.js";
import type { LoweredProviderMessage } from "@tetral/gateway-lowering/src/request.js";
import { GatewayProviderRules } from "@tetral/gateway-lowering/src/rules/index.js";
import { validateProviderRequest } from "@tetral/gateway-protocol/src/bounds.js";
import { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import type { ResolvedProviderCredential } from "../../src/providers/credentials.js";

// ProviderRequests the Runtime built from Bridge envelopes, written by the
// Runtime's tests (see testdata/README.md at the repository root). Each row
// states what every approved model path must send for one of them.
const vectors = new URL("../../../../../../testdata/provider-requests/", import.meta.url);

type ToolPair = {
	readonly id: string;
	readonly name: string;
	readonly input: unknown;
	readonly output: unknown;
	readonly isError?: true;
};
type Row = {
	readonly tools: readonly ToolPair[];
	// Assistant text and reasoning the Runtime declared. Every rule set keeps
	// each and invents no Assistant text; it may send declared reasoning as text.
	readonly assistantText: readonly string[];
	// The request also reaches each provider family's HTTP body.
	readonly wire?: {
		// Text that appears exactly once in the body.
		readonly once?: string;
	};
};

const cancelled = { type: "text", text: "[tool execution cancelled]" };

// The Runtime sends a Tool error as the error object it declared.
function toolError(message: string) {
	return { error: { type: "runtime_invalid_sequence", message, retryable: false } };
}

const rows: Record<string, Row> = {
	"cleanup-expired-tool-error": {
		tools: [
			{
				id: "call_cleanup_narrow_tool",
				name: "Read",
				input: {},
				output: {
					error: {
						type: "cleanup_expired",
						message: "Sandbox tool execution expired during session cleanup.",
						retryable: false,
					},
				},
				isError: true,
			},
		],
		assistantText: [],
	},
	// The internal cancellation error stays out; the model sees the fixed text.
	"durable-tool-cancelled-settled": {
		tools: [{ id: "call_durable_cancel", name: "Read", input: {}, output: cancelled, isError: true }],
		assistantText: [],
	},
	"durable-tool-error-settled": {
		tools: [
			{
				id: "call_durable_error",
				name: "Read",
				input: { file_path: "/missing.txt" },
				output: toolError("path does not exist\n\nPartial result:\n{}"),
				isError: true,
			},
		],
		assistantText: ["provider-declared reasoning"],
	},
	// Only the final text with its notice; the truncation flag is not output.
	"durable-tool-truncated-settled": {
		tools: [
			{
				id: "call_durable_truncation",
				name: "Read",
				input: {},
				output: {
					text: "partial output\n\n[Tool output was truncated to fit the provider context limit.]",
				},
			},
		],
		assistantText: [],
	},
	// An internal repair reaches the model as one call/error pair.
	"invalid-tool-repair-continued": {
		tools: [
			{
				id: "call_invalid_tool_repair_fixture",
				name: "exec_command",
				input: { command: "must not execute" },
				output: toolError("disabled or unknown tool call: exec_command"),
				isError: true,
			},
		],
		assistantText: ["continuing", "continued"],
		wire: {},
	},
	"mcp-five-tools-settled": {
		tools: [
			{
				id: "call_mcp_durable_claim",
				name: "create_issue",
				input: { body: "Details", title: "Bug" },
				output: { text: "takeover claimant" },
			},
			{
				id: "call_mcp_production_cancelled",
				name: "create_issue",
				input: { mode: "cancel-between" },
				output: cancelled,
				isError: true,
			},
			{
				id: "call_mcp_oauth_success",
				name: "create_issue",
				input: { mode: "oauth-success" },
				output: { text: "oauth refreshed" },
			},
			{
				id: "call_mcp_oauth_failure",
				name: "create_issue",
				input: { mode: "oauth-failure" },
				output: { error: { type: "tool_error", message: "MCP tool execution is unavailable." } },
				isError: true,
			},
			{
				id: "call_mcp_production_cleanup",
				name: "create_issue",
				input: { mode: "cleanup" },
				output: { text: "reacquired claimant" },
			},
		],
		assistantText: [],
	},
	// Results stored in the reverse of call order still pair with their calls.
	"multi-tool-settled-out-of-order": {
		tools: [
			{ id: "call_multi_a", name: "Read", input: {}, output: { text: "A complete" } },
			{ id: "call_multi_b", name: "Glob", input: {}, output: { text: "B complete" } },
		],
		assistantText: [],
	},
	// A child's first request carries its parent's prefix once and no Tool.
	"subagent-first-request-with-parent-prefix": {
		tools: [],
		assistantText: [],
		wire: { once: "safe parent prefix text" },
	},
};

function vectorNames(): string[] {
	return readdirSync(vectors)
		.filter((name) => name.endsWith(".json"))
		.map((name) => name.slice(0, -".json".length))
		.sort();
}

function runtimeRequest(name: string, providerId: string, modelId: string): ProviderRequest {
	const request = ProviderRequest.fromJSON(
		JSON.parse(readFileSync(new URL(`${name}.json`, vectors), "utf8")),
	);
	return { ...request, model: { providerId, modelId, variant: "" } };
}

describe("Runtime ProviderRequest vectors", () => {
	test("every vector has a row and every row a vector", () => {
		expect(vectorNames()).toEqual(Object.keys(rows).sort());
	});

	const lowering = Object.keys(rows).flatMap((name) =>
		GatewayProviderRules.map(
			(rules) => [name, `${rules.providerId}/${rules.modelId}`, rules] as const,
		),
	);
	// Cache placement and similar options are each rule set's own; the claim is
	// the pairing and the text.
	test.each(lowering)("%s lowers for %s", (name, _model, rules) => {
		const row = rows[name] as Row;
		const request = runtimeRequest(name, rules.providerId, rules.modelId);
		expect(validateProviderRequest(request)).toMatchObject({ ok: true });
		const { messages } = lowerProviderRequest(request, rules, { modelOutputTokenLimit: 32_000 });

		const calls = partsOf(messages, "assistant", "tool-call");
		const results = partsOf(messages, "tool", "tool-result");
		expect(calls.map(({ part }) => part)).toMatchObject(
			row.tools.map((tool) => ({ toolCallId: tool.id, toolName: tool.name })),
		);
		expect(results).toHaveLength(row.tools.length);
		for (const tool of row.tools) {
			const call = calls.find(({ part }) => part.toolCallId === tool.id);
			const result = results.find(({ part }) => part.toolCallId === tool.id);
			expect(call?.part.input).toEqual(tool.input);
			expect(result?.part).toMatchObject({ toolName: tool.name });
			expect(result?.part.output).toEqual(tool.output);
			expect(result?.part.isError).toBe(tool.isError);
			expect(result?.message).toBeGreaterThan(call?.message ?? Number.POSITIVE_INFINITY);
		}
		const declared = [...row.assistantText];
		for (const { part } of partsOf(messages, "assistant", "text")) {
			expect(declared).toContain(part.text);
			declared.splice(declared.indexOf(part.text), 1);
		}
		const lowered = JSON.stringify(messages);
		for (const text of row.assistantText) expect(lowered).toContain(text);
	});

	const wire = Object.entries(rows).flatMap(([name, row]) =>
		row.wire === undefined ? [] : families.map((family) => [name, family.family, family] as const),
	);
	test.each(wire)("%s reaches the %s HTTP body", async (name, _family, family) => {
		const row = rows[name] as Row;
		const body = await httpBody(family, runtimeRequest(name, family.providerId, family.modelId));
		const encoded = JSON.stringify(body);
		const pairs = wireToolPairs(family.family, body);
		expect(pairs.calls.map(({ id, name, input }) => ({ id, name, input }))).toEqual(
			row.tools.map(({ id, name, input }) => ({ id, name, input })),
		);
		expect(pairs.results).toHaveLength(row.tools.length);
		for (const tool of row.tools) {
			const call = pairs.calls.find((candidate) => candidate.id === tool.id);
			const result = pairs.results.find((candidate) => candidate.id === tool.id);
			expect(JSON.parse(result?.content ?? "null")).toEqual(tool.output);
			// Only Anthropic's body marks a Tool error; the other families
			// carry it in the output alone.
			if (family.family === "anthropic") expect(result?.isError === true).toBe(tool.isError === true);
			expect(result?.index).toBeGreaterThan(call?.index ?? Number.POSITIVE_INFINITY);
		}
		for (const text of row.assistantText) expect(encoded).toContain(text);
		if (row.wire?.once !== undefined) {
			expect(encoded.split(row.wire.once)).toHaveLength(2);
		}
	});
});

// partsOf lists the parts of one type in messages of one role, with the index
// of the message that carries each.
function partsOf(
	messages: readonly LoweredProviderMessage[],
	role: LoweredProviderMessage["role"],
	type: string,
): { message: number; part: any }[] {
	return messages.flatMap((message, index) =>
		message.role === role && Array.isArray(message.content)
			? message.content
					.filter((part: { type: string }) => part.type === type)
					.map((part: unknown) => ({ message: index, part }))
			: [],
	);
}

type Family = {
	readonly family: "anthropic" | "openai" | "openai-compatible";
	readonly providerId: string;
	readonly modelId: string;
	readonly credential: ResolvedProviderCredential;
	readonly stream: URL;
};

function credential(
	providerId: string,
	supplyMode: string,
	accessMode: string,
): ResolvedProviderCredential {
	return {
		source: "session",
		authType: "provider_api_key",
		providerId,
		supplyMode,
		vaultId: "vlt_wire",
		credentialId: "cred_wire",
		accessMode,
		apiKey: `sk-wire-${providerId}`,
	} as ResolvedProviderCredential;
}

const families: readonly Family[] = [
	{
		family: "anthropic",
		providerId: "anthropic",
		modelId: "claude-opus-4-8",
		credential: credential("anthropic", "anthropic-api-key", "api_key"),
		stream: new URL("../golden/fixtures/anthropic-claude-opus-4-8-live-2026-07-03.sse", import.meta.url),
	},
	{
		family: "openai",
		providerId: "openai",
		modelId: "gpt-5.5",
		credential: credential("openai", "openai-api-key", "user_api_key"),
		stream: new URL("../golden/fixtures/openai-gpt-5.5-responses-live-2026-07-06.sse", import.meta.url),
	},
	{
		family: "openai-compatible",
		providerId: "zai",
		modelId: "glm-5.2",
		credential: credential("zai", "zai-coding-api-key", "api_key"),
		stream: new URL("../golden/fixtures/zai-glm-5.2-live-2026-07-04.sse", import.meta.url),
	},
];

// httpBody streams the request through the production provider client and
// returns the one HTTP body it sent; the response is a recorded stream.
async function httpBody(family: Family, request: ProviderRequest): Promise<Record<string, unknown>> {
	const recorded = readFileSync(family.stream, "utf8");
	const bodies: Record<string, unknown>[] = [];
	const fetchImpl = Object.assign(
		async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
			bodies.push((await new Request(input, init).json()) as Record<string, unknown>);
			return new Response(recorded.endsWith("\n\n") ? recorded : `${recorded}\n`, {
				status: 200,
				headers: { "content-type": "text/event-stream", "cache-control": "no-cache" },
			});
		},
		{ preconnect: () => {} },
	) satisfies FetchFunction;
	for await (const _event of new ProviderClientRegistry({ fetch: fetchImpl }).stream({
		request,
		credential: family.credential,
	})) {
		// Consuming the stream makes the client finish its HTTP conversion.
	}
	expect(bodies).toHaveLength(1);
	return bodies[0] as Record<string, unknown>;
}

type WireCall = { id: unknown; name: unknown; input: unknown; index: number };
type WireResult = { id: unknown; content: string; isError?: unknown; index: number };

// wireToolPairs reads the Tool calls and results out of one family's body,
// each with the index of the message or item that carries it.
function wireToolPairs(
	family: Family["family"],
	body: Record<string, unknown>,
): { calls: WireCall[]; results: WireResult[] } {
	type Item = Record<string, unknown>;
	if (family === "anthropic") {
		const messages = body.messages as { role: string; content: Item[] | string }[];
		const parts = messages.flatMap((message, index) =>
			Array.isArray(message.content) ? message.content.map((part) => ({ index, part })) : [],
		);
		return {
			calls: parts
				.filter(({ part }) => part.type === "tool_use")
				.map(({ index, part }) => ({ id: part.id, name: part.name, input: part.input, index })),
			results: parts
				.filter(({ part }) => part.type === "tool_result")
				.map(({ index, part }) => ({
					id: part.tool_use_id,
					content: String(part.content),
					isError: part.is_error,
					index,
				})),
		};
	}
	if (family === "openai") {
		const items = (body.input as Item[]).map((item, index) => ({ index, item }));
		return {
			calls: items
				.filter(({ item }) => item.type === "function_call")
				.map(({ index, item }) => ({
					id: item.call_id,
					name: item.name,
					input: JSON.parse(String(item.arguments)),
					index,
				})),
			results: items
				.filter(({ item }) => item.type === "function_call_output")
				.map(({ index, item }) => ({ id: item.call_id, content: String(item.output), index })),
		};
	}
	const messages = (body.messages as Item[]).map((message, index) => ({ index, message }));
	return {
		calls: messages.flatMap(({ index, message }) =>
			Array.isArray(message.tool_calls)
				? (message.tool_calls as Item[]).map((call) => {
						const fn = call.function as Item;
						return { id: call.id, name: fn.name, input: JSON.parse(String(fn.arguments)), index };
					})
				: [],
		),
		results: messages
			.filter(({ message }) => message.role === "tool")
			.map(({ index, message }) => ({ id: message.tool_call_id, content: String(message.content), index })),
	};
}
