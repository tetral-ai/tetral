/**
 * Projects sealed Runtime context into the provider-only carrier. Open Request
 * drafts are not accepted here: ContextManager exposes them through a separate
 * active-lifecycle slot until Request End seals the entry.
 */

import type {
	ProviderContextEntry,
	ProviderContextItem,
	ProviderToolResult,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderContextRole } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderError } from "../contracts/provider.js";
import { normalizeProviderError } from "../contracts/provider.js";
import type {
	RuntimeContextEntry,
	RuntimeContextPart,
} from "../contracts/runtime.js";
import { RuntimeContextEntrySchema } from "../contracts/runtime.js";

export type RuntimeProviderContextResult =
	| { readonly ok: true; readonly context: readonly ProviderContextEntry[] }
	| { readonly ok: false; readonly error: ProviderError };

export function toGatewayProviderContext(
	input: unknown,
): RuntimeProviderContextResult {
	return toGatewayProviderContextSegments([input]);
}

export function toGatewayProviderContextSegments(
	inputs: readonly unknown[],
): RuntimeProviderContextResult {
	if (inputs.length === 0) {
		return { ok: false, error: runtimeInputError("schema") };
	}
	const calls = new Set<string>();
	const results = new Set<string>();
	const context: ProviderContextEntry[] = [];
	let entryCount = 0;
	for (const input of inputs) {
		if (!Array.isArray(input))
			return { ok: false, error: runtimeInputError("schema") };
		const seenSequences = new Set<number>();
		for (const value of input) {
			const parsed = RuntimeContextEntrySchema.safeParse(value);
			if (!parsed.success)
				return { ok: false, error: runtimeInputError("schema") };
			const entry = parsed.data;
			entryCount += 1;
			if (seenSequences.has(entry.messageSequence)) {
				return {
					ok: false,
					error: runtimeInputError("duplicate_message_sequence"),
				};
			}
			seenSequences.add(entry.messageSequence);
			const content: ProviderContextItem[] = [];
			for (const part of entry.parts) {
				const projected = projectPart(entry, part, calls, results);
				if (!projected.ok)
					return { ok: false, error: runtimeInputError(projected.reason) };
				content.push(...projected.content);
			}
			if (content.length > 0) {
				context.push({ role: providerRole(entry), content });
			}
		}
	}
	if (entryCount === 0)
		return { ok: false, error: runtimeInputError("schema") };
	return { ok: true, context };
}

type ContentProjection =
	| { readonly ok: true; readonly content: readonly ProviderContextItem[] }
	| { readonly ok: false; readonly reason: string };

function projectPart(
	entry: RuntimeContextEntry,
	part: RuntimeContextPart,
	calls: Set<string>,
	results: Set<string>,
): ContentProjection {
	switch (part.type) {
		case "text":
			return part.text.length === 0
				? { ok: true, content: [] }
				: { ok: true, content: [{ text: { text: part.text } }] };
		case "reasoning":
			if (entry.contextKind !== "assistant")
				return { ok: false, reason: "unsupported_content" };
			if (
				part.text.length === 0 &&
				(part.providerMetadata === undefined ||
					Object.keys(part.providerMetadata).length === 0)
			) {
				return { ok: true, content: [] };
			}
			return {
				ok: true,
				content: [
					{
						reasoning: {
							text: part.text,
							metadataJson: JSON.stringify(part.providerMetadata ?? {}),
						},
					},
				],
			};
		case "tool_call":
			if (
				entry.contextKind !== "assistant" ||
				calls.has(part.modelToolCallId)
			) {
				return { ok: false, reason: "conflicting_tool_call_pair" };
			}
			calls.add(part.modelToolCallId);
			return {
				ok: true,
				content: [
					{
						toolCall: {
							modelToolCallId: part.modelToolCallId,
							name: part.toolName,
							inputJson: JSON.stringify(part.canonicalInput),
						},
					},
				],
			};
		case "tool_result":
			if (
				entry.contextKind !== "assistant" ||
				!calls.has(part.modelToolCallId)
			) {
				return { ok: false, reason: "missing_tool_call" };
			}
			if (results.has(part.modelToolCallId))
				return { ok: false, reason: "duplicate_tool_result" };
			results.add(part.modelToolCallId);
			return { ok: true, content: [{ toolResult: toolResult(part) }] };
	}
}

function providerRole(entry: RuntimeContextEntry): ProviderContextRole {
	return entry.contextKind === "assistant"
		? ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT
		: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER;
}

function toolResult(
	part: Extract<RuntimeContextPart, { readonly type: "tool_result" }>,
): ProviderToolResult {
	switch (part.result.type) {
		case "completed":
			return {
				modelToolCallId: part.modelToolCallId,
				completed: {
					outputJson: JSON.stringify({ text: part.result.output.text }),
				},
			};
		case "error":
			return {
				modelToolCallId: part.modelToolCallId,
				error: { errorJson: JSON.stringify({ error: part.result.error }) },
			};
		case "cancelled":
			return { modelToolCallId: part.modelToolCallId, cancelled: {} };
	}
}

function runtimeInputError(reason: string): ProviderError {
	return normalizeProviderError({
		code: "provider_invalid_request",
		retryable: false,
		message: `Runtime context is not valid for Gateway ProviderRequest: ${reason}.`,
	});
}
