import { describe, expect, test } from "bun:test";
import { readdir, readFile } from "node:fs/promises";
import type { FetchFunction } from "@ai-sdk/provider-utils";
import { classifyProviderStreamError } from "@tetral/gateway-lowering/src/errors.js";
import type { ResolvedProviderRequestAttachment } from "@tetral/gateway-lowering/src/request.js";
import type {
	ProviderRequest,
	ProviderStreamEvent,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
	ProviderContextRole,
	ProviderFinishReason,
	ProviderRequestKind,
	ProviderStreamEventType,
	providerFinishReasonToJSON,
	providerStreamEventTypeToJSON,
	SystemCacheHint,
	SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import type { ResolvedProviderCredential } from "../../src/providers/credentials.js";
import {
	OpenAICodexResponsesEndpoint,
	OpenAIOAuthDummyAPIKey,
} from "../../src/providers/openai-oauth.js";
import { ProviderKeyFailureError } from "../../src/providers/pool.js";
import { validProviderRequest } from "../unit/fixtures.js";

const OpenAIFixtureUrl = new URL(
	"fixtures/openai-gpt-5.5-responses-live-2026-07-06.sse",
	import.meta.url,
);
const OpenAIGPT56SolFixtureUrl = new URL(
	"fixtures/openai-gpt-5.6-sol-responses-live-2026-07-19.sse",
	import.meta.url,
);
const OpenAIGPT56SolOAuthFixtureUrl = new URL(
	"fixtures/openai-gpt-5.6-sol-oauth-responses-live-2026-07-19.sse",
	import.meta.url,
);
const OpenAIDisconnectFixtureUrl = new URL(
	"fixtures/openai-gpt-5.5-responses-disconnect-prefix-2026-07-06.sse",
	import.meta.url,
);
const OpenAIHttpErrorFixtureUrl = new URL(
	"fixtures/openai-gpt-5.5-responses-http-error-2026-07-06.txt",
	import.meta.url,
);
const AnthropicFixtureUrl = new URL(
	"fixtures/anthropic-claude-opus-4-8-live-2026-07-03.sse",
	import.meta.url,
);
const AnthropicFableFixtureUrl = new URL(
	"fixtures/anthropic-claude-fable-5-live-2026-07-19.sse",
	import.meta.url,
);
const AnthropicCacheHitFixtureUrl = new URL(
	"fixtures/anthropic-claude-opus-4-8-cache-hit-2026-07-09.sse",
	import.meta.url,
);
const AnthropicDisconnectFixtureUrl = new URL(
	"fixtures/anthropic-claude-opus-4-8-disconnect-prefix-2026-07-03.sse",
	import.meta.url,
);
const KimiK3FixtureUrl = new URL(
	"fixtures/moonshotai-kimi-k3-live-2026-07-19.sse",
	import.meta.url,
);
const KimiK3ToolFixtureUrl = new URL(
	"fixtures/moonshotai-kimi-k3-tool-live-2026-07-19.sse",
	import.meta.url,
);
const KimiCacheHitFixtureUrl = new URL(
	"fixtures/moonshotai-kimi-k3-cache-hit-2026-07-19.sse",
	import.meta.url,
);
const KimiDisconnectFixtureUrl = new URL(
	"fixtures/moonshotai-kimi-k3-disconnect-prefix-2026-07-19.sse",
	import.meta.url,
);
const KimiHttpErrorFixtureUrl = new URL(
	"fixtures/moonshotai-kimi-k3-http-error-2026-07-19.txt",
	import.meta.url,
);
const DeepSeekFixtureUrl = new URL(
	"fixtures/deepseek-deepseek-v4-pro-live-2026-07-06.sse",
	import.meta.url,
);
const DeepSeekDisconnectFixtureUrl = new URL(
	"fixtures/deepseek-deepseek-v4-pro-disconnect-prefix-2026-07-06.sse",
	import.meta.url,
);
const DeepSeekHttpErrorFixtureUrl = new URL(
	"fixtures/deepseek-deepseek-v4-pro-http-error-2026-07-06.txt",
	import.meta.url,
);
const ZaiFixtureUrl = new URL(
	"fixtures/zai-glm-5.2-live-2026-07-04.sse",
	import.meta.url,
);
const ZaiDisconnectFixtureUrl = new URL(
	"fixtures/zai-glm-5.2-disconnect-prefix-2026-07-04.sse",
	import.meta.url,
);
const ZaiHttpErrorFixtureUrl = new URL(
	"fixtures/zai-glm-5.2-http-error-2026-07-04.txt",
	import.meta.url,
);
const AnthropicBetaHeader =
	"structured-outputs-2025-11-13,interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14";
const ApprovalReviewerOutputSchemaUrl = new URL(
	"../../../../../agent-runtime/packages/runtime-pod/src/assets/approval-reviewer-output-schema.json",
	import.meta.url,
);

(
	globalThis as typeof globalThis & { AI_SDK_LOG_WARNINGS?: boolean }
).AI_SDK_LOG_WARNINGS = false;

describe("provider golden fixture provenance", () => {
	test("requires date and model provenance on every JSON golden artifact", async () => {
		const files = await collectGoldenJsonFixturePaths(
			new URL(".", import.meta.url),
		);
		expect(files.length).toBeGreaterThan(0);
		for (const relativePath of files) {
			await readGoldenJsonFixture(relativePath);
		}
	});

	test("pins every approved model's output-cap boundary on the provider wire", async () => {
		const cases = [
			{
				request: {
					...openAIGoldenRequest(),
					limits: { maxOutputTokens: 128_001, timeoutMs: 60_000 },
				},
				credential: sessionOpenAICredential(),
				fixture: OpenAIFixtureUrl,
				field: "max_output_tokens",
				expected: undefined,
			},
			{
				request: {
					...openAIGPT56SolGoldenRequest(),
					limits: { maxOutputTokens: 128_001, timeoutMs: 60_000 },
				},
				credential: sessionOpenAICredential(),
				fixture: OpenAIGPT56SolFixtureUrl,
				field: "max_output_tokens",
				expected: undefined,
			},
			{
				request: {
					...anthropicGoldenRequest(),
					limits: { maxOutputTokens: 128_001, timeoutMs: 60_000 },
				},
				credential: sessionAnthropicCredential(),
				fixture: AnthropicFixtureUrl,
				field: "max_tokens",
				expected: 128_000,
			},
			{
				request: {
					...anthropicFableGoldenRequest(),
					limits: { maxOutputTokens: 128_001, timeoutMs: 60_000 },
				},
				credential: sessionAnthropicCredential(),
				fixture: AnthropicFableFixtureUrl,
				field: "max_tokens",
				expected: 128_000,
			},
			{
				request: {
					...deepSeekGoldenRequest(),
					limits: { maxOutputTokens: 384_001, timeoutMs: 60_000 },
				},
				credential: sessionDeepSeekCredential(),
				fixture: DeepSeekFixtureUrl,
				field: "max_tokens",
				expected: 384_000,
			},
			{
				request: {
					...kimiK3GoldenRequest(),
					limits: { maxOutputTokens: 131_073, timeoutMs: 60_000 },
				},
				credential: sessionKimiCredential(),
				fixture: KimiK3FixtureUrl,
				field: "max_tokens",
				expected: 131_072,
			},
			{
				request: {
					...zaiGoldenRequest(),
					limits: { maxOutputTokens: 131_073, timeoutMs: 60_000 },
				},
				credential: sessionZaiCredential(),
				fixture: ZaiFixtureUrl,
				field: "max_tokens",
				expected: 131_072,
			},
		] as const;

		for (const scenario of cases) {
			const mock = createMockAnthropicServer(
				await readFile(scenario.fixture, "utf8"),
			);
			const registry = new ProviderClientRegistry({ fetch: mock.fetch });
			try {
				await collectEvents(
					registry.stream({
						request: scenario.request,
						credential: scenario.credential,
					}),
				);
				expect(mock.requests).toHaveLength(1);
				if (scenario.expected === undefined) {
					expect(mock.requests[0]!.body).not.toHaveProperty(scenario.field);
				} else {
					expect(mock.requests[0]!.body[scenario.field]).toBe(
						scenario.expected,
					);
				}
			} finally {
				await mock.close();
			}
		}
	});

	test("emits plain-text attachments as Anthropic document text blocks on both Anthropic-family routes", async () => {
		const attachment = plainTextAttachment("notes.txt", "hello");
		const scenarios: readonly {
			readonly request: ProviderRequest;
			readonly credential: ResolvedProviderCredential;
			readonly fixture: URL;
		}[] = [
			{
				request: {
					...anthropicGoldenRequest(),
					requestId: "req_anthropic_plain_text",
					modelRequestId: "mreq_anthropic_plain_text",
					attachments: [attachment.request],
				},
				credential: sessionAnthropicCredential(),
				fixture: AnthropicFixtureUrl,
			},
			{
				request: {
					...kimiK3GoldenRequest(),
					requestId: "req_kimi_plain_text",
					modelRequestId: "mreq_kimi_plain_text",
					attachments: [attachment.request],
				},
				credential: sessionKimiCredential(),
				fixture: KimiK3FixtureUrl,
			},
		];

		for (const scenario of scenarios) {
			const mock = createMockAnthropicServer(
				await readFile(scenario.fixture, "utf8"),
			);
			const registry = new ProviderClientRegistry({ fetch: mock.fetch });
			try {
				await collectEvents(
					registry.stream({
						request: scenario.request,
						credential: scenario.credential,
						resolvedAttachments: [attachment.resolved],
					}),
				);

				expect(mock.requests).toHaveLength(1);
				const messages = mock.requests[0]!.body.messages as readonly {
					readonly content: readonly Record<string, unknown>[];
				}[];
				const documentBlocks = messages
					.flatMap((message) => message.content)
					.filter((part) => part.type === "document");
				expect(documentBlocks).toEqual([
					expect.objectContaining({
						type: "document",
						source: {
							type: "text",
							media_type: "text/plain",
							data: "hello",
						},
						title: "notes.txt",
					}),
				]);
				expect(JSON.stringify(messages)).not.toContain(
					"ERROR: cannot read attachment",
				);
			} finally {
				await mock.close();
			}
		}
	});
});

describe("multi-Tool captured provider wire composition", () => {
	test("preserves grouped call and result identity after independent durable settlements", async () => {
		const scenarios: readonly {
			readonly family: ProviderWireFamily;
			readonly request: ProviderRequest;
			readonly credential: ResolvedProviderCredential;
			readonly fixture: URL;
		}[] = [
			{
				family: "anthropic",
				request: anthropicGoldenRequest(),
				credential: sessionAnthropicCredential(),
				fixture: AnthropicFixtureUrl,
			},
			{
				family: "openai",
				request: openAIGoldenRequest(),
				credential: sessionOpenAICredential(),
				fixture: OpenAIFixtureUrl,
			},
			{
				family: "openai-compatible",
				request: zaiGoldenRequest(),
				credential: sessionZaiCredential(),
				fixture: ZaiFixtureUrl,
			},
		];

		for (const scenario of scenarios) {
			const mock = createMockAnthropicServer(
				await readFile(scenario.fixture, "utf8"),
			);
			const registry = new ProviderClientRegistry({ fetch: mock.fetch });
			try {
				const afterFirstSettlement = multiToolHistoryRequest(scenario.request, [
					"call/shared",
				]);
				expect(providerContextPendingToolCallIds(afterFirstSettlement)).toEqual(
					["call:shared", "call_gamma"],
				);
				const afterSecondSettlement = multiToolHistoryRequest(scenario.request, [
					"call/shared",
					"call:shared",
				]);
				expect(providerContextPendingToolCallIds(afterSecondSettlement)).toEqual([
					"call_gamma",
				]);
				const settledRequest = multiToolHistoryRequest(scenario.request, [
					"call/shared",
					"call:shared",
					"call_gamma",
				]);
				expect(providerContextPendingToolCallIds(settledRequest)).toEqual([]);
				await collectEvents(
					registry.stream({
						request: settledRequest,
						credential: scenario.credential,
					}),
				);

				expect(mock.requests).toHaveLength(1);
				assertCapturedMultiToolWire(scenario.family, mock.requests[0]!.body, [
					"call/shared",
					"call:shared",
					"call_gamma",
				]);
			} finally {
				await mock.close();
			}
		}
	});

	test("emits no invented Anthropic Assistant text for completed MCP, errored builtin, and reasoning-only history", async () => {
		const mock = createMockAnthropicServer(
			await readFile(AnthropicFixtureUrl, "utf8"),
		);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			await collectEvents(
				registry.stream({
					request: noInventionToolHistoryRequest(
						anthropicGoldenRequest(),
						JSON.stringify({ anthropic: { signature: "sig_no_invention" } }),
					),
					credential: sessionAnthropicCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const body = mock.requests[0]!.body;
			const messages = body.messages as readonly {
				readonly role: string;
				readonly content: readonly Record<string, unknown>[];
			}[];
			const assistantTypes = messages
				.filter((message) => message.role === "assistant")
				.map((message) => message.content.map((part) => part.type));
			expect(assistantTypes).toEqual([
				["thinking", "tool_use"],
				["thinking", "tool_use"],
				["thinking"],
			]);

			const wire = capturedToolWire("anthropic", body);
			expect(wire.calls.map((call) => [call.id, call.name, call.input])).toEqual([
				["call_mcp_completed", "github_create_issue", { title: "fixed" }],
				["call_builtin_error", "exec_command", { cmd: "false" }],
			]);
			expect(wire.results.map((result) => result.id)).toEqual([
				"call_mcp_completed",
				"call_builtin_error",
			]);
			expect(capturedToolSequence(wire)).toEqual([
				["call", "call_mcp_completed"],
				["result", "call_mcp_completed"],
				["call", "call_builtin_error"],
				["result", "call_builtin_error"],
			]);
			expect(wire.results[0]!.output).toBe(JSON.stringify({ text: "mcp-completed-canary" }));
			expect(wire.results[1]!.output).toBe(JSON.stringify({ error: { message: "builtin-error-canary" } }));
		} finally {
			await mock.close();
		}
	});

	test("serializes empty signed Anthropic reasoning at the adapter request boundary", async () => {
		const mock = createMockAnthropicServer(
			await readFile(AnthropicFixtureUrl, "utf8"),
		);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const base = anthropicGoldenRequest();
			await collectEvents(
				registry.stream({
					request: {
						...base,
						requestId: "req_empty_signed_reasoning",
						modelRequestId: "mreq_empty_signed_reasoning",
						context: [
							{
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
								content: [{ reasoning: {
									text: "",
									metadataJson: JSON.stringify({ anthropic: { signature: "sig_empty_reasoning" } }),
								} }],
							},
							{
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
								content: [{ text: { text: "Continue." } }],
							},
						],
					},
					credential: sessionAnthropicCredential(),
				}),
			);

			const messages = mock.requests[0]!.body.messages as readonly {
				readonly role: string;
				readonly content: readonly Record<string, unknown>[];
			}[];
			expect(messages[0]).toMatchObject({
				role: "assistant",
				content: [{ type: "thinking", thinking: "", signature: "sig_empty_reasoning" }],
			});
		} finally {
			await mock.close();
		}
	});

	test("keeps the same completed and errored Tool exchange identities on OpenAI wire families", async () => {
		const scenarios: readonly {
			readonly family: "openai" | "openai-compatible";
			readonly request: ProviderRequest;
			readonly credential: ResolvedProviderCredential;
			readonly fixture: URL;
		}[] = [
			{
				family: "openai",
				request: noInventionToolHistoryRequest(
					openAIGoldenRequest(),
					JSON.stringify({ openai: { encrypted_content: "enc_no_invention", itemId: "rs_removed" } }),
				),
				credential: sessionOpenAICredential(),
				fixture: OpenAIFixtureUrl,
			},
			{
				family: "openai-compatible",
				request: noInventionToolHistoryRequest(zaiGoldenRequest(), "{}"),
				credential: sessionZaiCredential(),
				fixture: ZaiFixtureUrl,
			},
		];

		for (const scenario of scenarios) {
			const mock = createMockAnthropicServer(
				await readFile(scenario.fixture, "utf8"),
			);
			const registry = new ProviderClientRegistry({ fetch: mock.fetch });
			try {
				await collectEvents(
					registry.stream({
						request: scenario.request,
						credential: scenario.credential,
					}),
				);
				expect(mock.requests).toHaveLength(1);
				const wire = capturedToolWire(scenario.family, mock.requests[0]!.body);
				expect(wire.calls.map((call) => [call.id, call.name, call.input]), scenario.family).toEqual([
					["call_mcp_completed", "github_create_issue", { title: "fixed" }],
					["call_builtin_error", "exec_command", { cmd: "false" }],
				]);
				expect(wire.results.map((result) => result.id), scenario.family).toEqual([
					"call_mcp_completed",
					"call_builtin_error",
				]);
				expect(capturedToolSequence(wire), scenario.family).toEqual([
					["call", "call_mcp_completed"],
					["result", "call_mcp_completed"],
					["call", "call_builtin_error"],
					["result", "call_builtin_error"],
				]);
				expect(wire.results[0]!.output, scenario.family).toBe(JSON.stringify({ text: "mcp-completed-canary" }));
				expect(wire.results[1]!.output, scenario.family).toBe(JSON.stringify({ error: { message: "builtin-error-canary" } }));
				if (scenario.family === "openai") {
					const reasoningItems = (mock.requests[0]!.body.input as readonly Record<string, unknown>[])
						.filter((item) => item.type === "reasoning");
					expect(reasoningItems).toHaveLength(3);
					expect(reasoningItems.map((item) => item.encrypted_content)).toEqual([
						"enc_no_invention",
						"enc_no_invention",
						"enc_no_invention",
					]);
					expect(JSON.stringify(reasoningItems)).not.toContain("rs_removed");
				} else {
					const assistantReasoning = (mock.requests[0]!.body.messages as readonly Record<string, unknown>[])
						.filter((message) => message.role === "assistant")
						.map((message) => message.reasoning_content)
						.filter((value) => typeof value === "string" && value.length > 0);
					expect(assistantReasoning).toEqual([
						"declared reasoning mcp",
						"declared reasoning builtin",
						"declared reasoning only",
					]);
				}
			} finally {
				await mock.close();
			}
		}
	});
});

describe("expanded provider catalog golden wire paths", () => {
	test("replays Claude Fable 5 through the frozen Anthropic rule family", async () => {
		const fixture = await readFile(AnthropicFableFixtureUrl, "utf8");
		expect(fixture).toContain(
			"recorded 2026-07-19 model_id=anthropic/claude-fable-5 source=real-provider",
		);
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const events = await collectEvents(
				registry.stream({
					request: anthropicFableGoldenRequest(),
					credential: sessionAnthropicCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			await expectCapturedRequestFixture(
				mock.requests[0]!,
				"anthropic-fable/basic-turn.http.json",
			);
			await expectProviderEventsFixture(
				events,
				"anthropic-fable/basic-turn.events.json",
			);
		} finally {
			await mock.close();
		}
	});

	test("replays Kimi K3 native thinking without injecting a thinking budget", async () => {
		const fixture = await readFile(KimiK3FixtureUrl, "utf8");
		expect(fixture).toContain(
			"recorded 2026-07-19 model_id=moonshotai/kimi-k3 source=real-provider",
		);
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const events = await collectEvents(
				registry.stream({
					request: kimiK3GoldenRequest(),
					credential: sessionKimiCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			expect(mock.requests[0]!.body).not.toHaveProperty("thinking");
			expect(mock.requests[0]!.body).not.toHaveProperty("output_config");
			await expectCapturedRequestFixture(
				mock.requests[0]!,
				"moonshotai-k3/basic-turn.http.json",
			);
			await expectProviderEventsFixture(
				events,
				"moonshotai-k3/basic-turn.events.json",
			);
		} finally {
			await mock.close();
		}
	});

	test("replays Kimi K3 thinking and tool_use history in transport-native form", async () => {
		const fixture = await readFile(KimiK3ToolFixtureUrl, "utf8");
		expect(fixture).toContain(
			"model_id=moonshotai/kimi-k3 source=real-provider",
		);
		expect(fixture).toContain("case=tool-use");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const firstTurn = await collectEvents(
				registry.stream({
					request: kimiK3ToolGoldenRequest(),
					credential: sessionKimiCredential(),
				}),
			);
			const reasoningText = firstTurn
				.map((event) => event.reasoning?.text ?? "")
				.join("");
			const signature = firstTurn
				.map((event) => event.reasoning?.metadataJson ?? "{}")
				.map(
					(metadataJson) =>
						JSON.parse(metadataJson) as { anthropic?: { signature?: string } },
				)
				.find((metadata) => metadata.anthropic?.signature !== undefined)
				?.anthropic?.signature;
			const toolCall = firstTurn.find(
				(event) => event.toolCall !== undefined,
			)?.toolCall;
			expect(reasoningText).not.toBe("");
			expect(signature).not.toBeUndefined();
			expect(toolCall).toMatchObject({
				name: "Read",
				inputJson: JSON.stringify({ path: "/workspace/app.ts" }),
			});

			await collectEvents(
				registry.stream({
					request: kimiK3ReplayGoldenRequest(
						reasoningText,
						signature!,
						toolCall!.id,
						toolCall!.name,
						toolCall!.inputJson,
					),
					credential: sessionKimiCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(2);
			const messages = mock.requests[1]!.body.messages as Array<{
				readonly role: string;
				readonly content: readonly Record<string, unknown>[];
			}>;
			const assistant = messages.find(
				(message) => message.role === "assistant",
			);
			expect(assistant?.content).toEqual(
				expect.arrayContaining([
					{
						type: "thinking",
						thinking: reasoningText,
						signature,
					},
					{
						type: "tool_use",
						id: toolCall!.id,
						name: toolCall!.name,
						input: JSON.parse(toolCall!.inputJson),
					},
				]),
			);
			expect(
				messages.some((message) =>
					message.content.some(
						(part) =>
							part.type === "tool_result" && part.tool_use_id === toolCall!.id,
					),
				),
			).toBe(true);
		} finally {
			await mock.close();
		}
	});
});

describe("OpenAI Responses golden wire path", () => {
	test("captures GPT-5.6 Sol through the official API-key supply", async () => {
		const fixture = await readFile(OpenAIGPT56SolFixtureUrl, "utf8");
		expect(fixture).toContain("model_id=openai/gpt-5.6-sol");
		expect(fixture).toContain("supply=official");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const events = await collectEvents(
				registry.stream({
					request: openAIGPT56SolGoldenRequest(),
					credential: sessionOpenAICredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.pathname).toBe("/v1/responses");
			expect(captured.body).toMatchObject({
				model: "gpt-5.6-sol",
				store: false,
				reasoning: { effort: "medium", summary: "auto" },
			});
			expect(captured.body).not.toHaveProperty("max_output_tokens");
			await expectCapturedRequestFixture(
				captured,
				"openai-gpt56-sol-official/basic-turn.http.json",
			);
			await expectProviderEventsFixture(
				events,
				"openai-gpt56-sol-official/basic-turn.events.json",
			);
		} finally {
			await mock.close();
		}
	});

	test("captures GPT-5.6 Sol through the ChatGPT OAuth supply", async () => {
		const fixture = await readFile(OpenAIGPT56SolOAuthFixtureUrl, "utf8");
		expect(fixture).toContain("model_id=openai/gpt-5.6-sol");
		expect(fixture).toContain("supply=chatgpt-oauth");
		expect(fixture).not.toContain('"safety_identifier":"user-');
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const events = await collectEvents(
				registry.stream({
					request: openAIGPT56SolGoldenRequest(
						"sesn_live_gpt56_sol_oauth_fixture",
					),
					credential: sessionOpenAIOAuthCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.pathname).toBe(
				new URL(OpenAICodexResponsesEndpoint).pathname,
			);
			expect(captured.headers.authorization).toBe("Bearer oauth-access");
			expect(captured.body).toMatchObject({
				model: "gpt-5.6-sol",
				instructions: "Reply tersely and do not call tools.",
				store: false,
				reasoning: { effort: "medium", summary: "auto" },
			});
			expect(captured.body).not.toHaveProperty("max_output_tokens");
			await expectCapturedRequestFixture(
				captured,
				"openai-gpt56-sol-oauth/basic-turn.http.json",
			);
			await expectProviderEventsFixture(
				events,
				"openai-gpt56-sol-oauth/basic-turn.events.json",
			);
		} finally {
			await mock.close();
		}
	});

	test("captures the official Responses request shape and raises the replayed fixture", async () => {
		const fixture = await readFile(OpenAIFixtureUrl, "utf8");
		expect(fixture).toContain(
			"recorded 2026-07-08 model_id=openai/gpt-5.5 source=real-provider",
		);
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const events = await collectEvents(
				registry.stream({
					request: openAIGoldenRequest(),
					credential: sessionOpenAICredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			await expectCapturedRequestFixture(
				captured,
				"openai/basic-turn.http.json",
			);
			expect(captured.method).toBe("POST");
			expect(captured.pathname).toBe("/v1/responses");
			expect(captured.headers.authorization).toBe("Bearer sk-session-openai");
			expect(captured.body).toMatchObject({
				model: "gpt-5.5",
				stream: true,
				store: false,
				prompt_cache_key: "sesn_live_openai_fixture",
				text: { verbosity: "low" },
				reasoning: { effort: "xhigh", summary: "auto" },
			});
			expect(captured.body).not.toHaveProperty("max_output_tokens");
			expect(captured.body).not.toHaveProperty("temperature");
			expect(captured.body).not.toHaveProperty("top_p");
			expect(captured.body.include).toContain("reasoning.encrypted_content");
			expect(JSON.stringify(captured.body.input)).not.toContain('"id"');
			expect(captured.body.input).toEqual([
				{
					role: "developer",
					content:
						"Keep visible output minimal and call tools exactly as requested.",
				},
				{
					type: "reasoning",
					encrypted_content: "enc_prior_reasoning",
					summary: [],
				},
				{
					role: "user",
					content: [
						{
							type: "input_text",
							text: 'Use brief hidden reasoning to verify 7 + 8 = 15. Then emit visible text exactly: ok. Then call Search exactly once with JSON input {"query":"tetral"}. Do not add any other visible text.',
						},
					],
				},
			]);
			expect(captured.body.tools).toEqual([
				{
					type: "function",
					name: "Search",
					description: "Search.",
					parameters: {
						type: "object",
						properties: { query: { type: "string" } },
						required: ["query"],
						additionalProperties: false,
					},
					strict: false,
				},
			]);
			expectProviderToolTurn(events, {
				reasoningIncludes: ["7", "15"],
				textIncludes: "ok",
				toolName: "Search",
				toolInputJson: JSON.stringify({ query: "tetral" }),
			});
			await expectProviderEventsFixture(
				events,
				"openai/basic-turn.events.json",
			);
			expect(events.at(-1)?.finish).toMatchObject({
				reason: ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS,
				usage: {
					inputUncachedTokens: 98,
					inputCacheReadTokens: 0,
					inputTotalTokens: 98,
					outputReasoningTokens: 1034,
					outputTotalTokens: 1062,
					totalTokens: 1160,
				},
				metadataJson: JSON.stringify({
					credential_source: "session",
				}),
			});
		} finally {
			await mock.close();
		}
	});

	test("captures the OAuth Responses rewrite, header swap, and instructions", async () => {
		const fixture = await readFile(OpenAIFixtureUrl, "utf8");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			await collectEvents(
				registry.stream({
					request: openAIGoldenRequest(),
					credential: sessionOpenAIOAuthCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.method).toBe("POST");
			expect(captured.pathname).toBe(
				new URL(OpenAICodexResponsesEndpoint).pathname,
			);
			expect(captured.headers.authorization).toBe("Bearer oauth-access");
			expect(captured.headers["chatgpt-account-id"]).toBe("acct_1");
			expect(captured.headers.authorization).not.toBe(
				`Bearer ${OpenAIOAuthDummyAPIKey}`,
			);
			expect(captured.body).toMatchObject({
				model: "gpt-5.5",
				stream: true,
				store: false,
				instructions:
					"Keep visible output minimal and call tools exactly as requested.",
				prompt_cache_key: "sesn_live_openai_fixture",
			});
			expect(JSON.stringify(captured.body.input)).not.toContain('"id"');
			expect(captured.body.input).toEqual([
				{
					type: "reasoning",
					encrypted_content: "enc_prior_reasoning",
					summary: [],
				},
				{
					role: "user",
					content: [
						{
							type: "input_text",
							text: 'Use brief hidden reasoning to verify 7 + 8 = 15. Then emit visible text exactly: ok. Then call Search exactly once with JSON input {"query":"tetral"}. Do not add any other visible text.',
						},
					],
				},
			]);
		} finally {
			await mock.close();
		}
	});

	test("turns a mid-stream OpenAI Responses transport disconnect into a retryable stream failure after partial events", async () => {
		const fixture = await readFile(OpenAIDisconnectFixtureUrl, "utf8");
		expect(fixture).toContain("source=real-provider-prefix");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture, {
			holdOpenAfterFixture: true,
		});
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const result = await collectEventsDisconnectingWhen(
				registry.stream({
					request: openAIGoldenRequest(),
					credential: sessionOpenAICredential(),
				}),
				(events) =>
					events
						.map((event) => event.type)
						.includes(
							ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
						) &&
					events
						.map((event) => event.reasoning?.text ?? "")
						.join("")
						.includes("Evaluating tool response"),
				mock.disconnect,
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.pathname).toBe("/v1/responses");
			expect(captured.headers.authorization).toBe("Bearer sk-session-openai");
			expect(result.events.map((event) => event.type)).toContain(
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
			);
			expect(
				result.events.map((event) => event.reasoning?.text ?? "").join(""),
			).toContain("Evaluating tool response");
			expect(result.error).toBeInstanceOf(Error);
			expect(classifyProviderStreamError(result.error)).toMatchObject({
				code: "provider_stream_error",
				retryable: true,
				fatal: false,
				statusCode: 502,
			});
		} finally {
			await mock.disconnect();
			await mock.close();
		}
	});

	test("classifies a recorded OpenAI Responses HTTP provider error without platform-key fallback", async () => {
		const fixture = await readFile(OpenAIHttpErrorFixtureUrl, "utf8");
		expect(fixture).toContain("source=real-provider-http-error");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(httpFixtureBody(fixture), {
			status: 400,
			contentType: "application/json; charset=utf-8",
		});
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const result = await collectEventsUntilError(
				registry.stream({
					request: openAIGoldenRequest(),
					credential: sessionOpenAICredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.pathname).toBe("/v1/responses");
			expect(captured.headers.authorization).toBe("Bearer sk-session-openai");
			expect(captured.body).toMatchObject({
				model: "gpt-5.5",
				stream: true,
				store: false,
			});
			expect(result.events).toHaveLength(0);
			expect(result.error).toBeInstanceOf(ProviderKeyFailureError);
			expect(
				(result.error as ProviderKeyFailureError).classification,
			).toMatchObject({
				action: "fail-fast",
				providerError: {
					code: "provider_request_invalid",
					retryable: false,
					fatal: true,
					statusCode: 400,
				},
			});
		} finally {
			await mock.close();
		}
	});
});

describe("Anthropic golden wire path", () => {
	test("lowers approval reviewer schema onto Anthropic provider-native output format", async () => {
		const outputSchemaJson = await readFile(
			ApprovalReviewerOutputSchemaUrl,
			"utf8",
		);
		const mock = createMockAnthropicServer(anthropicStructuredOutputFixture());
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const request = anthropicGoldenRequest();
			if (request.model === undefined) {
				throw new Error("anthropic golden request is missing its model");
			}
			const events = await collectEvents(
				registry.stream({
					request: {
						...request,
						requestKind:
							ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
						model: { ...request.model, variant: "" },
						outputSchemaJson,
					},
					credential: sessionAnthropicCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.headers["anthropic-beta"]).toBe(AnthropicBetaHeader);
			expect(captured.body.output_config).toEqual({
				effort: "xhigh",
				format: {
					type: "json_schema",
					schema: JSON.parse(outputSchemaJson),
				},
			});
			expect(captured.body.tools).toEqual([
				expect.objectContaining({ name: "Read" }),
			]);
			expect(JSON.stringify(captured.body.tools)).not.toContain(
				'"name":"json"',
			);
			expect(events.map((event) => event.text?.text ?? "").join("")).toContain(
				'"outcome":"allow"',
			);
		} finally {
			await mock.close();
		}
	});

	test("captures the AI SDK HTTP request and raises the recorded Anthropic SSE fixture", async () => {
		const fixture = await readFile(AnthropicFixtureUrl, "utf8");
		expect(fixture).toContain(
			"recorded 2026-07-03 model_id=anthropic/claude-opus-4-8 source=real-provider",
		);
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const events = await collectEvents(
				registry.stream({
					request: anthropicGoldenRequest(),
					credential: sessionAnthropicCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.method).toBe("POST");
			expect(captured.pathname).toBe("/v1/messages");
			expect(captured.headers["x-api-key"]).toBe("sk-session-anthropic");
			expect(captured.headers["anthropic-beta"]).toBe(AnthropicBetaHeader);

			expect(captured.body).toMatchObject({
				model: "claude-opus-4-8",
				stream: true,
				max_tokens: 1024,
				thinking: { type: "adaptive", display: "summarized" },
				output_config: { effort: "high" },
			});
			expect(captured.body).not.toHaveProperty("temperature");
			expect(captured.body).not.toHaveProperty("top_p");
			expect(captured.body).not.toHaveProperty("top_k");
			expect(captured.body.system).toEqual([
				{
					type: "text",
					text: "You are a deterministic fixture recorder for Tetral Gateway tests. Use summarized thinking when available before acting.",
					cache_control: { type: "ephemeral" },
				},
				{
					type: "text",
					text: "Operate as the session fixture specialist.",
					cache_control: { type: "ephemeral" },
				},
				{
					type: "text",
					text: "When asked to use a tool, call exactly the named tool with the requested JSON input.",
				},
			]);
			expect(captured.body.messages).toEqual([
				{
					role: "user",
					content: [
						{
							type: "text",
							text: 'Use your thinking block to verify this checksum before acting: 271828 + 314159 = 585987. After that, emit visible text exactly: reading now. Then call the Read tool exactly once with JSON input {"path":"/workspace/app.ts"}. Do not add other visible text.',
							cache_control: { type: "ephemeral" },
						},
					],
				},
			]);
			expect(captured.body.tools).toEqual([
				{
					name: "Read",
					description: "Read a file.",
					input_schema: {
						type: "object",
						properties: { path: { type: "string" } },
						required: ["path"],
						additionalProperties: false,
					},
					eager_input_streaming: true,
				},
			]);

			expect(events.map((event) => event.type)).toEqual([
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
			]);
			const reasoningText = events
				.map((event) => event.reasoning?.text ?? "")
				.join("");
			expect(reasoningText).toContain("271828");
			expect(reasoningText).toContain("585987");
			expect(
				events.some((event) =>
					event.reasoning?.metadataJson.includes("signature"),
				),
			).toBe(true);
			expect(events.map((event) => event.text?.text ?? "").join("")).toBe(
				"reading now.",
			);
			expect(events.map((event) => event.toolInput?.text ?? "").join("")).toBe(
				'{"path": "/workspace/app.ts"}',
			);
			expect(events[12]?.toolCall).toMatchObject({
				name: "Read",
				inputJson: JSON.stringify({ path: "/workspace/app.ts" }),
			});
			expect(events[13]?.finish).toMatchObject({
				reason: ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS,
				usage: {
					inputUncachedTokens: 532,
					inputCacheReadTokens: 0,
					inputCacheWriteTokens: 0,
					inputTotalTokens: 532,
					outputTotalTokens: 78,
					totalTokens: 610,
				},
				metadataJson: JSON.stringify({
					credential_source: "session",
					raw_finish_reason: "tool_use",
				}),
			});
			expect(events[13]?.finish?.usage?.providerUsageJson).toContain(
				"noCacheTokens",
			);
			await expectProviderEventsFixture(
				events,
				"anthropic/basic-turn.events.json",
			);
		} finally {
			await mock.close();
		}
	});

	test("turns a mid-stream Anthropic transport disconnect into a retryable stream failure after partial events", async () => {
		const fixture = await readFile(AnthropicDisconnectFixtureUrl, "utf8");
		expect(fixture).toContain("source=real-provider-prefix");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture, {
			holdOpenAfterFixture: true,
		});
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const result = await collectEventsDisconnectingWhen(
				registry.stream({
					request: anthropicGoldenRequest(),
					credential: sessionAnthropicCredential(),
				}),
				(events) => {
					const types = events.map((event) => event.type);
					return (
						types.length === 2 &&
						types[0] ===
							ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START &&
						types[1] ===
							ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA &&
						events
							.map((event) => event.reasoning?.text ?? "")
							.join("")
							.includes("271828")
					);
				},
				mock.disconnect,
			);

			expect(mock.requests).toHaveLength(1);
			expect(mock.requests[0]?.headers["anthropic-beta"]).toBe(
				AnthropicBetaHeader,
			);
			expect(result.events.map((event) => event.type)).toEqual([
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA,
			]);
			expect(
				result.events.map((event) => event.reasoning?.text ?? "").join(""),
			).toContain("271828");
			expect(result.error).toBeInstanceOf(Error);
			expect((result.error as Error).message).toContain(
				"socket connection was closed unexpectedly",
			);
			expect(classifyProviderStreamError(result.error)).toMatchObject({
				code: "provider_stream_error",
				retryable: true,
				fatal: false,
				statusCode: 502,
			});
		} finally {
			await mock.disconnect();
			await mock.close();
		}
	});

	test("captures an official Anthropic cache-hit recording without double-counting cached input", async () => {
		const fixture = await readFile(AnthropicCacheHitFixtureUrl, "utf8");
		expect(fixture).toContain("cache_hit=true");
		expectFixtureHasNoProviderSecrets(fixture);
		const frames = providerCacheUsageFrames(fixture);
		expect(frames.messageStart).toMatchObject({
			input_tokens: 2,
			cache_creation_input_tokens: 0,
			cache_read_input_tokens: 15614,
		});
		expect(frames.terminalDelta).toMatchObject({
			input_tokens: 2,
			cache_creation_input_tokens: 0,
			cache_read_input_tokens: 15614,
		});
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const events = await collectEvents(
				registry.stream({
					request: anthropicCacheHitGoldenRequest(),
					credential: sessionAnthropicCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			await expectCapturedRequestFixture(
				captured,
				"anthropic-cache-hit/basic-turn.http.json",
			);
			expect(captured.headers["anthropic-beta"]).toBe(
				"interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14",
			);
			expect(captured.body.system).toMatchObject([
				{ cache_control: { type: "ephemeral" } },
			]);
			expect(captured.body.messages).toMatchObject([
				{ content: [{ cache_control: { type: "ephemeral" } }] },
			]);
			await expectProviderEventsFixture(
				events,
				"anthropic-cache-hit/basic-turn.events.json",
			);
			expect(events.at(-1)?.finish).toMatchObject({
				reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
				usage: {
					inputUncachedTokens: 2,
					inputCacheReadTokens: 15614,
					inputCacheWriteTokens: 0,
					inputTotalTokens: 15616,
					outputTotalTokens: 4,
					totalTokens: 15620,
				},
			});
		} finally {
			await mock.close();
		}
	});
});

describe("Session provider golden wire path", () => {
	test("turns a mid-stream Kimi transport disconnect into a retryable stream failure after partial events", async () => {
		const fixture = await readFile(KimiDisconnectFixtureUrl, "utf8");
		expect(fixture).toContain("source=real-provider-prefix");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture, {
			holdOpenAfterFixture: true,
		});
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const result = await collectEventsDisconnectingWhen(
				registry.stream({
					request: kimiK3GoldenRequest(),
					credential: sessionKimiCredential(),
				}),
				(events) =>
					events
						.map((event) => event.type)
						.includes(
							ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
						) &&
					events
						.map((event) => event.reasoning?.text ?? "")
						.join("")
						.includes("The"),
				mock.disconnect,
			);

			expect(mock.requests).toHaveLength(1);
			expect(mock.requests[0]?.headers["anthropic-beta"]).toBeUndefined();
			expect(result.events.map((event) => event.type)).toContain(
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
			);
			expect(
				result.events.map((event) => event.reasoning?.text ?? "").join(""),
			).toContain("The");
			expect(result.error).toBeInstanceOf(Error);
			expect(classifyProviderStreamError(result.error)).toMatchObject({
				code: "provider_stream_error",
				retryable: true,
				fatal: false,
				statusCode: 502,
			});
		} finally {
			await mock.disconnect();
			await mock.close();
		}
	});

	test("captures a Kimi cache-hit recording from the terminal message_delta usage frame", async () => {
		const fixture = await readFile(KimiCacheHitFixtureUrl, "utf8");
		expect(fixture).toContain("cache_hit=true");
		expectFixtureHasNoProviderSecrets(fixture);
		const frames = providerCacheUsageFrames(fixture);
		expect(frames.messageStart).toMatchObject({
			input_tokens: 8420,
			cache_creation_input_tokens: 0,
			cache_read_input_tokens: 0,
		});
		expect(frames.terminalDelta).toMatchObject({
			input_tokens: 228,
			cache_creation_input_tokens: 0,
			cache_read_input_tokens: 8192,
		});
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const events = await collectEvents(
				registry.stream({
					request: kimiCacheHitGoldenRequest(),
					credential: sessionKimiCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			await expectCapturedRequestFixture(
				captured,
				"moonshotai-k3-cache-hit/basic-turn.http.json",
			);
			expect(captured.headers["anthropic-beta"]).toBeUndefined();
			expect(captured.body.system).toMatchObject([
				{ cache_control: { type: "ephemeral" } },
			]);
			expect(captured.body.messages).toMatchObject([
				{ content: [{ cache_control: { type: "ephemeral" } }] },
			]);
			await expectProviderEventsFixture(
				events,
				"moonshotai-k3-cache-hit/basic-turn.events.json",
			);
			expect(events.at(-1)?.finish).toMatchObject({
				reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
				usage: {
					inputUncachedTokens: 228,
					inputCacheReadTokens: 8192,
					inputCacheWriteTokens: 0,
					inputTotalTokens: 8420,
					outputTotalTokens: 37,
					totalTokens: 8457,
				},
			});
		} finally {
			await mock.close();
		}
	});

	test("classifies a recorded Kimi HTTP provider error without platform-key fallback", async () => {
		const fixture = await readFile(KimiHttpErrorFixtureUrl, "utf8");
		expect(fixture).toContain("source=real-provider-http-error");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(httpFixtureBody(fixture), {
			status: 400,
			contentType: "application/json; charset=utf-8",
		});
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const result = await collectEventsUntilError(
				registry.stream({
					request: kimiK3GoldenRequest(),
					credential: sessionKimiCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.pathname).toBe("/coding/v1/messages");
			expect(captured.headers["x-api-key"]).toBe("sk-session-kimi");
			expect(captured.headers["anthropic-beta"]).toBeUndefined();
			expect(captured.body).toMatchObject({
				model: "k3",
				stream: true,
			});
			expect(result.events).toHaveLength(0);
			expect(result.error).toBeInstanceOf(ProviderKeyFailureError);
			expect(
				(result.error as ProviderKeyFailureError).classification,
			).toMatchObject({
				action: "fail-fast",
				providerError: {
					code: "provider_request_invalid",
					retryable: false,
					fatal: true,
					statusCode: 400,
				},
			});
		} finally {
			await mock.close();
		}
	});

	test("captures the Z.ai openai-compatible request shape and raises the replayed fixture", async () => {
		const fixture = await readFile(ZaiFixtureUrl, "utf8");
		expect(fixture).toContain(
			"recorded 2026-07-04 model_id=zai/glm-5.2 source=real-provider",
		);
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const events = await collectEvents(
				registry.stream({
					request: zaiGoldenRequest(),
					credential: sessionZaiCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			await expectCapturedRequestFixture(captured, "zai/basic-turn.http.json");
			expect(captured.method).toBe("POST");
			expect(captured.pathname).toBe("/api/coding/paas/v4/chat/completions");
			expect(captured.headers.authorization).toBe("Bearer sk-session-zai");
			expect(captured.body).toMatchObject({
				model: "glm-5.2",
				stream: true,
				stream_options: { include_usage: true },
				max_tokens: 1024,
				thinking: { type: "enabled", clear_thinking: false },
				reasoning_effort: "high",
			});
			expect(captured.body).not.toHaveProperty("temperature");
			expect(captured.body).not.toHaveProperty("top_p");
			expect(captured.body.messages).toEqual([
				{
					role: "system",
					content:
						"Keep visible output minimal. Do not explain. Use tool calls when requested.",
				},
				{
					role: "user",
					content:
						'Use brief hidden reasoning to verify 7 + 8 = 15. Then emit visible text exactly: ok. Then call Search exactly once with JSON input {"query":"tetral"}. Do not add any other visible text.',
				},
			]);
			expect(captured.body.tools).toEqual([
				{
					type: "function",
					function: {
						name: "Search",
						description: "Search.",
						parameters: {
							type: "object",
							properties: { query: { type: "string" } },
							required: ["query"],
							additionalProperties: false,
						},
					},
				},
			]);
			expectProviderToolTurn(events, {
				reasoningIncludes: ["7", "15"],
				textIncludes: "ok",
				toolName: "Search",
				toolInputJson: JSON.stringify({ query: "tetral" }),
			});
			await expectProviderEventsFixture(events, "zai/basic-turn.events.json");
			expect(events.at(-1)?.finish).toMatchObject({
				reason: ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS,
				usage: {
					inputUncachedTokens: 24,
					inputCacheReadTokens: 192,
					inputTotalTokens: 216,
					outputReasoningTokens: 66,
					outputTotalTokens: 79,
					totalTokens: 295,
				},
				metadataJson: JSON.stringify({
					credential_source: "session",
					raw_finish_reason: "tool_calls",
				}),
			});
		} finally {
			await mock.close();
		}
	});

	test("carries Z.ai assistant reasoning_content on the openai-compatible wire", async () => {
		const fixture = await readFile(ZaiFixtureUrl, "utf8");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			await collectEvents(
				registry.stream({
					request: zaiGoldenRequest({
						context: [
							{
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
								content: [
									{ reasoning: { text: "hidden thought", metadataJson: "{}" } },
									{ text: { text: "visible history" } },
								],
							},
							{
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
								content: [{ text: { text: "plain history" } }],
							},
							{
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
								content: [{ text: { text: "continue" } }],
							},
						],
					}),
					credential: sessionZaiCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			expect(JSON.stringify(mock.requests[0]?.body)).not.toContain(
				"openaiCompatible",
			);
			expect(mock.requests[0]?.body.messages).toEqual([
				{
					role: "system",
					content:
						"Keep visible output minimal. Do not explain. Use tool calls when requested.",
				},
				{
					role: "assistant",
					content: "visible history",
					reasoning_content: "hidden thought",
				},
				{
					role: "assistant",
					content: "plain history",
					reasoning_content: "",
				},
				{
					role: "user",
					content: "continue",
				},
			]);
		} finally {
			await mock.close();
		}
	});

	test("carries DeepSeek assistant reasoning_content on the openai-compatible wire", async () => {
		const fixture = await readFile(DeepSeekFixtureUrl, "utf8");
		expect(fixture).toContain(
			"recorded 2026-07-08 model_id=deepseek/deepseek-v4-pro source=real-provider",
		);
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const events = await collectEvents(
				registry.stream({
					request: deepSeekGoldenRequest({
						context: [
							{
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
								content: [{ text: { text: "Remember the prior answer." } }],
							},
							{
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
								content: [
									{
										reasoning: {
											text: "User asks for capital.",
											metadataJson: "{}",
										},
									},
									{ text: { text: "Paris." } },
								],
							},
							{
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
								content: [{ text: { text: "Continue." } }],
							},
						],
					}),
					credential: sessionDeepSeekCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			await expectCapturedRequestFixture(
				captured,
				"deepseek/basic-turn.http.json",
			);
			expect(captured.method).toBe("POST");
			expect(captured.pathname).toBe("/chat/completions");
			expect(captured.headers.authorization).toBe("Bearer sk-session-deepseek");
			expect(JSON.stringify(captured.body)).not.toContain("openaiCompatible");
			expect(captured.body).toMatchObject({
				stream: true,
				stream_options: { include_usage: true },
			});
			expect(captured.body.messages).toEqual([
				{
					role: "system",
					content:
						"Keep visible output minimal. Do not explain. Use tool calls when requested.",
				},
				{
					role: "user",
					content: "Remember the prior answer.",
				},
				{
					role: "assistant",
					content: "Paris.",
					reasoning_content: "User asks for capital.",
				},
				{
					role: "user",
					content: "Continue.",
				},
			]);
			expect(events.map((event) => event.type)).toContain(
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA,
			);
			expect(events.map((event) => event.text?.text ?? "").join("")).toBe("ok");
			await expectProviderEventsFixture(
				events,
				"deepseek/basic-turn.events.json",
			);
			expect(events.at(-1)?.finish).toMatchObject({
				reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
				usage: {
					inputUncachedTokens: 22,
					inputCacheReadTokens: 0,
					inputTotalTokens: 22,
					outputReasoningTokens: 20,
					outputTotalTokens: 22,
					totalTokens: 44,
				},
				metadataJson: JSON.stringify({
					credential_source: "session",
					raw_finish_reason: "stop",
				}),
			});
		} finally {
			await mock.close();
		}
	});

	test("sends approval-reviewer output as DeepSeek JSON-object mode", async () => {
		const [policyPrompt, outputSchemaJson] = await Promise.all([
			readFile(
				new URL(
					"../../../../../agent-runtime/packages/runtime-pod/src/assets/approval-reviewer-policy.md",
					import.meta.url,
				),
				"utf8",
			),
			readFile(ApprovalReviewerOutputSchemaUrl, "utf8"),
		]);
		const outputSchema = JSON.parse(outputSchemaJson) as Record<
			string,
			unknown
		>;
		const prompt = JSON.stringify(
			{
				output_schema: outputSchema,
				review_id: "arvw_wire",
				target_tool_name: "Bash",
				action_json: { cmd: "true" },
			},
			null,
			2,
		);
		const mock = createMockAnthropicServer(deepSeekStructuredOutputFixture());
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			await collectEvents(
				registry.stream({
					request: deepSeekGoldenRequest({
						requestKind:
							ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
						outputSchemaJson,
						system: [
							{
								kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
								text: "You review one proposed action.",
								cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_NONE,
							},
							{
								kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
								text: policyPrompt.trim(),
								cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
							},
						],
						context: [
							{
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
								content: [{ text: { text: prompt } }],
							},
						],
					}),
					credential: sessionDeepSeekCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.body.model).toBe("deepseek-v4-pro");
			expect(captured.body.response_format).toEqual({ type: "json_object" });
			expect(JSON.stringify(captured.body)).not.toContain("json_schema");
			const messages = JSON.stringify(captured.body.messages);
			expect(messages).toContain(
				"Return only JSON that matches the output_schema",
			);
			for (const field of [
				"outcome",
				"risk_level",
				"user_authorization",
				"rationale",
			]) {
				expect(messages).toContain(field);
			}
		} finally {
			await mock.close();
		}
	});

	test("keeps ordinary DeepSeek requests out of structured-output mode", async () => {
		const mock = createMockAnthropicServer(
			await readFile(DeepSeekFixtureUrl, "utf8"),
		);
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			await collectEvents(
				registry.stream({
					request: deepSeekGoldenRequest(),
					credential: sessionDeepSeekCredential(),
				}),
			);
			expect(mock.requests).toHaveLength(1);
			expect(mock.requests[0]!.body).not.toHaveProperty("response_format");
		} finally {
			await mock.close();
		}
	});

	test("turns a mid-stream DeepSeek transport disconnect into a retryable stream failure after partial events", async () => {
		const fixture = await readFile(DeepSeekDisconnectFixtureUrl, "utf8");
		expect(fixture).toContain("source=real-provider-prefix");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture, {
			holdOpenAfterFixture: true,
		});
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const result = await collectEventsDisconnectingWhen(
				registry.stream({
					request: deepSeekGoldenRequest(),
					credential: sessionDeepSeekCredential(),
				}),
				(events) =>
					events
						.map((event) => event.type)
						.includes(
							ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
						) &&
					events
						.map((event) => event.reasoning?.text ?? "")
						.join("")
						.includes("We"),
				mock.disconnect,
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.pathname).toBe("/chat/completions");
			expect(captured.headers.authorization).toBe("Bearer sk-session-deepseek");
			expect(result.events.map((event) => event.type)).toContain(
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
			);
			expect(
				result.events.map((event) => event.reasoning?.text ?? "").join(""),
			).toContain("We");
			expect(result.error).toBeInstanceOf(Error);
			expect(classifyProviderStreamError(result.error)).toMatchObject({
				code: "provider_stream_error",
				retryable: true,
				fatal: false,
				statusCode: 502,
			});
		} finally {
			await mock.disconnect();
			await mock.close();
		}
	});

	test("classifies a recorded DeepSeek HTTP provider error without platform-key fallback", async () => {
		const fixture = await readFile(DeepSeekHttpErrorFixtureUrl, "utf8");
		expect(fixture).toContain("source=real-provider-http-error");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(httpFixtureBody(fixture), {
			status: 400,
			contentType: "application/json; charset=utf-8",
		});
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const result = await collectEventsUntilError(
				registry.stream({
					request: deepSeekGoldenRequest(),
					credential: sessionDeepSeekCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.pathname).toBe("/chat/completions");
			expect(captured.headers.authorization).toBe("Bearer sk-session-deepseek");
			expect(captured.body).toMatchObject({
				model: "deepseek-v4-pro",
				stream: true,
			});
			expect(result.events).toHaveLength(0);
			expect(result.error).toBeInstanceOf(ProviderKeyFailureError);
			expect(
				(result.error as ProviderKeyFailureError).classification,
			).toMatchObject({
				action: "fail-fast",
				providerError: {
					code: "provider_request_invalid",
					retryable: false,
					fatal: true,
					statusCode: 400,
				},
			});
		} finally {
			await mock.close();
		}
	});

	test("turns a mid-stream Z.ai transport disconnect into a retryable stream failure after partial events", async () => {
		const fixture = await readFile(ZaiDisconnectFixtureUrl, "utf8");
		expect(fixture).toContain("source=real-provider-prefix");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(fixture, {
			holdOpenAfterFixture: true,
		});
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const result = await collectEventsDisconnectingWhen(
				registry.stream({
					request: zaiGoldenRequest(),
					credential: sessionZaiCredential(),
				}),
				(events) =>
					events
						.map((event) => event.type)
						.includes(
							ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
						) &&
					events
						.map((event) => event.reasoning?.text ?? "")
						.join("")
						.includes("The"),
				mock.disconnect,
			);

			expect(mock.requests).toHaveLength(1);
			expect(result.events.map((event) => event.type)).toContain(
				ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
			);
			expect(
				result.events.map((event) => event.reasoning?.text ?? "").join(""),
			).toContain("The");
			expect(result.error).toBeInstanceOf(Error);
			expect(classifyProviderStreamError(result.error)).toMatchObject({
				code: "provider_stream_error",
				retryable: true,
				fatal: false,
				statusCode: 502,
			});
		} finally {
			await mock.disconnect();
			await mock.close();
		}
	});

	test("classifies a recorded Z.ai HTTP provider error without platform-key fallback", async () => {
		const fixture = await readFile(ZaiHttpErrorFixtureUrl, "utf8");
		expect(fixture).toContain("source=real-provider-http-error");
		expectFixtureHasNoProviderSecrets(fixture);
		const mock = createMockAnthropicServer(httpFixtureBody(fixture), {
			status: 400,
			contentType: "application/json; charset=utf-8",
		});
		const registry = new ProviderClientRegistry({ fetch: mock.fetch });
		try {
			const result = await collectEventsUntilError(
				registry.stream({
					request: zaiGoldenRequest(),
					credential: sessionZaiCredential(),
				}),
			);

			expect(mock.requests).toHaveLength(1);
			const captured = mock.requests[0]!;
			expect(captured.pathname).toBe("/api/coding/paas/v4/chat/completions");
			expect(captured.headers.authorization).toBe("Bearer sk-session-zai");
			expect(captured.body).toMatchObject({
				model: "glm-5.2",
				stream: true,
			});
			expect(result.events).toHaveLength(0);
			expect(result.error).toBeInstanceOf(ProviderKeyFailureError);
			expect(
				(result.error as ProviderKeyFailureError).classification,
			).toMatchObject({
				action: "fail-fast",
				providerError: {
					code: "provider_request_invalid",
					retryable: false,
					fatal: true,
					statusCode: 400,
				},
			});
		} finally {
			await mock.close();
		}
	});
});

type ProviderWireFamily = "anthropic" | "openai" | "openai-compatible";

const MultiToolCalls = [
	{ modelToolCallId: "call:shared", name: "Alpha", input: { value: "alpha" } },
	{ modelToolCallId: "call/shared", name: "Beta", input: { value: "beta" } },
	{ modelToolCallId: "call_gamma", name: "Gamma", input: { value: "gamma" } },
] as const;

function noInventionToolHistoryRequest(
	base: ProviderRequest,
	signedReasoningMetadataJson?: string,
): ProviderRequest {
	const reasoning = (suffix: string) =>
		signedReasoningMetadataJson === undefined
			? []
			: [{
					reasoning: {
						text: `declared reasoning ${suffix}`,
						metadataJson: signedReasoningMetadataJson,
					},
				}];
	return {
		...base,
		requestId: `${base.requestId}_no_invention`,
		modelRequestId: `${base.modelRequestId}_no_invention`,
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [{ text: { text: "Continue from the settled Tool history." } }],
			},
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
				content: [
					...reasoning("mcp"),
					{ toolCall: {
						modelToolCallId: "call_mcp_completed",
						name: "github_create_issue",
						inputJson: JSON.stringify({ title: "fixed" }),
					} },
				],
			},
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
				content: [{ toolResult: {
					modelToolCallId: "call_mcp_completed",
					completed: { outputJson: JSON.stringify({ text: "mcp-completed-canary" }) },
					error: undefined,
					cancelled: undefined,
				} }],
			},
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
				content: [
					...reasoning("builtin"),
					{ toolCall: {
						modelToolCallId: "call_builtin_error",
						name: "exec_command",
						inputJson: JSON.stringify({ cmd: "false" }),
					} },
				],
			},
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
				content: [{ toolResult: {
					modelToolCallId: "call_builtin_error",
					completed: undefined,
					error: { errorJson: JSON.stringify({ error: { message: "builtin-error-canary" } }) },
					cancelled: undefined,
				} }],
			},
			...(signedReasoningMetadataJson === undefined
				? []
				: [{
						role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
						content: reasoning("only"),
					}]),
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [{ text: { text: "Proceed." } }],
			},
		],
		tools: [
			{
				name: "github_create_issue",
				description: "Create an issue.",
				function: {
					inputSchemaJson: JSON.stringify({ type: "object", properties: { title: { type: "string" } }, required: ["title"], additionalProperties: false }),
					outputSchemaJson: undefined,
				},
				freeform: undefined,
			},
			{
				name: "exec_command",
				description: "Execute a command.",
				function: {
					inputSchemaJson: JSON.stringify({ type: "object", properties: { cmd: { type: "string" } }, required: ["cmd"], additionalProperties: false }),
					outputSchemaJson: undefined,
				},
				freeform: undefined,
			},
		],
	};
}

function multiToolHistoryRequest(
	base: ProviderRequest,
	settledCallIds: readonly string[],
): ProviderRequest {
	const callIds = new Set<string>(
		MultiToolCalls.map((call) => call.modelToolCallId),
	);
	return {
		...base,
		requestId: `${base.requestId}_multi_tool_${settledCallIds.length}`,
		modelRequestId: `${base.modelRequestId}_multi_tool_${settledCallIds.length}`,
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [
					{ text: { text: "Run Alpha, Beta, and Gamma independently." } },
				],
			},
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
				content: MultiToolCalls.map((call) => ({
					toolCall: {
						modelToolCallId: call.modelToolCallId,
						name: call.name,
						inputJson: JSON.stringify(call.input),
					},
				})),
			},
			...settledCallIds.map((modelToolCallId) => {
				if (!callIds.has(modelToolCallId)) {
					throw new Error(`unknown settled Tool Call ${modelToolCallId}`);
				}
				return {
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
					content: [
						{
							toolResult: {
								modelToolCallId,
								completed: {
									outputJson: JSON.stringify({ settled: modelToolCallId }),
								},
								error: undefined,
								cancelled: undefined,
							},
						},
					],
				};
			}),
		],
		tools: MultiToolCalls.map((call) => ({
			name: call.name,
			description: `Run ${call.name}.`,
			function: {
				inputSchemaJson: JSON.stringify({
					type: "object",
					properties: { value: { type: "string" } },
					required: ["value"],
					additionalProperties: false,
				}),
				outputSchemaJson: undefined,
			},
			freeform: undefined,
		})),
	};
}

function providerContextPendingToolCallIds(
	request: ProviderRequest,
): readonly string[] {
	const callIds = request.context.flatMap((entry) =>
		entry.content.flatMap((item) =>
			item.toolCall === undefined ? [] : [item.toolCall.modelToolCallId],
		),
	);
	const resultIds = new Set(
		request.context.flatMap((entry) =>
			entry.content.flatMap((item) =>
				item.toolResult === undefined ? [] : [item.toolResult.modelToolCallId],
			),
		),
	);
	return callIds.filter((id) => !resultIds.has(id));
}

function assertCapturedMultiToolWire(
	family: ProviderWireFamily,
	body: Record<string, unknown>,
	expectedResultIds: readonly string[],
): void {
	const wire = capturedToolWire(family, body);
	const originalCallIds: string[] = MultiToolCalls.map((call) => call.modelToolCallId);
	const expectedCallIds: string[] = family === "anthropic"
		? ["call_shared", "call_shared_2", "call_gamma"]
		: originalCallIds;
	const providerIdByOriginal = new Map<string, string>(
		originalCallIds.map((id, index) => [id, expectedCallIds[index]!] as const),
	);
	expect(wire.calls.map((call) => call.id)).toEqual(expectedCallIds);
	expect(new Set(wire.calls.map((call) => call.id)).size).toBe(
		expectedCallIds.length,
	);
	expect(wire.results.map((result) => result.id)).toEqual(
		expectedResultIds.map((id) => providerIdByOriginal.get(id)!),
	);
	expect(new Set(wire.results.map((result) => result.id)).size).toBe(
		expectedResultIds.length,
	);

	for (const expected of MultiToolCalls) {
		const call = wire.calls.find(
			(candidate) => candidate.id === providerIdByOriginal.get(expected.modelToolCallId),
		);
		expect(call).toMatchObject({
			id: providerIdByOriginal.get(expected.modelToolCallId),
			name: expected.name,
			input: expected.input,
		});
	}
	for (const [index, result] of wire.results.entries()) {
		const call = wire.calls.find((candidate) => candidate.id === result.id);
		expect(call).toBeDefined();
		expect(result.index).toBeGreaterThan(call!.index);
		expect(JSON.stringify(result.output)).toContain(expectedResultIds[index]!);
	}

	const pendingCallIds = originalCallIds.filter(
		(id) => !expectedResultIds.includes(id),
	);
	expect(wire.calls.filter((call) => !wire.results.some((result) => result.id === call.id)).map((call) => call.id)).toEqual(
		pendingCallIds.map((id) => providerIdByOriginal.get(id)!),
	);
	assertCapturedToolMessageGrouping(family, body, expectedCallIds, wire.results.map((result) => result.id));
}

function assertCapturedToolMessageGrouping(
	family: ProviderWireFamily,
	body: Record<string, unknown>,
	expectedCallIds: readonly string[],
	expectedResultIds: readonly string[],
): void {
	if (family === "anthropic") {
		const messages = body.messages as readonly { readonly role: string; readonly content: readonly Record<string, unknown>[] }[];
		const callMessages = messages.filter((message) => message.content.some((part) => part.type === "tool_use"));
		expect(callMessages).toHaveLength(1);
		expect(callMessages[0]?.role).toBe("assistant");
		expect(callMessages[0]?.content.filter((part) => part.type === "tool_use").map((part) => part.id)).toEqual([...expectedCallIds]);
		const resultMessages = messages.filter((message) => message.content.some((part) => part.type === "tool_result"));
		expect(resultMessages.every((message) => message.role === "user")).toBe(true);
		expect(resultMessages.flatMap((message) => message.content.filter((part) => part.type === "tool_result").map((part) => part.tool_use_id))).toEqual([...expectedResultIds]);
		return;
	}
	if (family === "openai") {
		const items = body.input as readonly Record<string, unknown>[];
		const callItems = items.filter((item) => item.type === "function_call");
		expect(callItems.map((item) => item.call_id)).toEqual([...expectedCallIds]);
		expect(callItems.every((item) => item.type === "function_call")).toBe(true);
		const resultItems = items.filter((item) => item.type === "function_call_output");
		expect(resultItems.map((item) => item.call_id)).toEqual([...expectedResultIds]);
		return;
	}
	const messages = body.messages as readonly Record<string, unknown>[];
	const callMessages = messages.filter((message) => Array.isArray(message.tool_calls));
	expect(callMessages).toHaveLength(1);
	expect(callMessages[0]?.role).toBe("assistant");
	expect((callMessages[0]?.tool_calls as readonly Record<string, unknown>[]).map((call) => call.id)).toEqual([...expectedCallIds]);
	const resultMessages = messages.filter((message) => message.role === "tool");
	expect(resultMessages.map((message) => message.tool_call_id)).toEqual([...expectedResultIds]);
}

function capturedToolWire(
	family: ProviderWireFamily,
	body: Record<string, unknown>,
): {
	readonly calls: readonly CapturedToolCall[];
	readonly results: readonly CapturedToolResult[];
} {
	if (family === "anthropic") {
		const messages = body.messages as readonly {
			readonly content: readonly Record<string, unknown>[];
		}[];
		return {
			calls: messages.flatMap((message, index) =>
				message.content.flatMap((part) =>
					part.type === "tool_use"
						? [
								{
									id: String(part.id),
									name: String(part.name),
									input: part.input,
									index,
								},
							]
						: [],
				),
			),
			results: messages.flatMap((message, index) =>
				message.content.flatMap((part) =>
					part.type === "tool_result"
						? [{ id: String(part.tool_use_id), output: part.content, index }]
						: [],
				),
			),
		};
	}
	if (family === "openai") {
		const items = body.input as readonly Record<string, unknown>[];
		return {
			calls: items.flatMap((item, index) =>
				item.type === "function_call"
					? [
							{
								id: String(item.call_id),
								name: String(item.name),
								input: JSON.parse(String(item.arguments)),
								index,
							},
						]
					: [],
			),
			results: items.flatMap((item, index) =>
				item.type === "function_call_output"
					? [{ id: String(item.call_id), output: item.output, index }]
					: [],
			),
		};
	}
	const messages = body.messages as readonly Record<string, unknown>[];
	return {
		calls: messages.flatMap((message, index) =>
			!Array.isArray(message.tool_calls)
				? []
				: (message.tool_calls as readonly Record<string, unknown>[]).map(
						(call) => {
							const fn = call.function as Record<string, unknown>;
							return {
								id: String(call.id),
								name: String(fn.name),
								input: JSON.parse(String(fn.arguments)),
								index,
							};
						},
					),
		),
		results: messages.flatMap((message, index) =>
			message.role === "tool"
				? [{ id: String(message.tool_call_id), output: message.content, index }]
				: [],
		),
	};
}

interface CapturedToolCall {
	readonly id: string;
	readonly name: string;
	readonly input: unknown;
	readonly index: number;
}

interface CapturedToolResult {
	readonly id: string;
	readonly output: unknown;
	readonly index: number;
}

function capturedToolSequence(wire: {
	readonly calls: readonly CapturedToolCall[];
	readonly results: readonly CapturedToolResult[];
}): readonly (readonly ["call" | "result", string])[] {
	return [
		...wire.calls.map((call) => ({ type: "call" as const, id: call.id, index: call.index })),
		...wire.results.map((result) => ({ type: "result" as const, id: result.id, index: result.index })),
	]
		.sort((left, right) => left.index - right.index)
		.map((item) => [item.type, item.id] as const);
}

interface CapturedAnthropicRequest {
	readonly method: string;
	readonly pathname: string;
	readonly headers: Record<string, string>;
	readonly body: Record<string, unknown>;
}

function createMockAnthropicServer(
	fixture: string,
	options: {
		readonly holdOpenAfterFixture?: boolean;
		readonly status?: number;
		readonly contentType?: string;
	} = {},
): {
	readonly requests: CapturedAnthropicRequest[];
	readonly fetch: FetchFunction;
	readonly disconnect: () => Promise<void>;
	readonly close: () => Promise<void>;
} {
	const requests: CapturedAnthropicRequest[] = [];
	let disconnected = false;
	let server: ReturnType<typeof Bun.serve>;
	server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		// Keep Bun's default behavior explicit: the 3 s collector watchdog must
		// fire before both bun test's 5 s case limit and this 10 s server timeout.
		idleTimeout: 10,
		async fetch(request) {
			const url = new URL(request.url);
			requests.push({
				method: request.method,
				pathname: url.pathname,
				headers: Object.fromEntries(
					Array.from(request.headers.entries()).map(([key, value]) => [
						key.toLowerCase(),
						value,
					]),
				),
				body: (await request.json()) as Record<string, unknown>,
			});
			return new Response(
				options.holdOpenAfterFixture === true
					? fixtureStreamHoldingOpen(fixture)
					: fixtureResponseBody(fixture, options.contentType),
				{
					status: options.status ?? 200,
					headers: {
						"content-type": options.contentType ?? "text/event-stream",
						"cache-control": "no-cache",
					},
				},
			);
		},
	});
	return {
		requests,
		fetch: Object.assign(
			async (
				input: Parameters<FetchFunction>[0],
				init?: Parameters<FetchFunction>[1],
			) => {
				const original = new Request(input, init);
				const originalUrl = new URL(original.url);
				const body =
					original.method === "GET" || original.method === "HEAD"
						? undefined
						: await original.text();
				const requestInit: RequestInit = {
					method: original.method,
					headers: original.headers,
					signal: original.signal,
				};
				if (body !== undefined) {
					requestInit.body = body;
				}
				return fetch(
					new URL(`${originalUrl.pathname}${originalUrl.search}`, server.url),
					requestInit,
				);
			},
			{
				preconnect: () => {},
			},
		) satisfies FetchFunction,
		disconnect: async () => {
			if (disconnected) {
				return;
			}
			disconnected = true;
			await server.stop(true);
		},
		close: async () => {
			await server.stop(true);
		},
	};
}

function fixtureResponseBody(fixture: string, contentType?: string): string {
	if (
		(contentType ?? "text/event-stream").startsWith("text/event-stream") &&
		!fixture.endsWith("\n\n")
	) {
		return `${fixture}\n`;
	}
	return fixture;
}

function anthropicStructuredOutputFixture(): string {
	return [
		"event: message_start",
		'data: {"type":"message_start","message":{"model":"claude-opus-4-8","id":"msg_structured","type":"message","role":"assistant","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}}',
		"",
		"event: content_block_start",
		'data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}',
		"",
		"event: content_block_delta",
		'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"{\\"risk_level\\":\\"low\\",\\"user_authorization\\":\\"high\\",\\"outcome\\":\\"allow\\",\\"rationale\\":\\"authorized\\"}"}}',
		"",
		"event: content_block_stop",
		'data: {"type":"content_block_stop","index":0}',
		"",
		"event: message_delta",
		'data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":20}}',
		"",
		"event: message_stop",
		'data: {"type":"message_stop"}',
		"",
	].join("\n");
}

function deepSeekStructuredOutputFixture(): string {
	return [
		'data: {"id":"chatcmpl-review","object":"chat.completion.chunk","created":1,"model":"deepseek-v4-pro","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}],"usage":null}',
		"",
		'data: {"id":"chatcmpl-review","object":"chat.completion.chunk","created":1,"model":"deepseek-v4-pro","choices":[{"index":0,"delta":{"content":"{\\"risk_level\\":\\"low\\",\\"user_authorization\\":\\"high\\",\\"outcome\\":\\"allow\\",\\"rationale\\":\\"authorized\\"}"},"finish_reason":null}],"usage":null}',
		"",
		'data: {"id":"chatcmpl-review","object":"chat.completion.chunk","created":1,"model":"deepseek-v4-pro","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}',
		"",
		"data: [DONE]",
		"",
	].join("\n");
}

function fixtureStreamHoldingOpen(fixture: string): ReadableStream<Uint8Array> {
	const bytes = new TextEncoder().encode(fixture);
	return new ReadableStream({
		start(controller) {
			controller.enqueue(bytes);
		},
	});
}

function expectProviderToolTurn(
	events: readonly ProviderStreamEvent[],
	options: {
		readonly reasoningIncludes: readonly string[];
		readonly textIncludes: string;
		readonly toolName: string;
		readonly toolInputJson: string;
	},
): void {
	const types = events.map((event) => event.type);
	const reasoningStart = types.indexOf(
		ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START,
	);
	const reasoningEnd = types.indexOf(
		ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END,
	);
	const textStart = types.indexOf(
		ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
	);
	const textEnd = types.indexOf(
		ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,
	);
	const toolInputStart = types.indexOf(
		ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START,
	);
	const toolInputEnd = types.indexOf(
		ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END,
	);
	const toolCallIndex = types.indexOf(
		ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
	);
	const finishIndex = types.indexOf(
		ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
	);
	expect(reasoningStart).toBeGreaterThanOrEqual(0);
	expect(reasoningEnd).toBeGreaterThan(reasoningStart);
	expect(textStart).toBeGreaterThan(reasoningEnd);
	expect(textEnd).toBeGreaterThan(textStart);
	expect(toolInputStart).toBeGreaterThan(reasoningEnd);
	expect(toolInputEnd).toBeGreaterThan(toolInputStart);
	expect(toolCallIndex).toBeGreaterThan(toolInputEnd);
	expect(finishIndex).toBe(types.length - 1);

	const reasoningText = events
		.map((event) => event.reasoning?.text ?? "")
		.join("");
	for (const expected of options.reasoningIncludes) {
		expect(reasoningText).toContain(expected);
	}
	expect(events.map((event) => event.text?.text ?? "").join("")).toContain(
		options.textIncludes,
	);
	expect(
		JSON.parse(events.map((event) => event.toolInput?.text ?? "").join("")),
	).toEqual(JSON.parse(options.toolInputJson));
	const toolCalls = events.filter(
		(event) =>
			event.type ===
			ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
	);
	expect(toolCalls).toHaveLength(1);
	expect(toolCalls[0]?.toolCall).toMatchObject({
		name: options.toolName,
		inputJson: options.toolInputJson,
	});
}

function providerCacheUsageFrames(fixture: string): {
	readonly messageStart: Record<string, unknown>;
	readonly terminalDelta: Record<string, unknown>;
} {
	let messageStart: Record<string, unknown> | undefined;
	let terminalDelta: Record<string, unknown> | undefined;
	for (const match of fixture.matchAll(/^data:\s*(\{.*\})$/gm)) {
		const frame = JSON.parse(match[1]!) as Record<string, unknown>;
		if (frame.type === "message_start") {
			const message = frame.message as Record<string, unknown> | undefined;
			messageStart = message?.usage as Record<string, unknown> | undefined;
		}
		if (frame.type === "message_delta") {
			terminalDelta = frame.usage as Record<string, unknown> | undefined;
		}
	}
	expect(messageStart).toBeDefined();
	expect(terminalDelta).toBeDefined();
	return {
		messageStart: messageStart ?? {},
		terminalDelta: terminalDelta ?? {},
	};
}

function httpFixtureBody(fixture: string): string {
	return fixture.replace(/^: fixture provenance: [^\n]*\n\n/, "");
}

function expectFixtureHasNoProviderSecrets(fixture: string): void {
	expect(fixture).not.toContain(["sk", "ant"].join("-"));
	expect(fixture).not.toContain(["sk", "kimi"].join("-"));
	expect(fixture).not.toContain(["sk", "session"].join("-"));
	expect(fixture).not.toContain("sk-session");
	expect(fixture.toLowerCase()).not.toContain("authorization");
	expect(fixture.toLowerCase()).not.toContain("x-api-key");
}

interface GoldenHTTPRequestFixture {
	readonly _fixtureProvenance: GoldenFixtureProvenance;
	readonly method: string;
	readonly pathname: string;
	readonly headers?: Record<string, string>;
	readonly body: Record<string, unknown>;
	readonly absentBodyFields?: readonly string[];
}

interface GoldenEventsFixture {
	readonly _fixtureProvenance: GoldenFixtureProvenance;
	readonly eventTypes?: readonly string[];
	readonly eventTypeRuns?: readonly {
		readonly type: string;
		readonly count: number;
	}[];
	readonly reasoningTextIncludes?: readonly string[];
	readonly text?: string;
	readonly textIncludes?: string;
	readonly toolInputJson?: string;
	readonly toolCalls?: readonly {
		readonly name: string;
		readonly inputJson: string;
	}[];
	readonly finish: {
		readonly reason: string;
		readonly usage: Record<string, unknown>;
		readonly metadata?: Record<string, unknown>;
	};
}

interface GoldenFixtureProvenance {
	readonly recorded: string;
	readonly model_id: string;
	readonly provider: string;
	readonly source: string;
	readonly source_fixture: string;
}

async function readGoldenJsonFixture<
	T extends { readonly _fixtureProvenance?: GoldenFixtureProvenance },
>(relativePath: string): Promise<T> {
	const fixture = await readFile(
		new URL(relativePath, import.meta.url),
		"utf8",
	);
	expectGoldenJsonFixtureHasNoProviderSecrets(fixture);
	const parsed = JSON.parse(fixture) as T;
	const provenance = parsed._fixtureProvenance;
	expect(provenance).toBeDefined();
	expect(provenance?.recorded).toMatch(/^\d{4}-\d{2}-\d{2}$/);
	expect(provenance?.model_id).toMatch(/^[a-z0-9]+\/[^/]+$/);
	expect(provenance?.provider).not.toBe("");
	expect(provenance?.source).toBe("real-provider");
	expect(provenance?.source_fixture).toMatch(/^fixtures\/.+/);
	return parsed;
}

async function collectGoldenJsonFixturePaths(
	root: URL,
	prefix = "",
): Promise<readonly string[]> {
	const entries = await readdir(root, { withFileTypes: true });
	const files: string[] = [];
	for (const entry of entries) {
		if (entry.name === "fixtures") {
			continue;
		}
		const entryURL = new URL(
			`${entry.name}${entry.isDirectory() ? "/" : ""}`,
			root,
		);
		const relativePath = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
		if (entry.isDirectory()) {
			files.push(
				...(await collectGoldenJsonFixturePaths(entryURL, relativePath)),
			);
			continue;
		}
		if (entry.isFile() && entry.name.endsWith(".json")) {
			files.push(relativePath);
		}
	}
	return files.sort();
}

function expectGoldenJsonFixtureHasNoProviderSecrets(fixture: string): void {
	expect(fixture).not.toContain(["sk", "ant"].join("-"));
	expect(fixture).not.toContain(["sk", "kimi"].join("-"));
	expect(fixture).not.toContain(["sk", "session"].join("-"));
	expect(fixture).not.toContain("sk-session");
}

async function expectCapturedRequestFixture(
	captured: CapturedAnthropicRequest,
	relativePath: string,
): Promise<void> {
	const expected =
		await readGoldenJsonFixture<GoldenHTTPRequestFixture>(relativePath);
	expect(captured.method).toBe(expected.method);
	expect(captured.pathname).toBe(expected.pathname);
	if (expected.headers !== undefined) {
		expect(redactCapturedHeaders(captured.headers)).toMatchObject(
			expected.headers,
		);
	}
	expect(captured.body).toMatchObject(expected.body);
	for (const field of expected.absentBodyFields ?? []) {
		expect(captured.body).not.toHaveProperty(field);
	}
}

async function expectProviderEventsFixture(
	events: readonly ProviderStreamEvent[],
	relativePath: string,
): Promise<void> {
	const expected =
		await readGoldenJsonFixture<GoldenEventsFixture>(relativePath);
	const eventTypes = events.map((event) =>
		providerStreamEventTypeToJSON(event.type),
	);
	if (expected.eventTypeRuns !== undefined) {
		expect(eventTypeRuns(eventTypes)).toEqual(expected.eventTypeRuns);
	} else {
		expect(eventTypes).toEqual([...(expected.eventTypes ?? [])]);
	}

	const reasoningText = events
		.map((event) => event.reasoning?.text ?? "")
		.join("");
	for (const text of expected.reasoningTextIncludes ?? []) {
		expect(reasoningText).toContain(text);
	}

	const visibleText = events.map((event) => event.text?.text ?? "").join("");
	if (expected.text !== undefined) {
		expect(visibleText).toBe(expected.text);
	}
	if (expected.textIncludes !== undefined) {
		expect(visibleText).toContain(expected.textIncludes);
	}

	if (expected.toolInputJson !== undefined) {
		expect(
			JSON.parse(events.map((event) => event.toolInput?.text ?? "").join("")),
		).toEqual(JSON.parse(expected.toolInputJson));
	}
	if (expected.toolCalls !== undefined) {
		expect(
			events
				.filter((event) => event.toolCall !== undefined)
				.map((event) => ({
					name: event.toolCall?.name ?? "",
					inputJson: event.toolCall?.inputJson ?? "",
				})),
		).toEqual([...expected.toolCalls]);
	}

	const finish = events.at(-1)?.finish;
	expect(finish).toBeDefined();
	expect(
		providerFinishReasonToJSON(
			finish?.reason ?? ProviderFinishReason.PROVIDER_FINISH_REASON_UNSPECIFIED,
		),
	).toBe(expected.finish.reason);
	expect(finish?.usage).toMatchObject(expected.finish.usage);
	if (expected.finish.metadata !== undefined) {
		expect(JSON.parse(finish?.metadataJson ?? "{}")).toEqual(
			expected.finish.metadata,
		);
	}
}

function eventTypeRuns(
	eventTypes: readonly string[],
): readonly { readonly type: string; readonly count: number }[] {
	const runs: { type: string; count: number }[] = [];
	for (const type of eventTypes) {
		const last = runs.at(-1);
		if (last?.type === type) {
			last.count++;
		} else {
			runs.push({ type, count: 1 });
		}
	}
	return runs;
}

function redactCapturedHeaders(
	headers: Record<string, string>,
): Record<string, string> {
	const redacted = { ...headers };
	if (redacted.authorization?.startsWith("Bearer sk-session-")) {
		redacted.authorization = "Bearer <session-api-key>";
	}
	if (redacted.authorization === "Bearer oauth-access") {
		redacted.authorization = "Bearer <oauth-access-token>";
	}
	if (redacted["x-api-key"]?.startsWith("sk-session-")) {
		redacted["x-api-key"] = "<session-api-key>";
	}
	return redacted;
}

function plainTextAttachment(
	filename: string,
	text: string,
): {
	readonly request: ProviderRequest["attachments"][number];
	readonly resolved: ResolvedProviderRequestAttachment;
} {
	const request: ProviderRequest["attachments"][number] = {
		transient: undefined,
		fileBacked: {
			sourceEventId: "sevt_plain_text",
			fileId: "file_plain_text",
		},
		mime: "text/plain",
		filename,
	};
	return {
		request,
		resolved: {
			...request,
			data: new TextEncoder().encode(text),
		},
	};
}

function openAIGoldenRequest(): ProviderRequest {
	return validProviderRequest({
		requestId: "req_live_openai_1",
		modelRequestId: "mreq_live_openai_1",
		model: { providerId: "openai", modelId: "gpt-5.5", variant: "xhigh" },
		sessionId: "sesn_live_openai_fixture",
		system: [
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				text: "Keep visible output minimal and call tools exactly as requested.",
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_NONE,
			},
		],
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
				content: [
					{
						reasoning: {
							text: "",
							metadataJson: JSON.stringify({
								openai: {
									encrypted_content: "enc_prior_reasoning",
									itemId: "rs_prior_item",
									id: "raw_reasoning_id_removed",
								},
							}),
						},
					},
				],
			},
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [
					{
						text: {
							text: 'Use brief hidden reasoning to verify 7 + 8 = 15. Then emit visible text exactly: ok. Then call Search exactly once with JSON input {"query":"tetral"}. Do not add any other visible text.',
						},
					},
				],
			},
		],
		tools: [
			{
				name: "Search",
				description: "Search.",
				function: {
					inputSchemaJson: JSON.stringify({
						type: "object",
						properties: { query: { type: "string" } },
						required: ["query"],
						additionalProperties: false,
					}),
					outputSchemaJson: undefined,
				},
			},
		],
		attachments: [],
		limits: {
			maxOutputTokens: 1024,
			timeoutMs: 60_000,
		},
	});
}

function openAIGPT56SolGoldenRequest(
	sessionId = "sesn_live_gpt56_sol_official_fixture",
): ProviderRequest {
	return {
		...openAIGoldenRequest(),
		requestId: "req_live_openai_56_1",
		modelRequestId: "mreq_live_openai_56_1",
		model: { providerId: "openai", modelId: "gpt-5.6-sol", variant: "" },
		sessionId,
		system: [
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				text: "Reply tersely and do not call tools.",
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
			},
		],
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [{ text: { text: "Reply with exactly: ok" } }],
			},
		],
		tools: [],
		limits: { maxOutputTokens: 128, timeoutMs: 120_000 },
	};
}

function anthropicGoldenRequest(): ProviderRequest {
	return validProviderRequest({
		requestId: "req_live_anthropic_1",
		modelRequestId: "mreq_live_anthropic_1",
		model: {
			providerId: "anthropic",
			modelId: "claude-opus-4-8",
			variant: "high",
		},
		sessionId: "sesn_live_anthropic_fixture",
		system: [
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				text: "You are a deterministic fixture recorder for Tetral Gateway tests. Use summarized thinking when available before acting.",
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
			},
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
				text: "Operate as the session fixture specialist.",
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
			},
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_TOOL_GUIDANCE,
				text: "When asked to use a tool, call exactly the named tool with the requested JSON input.",
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
			},
		],
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [
					{
						text: {
							text: 'Use your thinking block to verify this checksum before acting: 271828 + 314159 = 585987. After that, emit visible text exactly: reading now. Then call the Read tool exactly once with JSON input {"path":"/workspace/app.ts"}. Do not add other visible text.',
						},
					},
				],
			},
		],
		tools: [
			{
				name: "Read",
				description: "Read a file.",
				function: {
					inputSchemaJson: JSON.stringify({
						type: "object",
						properties: { path: { type: "string" } },
						required: ["path"],
						additionalProperties: false,
					}),
					outputSchemaJson: undefined,
				},
			},
		],
		attachments: [],
		limits: {
			maxOutputTokens: 1024,
			timeoutMs: 60_000,
		},
	});
}

function anthropicFableGoldenRequest(): ProviderRequest {
	return validProviderRequest({
		requestId: "req_live_fable_1",
		modelRequestId: "mreq_live_fable_1",
		model: { providerId: "anthropic", modelId: "claude-fable-5", variant: "" },
		sessionId: "sesn_live_fable_fixture",
		system: [
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				text: "Reply tersely and do not call tools.",
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_NONE,
			},
		],
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [{ text: { text: "Reply with exactly: ok" } }],
			},
		],
		tools: [],
		attachments: [],
		limits: { maxOutputTokens: 128, timeoutMs: 120_000 },
	});
}

function anthropicCacheHitGoldenRequest(): ProviderRequest {
	return validProviderRequest({
		requestId: "req_cache_hit_anthropic_1",
		modelRequestId: "mreq_cache_hit_anthropic_1",
		model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
		sessionId: "sesn_cache_hit_anthropic_fixture",
		system: [
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				text: cacheHitSystemBlock(),
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
			},
		],
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [
					{
						text: { text: "Reply with exactly: ok" },
					},
				],
			},
		],
		tools: [],
		attachments: [],
		limits: {
			maxOutputTokens: 32,
			timeoutMs: 120_000,
		},
	});
}

function kimiK3GoldenRequest(): ProviderRequest {
	return validProviderRequest({
		requestId: "req_live_k3_1",
		modelRequestId: "mreq_live_k3_1",
		model: { providerId: "moonshotai", modelId: "kimi-k3", variant: "" },
		sessionId: "sesn_live_k3_fixture",
		system: [
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				text: "Reply tersely and do not call tools.",
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_NONE,
			},
		],
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [{ text: { text: "Reply with exactly: ok" } }],
			},
		],
		tools: [],
		attachments: [],
		limits: { maxOutputTokens: 1024, timeoutMs: 120_000 },
	});
}

function kimiK3ToolGoldenRequest(): ProviderRequest {
	return validProviderRequest({
		...kimiK3GoldenRequest(),
		requestId: "req_live_k3_tool_1",
		modelRequestId: "mreq_live_k3_tool_1",
		system: [
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				text: "Call the Read tool exactly once with the requested path.",
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_NONE,
			},
		],
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [
					{
						text: {
							text: 'Call Read exactly once with JSON input {"path":"/workspace/app.ts"}.',
						},
					},
				],
			},
		],
		tools: [
			{
				name: "Read",
				description: "Read a file.",
				function: {
					inputSchemaJson: JSON.stringify({
						type: "object",
						properties: { path: { type: "string" } },
						required: ["path"],
						additionalProperties: false,
					}),
					outputSchemaJson: undefined,
				},
			},
		],
	});
}

function kimiK3ReplayGoldenRequest(
	reasoningText: string,
	signature: string,
	toolCallID: string,
	toolName: string,
	toolInputJSON: string,
): ProviderRequest {
	const request = kimiK3ToolGoldenRequest();
	return {
		...request,
		requestId: "req_live_k3_replay_1",
		modelRequestId: "mreq_live_k3_replay_1",
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
				content: [
					{
						reasoning: {
							text: reasoningText,
							metadataJson: JSON.stringify({ anthropic: { signature } }),
						},
					},
					{
						toolCall: {
							modelToolCallId: toolCallID,
							name: toolName,
							inputJson: toolInputJSON,
						},
					},
					{
						toolResult: {
							modelToolCallId: toolCallID,
							completed: {
								outputJson: JSON.stringify({ content: "fixture tool result" }),
							},
							error: undefined,
							cancelled: undefined,
						},
					},
				],
			},
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [{ text: { text: "Continue." } }],
			},
		],
	};
}

function kimiCacheHitGoldenRequest(): ProviderRequest {
	return validProviderRequest({
		requestId: "req_cache_hit_kimi_1",
		modelRequestId: "mreq_cache_hit_kimi_1",
		model: { providerId: "moonshotai", modelId: "kimi-k3", variant: "" },
		sessionId: "sesn_cache_hit_kimi_fixture",
		system: [
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				text: cacheHitSystemBlock(),
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
			},
		],
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [
					{
						text: { text: "Reply with exactly: ok" },
					},
				],
			},
		],
		tools: [],
		attachments: [],
		limits: {
			maxOutputTokens: 32,
			timeoutMs: 120_000,
		},
	});
}

function cacheHitSystemBlock(): string {
	return Array.from(
		{ length: 260 },
		(_, index) =>
			`Tetral cache recording invariant line ${String(index).padStart(3, "0")}: this stable block is intentionally repeated so provider prompt-cache minimums are exceeded for a deterministic cache-hit fixture.`,
	).join("\\n");
}

function zaiGoldenRequest(
	overrides: Partial<ProviderRequest> = {},
): ProviderRequest {
	return validProviderRequest({
		requestId: "req_live_zai_1",
		modelRequestId: "mreq_live_zai_1",
		model: { providerId: "zai", modelId: "glm-5.2", variant: "high" },
		sessionId: "sesn_live_zai_fixture",
		system: [
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				text: "Keep visible output minimal. Do not explain. Use tool calls when requested.",
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_NONE,
			},
		],
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [
					{
						text: {
							text: 'Use brief hidden reasoning to verify 7 + 8 = 15. Then emit visible text exactly: ok. Then call Search exactly once with JSON input {"query":"tetral"}. Do not add any other visible text.',
						},
					},
				],
			},
		],
		tools: [
			{
				name: "Search",
				description: "Search.",
				function: {
					inputSchemaJson: JSON.stringify({
						type: "object",
						properties: { query: { type: "string" } },
						required: ["query"],
						additionalProperties: false,
					}),
					outputSchemaJson: undefined,
				},
			},
		],
		attachments: [],
		limits: {
			maxOutputTokens: 1024,
			timeoutMs: 60_000,
		},
		...overrides,
	});
}

function deepSeekGoldenRequest(
	overrides: Partial<ProviderRequest> = {},
): ProviderRequest {
	return validProviderRequest({
		requestId: "req_live_deepseek_1",
		modelRequestId: "mreq_live_deepseek_1",
		model: {
			providerId: "deepseek",
			modelId: "deepseek-v4-pro",
			variant: "high",
		},
		sessionId: "sesn_live_deepseek_fixture",
		system: [
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				text: "Keep visible output minimal. Do not explain. Use tool calls when requested.",
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_NONE,
			},
		],
		context: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [
					{
						text: { text: "Say ok." },
					},
				],
			},
		],
		tools: [],
		attachments: [],
		limits: {
			maxOutputTokens: 1024,
			timeoutMs: 60_000,
		},
		...overrides,
	});
}

async function collectEvents(
	events: AsyncIterable<ProviderStreamEvent>,
): Promise<readonly ProviderStreamEvent[]> {
	const output: ProviderStreamEvent[] = [];
	for await (const event of events) {
		output.push(event);
	}
	return output;
}

async function collectEventsUntilError(
	events: AsyncIterable<ProviderStreamEvent>,
): Promise<{
	readonly events: readonly ProviderStreamEvent[];
	readonly error: unknown;
}> {
	const output: ProviderStreamEvent[] = [];
	try {
		for await (const event of events) {
			output.push(event);
		}
	} catch (error) {
		return { events: output, error };
	}
	throw new Error("expected provider stream to fail");
}

async function collectEventsDisconnectingWhen(
	events: AsyncIterable<ProviderStreamEvent>,
	predicate: (events: readonly ProviderStreamEvent[]) => boolean,
	disconnect: () => Promise<void>,
	watchdogMs = 3_000,
): Promise<{
	readonly events: readonly ProviderStreamEvent[];
	readonly error: unknown;
}> {
	const output: ProviderStreamEvent[] = [];
	const iterator = events[Symbol.asyncIterator]();
	let disconnected = false;
	const disconnectOnce = async (): Promise<void> => {
		if (disconnected) {
			return;
		}
		disconnected = true;
		await disconnect();
	};
	const diagnostic = (stage: string): string => {
		const eventTypes = output.map((event) =>
			providerStreamEventTypeToJSON(event.type),
		);
		return `timed out waiting for provider stream ${stage} after ${watchdogMs} ms; received event types: ${JSON.stringify(eventTypes)}`;
	};

	if (predicate(output)) {
		await disconnectOnce();
	}

	while (true) {
		let watchdog: ReturnType<typeof setTimeout> | undefined;
		let watchdogExpired = false;
		let next: IteratorResult<ProviderStreamEvent>;
		try {
			next = await Promise.race([
				iterator.next(),
				new Promise<never>((_, reject) => {
					watchdog = setTimeout(() => {
						watchdogExpired = true;
						reject(new Error(diagnostic("predicate/error")));
					}, watchdogMs);
				}),
			]);
		} catch (error) {
			if (watchdog !== undefined) {
				clearTimeout(watchdog);
			}
			if (watchdogExpired) {
				await disconnectOnce();
				throw error;
			}
			if (!disconnected) {
				await disconnectOnce();
				throw error;
			}
			return { events: output, error };
		} finally {
			if (watchdog !== undefined) {
				clearTimeout(watchdog);
			}
		}
		if (next.done) {
			throw new Error("expected provider stream to fail");
		}
		output.push(next.value);
		if (!disconnected && predicate(output)) {
			await disconnectOnce();
		}
	}
}

function sessionAnthropicCredential(): ResolvedProviderCredential {
	return {
		source: "session",
		authType: "provider_api_key",
		providerId: "anthropic",
		supplyMode: "anthropic-api-key",
		vaultId: "vlt_1",
		credentialId: "cred_1",
		accessMode: "api_key",
		apiKey: "sk-session-anthropic",
	};
}

function sessionOpenAICredential(): ResolvedProviderCredential {
	return {
		source: "session",
		authType: "provider_api_key",
		providerId: "openai",
		supplyMode: "openai-api-key",
		vaultId: "vlt_1",
		credentialId: "cred_openai",
		accessMode: "user_api_key",
		apiKey: "sk-session-openai",
	};
}

function sessionOpenAIOAuthCredential(): ResolvedProviderCredential {
	return {
		source: "session",
		authType: "provider_oauth",
		providerId: "openai",
		supplyMode: "openai-chatgpt-oauth",
		vaultId: "vlt_1",
		credentialId: "cred_openai_oauth",
		accessMode: "oauth",
		accessToken: "oauth-access",
		refreshToken: "oauth-refresh",
		expiresAt: "2999-01-01T00:00:00Z",
		accountId: "acct_1",
	};
}

function sessionKimiCredential(): ResolvedProviderCredential {
	return {
		source: "session",
		authType: "provider_api_key",
		providerId: "moonshotai",
		supplyMode: "moonshotai-code-api-key",
		vaultId: "vlt_1",
		credentialId: "cred_kimi",
		accessMode: "api_key",
		apiKey: "sk-session-kimi",
	};
}

function sessionDeepSeekCredential(): ResolvedProviderCredential {
	return {
		source: "session",
		authType: "provider_api_key",
		providerId: "deepseek",
		supplyMode: "deepseek-api-key",
		vaultId: "vlt_1",
		credentialId: "cred_deepseek",
		accessMode: "user_api_key",
		apiKey: "sk-session-deepseek",
	};
}

function sessionZaiCredential(): ResolvedProviderCredential {
	return {
		source: "session",
		authType: "provider_api_key",
		providerId: "zai",
		supplyMode: "zai-coding-api-key",
		vaultId: "vlt_1",
		credentialId: "cred_zai",
		accessMode: "api_key",
		apiKey: "sk-session-zai",
	};
}
