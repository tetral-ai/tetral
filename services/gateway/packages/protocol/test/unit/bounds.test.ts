import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import {
	MaxIdBytes,
	MaxMetadataBytes,
	MaxProviderContextTextJsonBytes,
	MaxProviderRequestAttachments,
	MaxProviderRequestToolOutputJsonBytes,
	MaxProviderToolCallInputJsonBytes,
	MaxProviderUsageJsonBytes,
	MaxSchemaBytes,
	MaxTextBytes,
	truncateUtf8Bytes,
	validateProviderRequest,
	validateProviderStreamEvent,
} from "@tetral/gateway-protocol/src/bounds.js";
import type { ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
	ProviderContextRole,
	ProviderFinishReason,
	ProviderRequestKind,
	ProviderRequest as ProviderRequestMessage,
	ProviderStreamEvent as ProviderStreamEventMessage,
	ProviderStreamEventType,
	SystemCacheHint,
	SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { validProviderRequest } from "./fixtures.js";

const approvalReviewerOutputSchemaJson = await readFile(
	new URL(
		"../../../../../agent-runtime/packages/runtime-pod/src/assets/approval-reviewer-output-schema.json",
		import.meta.url,
	),
	"utf8",
);

describe("Gateway protocol bounds", () => {
  test("preserves real Bridge Start ACK identity separately from new Gateway content IDs", () => {
    expect(validateProviderRequest(validProviderRequest({modelRequestStartEventId:"evt_07105b47741e2ac3"}))).toEqual({ok:true});
    for(const modelRequestStartEventId of ["", "x".repeat(MaxIdBytes+1), "evt_\ud800"])
      expectInvalid(validateProviderRequest(validProviderRequest({modelRequestStartEventId})));
    expectInvalid(validateProviderStreamEvent({frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_THINKING_STARTED,thinkingStarted:{providerPartId:"reason",eventId:"evt_07105b47741e2ac3"}}));
  });
	test("accepts complete reasoning beyond retired fragment limits while retaining metadata bounds", () => {
    const base = {frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_COMPLETE};
    const reasoningComplete={providerPartId:"r",thinkingEventId:"evt_00000000000000000000000000000001",text:"x".repeat(65537),providerMetadataJson:`{"x":"${"m".repeat(16384-8)}"}`};
    expect(validateProviderStreamEvent({...base,reasoningComplete})).toEqual({ok:true});
    expectInvalid(validateProviderStreamEvent({...base,reasoningComplete:{...reasoningComplete,providerMetadataJson:`{"x":"${"m".repeat(16384-7)}"}`}}));
	});

	test("accepts a valid Runtime-to-Gateway ProviderRequest snapshot", () => {
		expect(validateProviderRequest(validProviderRequest())).toEqual({
			ok: true,
		});
	});

	test("rejects non-text user context before the lowering boundary", () => {
		const base = validProviderRequest();
		for (const content of [
			[{ reasoning: { text: "private", metadataJson: "{}" } }],
			[{
				toolCall: {
					modelToolCallId: "call_user",
					name: "Read",
					inputJson: "{}",
				},
			}],
			[{
				toolResult: {
					modelToolCallId: "call_user",
					completed: { outputJson: "{}" },
				},
			}],
		]) {
			expectInvalid(
				validateProviderRequest({
					...base,
					context: [{
						role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
						content,
					}],
				}),
			);
		}
	});

	test("admits exactly one bounded function or freeform tool declaration arm", () => {
		const base = validProviderRequest();
		const tool = base.tools[0]!;
		expect(
			validateProviderRequest({
				...base,
				tools: [
					{
						...tool,
						function: undefined,
						freeform: { larkGrammar: "start: PATCH" },
					},
				],
			}),
		).toEqual({ ok: true });

		for (const invalidTool of [
			{ ...tool, function: undefined },
			{ ...tool, freeform: { larkGrammar: "start: PATCH" } },
			{ ...tool, function: { inputSchemaJson: "[]" } },
			{ ...tool, function: { inputSchemaJson: "not-json" } },
			{
				...tool,
				function: {
					inputSchemaJson: JSON.stringify({ type: "object" }),
					outputSchemaJson: "[]",
				},
			},
			{ ...tool, function: undefined, freeform: { larkGrammar: "" } },
			{
				...tool,
				function: undefined,
				freeform: { larkGrammar: "x".repeat(MaxSchemaBytes + 1) },
			},
		]) {
			expectInvalid(validateProviderRequest({ ...base, tools: [invalidTool] }));
		}
	});

	test("admits multi-megabyte provider context items while keeping system segments at 64 KiB", () => {
		expect(MaxProviderContextTextJsonBytes).toBe(16 * 1024 * 1024);
		const multiMegabyteText = "x".repeat(2 * 1024 * 1024);
		const base = validProviderRequest();
		for (const [role, part] of [
			[
				ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				{ text: { text: multiMegabyteText } },
			],
			[
				ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
				{ reasoning: { text: multiMegabyteText, metadataJson: "{}" } },
			],
		] as const) {
			expect(
				validateProviderRequest({
					...base,
					context: [{ role, content: [part] }],
				}),
			).toEqual({ ok: true });
		}

		expectInvalid(
			validateProviderRequest({
				...base,
				system: [{ ...base.system[0]!, text: "x".repeat(MaxTextBytes + 1) }],
			}),
		);
	});

	test("rejects empty text parts while retaining signed empty reasoning", () => {
		const base = validProviderRequest();
		expectInvalid(
			validateProviderRequest({
				...base,
				context: [
					{
						...base.context[0]!,
						content: [{ text: { text: "" } }],
					},
				],
			}),
		);
		expect(
			validateProviderRequest({
				...base,
				context: [
					{
						...base.context[0]!,
						role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
						content: [
							{
								reasoning: {
									text: "",
									metadataJson: JSON.stringify({
										anthropic: { signature: "sig_1" },
									}),
								},
							},
						],
					},
				],
			}),
		).toEqual({ ok: true });
	});

	test("admits signed empty reasoning while retaining the reasoning byte ceiling", () => {
		const base = validProviderRequest();
		const withReasoning = (text: string) => ({
			...base,
			context: [
				{
					...base.context[0]!,
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
					content: [
						{
							reasoning: {
								text,
								metadataJson: JSON.stringify({
									anthropic: { signature: "sig_1" },
								}),
							},
						},
					],
				},
			],
		});

		expect(validateProviderRequest(withReasoning(""))).toEqual({ ok: true });
		expect(
			validateProviderRequest(
				withReasoning("x".repeat(MaxProviderContextTextJsonBytes - 2)),
			),
		).toEqual({ ok: true });
		expectInvalid(
			validateProviderRequest(
				withReasoning("x".repeat(MaxProviderContextTextJsonBytes - 1)),
			),
		);
	});

	test("uses the protocol identifier limit for provider stream ids", () => {
    const base={frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE};
    const textComplete={providerPartId:"i".repeat(128),eventId:"evt_00000000000000000000000000000001",text:"body"};
    expect(validateProviderStreamEvent({...base,textComplete})).toEqual({ok:true});
    expectInvalid(validateProviderStreamEvent({...base,textComplete:{...textComplete,providerPartId:"i".repeat(129)}}));
	});

	test("accepts all contract-owned ProviderRequest request kinds", () => {
		for (const requestKind of [
			ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
		]) {
			expect(
				validateProviderRequest(
					validProviderRequest({
						requestKind,
						outputSchemaJson:
							requestKind ===
							ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER
								? approvalReviewerOutputSchemaJson
								: undefined,
					}),
				),
			).toEqual({ ok: true });
		}
	});

	test("requires a request-level output schema only for approval reviewer requests", () => {
		expect(
			validateProviderRequest(
				validProviderRequest({
					requestKind:
						ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
					outputSchemaJson: approvalReviewerOutputSchemaJson,
				}),
			),
		).toEqual({ ok: true });

		for (const requestKind of [
			ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
		]) {
			expect(
				validateProviderRequest(
					validProviderRequest({ requestKind, outputSchemaJson: undefined }),
				),
			).toEqual({ ok: true });
			expectInvalid(
				validateProviderRequest(
					validProviderRequest({
						requestKind,
						outputSchemaJson: approvalReviewerOutputSchemaJson,
					}),
				),
			);
		}

		for (const outputSchemaJson of [
			undefined,
			"",
			"not-json",
			"[]",
			JSON.stringify(true),
		]) {
			expectInvalid(
				validateProviderRequest(
					validProviderRequest({
						requestKind:
							ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
						outputSchemaJson,
					}),
				),
			);
		}
	});

	test("round-trips the request-level output schema through generated protobuf stubs", () => {
		const request = validProviderRequest({
			requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
			outputSchemaJson: approvalReviewerOutputSchemaJson,
		});

		const decoded = ProviderRequestMessage.decode(
			ProviderRequestMessage.encode(request).finish(),
		);

		expect(decoded.outputSchemaJson).toBe(approvalReviewerOutputSchemaJson);
		expect(validateProviderRequest(decoded)).toEqual({ ok: true });
	});

	test("accepts all contract-owned system segments and closed provider Tool outcomes", () => {
		const base = validProviderRequest();
		const system = base.system[0]!;
		for (const kind of [
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_TOOL_GUIDANCE,
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL,
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
		]) {
			expect(
				validateProviderRequest(
					validProviderRequest({
						system: [{ ...system, kind }],
					}),
				),
			).toEqual({ ok: true });
		}
		for (const cacheHint of [
			SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
			SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
			SystemCacheHint.SYSTEM_CACHE_HINT_NONE,
		]) {
			expect(
				validateProviderRequest(
					validProviderRequest({
						system: [{ ...system, cacheHint }],
					}),
				),
			).toEqual({ ok: true });
		}
		for (const outcome of [
			{
				completed: { outputJson: "{}" },
				error: undefined,
				cancelled: undefined,
			},
			{
				completed: undefined,
				error: { errorJson: "{}" },
				cancelled: undefined,
			},
			{ completed: undefined, error: undefined, cancelled: {} },
		]) {
			expect(
				validateProviderRequest(
					validProviderRequest({
						context: [
							{
								...base.context[0]!,
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
								content: [
									{
										toolCall: {
											modelToolCallId: "call_1",
											name: "Read",
											inputJson: "{}",
										},
									},
									{ toolResult: { modelToolCallId: "call_1", ...outcome } },
								],
							},
						],
					}),
				),
			).toEqual({ ok: true });
		}
	});

	test("uses independent byte ceilings for provider tool input, tool output, and usage JSON", () => {
		const base = validProviderRequest();
		const message = base.context[0]!;
		const toolCall = {
			toolCall: {
				modelToolCallId: "call_1",
				name: "Read",
				inputJson: jsonObjectAtBytes(MaxProviderToolCallInputJsonBytes),
			},
		};
		const toolResult = {
			toolResult: {
				modelToolCallId: "call_1",
				completed: {
					outputJson: jsonObjectAtBytes(MaxProviderRequestToolOutputJsonBytes),
				},
				error: undefined,
				cancelled: undefined,
			},
		};
		expect(
			validateProviderRequest({
				...base,
				context: [
					{
						...message,
						role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
						content: [toolCall, toolResult],
					},
				],
			}),
		).toEqual({ ok: true });
		expectInvalid(
			validateProviderRequest({
				...base,
				context: [
					{
						...message,
						role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
						content: [
							toolCall,
							{
								toolResult: {
									...toolResult.toolResult,
									completed: {
										outputJson: jsonObjectAtBytes(
											MaxProviderRequestToolOutputJsonBytes + 1,
										),
									},
								},
							},
						],
					},
				],
			}),
		);

		const eventBase = {
			type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL_COMPLETE,
		};
		expect(
			validateProviderStreamEvent({
        frameSequence:1,
				...eventBase,
				toolCallComplete: {
					modelToolCallId: "call_large",
					name: "memory",
					inputJson: jsonObjectAtBytes(MaxProviderToolCallInputJsonBytes),
					providerMetadataJson: "{}",
				},
			}),
		).toEqual({ ok: true });
		expectInvalid(
			validateProviderStreamEvent({
        frameSequence:1,
				...eventBase,
				toolCallComplete: {
					modelToolCallId: "call_large",
					name: "memory",
					inputJson: jsonObjectAtBytes(MaxProviderToolCallInputJsonBytes + 1),
					providerMetadataJson: "{}",
				},
			}),
		);

		const finish = (providerUsageJson: string): ProviderStreamEvent => ({
			frameSequence: 1,
			type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
			finish: {
				reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
				usage: {
					inputTotalTokens: 1,
					inputUncachedTokens: 1,
					outputTotalTokens: 1,
					providerUsageJson,
				},
				metadataJson: "{}",
			},
		});
		expect(
			validateProviderStreamEvent(
				finish(jsonObjectAtBytes(MaxProviderUsageJsonBytes)),
			),
		).toEqual({ ok: true });
		expectInvalid(
			validateProviderStreamEvent(
				finish(jsonObjectAtBytes(MaxProviderUsageJsonBytes + 1)),
			),
		);
	});

	test("rejects lone JSON surrogates at stream ingress while accepting a scalar pair", () => {
    const base={frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL_COMPLETE};
    const toolCallComplete={modelToolCallId:"call",name:"Read",inputJson:'{"x":"\\ud83d\\ude00"}',providerMetadataJson:"{}"};
    expect(validateProviderStreamEvent({...base,toolCallComplete})).toEqual({ok:true});
    for(const inputJson of ['{"x":"\\ud83d"}','{"x":"\\ude00"}'])expectInvalid(validateProviderStreamEvent({...base,toolCallComplete:{...toolCallComplete,inputJson}}));
	});

	test("truncates provider errors by UTF-8 bytes without splitting code points", () => {
		expect(truncateUtf8Bytes(`${"a".repeat(5)}éé`, 8)).toBe("aaaaaé");
		expect(
			new TextEncoder().encode(truncateUtf8Bytes("你".repeat(4), 8)).byteLength,
		).toBe(6);
	});

	test("pairs provider Tool calls and terminal results only by modelToolCallId", () => {
		const base = validProviderRequest();
		const message = base.context[0]!;
		const call = {
			toolCall: {
				modelToolCallId: "call_repair",
				name: "unknown_tool",
				inputJson: "{}",
			},
		};

		expect(
			validateProviderRequest(
				validProviderRequest({
					context: [
						{
							...message,
							role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
							content: [call],
						},
					],
				}),
			),
		).toEqual({ ok: true });
		for (const outcome of [
			{
				completed: { outputJson: "{}" },
				error: undefined,
				cancelled: undefined,
			},
			{
				completed: undefined,
				error: { errorJson: "{}" },
				cancelled: undefined,
			},
			{ completed: undefined, error: undefined, cancelled: {} },
		]) {
			expect(
				validateProviderRequest(
					validProviderRequest({
						context: [
							{
								...message,
								role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
								content: [
									call,
									{
										toolResult: { modelToolCallId: "call_repair", ...outcome },
									},
								],
							},
						],
					}),
				),
			).toEqual({ ok: true });
		}
	});

	test("validates ProviderRequest attachment lowering hints", () => {
		const base = validProviderRequest();
		const attachment = base.attachments[0]!;
		for (const mime of [
			"application/pdf",
			"image/png",
			"image/jpeg",
			"image/gif",
			"image/webp",
		]) {
			expect(
				validateProviderRequest(
					validProviderRequest({
						attachments: [{ ...attachment, mime }],
					}),
				),
			).toEqual({ ok: true });
		}
		expect(
			validateProviderRequest(
				validProviderRequest({
					attachments: [
						{
							...attachment,
							transient: {
								...attachment.transient!,
								pageRange: "1-3,5",
								detail: "high",
							},
						},
					],
				}),
			),
		).toEqual({ ok: true });
		expect(
			validateProviderRequest(
				validProviderRequest({
					attachments: [
						{
							...attachment,
							transient: {
								...attachment.transient!,
								pageRange: "",
								detail: "auto",
							},
						},
					],
				}),
			),
		).toEqual({ ok: true });
		expect(
			validateProviderRequest(
				validProviderRequest({
					attachments: [
						{
							...attachment,
							transient: {
								...attachment.transient!,
								pageRange: "",
								detail: "provider-defined-detail",
							},
						},
					],
				}),
			),
		).toEqual({ ok: true });
		expect(
			validateProviderRequest(
				validProviderRequest({
					attachments: [
						{
							...attachment,
							transient: {
								...attachment.transient!,
								pageRange: "",
								detail: "d".repeat(128),
							},
						},
					],
				}),
			),
		).toEqual({ ok: true });
		expect(
			validateProviderRequest(
				validProviderRequest({
					attachments: [
						{
							transient: undefined,
							fileBacked: { sourceEventId: "sevt_user_1", fileId: "file_1" },
							mime: "text/plain",
							filename: "notes.txt",
						},
					],
				}),
			),
		).toEqual({ ok: true });

		for (const request of [
			validProviderRequest({
				attachments: [
					{
						...attachment,
						transient: {
							...attachment.transient!,
							pageRange: "0",
							detail: "auto",
						},
					},
				],
			}),
			validProviderRequest({
				attachments: [
					{
						...attachment,
						transient: {
							...attachment.transient!,
							pageRange: "3-1",
							detail: "auto",
						},
					},
				],
			}),
			validProviderRequest({
				attachments: [
					{
						...attachment,
						transient: {
							...attachment.transient!,
							pageRange: "1, two",
							detail: "auto",
						},
					},
				],
			}),
			validProviderRequest({
				attachments: [
					{
						...attachment,
						transient: {
							...attachment.transient!,
							pageRange: "1-2,".repeat(32),
							detail: "auto",
						},
					},
				],
			}),
			validProviderRequest({
				attachments: [
					{
						...attachment,
						transient: { ...attachment.transient!, pageRange: "", detail: "" },
					},
				],
			}),
			validProviderRequest({
				attachments: [
					{
						...attachment,
						transient: {
							...attachment.transient!,
							pageRange: "",
							detail: "d".repeat(129),
						},
					},
				],
			}),
			validProviderRequest({
				attachments: [{ ...attachment, mime: "image/svg+xml" }],
			}),
			validProviderRequest({
				attachments: [{ ...attachment, mime: "image/bmp" }],
			}),
			validProviderRequest({
				attachments: [{ ...attachment, mime: "text/plain" }],
			}),
			validProviderRequest({
				attachments: [
					{ ...attachment, transient: undefined, fileBacked: undefined },
				],
			}),
			validProviderRequest({
				attachments: [
					{
						...attachment,
						fileBacked: { sourceEventId: "sevt_user_1", fileId: "file_1" },
					},
				],
			}),
		]) {
			expectInvalid(validateProviderRequest(request));
		}
	});

	test("round-trips exactly one ProviderRequest attachment origin", () => {
		const transient = validProviderRequest().attachments[0]!;
		const fileBacked = {
			transient: undefined,
			fileBacked: { sourceEventId: "sevt_user_1", fileId: "file_1" },
			mime: "application/pdf",
			filename: "report.pdf",
		};

		for (const attachment of [transient, fileBacked]) {
			const request = validProviderRequest({ attachments: [attachment] });
			const decoded = ProviderRequestMessage.decode(
				ProviderRequestMessage.encode(request).finish(),
			);
			expect(decoded.attachments).toEqual([attachment]);
			expect(validateProviderRequest(decoded)).toEqual({ ok: true });
		}
	});

	test("rejects ProviderRequest attachment overflow before provider work", () => {
		const attachment = validProviderRequest().attachments[0]!;
		const attachments = Array.from(
			{ length: MaxProviderRequestAttachments },
			(_, index) => ({
				...attachment,
				transient: { ...attachment.transient!, attachmentRef: `att_${index}` },
			}),
		);

		expect(
			validateProviderRequest(validProviderRequest({ attachments })),
		).toEqual({ ok: true });
		expectInvalid(
			validateProviderRequest(
				validProviderRequest({
					attachments: [
						...attachments,
						{
							...attachment,
							transient: {
								...attachment.transient!,
								attachmentRef: "att_overflow",
							},
						},
					],
				}),
			),
		);
	});

	test("rejects duplicate file-backed attachment origins before provider work", () => {
		const fileBacked = {
			transient: undefined,
			fileBacked: {
				sourceEventId: "sevt_user_duplicate",
				fileId: "file_duplicate",
			},
			mime: "image/png",
			filename: "duplicate.png",
		};

		expectInvalid(
			validateProviderRequest(
				validProviderRequest({
					attachments: [
						fileBacked,
						{ ...fileBacked, filename: "same-origin-again.png" },
					],
				}),
			),
		);
	});

	test("rejects malformed ProviderRequest fields before Gateway side effects", () => {
		const base = validProviderRequest();
		for (const request of [
			{ ...base, requestId: "" },
			{ ...base, modelRequestId: "" },
			{ ...base, workspaceId: "" },
			{ ...base, sessionId: "" },
			{ ...base, sessionThreadId: "" },
			{ ...base, bindingId: "" },
			{ ...base, bindingGeneration: 0 },
			{ ...base, runtimeBindingToken: "" },
			{ ...base, requestKind: 0 },
			{ ...base, model: undefined },
			{ ...base, model: { providerId: "", modelId: "gpt-test", variant: "" } },
			{
				...base,
				tools: [{ ...base.tools[0]!, function: { inputSchemaJson: "[]" } }],
			},
			{
				...base,
				tools: [
					{ ...base.tools[0]!, function: { inputSchemaJson: "not-json" } },
				],
			},
			{
				...base,
				attachments: [{ ...base.attachments[0]!, mime: "text/plain" }],
			},
			{ ...base, limits: undefined },
			{ ...base, limits: { maxOutputTokens: -1, timeoutMs: 30_000 } },
			{
				...base,
				context: [{ ...base.context[0]!, role: 99 as ProviderContextRole }],
			},
			{ ...base, context: [{ ...base.context[0]!, content: [] }] },
		]) {
			expectInvalid(validateProviderRequest(request));
		}
	});

	test("accepts a zero output limit as unset so the model's documented limit governs", () => {
		const base = validProviderRequest();
		expect(
			validateProviderRequest({
				...base,
				limits: { maxOutputTokens: 0, timeoutMs: 30_000 },
			}),
		).toEqual({ ok: true });
	});

	test("validates ProviderStreamEvent payload shape", () => {
		expect(
			validateProviderStreamEvent({
        frameSequence:1,
				type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
				providerError: {
					metadataJson: "{}",
					error: {
						code: "provider_unavailable",
						message: "unavailable",
						retryable: true,
						fatal: false,
						statusCode: 503,
						retryAfterMs: 0,
					},
				},
			}),
		).toEqual({ ok: true });
	});

	test("accepts one bounded non-terminal attachment rejection envelope", () => {
		const event = {
            frameSequence:1,
			type: 13,
			attachmentRejections: {
				rejections: [
					{
						transientAttachmentRef: "att_expired",
						fileBacked: undefined,
						reason: 1,
					},
					{
						transientAttachmentRef: undefined,
						fileBacked: {
							sourceEventId: "sevt_user_large",
							fileId: "file_large",
						},
						reason: 2,
					},
				],
			},
		} as unknown as ProviderStreamEvent;

		expect(validateProviderStreamEvent(event)).toEqual({ ok: true });
	});

	test("rejects negative ProviderError numeric fields", () => {
		const providerError = {
			metadataJson: "{}",
			error: {
				code: "provider_unavailable",
				message: "unavailable",
				retryable: true,
				fatal: false,
				statusCode: 503,
				retryAfterMs: 0,
			},
		};

		for (const error of [
			{ ...providerError.error, statusCode: -1 },
			{ ...providerError.error, retryAfterMs: -1 },
		]) {
			expectInvalid(
				validateProviderStreamEvent({
        frameSequence:1,
					type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
					providerError: { ...providerError, error },
				}),
			);
		}
	});

  test("preserves Finish usage validation and model-limit protobuf fields", () => {
    const usage = {inputTotalTokens:1,inputUncachedTokens:1,outputTotalTokens:2,totalTokens:3,providerUsageJson:"{}"};
    const finish = {reason:ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,usage,metadataJson:"{}",contextWindowTokens:500_000,inputLimitTokens:372_000,outputTokenLimit:128_000};
    const frame = {frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,finish};
    expect(validateProviderStreamEvent(frame)).toEqual({ok:true});
    const decoded = ProviderStreamEventMessage.decode(ProviderStreamEventMessage.encode(frame).finish());
    expect(decoded.finish).toMatchObject({usage,contextWindowTokens:500_000,inputLimitTokens:372_000,outputTokenLimit:128_000});
    expect(validateProviderStreamEvent(decoded)).toEqual({ok:true});
    for (const invalid of [
      {...finish,usage:{...usage,inputTotalTokens:-1}},
      {...finish,usage:{...usage,providerUsageJson:"not-json"}},
      {...finish,contextWindowTokens:0},
      {...finish,inputLimitTokens:1.5},
    ]) expectInvalid(validateProviderStreamEvent({...frame,finish:invalid}));
  });

	test("rejects malformed ProviderStreamEvent payloads per event variant", () => {
    const base={frameSequence:1};
    const id="evt_00000000000000000000000000000001";
    const valid:ProviderStreamEvent[]=[
      {...base,type:14,thinkingStarted:{providerPartId:"r",eventId:id}},
      {...base,type:15,textComplete:{providerPartId:"t",eventId:id,text:"body"}},
      {...base,type:16,reasoningComplete:{providerPartId:"r",thinkingEventId:id,text:"",providerMetadataJson:'{"anthropic":{"signature":"signed"}}'}},
      {...base,type:17,toolCallComplete:{modelToolCallId:"call",name:"Read",inputJson:"{}",providerMetadataJson:"{}"}},
      {...base,type:11,finish:{reason:ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,metadataJson:"{}"}},
      {...base,type:12,providerError:{metadataJson:"{}",error:{code:"provider_error",message:"safe",retryable:true,fatal:false,statusCode:503,retryAfterMs:0}}},
      {...base,type:13,attachmentRejections:{rejections:[{transientAttachmentRef:"att",reason:1}]}}
    ];
    for(const frame of valid){expect(validateProviderStreamEvent(frame)).toEqual({ok:true});expect(validateProviderStreamEvent(ProviderStreamEventMessage.decode(ProviderStreamEventMessage.encode(frame).finish()))).toEqual({ok:true});for(const frameSequence of [0,-1,0xffffffff+1,1.5])expectInvalid(validateProviderStreamEvent({...frame,frameSequence}));}
    for(const type of [0,1,2,3,4,5,6,7,8,9,10,-1,18])expectInvalid(validateProviderStreamEvent({...valid[1]!,type}));
    expectInvalid(validateProviderStreamEvent({...valid[1]!,reasoningComplete:valid[2]!.reasoningComplete}));
    expectInvalid(validateProviderStreamEvent({...valid[1]!,textComplete:{providerPartId:"t",eventId:id,text:""}}));
    expectInvalid(validateProviderStreamEvent({...valid[1]!,textComplete:{providerPartId:"t",eventId:"sevt_legacy",text:"body"}}));
    expectInvalid(validateProviderStreamEvent({...valid[1]!,textComplete:{providerPartId:"t",eventId:id,text:"\ud83d"}}));
    for(const reason of [1,2,3,4,5,6,7])expect(validateProviderStreamEvent({...valid[4]!,finish:{reason,metadataJson:"{}"}})).toEqual({ok:true});
    for(const bad of [{reason:0,metadataJson:"{}"},{reason:1,metadataJson:"broken"},{reason:1,metadataJson:"{}",outputTokenLimit:-1}])expectInvalid(validateProviderStreamEvent({...valid[4]!,finish:bad}));
	});
});

function expectInvalid(result: {
	readonly ok: boolean;
	readonly message?: string;
}) {
	expect(result.ok).toBe(false);
	if (!result.ok) {
		expect(result.message).toBe("invalid internal request");
	}
}

function jsonObjectAtBytes(bytes: number): string {
	const fixedBytes = new TextEncoder().encode('{"x":""}').byteLength;
	return `{"x":"${"x".repeat(bytes - fixedBytes)}"}`;
}
