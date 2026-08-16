import { describe, expect, test } from "bun:test";
import type { CallOptions } from "@grpc/grpc-js";
import { Metadata } from "@grpc/grpc-js";
import type {
	RuntimeInternalToolRepairCommit,
	SessionEventEnvelope,
	SessionEventWriterFinishIdleEnvelope,
	SessionEventWriterRequestEndEnvelope,
	SessionEventWriterRuntimeTerminationEnvelope,
	SessionEventWriterToolSettlementEnvelope,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
	AgentRuntimeBridgeServiceClient,
	CommitInputsRequest,
	CommitInternalToolRepairRequest,
	CommitRuntimeTerminationRequest,
	CommitTaskNotificationResultRequest,
	FinishIdleRequest,
	LoadContextRequest,
	ReadAgentMailRequest,
	SettleToolResultRequest,
	WriteEventRequest,
	WriteRequestEndRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
	BridgeAPIContextLoader,
	BridgeAPIControlInputCommitter,
	BridgeAPIEventWriter,
	BridgeAPIInternalToolRepairCommitter,
} from "../../src/bridge-client.js";
import type {
	RuntimePodLogger,
	RuntimePodLogRecord,
} from "../../src/logger.js";

describe("Bridge operation-specific Runtime adapters", () => {
	test("loads sealed context and the open Request draft as direct durable facts", async () => {
		const bridge = new TypedBridge();
		const loader = new BridgeAPIContextLoader(options(bridge));

		const loaded = await loader.loadThreadContext(threadScope());

		expect(bridge.loadContextRequests).toEqual([
			{ scope: expect.objectContaining({ sessionThreadId: "thrd_1" }) },
		]);
		expect(loaded.contextEntries).toEqual([
			{
				messageSequence: 1,
				contextKind: "user",
				parts: [{ type: "text", text: "hello" }],
			},
			{
				messageSequence: 2,
				contextKind: "assistant",
				parts: [
					{
						type: "tool_call",
						modelToolCallId: "call_1",
						toolName: "lookup",
						canonicalInput: { city: "Paris" },
					},
					{
						type: "tool_result",
						modelToolCallId: "call_1",
						result: {
							type: "completed",
							output: { text: "sunny" },
						},
					},
				],
			},
		]);
		expect(loaded.openRequestDraft).toEqual({
			modelRequestId: "mrq_open",
			messageSequence: 3,
			parts: [{ type: "text", text: "draft" }],
		});
	});

	test("rejects malformed direct durable context without a compatibility parser", async () => {
		const bridge = new TypedBridge();
		bridge.contextJson = JSON.stringify({
			...durableContext(),
			contextEntries: [{ id: "legacy_message" }],
		});
		const loader = new BridgeAPIContextLoader(options(bridge));

		await expect(loader.loadThreadContext(threadScope())).rejects.toMatchObject(
			{ code: "schema_mismatch" },
		);
	});

	test("rejects unknown top-level cold fields with safe phase diagnostics", async () => {
		const bridge = new TypedBridge();
		const records: RuntimePodLogRecord[] = [];
		const logger: RuntimePodLogger = {
			info: (record) => records.push(record),
			error: (record) => records.push(record),
		};
		const loader = new BridgeAPIContextLoader(options(bridge, logger));

		bridge.contextJson = "{malformed";
		await expect(loader.loadThreadContext(threadScope())).rejects.toMatchObject(
			{ code: "schema_mismatch" },
		);
		expect(records.at(-1)).toMatchObject({
			event: "runtime_context_load_parse_failed",
			phase: "context_json_parse",
			reason: "invalid_context_json",
			"session.id": "sesn_1",
			"thread.id": "thrd_1",
		});

		bridge.contextJson = JSON.stringify({
			...durableContext(),
			unexpectedField: { value: "secret-payload" },
		});
		await expect(loader.loadThreadContext(threadScope())).rejects.toMatchObject(
			{ code: "schema_mismatch" },
		);
		expect(records.at(-1)).toMatchObject({
			event: "runtime_context_load_parse_failed",
			phase: "context_envelope_parse",
			reason: "invalid_context_envelope_shape",
			"session.id": "sesn_1",
			"thread.id": "thrd_1",
		});
	});

	test("classifies every cold-context parse boundary without logging payloads", async () => {
		const canary = "COLD_CONTEXT_SECRET_CANARY";
		const cases: ReadonlyArray<{
			readonly phase: RuntimePodLogRecord["phase"];
			readonly reason: RuntimePodLogRecord["reason"];
			readonly context: () => string;
		}> = [
			{
				phase: "context_json_parse",
				reason: "invalid_context_json",
				context: () => `{"${canary}"`,
			},
			{
				phase: "context_envelope_parse",
				reason: "invalid_context_envelope_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), unexpected: canary }),
			},
			{
				phase: "durable_context_parse",
				reason: "invalid_durable_context_shape",
				context: () =>
					JSON.stringify({
						...durableContext(),
						contextEntries: [{ canary }],
					}),
			},
			{
				phase: "open_request_draft_parse",
				reason: "invalid_open_request_draft_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), openRequestDraft: canary }),
			},
			{
				phase: "turn_facts_parse",
				reason: "invalid_turn_facts_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), turnFacts: { events: canary } }),
			},
			{
				phase: "thread_context_prefix_parse",
				reason: "invalid_thread_context_prefix_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), threadContextPrefix: canary }),
			},
			{
				phase: "thread_metadata_parse",
				reason: "invalid_thread_metadata_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), thread: { role: canary } }),
			},
			{
				phase: "runtime_config_parse",
				reason: "invalid_runtime_config_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), runtimeConfig: canary }),
			},
			{
				phase: "mcp_manifests_parse",
				reason: "invalid_mcp_manifests_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), mcpManifests: canary }),
			},
			{
				phase: "pending_tool_uses_parse",
				reason: "invalid_pending_tool_uses_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), pendingToolUses: canary }),
			},
			{
				phase: "pending_sandbox_executions_parse",
				reason: "invalid_pending_sandbox_executions_shape",
				context: () =>
					JSON.stringify({
						...durableContext(),
						pendingSandboxExecutions: canary,
					}),
			},
			{
				phase: "pending_attachments_parse",
				reason: "invalid_pending_attachments_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), pendingAttachments: canary }),
			},
			{
				phase: "pending_agent_mail_parse",
				reason: "invalid_pending_agent_mail_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), pendingAgentMail: canary }),
			},
		];
		for (const testCase of cases) {
			const bridge = new TypedBridge();
			const records: RuntimePodLogRecord[] = [];
			bridge.contextJson = testCase.context();
			const loader = new BridgeAPIContextLoader(
				options(bridge, {
					info: (record) => records.push(record),
					error: (record) => records.push(record),
				}),
			);
			let rejected = false;
			try {
				await loader.loadThreadContext(threadScope());
			} catch (error) {
				rejected = true;
				expect(error).toMatchObject({ code: "schema_mismatch" });
			}
			if (!rejected) {
				throw new Error(`${testCase.phase} unexpectedly accepted its canary`);
			}
			expect(records).toHaveLength(1);
			expect(records[0]).toMatchObject({
				event: "runtime_context_load_parse_failed",
				phase: testCase.phase,
				reason: testCase.reason,
				"workspace.id": "wksp_1",
				"session.id": "sesn_1",
				"thread.id": "thrd_1",
			});
			expect(JSON.stringify(records)).not.toContain(canary);
		}
	});

	test("a throwing cold-context logger cannot replace the parse rejection", async () => {
		const bridge = new TypedBridge();
		let loggerCalls = 0;
		bridge.contextJson = JSON.stringify({
			...durableContext(),
			pendingAgentMail: "malformed",
		});
		const loader = new BridgeAPIContextLoader(
			options(bridge, {
				info: () => undefined,
				error: () => {
					loggerCalls += 1;
					throw new Error("logger failure");
				},
			}),
		);
		await expect(loader.loadThreadContext(threadScope())).rejects.toMatchObject({
			code: "schema_mismatch",
			reason: "load context returned malformed direct durable facts",
		});
		expect(loggerCalls).toBe(1);
	});

	test("rejects retired snake-case cold aliases even beside valid camel-case fields", async () => {
		const validRuntimeConfig = {
			configGeneration: 2,
			approvalMode: "default",
			toolPolicy: {},
			installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
		};
		const validManifest = {
			mcpServerName: "github",
			manifestETag: "etag_1",
			manifestGeneration: 1,
			readiness: "ready",
			diagnostic: null,
			tools: [
				{
					name: "search",
					description: "search",
					inputSchema: { type: "object" },
				},
			],
		};
		const cases = [
			{ runtimeConfig: validRuntimeConfig, runtime_config: validRuntimeConfig },
			{ runtimeConfig: { ...validRuntimeConfig, config_generation: 2 } },
			{ runtimeConfig: { ...validRuntimeConfig, approval_mode: "default" } },
			{ runtimeConfig: { ...validRuntimeConfig, tool_policy: {} } },
			{ runtimeConfig: { ...validRuntimeConfig, installed_tools: [] } },
			{ mcpManifests: [{ ...validManifest, mcp_server_name: "github" }] },
			{ mcpManifests: [{ ...validManifest, manifest_etag: "etag_1" }] },
			{ mcpManifests: [{ ...validManifest, manifest_generation: 1 }] },
			{
				mcpManifests: [
					{
						...validManifest,
						tools: [
							{ ...validManifest.tools[0], input_schema: { type: "object" } },
						],
					},
				],
			},
			{
				pendingAttachments: [
					{
						origin: {
							transient: {
								attachmentRef: "att_1",
							},
							file_backed: { sourceEventId: "evt_1", fileId: "file_1" },
						},
						mime: "image/png",
						filename: "image.png",
					},
				],
			},
		];
		for (const legacy of cases) {
			const bridge = new TypedBridge();
			bridge.contextJson = JSON.stringify({ ...durableContext(), ...legacy });
			const loader = new BridgeAPIContextLoader(options(bridge));
			await expect(
				loader.loadThreadContext(threadScope()),
			).rejects.toMatchObject({ code: "schema_mismatch" });
		}
	});

	test("reads only target-owned mail identity and content", async () => {
		const bridge = new TypedBridge();
		const loader = new BridgeAPIContextLoader(options(bridge));
		bridge.readMailResponse = {
			found: { deliveryId: "delivery_1", content: "done" },
		};

		await expect(
			loader.readAgentMail(threadScope(), "thrd_source"),
		).resolves.toEqual({
			deliveryId: "delivery_1",
			content: "done",
		});
		expect(bridge.readMailRequests[0]).toEqual({
			scope: expect.objectContaining({ sessionThreadId: "thrd_1" }),
			sourceThreadId: "thrd_source",
		});

		bridge.readMailResponse = {
			found: { deliveryId: "delivery_1", content: "done" },
			empty: {},
		};
		await expect(
			loader.readAgentMail(threadScope(), "thrd_source"),
		).rejects.toMatchObject({ code: "schema_mismatch" });
	});

	test("maps control input results without caller payload echoes", async () => {
		const bridge = new TypedBridge();
		const committer = new BridgeAPIControlInputCommitter(options(bridge));
		bridge.commitInputsResponse = {
			committed: {
				context: { assignedContextSequences: [7], pendingAttachmentJson: [] },
			},
		};

		await expect(
			committer.commitControlInput({
				scope: controlScope("rin_confirm"),
				inputKind: "tool_confirmation",
			}),
		).resolves.toEqual({
			ok: true,
			type: "committed",
			assignedContextSequences: [7],
			pendingAttachments: [],
			interruptToolResults: [],
		});
		expect(bridge.commitInputsRequests[0]).toEqual({
			scope: expect.objectContaining({ sessionThreadId: "thrd_1" }),
			runtimeInputId: "rin_confirm",
			approvalReviewText: [],
		});

		bridge.commitInputsResponse = {
			committed: {
				interrupt: {
					interruptToolResults: [{
						toolUseEventId: "tool_1",
						cancelled: {
							errorJson: JSON.stringify({
								type: "runtime_shutdown",
								message: "internal cancellation detail",
								retryable: false,
							}),
						},
					}],
				},
			},
		};
		await expect(
			committer.commitControlInput({
				scope: controlScope("rin_interrupt"),
				inputKind: "interrupt_control",
			}),
		).resolves.toEqual({
			ok: true,
			type: "committed",
			assignedContextSequences: [],
			pendingAttachments: [],
			interruptToolResults: [
				{ toolUseEventId: "tool_1", result: { type: "cancelled" } },
			],
		});
	});

	test("rejects zero, multiple, and method-mismatched CommitInputs result arms", async () => {
		const bridge = new TypedBridge();
		const committer = new BridgeAPIControlInputCommitter(options(bridge));
		for (const response of [
			{},
			{
				committed: {
					context: { assignedContextSequences: [1], pendingAttachmentJson: [] },
				},
				stale: {},
			},
			{ committed: { interrupt: { interruptToolResults: [] } } },
		]) {
			bridge.commitInputsResponse = response;
			await expect(
				committer.commitControlInput({
					scope: controlScope("rin_bad"),
					inputKind: "tool_confirmation",
				}),
			).resolves.toMatchObject({
				ok: false,
				retryable: false,
				errorCode: "bridge_commit_rejected",
			});
		}
	});

	test("rejects an interrupt application on the accepted-context commit boundary", async () => {
		const bridge = new TypedBridge();
		const loader = new BridgeAPIContextLoader(options(bridge));
		bridge.commitInputsResponse = {
			committed: { interrupt: { interruptToolResults: [] } },
		};
		await expect(
			loader.commitAcceptedInput({
				...controlScope("rin_message"),
				inputOrder: 1,
				kind: "messages",
				contentJson: '{"type":"message"}',
			}),
		).rejects.toMatchObject({ code: "schema_mismatch" });
	});

	test("settles task notification from its Inbox identity and returns only assigned context", async () => {
		const bridge = new TypedBridge();
		const loader = new BridgeAPIContextLoader(options(bridge));
		bridge.taskNotificationResponse = {
			committed: { assignedContextSequences: [11] },
		};

		await expect(
			loader.commitAcceptedInput({
				...controlScope("rin_task"),
				inputOrder: 4,
				kind: "task_notification",
				taskId: "task_1",
				sourceToolUseEventId: "tool_evt_1",
				status: "completed",
				notificationJson: '{"status":"completed"}',
			}),
		).resolves.toEqual({
			type: "committed",
			assignedContextSequences: [11],
			pendingAttachments: [],
			interruptToolResults: [],
		});
		expect(bridge.taskNotificationRequests[0]).toEqual({
			scope: expect.objectContaining({ sessionThreadId: "thrd_1" }),
			runtimeInputId: "rin_task",
		});
	});

	test("lowers request-local Assistant parts to a narrow context delta", async () => {
		const bridge = new TypedBridge();
		bridge.writeEventResponse = {
			committed: {
				eventId: "evt_1",
				eventSequence: 8,
				assignedMessageSequence: 4,
				createdToolUseEventIds: ["tool_evt_1"],
			},
		};
		const writer = new BridgeAPIEventWriter(options(bridge));
		const envelope: SessionEventEnvelope = {
			...eventScope("write_1"),
			modelRequestId: "mrq_1",
			event: {
				type: "agent.message",
				content: [{ type: "text", text: "hello" }],
			},
			assistantContextAppend: {
				parts: [
					{ type: "text", text: "hello", truncated: false },
					{
						type: "reasoning",
						text: "think",
						truncated: false,
						providerMetadata: { trace: "x" },
					},
					{
						type: "tool",
						modelToolCallId: "call_1",
						toolName: "lookup",
						state: {
							status: "running",
							input: {
								value: { city: "Paris" },
								preview: "Paris",
								truncated: false,
							},
						},
					},
				],
			},
		};

		await expect(writer.append(envelope)).resolves.toEqual({
			ok: true,
			type: "committed",
			eventId: "evt_1",
			assistant: { messageSequence: 4, createdToolUseEventIds: ["tool_evt_1"] },
		});
		expect(bridge.writeEventRequests[0]?.assistantContextDelta).toEqual({
			parts: [
				{ text: { text: "hello" } },
				{
					reasoning: {
						text: "think",
						providerMetadataJson: JSON.stringify({ trace: "x" }),
					},
				},
				{
					toolCall: {
						modelToolCallId: "call_1",
						toolName: "lookup",
						canonicalInputJson: JSON.stringify({ city: "Paris" }),
					},
				},
			],
		});
	});

	test("requires exactly one complete WriteEvent result arm", async () => {
		const bridge = new TypedBridge();
		const writer = new BridgeAPIEventWriter(options(bridge));
		const envelope: SessionEventEnvelope = {
			...eventScope("write_bad"),
			event: { type: "session.status_running" },
		};
		for (const response of [
			{},
			{
				committed: {
					eventId: "evt",
					eventSequence: 1,
					createdToolUseEventIds: [],
				},
				stale: {},
			},
			{
				committed: {
					eventId: "",
					eventSequence: 0,
					createdToolUseEventIds: [],
				},
			},
		]) {
			bridge.writeEventResponse = response;
			await expect(writer.append(envelope)).resolves.toMatchObject({
				ok: false,
				error: { code: "schema_mismatch" },
			});
		}
	});

	test("settles Tool results through the payload-free closed result union", async () => {
		const bridge = new TypedBridge();
		const writer = new BridgeAPIEventWriter(options(bridge));
		bridge.toolSettlementResponse = { duplicate: {} };

		await expect(writer.settleToolResult(toolSettlement())).resolves.toEqual({
			ok: true,
			result: { type: "duplicate" },
		});
		expect(bridge.toolSettlementRequests[0]?.settlement).toEqual({
			toolUseEventId: "tool_evt_1",
			error: { errorJson: expect.any(String), serverToolUse: undefined },
		});

		bridge.toolSettlementResponse = { committed: {}, stale: {} };
		await expect(
			writer.settleToolResult(toolSettlement()),
		).resolves.toMatchObject({ ok: false, error: { code: "schema_mismatch" } });
	});

	test("maps every RequestEnd result variant and rejects contradictory arms", async () => {
		const bridge = new TypedBridge();
		const writer = new BridgeAPIEventWriter(options(bridge));
		const cases: Array<{
			response: unknown;
			expected: unknown;
			envelope: SessionEventWriterRequestEndEnvelope;
		}> = [
			{
				response: {
					committed: {
						requestEndEventId: "end_1",
						ordinary: { sealedMessageSequence: 5 },
						interruptToolResults: [],
						pendingAttachmentJson: [],
					},
				},
				expected: {
					ok: true,
					type: "committed",
					requestEndEventId: "end_1",
					outcome: { type: "ordinary", sealedMessageSequence: 5 },
					interruptToolResults: [],
					pendingAttachments: [],
				},
				envelope: requestEndEnvelope(),
			},
			{
				response: {
					duplicate: {
						requestEndEventId: "end_2",
						rescheduled: { effectiveDeadline: "2026-08-15T00:00:00.000Z" },
						interruptToolResults: [],
						pendingAttachmentJson: [],
					},
				},
				expected: {
					ok: true,
					type: "duplicate",
					requestEndEventId: "end_2",
					outcome: {
						type: "rescheduled",
						effectiveDeadline: "2026-08-15T00:00:00.000Z",
					},
					interruptToolResults: [],
					pendingAttachments: [],
				},
				envelope: {
					...requestEndEnvelope(),
					reschedule: {
						attempt: 2,
						deadline: "2026-08-15T00:00:00.000Z",
						backoffMs: 100,
					},
				},
			},
			{
				response: {
					committed: {
						requestEndEventId: "end_3",
						compacted: {
							compactionEventId: "compact_1",
							checkpointMessageSequence: 6,
						},
						interruptToolResults: [],
						pendingAttachmentJson: [],
					},
				},
				expected: {
					ok: true,
					type: "committed",
					requestEndEventId: "end_3",
					outcome: {
						type: "compacted",
						compactionEventId: "compact_1",
						checkpointMessageSequence: 6,
					},
					interruptToolResults: [],
					pendingAttachments: [],
				},
				envelope: {
					...requestEndEnvelope(),
					compactionContext: {
						parts: [{ type: "text" as const, text: "summary" }],
					},
					compactedThroughMessageSequence: 4,
					compactionEventPayloadJson: "{}",
				},
			},
		];
		for (const item of cases) {
			bridge.writeRequestEndResponse = item.response;
			const actual = await writer.writeRequestEnd(item.envelope);
			expect(actual).toEqual(item.expected as unknown as typeof actual);
		}
		bridge.writeRequestEndResponse = {
			committed: {
				requestEndEventId: "end_bad",
				ordinary: {},
				compacted: {
					compactionEventId: "compact",
					checkpointMessageSequence: 1,
				},
				interruptToolResults: [],
				pendingAttachmentJson: [],
			},
		};
		await expect(
			writer.writeRequestEnd(requestEndEnvelope()),
		).resolves.toMatchObject({ ok: false, error: { code: "schema_mismatch" } });
	});

	test("lowers trailing and compaction context without Message snapshots", async () => {
		const bridge = new TypedBridge();
		const writer = new BridgeAPIEventWriter(options(bridge));
		bridge.writeRequestEndResponse = {
			committed: {
				requestEndEventId: "end_1",
				ordinary: { sealedMessageSequence: 4 },
				interruptToolResults: [],
				pendingAttachmentJson: [],
			},
		};
		await writer.writeRequestEnd({
			...requestEndEnvelope(),
			trailingContextAppend: {
				parts: [{ type: "text", text: "tail", truncated: false }],
			},
		});
		expect(bridge.writeRequestEndRequests.at(-1)?.trailingContextDelta).toEqual(
			{ parts: [{ text: { text: "tail" } }] },
		);

		bridge.writeRequestEndResponse = {
			committed: {
				requestEndEventId: "end_2",
				compacted: {
					compactionEventId: "compact_1",
					checkpointMessageSequence: 5,
				},
				interruptToolResults: [],
				pendingAttachmentJson: [],
			},
		};
		await writer.writeRequestEnd({
			...requestEndEnvelope(),
			compactionContext: {
				parts: [
					{
						type: "tool_call",
						modelToolCallId: "call_1",
						toolName: "lookup",
						canonicalInput: { q: "x" },
					},
					{
						type: "tool_result",
						modelToolCallId: "call_1",
						result: { type: "cancelled" },
					},
				],
			},
			compactedThroughMessageSequence: 4,
			compactionEventPayloadJson: "{}",
		});
		expect(bridge.writeRequestEndRequests.at(-1)?.compactionContext).toEqual({
			parts: [
				{
					toolCall: {
						modelToolCallId: "call_1",
						toolName: "lookup",
						canonicalInputJson: JSON.stringify({ q: "x" }),
					},
				},
				{ toolResult: { modelToolCallId: "call_1", cancelled: {} } },
			],
		});
	});

	test("FinishIdle sends only bounded completion text and validates its closed result", async () => {
		const bridge = new TypedBridge();
		const writer = new BridgeAPIEventWriter(options(bridge));
		bridge.finishIdleResponse = { duplicate: { idleEventId: "idle_1" } };

		await expect(writer.finishIdle!(finishIdleEnvelope())).resolves.toEqual({
			ok: true,
			type: "duplicate",
			idleEventId: "idle_1",
		});
		expect(bridge.finishIdleRequests[0]).toEqual({
			scope: expect.any(Object),
			durableTurnId: "turn_1",
			stopReasonJson: JSON.stringify({ type: "end_turn" }),
			completionMailText: "complete",
		});

		bridge.finishIdleResponse = {
			committed: { idleEventId: "idle" },
			stale: {},
		};
		await expect(
			writer.finishIdle!(finishIdleEnvelope()),
		).resolves.toMatchObject({ ok: false, error: { code: "schema_mismatch" } });
	});

	test("internal repair sends one terminal call/result declaration and returns assigned facts only", async () => {
		const bridge = new TypedBridge();
		const committer = new BridgeAPIInternalToolRepairCommitter(options(bridge));
		bridge.repairResponse = {
			committed: { repairEventId: "repair_evt_1", assignedMessageSequence: 9 },
		};

		await expect(
			committer.commitInternalToolRepair(internalRepair()),
		).resolves.toEqual({
			ok: true,
			type: "committed",
			repairEventId: "repair_evt_1",
			assignedMessageSequence: 9,
		});
		expect(bridge.repairRequests[0]).toEqual({
			scope: expect.any(Object),
			modelRequestId: "mrq_1",
			modelToolCallId: "call_bad",
			toolName: "missing_tool",
			canonicalInputJson: JSON.stringify({ x: 1 }),
			error: {
				errorJson: JSON.stringify(internalRepair().error),
				serverToolUse: undefined,
			},
			repairKey: "repair_key_1",
		});

		bridge.repairResponse = {
			committed: { repairEventId: "repair", assignedMessageSequence: 1 },
			duplicate: { repairEventId: "repair", assignedMessageSequence: 1 },
		};
		await expect(
			committer.commitInternalToolRepair(internalRepair()),
		).resolves.toMatchObject({ ok: false, error: { code: "unavailable" } });
	});

	test("runtime termination keeps its independent closed result union", async () => {
		const bridge = new TypedBridge();
		const writer = new BridgeAPIEventWriter(options(bridge));
		bridge.terminationResponse = {
			duplicate: { failureEventId: "failure_1", closeoutEventId: "closeout_1" },
		};
		await expect(
			writer.commitRuntimeTermination!(terminationEnvelope()),
		).resolves.toEqual({
			ok: true,
			type: "duplicate",
			failureEventId: "failure_1",
			closeoutEventId: "closeout_1",
		});
		bridge.terminationResponse = {
			committed: { failureEventId: "failure", closeoutEventId: "closeout" },
			stale: {},
		};
		await expect(
			writer.commitRuntimeTermination!(terminationEnvelope()),
		).resolves.toMatchObject({ ok: false, error: { code: "schema_mismatch" } });
	});
});

class TypedBridge {
	readonly loadContextRequests: LoadContextRequest[] = [];
	readonly readMailRequests: ReadAgentMailRequest[] = [];
	readonly commitInputsRequests: CommitInputsRequest[] = [];
	readonly taskNotificationRequests: CommitTaskNotificationResultRequest[] = [];
	readonly writeEventRequests: WriteEventRequest[] = [];
	readonly toolSettlementRequests: SettleToolResultRequest[] = [];
	readonly writeRequestEndRequests: WriteRequestEndRequest[] = [];
	readonly finishIdleRequests: FinishIdleRequest[] = [];
	readonly repairRequests: CommitInternalToolRepairRequest[] = [];
	readonly terminationRequests: CommitRuntimeTerminationRequest[] = [];
	contextJson = JSON.stringify(durableContext());
	readMailResponse: unknown = { empty: {} };
	commitInputsResponse: unknown = {
		committed: {
			context: { assignedContextSequences: [1], pendingAttachmentJson: [] },
		},
	};
	taskNotificationResponse: unknown = {
		committed: { assignedContextSequences: [1] },
	};
	writeEventResponse: unknown = {
		committed: {
			eventId: "evt_1",
			eventSequence: 1,
			createdToolUseEventIds: [],
		},
	};
	toolSettlementResponse: unknown = { committed: {} };
	writeRequestEndResponse: unknown = {
		committed: {
			requestEndEventId: "end_1",
			ordinary: {},
			interruptToolResults: [],
			pendingAttachmentJson: [],
		},
	};
	finishIdleResponse: unknown = { committed: { idleEventId: "idle_1" } };
	repairResponse: unknown = {
		committed: { repairEventId: "repair_1", assignedMessageSequence: 1 },
	};
	terminationResponse: unknown = {
		committed: { failureEventId: "failure_1", closeoutEventId: "closeout_1" },
	};

	client(): AgentRuntimeBridgeServiceClient {
		return {
			loadContext: (
				request: LoadContextRequest,
				_metadata: Metadata,
				callback: Callback,
			) => {
				this.loadContextRequests.push(request);
				callback(null, {
					contextJson: this.contextJson,
					runtimeBindingToken: "binding_token",
				});
				return grpcCall();
			},
			readAgentMail: (
				request: ReadAgentMailRequest,
				_metadata: Metadata,
				callback: Callback,
			) => {
				this.readMailRequests.push(request);
				callback(null, this.readMailResponse);
				return grpcCall();
			},
			commitInputs: (
				request: CommitInputsRequest,
				_metadata: Metadata,
				_options: CallOptions,
				callback: Callback,
			) => {
				this.commitInputsRequests.push(request);
				callback(null, this.commitInputsResponse);
				return grpcCall();
			},
			commitTaskNotificationResult: (
				request: CommitTaskNotificationResultRequest,
				_metadata: Metadata,
				callback: Callback,
			) => {
				this.taskNotificationRequests.push(request);
				callback(null, this.taskNotificationResponse);
				return grpcCall();
			},
			writeEvent: (
				request: WriteEventRequest,
				_metadata: Metadata,
				callback: Callback,
			) => {
				this.writeEventRequests.push(request);
				callback(null, this.writeEventResponse);
				return grpcCall();
			},
			settleToolResult: (
				request: SettleToolResultRequest,
				_metadata: Metadata,
				_options: CallOptions,
				callback: Callback,
			) => {
				this.toolSettlementRequests.push(request);
				callback(null, this.toolSettlementResponse);
				return grpcCall();
			},
			writeRequestEnd: (
				request: WriteRequestEndRequest,
				_metadata: Metadata,
				callback: Callback,
			) => {
				this.writeRequestEndRequests.push(request);
				callback(null, this.writeRequestEndResponse);
				return grpcCall();
			},
			finishIdle: (
				request: FinishIdleRequest,
				_metadata: Metadata,
				callback: Callback,
			) => {
				this.finishIdleRequests.push(request);
				callback(null, this.finishIdleResponse);
				return grpcCall();
			},
			commitInternalToolRepair: (
				request: CommitInternalToolRepairRequest,
				_metadata: Metadata,
				callback: Callback,
			) => {
				this.repairRequests.push(request);
				callback(null, this.repairResponse);
				return grpcCall();
			},
			commitRuntimeTermination: (
				request: CommitRuntimeTerminationRequest,
				_metadata: Metadata,
				callback: Callback,
			) => {
				this.terminationRequests.push(request);
				callback(null, this.terminationResponse);
				return grpcCall();
			},
		} as unknown as AgentRuntimeBridgeServiceClient;
	}
}

type Callback = (error: Error | null, response: unknown) => void;

function options(bridge: TypedBridge, logger?: RuntimePodLogger) {
	return {
		address: "bridge.test:9090",
		tokenPath: "/var/run/token",
		client: bridge.client(),
		metadataFactory: async () => new Metadata(),
		sleep: async () => undefined,
		...(logger === undefined ? {} : { logger }),
	};
}

function threadScope() {
	return {
		workspaceId: "wksp_1",
		sessionId: "sesn_1",
		sessionThreadId: "thrd_1",
		bindingId: "bind_1",
		bindingGeneration: 3,
		targetPodUid: "pod_1",
	};
}

function controlScope(runtimeInputId: string) {
	return { ...threadScope(), runtimeInputId };
}

function eventScope(writeId: string) {
	return { ...threadScope(), writeId };
}

function durableContext() {
	return {
		contextEntries: [
			{
				messageSequence: 1,
				contextKind: "user",
				parts: [{ type: "text", text: "hello" }],
			},
			{
				messageSequence: 2,
				contextKind: "assistant",
				parts: [
					{
						type: "tool_call",
						modelToolCallId: "call_1",
						toolName: "lookup",
						canonicalInput: { city: "Paris" },
					},
					{
						type: "tool_result",
						modelToolCallId: "call_1",
						result: {
							type: "completed",
							output: { text: "sunny" },
						},
					},
				],
			},
		],
		openRequestDraft: {
			modelRequestId: "mrq_open",
			messageSequence: 3,
			parts: [{ type: "text", text: "draft" }],
		},
		turnFacts: { events: [], internalRepairs: [] },
		thread: {
			parentThreadId: null,
			role: "main",
			visibility: "public",
			taskName: null,
			agentType: "general",
			status: "idle",
		},
	};
}

function requestEndEnvelope(): SessionEventWriterRequestEndEnvelope {
	return {
		...eventScope("end_write_1"),
		modelRequestId: "mrq_1",
		isError: false,
		finishReason: "stop",
	};
}

function finishIdleEnvelope(): SessionEventWriterFinishIdleEnvelope {
	return {
		...threadScope(),
		durableTurnId: "turn_1",
		stopReason: { type: "end_turn" },
		completionMailText: "complete",
	};
}

function toolSettlement(): SessionEventWriterToolSettlementEnvelope {
	return {
		...threadScope(),
		settlement: {
			toolUseEventId: "tool_evt_1",
			outcome: {
				type: "error",
				error: {
					type: "runtime",
					code: "runtime_invalid_sequence",
					message: "failed",
					retryable: false,
					fatal: true,
				},
			},
		},
	};
}

function internalRepair(): RuntimeInternalToolRepairCommit {
	return {
		...threadScope(),
		modelRequestId: "mrq_1",
		modelToolCallId: "call_bad",
		toolName: "missing_tool",
		repairKey: "repair_key_1",
		canonicalInput: { x: 1 },
		error: { type: "invalid_tool", message: "unknown tool", retryable: false },
	};
}

function terminationEnvelope(): SessionEventWriterRuntimeTerminationEnvelope {
	return {
		...eventScope("termination_write_1"),
		failure: {
			type: "runtime",
			code: "runtime_invalid_sequence",
			message: "runtime failed",
			retryable: false,
			fatal: true,
		},
	};
}

function grpcCall() {
	return { cancel: () => undefined };
}
