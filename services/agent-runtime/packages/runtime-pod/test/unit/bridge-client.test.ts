// Layer 1: the Runtime's Bridge adapters against an in-process stand-in for
// the Bridge client.
import { writtenLogs } from "../fixtures/written-logs.js";
import { describe, expect, test } from "bun:test";
import type {
	CallOptions,
	sendUnaryData,
	ServerUnaryCall,
	ServiceDefinition,
	UntypedServiceImplementation,
} from "@grpc/grpc-js";
import { Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
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
	CloseApprovalReviewerRequest,
	FinishIdleRequest,
	LoadContextRequest,
	ReadAgentMailRequest,
	SettleToolResultRequest,
	WriteEventRequest,
	WriteRequestEndRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
	AgentRuntimeBridgeServiceService,
	WriteEventRequest as WriteEventRequestMessage,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { MaxGrpcInboundMessageBytes } from "../../src/bounds.js";
import {
	BridgeAPIApprovalReviewerThreadCreator,
	BridgeAPIContextLoader,
	BridgeAPIControlInputCommitter,
	BridgeAPIEventWriter,
	BridgeAPIInternalToolRepairCommitter,
	BridgeAPITaskNotificationCommitter,
} from "../../src/bridge-client.js";
import type {
	ApprovalReviewerThreadClose,
	ApprovalReviewerThreadCreation,
} from "../../src/approval-reviewer.js";
import type {
	RuntimePodLogger,
	RuntimePodLogRecord,
} from "../../src/logger.js";

describe("Bridge operation-specific Runtime adapters", () => {
	// Each adapter opens its own channel to a stand-in Bridge on loopback. A
	// response above the control channel's fuse reaches only the two adapters
	// that carry durable context; the others end as if Bridge had not answered.
	// The event writer also sends an event above that fuse.
	test("only the context loader and the event writer carry a message above the control fuse", async () => {
		const largeText = "x".repeat(MaxGrpcInboundMessageBytes + 1);
		const largeEventId = "evt_00000000000000000000000000000001";
		const outcomes = async (paddingBytes: number) => {
			const bridge = new TypedBridge();
			bridge.commitInputsResponse = {
				committed: { context: { assignedContextSequences: [7], pendingAttachmentJson: [] } },
			};
			bridge.approvalEnsureResponse = { stale: {} };
			bridge.repairResponse = { committed: { repairEventId: "repair_evt_1", assignedMessageSequence: 9 } };
			const served = await serveOverLoopback(bridge, paddingBytes);
			const own = {
				address: served.address,
				tokenPath: "/var/run/token",
				metadataFactory: async () => new Metadata(),
				sleep: async () => undefined,
			};
			const controlInputs = new BridgeAPIControlInputCommitter(own);
			const taskNotifications = new BridgeAPITaskNotificationCommitter(own);
			const reviewers = new BridgeAPIApprovalReviewerThreadCreator(own);
			const loader = new BridgeAPIContextLoader(own);
			const writer = new BridgeAPIEventWriter(own);
			const repairs = new BridgeAPIInternalToolRepairCommitter(own);
			const settled = (call: Promise<unknown>) =>
				call.then(
					(value) => JSON.stringify({ value }),
					(error: unknown) => JSON.stringify({ error }),
				);
			try {
				return {
					controlInputs: await settled(
						controlInputs.commitControlInput({ scope: controlScope("rin_confirm"), inputKind: "tool_confirmation" }),
					),
					taskNotifications: await settled(
						taskNotifications.commitTaskNotification({
							scope: threadScope() as Parameters<BridgeAPITaskNotificationCommitter["commitTaskNotification"]>[0]["scope"],
							runtimeInputId: "rin_notification",
						}),
					),
					reviewers: await settled(
						reviewers.createApprovalReviewerThread({
							request: threadScope() as ApprovalReviewerThreadCreation["request"],
							reviewId: "arvw_channel",
							isTrunk: true,
							ensureOperationId: "aprv_ensure_channel",
						}),
					),
					loader: await settled(loader.loadThreadContext(threadScope())),
					writer: await settled(
						writer.append({ ...eventScope("write_status"), event: { type: "session.status_running" } }),
					),
					repairs: await settled(repairs.commitInternalToolRepair(internalRepair())),
					largeEvent: await settled(
						(async () => {
							bridge.writeEventResponse = {
								committed: { eventId: largeEventId, eventSequence: 8, assignedMessageSequence: 4, createdToolUseEventIds: [] },
							};
							return writer.append({
								...eventScope("write_large"),
								modelRequestId: "mrq_1",
								preallocatedEventId: largeEventId,
								event: { type: "agent.message", content: [{ type: "text", text: largeText }] },
								assistantContextAppend: { parts: [{ type: "text", text: largeText, truncated: false }] },
							});
						})(),
					),
					largestEventBytes: Math.max(
						...bridge.writeEventRequests.map((request) => WriteEventRequestMessage.encode(request).finish().length),
					),
				};
			} finally {
				await Promise.all([controlInputs, taskNotifications, reviewers, loader, writer, repairs].map((adapter) => adapter.close()));
				await served.close();
			}
		};
		const plain = await outcomes(0);
		const large = await outcomes(MaxGrpcInboundMessageBytes + 1);
		expect(JSON.parse(plain.controlInputs)).toMatchObject({ value: { ok: true } });
		expect(JSON.parse(plain.taskNotifications)).toMatchObject({ value: { ok: true } });
		expect(JSON.parse(plain.reviewers)).toMatchObject({ value: { staleCustody: true } });
		expect(JSON.parse(plain.repairs)).toMatchObject({ value: { ok: true } });
		for (const control of ["controlInputs", "taskNotifications", "reviewers", "repairs"] as const) {
			expect(JSON.parse(large[control]), control).toMatchObject({ value: { ok: false } });
			expect(large[control], control).not.toBe(plain[control]);
		}
		expect(JSON.parse(plain.loader)).toMatchObject({ value: { runtimeBindingToken: "binding_token" } });
		expect(JSON.parse(plain.writer)).toMatchObject({ value: { ok: true } });
		expect({ loader: large.loader, writer: large.writer }).toEqual({ loader: plain.loader, writer: plain.writer });
		expect(JSON.parse(plain.largeEvent)).toMatchObject({ value: { ok: true } });
		expect(plain.largestEventBytes).toBeGreaterThan(MaxGrpcInboundMessageBytes);
	}, 30_000);

	test("maps typed Reviewer ensure stale before attempting input admission", async () => {
		for (const isTrunk of [true, false]) {
			const bridge = new TypedBridge();
			bridge.approvalEnsureResponse = { stale: {} };
			const creator = new BridgeAPIApprovalReviewerThreadCreator(options(bridge));

			await expect(
				creator.createApprovalReviewerThread({
					request: threadScope() as ApprovalReviewerThreadCreation["request"],
					reviewId: `arvw_stale_ensure_${isTrunk ? "trunk" : "sidecar"}`,
					isTrunk,
					...(isTrunk
						? { ensureOperationId: "aprv_ensure_stale" }
						: {}),
				}),
			).resolves.toEqual({
				ok: false,
				message: "approval reviewer thread creation is stale",
				staleCustody: true,
			});
			expect(bridge.approvalAdmissionRequests).toEqual([]);
		}
	});

	test("maps typed Reviewer admission stale without classifying it as malformed", async () => {
		const bridge = new TypedBridge();
		bridge.approvalAdmissionResponse = { stale: {} };
		const creator = new BridgeAPIApprovalReviewerThreadCreator(options(bridge));

		await expect(
			creator.createApprovalReviewerThread({
				request: threadScope() as ApprovalReviewerThreadCreation["request"],
				reviewId: "arvw_stale_admission",
				isTrunk: true,
				ensureOperationId: "aprv_ensure_stale_admission",
			}),
		).resolves.toEqual({
			ok: false,
			message: "approval reviewer input admission is stale",
			staleCustody: true,
		});
	});

	test("replays an ambiguous Reviewer admission with the exact request", async () => {
		const bridge = new TypedBridge();
		bridge.approvalAdmissionFailures.push(
			Object.assign(new Error("admission ACK lost"), { code: status.UNKNOWN }),
		);
		bridge.approvalAdmissionResponse = {
			duplicate: { runtimeInputId: "rin_reviewer" },
		};
		const creator = new BridgeAPIApprovalReviewerThreadCreator(options(bridge));

		await expect(
			creator.createApprovalReviewerThread({
				request: threadScope() as ApprovalReviewerThreadCreation["request"],
				reviewId: "arvw_lost_admission_ack",
				isTrunk: true,
				ensureOperationId: "aprv_ensure_lost_admission_ack",
			}),
		).resolves.toEqual({
			ok: true,
			reviewerThreadId: "thrd_reviewer",
			runtimeInputId: "rin_reviewer",
		});
		expect(bridge.approvalAdmissionRequests).toHaveLength(2);
		expect(bridge.approvalAdmissionRequests[1]).toEqual(
			bridge.approvalAdmissionRequests[0],
		);
	});

	test("does not replay a definitive Reviewer admission rejection", async () => {
		const bridge = new TypedBridge();
		bridge.approvalAdmissionFailures.push(
			Object.assign(new Error("invalid admission"), {
				code: status.INVALID_ARGUMENT,
			}),
		);
		const creator = new BridgeAPIApprovalReviewerThreadCreator(options(bridge));

		await expect(
			creator.createApprovalReviewerThread({
				request: threadScope() as ApprovalReviewerThreadCreation["request"],
				reviewId: "arvw_invalid_admission",
				isTrunk: true,
				ensureOperationId: "aprv_ensure_invalid_admission",
			}),
		).resolves.toEqual({
			ok: false,
			message: "approval reviewer input admission is unavailable",
		});
		expect(bridge.approvalAdmissionRequests).toHaveLength(1);
	});

	test("replays an ambiguous Reviewer close with the exact settlement authority", async () => {
		const bridge = new TypedBridge();
		bridge.approvalCloseFailures.push(
			Object.assign(new Error("close ACK lost"), { code: status.UNKNOWN }),
		);
		bridge.approvalCloseResponse = { duplicate: {} };
		const creator = new BridgeAPIApprovalReviewerThreadCreator(options(bridge));
		const close: ApprovalReviewerThreadClose = {
			request: threadScope() as ApprovalReviewerThreadClose["request"],
			reviewId: "arvw_lost_close_ack",
			isTrunk: false,
			reviewerThreadId: "thrd_reviewer",
			settlement: { type: "failure", eventId: "evt_reviewer_failure" },
		};

		await expect(creator.closeApprovalReviewerThread(close)).resolves.toEqual({
			ok: true,
		});
		expect(bridge.approvalCloseRequests).toHaveLength(2);
		expect(bridge.approvalCloseRequests[1]).toEqual(
			bridge.approvalCloseRequests[0],
		);
	});

	test("does not replay a definitive Reviewer close rejection", async () => {
		const bridge = new TypedBridge();
		bridge.approvalCloseFailures.push(
			Object.assign(new Error("invalid close"), {
				code: status.INVALID_ARGUMENT,
			}),
		);
		const creator = new BridgeAPIApprovalReviewerThreadCreator(options(bridge));

		await expect(
			creator.closeApprovalReviewerThread({
				request: threadScope() as ApprovalReviewerThreadClose["request"],
				reviewId: "arvw_invalid_close",
				isTrunk: false,
				reviewerThreadId: "thrd_reviewer",
				settlement: { type: "decision", eventId: "evt_reviewer_decision" },
			}),
		).resolves.toEqual({
			ok: false,
			message: "approval reviewer thread close is unavailable",
		});
		expect(bridge.approvalCloseRequests).toHaveLength(1);
	});

	test("loads unified committed messages and current request identity as direct durable facts", async () => {
		const bridge = new TypedBridge();
		const loader = new BridgeAPIContextLoader(options(bridge));

		const loaded = await loader.loadThreadContext(threadScope());

		expect(bridge.loadContextRequests).toEqual([
			{
				scope: expect.objectContaining({ sessionThreadId: "thrd_1" }),
				recoveryLeaseRef: undefined,
				sourceEventId: "",
				handoffId: "",
			},
		]);
		expect(loaded.messages).toEqual([
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
			{
				messageSequence: 3,
				contextKind: "assistant",
				parts: [{ type: "text", text: "draft" }],
			},
		]);
		expect(loaded.currentRequestMessage).toEqual({
			modelRequestId: "mrq_open",
			assistantMessageSequence: 3,
		});
	});

	test.each([
		{ answer: "UNAVAILABLE", code: status.UNAVAILABLE, loaderCode: "unavailable" },
		{ answer: "DEADLINE_EXCEEDED", code: status.DEADLINE_EXCEEDED, loaderCode: "timeout" },
	])(
		"a context load that ends in $answer is a retryable loader error",
		async ({ code, loaderCode }) => {
			const bridge = new TypedBridge();
			bridge.loadContextFailures.push(
				Object.assign(new Error("no answer"), { code }),
			);
			const loader = new BridgeAPIContextLoader(options(bridge));

			await expect(loader.loadThreadContext(threadScope())).rejects.toEqual({
				type: "context-loader",
				code: loaderCode,
				message: "Context loader operation failed.",
				retryable: true,
				fatal: false,
				sessionId: threadScope().sessionId,
				reason: "Bridge did not answer the context load",
			});
		},
	);

	test("a context load that Bridge refused is not a retryable loader error", async () => {
		const bridge = new TypedBridge();
		bridge.loadContextFailures.push(
			Object.assign(new Error("recovery authority is stale"), {
				code: status.FAILED_PRECONDITION,
			}),
		);
		const loader = new BridgeAPIContextLoader(options(bridge));

		await expect(
			loader.loadThreadContext(threadScope()),
		).rejects.not.toMatchObject({ retryable: true });
	});

	test("rejects malformed direct durable context without a compatibility parser", async () => {
		const bridge = new TypedBridge();
		bridge.contextJson = JSON.stringify({
			...durableContext(),
			messages: [{ id: "legacy_message" }],
		});
		const loader = new BridgeAPIContextLoader(options(bridge));

		await expect(loader.loadThreadContext(threadScope())).rejects.toMatchObject(
			{ code: "schema_mismatch" },
		);
	});

	test("rejects retired split content fields and requires an explicit current reference", async () => {
		for (const invalid of [
			{ ...durableContext(), contextEntries: [] },
			{ ...durableContext(), openRequestDraft: null },
			{ ...durableContext(), currentRequestMessage: undefined },
		]) {
			const bridge = new TypedBridge();
			bridge.contextJson = JSON.stringify(invalid);
			const loader = new BridgeAPIContextLoader(options(bridge));
			await expect(loader.loadThreadContext(threadScope())).rejects.toMatchObject({ code: "schema_mismatch" });
		}
		const bridge = new TypedBridge();
		bridge.contextJson = JSON.stringify({ ...durableContext(), currentRequestMessage: null });
		const loader = new BridgeAPIContextLoader(options(bridge));
		expect((await loader.loadThreadContext(threadScope())).currentRequestMessage).toBeNull();
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
						messages: [{ canary }],
					}),
			},
			{
				phase: "current_request_message_parse",
				reason: "invalid_current_request_message_shape",
				context: () =>
					JSON.stringify({ ...durableContext(), currentRequestMessage: canary }),
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
			const records: Record<string, unknown>[] = [];
			bridge.contextJson = testCase.context();
			const loader = new BridgeAPIContextLoader(options(bridge, writtenLogs(records)));
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
				"error.code": "operation_failed",
				"error.message": `the Bridge's context projection was rejected at ${testCase.phase} (${testCase.reason})`,
			});
			expect(Object.keys(records[0]!).sort()).toEqual(
				[
					"component",
					"deployment.environment",
					"error.class",
					"error.code",
					"error.message",
					"error.message_safe",
					"event",
					"event.kind",
					"level",
					"log.type",
					"message",
					"msg",
					"operation",
					"phase",
					"process.pid",
					"reason",
					"service.instance.id",
					"service.name",
					"service.version",
					"session.id",
					"thread.id",
					"time",
					"workspace.id",
				].sort(),
			);
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
			interruptLeaseRef: undefined,
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

	test("retries an outcome-unknown interrupt with the same durable identity", async () => {
		const bridge = new TypedBridge();
		const committer = new BridgeAPIControlInputCommitter(options(bridge));
		bridge.commitInputsFailures.push(
			Object.assign(new Error("response lost"), { code: status.UNKNOWN }),
		);
		bridge.commitInputsResponse = {
			committed: { interrupt: { interruptToolResults: [] } },
		};
		await expect(
			committer.commitControlInput({
				scope: controlScope("rin_interrupt_lost_response"),
				inputKind: "interrupt_control",
			}),
		).resolves.toMatchObject({ ok: true, type: "committed" });
		expect(bridge.commitInputsRequests).toHaveLength(2);
		expect(bridge.commitInputsRequests[0]).toEqual(
			bridge.commitInputsRequests[1],
		);
	});

	test("keeps barrier-stale distinct from ordinary stale custody", async () => {
		const bridge = new TypedBridge();
		const committer = new BridgeAPIControlInputCommitter(options(bridge));
		bridge.commitInputsResponse = { stale: {} };
		await expect(
			committer.commitControlInput({
				scope: controlScope("rin_stale_control"),
				inputKind: "tool_confirmation",
			}),
		).resolves.toEqual({ ok: true, type: "stale" });

		const loader = new BridgeAPIContextLoader(options(bridge));
		await expect(
			loader.commitAcceptedInput({
				...controlScope("rin_stale_message"),
				inputOrder: 1,
				kind: "messages",
				contentJson: '{"messages":[]}',
			}),
		).resolves.toEqual({ type: "stale_custody" });

		bridge.taskNotificationResponse = { stale: {} };
		await expect(
			loader.commitAcceptedInput({
				...controlScope("rin_stale_task"),
				inputOrder: 2,
				kind: "task_notification",
				taskId: "task_stale",
				sourceToolUseEventId: "evt_tool_stale",
				status: "completed",
				notificationJson: '{"status":"completed"}',
			}),
		).resolves.toEqual({ type: "stale_custody" });

		bridge.commitInputsResponse = { barrierStale: {} };
		await expect(
			committer.commitControlInput({
				scope: controlScope("rin_barrier_control"),
				inputKind: "tool_confirmation",
			}),
		).resolves.toEqual({ ok: true, type: "barrier_stale" });

		await expect(
			loader.commitAcceptedInput({
				...controlScope("rin_barrier_message"),
				inputOrder: 3,
				kind: "messages",
				contentJson: '{"messages":[]}',
			}),
		).resolves.toEqual({ type: "barrier_stale_custody" });

		bridge.taskNotificationResponse = { barrierStale: {} };
		await expect(
			loader.commitAcceptedInput({
				...controlScope("rin_barrier_task"),
				inputOrder: 4,
				kind: "task_notification",
				taskId: "task_barrier",
				sourceToolUseEventId: "evt_tool_barrier",
				status: "completed",
				notificationJson: '{"status":"completed"}',
			}),
		).resolves.toEqual({ type: "barrier_stale_custody" });
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
				eventId: "evt_00000000000000000000000000000001",
				eventSequence: 8,
				assignedMessageSequence: 4,
				createdToolUseEventIds: ["tool_evt_1"],
			},
		};
		const writer = new BridgeAPIEventWriter(options(bridge));
		const text = "x".repeat(1024 * 1024);
		const envelope: SessionEventEnvelope = {
			...eventScope("write_1"),
			modelRequestId: "mrq_1",
			preallocatedEventId: "evt_00000000000000000000000000000001",
			event: {
				type: "agent.message",
				content: [{ type: "text", text }],
			},
			assistantContextAppend: {
				parts: [
					{ type: "text", text, truncated: false },
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

		const before = Date.now();
		await expect(writer.append(envelope)).resolves.toEqual({
			ok: true,
			type: "committed",
			eventId: "evt_00000000000000000000000000000001",
			assistant: { messageSequence: 4, createdToolUseEventIds: ["evt_00000000000000000000000000000001"] },
		});
		const after = Date.now();
		// The attempt deadline grows 2 s per encoded MiB over the 3 s base.
		const encodedBytes = WriteEventRequestMessage.encode(
			bridge.writeEventRequests[0]!,
		).finish().byteLength;
		const scaledMs = 3000 + Math.ceil((2000 * encodedBytes) / (1024 * 1024));
		expect(encodedBytes).toBeGreaterThan(2 * 1024 * 1024);
		expect(bridge.writeEventDeadlines[0]).toBeGreaterThanOrEqual(before + scaledMs);
		expect(bridge.writeEventDeadlines[0]).toBeLessThanOrEqual(after + scaledMs);
		expect(bridge.writeEventRequests[0]?.assistantContextDelta).toEqual({
			parts: [
				{ text: { text } },
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
						providerInputJson: JSON.stringify({ city: "Paris" }),
					},
				},
			],
		});
		expect(bridge.writeEventRequests[0]?.preallocatedEventId).toBe(envelope.preallocatedEventId);

		bridge.writeEventResponse = {
			committed: { eventId: "evt_status", eventSequence: 9, createdToolUseEventIds: [] },
		};
		const statusBefore = Date.now();
		await expect(
			writer.append({ ...eventScope("write_status"), event: { type: "session.status_running" } }),
		).resolves.toMatchObject({ ok: true, eventId: "evt_status" });
		const statusAfter = Date.now();
		// A small event keeps the base; its few hundred bytes round up to one millisecond.
		expect(bridge.writeEventDeadlines[1]).toBeGreaterThanOrEqual(statusBefore + 3000);
		expect(bridge.writeEventDeadlines[1]).toBeLessThanOrEqual(statusAfter + 3001);
	});

	test("accepts only matching preallocated text and thinking receipt identities", async () => {
		const eventId = "evt_00000000000000000000000000000001";
		for (const eventType of ["agent.message", "agent.thinking"] as const) {
			for (const outcome of ["committed", "duplicate"] as const) {
				const bridge = new TypedBridge();
				const writer = new BridgeAPIEventWriter(options(bridge));
				const envelope: SessionEventEnvelope = {
					...eventScope(`identity_${eventType}_${outcome}`),
					modelRequestId: "mrq_1", preallocatedEventId: eventId,
					event: eventType === "agent.message"
						? { type: eventType, content: [{ type: "text", text: "alpha" }] }
						: { type: eventType },
					...(eventType === "agent.message" ? {
						assistantContextAppend: { parts: [{ type: "text" as const, text: "alpha", truncated: false }] },
					} : {}),
				};
				const receipt = { eventId, eventSequence: 8, createdToolUseEventIds: [],
					...(eventType === "agent.message" ? { assignedMessageSequence: 4 } : {}),
				};
				bridge.writeEventResponse = { [outcome]: receipt };
				await expect(writer.append(envelope)).resolves.toMatchObject({ ok: true, type: outcome, eventId });
				bridge.writeEventResponse = { [outcome]: { ...receipt, eventId: "evt_00000000000000000000000000000002" } };
				await expect(writer.append(envelope)).resolves.toMatchObject({ ok: false, error: { code: "schema_mismatch" } });
				expect(bridge.writeEventRequests.at(-1)?.preallocatedEventId).toBe(eventId);
			}
		}
	});

	test("declares provider and canonical execution Tool inputs separately", async () => {
		const bridge = new TypedBridge();
		bridge.writeEventResponse = {
			committed: {
				eventId: "evt_patch",
				assignedMessageSequence: 4,
				createdToolUseEventIds: ["tool_evt_patch"],
			},
		};
		const writer = new BridgeAPIEventWriter(options(bridge));
		const rawPatch =
			"*** Begin Patch\n*** Add File: note.txt\n+hello\n*** End Patch\n";
		await expect(
			writer.append({
				...eventScope("write_patch"),
				modelRequestId: "mrq_patch",
				event: {
					type: "agent.tool_use",
					name: "apply_patch",
					input: { patch: rawPatch },
					evaluated_permission: "allow",
				},
				assistantContextAppend: {
					parts: [
						{
							type: "tool",
							modelToolCallId: "call_patch",
							toolName: "apply_patch",
							state: {
								status: "running",
								input: {
									value: rawPatch,
									preview: rawPatch,
									truncated: false,
								},
							},
						},
					],
				},
				distinctProviderInput: rawPatch,
				toolRouteCapability: "sandbox_execute",
			}),
		).resolves.toMatchObject({ ok: true, eventId: "evt_patch" });
		expect(bridge.writeEventRequests[0]).toMatchObject({
			eventType: "",
			payloadJson: "",
			assistantContextDelta: undefined,
			toolDeclaration: {
				modelToolCallId: "call_patch",
				toolName: "apply_patch",
				publicExecutionInputJson: JSON.stringify({ patch: rawPatch }),
				distinctProviderInputJson: JSON.stringify(rawPatch),
				evaluatedPermission: "allow",
				routeCapability: "sandbox_execute",
				leadingReasoning: [],
			},
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

	test("retries a settlement whose acknowledgement was lost and accepts the duplicate", async () => {
		const bridge = new TypedBridge();
		const writer = new BridgeAPIEventWriter(options(bridge));
		bridge.toolSettlementFailures.push(
			Object.assign(new Error("acknowledgement lost"), { code: status.UNAVAILABLE }),
		);
		bridge.toolSettlementResponse = { duplicate: {} };
		await expect(writer.settleToolResult(toolSettlement())).resolves.toEqual({
			ok: true,
			result: { type: "duplicate" },
		});
		expect(bridge.toolSettlementRequests).toHaveLength(2);
		expect(bridge.toolSettlementRequests[0]).toEqual(bridge.toolSettlementRequests[1]);
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
					},
				},
				expected: {
					ok: true,
					type: "committed",
					requestEndEventId: "end_1",
					outcome: { type: "ordinary", sealedMessageSequence: 5 },
					interruptToolResults: [],
				},
				envelope: requestEndEnvelope(),
			},
			{
				response: {
					duplicate: {
						requestEndEventId: "end_2",
						rescheduled: { effectiveDeadline: "2026-08-15T00:00:00.000Z" },
						interruptToolResults: [],
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
						providerInputJson: JSON.stringify({ q: "x" }),
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
			reasoningPrefixContextDelta: undefined,
		});

		bridge.repairResponse = {
			committed: { repairEventId: "repair", assignedMessageSequence: 1 },
			duplicate: { repairEventId: "repair", assignedMessageSequence: 1 },
		};
		await expect(
			committer.commitInternalToolRepair(internalRepair()),
		).resolves.toMatchObject({ ok: false, error: { code: "unavailable" } });
	});

	test("internal repair preserves ordered signed reasoning before its terminal call and result", async () => {
		const bridge = new TypedBridge();
		const committer = new BridgeAPIInternalToolRepairCommitter(options(bridge));
		bridge.repairResponse = {
			committed: { repairEventId: "repair_evt_1", assignedMessageSequence: 9 },
		};
		await expect(committer.commitInternalToolRepair({
			...internalRepair(),
			reasoningPrefixContextDelta: { parts: [
				{ type: "reasoning", text: "first", truncated: false, providerMetadata: { anthropic: { signature: "signed-first" } } },
				{ type: "reasoning", text: "second", truncated: false },
			] },
		})).resolves.toMatchObject({ ok: true, type: "committed", assignedMessageSequence: 9 });
		expect(bridge.repairRequests[0]?.reasoningPrefixContextDelta).toEqual({ parts: [
			{ reasoning: { text: "first", providerMetadataJson: '{"anthropic":{"signature":"signed-first"}}' } },
			{ reasoning: { text: "second", providerMetadataJson: undefined } },
		] });
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
	readonly approvalAdmissionRequests: unknown[] = [];
	readonly approvalAdmissionFailures: Array<Error & { readonly code?: number }> =
		[];
	readonly approvalCloseRequests: CloseApprovalReviewerRequest[] = [];
	readonly approvalCloseFailures: Array<Error & { readonly code?: number }> = [];
	readonly loadContextRequests: LoadContextRequest[] = [];
	readonly loadContextFailures: Array<Error & { readonly code?: number }> = [];
	readonly readMailRequests: ReadAgentMailRequest[] = [];
	readonly commitInputsRequests: CommitInputsRequest[] = [];
	readonly commitInputsFailures: Array<Error & { readonly code?: number }> = [];
	readonly taskNotificationRequests: CommitTaskNotificationResultRequest[] = [];
	readonly writeEventRequests: WriteEventRequest[] = [];
	readonly writeEventDeadlines: Array<CallOptions["deadline"]> = [];
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
	readonly toolSettlementFailures: Array<Error & { readonly code?: number }> = [];
	writeRequestEndResponse: unknown = {
		committed: {
			requestEndEventId: "end_1",
			ordinary: {},
			interruptToolResults: [],
		},
	};
	finishIdleResponse: unknown = { committed: { idleEventId: "idle_1" } };
	repairResponse: unknown = {
		committed: { repairEventId: "repair_1", assignedMessageSequence: 1 },
	};
	terminationResponse: unknown = {
		committed: { failureEventId: "failure_1", closeoutEventId: "closeout_1" },
	};
	approvalEnsureResponse: unknown = {
		committed: { reviewerThreadId: "thrd_reviewer" },
	};
	approvalAdmissionResponse: unknown = {
		committed: { runtimeInputId: "rin_reviewer" },
	};
	approvalCloseResponse: unknown = { committed: {} };

	client(): AgentRuntimeBridgeServiceClient {
		return {
			ensureApprovalReviewerTrunk: (
				_request: unknown,
				_metadata: Metadata,
				_options: CallOptions,
				callback: Callback,
			) => {
				callback(null, this.approvalEnsureResponse);
				return grpcCall();
			},
			ensureApprovalReviewerSidecar: (
				_request: unknown,
				_metadata: Metadata,
				_options: CallOptions,
				callback: Callback,
			) => {
				callback(null, this.approvalEnsureResponse);
				return grpcCall();
			},
				admitApprovalReviewInput: (
				request: unknown,
				_metadata: Metadata,
				_options: CallOptions,
				callback: Callback,
			) => {
				this.approvalAdmissionRequests.push(request);
				const failure = this.approvalAdmissionFailures.shift();
				if (failure !== undefined) {
					callback(failure, undefined);
					return grpcCall();
				}
					callback(null, this.approvalAdmissionResponse);
					return grpcCall();
				},
				closeApprovalReviewer: (
					request: CloseApprovalReviewerRequest,
					_metadata: Metadata,
					_options: CallOptions,
					callback: Callback,
				) => {
					this.approvalCloseRequests.push(request);
					const failure = this.approvalCloseFailures.shift();
					if (failure !== undefined) {
						callback(failure, undefined);
						return grpcCall();
					}
					callback(null, this.approvalCloseResponse);
					return grpcCall();
				},
			loadContext: (
				request: LoadContextRequest,
				_metadata: Metadata,
				_options: CallOptions,
				callback: Callback,
			) => {
				this.loadContextRequests.push(request);
				const failure = this.loadContextFailures.shift();
				if (failure !== undefined) {
					callback(failure, undefined);
					return grpcCall();
				}
				callback(null, {
					contextJson: this.contextJson,
					runtimeBindingToken: "binding_token",
				});
				return grpcCall();
			},
			readAgentMail: (
				request: ReadAgentMailRequest,
				_metadata: Metadata,
				_options: CallOptions,
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
				const failure = this.commitInputsFailures.shift();
				if (failure !== undefined) {
					callback(failure, undefined);
					return grpcCall();
				}
				callback(null, this.commitInputsResponse);
				return grpcCall();
			},
			commitTaskNotificationResult: (
				request: CommitTaskNotificationResultRequest,
				_metadata: Metadata,
				_options: CallOptions,
				callback: Callback,
			) => {
				this.taskNotificationRequests.push(request);
				callback(null, this.taskNotificationResponse);
				return grpcCall();
			},
			writeEvent: (
				request: WriteEventRequest,
				_metadata: Metadata,
				options: CallOptions,
				callback: Callback,
			) => {
				this.writeEventRequests.push(request);
				this.writeEventDeadlines.push(options.deadline);
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
				const failure = this.toolSettlementFailures.shift();
				if (failure !== undefined) {
					callback(failure, undefined);
					return grpcCall();
				}
				callback(null, this.toolSettlementResponse);
				return grpcCall();
			},
			writeRequestEnd: (
				request: WriteRequestEndRequest,
				_metadata: Metadata,
				_options: CallOptions,
				callback: Callback,
			) => {
				this.writeRequestEndRequests.push(request);
				callback(null, this.writeRequestEndResponse);
				return grpcCall();
			},
			finishIdle: (
				request: FinishIdleRequest,
				_metadata: Metadata,
				_options: CallOptions,
				callback: Callback,
			) => {
				this.finishIdleRequests.push(request);
				callback(null, this.finishIdleResponse);
				return grpcCall();
			},
			commitInternalToolRepair: (
				request: CommitInternalToolRepairRequest,
				_metadata: Metadata,
				_options: CallOptions,
				callback: Callback,
			) => {
				this.repairRequests.push(request);
				callback(null, this.repairResponse);
				return grpcCall();
			},
			commitRuntimeTermination: (
				request: CommitRuntimeTerminationRequest,
				_metadata: Metadata,
				_options: CallOptions,
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

// serveOverLoopback serves the stand-in on a real channel. Every response is
// followed by paddingBytes in a field no response declares, which the
// client's decoder skips, so the channel carries a message of that size
// whichever RPC answers.
async function serveOverLoopback(bridge: TypedBridge, paddingBytes: number) {
	const varint = (value: number) => {
		const bytes: number[] = [];
		for (; value > 0x7f; value = Math.floor(value / 128)) bytes.push((value % 128) | 0x80);
		return Buffer.from([...bytes, value]);
	};
	const padding =
		paddingBytes === 0 ? Buffer.alloc(0) : Buffer.concat([varint((15_000 << 3) | 2), varint(paddingBytes), Buffer.alloc(paddingBytes)]);
	const service: ServiceDefinition = Object.fromEntries(
		Object.entries(AgentRuntimeBridgeServiceService as ServiceDefinition).map(([name, method]) => [
			name,
			{ ...method, responseSerialize: (value: unknown) => Buffer.concat([method.responseSerialize(value), padding]) },
		]),
	);
	const client = bridge.client() as unknown as Record<
		string,
		((request: unknown, metadata: Metadata, options: CallOptions, callback: Callback) => unknown) | undefined
	>;
	const handlers: UntypedServiceImplementation = Object.fromEntries(
		Object.keys(service).map((name) => [
			name,
			(call: ServerUnaryCall<unknown, unknown>, callback: sendUnaryData<unknown>) => {
				const answer = client[name];
				if (answer === undefined) {
					callback({ code: status.UNIMPLEMENTED });
					return;
				}
				answer(call.request, call.metadata, {}, callback as Callback);
			},
		]),
	);
	const server = new Server({ "grpc.max_receive_message_length": -1, "grpc.max_send_message_length": -1 });
	server.addService(service, handlers);
	const port = await new Promise<number>((resolve, reject) =>
		server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, bound) =>
			error === null ? resolve(bound) : reject(error),
		),
	);
	return {
		address: `127.0.0.1:${port}`,
		close: () => new Promise<void>((resolve) => server.tryShutdown(() => resolve())),
	};
}

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
		runtimeProcessId: "process-test",
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
		messages: [
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
			{
				messageSequence: 3,
				contextKind: "assistant",
				parts: [{ type: "text", text: "draft" }],
			},
		],
		currentRequestMessage: {
			modelRequestId: "mrq_open",
			assistantMessageSequence: 3,
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
			providerContextRetention: { disposition: "completed", toolUseEventIds: [], repairEventIds: [] },
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
