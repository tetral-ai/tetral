import { describe, expect, test } from "bun:test";
import { status } from "@grpc/grpc-js";
import type { AcceptedInputCommitResult } from "@tetral/agent-runtime-core/src/context/context-loader.js";
import type {
	RuntimeContextEntry,
	RuntimeInterruptToolResult,
	RuntimeJsonValue,
	SessionEvent,
	SessionEventEnvelope,
	SessionEventWriter,
	SessionEventWriterAppendResult,
	SessionEventWriterRequestEndEnvelope,
	SessionEventWriterToolSettlementEnvelope,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import {
	normalizeContextLoaderError,
	normalizeSessionEventWriterError,
	RuntimeInternalToolRepairStore,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { acceptedInputContextDrafts } from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import { AutoApprovalReviewerManager } from "@tetral/agent-runtime-core/src/session/approval-reviewer-manager.js";
import type * as SessionManager from "@tetral/agent-runtime-core/src/session/session-manager.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeControlInputCommitResult,
	RuntimeControlInputDeclaration,
	RuntimeControlInputState,
	RuntimeThreadAddressState,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import type { ThreadTurnLoadFacts } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import type { RuntimeApprovalReviewRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import {
	createToolCatalog,
	lookupToolEntry,
} from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { ProviderRequestKind } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Stream } from "effect";
import type { ApprovalReviewerThreadCreation } from "../../src/approval-reviewer.js";
import { createRuntimeApprovalReviewer } from "../../src/approval-reviewer.js";
import {
	runtimeModelForThread,
	runtimeToolPolicyForThread,
} from "../../src/command.js";
import type { RuntimeCoreHostsOptions } from "../../src/core-hosts.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";

const emptyTurnFacts: ThreadTurnLoadFacts = { events: [], internalRepairs: [] };

function textContextEntry(
	messageSequence: number,
	contextKind: "user" | "assistant" | "runtime_notification" | "compaction",
	text: string,
): RuntimeContextEntry {
	return { messageSequence, contextKind, parts: [{ type: "text", text }] };
}

function pendingToolContextEntry(input: {
	readonly messageSequence: number;
	readonly modelToolCallId: string;
	readonly toolName: string;
	readonly canonicalInput: RuntimeJsonValue;
}): RuntimeContextEntry {
	return {
		messageSequence: input.messageSequence,
		contextKind: "assistant",
		parts: [
			{
				type: "tool_call",
				modelToolCallId: input.modelToolCallId,
				toolName: input.toolName,
				canonicalInput: input.canonicalInput,
			},
		],
	};
}

function acceptedInputResult(
	input: RuntimeAcceptedInputState,
	messageSequenceStart = 1,
): AcceptedInputCommitResult {
	const drafts = acceptedInputContextDrafts(input);
	return {
		type: "committed",
		assignedContextSequences: drafts.map(
			(_draft, index) => messageSequenceStart + index,
		),
		pendingAttachments: [],
		interruptToolResults: [],
	};
}

function turnFactsForPendingTool(input: {
	readonly modelRequestId: string;
	readonly toolUseEventId: string;
	readonly modelToolCallId: string;
	readonly toolName: string;
	readonly family: "agent.tool_use" | "agent.mcp_tool_use";
}): ThreadTurnLoadFacts {
	return {
		events: [
			{
				eventId: `start_${input.modelRequestId}`,
				eventSequence: 1,
				type: "span.model_request_start",
				modelRequestId: input.modelRequestId,
				requestStart: {
					requestKind: "agent_provider_request",
					contextThroughMessageSequence: 1,
				},
			},
			{
				eventId: input.toolUseEventId,
				eventSequence: 2,
				type: input.family,
				modelRequestId: input.modelRequestId,
				toolUse: {
					modelToolCallId: input.modelToolCallId,
					toolName: input.toolName,
				},
			},
			{
				eventId: `end_${input.modelRequestId}`,
				eventSequence: 3,
				type: "span.model_request_end",
				modelRequestId: input.modelRequestId,
				requestEnd: {
					requestStartEventId: `start_${input.modelRequestId}`,
					isError: false,
					rescheduled: false,
				},
			},
		],
		internalRepairs: [],
	};
}

describe("Runtime core host production assembly", () => {
	test("reviewer input receipt starts one reviewer request and settles the parent gate once", async () => {
		const observations: string[] = [];
		const providerRequests: Array<{
			readonly requestKind: ProviderRequestKind;
		}> = [];
		const appended: SessionEvent[] = [];
		let inputCommits = 0;
		let nextMessageSequence = 2;
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			const result = successfulEventAppend(envelope);
			if (!result.ok || result.type === "stale") {
				throw new Error(
					"composed reviewer writer requires a committed append result",
				);
			}
			return {
				...result,
				...(!("assistant" in result)
					? {}
					: {
							assistant: {
								...result.assistant,
								messageSequence: nextMessageSequence++,
							},
						}),
			};
		});
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-08-11T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => ({
						contextEntries: [],
						turnFacts: emptyTurnFacts,
						runtimeBindingToken: "runtime-binding-token-reviewer-composed",
					}),
					commitAcceptedInput: async (input) => {
						inputCommits += 1;
						observations.push("input-receipt");
						return acceptedInputResult(input);
					},
				},
				threadLoop: {
					sessionEventWriter: baseWriter,
					providerCallRuntime: {
						...DefaultProviderCallRuntimeConfig,
						approvalReviewerPolicy:
							"Review the proposed action and return the required decision JSON.",
						timeoutMs: 1_000,
					},
					llmService: {
						stream: (request) => {
							providerRequests.push(request);
							observations.push("provider-request");
							return Stream.fromIterable([
								{ type: "text-start" as const, id: "review-decision" },
								{
									type: "text-delta" as const,
									id: "review-decision",
									text_delta: JSON.stringify({
										outcome: "allow",
										risk_level: "low",
										user_authorization: "high",
										rationale: "composed allow",
									}),
								},
								{ type: "text-end" as const, id: "review-decision" },
								{ type: "finish" as const, finishReason: "stop" as const },
							]);
						},
					},
				},
			}),
		});
		const threadCreator = {
			createApprovalReviewerThread: async (
				input: ApprovalReviewerThreadCreation,
			) => ({
				ok: true as const,
				reviewerThreadId: input.isTrunk
					? "thr_reviewer_trunk"
					: `thr_reviewer_sidecar_${input.reviewId}`,
				runtimeInputId: `rin_${input.reviewId}`,
			}),
			closeApprovalReviewerThread: async (
				_input: ApprovalReviewerThreadCreation,
			) => ({ ok: true as const }),
		};
		try {
			const reviewerHost = {
				...hosts.subAgentRunHost,
				inspectReviewerExecution: async (
					...args: Parameters<
						typeof hosts.subAgentRunHost.inspectReviewerExecution
					>
				) => {
					const result = await hosts.subAgentRunHost.inspectReviewerExecution(
						...args,
					);
					observations.push(
						`inspect:${result.ok ? String(result.observed) : result.reason}`,
					);
					return result;
				},
			};
			const reviewer = createRuntimeApprovalReviewer(() => reviewerHost, {
				model: { providerId: "anthropic", modelId: "claude-opus-4-8" },
				threadCreator,
				waitTimeoutMs: 2_000,
			});
			const result = await bounded(
				"composed reviewer progression",
				Effect.runPromise(reviewer(composedReviewRequest())),
			);
			if (result.type !== "decision") {
				throw new Error(
					`composed reviewer failed: ${JSON.stringify({ result, observations, appended })}`,
				);
			}

			expect(result).toMatchObject({
				type: "decision",
				outcome: "allow",
				message: "composed allow",
			});
			expect(inputCommits).toBe(1);
			expect(observations).toEqual([
				"input-receipt",
				"provider-request",
				"inspect:true",
			]);
			expect(providerRequests).toHaveLength(1);
			expect(providerRequests[0]?.requestKind).toBe(
				ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
			);
			expect(
				appended.filter((event) => event.type === "span.model_request_start"),
			).toHaveLength(1);
			expect(
				appended.filter((event) => event.type === "approval_review.decision"),
			).toHaveLength(1);
		} finally {
			await hosts.close();
		}
	});

	test("acknowledged sidecar outcome closes durably before its retained run slot is released", async () => {
		const firstProviderRelease = deferred<void>();
		const lifecycle: string[] = [];
		const reviewerTokens = new Map<
			string,
			SessionManager.ReviewerExecutionToken
		>();
		const reviewerControls = new Map<string, RuntimeThreadAddressState>();
		let providerCalls = 0;
		let nextMessageSequence = 2;
		const baseWriter = writerFrom((envelope) => {
			const result = successfulEventAppend(envelope);
			if (!result.ok || result.type === "stale") {
				throw new Error(
					"sidecar composition requires a committed append result",
				);
			}
			return {
				...result,
				...(!("assistant" in result)
					? {}
					: {
							assistant: {
								...result.assistant,
								messageSequence: nextMessageSequence++,
							},
						}),
			};
		});
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-08-12T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => ({
						contextEntries: [],
						turnFacts: emptyTurnFacts,
						runtimeBindingToken: "runtime-binding-token-reviewer-composed",
					}),
					commitAcceptedInput: async (input) => acceptedInputResult(input),
				},
				threadLoop: {
					sessionEventWriter: baseWriter,
					providerCallRuntime: {
						...DefaultProviderCallRuntimeConfig,
						approvalReviewerPolicy:
							"Review the proposed action and return the required decision JSON.",
						timeoutMs: 1_000,
					},
					llmService: {
						stream: () => {
							providerCalls += 1;
							const events = Stream.fromIterable([
								{ type: "text-start" as const, id: `review-${providerCalls}` },
								{
									type: "text-delta" as const,
									id: `review-${providerCalls}`,
									text_delta: JSON.stringify({
										outcome: "allow",
										risk_level: "low",
										user_authorization: "high",
										rationale: "composed allow",
									}),
								},
								{ type: "text-end" as const, id: `review-${providerCalls}` },
								{ type: "finish" as const, finishReason: "stop" as const },
							]);
							return providerCalls === 1
								? Stream.unwrap(
										Effect.promise(async () => {
											await firstProviderRelease.promise;
											return events;
										}),
									)
								: events;
						},
					},
				},
			}),
		});
		type ReviewerControl = Parameters<
			typeof hosts.subAgentRunHost.enqueueThreadInput
		>[0];
		const reviewerHost = {
			...hosts.subAgentRunHost,
			enqueueThreadInput: async (input: ReviewerControl) => {
				const result = await hosts.subAgentRunHost.enqueueThreadInput(input);
				if (
					input.kind === "approval_review" &&
					result.ok &&
					result.reviewerExecutionToken !== undefined
				) {
					reviewerTokens.set(
						input.sessionThreadId,
						result.reviewerExecutionToken,
					);
					reviewerControls.set(input.sessionThreadId, input);
				}
				return result;
			},
			commitApprovalReviewDecision: async (
				...args: Parameters<
					typeof hosts.subAgentRunHost.commitApprovalReviewDecision
				>
			) => {
				const result = await hosts.subAgentRunHost.commitApprovalReviewDecision(
					...args,
				);
				lifecycle.push("outcome-receipt");
				return result;
			},
			markThreadClosed: async (
				...args: Parameters<typeof hosts.subAgentRunHost.markThreadClosed>
			) => {
				lifecycle.push("hot-release");
				return await hosts.subAgentRunHost.markThreadClosed(...args);
			},
		};
		const threadCreator = {
			createApprovalReviewerThread: async (
				input: ApprovalReviewerThreadCreation,
			) => ({
				ok: true as const,
				reviewerThreadId: input.isTrunk
					? "thr_reviewer_trunk"
					: `thr_reviewer_sidecar_${input.reviewId}`,
				runtimeInputId: `rin_${input.reviewId}`,
			}),
			closeApprovalReviewerThread: async (
				creation: ApprovalReviewerThreadCreation,
			) => {
				const reviewerThreadId = creation.reviewerThreadId;
				if (reviewerThreadId === undefined) {
					throw new Error("sidecar close has no reviewer thread identity");
				}
				const control = reviewerControls.get(reviewerThreadId);
				const token = reviewerTokens.get(reviewerThreadId);
				if (control === undefined || token === undefined) {
					throw new Error("sidecar close has no retained execution identity");
				}
				expect(
					await hosts.subAgentRunHost.markThreadActive(control),
				).toMatchObject({
					ok: false,
					reason: "thread_busy",
				});
				const snapshot = await hosts.subAgentRunHost.inspectReviewerExecution(
					control,
					token,
				);
				expect(snapshot).toMatchObject({
					ok: true,
					observed: true,
					status: "idle",
				});
				lifecycle.push("durable-close-receipt");
				return { ok: true as const };
			},
		};
		try {
			const manager = new AutoApprovalReviewerManager();
			const reviewer = createRuntimeApprovalReviewer(() => reviewerHost, {
				model: { providerId: "anthropic", modelId: "claude-opus-4-8" },
				threadCreator,
				waitTimeoutMs: 2_000,
			});
			const request = {
				...composedReviewRequest(),
				approvalReviewerManager: manager,
			};
			const trunk = Effect.runPromise(
				reviewer({
					...request,
					targetModelToolCallId: "tool_call_reviewer_trunk",
				}),
			);
			await waitUntil(
				() => providerCalls === 1,
				"reviewer trunk provider request",
			);
			const sidecar = await bounded(
				"reviewer sidecar settlement",
				Effect.runPromise(
					reviewer({
						...request,
						targetModelToolCallId: "tool_call_reviewer_sidecar",
					}),
				),
			);
			expect(sidecar).toMatchObject({ type: "decision", outcome: "allow" });
			expect(lifecycle).toEqual([
				"outcome-receipt",
				"durable-close-receipt",
				"hot-release",
			]);
			firstProviderRelease.resolve(undefined);
			await bounded("reviewer trunk settlement", trunk);
		} finally {
			firstProviderRelease.resolve(undefined);
			await hosts.close();
		}
	});

	test("reviewer failure retries one durable identity before reusing its idle trunk", async () => {
		const failureAttempts: SessionEventEnvelope[] = [];
		const reviewerRequestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const reviewerIdleEvents: SessionEventEnvelope[] = [];
		const acceptedReviewerThreadIds: string[] = [];
		const lifecycleOrder: string[] = [];
		let providerCalls = 0;
		const baseWriter = writerFrom((envelope) => {
			if (envelope.event.type === "approval_review.failure") {
				failureAttempts.push(envelope);
				lifecycleOrder.push("failure_outcome");
				if (failureAttempts.length < 3) {
					return {
						ok: false,
						error: normalizeSessionEventWriterError({
							code: "unavailable",
							sessionId: envelope.sessionId,
							writeId: envelope.writeId,
						}),
					};
				}
			}
			if (envelope.event.type === "session.status_idle") {
				reviewerIdleEvents.push(envelope);
				lifecycleOrder.push("idle");
			}
			return successfulEventAppend(envelope);
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				reviewerRequestEnds.push(envelope);
				lifecycleOrder.push("request_end");
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const manager = new AutoApprovalReviewerManager();
		const enqueueResults: SessionManager.AcceptInputResult[] = [];
		const preloadResults: SessionManager.ThreadLifecycleResult[] = [];
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-08-12T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => ({
						contextEntries: [],
						turnFacts: emptyTurnFacts,
						runtimeBindingToken: "runtime-binding-token-reviewer-failure",
					}),
					commitAcceptedInput: async (input) => {
						if (input.kind === "approval_review") {
							acceptedReviewerThreadIds.push(input.sessionThreadId);
							return acceptedInputResult(
								input,
								acceptedReviewerThreadIds.length,
							);
						}
						return acceptedInputResult(input);
					},
				},
				threadLoop: {
					sessionEventWriter: writer,
					providerCallRuntime: {
						...DefaultProviderCallRuntimeConfig,
						approvalReviewerPolicy:
							"Review the proposed action and return the required decision JSON.",
						timeoutMs: 1_000,
					},
					llmService: {
						stream: () => {
							providerCalls += 1;
							if (providerCalls === 1) {
								return Stream.fromIterable([
									{
										type: "provider-error" as const,
										error: {
											type: "provider",
											code: "provider_stream_error",
											message: "Provider request failed.",
											retryable: false,
											fatal: true,
											providerId: "anthropic",
											modelId: "claude-opus-4-8",
											statusCode: 500,
										} as const,
									},
								]);
							}
							const text = JSON.stringify({
								outcome: "allow",
								risk_level: "low",
								user_authorization: "high",
								rationale: "reused trunk",
							});
							return Stream.fromIterable([
								{ type: "text-start" as const, id: `review-${providerCalls}` },
								{
									type: "text-delta" as const,
									id: `review-${providerCalls}`,
									text_delta: text,
								},
								{ type: "text-end" as const, id: `review-${providerCalls}` },
								{ type: "finish" as const, finishReason: "stop" as const },
							]);
						},
					},
				},
			}),
		});
		const threadCreator = {
			createApprovalReviewerThread: async (
				input: ApprovalReviewerThreadCreation,
			) => ({
				ok: true as const,
				reviewerThreadId: input.isTrunk
					? "thr_reviewer_trunk"
					: `thr_reviewer_sidecar_${input.reviewId}`,
				runtimeInputId: `rin_${input.reviewId}`,
			}),
			closeApprovalReviewerThread: async (
				_input: ApprovalReviewerThreadCreation,
			) => ({ ok: true as const }),
		};
		try {
			const reviewerHost = {
				...hosts.subAgentRunHost,
				preloadThread: async (
					...args: Parameters<typeof hosts.subAgentRunHost.preloadThread>
				) => {
					const result = await hosts.subAgentRunHost.preloadThread(...args);
					preloadResults.push(result);
					return result;
				},
				enqueueThreadInput: async (
					...args: Parameters<typeof hosts.subAgentRunHost.enqueueThreadInput>
				) => {
					const result = await hosts.subAgentRunHost.enqueueThreadInput(
						...args,
					);
					enqueueResults.push(result);
					return result;
				},
			};
			const reviewer = createRuntimeApprovalReviewer(() => reviewerHost, {
				model: { providerId: "anthropic", modelId: "claude-opus-4-8" },
				threadCreator,
				waitTimeoutMs: 2_000,
			});
			const request = {
				...composedReviewRequest(),
				approvalReviewerManager: manager,
			};
			const failed = await bounded(
				"composed reviewer failure",
				Effect.runPromise(reviewer(request)),
			);
			expect(preloadResults).toEqual([expect.objectContaining({ ok: true })]);
			expect(failed).toEqual({
				type: "failed",
				message: "approval reviewer returned no decision",
			});
			expect(reviewerRequestEnds).toHaveLength(1);
			expect(reviewerRequestEnds[0]?.modelRequestId).toBeDefined();
			expect(reviewerRequestEnds[0]?.isError).toBe(true);
			expect(reviewerIdleEvents).toHaveLength(1);
			expect(failureAttempts).toHaveLength(3);
			expect(lifecycleOrder).toEqual([
				"request_end",
				"idle",
				"failure_outcome",
				"failure_outcome",
				"failure_outcome",
			]);
			const failureWriteId = failureAttempts[0]?.writeId;
			if (failureWriteId === undefined) {
				throw new Error("reviewer failure did not reach its durable writer");
			}
			expect(
				new Set(failureAttempts.map((envelope) => envelope.writeId)),
			).toEqual(new Set([failureWriteId]));
			expect(
				failureAttempts.map((envelope) => JSON.stringify(envelope.event)),
			).toEqual([
				JSON.stringify(failureAttempts[0]?.event),
				JSON.stringify(failureAttempts[0]?.event),
				JSON.stringify(failureAttempts[0]?.event),
			]);
			const reviewerThreadId = acceptedReviewerThreadIds[0];
			if (reviewerThreadId === undefined) {
				throw new Error("reviewer failure did not commit accepted input");
			}
			expect(
				await hosts.subAgentRunHost.inspectThread({
					...commandScope(request.sessionId),
					workspaceId: request.workspaceId,
					sessionThreadId: reviewerThreadId,
					bindingId: request.bindingId,
					bindingGeneration: request.bindingGeneration,
					targetPodUid: request.targetPodUid,
				}),
			).toMatchObject({ ok: true, observed: true, status: "idle" });

			const reused = await bounded(
				"reused reviewer trunk",
				Effect.runPromise(
					reviewer({
						...request,
						modelRequestId: "mreq_reviewer_parent_next",
						parentBoundaryEventId: "sevt_reviewer_parent_next",
						targetModelToolCallId: "tool_call_reviewer_composed_next",
					}),
				),
			);
			expect(enqueueResults).toEqual([
				expect.objectContaining({ ok: true, started: true }),
				expect.objectContaining({ ok: true, started: true }),
			]);
			expect(acceptedReviewerThreadIds).toEqual([
				reviewerThreadId,
				reviewerThreadId,
			]);
			expect(providerCalls).toBe(2);
			expect(reused).toMatchObject({
				type: "decision",
				outcome: "allow",
				message: "reused trunk",
			});
		} finally {
			await hosts.close();
		}
	});

	test("a deterministic reviewer failure rejection stops at the composed durable writer", async () => {
		const failureAttempts: SessionEventEnvelope[] = [];
		const writer = writerFrom((envelope) => {
			if (envelope.event.type === "approval_review.failure") {
				failureAttempts.push(envelope);
				return {
					ok: false,
					error: normalizeSessionEventWriterError({
						code: "schema_mismatch",
						sessionId: envelope.sessionId,
						writeId: envelope.writeId,
					}),
				};
			}
			return successfulEventAppend(envelope);
		});
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-08-12T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => ({
						contextEntries: [],
						turnFacts: emptyTurnFacts,
						runtimeBindingToken: "runtime-binding-token-reviewer-rejection",
					}),
					commitAcceptedInput: async (input) => acceptedInputResult(input),
				},
				threadLoop: {
					sessionEventWriter: writer,
					providerCallRuntime: {
						...DefaultProviderCallRuntimeConfig,
						approvalReviewerPolicy:
							"Review the proposed action and return the required decision JSON.",
						timeoutMs: 1_000,
					},
					llmService: {
						stream: () =>
							Stream.fromIterable([
								{
									type: "provider-error" as const,
									error: {
										type: "provider",
										code: "provider_stream_error",
										message: "Provider request failed.",
										retryable: false,
										fatal: true,
										providerId: "anthropic",
										modelId: "claude-opus-4-8",
										statusCode: 500,
									} as const,
								},
							]),
					},
				},
			}),
		});
		try {
			const reviewer = createRuntimeApprovalReviewer(
				() => hosts.subAgentRunHost,
				{
					model: { providerId: "anthropic", modelId: "claude-opus-4-8" },
					threadCreator: {
						createApprovalReviewerThread: async (input) => ({
							ok: true as const,
							reviewerThreadId: input.isTrunk
								? "thr_reviewer_trunk"
								: `thr_reviewer_sidecar_${input.reviewId}`,
							runtimeInputId: `rin_${input.reviewId}`,
						}),
						closeApprovalReviewerThread: async () => ({ ok: true as const }),
					},
					waitTimeoutMs: 2_000,
				},
			);

			await expect(
				bounded(
					"deterministic reviewer failure rejection",
					Effect.runPromise(reviewer(composedReviewRequest())),
				),
			).resolves.toMatchObject({
				type: "settlement_failed",
				error: { code: "schema_mismatch", retryable: false },
			});
			expect(failureAttempts).toHaveLength(1);
			expect(failureAttempts[0]?.event).toMatchObject({
				type: "approval_review.failure",
				failure_kind: "runtime_failure",
			});
		} finally {
			await hosts.close();
		}
	});

	test("resident threads admit stored completion envelopes through the local command channel", async () => {
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies(),
		});
		try {
			const accepted = await bounded(
				"initial accepted input",
				hosts.commandRunHost.handleAcceptInput(acceptedInput("sesn_1")),
			);
			expect(accepted).toMatchObject({ ok: true, sessionId: "sesn_1" });
			const mailOperation = hosts.commandRunHost.handleAgentMail?.({
				...commandScope("sesn_1"),
				runtimeInputId: "agent_mail:delivery_warm_push",
				deliveryId: "delivery_warm_push",
				kind: "inter_agent_message",
				content: "child result",
			});
			const mail =
				mailOperation === undefined
					? undefined
					: await bounded("resident agent mail", mailOperation);
			expect(mail).toMatchObject({
				ok: true,
				sessionId: "sesn_1",
				applied: true,
			});

			const cleanup = await bounded(
				"resident cleanup",
				waitForCleanup(hosts.cleanupRunHost, "sesn_1"),
			);
			expect(cleanup).toMatchObject({ ok: true, sessionId: "sesn_1" });
		} finally {
			await bounded("resident host close", hosts.close());
		}
	});

	test("cold installation accepts target-owned pending mail in sent order before the triggering input", async () => {
		const observations: string[] = [];
		const descriptors = ["delivery_cold_1", "delivery_cold_2"].map(
			(deliveryId, index) => ({
				deliveryId,
				content: `child result ${index + 1}`,
			}),
		);
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => {
						observations.push("load");
						return {
							contextEntries: [],
							turnFacts: emptyTurnFacts,
							thread: {
								role: "main",
								visibility: "public",
								agentType: "general",
								status: "idle",
							},
							runtimeBindingToken: "runtime-binding-token-cold-mail",
							pendingAgentMail: descriptors,
						};
					},
					commitAcceptedInput: async (input) => {
						observations.push(`commit:${input.runtimeInputId}`);
						return acceptedInputResult(input);
					},
				},
			}),
		});
		try {
			expect(
				await hosts.commandRunHost.handleAcceptInput(
					acceptedInput("sesn_cold_mail"),
				),
			).toMatchObject({
				ok: true,
				sessionId: "sesn_cold_mail",
			});
			expect(observations).toEqual(["load"]);
		} finally {
			await hosts.close();
		}
	});

	test("cold child installation accepts its target-owned direct instruction", async () => {
		const observations: string[] = [];
		const deliveryId = "delivery_cold_direct";
		const parentThreadId = "thrd_cold_direct_parent";
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => ({
						contextEntries: [],
						turnFacts: emptyTurnFacts,
						thread: {
							parentThreadId,
							role: "subagent",
							visibility: "public",
							taskName: "child",
							agentType: "general",
							status: "idle",
						},
						runtimeBindingToken: "runtime-binding-token-cold-direct",
						pendingAgentMail: [
							{
								deliveryId,
								content: "parent instruction",
							},
						],
					}),
					commitAcceptedInput: async (input) => acceptedInputResult(input),
				},
			}),
		});
		try {
			expect(
				await hosts.commandRunHost.handleAcceptInput(
					acceptedInput("sesn_cold_direct"),
				),
			).toMatchObject({
				ok: true,
				sessionId: "sesn_cold_direct",
			});
			expect(observations).toEqual([]);
		} finally {
			await hosts.close();
		}
	});

	test("cold mail fails before installation when durable thread lineage is absent", async () => {
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => ({
						contextEntries: [],
						turnFacts: emptyTurnFacts,
						runtimeBindingToken: "runtime-binding-token-cold-no-lineage",
						pendingAgentMail: [
							{
								deliveryId: "delivery_cold_no_lineage",
								content: "parent instruction",
							},
						],
					}),
				},
			}),
		});
		try {
			await expect(
				hosts.commandRunHost.handleAcceptInput(
					acceptedInput("sesn_cold_mail_no_lineage"),
				),
			).resolves.toMatchObject({ ok: false });
		} finally {
			await hosts.close();
		}
	});

	test("cold agent-mail trigger starts the loaded input instead of ACKing it idle", async () => {
		const deliveryId = "delivery_cold_trigger";
		const committed = deferred<void>();
		const commits: string[] = [];
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => ({
						contextEntries: [],
						turnFacts: emptyTurnFacts,
						thread: {
							role: "main",
							visibility: "public",
							agentType: "general",
							status: "idle",
						},
						runtimeBindingToken: "runtime-binding-token-cold-trigger",
						pendingAgentMail: [
							{
								deliveryId,
								content: "stored completion",
							},
						],
					}),
					commitAcceptedInput: async (input) => {
						commits.push(input.runtimeInputId);
						committed.resolve();
						return acceptedInputResult(input);
					},
				},
			}),
		});
		try {
			const result = await hosts.commandRunHost.handleAgentMail?.({
				...commandScope("sesn_cold_trigger"),
				runtimeInputId: `agent_mail:${deliveryId}`,
				deliveryId,
				kind: "inter_agent_message",
				content: "stored completion",
			});
			const snapshot = await hosts.subAgentRunHost.inspectThread(
				commandScope("sesn_cold_trigger"),
			);
			expect({ result, snapshot }).toMatchObject({
				result: { ok: true, applied: true },
			});
			await committed.promise;
			expect(commits).toEqual([`agent_mail:${deliveryId}`]);
		} finally {
			await hosts.close();
		}
	});

	test("completion pull reads the target-owned stored envelope without loading context", async () => {
		const observations: string[] = [];
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => {
						observations.push("load");
						return {
							contextEntries: [],
							turnFacts: emptyTurnFacts,
							runtimeBindingToken: "runtime-binding-token-pull",
						};
					},
					readAgentMail: async (_command, childThreadId) => {
						observations.push(`read:${childThreadId}`);
						return {
							deliveryId: "delivery_pull",
							content: "stored completion",
						};
					},
				},
			}),
		});
		try {
			expect(
				await hosts.subAgentRunHost.pullAgentMail?.(
					commandScope("sesn_pull"),
					"thrd_child",
				),
			).toEqual({
				deliveryId: "delivery_pull",
				finalMessage: "stored completion",
			});
			expect(observations).toEqual(["read:thrd_child"]);
		} finally {
			await hosts.close();
		}
	});

	test("completion pull returns no message when the durable reader is empty", async () => {
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					readAgentMail: async () => undefined,
				},
			}),
		});
		try {
			await expect(
				hosts.subAgentRunHost.pullAgentMail?.(
					commandScope("sesn_pull_empty"),
					"thrd_child",
				),
			).resolves.toBeUndefined();
		} finally {
			await hosts.close();
		}
	});

	test("message command exposes an installing ThreadEntry during its sole cold load", async () => {
		const observations: string[] = [];
		let loadCount = 0;
		let hosts: Awaited<ReturnType<typeof buildRuntimeCoreHosts>> | undefined;
		hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async (command) => {
						loadCount += 1;
						observations.push("loadThreadContext");
						const inspected = await hosts?.subAgentRunHost.inspectThread(
							commandScope(command.sessionId),
						);
						observations.push(
							`observed:${inspected?.ok === true ? inspected.observed : "unavailable"}`,
						);
						return {
							contextEntries: [],
							turnFacts: emptyTurnFacts,
							runtimeBindingToken: "runtime-binding-token-cold",
						};
					},
				},
			}),
		});
		try {
			const accepted = await hosts.commandRunHost.handleAcceptInput(
				acceptedInput("sesn_cold"),
			);
			const inspected = await hosts.subAgentRunHost.inspectThread(
				commandScope("sesn_cold"),
			);

			expect(accepted).toMatchObject({
				ok: true,
				sessionId: "sesn_cold",
				created: true,
			});
			expect(observations).toEqual(["loadThreadContext", "observed:true"]);
			expect(inspected).toMatchObject({
				ok: true,
				sessionId: "sesn_cold",
				sessionThreadId: "thrd_1",
				observed: true,
			});
			expect(
				await hosts.commandRunHost.handleAcceptInput({
					...acceptedInput("sesn_cold"),
					runtimeInputId: "rin_warm",
					inputOrder: 2,
				}),
			).toMatchObject({ ok: true, sessionId: "sesn_cold" });
			expect(loadCount).toBe(1);
		} finally {
			await hosts.close();
		}
	});

	test("cold interrupt and sibling commands join one complete preload", async () => {
		const loadStarted = deferred<void>();
		const releaseLoad = deferred<void>();
		let loadCount = 0;
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => {
						loadCount += 1;
						loadStarted.resolve();
						await releaseLoad.promise;
						return {
							contextEntries: [],
							turnFacts: emptyTurnFacts,
							runtimeBindingToken: "runtime-binding-token-singleflight",
						};
					},
				},
				threadLoop: {
					sessionEventWriter: testSessionEventWriter(),
					llmService: {
						stream: () =>
							Stream.fromIterable([
								{ type: "text-start" as const, id: "singleflight" },
								{
									type: "text-delta" as const,
									id: "singleflight",
									text_delta: "done",
								},
								{ type: "text-end" as const, id: "singleflight" },
								{ type: "finish" as const, finishReason: "stop" as const },
							]),
					},
				},
			}),
		});
		try {
			const scope = commandScope("sesn_singleflight");
			const interruptCommand = {
				...scope,
				runtimeInputId: "rin_interrupt_singleflight",
				inputOrder: 1,
				origin: "user" as const,
			};
			const interrupt = hosts.commandRunHost.handleInterruptControl(
				"sesn_singleflight",
				interruptCommand,
				async (declaration) =>
					controlCommitResult(interruptCommand, declaration),
			);
			await loadStarted.promise;
			const message = hosts.commandRunHost.handleAcceptInput({
				...acceptedInput("sesn_singleflight"),
				runtimeInputId: "rin_message_singleflight",
			});
			const config = hosts.commandRunHost.handleRuntimeConfigPatch(
				"sesn_singleflight",
				{
					...scope,
					bindingId: "bind_singleflight_replacement",
					bindingGeneration: 2,
					configIdentity: "session:2",
					generation: 2,
					contentJson: '{"config_generation":2}',
				},
			);
			const task = hosts.commandRunHost.handleTaskNotification(
				"sesn_singleflight",
				{
					...scope,
					kind: "task_notification",
					runtimeInputId: "rin_task_singleflight",
					inputOrder: 0,
					taskId: "task_singleflight",
					sourceToolUseEventId: "sevt_tool_singleflight",
					status: "completed",
					notificationJson:
						'{"task_id":"task_singleflight","source_tool_use_event_id":"sevt_tool_singleflight","status":"completed"}',
				},
			);
			await Promise.resolve();
			expect(await hosts.subAgentRunHost.inspectThread(scope)).toMatchObject({
				ok: true,
				observed: true,
			});

			releaseLoad.resolve();
			const [interruptResult, messageResult, configResult, taskResult] =
				await bounded(
					"singleflight command completion",
					Promise.all([interrupt, message, config, task]),
				);
			expect(
				await hosts.subAgentRunHost.waitThread(scope, 2_000),
			).toMatchObject({
				ok: true,
				observed: true,
				timedOut: false,
			});
			expect(interruptResult).toEqual({
				ok: true,
				sessionId: "sesn_singleflight",
				created: false,
				interrupted: false,
				idleInterrupt: true,
			});
			expect(messageResult).toMatchObject({
				ok: true,
				sessionId: "sesn_singleflight",
			});
			expect(configResult).toEqual({
				ok: false,
				sessionId: "sesn_singleflight",
				reason: "control_busy",
			});
			expect(taskResult).toMatchObject({
				ok: true,
				sessionId: "sesn_singleflight",
				applied: true,
			});
			expect(loadCount).toBe(1);
			const finalSnapshot = await hosts.subAgentRunHost.inspectThread(scope);
			expect(finalSnapshot).toMatchObject({
				ok: true,
				observed: false,
			});
		} finally {
			await hosts.close();
		}
	});

	test("approved pending MCP call resumes after cold manifest install", async () => {
		const observations: string[] = [];
		const runToolCalls: string[] = [];
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const terminalResultAppended = deferred<void>();
		const pendingInput = { query: "tetral" };
		const loadedEntries = [
			textContextEntry(1, "user", "hello"),
			pendingToolContextEntry({
				messageSequence: 2,
				modelToolCallId: "tool-1",
				toolName: "github_search",
				canonicalInput: pendingInput,
			}),
		];
		const replacementScope = {
			...commandScope("sesn_cold_confirm"),
			bindingId: "bind_2",
			bindingGeneration: 2,
			targetPodUid: "pod_2",
		};

		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async (command) => {
						observations.push(
							`load:${command.bindingId}:${command.bindingGeneration}`,
						);
						return {
							contextEntries: loadedEntries,
							turnFacts: turnFactsForPendingTool({
								modelRequestId: "mrq_cold_confirm",
								toolUseEventId: "sevt_tool_1",
								modelToolCallId: "tool-1",
								toolName: "github_search",
								family: "agent.mcp_tool_use",
							}),
							runtimeBindingToken: "runtime-binding-token-cold-confirm",
							runtimeConfigPatch: {
								...command,
								configIdentity: "session:5",
								generation: 5,
								coldLoad: true,
								installedBuiltinFamily: "claude",
								contentJson: JSON.stringify({
									config_generation: 5,
									runtime_config: {
										agent: { config: { model: "openai/gpt-5.5" } },
										installedTools: [
											{ type: "tetral_agent_toolset", family: "claude" },
										],
									},
									tool_policy: { mcpToolsets: [{ mcpServerName: "github" }] },
								}),
							},
							mcpManifests: [
								{
									...command,
									configIdentity: "mcp:github:7",
									generation: 7,
									mcpServerName: "github",
									manifestETag: "etag_7",
									contentJson: JSON.stringify({
										mcp_manifest: {
											mcp_server_name: "github",
											manifest_etag: "etag_7",
											manifest_generation: 7,
											tools: [
												{
													name: "github_search",
													description: "Search GitHub",
													input_schema: { type: "object" },
												},
											],
										},
									}),
								},
							],
							pendingToolUses: [
								{
									toolUseEventId: "sevt_tool_1",
									modelRequestId: "mrq_cold_confirm",
									modelToolCallId: "tool-1",
									toolName: "github_search",
									input: pendingInput,
									decision: "allow",
									status: "resolving",
								},
							],
						};
					},
				},
				threadLoop: {
					providerCallRuntime: {
						systemInstructions: "cold confirmation system",
					},
					runtimeModel: (session) =>
						runtimeModelForThread(
							session.identity.threadRole,
							session.configuration.patches().map((patch) => patch.contentJson),
							{ providerId: "anthropic", modelId: "claude-opus-4-8" },
						),
					runtimePolicy: (session) => {
						const policy = runtimeToolPolicyForThread(
							session.identity.threadRole,
							session.configuration.patches().map((patch) => patch.contentJson),
							session.configuration.installedBuiltinFamily(),
						);
						if (
							lookupToolEntry(policy.toolCatalog, "github_search") !== undefined
						) {
							observations.push("manifest:installed");
						}
						return policy;
					},
					sessionEventWriter: {
						settleToolResult: async (envelope) => {
							settlements.push(envelope);
							terminalResultAppended.resolve();
							return { ok: true, result: { type: "committed" } };
						},
						append: async (envelope) => {
							appended.push(envelope.event);
							return successfulEventAppend(envelope);
						},
						writeRequestEnd: async (envelope) => requestEndResult(envelope),
						finishIdle: async (envelope) => ({
							ok: true,
							type: "committed",
							idleEventId: `evt_${envelope.durableTurnId}`,
						}),
					},
					runTool: (request) => {
						observations.push("tool:invoked");
						runToolCalls.push(
							`${request.modelRequestId}:${request.modelToolCallId}:${request.toolUseEventId}`,
						);
						expect(request.input).toEqual(pendingInput);
						expect(request.currentModel).toEqual({
							providerId: "openai",
							modelId: "gpt-5.5",
						});
						expect(request.entry.route).toEqual({
							kind: "gateway",
							operation: "RunMcpTool",
							mcpServerName: "github",
						});
						expect(request.bindingId).toBe("bind_2");
						expect(request.bindingGeneration).toBe(2);
						return {
							type: "completed",
							output: { text: "approved", truncated: false },
						};
					},
				},
			}),
		});
		try {
			const confirmationCommand = {
				...replacementScope,
				runtimeInputId: "rin_confirm_cold",
				toolUseEventId: "sevt_tool_1",
				decision: "allow",
			} as const;
			const result = await bounded(
				"cold MCP confirmation",
				hosts.commandRunHost.handleToolConfirmation(
					"sesn_cold_confirm",
					confirmationCommand,
					async (declaration) =>
						controlCommitResult(confirmationCommand, declaration, {
							assignedContextSequences: [3],
						}),
				),
			);

			if (!result.ok) {
				throw new Error(
					`cold MCP confirmation failed: ${JSON.stringify({ result, observations })}`,
				);
			}
			expect(result).toMatchObject({
				ok: true,
				sessionId: "sesn_cold_confirm",
				applied: false,
			});
			try {
				await bounded(
					"cold MCP terminal settlement",
					terminalResultAppended.promise,
				);
			} catch {
				const snapshot =
					await hosts.subAgentRunHost.inspectThread(replacementScope);
				throw new Error(
					`cold MCP recovery did not settle: ${JSON.stringify({ result, observations, runToolCalls, settlements, snapshot })}`,
				);
			}
			expect(observations[0]).toBe("load:bind_2:2");
			const manifestInstalledAt = observations.indexOf("manifest:installed");
			const toolInvokedAt = observations.indexOf("tool:invoked");
			expect(manifestInstalledAt).toBeGreaterThan(0);
			expect(toolInvokedAt).toBeGreaterThan(manifestInstalledAt);
			expect(runToolCalls).toEqual(["mrq_cold_confirm:tool-1:sevt_tool_1"]);
			expect(settlements).toEqual([
				expect.objectContaining({
					settlement: {
						toolUseEventId: "sevt_tool_1",
						outcome: {
							type: "completed",
							output: { text: "approved", truncated: false },
						},
					},
				}),
			]);
			expect(JSON.stringify(appended)).not.toContain("unknown");
		} finally {
			await hosts.close();
		}
	});

	test("cold interrupt installs pending tools before committing their cancellation", async () => {
		const observations: string[] = [];
		const pendingInput = { file_path: "src/a.ts", content: "ok" };
		const loadedEntries = [
			textContextEntry(1, "user", "hello"),
			pendingToolContextEntry({
				messageSequence: 2,
				modelToolCallId: "tool-1",
				toolName: "Write",
				canonicalInput: pendingInput,
			}),
		];

		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async (command) => {
						observations.push(`load:${command.sessionThreadId}`);
						return {
							contextEntries: loadedEntries,
							turnFacts: turnFactsForPendingTool({
								modelRequestId: "mrq_interrupt_confirm",
								toolUseEventId: "sevt_tool_1",
								modelToolCallId: "tool-1",
								toolName: "Write",
								family: "agent.tool_use",
							}),
							runtimeBindingToken: "runtime-binding-token-interrupt-confirm",
							runtimeConfigPatch: {
								...command,
								configIdentity: "session:3",
								generation: 3,
								coldLoad: true,
								installedBuiltinFamily: "claude",
								contentJson: JSON.stringify({
									config_generation: 3,
									runtime_config: {
										agent: { config: { model: "fake/fake-chat" } },
										installedTools: [
											{ type: "tetral_agent_toolset", family: "claude" },
										],
									},
								}),
							},
							pendingToolUses: [
								{
									toolUseEventId: "sevt_tool_1",
									modelRequestId: "mrq_interrupt_confirm",
									modelToolCallId: "tool-1",
									toolName: "Write",
									input: pendingInput,
									decision: "allow",
									status: "resolving",
								},
							],
						};
					},
				},
			}),
		});
		try {
			const interruptCommand = {
				...commandScope("sesn_interrupt_confirm"),
				runtimeInputId: "rin_interrupt_before_confirm",
				inputOrder: 2,
				origin: "user" as const,
			};
			let committedDeclaration: RuntimeControlInputDeclaration | undefined;
			const interrupt = await hosts.commandRunHost.handleInterruptControl(
				"sesn_interrupt_confirm",
				interruptCommand,
				async (declaration) => {
					committedDeclaration = declaration;
					return controlCommitResult(interruptCommand, declaration, {
						interruptToolResults: [
							{ toolUseEventId: "sevt_tool_1", result: { type: "cancelled" } },
						],
					});
				},
			);
			const shell = await hosts.subAgentRunHost.inspectThread(
				commandScope("sesn_interrupt_confirm"),
			);

			expect(interrupt).toEqual({
				ok: true,
				sessionId: "sesn_interrupt_confirm",
				created: false,
				interrupted: false,
				idleInterrupt: true,
			});
			expect(observations).toEqual(["load:thrd_1"]);
			expect(committedDeclaration).toEqual({ inputKind: "interrupt" });
			expect(shell).toMatchObject({
				ok: true,
				observed: true,
				hasPendingApprovalToolJobs: false,
			});
		} finally {
			await hosts.close();
		}
	});

	test("runtime config acknowledges without creating thread residency", async () => {
		const observations: string[] = [];
		let hosts: Awaited<ReturnType<typeof buildRuntimeCoreHosts>> | undefined;
		hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async (command) => {
						observations.push(`load:${command.sessionThreadId}`);
						const inspected = await hosts?.subAgentRunHost.inspectThread(
							commandScope(command.sessionId),
						);
						observations.push(
							`observed:${inspected?.ok === true ? inspected.observed : "unavailable"}`,
						);
						return {
							contextEntries: [],
							turnFacts: emptyTurnFacts,
							runtimeBindingToken: "runtime-binding-token-config",
						};
					},
				},
			}),
		});
		try {
			const result = await hosts.commandRunHost.handleRuntimeConfigPatch(
				"sesn_cold_config",
				{
					...commandScope("sesn_cold_config"),
					configIdentity: "session:6",
					generation: 6,
					contentJson: '{"config_generation":6}',
				},
			);
			const inspected = await hosts.subAgentRunHost.inspectThread(
				commandScope("sesn_cold_config"),
			);

			expect(result).toEqual({
				ok: true,
				sessionId: "sesn_cold_config",
				created: false,
				applied: false,
				noResidency: true,
			});
			expect(observations).toEqual([]);
			expect(inspected).toMatchObject({ ok: true, observed: false });
		} finally {
			await hosts?.close();
		}
	});

	test.each([
		{ code: "schema_mismatch" as const, retryable: false },
		{ code: "unavailable" as const, retryable: true },
	])(
		"cold context load reports $code retryability in band",
		async ({ code, retryable }) => {
			const hosts = await buildRuntimeCoreHosts({
				maxLocalSessions: 1,
				now: () => "2026-06-16T00:00:00.000Z",
				...testCoreDependencies({
					contextLoader: {
						loadThreadContext: async (command) => {
							throw normalizeContextLoaderError({
								code,
								sessionId: command.sessionId,
								reason: "cold context could not be loaded",
							});
						},
					},
				}),
			});
			try {
				await expect(
					hosts.commandRunHost.handleAcceptInput(
						acceptedInput(`sesn_context_${code}`),
					),
				).resolves.toMatchObject({
					ok: false,
					reason: "context_load_failed",
					retryable,
				});
			} finally {
				await hosts.close();
			}
		},
	);

	test("cold Request Start without Request End remains passive until durable repair", async () => {
		let providerCalls = 0;
		let appendCalls = 0;
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => ({
						contextEntries: [],
						turnFacts: {
							events: [
								{
									eventId: "sevt_open_running",
									eventSequence: 1,
									type: "session.status_running",
								},
								{
									eventId: "sevt_open_start",
									eventSequence: 2,
									type: "span.model_request_start",
									modelRequestId: "mreq_open_cold",
									requestStart: {
										requestKind: "agent_provider_request",
										contextThroughMessageSequence: 0,
									},
								},
							],
							internalRepairs: [],
						},
						runtimeBindingToken: "runtime-binding-token-open-cold",
					}),
				},
				threadLoop: {
					llmService: {
						stream: () => {
							providerCalls += 1;
							return Stream.empty;
						},
					},
					sessionEventWriter: {
						settleToolResult: async () => ({
							ok: true,
							result: { type: "committed" },
						}),
						append: async (envelope) => {
							appendCalls += 1;
							return successfulEventAppend(envelope);
						},
						writeRequestEnd: async (envelope) => requestEndResult(envelope),
					},
				},
			}),
		});
		try {
			expect(
				await hosts.subAgentRunHost.preloadThread(
					commandScope("sesn_open_cold"),
				),
			).toMatchObject({
				ok: true,
				applied: true,
			});
			await Promise.resolve();
			expect(providerCalls).toBe(0);
			expect(appendCalls).toBe(0);
		} finally {
			await hosts.close();
		}
	});

	test("LoadContext excludes request facts superseded by the current run", async () => {
		const providerRequests: string[] = [];
		const appended: SessionEvent[] = [];
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => ({
						contextEntries: [],
						turnFacts: {
							events: [
								{
									eventId: "sevt_superseded_run",
									eventSequence: 1,
									type: "session.status_running" as const,
								},
								{
									eventId: "sevt_superseded_start",
									eventSequence: 2,
									type: "span.model_request_start" as const,
									modelRequestId: "mreq_superseded",
									requestStart: {
										requestKind: "agent_provider_request" as const,
										contextThroughMessageSequence: 0,
									},
								},
								{
									eventId: "sevt_superseded_tool",
									eventSequence: 3,
									type: "agent.tool_use" as const,
									modelRequestId: "mreq_superseded",
									toolUse: {
										modelToolCallId: "call_superseded",
										toolName: "Write",
									},
								},
								{
									eventId: "sevt_current_run",
									eventSequence: 4,
									type: "session.status_running" as const,
								},
							],
							internalRepairs: [],
						},
						durableTurnId: "sevt_current_run",
						runtimeBindingToken: "runtime-binding-token-current-run",
					}),
				},
				threadLoop: {
					sessionEventWriter: writerFrom((envelope) => {
						appended.push(envelope.event);
						return successfulEventAppend(envelope);
					}),
					providerCallRuntime: {
						...DefaultProviderCallRuntimeConfig,
						timeoutMs: 1_000,
					},
					runtimePolicy: () => ({}),
					llmService: {
						stream: (request) => {
							providerRequests.push(request.modelRequestId);
							return Stream.fromIterable([
								{ type: "text-start" as const, id: "current" },
								{
									type: "text-delta" as const,
									id: "current",
									text_delta: "current response",
								},
								{ type: "text-end" as const, id: "current" },
								{ type: "finish" as const, finishReason: "stop" as const },
							]);
						},
					},
				},
			}),
		});
		try {
			const scope = commandScope("sesn_superseded_request_facts");
			expect(
				await hosts.commandRunHost.handleAcceptInput(
					{
						...acceptedInput(scope.sessionId),
						contentJson: JSON.stringify({
							messages: [
								{ parts: [{ type: "text", text: "new current input" }] },
							],
						}),
					},
				),
			).toMatchObject({ ok: true, started: true });
			await waitUntil(
				() => providerRequests.length === 1,
				"current request after LoadContext projection",
			);
			expect(providerRequests).toHaveLength(1);
			expect(providerRequests).not.toContain("mreq_superseded");
			expect(
				appended.filter((event) => event.type === "session.error"),
			).toEqual([]);
		} finally {
			await hosts.close();
		}
	});

	test("cold settled requires-action closeout opens exactly one successor run", async () => {
		const providerRequests: string[] = [];
		const appended: SessionEvent[] = [];
		const hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async () => ({
						contextEntries: [],
						turnFacts: {
							events: [
								{
									eventId: "sevt_settled_idle_run",
									eventSequence: 1,
									type: "session.status_running" as const,
								},
								{
									eventId: "sevt_settled_idle_start",
									eventSequence: 2,
									type: "span.model_request_start" as const,
									modelRequestId: "mreq_settled_idle",
									requestStart: {
										requestKind: "agent_provider_request" as const,
										contextThroughMessageSequence: 0,
									},
								},
								{
									eventId: "sevt_settled_idle_tool",
									eventSequence: 3,
									type: "agent.tool_use" as const,
									modelRequestId: "mreq_settled_idle",
									toolUse: {
										modelToolCallId: "call_settled_idle",
										toolName: "Write",
									},
								},
								{
									eventId: "sevt_settled_idle_end",
									eventSequence: 4,
									type: "span.model_request_end" as const,
									modelRequestId: "mreq_settled_idle",
									requestEnd: {
										requestStartEventId: "sevt_settled_idle_start",
										isError: false,
										rescheduled: false,
									},
								},
								{
									eventId: "sevt_settled_idle_result",
									eventSequence: 5,
									type: "agent.tool_result" as const,
									modelRequestId: "mreq_settled_idle",
									toolResult: {
										modelToolCallId: "call_settled_idle",
										toolName: "Write",
										outcome: "completed" as const,
									},
								},
								{
									eventId: "sevt_settled_idle_closeout",
									eventSequence: 6,
									type: "session.status_idle" as const,
									idle: { stopReason: "requires_action" },
								},
							],
							internalRepairs: [],
						},
						runtimeBindingToken: "runtime-binding-token-settled-idle",
					}),
				},
				threadLoop: {
					sessionEventWriter: writerFrom((envelope) => {
						appended.push(envelope.event);
						return successfulEventAppend(envelope);
					}),
					providerCallRuntime: {
						...DefaultProviderCallRuntimeConfig,
						timeoutMs: 1_000,
					},
					runtimePolicy: () => ({}),
					llmService: {
						stream: (request) => {
							providerRequests.push(request.modelRequestId);
							return Stream.fromIterable([
								{ type: "text-start" as const, id: "successor" },
								{
									type: "text-delta" as const,
									id: "successor",
									text_delta: "successor response",
								},
								{ type: "text-end" as const, id: "successor" },
								{ type: "finish" as const, finishReason: "stop" as const },
							]);
						},
					},
				},
			}),
		});
		try {
			const scope = commandScope("sesn_settled_idle_cold");
			expect(
				await hosts.commandRunHost.handleAcceptInput(
					acceptedInput(scope.sessionId),
				),
			).toMatchObject({ ok: true, started: true });
			await waitUntil(
				() =>
					appended.filter((event) => event.type === "session.status_running")
						.length === 1,
				"successor run after cold settled closeout",
			);
			expect(providerRequests).not.toContain("mreq_settled_idle");
			expect(
				appended.filter((event) => event.type === "session.status_running"),
			).toHaveLength(1);
			expect(
				appended.filter((event) => event.type === "session.error"),
			).toEqual([]);
		} finally {
			await hosts.close();
		}
	});

	test("task notification cold-loads thread context before hot settlement", async () => {
		const observations: string[] = [];
		const committedEntry = textContextEntry(
			1,
			"runtime_notification",
			"task completed",
		);
		let hosts: Awaited<ReturnType<typeof buildRuntimeCoreHosts>> | undefined;
		hosts = await buildRuntimeCoreHosts({
			maxLocalSessions: 4,
			now: () => "2026-06-16T00:00:00.000Z",
			...testCoreDependencies({
				contextLoader: {
					loadThreadContext: async (command) => {
						observations.push(`load:${command.sessionThreadId}`);
						const inspected = await hosts?.subAgentRunHost.inspectThread(
							commandScope(command.sessionId),
						);
						observations.push(
							`observed:${inspected?.ok === true ? inspected.observed : "unavailable"}`,
						);
						return {
							contextEntries: [committedEntry],
							turnFacts: emptyTurnFacts,
							runtimeBindingToken: "runtime-binding-token-task",
						};
					},
				},
			}),
		});
		try {
			const result = await hosts.commandRunHost.handleTaskNotification(
				"sesn_cold_task",
				{
					...commandScope("sesn_cold_task"),
					kind: "task_notification",
					runtimeInputId: "rin_task_cold",
					inputOrder: 0,
					taskId: "task_1",
					sourceToolUseEventId: "sevt_tool_1",
					status: "completed",
					notificationJson:
						'{"task_id":"task_1","source_tool_use_event_id":"sevt_tool_1","status":"completed"}',
				},
			);
			const inspected = await hosts.subAgentRunHost.inspectThread(
				commandScope("sesn_cold_task"),
			);

			expect(result).toEqual({
				ok: true,
				sessionId: "sesn_cold_task",
				created: true,
				applied: true,
			});
			expect(observations).toEqual(["load:thrd_1", "observed:true"]);
			expect(inspected).toMatchObject({
				ok: true,
				observed: true,
				entries: [committedEntry],
			});
		} finally {
			await hosts?.close();
		}
	});
});

async function waitForCleanup(
	cleanupRunHost: Awaited<
		ReturnType<typeof buildRuntimeCoreHosts>
	>["cleanupRunHost"],
	sessionId: string,
) {
	for (let attempt = 0; attempt < 20; attempt += 1) {
		const result = await cleanupRunHost.handleCleanupSession(
			cleanupScope(sessionId),
		);
		if (result.ok) {
			return result;
		}
		await new Promise<void>((resolve) => {
			setTimeout(resolve, 5);
		});
	}
	return await cleanupRunHost.handleCleanupSession(cleanupScope(sessionId));
}

function cleanupScope(sessionId: string) {
	const scope = commandScope(sessionId);
	return {
		workspaceId: scope.workspaceId,
		sessionId: scope.sessionId,
		bindingId: scope.bindingId,
		bindingGeneration: scope.bindingGeneration,
		targetPodUid: scope.targetPodUid,
		cleanupOperationId: "cleanup_test",
	};
}

function commandScope(sessionId: string) {
	return {
		workspaceId: "wksp_1",
		sessionId,
		sessionThreadId: "thrd_1",
		bindingId: "bind_1",
		bindingGeneration: 1,
		targetPodUid: "pod_1",
		runtimeInputId: "rin_cleanup",
	};
}

function acceptedInput(sessionId: string) {
	return {
		kind: "messages" as const,
		workspaceId: "wksp_1",
		sessionId,
		sessionThreadId: "thrd_1",
		bindingId: "bind_1",
		bindingGeneration: 1,
		targetPodUid: "pod_1",
		runtimeInputId: "rin_1",
		inputOrder: 1,
		contentJson: JSON.stringify({ text: "test input" }),
	};
}

function composedReviewRequest(): RuntimeApprovalReviewRequest {
	const sessionId = "sesn_reviewer_composed";
	return {
		workspaceId: "wksp_1",
		sessionId,
		sessionThreadId: "thrd_reviewer_parent",
		bindingId: "bind_reviewer_composed",
		bindingGeneration: 1,
		targetPodUid: "pod_reviewer_composed",
		runtimeBindingToken: "runtime-binding-token-reviewer-composed",
		modelRequestId: "mreq_reviewer_parent",
		parentBoundaryEventId: "sevt_reviewer_parent_start",
		targetModelToolCallId: "tool_call_reviewer_composed",
		targetToolName: "Write",
		actionJson: { path: "src/a.ts", content: "ok" },
		approvalReviewerManager: new AutoApprovalReviewerManager(),
		parentTranscript: {
			generation: 1,
			entries: [textContextEntry(1, "user", "review this action")],
		},
		currentAssistantDraft: [],
		siblingToolCalls: [
			{
				modelToolCallId: "tool_call_reviewer_composed",
				toolName: "Write",
				actionJson: { path: "src/a.ts", content: "ok" },
			},
		],
		policyContext: {
			approvalMode: "approve_for_me",
			permissionPolicy: "always_ask",
		},
		currentModel: { providerId: "anthropic", modelId: "claude-opus-4-8" },
	};
}

function deferred<T>(): {
	readonly promise: Promise<T>;
	readonly resolve: (value: T) => void;
} {
	let resolve: (value: T) => void = () => undefined;
	const promise = new Promise<T>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

async function bounded<T>(label: string, operation: Promise<T>): Promise<T> {
	return await Promise.race([
		operation,
		new Promise<never>((_, reject) =>
			setTimeout(() => reject(new Error(`timed out during ${label}`)), 2_000),
		),
	]);
}

async function waitUntil(
	predicate: () => boolean,
	label: string,
): Promise<void> {
	await bounded(
		label,
		(async () => {
			while (!predicate()) {
				await new Promise((resolve) => setTimeout(resolve, 1));
			}
		})(),
	);
}

function testCoreDependencies(
	overrides: {
		readonly contextLoader?: Partial<RuntimeCoreHostsOptions["contextLoader"]>;
		readonly threadLoop?: Partial<RuntimeCoreHostsOptions["threadLoop"]>;
	} = {},
): Pick<RuntimeCoreHostsOptions, "contextLoader" | "threadLoop"> {
	let nextCommittedMessageSequence = 1;
	let nextRuntimeID = 0;
	return {
		contextLoader: {
			loadThreadContext: async () => ({
				contextEntries: [],
				turnFacts: emptyTurnFacts,
				runtimeBindingToken: "runtime-binding-token-test",
			}),
			commitAcceptedInput: async (input) =>
				acceptedInputResult(input, nextCommittedMessageSequence++),
			...overrides.contextLoader,
		},
		threadLoop: {
			internalToolRepairStore: new RecordingMessageStore(),
			sessionEventWriter: testSessionEventWriter(),
			runtime: {
				now: () => "2026-06-16T00:00:00.000Z",
				monotonicMs: () => 0,
				createId: (prefix) => `${prefix}_${++nextRuntimeID}`,
				sleep: async (durationMs) => {
					if (durationMs >= 3_000) {
						return await new Promise<boolean>(() => undefined);
					}
					return true;
				},
			},
			llmService: {
				stream: () =>
					Stream.fromIterable([
						{ type: "text-start" as const, id: "text-default" },
						{
							type: "text-delta" as const,
							id: "text-default",
							text_delta: "ok",
						},
						{ type: "text-end" as const, id: "text-default" },
						{ type: "finish" as const, finishReason: "stop" as const },
					]),
			},
			storeOperationTimeoutMs: 100,
			runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
			runtimePolicy: () => ({
				toolCatalog: createToolCatalog({ family: "claude" }),
			}),
			...overrides.threadLoop,
		},
	};
}

function testSessionEventWriter(onFinishIdle?: () => void): SessionEventWriter {
	const writer = writerFrom(successfulEventAppend);
	return onFinishIdle === undefined
		? writer
		: {
				...writer,
				finishIdle: async (envelope) => {
					const result = await writer.finishIdle!(envelope);
					onFinishIdle();
					return result;
				},
			};
}

function writerFrom(
	append: (envelope: SessionEventEnvelope) => SessionEventWriterAppendResult,
): SessionEventWriter {
	let nextAssistantMessageSequence = 2;
	const appendWithAssignments = (
		envelope: SessionEventEnvelope,
	): SessionEventWriterAppendResult => {
		const result = append(envelope);
		if (!result.ok || result.type === "stale") return result;
		return {
			...result,
			...(!("assistant" in result)
				? {}
				: {
						assistant: {
							...result.assistant,
							messageSequence: nextAssistantMessageSequence++,
						},
					}),
		};
	};
	return {
		append: async (envelope) => appendWithAssignments(envelope),
		settleToolResult: async () => ({ ok: true, result: { type: "committed" } }),
		writeRequestEnd: async (envelope) => requestEndResult(envelope),
		finishIdle: async (envelope) => {
			const appended = appendWithAssignments({
				workspaceId: envelope.workspaceId,
				sessionId: envelope.sessionId,
				sessionThreadId: envelope.sessionThreadId,
				bindingId: envelope.bindingId,
				bindingGeneration: envelope.bindingGeneration,
				targetPodUid: envelope.targetPodUid,
				writeId: envelope.durableTurnId,
				event: {
					type: "session.status_idle",
					stop_reason: envelope.stopReason,
				},
			});
			if (!appended.ok) return appended;
			if (appended.type === "stale") return { ok: true, type: "stale" };
			return {
				ok: true,
				type: appended.type,
				idleEventId: `idle_${envelope.durableTurnId}`,
			};
		},
		commitRuntimeTermination: async (envelope) => ({
			ok: true,
			type: "committed",
			failureEventId: `failure_${envelope.writeId}`,
			closeoutEventId: `closeout_${envelope.writeId}`,
		}),
	};
}

class RecordingMessageStore extends RuntimeInternalToolRepairStore {
	protected async commitInternalToolRepairRecord(): Promise<never> {
		throw new Error("internal tool repair is not exercised by this host test");
	}
}

function controlCommitResult(
	_scope: RuntimeControlInputState,
	_declaration: RuntimeControlInputDeclaration,
	overrides: {
		readonly assignedContextSequences?: readonly number[];
		readonly interruptToolResults?: readonly RuntimeInterruptToolResult[];
	} = {},
): RuntimeControlInputCommitResult {
	return {
		ok: true,
		type: "committed",
		assignedContextSequences: overrides.assignedContextSequences ?? [],
		pendingAttachments: [],
		interruptToolResults: overrides.interruptToolResults ?? [],
	};
}

function requestEndResult(envelope: SessionEventWriterRequestEndEnvelope) {
	return {
		ok: true as const,
		type: "committed" as const,
		requestEndEventId: `evt_${envelope.writeId}`,
		outcome: { type: "ordinary" as const },
		interruptToolResults: [],
		pendingAttachments: [],
	};
}

function successfulEventAppend(
	envelope: SessionEventEnvelope,
): SessionEventWriterAppendResult {
	const eventId = `evt_${envelope.writeId}`;
	return {
		ok: true,
		type: "committed",
		eventId,
		...(envelope.assistantContextAppend === undefined
			? {}
			: {
					assistant: {
						messageSequence: 1,
						createdToolUseEventIds: envelope.assistantContextAppend.parts
							.filter((part) => part.type === "tool")
							.map(
								(_part, partIndex) =>
									`tool_use_${envelope.writeId}_${partIndex}`,
							),
					},
				}),
	};
}
