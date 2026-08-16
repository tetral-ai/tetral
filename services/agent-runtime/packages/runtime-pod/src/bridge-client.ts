/**
 * @packageDocumentation
 * Adapts Runtime Core persistence and lifecycle ports to authenticated Agent Runtime Bridge
 * gRPC calls. Runtime Pod composition constructs these adapters, and Runtime Core uses them to
 * load cold thread state, commit inputs and events, manage reviewer threads, repair internal tool
 * state, and refresh binding tokens. The adapters call the generated Bridge client and outbound
 * metadata factory, accept committed or duplicate acknowledgements as durable proof, validate
 * populated projected JSON before exposing it, and keep per-thread scope bookkeeping local and disposable.
 */

import type { CallOptions, Metadata, ServiceError } from "@grpc/grpc-js";
import { credentials, status } from "@grpc/grpc-js";
import type {
	AcceptedInputCommitResult,
	ContextLoader,
	RuntimeLoadedAgentMail,
	RuntimeLoadedPendingToolUse,
} from "@tetral/agent-runtime-core/src/context/context-loader.js";
import type {
	RuntimeAssistantContextAppend,
	RuntimeContextEntry,
	RuntimeInternalToolRepairCommit,
	RuntimeInternalToolRepairCommitResult,
	RuntimeInterruptToolResult,
	RuntimeOpenRequestDraft,
	RuntimeProviderAttachment,
	RuntimeToolSettlementDeclaration,
	SessionEventEnvelope,
	SessionEventWriter,
	SessionEventWriterAppendResult,
	SessionEventWriterFinishIdleEnvelope,
	SessionEventWriterFinishIdleResult,
	SessionEventWriterRequestEndEnvelope,
	SessionEventWriterRequestEndResult,
	SessionEventWriterRuntimeTerminationEnvelope,
	SessionEventWriterRuntimeTerminationResult,
	SessionEventWriterToolSettlementAttempt,
	SessionEventWriterToolSettlementEnvelope,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import {
	finalizeRuntimeToolOutput,
	normalizeContextLoaderError,
	normalizeRuntimeInternalToolRepairStoreError,
	normalizeSessionEventWriterError,
	RuntimeContextEntrySchema,
	RuntimeJsonValueSchema,
	RuntimeOpenRequestDraftSchema,
	RuntimeToolErrorSchema,
	runtimeToolErrorFromFailure,
	SessionEventWriterRetryPolicy,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { sessionEventForDurableWrite } from "@tetral/agent-runtime-core/src/runtime/session-event-writer.js";
import type { ThreadContextPrefix } from "@tetral/agent-runtime-core/src/session/context-manager.js";
import type { RuntimeThreadIdentity } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeAcceptedThreadMetadataState,
	RuntimeConfigPatchState,
	RuntimePreloadedSandboxExecutionState,
	RuntimeThreadAddressState,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import type { ThreadTurnLoadFacts } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { ThreadTurnLoadFactsSchema } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { MailFetchMaxEnvelopes } from "@tetral/agent-runtime-protocol/src/bounds.js";
import type {
	AdmitApprovalReviewInputRequest,
	AdmitApprovalReviewInputResponse,
	RuntimeInterruptToolResult as BridgeRuntimeInterruptToolResult,
	CloseApprovalReviewerRequest,
	CloseApprovalReviewerResponse,
	CommitInputsRequest,
	CommitInputsResponse,
	CommitInternalToolRepairRequest,
	CommitInternalToolRepairResponse,
	CommitRuntimeTerminationRequest,
	CommitRuntimeTerminationResponse,
	CommitTaskNotificationResultResponse,
	EnsureApprovalReviewerSidecarRequest,
	EnsureApprovalReviewerSidecarResponse,
	EnsureApprovalReviewerTrunkRequest,
	EnsureApprovalReviewerTrunkResponse,
	FinishIdleRequest,
	FinishIdleResponse,
	LoadContextRequest,
	LoadContextResponse,
	MarkChildThreadActiveResponse,
	ReadAgentMailRequest,
	ReadAgentMailResponse,
	RefreshRuntimeBindingTokenRequest,
	RefreshRuntimeBindingTokenResponse,
	RuntimeContextDelta,
	RuntimeScope,
	SettleToolResultRequest,
	SettleToolResultResponse,
	WriteEventRequest,
	WriteEventResponse,
	WriteRequestEndRequest,
	WriteRequestEndResponse,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
	AgentRuntimeBridgeServiceClient,
	WriteEventRequest as WriteEventRequestMessage,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
	ApprovalReviewerThreadCreation,
	RuntimeApprovalReviewerThreadCreator,
} from "./approval-reviewer.js";
import type { ServiceAccountTokenConfig } from "./auth.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import {
	bridgeDurableContextGrpcChannelOptions,
	grpcClientChannelOptions,
	MaxBridgeDurableContextGrpcMessageBytes,
} from "./bounds.js";
import type {
	RuntimeContextLoadParsePhase,
	RuntimeContextLoadParseReason,
	RuntimePodLogger,
} from "./logger.js";
import { recordContextLoadParseFailure } from "./logger.js";
import type { RuntimeControlInputCommitter } from "./runtime-service.js";

/** Configures the Bridge adapter that durably settles interrupt and tool-confirmation inputs. */
export interface BridgeAPIControlInputCommitterOptions {
	readonly address: string;
	readonly tokenPath: string;
	readonly metadataFactory?: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	readonly client?: AgentRuntimeBridgeServiceClient;
	readonly sleep?: (durationMs: number) => Promise<void>;
}

/**
 * Commits control inputs through Bridge before Runtime Core treats the control action as durable.
 * Authentication and transport failures remain retryable unless the gRPC status identifies a
 * deterministic request or idempotency conflict; a replay returns the same committed application.
 */
export class BridgeAPIControlInputCommitter
	implements RuntimeControlInputCommitter
{
	private readonly client: AgentRuntimeBridgeServiceClient;
	private readonly metadataFactory: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	private readonly sleep: (durationMs: number) => Promise<void>;

	constructor(private readonly options: BridgeAPIControlInputCommitterOptions) {
		this.client =
			options.client ??
			new AgentRuntimeBridgeServiceClient(
				options.address,
				credentials.createInsecure(),
				grpcClientChannelOptions(),
			);
		this.metadataFactory =
			options.metadataFactory ?? buildOutboundBearerMetadata;
		this.sleep =
			options.sleep ??
			(async (durationMs) =>
				await new Promise<void>((resolve) => setTimeout(resolve, durationMs)));
	}

	/** Commits one frozen interrupt or tool-confirmation declaration. */
	async commitControlInput(
		input: Parameters<RuntimeControlInputCommitter["commitControlInput"]>[0],
	) {
		let metadata: Metadata;
		try {
			metadata = await this.metadataFactory({
				tokenPath: this.options.tokenPath,
			});
		} catch {
			return {
				ok: false as const,
				retryable: true,
				errorCode: "bridge_token_unavailable",
				message: "control input durable commit failed",
			};
		}
		const request: CommitInputsRequest = {
			scope: bridgeScope(input.scope),
			runtimeInputId: input.scope.runtimeInputId,
			approvalReviewText: [],
		};
		let response: CommitInputsResponse | undefined;
		for (let attempt = 0; response === undefined; attempt += 1) {
			try {
				response = await commitInputs(this.client, request, metadata);
			} catch (error) {
				if (
					!bridgeDeclarationTransportUnknown(error) ||
					attempt >= SessionEventWriterRetryPolicy.backoffMs.length
				) {
					return {
						ok: false as const,
						retryable: bridgeCommitErrorRetryable(error),
						errorCode: "bridge_commit_unavailable",
						message: "control input durable commit failed",
					};
				}
				const backoffMs = SessionEventWriterRetryPolicy.backoffMs[attempt]!;
				await this.sleep(backoffMs);
			}
		}
		if (
			!exactlyOneDefined(response.committed, response.stale)
		) {
			return {
				ok: false as const,
				retryable: false,
				errorCode: "bridge_commit_rejected",
				message: "control input durable commit returned malformed outcome",
			};
		}
		if (response.stale !== undefined) {
			return { ok: true as const, type: "stale" as const };
		}
		const result = response.committed;
		if (result !== undefined) {
			if (!exactlyOneDefined(result.context, result.interrupt)) {
				return {
					ok: false as const,
					retryable: false,
					errorCode: "bridge_commit_rejected",
					message:
						"control input durable commit returned malformed application",
				};
			}
			const matchesKind =
				input.inputKind === "interrupt_control"
					? result.interrupt !== undefined
					: result.context !== undefined;
			if (!matchesKind)
				return {
					ok: false as const,
					retryable: false,
					errorCode: "bridge_commit_rejected",
					message:
						"control input durable commit returned mismatched application",
				};
			try {
				return {
					ok: true as const,
					type: "committed" as const,
					assignedContextSequences:
						result.context?.assignedContextSequences ?? [],
					pendingAttachments: parsePendingAttachmentJson(
						result.context?.pendingAttachmentJson ?? [],
					),
					interruptToolResults: parseInterruptToolResults(
						result.interrupt?.interruptToolResults ?? [],
					),
				};
			} catch {
				return {
					ok: false as const,
					retryable: false,
					errorCode: "bridge_commit_rejected",
					message:
						"control input durable commit returned malformed application",
				};
			}
		}
		return {
			ok: false as const,
			retryable: false,
			errorCode: "bridge_commit_rejected",
			message: "control input durable commit rejected",
		};
	}
}

/** Configures the Bridge adapter for dedicated background-task notification settlement. */
export interface BridgeAPITaskNotificationCommitterOptions {
	readonly address: string;
	readonly tokenPath: string;
	readonly metadataFactory?: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	readonly client?: AgentRuntimeBridgeServiceClient;
}

/**
 * Settles a loop-authored terminal background-task notification through Bridge. Bridge may park
 * closing custody or return stale/rejected durable outcomes; malformed results fail without retry.
 */
export class BridgeAPITaskNotificationCommitter {
	private readonly client: AgentRuntimeBridgeServiceClient;
	private readonly metadataFactory: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;

	constructor(
		private readonly options: BridgeAPITaskNotificationCommitterOptions,
	) {
		this.client =
			options.client ??
			new AgentRuntimeBridgeServiceClient(
				options.address,
				credentials.createInsecure(),
				grpcClientChannelOptions(),
			);
		this.metadataFactory =
			options.metadataFactory ?? buildOutboundBearerMetadata;
	}

	/** Commits one task notification and returns only its operation-specific durable result. */
	async commitTaskNotification(input: {
		readonly scope: RuntimeAcceptedInputState;
		readonly runtimeInputId: string;
	}) {
		let metadata: Metadata;
		try {
			metadata = await this.metadataFactory({
				tokenPath: this.options.tokenPath,
			});
		} catch {
			return {
				ok: false as const,
				retryable: true,
				errorCode: "bridge_token_unavailable",
				message: "task notification durable commit failed",
			};
		}
		const request = {
			scope: {
				workspaceId: input.scope.workspaceId,
				sessionId: input.scope.sessionId,
				sessionThreadId: input.scope.sessionThreadId,
				binding: {
					bindingId: input.scope.bindingId,
					bindingGeneration: input.scope.bindingGeneration,
					targetPodUid: input.scope.targetPodUid,
				},
			},
			runtimeInputId: input.runtimeInputId,
		};
		let response: CommitTaskNotificationResultResponse;
		try {
			response = await commitTaskNotificationResult(
				this.client,
				request,
				metadata,
			);
		} catch (error) {
			const code = grpcStatusCode(error);
			if (code === status.INVALID_ARGUMENT) {
				return {
					ok: false as const,
					retryable: false,
					errorCode: "task_notification_result_invalid",
					message: "task notification durable commit failed",
				};
			}
			if (code === status.ALREADY_EXISTS) {
				return {
					ok: true as const,
					rejected: true as const,
					errorCode: "task_notification_payload_mismatch" as const,
				};
			}
			return {
				ok: false as const,
				// A queued Inbox without Queue custody is a durable invariant violation;
				// live-lease contention and all other transport failures remain retryable.
				retryable: code !== status.FAILED_PRECONDITION,
				errorCode: "bridge_commit_unavailable",
				message: "task notification durable commit failed",
			};
		}
		if (
			!exactlyOneDefined(
				response.committed,
				response.stale,
				response.parked,
				response.rejected,
			)
		) {
			return {
				ok: false as const,
				retryable: true,
				errorCode: "bridge_task_notification_projection_invalid",
				message: "task notification durable commit returned malformed outcome",
			};
		}
		const committed = response.committed;
		if (committed !== undefined) {
			if (
				committed.assignedContextSequences.length !== 1 ||
				!Number.isSafeInteger(committed.assignedContextSequences[0]) ||
				committed.assignedContextSequences[0]! <= 0
			) {
				return {
					ok: false as const,
					retryable: false,
					errorCode: "bridge_task_notification_projection_invalid",
					message:
						"task notification durable commit returned malformed context assignment",
				};
			}
			return {
				ok: true as const,
				type: "committed" as const,
				assignedContextSequences: committed.assignedContextSequences,
			};
		}
		if (response.stale !== undefined) {
			return { ok: true as const, stale: true as const };
		}
		if (response.parked !== undefined) {
			return { ok: true as const, deferred: true as const };
		}
		if (response.rejected !== undefined) {
			return {
				ok: true as const,
				rejected: true as const,
				errorCode: "task_notification_result_invalid" as const,
			};
		}
		return {
			ok: false as const,
			retryable: false,
			errorCode: "bridge_commit_rejected",
			message: "task notification durable commit rejected",
		};
	}
}

function taskNotificationRejectionCode(
	errorCode: string,
): errorCode is
	| "task_notification_result_invalid"
	| "task_notification_message_invalid"
	| "task_notification_payload_mismatch" {
	return (
		errorCode === "task_notification_result_invalid" ||
		errorCode === "task_notification_message_invalid" ||
		errorCode === "task_notification_payload_mismatch"
	);
}

/** Configures Bridge-backed creation and closure of temporary approval-reviewer threads. */
export interface BridgeAPIApprovalReviewerThreadCreatorOptions {
	readonly address: string;
	readonly tokenPath: string;
	readonly metadataFactory?: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	readonly client?: AgentRuntimeBridgeServiceClient;
}

/**
 * Manages approval-reviewer child-thread rows through Bridge. The approval reviewer calls this
 * adapter around its execution.
 */
export class BridgeAPIApprovalReviewerThreadCreator
	implements RuntimeApprovalReviewerThreadCreator
{
	private readonly client: AgentRuntimeBridgeServiceClient;
	private readonly metadataFactory: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;

	constructor(
		private readonly options: BridgeAPIApprovalReviewerThreadCreatorOptions,
	) {
		this.client =
			options.client ??
			new AgentRuntimeBridgeServiceClient(
				options.address,
				credentials.createInsecure(),
				grpcClientChannelOptions(),
			);
		this.metadataFactory =
			options.metadataFactory ?? buildOutboundBearerMetadata;
	}

	/** Creates a prefixless trunk or a sidecar whose durable parent context is selected by Bridge. */
	async createApprovalReviewerThread(input: ApprovalReviewerThreadCreation) {
		let metadata: Metadata;
		try {
			metadata = await this.metadataFactory({
				tokenPath: this.options.tokenPath,
			});
		} catch {
			return {
				ok: false as const,
				message: "approval reviewer thread credential is unavailable",
			};
		}
		let response:
			| EnsureApprovalReviewerTrunkResponse
			| EnsureApprovalReviewerSidecarResponse;
		try {
			response = input.isTrunk
				? await ensureApprovalReviewerTrunk(
						this.client,
						{
							scope: approvalReviewerParentScope(input),
							ensureOperationId: input.ensureOperationId ?? "",
						},
						metadata,
					)
				: await ensureApprovalReviewerSidecar(
						this.client,
						{
							scope: approvalReviewerParentScope(input),
							reviewId: input.reviewId,
						},
						metadata,
					);
		} catch {
			return {
				ok: false as const,
				message: "approval reviewer thread creation is unavailable",
			};
		}
		if (!exactlyOneDefined(response.committed, response.duplicate)) {
			return {
				ok: false as const,
				message: "approval reviewer thread result was malformed",
			};
		}
		const outcome = response.committed ?? response.duplicate;
		const reviewerThreadId = outcome?.reviewerThreadId;
		if (reviewerThreadId === undefined || reviewerThreadId.length === 0) {
			return {
				ok: false as const,
				message: "approval reviewer thread was not acknowledged",
			};
		}
		let admitted: AdmitApprovalReviewInputResponse;
		try {
			admitted = await admitApprovalReviewInput(
				this.client,
				{
					scope: approvalReviewerParentScope(input),
					reviewerThreadId,
					reviewId: input.reviewId,
				},
				metadata,
			);
		} catch {
			return {
				ok: false as const,
				message: "approval reviewer input admission is unavailable",
			};
		}
		if (!exactlyOneDefined(admitted.committed, admitted.duplicate)) {
			return {
				ok: false as const,
				message: "approval reviewer input admission result was malformed",
			};
		}
		const runtimeInputId = (admitted.committed ?? admitted.duplicate)
			?.runtimeInputId;
		if (runtimeInputId === undefined || runtimeInputId.length === 0) {
			return {
				ok: false as const,
				message: "approval reviewer input admission result was malformed",
			};
		}
		return { ok: true as const, reviewerThreadId, runtimeInputId };
	}

	/** Closes the reviewer child thread and releases local scope only after Bridge acknowledges it. */
	async closeApprovalReviewerThread(input: ApprovalReviewerThreadCreation) {
		let metadata: Metadata;
		try {
			metadata = await this.metadataFactory({
				tokenPath: this.options.tokenPath,
			});
		} catch {
			return {
				ok: false as const,
				message: "approval reviewer thread credential is unavailable",
			};
		}
		let response: CloseApprovalReviewerResponse;
		try {
			response = await closeApprovalReviewer(
				this.client,
				{
					scope: approvalReviewerParentScope(input),
					reviewerThreadId: input.reviewerThreadId ?? "",
					reviewId: input.reviewId,
				},
				metadata,
			);
		} catch {
			return {
				ok: false as const,
				message: "approval reviewer thread close is unavailable",
			};
		}
		if (
			!exactlyOneDefined(response.committed, response.duplicate, response.stale)
		) {
			return {
				ok: false as const,
				message: "bridge_child_lifecycle_result_invalid",
			};
		}
		if (response.stale !== undefined) {
			return {
				ok: false as const,
				message: "scope_superseded",
				discardHotState: true,
			};
		}
		return { ok: true as const };
	}
}

/**
 * Configures Bridge context loading and binding-token refresh.
 */
export interface BridgeAPIContextLoaderOptions {
	readonly address: string;
	readonly tokenPath: string;
	readonly metadataFactory?: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	readonly client?: AgentRuntimeBridgeServiceClient;
	readonly logger?: RuntimePodLogger | undefined;
	readonly nowEpochMs?: () => number;
	readonly refreshMarginMs?: number;
	readonly sleep?: (durationMs: number) => Promise<void>;
}

const RuntimeBindingTokenRefreshPolicy = {
	attempts: 3,
	marginMs: 60_000,
	backoffMs: [100, 300],
} as const;

/**
 * Implements Runtime Core's authenticated Bridge boundary. It loads one complete cold baseline
 * for a thread residency and separately commits accepted-input declarations; each closed result
 * returns only operation-specific Bridge-assigned facts consumed by hot-state application and never
 * triggers another context load. Cold reads are
 * validated before any durable state is exposed to Runtime Core.
 * It coalesces concurrent binding-token refreshes for the same binding identity.
 */
export class BridgeAPIContextLoader implements ContextLoader {
	private readonly client: AgentRuntimeBridgeServiceClient;
	private readonly metadataFactory: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	private readonly bindingTokenRefreshes = new Map<string, Promise<string>>();
	private readonly nowEpochMs: () => number;
	private readonly refreshMarginMs: number;
	private readonly sleep: (durationMs: number) => Promise<void>;
	private readonly taskNotificationCommitter: BridgeAPITaskNotificationCommitter;

	constructor(private readonly options: BridgeAPIContextLoaderOptions) {
		this.client =
			options.client ??
			new AgentRuntimeBridgeServiceClient(
				options.address,
				credentials.createInsecure(),
				bridgeDurableContextGrpcChannelOptions(),
			);
		this.metadataFactory =
			options.metadataFactory ?? buildOutboundBearerMetadata;
		this.nowEpochMs = options.nowEpochMs ?? (() => Date.now());
		this.refreshMarginMs =
			options.refreshMarginMs ?? RuntimeBindingTokenRefreshPolicy.marginMs;
		this.sleep =
			options.sleep ??
			(async (durationMs) =>
				await new Promise<void>((resolve) => setTimeout(resolve, durationMs)));
		this.taskNotificationCommitter = new BridgeAPITaskNotificationCommitter(
			options,
		);
	}

	/**
	 * Returns a still-valid binding token or refreshes it through Bridge with bounded retries.
	 * Concurrent refreshes for the same thread and binding share one in-flight request.
	 */
	async refreshRuntimeBindingToken(
		identity: RuntimeThreadIdentity,
		options: { readonly force?: boolean | undefined } = {},
	): Promise<string> {
		if (
			options.force !== true &&
			!bindingTokenNeedsRefresh(
				identity.runtimeBindingToken,
				this.nowEpochMs(),
				this.refreshMarginMs,
			)
		) {
			return identity.runtimeBindingToken;
		}
		const key = bindingTokenRefreshKey(identity);
		const existing = this.bindingTokenRefreshes.get(key);
		if (existing !== undefined) {
			return await existing;
		}
		const pending = this.refreshRuntimeBindingTokenOnce(identity);
		this.bindingTokenRefreshes.set(key, pending);
		try {
			return await pending;
		} finally {
			if (this.bindingTokenRefreshes.get(key) === pending) {
				this.bindingTokenRefreshes.delete(key);
			}
		}
	}

	private async refreshRuntimeBindingTokenOnce(
		identity: RuntimeThreadIdentity,
	): Promise<string> {
		for (
			let attempt = 1;
			attempt <= RuntimeBindingTokenRefreshPolicy.attempts;
			attempt += 1
		) {
			try {
				const metadata = await this.metadataFactory({
					tokenPath: this.options.tokenPath,
				});
				const response = await refreshRuntimeBindingToken(
					this.client,
					{
						scope: bindingTokenRefreshScope(identity),
					},
					metadata,
				);
				if (response.runtimeBindingToken.length === 0) {
					throw new Error(
						"runtime binding token refresh returned an empty token",
					);
				}
				return response.runtimeBindingToken;
			} catch (error) {
				if (grpcStatusCode(error) === status.FAILED_PRECONDITION) {
					throw normalizeContextLoaderError({
						code: "superseded",
						sessionId: identity.sessionId,
						reason: "runtime binding custody is stale",
					});
				}
				if (
					!refreshRuntimeBindingTokenErrorRetryable(error) ||
					attempt === RuntimeBindingTokenRefreshPolicy.attempts
				) {
					throw normalizeContextLoaderError({
						code: "unavailable",
						sessionId: identity.sessionId,
						reason: "runtime binding token refresh failed",
					});
				}
				await this.sleep(
					RuntimeBindingTokenRefreshPolicy.backoffMs[attempt - 1] ?? 0,
				);
			}
		}
		throw normalizeContextLoaderError({
			code: "unavailable",
			sessionId: identity.sessionId,
			reason: "runtime binding token refresh failed",
		});
	}

	/** Loads and validates the complete cold-start projection for the supplied thread command. */
	async loadThreadContext(command: RuntimeThreadAddressState): Promise<{
		readonly contextEntries: readonly RuntimeContextEntry[];
		readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
		readonly turnFacts: ThreadTurnLoadFacts;
		readonly threadContextPrefix?: ThreadContextPrefix | undefined;
		readonly durableTurnId?: string | undefined;
		readonly runtimeBindingToken: string;
		readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
		readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
		readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
		readonly pendingToolUses?:
			| readonly RuntimeLoadedPendingToolUse[]
			| undefined;
		readonly pendingSandboxExecutions?:
			| readonly RuntimePreloadedSandboxExecutionState[]
			| undefined;
		readonly pendingAttachments?:
			| readonly RuntimeProviderAttachment[]
			| undefined;
		readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
	}> {
		return await this.loadContext(command);
	}

	/** Reads target-owned durable mail without mutating sender or Inbox state. */
	async readAgentMail(
		command: RuntimeThreadAddressState,
		sourceThreadId: string,
	): Promise<RuntimeLoadedAgentMail | undefined> {
		const response = await readAgentMail(
			this.client,
			{
				scope: bridgeScope(command),
				sourceThreadId,
			},
			await this.metadata(),
		);
		if (!exactlyOneDefined(response.found, response.empty)) {
			throw normalizeContextLoaderError({
				code: "schema_mismatch",
				sessionId: command.sessionId,
				reason: "agent mail reader returned an invalid result",
			});
		}
		if (response.empty !== undefined) {
			return undefined;
		}
		const found = response.found;
		if (
			found === undefined ||
			found.deliveryId.length === 0 ||
			found.content.length === 0
		) {
			throw normalizeContextLoaderError({
				code: "schema_mismatch",
				sessionId: command.sessionId,
				reason: "agent mail reader returned an invalid result",
			});
		}
		return {
			deliveryId: found.deliveryId,
			content: found.content,
		};
	}

	/**
	 * Commits accepted input and returns only operation-specific durable facts.
	 * Cold installation is the sole context read during one thread residency.
	 */
	async commitAcceptedInput(
		input: RuntimeAcceptedInputState,
		options?: { readonly approvalReviewText?: readonly string[] | undefined },
	): Promise<AcceptedInputCommitResult> {
		if (input.kind === "task_notification") {
			const result =
				await this.taskNotificationCommitter.commitTaskNotification({
					scope: input,
					runtimeInputId: input.runtimeInputId,
				});
			if (!result.ok) {
				throw normalizeContextLoaderError({
					code: result.retryable ? "unavailable" : "schema_mismatch",
					sessionId: input.sessionId,
					reason: result.errorCode,
				});
			}
			if ("deferred" in result) {
				return { type: "task_notification_deferred" };
			}
			if ("rejected" in result) {
				if (
					result.errorCode !== undefined &&
					taskNotificationRejectionCode(result.errorCode)
				) {
					return {
						type: "task_notification_rejected",
						errorCode: result.errorCode,
					};
				}
				throw normalizeContextLoaderError({
					code: "schema_mismatch",
					sessionId: input.sessionId,
					reason: "task notification rejection is invalid",
				});
			}
			if ("stale" in result) {
				return { type: "stale_custody" };
			}
			return {
				type: result.type,
				assignedContextSequences: result.assignedContextSequences,
				pendingAttachments: [],
				interruptToolResults: [],
			};
		}
		const metadata = await this.metadata();
		const scope = bridgeScope(input);
		const request = {
			scope,
			runtimeInputId: input.runtimeInputId,
			approvalReviewText:
				input.kind === "approval_review"
					? [...(options?.approvalReviewText ?? input.promptText)]
					: [],
		};
		let response: CommitInputsResponse;
		try {
			response = await commitInputs(this.client, request, metadata);
		} catch (error) {
			const code =
				typeof error === "object" && error !== null && "code" in error
					? (error as { readonly code?: unknown }).code
					: undefined;
			throw normalizeContextLoaderError({
				code:
					code === status.INVALID_ARGUMENT ||
					code === status.ALREADY_EXISTS ||
					code === status.FAILED_PRECONDITION
						? "schema_mismatch"
						: "unavailable",
				sessionId: input.sessionId,
				reason: "commit inputs transport failed",
			});
		}
		if (
			!exactlyOneDefined(response.committed, response.stale)
		) {
			throw normalizeContextLoaderError({
				code: "schema_mismatch",
				sessionId: input.sessionId,
				reason: "commit inputs returned malformed outcome",
			});
		}
		if (response.stale !== undefined) {
			return { type: "stale_custody" };
		}
		const result = response.committed;
		if (result === undefined) {
			throw normalizeContextLoaderError({
				code: "schema_mismatch",
				sessionId: input.sessionId,
				reason: "commit inputs returned no outcome",
			});
		}
		if (result.context === undefined || result.interrupt !== undefined) {
			throw normalizeContextLoaderError({
				code: "schema_mismatch",
				sessionId: input.sessionId,
				reason: "commit inputs returned a non-context application",
			});
		}
		const context = result.context;
		return {
			type: "committed",
			assignedContextSequences: context?.assignedContextSequences ?? [],
			pendingAttachments:
				parsePendingAttachments(
					(context?.pendingAttachmentJson ?? []).map((item) =>
						JSON.parse(item),
					),
				) ?? [],
			interruptToolResults: [],
		};
	}

	private async loadContext(input: RuntimeThreadAddressState): Promise<{
		readonly contextEntries: readonly RuntimeContextEntry[];
		readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
		readonly turnFacts: ThreadTurnLoadFacts;
		readonly threadContextPrefix?: ThreadContextPrefix | undefined;
		readonly durableTurnId?: string | undefined;
		readonly runtimeBindingToken: string;
		readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
		readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
		readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
		readonly pendingToolUses?:
			| readonly RuntimeLoadedPendingToolUse[]
			| undefined;
		readonly pendingSandboxExecutions?:
			| readonly RuntimePreloadedSandboxExecutionState[]
			| undefined;
		readonly pendingAttachments?:
			| readonly RuntimeProviderAttachment[]
			| undefined;
		readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
	}> {
		const metadata = await this.metadata();
		const response = await loadContext(
			this.client,
			{
				scope: bridgeScope(input),
			},
			metadata,
		);
		const parsed = parseContextPayload(
			response.contextJson,
			input,
			this.options.logger,
		);
		return { ...parsed, runtimeBindingToken: response.runtimeBindingToken };
	}

	private async metadata(): Promise<Metadata> {
		return await this.metadataFactory({ tokenPath: this.options.tokenPath });
	}
}

/**
 * Configures the Bridge event writer.
 */
export interface BridgeAPIEventWriterOptions {
	readonly address: string;
	readonly tokenPath: string;
	readonly metadataFactory?: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	readonly client?: AgentRuntimeBridgeServiceClient;
	readonly sleep?: ((durationMs: number) => Promise<void>) | undefined;
}

/**
 * Persists Runtime Core semantic events, request ends, idle transitions, and runtime termination
 * through Bridge. Each write carries its thread scope and requires a committed or duplicate ACK;
 * rejected ACKs preserve closeout release sentinels; reviewer-outcome contract
 * rejections are deterministic and stop the shared writer retry policy.
 */
export class BridgeAPIEventWriter implements SessionEventWriter {
	private readonly client: AgentRuntimeBridgeServiceClient;
	private readonly metadataFactory: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	private readonly sleep: (durationMs: number) => Promise<void>;

	constructor(private readonly options: BridgeAPIEventWriterOptions) {
		this.client =
			options.client ??
			new AgentRuntimeBridgeServiceClient(
				options.address,
				credentials.createInsecure(),
				bridgeDurableContextGrpcChannelOptions(),
			);
		this.metadataFactory =
			options.metadataFactory ?? buildOutboundBearerMetadata;
		this.sleep =
			options.sleep ??
			(async (durationMs) =>
				await new Promise<void>((resolve) => setTimeout(resolve, durationMs)));
	}

	/** Writes one semantic event and its operation-specific durable projection. */
	async append(
		envelope: SessionEventEnvelope,
	): Promise<SessionEventWriterAppendResult> {
		const replayUnknownTransport =
			envelope.event.type === "approval_review.failure";
		try {
			const event = sessionEventForDurableWrite(envelope.event);
			const request: WriteEventRequest = {
				scope: bridgeScope(envelope),
				runtimeWriteId: envelope.writeId,
				modelRequestId:
					envelope.modelRequestId ?? modelRequestIdForEvent(event),
				eventType: event.type,
				payloadJson: JSON.stringify(event),
				sessionVisible: false,
				contextThroughMessageSequence: envelope.contextThroughMessageSequence,
				requestKind: envelope.requestKind ?? "",
				assistantContextDelta:
					envelope.assistantContextAppend === undefined
						? undefined
						: runtimeContextDeltaForBridge(envelope.assistantContextAppend),
			};
			if (
				WriteEventRequestMessage.encode(request).finish().byteLength >
				MaxBridgeDurableContextGrpcMessageBytes
			) {
				return eventWriterOperationSchemaFailure(
					envelope.sessionId,
					envelope.writeId,
				);
			}
			const metadata = await this.metadataFactory({
				tokenPath: this.options.tokenPath,
			});
			const response = await writeEvent(this.client, request, metadata);
			if (
				!exactlyOneDefined(
					response.committed,
					response.duplicate,
					response.stale,
				)
			) {
				return eventWriterOperationSchemaFailure(
					envelope.sessionId,
					envelope.writeId,
				);
			}
			if (response.stale !== undefined) {
				return { ok: true, type: "stale" };
			}
			const result = response.committed ?? response.duplicate;
			const expectedToolUseEventIds =
				envelope.assistantContextAppend?.parts.filter(
					(part) => part.type === "tool",
				).length ?? 0;
			if (
				result === undefined ||
				result.eventId.length === 0 ||
				(envelope.assistantContextAppend === undefined &&
					(result.assignedMessageSequence !== undefined ||
						result.createdToolUseEventIds.length !== 0)) ||
				(envelope.assistantContextAppend !== undefined &&
					(result.assignedMessageSequence === undefined ||
						!Number.isSafeInteger(result.assignedMessageSequence) ||
						result.assignedMessageSequence <= 0 ||
						result.createdToolUseEventIds.length !== expectedToolUseEventIds ||
						result.createdToolUseEventIds.some(
							(eventId) => eventId.length === 0,
						) ||
						new Set(result.createdToolUseEventIds).size !==
							result.createdToolUseEventIds.length))
			) {
				return eventWriterOperationSchemaFailure(
					envelope.sessionId,
					envelope.writeId,
				);
			}
			return {
				ok: true,
				type: response.duplicate === undefined ? "committed" : "duplicate",
				eventId: result.eventId,
				...(result.assignedMessageSequence === undefined
					? {}
					: {
							assistant: {
								messageSequence: result.assignedMessageSequence,
								createdToolUseEventIds: result.createdToolUseEventIds,
							},
						}),
			};
		} catch (error) {
			return eventWriterTransportFailure(
				envelope.sessionId,
				envelope.writeId,
				error,
				replayUnknownTransport,
			);
		}
	}

	/** Settles one durable Tool target without returning payload, Event, or time echoes. */
	async settleToolResult(
		envelope: SessionEventWriterToolSettlementEnvelope,
	): Promise<SessionEventWriterToolSettlementAttempt> {
		const request: SettleToolResultRequest = {
			scope: bridgeScope(envelope),
			settlement: runtimeToolSettlementForBridge(
				envelope.settlement.toolUseEventId,
				envelope.settlement.outcome,
			),
		};
		let metadata: Metadata;
		try {
			metadata = await this.metadataFactory({
				tokenPath: this.options.tokenPath,
			});
		} catch (error) {
			return toolSettlementTransportFailure(envelope, error);
		}
		for (let attempt = 0; ; attempt += 1) {
			try {
				return parseToolSettlementResponse(
					await settleToolResult(this.client, request, metadata),
					envelope,
				);
			} catch (error) {
				if (
					!bridgeDeclarationTransportUnknown(error) ||
					attempt >= SessionEventWriterRetryPolicy.backoffMs.length
				) {
					return toolSettlementTransportFailure(envelope, error);
				}
				await this.sleep(SessionEventWriterRetryPolicy.backoffMs[attempt]!);
			}
		}
	}

	/**
	 * Writes a terminal model-request record, assistant seal, normalized usage, consumed
	 * attachments, optional reasoning, and optional reschedule request, then validates the closed result.
	 */
	async writeRequestEnd(
		envelope: SessionEventWriterRequestEndEnvelope,
	): Promise<SessionEventWriterRequestEndResult> {
		try {
			const metadata = await this.metadataFactory({
				tokenPath: this.options.tokenPath,
			});
			const inputUncachedTokens = envelope.usage?.inputTokens ?? 0;
			const cacheReadTokens = envelope.usage?.cacheReadTokens ?? 0;
			const cacheWriteTokens = envelope.usage?.cacheWriteTokens ?? 0;
			const inputTokens =
				inputUncachedTokens + cacheReadTokens + cacheWriteTokens;
			const outputTokens = envelope.usage?.outputTokens ?? 0;
			const request: WriteRequestEndRequest = {
				scope: bridgeScope(envelope),
				runtimeWriteId: envelope.writeId,
				modelRequestId: envelope.modelRequestId,
				finishReason: envelope.finishReason,
				isError: envelope.isError,
				errorKind: envelope.errorKind ?? "",
				consumedAttachmentRefs: [...(envelope.consumedAttachmentRefs ?? [])],
				consumedFileAttachments: (envelope.consumedFileAttachments ?? []).map(
					(attachment) => ({
						sourceEventId: attachment.sourceEventId,
						fileId: attachment.fileId,
					}),
				),
				reschedule:
					envelope.reschedule === undefined
						? undefined
						: {
								attempt: envelope.reschedule.attempt,
								deadline: envelope.reschedule.deadline,
								backoffMs: envelope.reschedule.backoffMs,
							},
				usageJson: JSON.stringify({
					input_tokens: inputTokens,
					cache_read_input_tokens: cacheReadTokens,
					cache_creation_input_tokens: cacheWriteTokens,
					output_tokens: outputTokens,
					reasoning_output_tokens: envelope.usage?.reasoningTokens ?? 0,
					total_tokens:
						envelope.usage?.totalTokens ?? inputTokens + outputTokens,
					provider_usage_json: envelope.usage?.providerUsageJson ?? "{}",
				}),
				trailingContextDelta:
					envelope.trailingContextAppend === undefined
						? undefined
						: runtimeContextDeltaForBridge(envelope.trailingContextAppend),
				compactionContext:
					envelope.compactionContext === undefined
						? undefined
						: runtimeSealedContextDeltaForBridge(envelope.compactionContext),
				prefixConsumption:
					envelope.prefixConsumption === undefined
						? undefined
						: {
								childThreadId: envelope.prefixConsumption.childThreadId,
								parentBoundaryEventId:
									envelope.prefixConsumption.parentBoundaryEventId,
							},
				compactedThroughMessageSequence:
					envelope.compactedThroughMessageSequence,
				compactionEventPayloadJson: envelope.compactionEventPayloadJson ?? "",
				interruptSettlement:
					envelope.interruptSettlement === undefined
						? undefined
						: { runtimeInputId: envelope.interruptSettlement.runtimeInputId },
			};
			const response = await writeRequestEnd(this.client, request, metadata);
			if (
				!exactlyOneDefined(
					response.committed,
					response.duplicate,
					response.stale,
				)
			) {
				return eventWriterOperationSchemaFailure(
					envelope.sessionId,
					envelope.writeId,
				);
			}
			if (response.stale !== undefined) return { ok: true, type: "stale" };
			const result = response.committed ?? response.duplicate;
			if (
				result === undefined ||
				result.requestEndEventId.length === 0 ||
				!exactlyOneDefined(
					result.ordinary,
					result.rescheduled,
					result.compacted,
				)
			) {
				return eventWriterOperationSchemaFailure(
					envelope.sessionId,
					envelope.writeId,
				);
			}
			const outcome =
				result.ordinary !== undefined
					? result.ordinary.sealedMessageSequence === undefined
						? { type: "ordinary" as const }
						: Number.isSafeInteger(result.ordinary.sealedMessageSequence) &&
								result.ordinary.sealedMessageSequence > 0
							? {
									type: "ordinary" as const,
									sealedMessageSequence: result.ordinary.sealedMessageSequence,
								}
							: undefined
					: result.rescheduled !== undefined
						? result.rescheduled.effectiveDeadline.length > 0 &&
							Number.isFinite(Date.parse(result.rescheduled.effectiveDeadline))
							? {
									type: "rescheduled" as const,
									effectiveDeadline: result.rescheduled.effectiveDeadline,
								}
							: undefined
						: result.compacted !== undefined &&
								result.compacted.compactionEventId.length > 0 &&
								Number.isSafeInteger(
									result.compacted.checkpointMessageSequence,
								) &&
								result.compacted.checkpointMessageSequence > 0
							? {
									type: "compacted" as const,
									compactionEventId: result.compacted.compactionEventId,
									checkpointMessageSequence:
										result.compacted.checkpointMessageSequence,
								}
							: undefined;
			let interruptToolResults: readonly RuntimeInterruptToolResult[];
			let pendingAttachments: readonly RuntimeProviderAttachment[];
			try {
				interruptToolResults = parseInterruptToolResults(
					result.interruptToolResults,
				);
				pendingAttachments = parsePendingAttachmentJson(
					result.pendingAttachmentJson,
				);
			} catch {
				return eventWriterOperationSchemaFailure(
					envelope.sessionId,
					envelope.writeId,
				);
			}
			const expectedOutcome =
				envelope.reschedule !== undefined
					? "rescheduled"
					: envelope.compactionContext !== undefined && !envelope.isError
						? "compacted"
						: "ordinary";
			if (outcome === undefined || outcome.type !== expectedOutcome) {
				return eventWriterOperationSchemaFailure(
					envelope.sessionId,
					envelope.writeId,
				);
			}
			return {
				ok: true,
				type: response.duplicate === undefined ? "committed" : "duplicate",
				requestEndEventId: result.requestEndEventId,
				outcome,
				interruptToolResults,
				pendingAttachments,
			};
		} catch (error) {
			const grpcCode =
				typeof error === "object" &&
				error !== null &&
				"code" in error &&
				typeof error.code === "number"
					? error.code
					: undefined;
			return grpcCode === status.ALREADY_EXISTS
				? {
						ok: false,
						error: normalizeSessionEventWriterError({
							code: "superseded",
							sessionId: envelope.sessionId,
							writeId: envelope.writeId,
						}),
					}
				: eventWriterOperationTransportFailure(
						envelope.sessionId,
						envelope.writeId,
						error,
					);
		}
	}

	/** Persists one database-named running interval's idle closeout. */
	async finishIdle(
		envelope: SessionEventWriterFinishIdleEnvelope,
	): Promise<SessionEventWriterFinishIdleResult> {
		try {
			const metadata = await this.metadataFactory({
				tokenPath: this.options.tokenPath,
			});
			const request: FinishIdleRequest = {
				scope: bridgeScope(envelope),
				durableTurnId: envelope.durableTurnId,
				stopReasonJson: JSON.stringify(envelope.stopReason),
				completionMailText: envelope.completionMailText,
			};
			const response = await finishIdle(this.client, request, metadata);
			if (
				!exactlyOneDefined(
					response.committed,
					response.duplicate,
					response.stale,
				)
			) {
				return eventWriterOperationSchemaFailure(
					envelope.sessionId,
					envelope.durableTurnId,
				);
			}
			if (response.stale !== undefined) return { ok: true, type: "stale" };
			const result = response.committed ?? response.duplicate;
			if (result === undefined || result.idleEventId.length === 0) {
				return eventWriterOperationSchemaFailure(
					envelope.sessionId,
					envelope.durableTurnId,
				);
			}
			return {
				ok: true,
				type: response.duplicate === undefined ? "committed" : "duplicate",
				idleEventId: result.idleEventId,
			};
		} catch (error) {
			return eventWriterOperationTransportFailure(
				envelope.sessionId,
				envelope.durableTurnId,
				error,
			);
		}
	}

	/** Commits atomic runtime termination closeout for the active thread scope. */
	async commitRuntimeTermination(
		envelope: SessionEventWriterRuntimeTerminationEnvelope,
	): Promise<SessionEventWriterRuntimeTerminationResult> {
		try {
			const metadata = await this.metadataFactory({
				tokenPath: this.options.tokenPath,
			});
			const request: CommitRuntimeTerminationRequest = {
				scope: bridgeScope(envelope),
				runtimeWriteId: envelope.writeId,
				failureJson: JSON.stringify(envelope.failure),
			};
			const response = await commitRuntimeTermination(
				this.client,
				request,
				metadata,
			);
			return parseRuntimeTerminationResult(response);
		} catch (error) {
			if (error instanceof RuntimeTerminationResultContractError) {
				return {
					ok: false,
					error: normalizeSessionEventWriterError({
						code: "schema_mismatch",
						sessionId: envelope.sessionId,
						writeId: envelope.writeId,
					}),
				};
			}
			const failure = eventWriterTransportFailure(
				envelope.sessionId,
				envelope.writeId,
				error,
			);
			return failure.ok
				? {
						ok: false,
						error: normalizeSessionEventWriterError({
							code: "unknown",
							sessionId: envelope.sessionId,
							writeId: envelope.writeId,
						}),
					}
				: failure;
		}
	}
}

/** Configures durable internal-tool repair writes against active thread binding scopes. */
export interface BridgeAPIInternalToolRepairCommitterOptions {
	readonly address: string;
	readonly tokenPath: string;
	readonly metadataFactory?: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;
	readonly client?: AgentRuntimeBridgeServiceClient;
}

/**
 * Persists Runtime Core's self-contained invalid-tool repair through Bridge. It maps missing scope,
 * authentication, transport, and conflicting ACKs into the message-store result vocabulary.
 */
export class BridgeAPIInternalToolRepairCommitter {
	private readonly client: AgentRuntimeBridgeServiceClient;
	private readonly metadataFactory: (
		config: ServiceAccountTokenConfig,
	) => Promise<Metadata>;

	constructor(
		private readonly options: BridgeAPIInternalToolRepairCommitterOptions,
	) {
		this.client =
			options.client ??
			new AgentRuntimeBridgeServiceClient(
				options.address,
				credentials.createInsecure(),
				grpcClientChannelOptions(),
			);
		this.metadataFactory =
			options.metadataFactory ?? buildOutboundBearerMetadata;
	}

	/** Commits one terminal invalid-tool repair and returns only newly assigned durable facts. */
	async commitInternalToolRepair(
		repair: RuntimeInternalToolRepairCommit,
	): Promise<RuntimeInternalToolRepairCommitResult> {
		let metadata: Metadata;
		try {
			metadata = await this.metadataFactory({
				tokenPath: this.options.tokenPath,
			});
		} catch {
			return internalToolRepairStoreFailure("unavailable", repair.sessionId);
		}
		const request: CommitInternalToolRepairRequest = {
			scope: bridgeScope(repair),
			modelRequestId: repair.modelRequestId,
			modelToolCallId: repair.modelToolCallId,
			toolName: repair.toolName,
			canonicalInputJson: JSON.stringify(repair.canonicalInput),
			error: {
				errorJson: JSON.stringify(repair.error),
				serverToolUse: undefined,
			},
			repairKey: repair.repairKey,
		};
		let response: CommitInternalToolRepairResponse;
		try {
			response = await commitInternalToolRepair(this.client, request, metadata);
		} catch (error) {
			return internalToolRepairStoreFailure(
				bridgeStoreErrorCode(error),
				repair.sessionId,
			);
		}
		if (
			!exactlyOneDefined(response.committed, response.duplicate, response.stale)
		) {
			return internalToolRepairStoreFailure("unavailable", repair.sessionId);
		}
		if (response.stale !== undefined) return { ok: true, type: "stale" };
		const result = response.committed ?? response.duplicate;
		if (
			result === undefined ||
			result.repairEventId.length === 0 ||
			!Number.isSafeInteger(result.assignedMessageSequence) ||
			result.assignedMessageSequence <= 0
		) {
			return internalToolRepairStoreFailure("unavailable", repair.sessionId);
		}
		return {
			ok: true,
			type: response.duplicate === undefined ? "committed" : "duplicate",
			repairEventId: result.repairEventId,
			assignedMessageSequence: result.assignedMessageSequence,
		};
	}
}

function commitTaskNotificationResult(
	client: AgentRuntimeBridgeServiceClient,
	request: Parameters<
		AgentRuntimeBridgeServiceClient["commitTaskNotificationResult"]
	>[0],
	metadata: Metadata,
): Promise<CommitTaskNotificationResultResponse> {
	return new Promise((resolve, reject) => {
		client.commitTaskNotificationResult(
			request,
			metadata,
			(
				error: ServiceError | null,
				response: CommitTaskNotificationResultResponse,
			) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function commitInternalToolRepair(
	client: AgentRuntimeBridgeServiceClient,
	request: CommitInternalToolRepairRequest,
	metadata: Metadata,
): Promise<CommitInternalToolRepairResponse> {
	return new Promise((resolve, reject) => {
		client.commitInternalToolRepair(
			request,
			metadata,
			(
				error: ServiceError | null,
				response: CommitInternalToolRepairResponse,
			) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function commitInputs(
	client: AgentRuntimeBridgeServiceClient,
	request: CommitInputsRequest,
	metadata: Metadata,
): Promise<CommitInputsResponse> {
	return new Promise((resolve, reject) => {
		const options: CallOptions = {
			deadline: Date.now() + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
		};
		client.commitInputs(
			request,
			metadata,
			options,
			(error: ServiceError | null, response: CommitInputsResponse) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function ensureApprovalReviewerTrunk(
	client: AgentRuntimeBridgeServiceClient,
	request: EnsureApprovalReviewerTrunkRequest,
	metadata: Metadata,
): Promise<EnsureApprovalReviewerTrunkResponse> {
	return new Promise((resolve, reject) => {
		client.ensureApprovalReviewerTrunk(
			request,
			metadata,
			(
				error: ServiceError | null,
				response: EnsureApprovalReviewerTrunkResponse,
			) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function ensureApprovalReviewerSidecar(
	client: AgentRuntimeBridgeServiceClient,
	request: EnsureApprovalReviewerSidecarRequest,
	metadata: Metadata,
): Promise<EnsureApprovalReviewerSidecarResponse> {
	return new Promise((resolve, reject) => {
		client.ensureApprovalReviewerSidecar(
			request,
			metadata,
			(
				error: ServiceError | null,
				response: EnsureApprovalReviewerSidecarResponse,
			) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function admitApprovalReviewInput(
	client: AgentRuntimeBridgeServiceClient,
	request: AdmitApprovalReviewInputRequest,
	metadata: Metadata,
): Promise<AdmitApprovalReviewInputResponse> {
	return new Promise((resolve, reject) => {
		client.admitApprovalReviewInput(
			request,
			metadata,
			(
				error: ServiceError | null,
				response: AdmitApprovalReviewInputResponse,
			) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function closeApprovalReviewer(
	client: AgentRuntimeBridgeServiceClient,
	request: CloseApprovalReviewerRequest,
	metadata: Metadata,
): Promise<CloseApprovalReviewerResponse> {
	return new Promise((resolve, reject) => {
		client.closeApprovalReviewer(
			request,
			metadata,
			(error: ServiceError | null, response: CloseApprovalReviewerResponse) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function readAgentMail(
	client: AgentRuntimeBridgeServiceClient,
	request: ReadAgentMailRequest,
	metadata: Metadata,
): Promise<ReadAgentMailResponse> {
	return new Promise((resolve, reject) => {
		client.readAgentMail(
			request,
			metadata,
			(error: ServiceError | null, response: ReadAgentMailResponse) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function loadContext(
	client: AgentRuntimeBridgeServiceClient,
	request: LoadContextRequest,
	metadata: Metadata,
): Promise<LoadContextResponse> {
	return new Promise((resolve, reject) => {
		client.loadContext(
			request,
			metadata,
			(error: ServiceError | null, response: LoadContextResponse) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function runtimeContextDeltaForBridge(
	append: RuntimeAssistantContextAppend,
): RuntimeContextDelta {
	return {
		parts: append.parts.map((part) => {
			if (part.type === "text") return { text: { text: part.text } };
			if (part.type === "reasoning") {
				return {
					reasoning: {
						text: part.text,
						...(part.providerMetadata === undefined
							? {}
							: {
									providerMetadataJson: JSON.stringify(part.providerMetadata),
								}),
					},
				};
			}
			if (!("input" in part.state) || part.state.input === undefined)
				throw new Error("assistant Tool call has no canonical input");
			return {
				toolCall: {
					modelToolCallId: part.modelToolCallId,
					toolName: part.toolName,
					canonicalInputJson: JSON.stringify(part.state.input.value),
				},
			};
		}),
	};
}

function runtimeSealedContextDeltaForBridge(context: {
	readonly parts: readonly RuntimeContextEntry["parts"][number][];
}): RuntimeContextDelta {
	return {
		parts: context.parts.map((part) => {
			switch (part.type) {
				case "text":
					return { text: { text: part.text } };
				case "reasoning":
					return {
						reasoning: {
							text: part.text,
							...(part.providerMetadata === undefined
								? {}
								: {
										providerMetadataJson: JSON.stringify(part.providerMetadata),
									}),
						},
					};
				case "tool_call":
					return {
						toolCall: {
							modelToolCallId: part.modelToolCallId,
							toolName: part.toolName,
							canonicalInputJson: JSON.stringify(part.canonicalInput),
						},
					};
				case "tool_result": {
					const result =
						part.result.type === "completed"
							? {
									completed: {
										outputJson: JSON.stringify(part.result.output),
										serverToolUse: undefined,
									},
								}
							: part.result.type === "error"
								? {
										error: {
											errorJson: JSON.stringify(part.result.error),
											serverToolUse: undefined,
										},
									}
								: {
										cancelled: {},
									};
					return {
						toolResult: { modelToolCallId: part.modelToolCallId, ...result },
					};
				}
			}
		}),
	};
}

function runtimeToolSettlementForBridge(
	toolUseEventId: string,
	settlement: RuntimeToolSettlementDeclaration["outcome"],
) {
	switch (settlement.type) {
		case "completed":
			const output = finalizeRuntimeToolOutput(settlement.output);
			return {
				toolUseEventId,
				completed: {
					outputJson: JSON.stringify(output),
					serverToolUse: settlement.serverToolUse,
				},
			};
		case "error":
			return {
				toolUseEventId,
				error: {
					errorJson: JSON.stringify(
						runtimeToolErrorFromFailure(settlement.error),
					),
					serverToolUse: settlement.serverToolUse,
				},
			};
		case "cancelled":
			return {
				toolUseEventId,
				cancelled:
					settlement.error === undefined
						? {}
						: {
								errorJson: JSON.stringify(
									runtimeToolErrorFromFailure(settlement.error),
								),
							},
			};
	}
}

function writeEvent(
	client: AgentRuntimeBridgeServiceClient,
	request: WriteEventRequest,
	metadata: Metadata,
): Promise<WriteEventResponse> {
	return new Promise((resolve, reject) => {
		client.writeEvent(
			request,
			metadata,
			(error: ServiceError | null, response: WriteEventResponse) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function settleToolResult(
	client: AgentRuntimeBridgeServiceClient,
	request: SettleToolResultRequest,
	metadata: Metadata,
): Promise<SettleToolResultResponse> {
	return new Promise((resolve, reject) => {
		const options: CallOptions = {
			deadline: Date.now() + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
		};
		client.settleToolResult(
			request,
			metadata,
			options,
			(error: ServiceError | null, response: SettleToolResultResponse) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function writeRequestEnd(
	client: AgentRuntimeBridgeServiceClient,
	request: WriteRequestEndRequest,
	metadata: Metadata,
): Promise<WriteRequestEndResponse> {
	return new Promise((resolve, reject) => {
		client.writeRequestEnd(
			request,
			metadata,
			(error: ServiceError | null, response: WriteRequestEndResponse) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function finishIdle(
	client: AgentRuntimeBridgeServiceClient,
	request: FinishIdleRequest,
	metadata: Metadata,
): Promise<FinishIdleResponse> {
	return new Promise((resolve, reject) => {
		client.finishIdle(
			request,
			metadata,
			(error: ServiceError | null, response: FinishIdleResponse) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function commitRuntimeTermination(
	client: AgentRuntimeBridgeServiceClient,
	request: CommitRuntimeTerminationRequest,
	metadata: Metadata,
): Promise<CommitRuntimeTerminationResponse> {
	return new Promise((resolve, reject) => {
		client.commitRuntimeTermination(
			request,
			metadata,
			(
				error: ServiceError | null,
				response: CommitRuntimeTerminationResponse,
			) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function refreshRuntimeBindingToken(
	client: AgentRuntimeBridgeServiceClient,
	request: RefreshRuntimeBindingTokenRequest,
	metadata: Metadata,
): Promise<RefreshRuntimeBindingTokenResponse> {
	return new Promise((resolve, reject) => {
		client.refreshRuntimeBindingToken(
			request,
			metadata,
			(
				error: ServiceError | null,
				response: RefreshRuntimeBindingTokenResponse,
			) => {
				if (error !== null) {
					reject(error);
					return;
				}
				resolve(response);
			},
		);
	});
}

function bridgeScope(input: {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
}): RuntimeScope {
	return {
		workspaceId: input.workspaceId,
		sessionId: input.sessionId,
		sessionThreadId: input.sessionThreadId,
		binding: {
			bindingId: input.bindingId,
			bindingGeneration: input.bindingGeneration,
			targetPodUid: input.targetPodUid,
		},
	};
}

function bindingTokenRefreshScope(
	identity: RuntimeThreadIdentity,
): RuntimeScope {
	return {
		workspaceId: identity.workspaceId,
		sessionId: identity.sessionId,
		sessionThreadId: identity.sessionThreadId,
		binding: {
			bindingId: identity.bindingId,
			bindingGeneration: identity.bindingGeneration,
			targetPodUid: identity.targetPodUid,
		},
	};
}

function bindingTokenRefreshKey(identity: RuntimeThreadIdentity): string {
	return [
		identity.workspaceId,
		identity.sessionId,
		identity.sessionThreadId,
		identity.bindingId,
		identity.bindingGeneration,
		identity.targetPodUid,
	].join("\u0000");
}

function bindingTokenNeedsRefresh(
	token: string,
	nowEpochMs: number,
	marginMs: number,
): boolean {
	const payload = token.split(".")[1];
	if (payload === undefined) {
		return true;
	}
	try {
		const parsed = JSON.parse(
			Buffer.from(payload, "base64url").toString("utf8"),
		) as unknown;
		if (
			!isRecord(parsed) ||
			typeof parsed.exp !== "number" ||
			!Number.isFinite(parsed.exp)
		) {
			return true;
		}
		return parsed.exp * 1_000 - nowEpochMs <= Math.max(0, marginMs);
	} catch {
		return true;
	}
}

function refreshRuntimeBindingTokenErrorRetryable(error: unknown): boolean {
	const code = grpcStatusCode(error);
	return code === status.UNAVAILABLE || code === status.DEADLINE_EXCEEDED;
}

function grpcStatusCode(error: unknown): unknown {
	return typeof error === "object" && error !== null && "code" in error
		? (error as { readonly code?: unknown }).code
		: undefined;
}

function approvalReviewerParentScope(
	input: ApprovalReviewerThreadCreation,
): RuntimeScope {
	return {
		workspaceId: input.request.workspaceId,
		sessionId: input.request.sessionId,
		sessionThreadId: input.request.sessionThreadId,
		binding: {
			bindingId: input.request.bindingId,
			bindingGeneration: input.request.bindingGeneration,
			targetPodUid: input.request.targetPodUid,
		},
	};
}

function exactlyOneDefined(...values: readonly unknown[]): boolean {
	return values.filter((value) => value !== undefined).length === 1;
}

function bridgeCommitErrorRetryable(error: unknown): boolean {
	const code =
		typeof error === "object" && error !== null && "code" in error
			? (error as { readonly code?: unknown }).code
			: undefined;
	return code !== status.INVALID_ARGUMENT && code !== status.ALREADY_EXISTS;
}

function bridgeDeclarationTransportUnknown(error: unknown): boolean {
	const code =
		typeof error === "object" && error !== null && "code" in error
			? (error as { readonly code?: unknown }).code
			: undefined;
	return (
		code === status.UNAVAILABLE ||
		code === status.DEADLINE_EXCEEDED ||
		code === status.INTERNAL ||
		code === status.UNKNOWN
	);
}

function bridgeStoreErrorCode(
	error: unknown,
): "unavailable" | "constraint_violation" {
	const code =
		typeof error === "object" && error !== null && "code" in error
			? (error as { readonly code?: unknown }).code
			: undefined;
	return code === status.ALREADY_EXISTS
		? "constraint_violation"
		: "unavailable";
}

function internalToolRepairStoreFailure(
	code: "unavailable" | "constraint_violation",
	sessionId: string,
	constraint?: string | undefined,
): RuntimeInternalToolRepairCommitResult {
	return {
		ok: false,
		error: normalizeRuntimeInternalToolRepairStoreError({
			code,
			operation: "commitInternalToolRepair",
			reason: "runtime_contract_validation",
			...(constraint !== undefined && constraint !== "" ? { constraint } : {}),
			sessionId,
		}),
	};
}

function parseContextPayload(
	contextJson: string,
	input: RuntimeThreadAddressState,
	logger?: RuntimePodLogger | undefined,
): {
	readonly contextEntries: readonly RuntimeContextEntry[];
	readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
	readonly turnFacts: ThreadTurnLoadFacts;
	readonly threadContextPrefix?: ThreadContextPrefix | undefined;
	readonly thread: RuntimeAcceptedThreadMetadataState;
	readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
	readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
	readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
	readonly pendingSandboxExecutions?:
		| readonly RuntimePreloadedSandboxExecutionState[]
		| undefined;
	readonly pendingAttachments?:
		| readonly RuntimeProviderAttachment[]
		| undefined;
	readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
} {
	try {
		const decoded: unknown = parseContextLoadPhase(
			logger,
			input,
			"context_json_parse",
			"invalid_context_json",
			() => JSON.parse(contextJson) as unknown,
		);
		const parsed = parseContextLoadPhase(
			logger,
			input,
			"context_envelope_parse",
			"invalid_context_envelope_shape",
			() => {
				const value = decoded;
				if (!isRecord(value)) {
					throw new Error("load context JSON is malformed");
				}
				for (const key of Object.keys(value)) {
					if (!LoadContextPayloadKeys.has(key)) {
						throw new Error("load context JSON contains an unknown field");
					}
				}
				return value;
			},
		);
		const contextEntries = parseContextLoadPhase(
			logger,
			input,
			"durable_context_parse",
			"invalid_durable_context_shape",
			() => {
				if (!Array.isArray(parsed.contextEntries)) {
					throw new Error("load context entries are malformed");
				}
				return parsed.contextEntries.map((entry) =>
					RuntimeContextEntrySchema.parse(entry),
				);
			},
		);
		const openRequestDraft = parseContextLoadPhase(
			logger,
			input,
			"open_request_draft_parse",
			"invalid_open_request_draft_shape",
			() =>
				parsed.openRequestDraft === undefined ||
				parsed.openRequestDraft === null
					? undefined
					: RuntimeOpenRequestDraftSchema.parse(parsed.openRequestDraft),
		);
		const turnFacts = parseContextLoadPhase(
			logger,
			input,
			"turn_facts_parse",
			"invalid_turn_facts_shape",
			() => ThreadTurnLoadFactsSchema.parse(parsed.turnFacts),
		);
		const threadContextPrefix = parseContextLoadPhase(
			logger,
			input,
			"thread_context_prefix_parse",
			"invalid_thread_context_prefix_shape",
			() => parseThreadContextPrefix(parsed.threadContextPrefix),
		);
		const thread = parseContextLoadPhase(
			logger,
			input,
			"thread_metadata_parse",
			"invalid_thread_metadata_shape",
			() => parseThreadMetadata(parsed.thread),
		);
		const runtimeConfigPatch = parseContextLoadPhase(
			logger,
			input,
			"runtime_config_parse",
			"invalid_runtime_config_shape",
			() => runtimeConfigPatchFromContextPayload(parsed, input),
		);
		const mcpManifests = parseContextLoadPhase(
			logger,
			input,
			"mcp_manifests_parse",
			"invalid_mcp_manifests_shape",
			() => mcpManifestPatchesFromContextPayload(parsed.mcpManifests, input),
		);
		const pendingToolUses = parseContextLoadPhase(
			logger,
			input,
			"pending_tool_uses_parse",
			"invalid_pending_tool_uses_shape",
			() => parsePendingToolUses(parsed.pendingToolUses),
		);
		const pendingSandboxExecutions = parseContextLoadPhase(
			logger,
			input,
			"pending_sandbox_executions_parse",
			"invalid_pending_sandbox_executions_shape",
			() => parsePendingSandboxExecutions(parsed.pendingSandboxExecutions),
		);
		const pendingAttachments = parseContextLoadPhase(
			logger,
			input,
			"pending_attachments_parse",
			"invalid_pending_attachments_shape",
			() => parsePendingAttachments(parsed.pendingAttachments),
		);
		const pendingAgentMail = parseContextLoadPhase(
			logger,
			input,
			"pending_agent_mail_parse",
			"invalid_pending_agent_mail_shape",
			() => parsePendingAgentMail(parsed.pendingAgentMail),
		);
		return {
			contextEntries,
			...(openRequestDraft !== undefined ? { openRequestDraft } : {}),
			turnFacts,
			...(threadContextPrefix !== undefined ? { threadContextPrefix } : {}),
			thread,
			...(runtimeConfigPatch !== undefined ? { runtimeConfigPatch } : {}),
			...(mcpManifests !== undefined ? { mcpManifests } : {}),
			...(pendingToolUses !== undefined ? { pendingToolUses } : {}),
			...(pendingSandboxExecutions !== undefined
				? { pendingSandboxExecutions }
				: {}),
			...(pendingAttachments !== undefined ? { pendingAttachments } : {}),
			...(pendingAgentMail !== undefined ? { pendingAgentMail } : {}),
		};
	} catch (error) {
		throw normalizeContextLoaderError({
			code: "schema_mismatch",
			rawError: error,
			sessionId: input.sessionId,
			reason: "load context returned malformed direct durable facts",
		});
	}
}

const LoadContextPayloadKeys = new Set([
	"contextEntries",
	"openRequestDraft",
	"turnFacts",
	"threadContextPrefix",
	"thread",
	"runtimeConfig",
	"mcpManifests",
	"pendingToolUses",
	"pendingSandboxExecutions",
	"pendingAttachments",
	"pendingAgentMail",
]);

function parseContextLoadPhase<T>(
	logger: RuntimePodLogger | undefined,
	input: RuntimeThreadAddressState,
	phase: RuntimeContextLoadParsePhase,
	reason: RuntimeContextLoadParseReason,
	parse: () => T,
): T {
	try {
		return parse();
	} catch (error) {
		recordContextLoadParseFailure(
			logger,
			input,
			phase,
			reason,
		);
		throw error;
	}
}

function parsePendingSandboxExecutions(
	value: unknown,
): readonly RuntimePreloadedSandboxExecutionState[] | undefined {
	if (value === undefined) {
		return undefined;
	}
	if (!Array.isArray(value)) {
		throw new Error("load context pendingSandboxExecutions is malformed");
	}
	const allowedStates = new Set<
		RuntimePreloadedSandboxExecutionState["executionState"]
	>([
		"pending",
		"preparing",
		"running",
		"waiting_activation",
		"waiting_materialization",
		"terminal_unconsumed",
	]);
	return value.map((item): RuntimePreloadedSandboxExecutionState => {
		if (!isRecord(item)) {
			throw new Error("load context pendingSandboxExecutions is malformed");
		}
		const executionState = requiredStringField(
			item,
			"executionState",
		) as RuntimePreloadedSandboxExecutionState["executionState"];
		if (!allowedStates.has(executionState)) {
			throw new Error("load context pendingSandboxExecutions is malformed");
		}
		return {
			toolUseEventId: requiredStringField(item, "toolUseEventId"),
			modelRequestId: requiredStringField(item, "modelRequestId"),
			modelToolCallId: requiredStringField(item, "modelToolCallId"),
			toolName: requiredStringField(item, "toolName"),
			input: RuntimeJsonValueSchema.parse(item.input ?? {}),
			executionState,
		};
	});
}

function parseThreadContextPrefix(
	value: unknown,
): ThreadContextPrefix | undefined {
	if (value === undefined || value === null) {
		return undefined;
	}
	if (!isRecord(value) || !Array.isArray(value.entries)) {
		throw new Error("load context threadContextPrefix is malformed");
	}
	return {
		childThreadId: requiredStringField(value, "childThreadId"),
		parentThreadId: requiredStringField(value, "parentThreadId"),
		parentBoundaryEventId: requiredStringField(value, "parentBoundaryEventId"),
		entries: value.entries.map((entry) =>
			RuntimeContextEntrySchema.parse(entry),
		),
	};
}

function parseThreadMetadata(
	value: unknown,
): RuntimeAcceptedThreadMetadataState {
	if (!isRecord(value)) {
		throw new Error("load context thread is malformed");
	}
	const role = stringField(value, "role");
	const visibility = stringField(value, "visibility");
	const status = stringField(value, "status");
	const agentType = stringField(value, "agentType");
	if (
		(role !== "main" && role !== "subagent" && role !== "approval_reviewer") ||
		(visibility !== "public" && visibility !== "internal") ||
		![
			"idle",
			"running",
			"requires_action",
			"closed_for_runtime",
			"rescheduling",
			"terminated",
			"failed",
		].includes(status ?? "") ||
		agentType === undefined
	) {
		throw new Error("load context thread is malformed");
	}
	const parentThreadId = stringField(value, "parentThreadId");
	const parentTaskName = stringField(value, "parentTaskName");
	const taskName = stringField(value, "taskName");
	return {
		role,
		visibility,
		status: status as RuntimeAcceptedThreadMetadataState["status"],
		agentType: agentType as NonNullable<
			RuntimeAcceptedThreadMetadataState["agentType"]
		>,
		...(parentThreadId !== undefined ? { parentThreadId } : {}),
		...(parentTaskName !== undefined ? { parentTaskName } : {}),
		...(taskName !== undefined ? { taskName } : {}),
	};
}

function parsePendingAgentMail(
	value: unknown,
): readonly RuntimeLoadedAgentMail[] | undefined {
	if (value === undefined) {
		return undefined;
	}
	if (!Array.isArray(value)) {
		throw new Error("load context pendingAgentMail is malformed");
	}
	const parsed = value.map((item): RuntimeLoadedAgentMail => {
		if (!isRecord(item)) {
			throw new Error("load context pendingAgentMail is malformed");
		}
		const deliveryId = requiredStringField(item, "deliveryId");
		const content = requiredStringField(item, "content");
		return {
			deliveryId,
			content,
		};
	});
	if (parsed.length > MailFetchMaxEnvelopes) {
		throw new Error("load context pendingAgentMail exceeds the envelope bound");
	}
	return parsed;
}

function parseInterruptToolResults(
	values: readonly BridgeRuntimeInterruptToolResult[],
): readonly RuntimeInterruptToolResult[] {
	const parsed = values.map((value) => {
		if (
			value.toolUseEventId.length === 0 ||
			!exactlyOneDefined(value.error, value.cancelled)
		) {
			throw new Error("Bridge interrupt Tool result is malformed");
		}
		if (value.error !== undefined) {
			return {
				toolUseEventId: value.toolUseEventId,
				result: {
					type: "error" as const,
					error: RuntimeToolErrorSchema.parse(
						JSON.parse(value.error.errorJson),
					),
				},
			};
		}
		if (value.cancelled?.errorJson !== undefined) {
			RuntimeToolErrorSchema.parse(JSON.parse(value.cancelled.errorJson));
		}
		return {
			toolUseEventId: value.toolUseEventId,
			result: { type: "cancelled" as const },
		};
	});
	if (
		new Set(parsed.map((value) => value.toolUseEventId)).size !== parsed.length
	) {
		throw new Error("Bridge interrupt Tool results contain duplicate targets");
	}
	return parsed;
}

function parsePendingAttachmentJson(
	values: readonly string[],
): readonly RuntimeProviderAttachment[] {
	return (
		parsePendingAttachments(
			values.map((value) => JSON.parse(value) as unknown),
		) ?? []
	);
}

function parsePendingAttachments(
	value: unknown,
): readonly RuntimeProviderAttachment[] | undefined {
	if (value === undefined) {
		return undefined;
	}
	if (!Array.isArray(value)) {
		throw new Error("load context pendingAttachments is malformed");
	}
	return value.map((item): RuntimeProviderAttachment => {
		if (!isRecord(item) || !isRecord(item.origin)) {
			throw new Error("load context pendingAttachments is malformed");
		}
		assertOnlyKeys(
			item.origin,
			["transient", "fileBacked"],
			"load context pendingAttachments is malformed",
		);
		const transient = recordField(item.origin, "transient");
		const fileBacked = recordField(item.origin, "fileBacked");
		if ((transient === undefined) === (fileBacked === undefined)) {
			throw new Error("load context pendingAttachments is malformed");
		}
		const mime = requiredStringField(item, "mime");
		const filename = stringField(item, "filename");
		if (filename === undefined) {
			throw new Error("load context pendingAttachments is malformed");
		}
		if (transient !== undefined) {
			if (
				![
					"image/png",
					"image/jpeg",
					"image/gif",
					"image/webp",
					"application/pdf",
				].includes(mime)
			) {
				throw new Error("load context pendingAttachments is malformed");
			}
			return {
				transient: {
					attachmentRef: requiredStringField(transient, "attachmentRef"),
					sourcePath: stringField(transient, "sourcePath") ?? "",
					pageRange: stringField(transient, "pageRange") ?? "",
					detail: stringField(transient, "detail") ?? "",
				},
				fileBacked: undefined,
				mime,
				filename,
			};
		}
		if (
			![
				"image/png",
				"image/jpeg",
				"image/gif",
				"image/webp",
				"application/pdf",
				"text/plain",
			].includes(mime)
		) {
			throw new Error("load context pendingAttachments is malformed");
		}
		return {
			transient: undefined,
			fileBacked: {
				sourceEventId: requiredStringField(fileBacked!, "sourceEventId"),
				fileId: requiredStringField(fileBacked!, "fileId"),
			},
			mime,
			filename,
		};
	});
}

function mcpManifestPatchesFromContextPayload(
	value: unknown,
	input: RuntimeThreadAddressState,
): readonly RuntimeConfigPatchState[] | undefined {
	if (value === undefined) {
		return undefined;
	}
	if (!Array.isArray(value)) {
		throw new Error("load context mcpManifests is malformed");
	}
	return value.map((item): RuntimeConfigPatchState => {
		if (!isRecord(item)) {
			throw new Error("load context mcpManifests is malformed");
		}
		assertOnlyKeys(
			item,
			[
				"mcpServerName",
				"manifestETag",
				"manifestGeneration",
				"readiness",
				"diagnostic",
				"tools",
			],
			"load context mcpManifests is malformed",
		);
		const mcpServerName = stringField(item, "mcpServerName");
		const manifestETag = stringField(item, "manifestETag");
		const generation = numberField(item, "manifestGeneration");
		const readiness = stringField(item, "readiness") ?? "ready";
		const diagnostic = stringField(item, "diagnostic");
		const readinessShape =
			item.readiness === undefined || typeof item.readiness === "string";
		const diagnosticShape =
			item.diagnostic === undefined ||
			item.diagnostic === null ||
			typeof item.diagnostic === "string";
		const etagAbsent = item.manifestETag === undefined;
		const ready =
			readiness === "ready" &&
			manifestETag !== undefined &&
			diagnostic === undefined &&
			diagnosticShape &&
			Array.isArray(item.tools);
		const unready =
			readiness === "unready" &&
			etagAbsent &&
			diagnostic !== undefined &&
			diagnosticShape &&
			Array.isArray(item.tools) &&
			item.tools.length === 0;
		if (
			mcpServerName === undefined ||
			generation === undefined ||
			!Number.isSafeInteger(generation) ||
			generation <= 0 ||
			!readinessShape ||
			(!ready && !unready)
		) {
			throw new Error("load context mcpManifests is malformed");
		}
		const manifestReadiness = readiness === "unready" ? "unready" : "ready";
		const tools = (item.tools as readonly unknown[]).map(
			(tool): Record<string, unknown> => {
				if (!isRecord(tool)) {
					throw new Error("load context mcpManifests is malformed");
				}
				assertOnlyKeys(
					tool,
					["name", "description", "inputSchema"],
					"load context mcpManifests is malformed",
				);
				const name = requiredStringField(tool, "name");
				const description = stringField(tool, "description");
				const inputSchema = tool.inputSchema;
				if (description === undefined || !isRecord(inputSchema)) {
					throw new Error("load context mcpManifests is malformed");
				}
				return { name, description, input_schema: inputSchema };
			},
		);
		return {
			...input,
			configIdentity: `mcp_manifest:${mcpServerName}`,
			generation,
			mcpServerName,
			...(manifestETag !== undefined ? { manifestETag } : {}),
			manifestReadiness,
			...(diagnostic !== undefined ? { manifestDiagnostic: diagnostic } : {}),
			contentJson: JSON.stringify({
				mcp_manifest: {
					mcp_server_name: mcpServerName,
					manifest_generation: generation,
					readiness: manifestReadiness,
					diagnostic: diagnostic ?? null,
					...(manifestETag !== undefined
						? { manifest_etag: manifestETag }
						: {}),
					...(manifestReadiness === "ready" ? { tools } : {}),
				},
			}),
		};
	});
}

function runtimeConfigPatchFromContextPayload(
	payload: Record<string, unknown>,
	input: RuntimeThreadAddressState,
): RuntimeConfigPatchState | undefined {
	const runtimeConfigValue = payload.runtimeConfig;
	if (runtimeConfigValue === undefined) {
		return undefined;
	}
	if (!isRecord(runtimeConfigValue)) {
		throw new Error("load context runtimeConfig is malformed");
	}
	const runtimeConfig = runtimeConfigValue;
	assertOnlyKeys(
		runtimeConfig,
		[
			"configGeneration",
			"approvalMode",
			"system",
			"memoryStores",
			"agent",
			"environment",
			"toolPolicy",
			"skills",
			"skillsIndex",
			"installedTools",
			"providerRescheduleBudget",
			"compactionRescheduleBudget",
		],
		"load context runtimeConfig is malformed",
	);
	const generation = numberField(runtimeConfig, "configGeneration");
	if (
		generation === undefined ||
		!Number.isSafeInteger(generation) ||
		generation <= 0
	) {
		throw new Error("load context runtimeConfig generation is malformed");
	}
	const approvalMode = stringField(runtimeConfig, "approvalMode");
	const toolPolicy = recordField(runtimeConfig, "toolPolicy");
	const installedBuiltinFamily =
		installedBuiltinFamilyFromRuntimeConfig(runtimeConfig);
	return {
		...input,
		configIdentity: "runtime_config",
		generation,
		coldLoad: true,
		installedBuiltinFamily,
		contentJson: JSON.stringify({
			config_generation: generation,
			...(approvalMode !== undefined ? { approval_mode: approvalMode } : {}),
			...(toolPolicy !== undefined ? { tool_policy: toolPolicy } : {}),
			runtime_config: runtimeConfig,
			...(payload.pendingToolUses !== undefined
				? { pending_tool_uses: payload.pendingToolUses }
				: {}),
		}),
	};
}

function installedBuiltinFamilyFromRuntimeConfig(
	runtimeConfig: Record<string, unknown>,
): "claude" | "gpt" {
	const tools = runtimeConfig.installedTools;
	if (!Array.isArray(tools)) {
		throw new Error("load context installed builtin family is malformed");
	}
	let family: "claude" | "gpt" | undefined;
	for (const tool of tools) {
		if (!isRecord(tool) || tool.type !== "tetral_agent_toolset") {
			continue;
		}
		if (
			family !== undefined ||
			(tool.family !== "claude" && tool.family !== "gpt")
		) {
			throw new Error("load context installed builtin family is malformed");
		}
		family = tool.family;
	}
	if (family === undefined) {
		throw new Error("load context installed builtin family is malformed");
	}
	return family;
}

function parsePendingToolUses(
	value: unknown,
): readonly RuntimeLoadedPendingToolUse[] | undefined {
	if (value === undefined) {
		return undefined;
	}
	if (!Array.isArray(value)) {
		throw new Error("load context pendingToolUses is malformed");
	}
	return value.map((item): RuntimeLoadedPendingToolUse => {
		if (!isRecord(item)) {
			throw new Error("load context pendingToolUses is malformed");
		}
		const status = stringField(item, "status");
		if (status !== "pending" && status !== "resolving") {
			throw new Error("load context pendingToolUses is malformed");
		}
		const decision = pendingToolDecision(item);
		const denyMessage = stringField(item, "denyMessage");
		return {
			toolUseEventId: requiredStringField(item, "toolUseEventId"),
			modelRequestId: requiredStringField(item, "modelRequestId"),
			modelToolCallId: requiredStringField(item, "modelToolCallId"),
			toolName: requiredStringField(item, "toolName"),
			input: RuntimeJsonValueSchema.parse(item.input ?? {}),
			...(decision !== undefined ? { decision } : {}),
			...(denyMessage !== undefined ? { denyMessage } : {}),
			status,
		};
	});
}

function pendingToolDecision(
	item: Record<string, unknown>,
): "allow" | "deny" | undefined {
	const decision = stringField(item, "decision");
	if (decision === undefined) {
		return undefined;
	}
	if (decision !== "allow" && decision !== "deny") {
		throw new Error("load context pendingToolUses is malformed");
	}
	return decision;
}

function recordField(
	value: Record<string, unknown>,
	field: string,
): Record<string, unknown> | undefined {
	const candidate = value[field];
	return isRecord(candidate) ? candidate : undefined;
}

function requiredStringField(
	value: Record<string, unknown>,
	field: string,
): string {
	const candidate = stringField(value, field);
	if (candidate === undefined || candidate.length === 0) {
		throw new Error(`load context ${field} is malformed`);
	}
	return candidate;
}

function stringField(
	value: Record<string, unknown>,
	field: string,
): string | undefined {
	const candidate = value[field];
	return typeof candidate === "string" ? candidate : undefined;
}

function numberField(
	value: Record<string, unknown>,
	field: string,
): number | undefined {
	const candidate = value[field];
	return typeof candidate === "number" ? candidate : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function assertOnlyKeys(
	value: Record<string, unknown>,
	allowed: readonly string[],
	message: string,
): void {
	const allowedKeys = new Set(allowed);
	if (Object.keys(value).some((key) => !allowedKeys.has(key))) {
		throw new Error(message);
	}
}

function modelRequestIdForEvent(event: SessionEventEnvelope["event"]): string {
	switch (event.type) {
		case "span.model_request_start":
			return event.model_request_id;
		case "span.model_request_end":
			return event.model_request_start_id;
		default:
			return "";
	}
}

function eventWriterRejected(
	sessionId: string,
	writeId: string,
	errorCode: string | undefined,
	deterministic = false,
): SessionEventWriterAppendResult {
	const code =
		errorCode === "scope_superseded"
			? "superseded"
			: errorCode === "closeout_unrepairable"
				? "unrepairable"
				: deterministic
					? "ack_mismatch"
					: "unavailable";
	return {
		ok: false,
		error: normalizeSessionEventWriterError({ code, sessionId, writeId }),
	};
}

function eventWriterSchemaFailure(
	sessionId: string,
	writeId: string,
): SessionEventWriterAppendResult {
	return {
		ok: false,
		error: normalizeSessionEventWriterError({
			code: "schema_mismatch",
			sessionId,
			writeId,
		}),
	};
}

function eventWriterOperationSchemaFailure(sessionId: string, writeId: string) {
	return {
		ok: false as const,
		error: normalizeSessionEventWriterError({
			code: "schema_mismatch",
			sessionId,
			writeId,
		}),
	};
}

class RuntimeTerminationResultContractError extends Error {}

function parseRuntimeTerminationResult(
	response: CommitRuntimeTerminationResponse,
): SessionEventWriterRuntimeTerminationResult {
	const variants = [
		response.committed,
		response.duplicate,
		response.stale,
	].filter((candidate) => candidate !== undefined).length;
	if (variants !== 1) {
		throw new RuntimeTerminationResultContractError(
			"CommitRuntimeTermination returned an invalid result variant",
		);
	}
	if (response.stale !== undefined) return { ok: true, type: "stale" };
	const terminal = response.committed ?? response.duplicate;
	if (
		terminal === undefined ||
		terminal.failureEventId.length === 0 ||
		terminal.closeoutEventId.length === 0
	) {
		throw new RuntimeTerminationResultContractError(
			"CommitRuntimeTermination returned incomplete durable facts",
		);
	}
	return {
		ok: true,
		type: response.committed === undefined ? "duplicate" : "committed",
		failureEventId: terminal.failureEventId,
		closeoutEventId: terminal.closeoutEventId,
	};
}

function parseToolSettlementResponse(
	response: SettleToolResultResponse,
	envelope: SessionEventWriterToolSettlementEnvelope,
): SessionEventWriterToolSettlementAttempt {
	const variants = [
		response.committed,
		response.duplicate,
		response.stale,
	].filter((candidate) => candidate !== undefined).length;
	if (variants !== 1) {
		return {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "schema_mismatch",
				sessionId: envelope.sessionId,
				writeId: envelope.settlement.toolUseEventId,
			}),
		};
	}
	if (response.committed !== undefined)
		return { ok: true, result: { type: "committed" } };
	if (response.duplicate !== undefined)
		return { ok: true, result: { type: "duplicate" } };
	return { ok: true, result: { type: "stale" } };
}

function toolSettlementTransportFailure(
	envelope: SessionEventWriterToolSettlementEnvelope,
	error: unknown,
): SessionEventWriterToolSettlementAttempt {
	const failure = eventWriterTransportFailure(
		envelope.sessionId,
		envelope.settlement.toolUseEventId,
		error,
		true,
	);
	return failure.ok
		? {
				ok: false,
				error: normalizeSessionEventWriterError({
					code: "unknown",
					sessionId: envelope.sessionId,
				}),
			}
		: { ok: false, error: failure.error };
}

function eventWriterTransportFailure(
	sessionId: string,
	writeId: string,
	error: unknown,
	retryUnknown = false,
): SessionEventWriterAppendResult {
	const grpcCode =
		typeof error === "object" &&
		error !== null &&
		"code" in error &&
		typeof error.code === "number"
			? error.code
			: undefined;
	const code =
		grpcCode === status.DEADLINE_EXCEEDED
			? "timeout"
			: grpcCode === status.UNAVAILABLE ||
					grpcCode === status.ABORTED ||
					grpcCode === status.RESOURCE_EXHAUSTED
				? "unavailable"
				: retryUnknown && grpcCode === status.UNKNOWN
					? "unavailable"
					: "unknown";
	return {
		ok: false,
		error: normalizeSessionEventWriterError({
			code,
			rawError: error,
			sessionId,
			writeId,
		}),
	};
}

function eventWriterOperationTransportFailure(
	sessionId: string,
	writeId: string,
	error: unknown,
) {
	const failure = eventWriterTransportFailure(sessionId, writeId, error);
	return failure.ok
		? {
				ok: false as const,
				error: normalizeSessionEventWriterError({
					code: "unknown",
					sessionId,
					writeId,
				}),
			}
		: failure;
}
