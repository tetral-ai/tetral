/**
 * @packageDocumentation
 * Defines the Runtime Pod metrics vocabulary and its no-op adapter.
 * It guards operation, stream-kind, outcome, and hot-state label sets so instrumentation callers
 * cannot create unbounded metric dimensions through this interface.
 * SessionManager, ThreadLoop, and runtime command handlers call the sink; concrete observability
 * wiring implements it, while the fallback calls no external service.
 */
/** Current in-process ownership counts reported by session orchestration. */
export interface RuntimeHotStateMetrics {
	readonly activeSessions: number;
	readonly activeThreads: number;
	readonly activeFibers: number;
	readonly pendingApprovals: number;
}

/** Closed outcome labels shared by Runtime latency observations. */
export type RuntimeMetricOutcome = "success" | "error" | "cancelled" | "rejected";
/** Context-loader read and accepted-input commit operations exposed as bounded metric labels. */
export type RuntimeContextLoadOperation = "build_context" | "build_thread_context" | "load_pending_input" | "commit_accepted_input";
/** Durable event-write operations exposed as bounded metric labels. */
export type RuntimeEventWriteOperation = "append" | "finish_idle" | "write_request_end" | "commit_runtime_termination";
/** Provider request classes exposed as bounded stream metric labels. */
export type RuntimeProviderStreamKind = "agent_provider_request" | "compaction_summary" | "approval_reviewer";
/** Cleanup command outcomes exposed as bounded metric labels. */
export type RuntimeCleanupCommandOutcome = "accepted" | "rejected" | "completed" | "failed";
/** Bounded labels for completed-content ACK and local application. */
export type RuntimeContentKind = "text" | "tool" | "reasoning" | "thinking" | "request_end" | "tool_result" | "internal_tool_repair";
export type RuntimeContentCommitPhase = "content_commit" | "content_apply" | "request_end_commit" | "request_end_apply";
export type RuntimeContentCommitOutcome = "committed" | "duplicate" | "stale" | "failed";
/** Local owner stages; remote execution time is deliberately a separate observation. */
export type RuntimeContinuationOperation = "member_barrier_wait" | "permit_wait" | "binding_refresh" | "approval_wait" | "tool_accept" | "tool_settle" | "context_reconstruct" | "cleanup_join";
export type RuntimeApprovalSource = "user" | "auto_reviewer";
interface RuntimeOperationScope {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly modelRequestId?: string | undefined;
	readonly requestKind: RuntimeProviderStreamKind;
}

/** Cold approval observations have no local start time and cannot enter a latency aggregate. */
export type RuntimeOperationObservation = RuntimeOperationScope & ({
	readonly operation: RuntimeContinuationOperation;
	readonly durationMs: number;
	readonly outcome: RuntimeMetricOutcome;
	readonly approvalSource?: RuntimeApprovalSource;
	readonly timingUnavailable?: false;
} | {
	readonly operation: "approval_wait";
	readonly approvalSource: RuntimeApprovalSource;
	readonly timingUnavailable: true;
	readonly durationMs?: never;
	readonly outcome: "unavailable";
});
/** Metrics boundary used by Runtime orchestration and provider-stream code. */
export interface RuntimeMetricsSink {
	readonly observeContinuationLatency?: (operation: RuntimeContinuationOperation, durationMs: number, outcome: RuntimeMetricOutcome, requestKind: RuntimeProviderStreamKind, approvalSource?: RuntimeApprovalSource) => void;
	readonly recordApprovalWaitDelta?: (delta: 1 | -1, requestKind: RuntimeProviderStreamKind, source: RuntimeApprovalSource) => void;
	readonly recordApprovalWaitUnavailable?: (requestKind: RuntimeProviderStreamKind, source: RuntimeApprovalSource) => void;
	readonly recordContentSubmissionDelta?: (entries: number, bytes: number) => void;
	readonly observeContentCommitLatency?: (kind: RuntimeContentKind, phase: RuntimeContentCommitPhase, durationMs: number, outcome: RuntimeContentCommitOutcome, requestKind?: RuntimeProviderStreamKind) => void;
	readonly recordHotState: (snapshot: RuntimeHotStateMetrics) => void;
	readonly addActiveToolFibers: (delta: number) => void;
	readonly addPendingApprovals: (delta: number) => void;
	readonly observeProviderStreamDuration: (kind: RuntimeProviderStreamKind, durationMs: number, outcome: RuntimeMetricOutcome) => void;
	readonly observeEventWriteLatency: (operation: RuntimeEventWriteOperation, durationMs: number, outcome: RuntimeMetricOutcome) => void;
	readonly observeContextLoadLatency: (operation: RuntimeContextLoadOperation, durationMs: number, outcome: RuntimeMetricOutcome) => void;
	readonly recordCleanupCommandOutcome: (outcome: RuntimeCleanupCommandOutcome) => void;
}

/** Metrics sink used when no observability adapter is installed. */
export const NoopRuntimeMetricsSink: RuntimeMetricsSink = {
	recordHotState: () => undefined,
	addActiveToolFibers: () => undefined,
	addPendingApprovals: () => undefined,
	observeProviderStreamDuration: () => undefined,
	observeEventWriteLatency: () => undefined,
	observeContextLoadLatency: () => undefined,
	recordCleanupCommandOutcome: () => undefined,
};
const safeSinks = new WeakMap<RuntimeMetricsSink, RuntimeMetricsSink>();
/** Synchronous observer failures never gain custody over Runtime work or cleanup. */
export function safeRuntimeMetricsSink(sink: RuntimeMetricsSink | undefined): RuntimeMetricsSink {
	if (sink === undefined)
		return NoopRuntimeMetricsSink;
	const existing = safeSinks.get(sink);
	if (existing !== undefined)
		return existing;
	const safe: RuntimeMetricsSink = {
		recordHotState: (...args) => {
			try {
				sink.recordHotState(...args);
			}
			catch {
			}
		},
		addActiveToolFibers: (...args) => {
			try {
				sink.addActiveToolFibers(...args);
			}
			catch {
			}
		},
		addPendingApprovals: (...args) => {
			try {
				sink.addPendingApprovals(...args);
			}
			catch {
			}
		},
		observeProviderStreamDuration: (...args) => {
			try {
				sink.observeProviderStreamDuration(...args);
			}
			catch {
			}
		},
		observeEventWriteLatency: (...args) => {
			try {
				sink.observeEventWriteLatency(...args);
			}
			catch {
			}
		},
		observeContextLoadLatency: (...args) => {
			try {
				sink.observeContextLoadLatency(...args);
			}
			catch {
			}
		},
		recordCleanupCommandOutcome: (...args) => {
			try {
				sink.recordCleanupCommandOutcome(...args);
			}
			catch {
			}
		},
		recordContentSubmissionDelta: (...args) => {
			try {
				sink.recordContentSubmissionDelta?.(...args);
			}
			catch {
			}
		},
		observeContentCommitLatency: (...args) => {
			try {
				sink.observeContentCommitLatency?.(...args);
			}
			catch {
			}
		},
		observeContinuationLatency: (...args) => {
			try {
				sink.observeContinuationLatency?.(...args);
			}
			catch {
			}
		},
		recordApprovalWaitDelta: (...args) => {
			try {
				sink.recordApprovalWaitDelta?.(...args);
			}
			catch {
			}
		},
		recordApprovalWaitUnavailable: (...args) => {
			try {
				sink.recordApprovalWaitUnavailable?.(...args);
			}
			catch {
			}
		},
	};
	safeSinks.set(sink, safe);
	return safe;
}
interface ApprovalObservationOptions extends RuntimeOperationScope {
	readonly source: RuntimeApprovalSource;
	readonly metrics?: RuntimeMetricsSink | undefined;
	readonly recordOperation?: ((observation: RuntimeOperationObservation) => void) | undefined;
}

/** The existing pending control owns this one-shot, hot residency observation. */
export function beginApprovalWaitObservation(options: ApprovalObservationOptions & {
	readonly monotonicMs: () => number;
}): (outcome: RuntimeMetricOutcome) => void {
	const metrics = safeRuntimeMetricsSink(options.metrics);
	const startedAt = options.monotonicMs();
	let finished = false;
	metrics.recordApprovalWaitDelta?.(1, options.requestKind, options.source);
	return (outcome) => {
		if (finished)
			return;
		finished = true;
		const durationMs = Math.max(0, options.monotonicMs() - startedAt);
		metrics.recordApprovalWaitDelta?.(-1, options.requestKind, options.source);
		metrics.observeContinuationLatency?.("approval_wait", durationMs, outcome, options.requestKind, options.source);
		try {
			options.recordOperation?.({ workspaceId: options.workspaceId, sessionId: options.sessionId,
				sessionThreadId: options.sessionThreadId, modelRequestId: options.modelRequestId,
				requestKind: options.requestKind, operation: "approval_wait", durationMs, outcome,
				approvalSource: options.source });
		}
		catch {
		}
	};
}

/** Reconstruction reports missing timing explicitly, without inventing a new wait start. */
export function recordUnavailableApprovalWait(options: ApprovalObservationOptions): void {
	safeRuntimeMetricsSink(options.metrics).recordApprovalWaitUnavailable?.(options.requestKind, options.source);
	try {
		options.recordOperation?.({ workspaceId: options.workspaceId, sessionId: options.sessionId,
			sessionThreadId: options.sessionThreadId, modelRequestId: options.modelRequestId,
			requestKind: options.requestKind, operation: "approval_wait", outcome: "unavailable",
			approvalSource: options.source, timingUnavailable: true });
	}
	catch {
	}
}
