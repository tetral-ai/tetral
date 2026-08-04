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

/** Metrics boundary used by Runtime orchestration and provider-stream code. */
export interface RuntimeMetricsSink {
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
