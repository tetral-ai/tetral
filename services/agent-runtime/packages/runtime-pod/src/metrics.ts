/**
 * Collects injected, process-local Runtime Pod observations and renders the operational Prometheus
 * endpoint. Runtime Core and command services write to the registry, the HTTP server reads snapshots,
 * and rendering combines those snapshots with lifecycle and process-memory gauges. Values are
 * normalized to finite non-negative numbers, snapshots copy mutable maps, and metrics remain a
 * read-only observability side channel that does not affect command or lifecycle decisions.
 */
import type {
  RuntimeCleanupCommandOutcome,
  RuntimeContextLoadOperation,
  RuntimeEventWriteOperation,
  RuntimeHotStateMetrics,
  RuntimeMetricOutcome,
  RuntimeMetricsSink,
  RuntimeProviderStreamKind,
} from "@tetral/agent-runtime-core/src/runtime/metrics.js";
import type { RuntimeCloseoutEvent } from "@tetral/agent-runtime-core/src/session/session-manager.js";
import type { RuntimePodLifecycle } from "./lifecycle.js";

/** Closed declaration-receipt outcomes exposed by Runtime Pod observability. */
export type RuntimeReceiptEvidenceOutcome =
  | "applied"
  | "stale_custody"
  | "binding_identity_mismatch"
  | "receipt_shape_invalid"
  | "declaration_digest_mismatch"
  | "receipt_application_failed";

interface Observation {
  count: number;
  sum: number;
}

/** Captures hot-state gauges, labelled latency summaries, and cleanup outcomes at one instant. */
export interface RuntimePodDomainMetricsSnapshot extends RuntimeHotStateMetrics {
  readonly activeToolFibers: number;
  readonly providerStreamDurationMs: ReadonlyMap<string, Observation>;
  readonly eventWriteLatencyMs: ReadonlyMap<string, Observation>;
  readonly contextLoadLatencyMs: ReadonlyMap<string, Observation>;
  readonly cleanupCommandOutcomes: ReadonlyMap<RuntimeCleanupCommandOutcome, number>;
  readonly closeoutEvents: ReadonlyMap<RuntimeCloseoutEvent["event"], number>;
  readonly receiptEvidence: ReadonlyMap<RuntimeReceiptEvidenceOutcome, number>;
}

/** Combines the Runtime Core metrics sink with snapshot access for HTTP exposition. */
export interface RuntimePodMetricsSource extends RuntimeMetricsSink {
  readonly recordCloseoutEvent: (event: RuntimeCloseoutEvent) => void;
  readonly recordReceiptEvidence: (outcome: RuntimeReceiptEvidenceOutcome) => void;
  readonly snapshot: () => RuntimePodDomainMetricsSnapshot;
}

/**
 * Stores Runtime Pod gauges, observation totals, and cleanup counters in memory.
 * Callers inject one registry into runtime services and HTTP composition instead of using global state.
 */
export class RuntimePodMetricsRegistry implements RuntimePodMetricsSource {
  private hotState: RuntimeHotStateMetrics = {
    activeSessions: 0,
    activeThreads: 0,
    activeFibers: 0,
    pendingApprovals: 0,
  };
  private activeToolFibers = 0;
  private readonly providerStreamDurationMs = new Map<string, Observation>();
  private readonly eventWriteLatencyMs = new Map<string, Observation>();
  private readonly contextLoadLatencyMs = new Map<string, Observation>();
  private readonly cleanupCommandOutcomes = new Map<RuntimeCleanupCommandOutcome, number>();
  private readonly closeoutEvents = new Map<RuntimeCloseoutEvent["event"], number>();
  private readonly receiptEvidence = new Map<RuntimeReceiptEvidenceOutcome, number>();

  /** Replaces the hot-state gauges after normalizing every value to a finite non-negative number. */
  recordHotState(snapshot: RuntimeHotStateMetrics): void {
    this.hotState = {
      activeSessions: nonNegative(snapshot.activeSessions),
      activeThreads: nonNegative(snapshot.activeThreads),
      activeFibers: nonNegative(snapshot.activeFibers),
      pendingApprovals: nonNegative(snapshot.pendingApprovals),
    };
  }

  /** Applies a delta to the active tool-fiber gauge and floors the resulting value at zero. */
  addActiveToolFibers(delta: number): void {
    this.activeToolFibers = nonNegative(this.activeToolFibers + delta);
  }

  /** Applies a delta to pending approvals and floors the resulting gauge at zero. */
  addPendingApprovals(delta: number): void {
    this.hotState = {
      ...this.hotState,
      pendingApprovals: nonNegative(this.hotState.pendingApprovals + delta),
    };
  }

  /** Records one provider stream duration under its stream kind and outcome labels. */
  observeProviderStreamDuration(kind: RuntimeProviderStreamKind, durationMs: number, outcome: RuntimeMetricOutcome): void {
    addObservation(this.providerStreamDurationMs, labelledKey({ kind, outcome }), durationMs);
  }

  /** Records one Bridge event-write latency under its operation and outcome labels. */
  observeEventWriteLatency(operation: RuntimeEventWriteOperation, durationMs: number, outcome: RuntimeMetricOutcome): void {
    addObservation(this.eventWriteLatencyMs, labelledKey({ operation, outcome }), durationMs);
  }

  /** Records one Bridge context-load latency under its operation and outcome labels. */
  observeContextLoadLatency(operation: RuntimeContextLoadOperation, durationMs: number, outcome: RuntimeMetricOutcome): void {
    addObservation(this.contextLoadLatencyMs, labelledKey({ operation, outcome }), durationMs);
  }

  /** Increments the counter for one cleanup command outcome. */
  recordCleanupCommandOutcome(outcome: RuntimeCleanupCommandOutcome): void {
    this.cleanupCommandOutcomes.set(outcome, (this.cleanupCommandOutcomes.get(outcome) ?? 0) + 1);
  }

  /** Counts one declaration receipt application or closed discard outcome. */
  recordReceiptEvidence(outcome: RuntimeReceiptEvidenceOutcome): void {
    this.receiptEvidence.set(outcome, (this.receiptEvidence.get(outcome) ?? 0) + 1);
  }

  /** Counts closeout alarms per affected thread and terminal closeout records per occurrence. */
  recordCloseoutEvent(event: RuntimeCloseoutEvent): void {
    const increment = event.event === "runtime_closeout_stalled" ? event.activeCloseouts : 1;
    this.closeoutEvents.set(event.event, (this.closeoutEvents.get(event.event) ?? 0) + increment);
  }

  /** Returns current gauges and defensive copies of all mutable observation maps. */
  snapshot(): RuntimePodDomainMetricsSnapshot {
    return {
      ...this.hotState,
      activeToolFibers: this.activeToolFibers,
      providerStreamDurationMs: new Map(this.providerStreamDurationMs),
      eventWriteLatencyMs: new Map(this.eventWriteLatencyMs),
      contextLoadLatencyMs: new Map(this.contextLoadLatencyMs),
      cleanupCommandOutcomes: new Map(this.cleanupCommandOutcomes),
      closeoutEvents: new Map(this.closeoutEvents),
      receiptEvidence: new Map(this.receiptEvidence),
    };
  }
}

const EmptyRuntimePodMetrics: RuntimePodMetricsSource = {
  recordHotState: () => undefined,
  addActiveToolFibers: () => undefined,
  addPendingApprovals: () => undefined,
  observeProviderStreamDuration: () => undefined,
  observeEventWriteLatency: () => undefined,
  observeContextLoadLatency: () => undefined,
  recordCleanupCommandOutcome: () => undefined,
  recordReceiptEvidence: () => undefined,
  recordCloseoutEvent: () => undefined,
  snapshot: () => ({
    activeSessions: 0,
    activeThreads: 0,
    activeFibers: 0,
    activeToolFibers: 0,
    pendingApprovals: 0,
    providerStreamDurationMs: new Map(),
    eventWriteLatencyMs: new Map(),
    contextLoadLatencyMs: new Map(),
    cleanupCommandOutcomes: new Map(),
    closeoutEvents: new Map(),
    receiptEvidence: new Map(),
  }),
};

/**
 * Renders lifecycle, domain, and process-memory observations in Prometheus text format.
 * Without a domain source, runtime gauges and counters default to zero and labelled summaries are empty;
 * lifecycle and process-memory observations still reflect the live process.
 */
export function runtimePodMetricsText(
  lifecycle: RuntimePodLifecycle,
  runtimeMetrics: RuntimePodMetricsSource = EmptyRuntimePodMetrics,
): string {
  const snapshot = lifecycle.metricsSnapshot();
  const runtimeSnapshot = runtimeMetrics.snapshot();
  const memory = process.memoryUsage();
  return [
    metric("runtimepod_ready", "Runtime Pod readiness state.", "gauge", snapshot.ready ? 1 : 0),
    metric("runtimepod_accepting_commands", "Runtime Pod command admission state.", "gauge", snapshot.accepting ? 1 : 0),
    metric("runtimepod_commands_in_flight", "Runtime Pod commands currently draining or executing.", "gauge", snapshot.inFlightCommands),
    metric("runtimepod_active_sessions", "Runtime Pod hot-state sessions currently resident.", "gauge", runtimeSnapshot.activeSessions),
    metric("runtimepod_active_threads", "Runtime Pod hot-state threads currently resident.", "gauge", runtimeSnapshot.activeThreads),
    metric("runtimepod_active_fibers", "Runtime Pod active thread-run fibers.", "gauge", runtimeSnapshot.activeFibers),
    metric("runtimepod_active_tool_fibers", "Runtime Pod active tool execution fibers.", "gauge", runtimeSnapshot.activeToolFibers),
    metric("runtimepod_pending_approvals", "Runtime Pod pending approval tool jobs.", "gauge", runtimeSnapshot.pendingApprovals),
    observationMetric(
      "runtimepod_provider_stream_duration_ms",
      "Runtime Pod provider stream duration in milliseconds.",
      runtimeSnapshot.providerStreamDurationMs,
    ),
    observationMetric(
      "runtimepod_event_write_latency_ms",
      "Runtime Pod Bridge event write latency in milliseconds.",
      runtimeSnapshot.eventWriteLatencyMs,
    ),
    observationMetric(
      "runtimepod_context_load_latency_ms",
      "Runtime Pod Bridge context-load latency in milliseconds.",
      runtimeSnapshot.contextLoadLatencyMs,
    ),
    cleanupOutcomeMetric(runtimeSnapshot.cleanupCommandOutcomes),
    receiptEvidenceMetric(runtimeSnapshot.receiptEvidence),
    closeoutEventMetric(runtimeSnapshot.closeoutEvents),
    metric("process_heap_used_bytes", "JavaScript heap bytes currently used by the process.", "gauge", memory.heapUsed),
    metric("process_rss_bytes", "Resident set size bytes for the process.", "gauge", memory.rss),
  ].join("");
}

function metric(name: string, help: string, type: "counter" | "gauge", value: number): string {
  return `# HELP ${name} ${help}\n# TYPE ${name} ${type}\n${name} ${formatMetricValue(value)}\n`;
}

function observationMetric(name: string, help: string, values: ReadonlyMap<string, Observation>): string {
  let text = `# HELP ${name} ${help}\n# TYPE ${name} summary\n`;
  for (const [labels, observation] of values) {
    text += `${name}_count${labels} ${formatMetricValue(observation.count)}\n`;
    text += `${name}_sum${labels} ${formatMetricValue(observation.sum)}\n`;
  }
  return text;
}

function cleanupOutcomeMetric(values: ReadonlyMap<RuntimeCleanupCommandOutcome, number>): string {
  const name = "runtimepod_cleanup_command_outcomes_total";
  let text = `# HELP ${name} Runtime Pod cleanup command outcomes.\n# TYPE ${name} counter\n`;
  for (const outcome of ["accepted", "rejected", "completed", "failed"] as const) {
    text += `${name}{outcome="${outcome}"} ${formatMetricValue(values.get(outcome) ?? 0)}\n`;
  }
  return text;
}

function closeoutEventMetric(values: ReadonlyMap<RuntimeCloseoutEvent["event"], number>): string {
  const name = "runtimepod_closeout_events_total";
  let text = `# HELP ${name} Runtime Pod failed-run closeout observations.\n# TYPE ${name} counter\n`;
  for (const event of [
    "runtime_closeout_stalled",
    "runtime_closeout_recovered",
    "runtime_closeout_unrepairable",
  ] as const) {
    text += `${name}{event="${event}"} ${formatMetricValue(values.get(event) ?? 0)}\n`;
  }
  return text;
}

function receiptEvidenceMetric(values: ReadonlyMap<RuntimeReceiptEvidenceOutcome, number>): string {
  const name = "runtimepod_receipt_evidence_total";
  let text = `# HELP ${name} Runtime declaration receipt application outcomes.\n# TYPE ${name} counter\n`;
  for (const outcome of [
    "applied",
    "stale_custody",
    "binding_identity_mismatch",
    "receipt_shape_invalid",
    "declaration_digest_mismatch",
    "receipt_application_failed",
  ] as const) {
    text += `${name}{outcome="${outcome}"} ${formatMetricValue(values.get(outcome) ?? 0)}\n`;
  }
  return text;
}

function labelledKey(labels: Readonly<Record<string, string>>): string {
  const entries = Object.entries(labels).sort(([left], [right]) => left.localeCompare(right));
  return `{${entries.map(([key, value]) => `${key}="${escapeLabel(value)}"`).join(",")}}`;
}

function escapeLabel(value: string): string {
  return value.replaceAll("\\", "\\\\").replaceAll("\n", "\\n").replaceAll('"', '\\"');
}

function addObservation(values: Map<string, Observation>, key: string, durationMs: number): void {
  const current = values.get(key) ?? { count: 0, sum: 0 };
  values.set(key, {
    count: current.count + 1,
    sum: current.sum + nonNegative(durationMs),
  });
}

function nonNegative(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, value);
}

function formatMetricValue(value: number): string {
  return String(nonNegative(value));
}
