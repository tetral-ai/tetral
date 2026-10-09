/**
 * Collects injected, process-local Runtime Pod observations and renders the operational Prometheus
 * endpoint. Runtime Core and command services write to the registry, the HTTP server reads snapshots,
 * and rendering combines those snapshots with lifecycle and process-memory gauges. Values are
 * normalized to finite non-negative numbers, snapshots copy mutable maps, and metrics remain a
 * read-only observability side channel that does not affect command or lifecycle decisions.
 */
import { OperationMetricsRegistry } from "@tetral/ts-observability";
import type { ShutdownPhaseLogger } from "@tetral/ts-observability";
import { readFileSync } from "node:fs";
import type {
	RuntimeApprovalSource,
	RuntimeContinuationOperation,
	RuntimeContentKind,
	RuntimeContentCommitPhase,
	RuntimeContentCommitOutcome,
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

/** Container values share one cgroup scope; an unlimited/unknown limit cannot advertise headroom. */
export interface ContainerMemoryObservation {
	readonly usageBytes: number;
	readonly limitBytes: number;
}
export function containerMemoryObservation(
	read: (path: string) => string = (path) => readFileSync(path, "utf8"),
): ContainerMemoryObservation | undefined {
	for (const [usagePath, limitPath] of [
		["/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.max"],
		[
			"/sys/fs/cgroup/memory/memory.usage_in_bytes",
			"/sys/fs/cgroup/memory/memory.limit_in_bytes",
		],
	]) {
		try {
			const usage = read(usagePath!).trim(),
				limit = read(limitPath!).trim();
			if (!/^[0-9]+$/.test(usage) || !/^[1-9][0-9]*$/.test(limit))
				return undefined;
			const usageBytes = Number(usage),
				limitBytes = Number(limit);
			if (
				!Number.isSafeInteger(usageBytes) ||
				!Number.isSafeInteger(limitBytes) ||
				limitBytes >= 2 ** 60
			)
				return undefined;
			return { usageBytes, limitBytes };
		} catch {
			/* Try the other cgroup ABI only when its files are unavailable. */
		}
	}
	return undefined;
}

interface Observation {
	count: number;
	sum: number;
}

/** Captures hot-state gauges, labelled latency summaries, and cleanup outcomes at one instant. */
export interface RuntimePodDomainMetricsSnapshot
	extends RuntimeHotStateMetrics {
	readonly activeToolFibers: number;
 readonly operationDurationText?: string;
 readonly pendingContentEntries?:number;
 readonly pendingContentBytes?:number;
 readonly contentCommitLatencyMs?:ReadonlyMap<string,Observation>;
 readonly continuationLatencyMs?:ReadonlyMap<string,Observation>;
	readonly approvalWaitStarted?: ReadonlyMap<string, number>;
	readonly approvalWaitOutstanding?: ReadonlyMap<string, number>;
	readonly approvalWaitUnavailable?: ReadonlyMap<string, number>;
	readonly providerStreamDurationMs: ReadonlyMap<string, Observation>;
	readonly eventWriteLatencyMs: ReadonlyMap<string, Observation>;
	readonly contextLoadLatencyMs: ReadonlyMap<string, Observation>;
	readonly cleanupCommandOutcomes: ReadonlyMap<
		RuntimeCleanupCommandOutcome,
		number
	>;
	readonly closeoutEvents: ReadonlyMap<RuntimeCloseoutEvent["event"], number>;
}

export type RuntimeShutdownPhase = "shutdown_quiesce" | "shutdown_report" | "shutdown_release" | "shutdown_local_join" | "shutdown_clients" | "shutdown_listeners";

/** Combines the Runtime Core metrics sink with snapshot access for HTTP exposition. */
export interface RuntimePodMetricsSource extends RuntimeMetricsSink {
 readonly observeShutdownPhase?: (phase: RuntimeShutdownPhase, durationMs: number, outcome: "success" | "error" | "timeout", logger?: ShutdownPhaseLogger) => void;
	readonly recordCloseoutEvent: (event: RuntimeCloseoutEvent) => void;
	readonly snapshot: () => RuntimePodDomainMetricsSnapshot;
}

/**
 * Stores Runtime Pod gauges, observation totals, and cleanup counters in memory.
 * Callers inject one registry into runtime services and HTTP composition instead of using global state.
 */
export class RuntimePodMetricsRegistry implements RuntimePodMetricsSource {
 readonly operations = new OperationMetricsRegistry("agent-runtime", [
  "agent_provider_request", "compaction_summary", "approval_reviewer", "approval_reviewer_compaction",
  "append", "finish_idle", "write_request_end", "commit_runtime_termination",
  "build_context", "build_thread_context", "load_pending_input", "commit_accepted_input",
  "content_commit", "content_apply", "request_end_commit", "request_end_apply",
  "member_barrier_wait", "permit_wait", "binding_refresh", "approval_wait", "tool_accept", "tool_settle", "context_reconstruct", "cleanup_join",
  "shutdown_quiesce", "shutdown_report", "shutdown_release", "shutdown_local_join", "shutdown_clients", "shutdown_listeners",
 ]);
 observeShutdownPhase(phase: RuntimeShutdownPhase, durationMs: number, outcome: "success" | "error" | "timeout", logger?: ShutdownPhaseLogger): void { this.operations.observeShutdown(phase,outcome,durationMs/1000,logger); }
	private hotState: RuntimeHotStateMetrics = {
		activeSessions: 0,
		activeThreads: 0,
		activeFibers: 0,
		pendingApprovals: 0,
	};
	private activeToolFibers = 0;
 private pendingContentEntries=0;
 private pendingContentBytes=0;
 private readonly contentCommitLatencyMs=new Map<string,Observation>();
 private readonly continuationLatencyMs=new Map<string,Observation>();
	private readonly approvalWaitStarted = new Map<string, number>();
	private readonly approvalWaitOutstanding = new Map<string, number>();
	private readonly approvalWaitUnavailable = new Map<string, number>();
	private readonly providerStreamDurationMs = new Map<string, Observation>();
	private readonly eventWriteLatencyMs = new Map<string, Observation>();
	private readonly contextLoadLatencyMs = new Map<string, Observation>();
	private readonly cleanupCommandOutcomes = new Map<
		RuntimeCleanupCommandOutcome,
		number
	>();
	private readonly closeoutEvents = new Map<
		RuntimeCloseoutEvent["event"],
		number
	>();

 recordContentSubmissionDelta(entries:number,bytes:number):void {
  this.pendingContentEntries=nonNegative(this.pendingContentEntries+entries);
  this.pendingContentBytes=nonNegative(this.pendingContentBytes+bytes);
 }
 observeContentCommitLatency(kind:RuntimeContentKind,phase:RuntimeContentCommitPhase,durationMs:number,outcome:RuntimeContentCommitOutcome,requestKind:RuntimeProviderStreamKind="agent_provider_request"):void {
  this.operations.observe(phase,outcome,durationMs/1000);
  addObservation(this.contentCommitLatencyMs,labelledKey({kind,phase,outcome,request_kind:requestKind}),durationMs);
 }

 observeContinuationLatency(operation:RuntimeContinuationOperation,durationMs:number,outcome:RuntimeMetricOutcome,requestKind:RuntimeProviderStreamKind,approvalSource?:RuntimeApprovalSource):void {
  this.operations.observe(operation,outcome,durationMs/1000);
  addObservation(this.continuationLatencyMs,labelledKey({operation,outcome,request_kind:requestKind,
    ...(approvalSource === undefined ? {} : { approval_source: approvalSource })}),durationMs);
 }

	recordApprovalWaitDelta(delta: 1 | -1, requestKind: RuntimeProviderStreamKind, source: RuntimeApprovalSource): void {
		const key = labelledKey({ request_kind: requestKind, approval_source: source });
		if (delta === 1) this.approvalWaitStarted.set(key, (this.approvalWaitStarted.get(key) ?? 0) + 1);
		this.approvalWaitOutstanding.set(key, nonNegative((this.approvalWaitOutstanding.get(key) ?? 0) + delta));
	}

	recordApprovalWaitUnavailable(requestKind: RuntimeProviderStreamKind, source: RuntimeApprovalSource): void {
		const key = labelledKey({ request_kind: requestKind, approval_source: source });
		this.approvalWaitUnavailable.set(key, (this.approvalWaitUnavailable.get(key) ?? 0) + 1);
	}

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
	observeProviderStreamDuration(
		kind: RuntimeProviderStreamKind,
		durationMs: number,
		outcome: RuntimeMetricOutcome,
	): void {
		this.operations.observe(kind,outcome,durationMs/1000);
		addObservation(
			this.providerStreamDurationMs,
			labelledKey({ kind, outcome }),
			durationMs,
		);
	}

	/** Records one Bridge event-write latency under its operation and outcome labels. */
	observeEventWriteLatency(
		operation: RuntimeEventWriteOperation,
		durationMs: number,
		outcome: RuntimeMetricOutcome,
	): void {
		this.operations.observe(operation,outcome,durationMs/1000);
		addObservation(
			this.eventWriteLatencyMs,
			labelledKey({ operation, outcome }),
			durationMs,
		);
	}

	/** Records one Bridge context-load latency under its operation and outcome labels. */
	observeContextLoadLatency(
		operation: RuntimeContextLoadOperation,
		durationMs: number,
		outcome: RuntimeMetricOutcome,
	): void {
		this.operations.observe(operation,outcome,durationMs/1000);
		addObservation(
			this.contextLoadLatencyMs,
			labelledKey({ operation, outcome }),
			durationMs,
		);
	}

	/** Increments the counter for one cleanup command outcome. */
	recordCleanupCommandOutcome(outcome: RuntimeCleanupCommandOutcome): void {
		this.cleanupCommandOutcomes.set(
			outcome,
			(this.cleanupCommandOutcomes.get(outcome) ?? 0) + 1,
		);
	}

	/** Counts closeout alarms per affected thread and terminal closeout records per occurrence. */
	recordCloseoutEvent(event: RuntimeCloseoutEvent): void {
		const increment =
			event.event === "runtime_closeout_stalled" ? event.activeCloseouts : 1;
		this.closeoutEvents.set(
			event.event,
			(this.closeoutEvents.get(event.event) ?? 0) + increment,
		);
	}

	/** Returns current gauges and defensive copies of all mutable observation maps. */
	snapshot(): RuntimePodDomainMetricsSnapshot {
		return {
			...this.hotState,
			activeToolFibers: this.activeToolFibers,
   operationDurationText:this.operations.render(),
   pendingContentEntries:this.pendingContentEntries,
   pendingContentBytes:this.pendingContentBytes,
   contentCommitLatencyMs:new Map(this.contentCommitLatencyMs),
   continuationLatencyMs:new Map(this.continuationLatencyMs),
			approvalWaitStarted: new Map(this.approvalWaitStarted),
			approvalWaitOutstanding: new Map(this.approvalWaitOutstanding),
			approvalWaitUnavailable: new Map(this.approvalWaitUnavailable),
			providerStreamDurationMs: new Map(this.providerStreamDurationMs),
			eventWriteLatencyMs: new Map(this.eventWriteLatencyMs),
			contextLoadLatencyMs: new Map(this.contextLoadLatencyMs),
			cleanupCommandOutcomes: new Map(this.cleanupCommandOutcomes),
			closeoutEvents: new Map(this.closeoutEvents),
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
	readContainerMemory: () =>
		| ContainerMemoryObservation
		| undefined = containerMemoryObservation,
): string {
	const snapshot = lifecycle.metricsSnapshot();
	const runtimeSnapshot = runtimeMetrics.snapshot();
	const memory = process.memoryUsage();
	let container: ContainerMemoryObservation | undefined;
	try {
		container = readContainerMemory();
	} catch {
		/* unknown remains ineligible */
	}
	const validContainer =
		container !== undefined &&
		Number.isSafeInteger(container.usageBytes) &&
		container.usageBytes >= 0 &&
		Number.isSafeInteger(container.limitBytes) &&
		container.limitBytes > 0;
	const capacity = lifecycle.sessionCapacity();
	return [
    runtimeSnapshot.operationDurationText ?? "",
		...(capacity === undefined
			? []
			: [
					metric(
						"runtimepod_session_capacity",
						"Configured local Session capacity.",
						"gauge",
						capacity,
					),
				]),
		...(validContainer
			? [
					metric(
						"runtimepod_container_memory_usage_bytes",
						"Current container cgroup memory usage.",
						"gauge",
						container!.usageBytes,
					),
					metric(
						"runtimepod_container_memory_limit_bytes",
						"Finite container cgroup memory limit.",
						"gauge",
						container!.limitBytes,
					),
				]
			: ["# runtimepod_container_memory_state unknown_or_unlimited\n"]),
		metric(
			"runtimepod_ready",
			"Runtime Pod readiness state.",
			"gauge",
			snapshot.ready ? 1 : 0,
		),
		metric(
			"runtimepod_accepting_commands",
			"Runtime Pod command admission state.",
			"gauge",
			snapshot.accepting ? 1 : 0,
		),
		metric(
			"runtimepod_commands_in_flight",
			"Runtime Pod commands currently draining or executing.",
			"gauge",
			snapshot.inFlightCommands,
		),
		metric(
			"runtimepod_active_sessions",
			"Runtime Pod hot-state sessions currently resident.",
			"gauge",
			runtimeSnapshot.activeSessions,
		),
		metric(
			"runtimepod_active_threads",
			"Runtime Pod hot-state threads currently resident.",
			"gauge",
			runtimeSnapshot.activeThreads,
		),
		metric(
			"runtimepod_active_fibers",
			"Runtime Pod active thread-run fibers.",
			"gauge",
			runtimeSnapshot.activeFibers,
		),
		metric(
			"runtimepod_active_tool_fibers",
			"Runtime Pod active tool execution fibers.",
			"gauge",
			runtimeSnapshot.activeToolFibers,
		),
  metric("runtimepod_pending_content_entries","Runtime semantic member submissions awaiting ACK/application.","gauge",runtimeSnapshot.pendingContentEntries??0),
  metric("runtimepod_pending_content_bytes","Encoded Runtime semantic member submissions awaiting ACK/application.","gauge",runtimeSnapshot.pendingContentBytes??0),
  observationMetric("runtimepod_continuation_latency_ms","Runtime owning continuation stages in milliseconds.",runtimeSnapshot.continuationLatencyMs??new Map()),
		labelledMetric("runtimepod_approval_wait_started_total", "Hot residency approval observations started.", "counter", runtimeSnapshot.approvalWaitStarted ?? new Map()),
		labelledMetric("runtimepod_approval_wait_outstanding", "Hot residency approval observations awaiting a local outcome.", "gauge", runtimeSnapshot.approvalWaitOutstanding ?? new Map()),
		labelledMetric("runtimepod_approval_wait_unavailable_total", "Cold approvals with no local start timestamp; excluded from latency summaries.", "counter", runtimeSnapshot.approvalWaitUnavailable ?? new Map()),
  observationMetric("runtimepod_content_commit_latency_ms","Completed content bridge ACK and local application latency in milliseconds.",runtimeSnapshot.contentCommitLatencyMs??new Map()),
		metric(
			"runtimepod_pending_approvals",
			"Runtime Pod pending approval tool jobs.",
			"gauge",
			runtimeSnapshot.pendingApprovals,
		),
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
		closeoutEventMetric(runtimeSnapshot.closeoutEvents),
		metric(
			"process_heap_used_bytes",
			"JavaScript heap bytes currently used by the process.",
			"gauge",
			memory.heapUsed,
		),
		metric(
			"process_rss_bytes",
			"Resident set size bytes for the process.",
			"gauge",
			memory.rss,
		),
	].join("");
}

function metric(
	name: string,
	help: string,
	type: "counter" | "gauge",
	value: number,
): string {
	return `# HELP ${name} ${help}\n# TYPE ${name} ${type}\n${name} ${formatMetricValue(value)}\n`;
}

function observationMetric(
	name: string,
	help: string,
	values: ReadonlyMap<string, Observation>,
): string {
	let text = `# HELP ${name} ${help}\n# TYPE ${name} summary\n`;
	for (const [labels, observation] of values) {
		text += `${name}_count${labels} ${formatMetricValue(observation.count)}\n`;
		text += `${name}_sum${labels} ${formatMetricValue(observation.sum)}\n`;
	}
	return text;
}

function labelledMetric(name: string, help: string, type: "counter" | "gauge", values: ReadonlyMap<string, number>): string {
	let text = `# HELP ${name} ${help}\n# TYPE ${name} ${type}\n`;
	for (const [labels, value] of values) text += `${name}${labels} ${formatMetricValue(value)}\n`;
	return text;
}

function cleanupOutcomeMetric(
	values: ReadonlyMap<RuntimeCleanupCommandOutcome, number>,
): string {
	const name = "runtimepod_cleanup_command_outcomes_total";
	let text = `# HELP ${name} Runtime Pod cleanup command outcomes.\n# TYPE ${name} counter\n`;
	for (const outcome of [
		"accepted",
		"rejected",
		"completed",
		"failed",
	] as const) {
		text += `${name}{outcome="${outcome}"} ${formatMetricValue(values.get(outcome) ?? 0)}\n`;
	}
	return text;
}

function closeoutEventMetric(
	values: ReadonlyMap<RuntimeCloseoutEvent["event"], number>,
): string {
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

function labelledKey(labels: Readonly<Record<string, string>>): string {
	const entries = Object.entries(labels).sort(([left], [right]) =>
		left.localeCompare(right),
	);
	return `{${entries.map(([key, value]) => `${key}="${escapeLabel(value)}"`).join(",")}}`;
}

function escapeLabel(value: string): string {
	return value
		.replaceAll("\\", "\\\\")
		.replaceAll("\n", "\\n")
		.replaceAll('"', '\\"');
}

function addObservation(
	values: Map<string, Observation>,
	key: string,
	durationMs: number,
): void {
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
