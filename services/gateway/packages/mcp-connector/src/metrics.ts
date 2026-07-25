/**
 * @packageDocumentation
 *
 * Owns the MCP connector's in-memory counters, gauge, latency sums, and
 * Prometheus text rendering. It guards monotonic event counts, deterministic
 * label ordering, and escaped label values while retaining no request bodies,
 * credentials, or result content. The service
 * shell records terminal calls and manifest activity, the MCP client reports
 * credential refresh outcomes through process wiring, and the operations
 * listener publishes the rendered snapshot.
 */

/** Enumerates the bounded outcomes recorded for credential refresh attempts. */
export type McpConnectorRefreshAttemptOutcome = "success" | "failed";

/** Carries one terminal MCP tool-call observation into the metrics registry. */
export interface McpConnectorRunToolMetric {
  readonly tool: string;
  readonly status: "completed" | "tool_error" | "runtime_error" | "unspecified";
  readonly errorKind: string;
  readonly durationSeconds: number;
}

type RunToolLabels = {
  readonly tool: string;
  readonly status: string;
  readonly error_kind: string;
};

type RunToolBucket = {
  calls: number;
  latencyCount: number;
  latencySum: number;
};

/** Aggregates connector metrics in memory and renders their Prometheus form. */
export class McpConnectorMetricsRegistry {
  readonly #runToolBuckets = new Map<string, RunToolBucket>();
  #sessionsActive = 0;
  #manifestRefreshes = 0;
  #refreshAttempts: Record<McpConnectorRefreshAttemptOutcome, number> = {
    success: 0,
    failed: 0,
  };

  /** Records one terminal call and clamps a negative duration before adding it to the latency sum. */
  recordRunTool(input: McpConnectorRunToolMetric): void {
    const labels = {
      tool: input.tool,
      status: input.status,
      error_kind: input.errorKind,
    };
    const key = runToolKey(labels);
    const bucket = this.#runToolBuckets.get(key) ?? { calls: 0, latencyCount: 0, latencySum: 0 };
    bucket.calls += 1;
    bucket.latencyCount += 1;
    bucket.latencySum += Math.max(0, input.durationSeconds);
    this.#runToolBuckets.set(key, bucket);
  }

  /** Truncates and clamps a finite active-session count; callers supply finite observations. */
  setSessionsActive(count: number): void {
    this.#sessionsActive = Math.max(0, Math.trunc(count));
  }

  /** Records one initial or notification-driven MCP manifest listing. */
  recordManifestRefresh(): void {
    this.#manifestRefreshes += 1;
  }

  /** Records the outcome of one attempted MCP credential refresh. */
  recordRefreshAttempt(outcome: McpConnectorRefreshAttemptOutcome): void {
    this.#refreshAttempts[outcome] += 1;
  }

  /** Renders a deterministic Prometheus text snapshot of the current registry. */
  render(): string {
    const lines: string[] = [
      "# HELP mcpconnector_calls_total MCP connector tool calls by tool, status, and error kind.",
      "# TYPE mcpconnector_calls_total counter",
    ];
    for (const [key, bucket] of sortedEntries(this.#runToolBuckets)) {
      lines.push(`mcpconnector_calls_total${formatRunToolLabels(JSON.parse(key) as RunToolLabels)} ${bucket.calls}`);
    }
    lines.push(
      "# HELP mcpconnector_call_latency_seconds MCP connector tool call latency in seconds.",
      "# TYPE mcpconnector_call_latency_seconds summary",
    );
    for (const [key, bucket] of sortedEntries(this.#runToolBuckets)) {
      const labels = formatRunToolLabels(JSON.parse(key) as RunToolLabels);
      lines.push(`mcpconnector_call_latency_seconds_count${labels} ${bucket.latencyCount}`);
      lines.push(`mcpconnector_call_latency_seconds_sum${labels} ${formatNumber(bucket.latencySum)}`);
    }
    lines.push(
      "# HELP mcpconnector_sessions_active Active MCP connector SDK sessions.",
      "# TYPE mcpconnector_sessions_active gauge",
      `mcpconnector_sessions_active ${this.#sessionsActive}`,
      "# HELP mcpconnector_manifest_refreshes_total MCP manifest refreshes from initial list and tools/list_changed.",
      "# TYPE mcpconnector_manifest_refreshes_total counter",
      `mcpconnector_manifest_refreshes_total ${this.#manifestRefreshes}`,
      "# HELP mcpconnector_refresh_attempts_total MCP OAuth refresh attempts by outcome.",
      "# TYPE mcpconnector_refresh_attempts_total counter",
      `mcpconnector_refresh_attempts_total{outcome="success"} ${this.#refreshAttempts.success}`,
      `mcpconnector_refresh_attempts_total{outcome="failed"} ${this.#refreshAttempts.failed}`,
    );
    return `${lines.join("\n")}\n`;
  }
}

function sortedEntries<T>(input: Map<string, T>): Array<readonly [string, T]> {
  return [...input.entries()].sort(([left], [right]) => left.localeCompare(right));
}

function runToolKey(labels: RunToolLabels): string {
  return JSON.stringify({
    tool: labels.tool,
    status: labels.status,
    error_kind: labels.error_kind,
  });
}

function formatRunToolLabels(labels: RunToolLabels): string {
  return `{tool="${escapeLabelValue(labels.tool)}",status="${escapeLabelValue(labels.status)}",error_kind="${escapeLabelValue(labels.error_kind)}"}`;
}

function escapeLabelValue(value: string): string {
  return value.replaceAll("\\", "\\\\").replaceAll("\n", "\\n").replaceAll("\"", "\\\"");
}

function formatNumber(value: number): string {
  return Number.isFinite(value) ? value.toString() : "0";
}
