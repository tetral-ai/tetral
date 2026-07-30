/**
 * Defines the Runtime Pod's structured log vocabulary and record factories. The process command
 * creates the JSON logger, while lifecycle and command handling emit records through it; this module
 * delegates serialization and common fields to the shared observability package. Startup adapter
 * failures use a fixed public-safe message, and the record factories add no raw request, token,
 * provider, or dependency payloads.
 */
import { createTetralJsonLogger, semanticErrorFields } from "@tetral/ts-observability";
import type { TetralJsonLogger, TetralLogRecord } from "@tetral/ts-observability";
import type { RuntimeCloseoutEvent } from "@tetral/agent-runtime-core/src/session/session-manager.js";
import type { RuntimeMetricsSink } from "@tetral/agent-runtime-core/src/runtime/metrics.js";
import type { RuntimeReceiptEvidenceOutcome } from "./metrics.js";

/** Structured JSON logger accepted by Runtime Pod composition and runtime services. */
export type RuntimePodLogger = TetralJsonLogger<RuntimePodLogRecord>;

/** Enumerates Runtime Pod-specific fields layered on the shared structured log record. */
export type RuntimePodLogRecord = TetralLogRecord & {
  readonly event: string;
  readonly kind?: "config_error" | "startup_error" | "shutdown_error";
  readonly "grpc.method"?: string;
  readonly "grpc.code"?: string;
  readonly "caller.service_account"?: string;
  readonly "runtime_input.id"?: string;
  readonly "closeout.active_count"?: number;
  readonly "closeout.error_code"?: RuntimeCloseoutEvent["errorCode"];
  readonly "startup.cause_class"?: string;
  readonly "declaration.source.kind"?: string;
  readonly "declaration.source.id"?: string;
  readonly "declaration.digest"?: string;
  readonly "receipt.application_disposition"?: "current_custody" | "stale_custody";
  readonly "receipt.discard_reason"?: Exclude<RuntimeReceiptEvidenceOutcome, "applied">;
};

/** Safe identity-only evidence emitted after one Bridge declaration response is validated. */
export interface RuntimeReceiptEvidence {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly operation: string;
  readonly sourceKind: string;
  readonly sourceId: string;
  readonly declarationDigest: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly applicationDisposition?: "current_custody" | "stale_custody" | undefined;
  readonly outcome: RuntimeReceiptEvidenceOutcome;
}

/** Minimal metric extension consumed by the receipt evidence recorder. */
export interface RuntimeReceiptEvidenceMetrics extends RuntimeMetricsSink {
  readonly recordReceiptEvidence?: ((outcome: RuntimeReceiptEvidenceOutcome) => void) | undefined;
}

/** Records receipt evidence without allowing metrics or log sinks to affect declaration handling. */
export function recordRuntimeReceiptEvidence(
  logger: RuntimePodLogger | undefined,
  metrics: RuntimeReceiptEvidenceMetrics | undefined,
  evidence: RuntimeReceiptEvidence,
): void {
  try {
    metrics?.recordReceiptEvidence?.(evidence.outcome);
    const applied = evidence.outcome === "applied";
    logger?.info({
      event: applied ? "runtime_receipt_applied" : "runtime_receipt_discarded",
      "event.kind": applied ? "runtime_receipt_applied" : "runtime_receipt_discarded",
      operation: evidence.operation,
      component: "agent-runtime",
      message: applied ? "runtime receipt applied" : "runtime receipt discarded",
      "workspace.id": evidence.workspaceId,
      "session.id": evidence.sessionId,
      "thread.id": evidence.sessionThreadId,
      "binding.id": evidence.bindingId,
      "binding.generation": evidence.bindingGeneration,
      "declaration.source.kind": evidence.sourceKind,
      "declaration.source.id": evidence.sourceId,
      "declaration.digest": evidence.declarationDigest,
      ...(evidence.applicationDisposition === undefined
        ? {}
        : { "receipt.application_disposition": evidence.applicationDisposition }),
      ...(applied ? {} : { "receipt.discard_reason": evidence.outcome }),
    });
  } catch {
    // Receipt application is authoritative; observability is not part of its custody boundary.
  }
}

/** Configures the JSON sink and optional service resource attributes for Runtime Pod logs. */
export interface JsonLoggerOptions {
  readonly write: (line: string) => void;
  readonly serviceName?: string;
  readonly deploymentEnvironment?: string;
  readonly serviceVersion?: string;
}

/** Creates a Runtime Pod JSON logger backed by the shared observability serializer. */
export function createJsonLogger(options: JsonLoggerOptions): RuntimePodLogger {
  return createTetralJsonLogger<RuntimePodLogRecord>({
    write: options.write,
    serviceName: options.serviceName ?? "agent-runtime",
    deploymentEnvironment: options.deploymentEnvironment,
    serviceVersion: options.serviceVersion,
  });
}

/**
 * Builds a startup failure record by retaining the supplied configuration message and collapsing
 * adapter startup failures to a fixed message.
 */
export function startupFailureLogRecord(input: {
  readonly kind: "config_error" | "startup_error";
  readonly message: string;
  readonly cause?: unknown;
}): RuntimePodLogRecord {
  const safeMessage = input.kind === "config_error" ? input.message : "runtime pod startup failed";
  return {
    event: "startup_failed",
    "event.kind": "startup_failed",
    operation: "startup",
    component: "agent-runtime",
    kind: input.kind,
    message: safeMessage,
    ...(input.kind === "startup_error" ? { "startup.cause_class": startupCauseClass(input.cause) } : {}),
    ...semanticErrorFields({ errorClass: input.kind, errorCode: input.kind, messageSafe: safeMessage }),
  };
}

/** Builds the lifecycle record emitted after Runtime Pod listeners are ready. */
export function workloadStartedLogRecord(): RuntimePodLogRecord {
  return {
    event: "workload.started",
    "event.kind": "started",
    operation: "workload.lifecycle",
    component: "workload",
    "listener.transport": "tcp",
    "readiness.state": "ready",
  };
}

/** Emits the started record without allowing a logging sink to change process readiness. */
export function logWorkloadStarted(logger: RuntimePodLogger): void {
  try {
    logger.info(workloadStartedLogRecord());
  } catch {
    // Listener readiness, not observability delivery, determines startup success.
  }
}

function startupCauseClass(cause: unknown): string {
  try {
    if ((typeof cause !== "object" || cause === null) && typeof cause !== "function") {
      return "unknown";
    }
    const constructor = Reflect.get(cause, "constructor") as unknown;
    if ((typeof constructor !== "object" || constructor === null) && typeof constructor !== "function") {
      return "unknown";
    }
    const name = Reflect.get(constructor, "name") as unknown;
    if (
      typeof name === "string" &&
      name.length <= 64 &&
      /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name)
    ) {
      return name;
    }
  } catch {
    // A thrown value may be a revoked Proxy or expose throwing accessors.
  }
  return "unknown";
}

/** Builds a typed shutdown failure record for active-run settlement or drain timeout failure. */
export function shutdownFailureLogRecord(input: {
  readonly event: "shutdown_active_run_settlement_failed" | "shutdown_drain_timeout";
  readonly message: string;
}): RuntimePodLogRecord {
  return {
    event: input.event,
    "event.kind": input.event,
    operation: "shutdown",
    component: "agent-runtime",
    kind: "shutdown_error",
    message: input.message,
    ...semanticErrorFields({ errorClass: "shutdown_error", errorCode: input.event, messageSafe: input.message }),
  };
}

/** Builds one bounded closeout alarm, recovery, or unrepairable-release record. */
export function runtimeCloseoutLogRecord(input: RuntimeCloseoutEvent): RuntimePodLogRecord {
  return {
    event: input.event,
    "event.kind": input.event,
    operation: "runtime_closeout",
    component: "agent-runtime",
    message: input.event,
    "closeout.active_count": input.activeCloseouts,
    ...(input.errorCode !== undefined ? { "closeout.error_code": input.errorCode } : {}),
    ...semanticErrorFields({
      errorClass: "runtime_closeout",
      errorCode: input.errorCode ?? input.event,
      messageSafe: input.event,
    }),
  };
}
