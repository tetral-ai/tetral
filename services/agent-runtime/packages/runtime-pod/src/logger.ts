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
};

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
}): RuntimePodLogRecord {
  const safeMessage = input.kind === "config_error" ? input.message : "runtime pod startup failed";
  return {
    event: "startup_failed",
    "event.kind": "startup_failed",
    operation: "startup",
    component: "agent-runtime",
    kind: input.kind,
    message: safeMessage,
    ...semanticErrorFields({ errorClass: input.kind, errorCode: input.kind, messageSafe: safeMessage }),
  };
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
