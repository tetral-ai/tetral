/**
 * @packageDocumentation
 *
 * Specializes the shared TypeScript observability logger for provider-gateway
 * records and startup failures. Process composition creates loggers here, then
 * the application, service shell, and credential pool emit structured records
 * through the returned interface. Encoding, protected resource fields, and
 * sensitive-value redaction remain owned by the shared logger. Configuration
 * failures preserve their supplied safe message; other startup failures are
 * reduced to a fixed public diagnostic.
 */

import { createTetralJsonLogger, semanticErrorFields } from "@tetral/ts-observability";
import type { TetralJsonLogger, TetralLogRecord } from "@tetral/ts-observability";

/** Extends shared structured records with provider-gateway event classification fields. */
export type GatewayLogRecord = TetralLogRecord & {
  readonly event?: string;
  readonly kind?: string;
};

/** Defines the shared JSON logger specialized for provider-gateway records. */
export type GatewayLogger = TetralJsonLogger<GatewayLogRecord>;

/** Creates a structured logger whose default service identity is `gateway`. */
export function createJsonLogger(options: {
  readonly write: (line: string) => void;
  readonly serviceName?: string;
  readonly deploymentEnvironment?: string;
  readonly serviceVersion?: string;
}): GatewayLogger {
  return createTetralJsonLogger<GatewayLogRecord>({
    write: options.write,
    serviceName: options.serviceName ?? "gateway",
    deploymentEnvironment: options.deploymentEnvironment,
    serviceVersion: options.serviceVersion,
  });
}

/**
 * Builds a structured startup-failure record, retaining configuration messages
 * and replacing other failure details with a fixed bounded message.
 */
export function startupFailureLogRecord(input: {
  readonly kind: "config_error" | "startup_error";
  readonly message: string;
}): GatewayLogRecord {
  const safeMessage = input.kind === "config_error" ? input.message : "gateway service startup failed";
  return {
    event: "startup_failed",
    "event.kind": "startup_failed",
    operation: "startup",
    component: "gateway",
    kind: input.kind,
    message: safeMessage,
    ...semanticErrorFields({ errorClass: input.kind, errorCode: input.kind, messageSafe: safeMessage }),
  };
}

/** Builds the lifecycle record emitted after both Gateway listeners are ready. */
export function workloadStartedLogRecord(): GatewayLogRecord {
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
export function logWorkloadStarted(logger: GatewayLogger): void {
  try {
    logger.info(workloadStartedLogRecord());
  } catch {
    // Listener readiness, not observability delivery, determines startup success.
  }
}
