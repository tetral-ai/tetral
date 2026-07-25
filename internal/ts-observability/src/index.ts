/**
 * @packageDocumentation
 *
 * Provides the shared structured JSON logger used by the Runtime Pod, Provider
 * Gateway, and MCP Connector service logger modules. Those callers specialize
 * the record type and supply a synchronous sink; this package delegates
 * serialization to `JSON.stringify` and passes each resulting JSON line to that
 * sink. Production callers currently select standard error, while tests may
 * inject an in-memory sink. This package does not collect metrics or open a
 * listener.
 *
 * Each `info` or `error` call emits one JSON object followed by a newline. The
 * logger-supplied `level` and the configured or defaulted `service.name`,
 * `service.version`, and `deployment.environment` fields overwrite fields with
 * the same names in the caller's record. Missing optional resource options use
 * `"unknown"`; record fields whose value is `undefined` are omitted.
 *
 * Redaction is a syntactic, field-level safeguard: matching sensitive key names
 * or token-shaped string values replace the complete scalar value with
 * `"[REDACTED]"`. It is not a complete semantic-safety boundary and does not
 * prove that arbitrary messages or unmatched values are safe. Callers remain
 * responsible for supplying only approved, bounded operational fields and for
 * excluding raw secrets, payloads, content, dependency bodies, and stack
 * traces.
 */

/** Scalar field values accepted by the shared structured log record. */
export type TetralLogFieldValue = string | number | boolean | undefined;

/**
 * Defines the common structured fields and permits service-specific scalar
 * fields. An `undefined` value requests omission from the emitted record.
 */
type TetralLogRecordFields = {
  readonly event?: string;
  readonly kind?: string;
  readonly message?: string;
  readonly operation?: string;
  readonly "event.kind"?: string;
  readonly component?: string;
  readonly "duration.ms"?: number;
  readonly retryable?: boolean;
  readonly terminal?: boolean;
  readonly "request.id"?: string;
  readonly "workspace.id"?: string;
  readonly "session.id"?: string;
  readonly "thread.id"?: string;
  readonly "job.id"?: string;
  readonly "queue.kind"?: string;
  readonly "binding.id"?: string;
  readonly "sandbox.id"?: string;
  readonly "cleanup.id"?: string;
  readonly "provider.request.id"?: string;
  readonly "trace.id"?: string;
  readonly "span.id"?: string;
  readonly "service.name"?: string;
  readonly "service.version"?: string;
  readonly "deployment.environment"?: string;
  readonly [key: string]: TetralLogFieldValue;
};

type TetralSemanticErrorFields = {
  readonly [semanticErrorOutcome]: true;
  readonly "error.class": string;
  readonly "error.code": string;
  readonly "error.message_safe": string;
};

type TetralNonErrorFields = {
  readonly [semanticErrorOutcome]?: false | undefined;
  readonly "error.class"?: never;
  readonly "error.code"?: never;
  readonly "error.message_safe"?: never;
};

/** Marks a record as a semantic failure without adding a serialized log field. */
export const semanticErrorOutcome: unique symbol = Symbol("tetral.semantic-error-outcome");

/**
 * Requires semantic failure records to carry the complete shared error tuple,
 * independently of the severity method used to emit them.
 */
export type TetralLogRecord = TetralLogRecordFields & (TetralSemanticErrorFields | TetralNonErrorFields);

/** Builds the complete shared tuple and internal discriminator for a semantic failure record. */
export function semanticErrorFields(input: {
  readonly errorClass: string;
  readonly errorCode: string;
  readonly messageSafe: string;
}): TetralSemanticErrorFields {
  return {
    [semanticErrorOutcome]: true,
    "error.class": input.errorClass,
    "error.code": input.errorCode,
    "error.message_safe": input.messageSafe,
  };
}

/**
 * Exposes synchronous severity methods for a shared or service-specialized log
 * record. Each method serializes and writes exactly one newline-terminated JSON
 * object; synchronous serialization or sink exceptions propagate to the caller.
 * The logger does not observe asynchronous stream errors, flushing, or
 * backpressure after the sink returns.
 */
export interface TetralJsonLogger<TRecord extends TetralLogRecord = TetralLogRecord> {
  readonly info: (record: TRecord) => void;
  readonly error: (record: TRecord) => void;
}

/**
 * Configures the synchronous JSON-line sink and the resource fields attached to
 * every record. Optional resource fields default to `"unknown"`.
 */
export interface TetralJsonLoggerOptions {
  readonly write: (line: string) => void;
  readonly serviceName: string;
  readonly deploymentEnvironment?: string | undefined;
  readonly serviceVersion?: string | undefined;
}

/**
 * Creates a logger for the shared record contract or a service-specific
 * extension. Logger-owned severity and resource fields take precedence over
 * same-named fields supplied in a record.
 */
export function createTetralJsonLogger<TRecord extends TetralLogRecord = TetralLogRecord>(
  options: TetralJsonLoggerOptions,
): TetralJsonLogger<TRecord> {
  const base = {
    "service.name": options.serviceName,
    "deployment.environment": options.deploymentEnvironment ?? "unknown",
    "service.version": options.serviceVersion ?? "unknown",
  };
  const emit = (level: "info" | "error", record: TRecord): void => {
    options.write(`${JSON.stringify({ ...redactLogRecord(record), level, ...base })}\n`);
  };
  return {
    info: (record) => emit("info", record),
    error: (record) => emit("error", record),
  };
}

const redacted = "[REDACTED]";
const sensitiveKeyPattern = /(?:^|[._-])(?:authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|session[_-]?token|secret|password|engine[_-]?vault[_-]?key)(?:[._-]|$)|^(?:authorizationToken|apiKey|accessToken|refreshToken|sessionToken|clientSecret|secretAccessKey|engineVaultKey)$/i;
const sensitiveValuePattern = /\bBearer\s+\S+|(?:sk|ant|ghp|github)[-_][A-Za-z0-9._-]{8,}|(?:access_token|refresh_token|client_secret|accessToken|refreshToken|clientSecret|token)=/i;

function redactLogRecord<TRecord extends TetralLogRecord>(record: TRecord): Record<string, TetralLogFieldValue> {
  const next: Record<string, TetralLogFieldValue> = {};
  for (const [key, value] of Object.entries(record)) {
    if (value === undefined) {
      continue;
    }
    if (sensitiveKeyPattern.test(key)) {
      next[key] = redacted;
      continue;
    }
    if (typeof value === "string" && sensitiveValuePattern.test(value)) {
      next[key] = redacted;
      continue;
    }
    next[key] = value;
  }
  return next;
}
