/**
 * @packageDocumentation
 * Provides Runtime's session-event normalization and a generic retrying writer.
 * It guards envelope validation, stable write identity, bounded retry and timeout
 * behavior, ACK matching, and public session.error projection at durable emission.
 * Unit-level callers exercise the generic writer; the production BridgeAPI writer
 * reuses sessionEventForDurableWrite while owning its transport policy directly.
 */
import type {
  RuntimeFailure,
  SessionEventWriterAppendEvent,
  SessionEventEnvelope,
  SessionEventWriter,
  SessionEventWriterAppendResult,
} from "../contracts/runtime.js";
import {
  RuntimeFailureSchema,
  SessionEventWriterAppendEventSchema,
  SessionEventEnvelopeSchema,
  SessionEventWriterAppendResultSchema,
  SessionEventWriterRetryPolicy,
  isRuntimeTerminationFailure,
  normalizeSessionEventWriterError,
} from "../contracts/runtime.js";

/** Bridge-facing transport used to append one durable event envelope. */
export interface SessionEventWriterTransport {
  readonly append: (envelope: SessionEventEnvelope) => Promise<unknown>;
}

/** Transport and timing dependencies for the generic retrying writer. */
export interface SessionEventWriterOptions extends SessionEventWriterTransport {
  readonly sleep: (durationMs: number) => Promise<boolean>;
}

/** Creates the retrying, ACK-validating durable event writer. */
export function createSessionEventWriter(options: SessionEventWriterOptions): Pick<SessionEventWriter, "append"> {
  return {
    append: async (input) => {
      const parsed = SessionEventEnvelopeSchema.parse(input);
      const envelope = SessionEventEnvelopeSchema.parse({
        ...parsed,
        event: sessionEventForDurableWrite(parsed.event),
      });
      let lastFailure: SessionEventWriterAppendResult | undefined;

      for (let attempt = 1; attempt <= SessionEventWriterRetryPolicy.attempts; attempt += 1) {
        const result = await appendWithTimeout(options, envelope);
        if (result.ok) {
          if (result.writeId !== envelope.writeId) {
            return {
              ok: false,
              error: normalizeSessionEventWriterError({
                code: "ack_mismatch",
                sessionId: envelope.sessionId,
                writeId: envelope.writeId,
              }),
            };
          }
          return result;
        }

        lastFailure = result;
        if (!result.error.retryable || attempt === SessionEventWriterRetryPolicy.attempts) {
          return result;
        }
        const backoffMs = SessionEventWriterRetryPolicy.backoffMs[attempt - 1] ?? 0;
        if (backoffMs > 0) {
          await options.sleep(backoffMs);
        }
      }

      return lastFailure ?? {
        ok: false,
        error: normalizeSessionEventWriterError({
          code: "unknown",
          sessionId: envelope.sessionId,
          writeId: envelope.writeId,
        }),
      };
    },
  };
}

/** Maps internal session.error payloads to their public durable event member. */
export function sessionEventForDurableWrite(event: SessionEventWriterAppendEvent): SessionEventWriterAppendEvent {
  if (event.type !== "session.error") {
    return event;
  }
  const failure = RuntimeFailureSchema.safeParse(event.error);
  if (!failure.success) {
    return event;
  }
  return SessionEventWriterAppendEventSchema.parse({
    type: "session.error",
    error: publicSessionError(failure.data),
  });
}

// session.error public member mapping, applied only on the durable write. Maps a
// RuntimeFailure onto the SDK error-union member:
//
// | failure                                | session.error member       |
// | -------------------------------------- | -------------------------- |
// | type != provider                       | unknown_error              |
// | provider, code=provider_rate_limited   | model_rate_limited_error   |
// | provider, retryStatus=exhausted        | model_overloaded_error     |
// | provider, otherwise                    | model_request_failed_error |
// (MCP members pass through unchanged elsewhere; billing/credential-host failures
// have no member and fall through to unknown_error.)
//
// retry_status (publicRetryStatus): ThreadLoop stamps exhausted on RuntimeFailure
// values when its retry lifecycle spends the budget; this mapper preserves that
// stamp, recognizes real termination as terminal, and otherwise defaults to
// retrying because it has no settlement-disposition input. Bridge-owned exhaustion
// paths emit their public errors independently.
// UPDATE-WITH: services/bridge/runtime_pod_lost.go,
//              services/bridge/runtime_termination.go
function publicSessionError(failure: RuntimeFailure): {
  readonly type: "model_overloaded_error" | "model_rate_limited_error" | "model_request_failed_error" | "unknown_error";
  readonly message: string;
  readonly retry_status: { readonly type: "retrying" | "exhausted" | "terminal" };
} {
  const type = failure.type !== "provider"
    ? "unknown_error"
    : failure.code === "provider_rate_limited"
    ? "model_rate_limited_error"
    : failure.retryStatus?.type === "exhausted"
    ? "model_overloaded_error"
    : "model_request_failed_error";
  return {
    type,
    message: failure.message,
    retry_status: publicRetryStatus(failure),
  };
}

function publicRetryStatus(failure: RuntimeFailure): { readonly type: "retrying" | "exhausted" | "terminal" } {
  if (failure.retryStatus?.type === "exhausted") {
    return { type: "exhausted" };
  }
  if (isRuntimeTerminationFailure(failure)) {
    return { type: "terminal" };
  }
  // Unstamped non-terminal failures reach this mapper only from in-run emissions.
  // Idle-accompanying closeout paths stamp exhausted before they write.
  return { type: "retrying" };
}

async function appendWithTimeout(
  options: SessionEventWriterOptions,
  envelope: SessionEventEnvelope,
): Promise<SessionEventWriterAppendResult> {
  return await Promise.race([
    appendOnce(options, envelope),
    options.sleep(SessionEventWriterRetryPolicy.timeoutPerAttemptMs).then((): SessionEventWriterAppendResult => ({
      ok: false,
      error: normalizeSessionEventWriterError({
        code: "timeout",
        sessionId: envelope.sessionId,
        writeId: envelope.writeId,
      }),
    })),
  ]);
}

async function appendOnce(
  options: SessionEventWriterTransport,
  envelope: SessionEventEnvelope,
): Promise<SessionEventWriterAppendResult> {
  try {
    const result = await options.append(envelope);
    const parsed = SessionEventWriterAppendResultSchema.safeParse(result);
    if (!parsed.success) {
      return {
        ok: false,
        error: normalizeSessionEventWriterError({
          code: "schema_mismatch",
          sessionId: envelope.sessionId,
          writeId: envelope.writeId,
        }),
      };
    }
    return parsed.data;
  } catch (error) {
    return {
      ok: false,
      error: normalizeSessionEventWriterError({
        code: "unknown",
        rawError: error,
        sessionId: envelope.sessionId,
        writeId: envelope.writeId,
      }),
    };
  }
}
