/**
 * What a failure record takes for error.message: the text of an error, or
 * nothing. Its text comes only from this component's own code, the operating
 * system, a linked library (including its view of a remote dependency) or a
 * helper process before Session work; never from what the work of a Session
 * or a peer product returned.
 */
export interface FailureCause {
  readonly text: string;
}

/** The cause of a failure whose only text is of a source its code declares with no_cause. */
export const noCause: FailureCause = Object.freeze({ text: "" });

const causeChainDepth = 8;

/**
 * Takes an error's text as the cause: its message followed by the messages
 * of its `cause` chain, as a wrapped Go error reads.
 */
export function causeOf(error: unknown): FailureCause {
  const parts: string[] = [];
  let current: unknown = error;
  for (let depth = 0; depth < causeChainDepth && current !== undefined && current !== null; depth++) {
    let text = notAnError, next: unknown;
    // A thrown value may refuse to be read, as a revoked proxy does.
    try {
      if (typeof current === "string") text = current;
      else if (typeof current === "object") {
        const { message, cause } = current as { message?: unknown; cause?: unknown };
        if (typeof message === "string") text = message;
        next = cause;
      } else if (typeof current !== "function" && typeof current !== "symbol") text = String(current);
    } catch { /* text stays notAnError */ }
    if (text !== "" && !parts.some((part) => part.includes(text))) parts.push(text);
    current = next;
  }
  return { text: shapeCause(parts.join(": ")) };
}

const notAnError = "a value that is not an error was thrown";

const causeLimitBytes = 1024;
const causeUrlPattern = /[A-Za-z][A-Za-z0-9+.-]*:\/\/[^\s"'<>`]+/g;
const causeUrlHostPattern = /^(?:\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._~%-]*)(?::[0-9]*)?$/;

/**
 * Keeps of each URL its scheme, host and path, turns control characters into
 * spaces, and cuts the text at 1,024 UTF-8 bytes. The Go library's shapeCause
 * is the same function; test-support/cause-shaping.json holds the rows both
 * are tested with.
 */
export function shapeCause(text: string): string {
  const shaped = text.replace(causeUrlPattern, shapeCauseUrl).replace(/[\x00-\x1f\x7f]/g, " ");
  const bytes = Buffer.from(shaped, "utf8");
  if (bytes.length <= causeLimitBytes) return shaped;
  let end = causeLimitBytes;
  while (end > 0 && ((bytes[end] as number) & 0xc0) === 0x80) end--;
  return `${bytes.subarray(0, end).toString("utf8")}[TRUNCATED]`;
}

function shapeCauseUrl(raw: string): string {
  const trimmed = raw.replace(/[.,;:!?)\]}]+$/, "");
  const separator = trimmed.indexOf("://");
  const scheme = trimmed.slice(0, separator);
  const rest = trimmed.slice(separator + 3);
  const authorityEnd = firstIndexOf(rest, "/?#");
  const authority = rest.slice(0, authorityEnd);
  const host = authority.slice(authority.lastIndexOf("@") + 1);
  if (!causeUrlHostPattern.test(host)) return `${scheme}://[REDACTED]${raw.slice(trimmed.length)}`;
  const tail = rest.slice(authorityEnd);
  return `${scheme}://${host}${tail.slice(0, firstIndexOf(tail, "?#"))}${raw.slice(trimmed.length)}`;
}

/** The index of the first of chars in text, or its length. */
function firstIndexOf(text: string, chars: string): number {
  for (let i = 0; i < text.length; i++) if (chars.includes(text[i] as string)) return i;
  return text.length;
}

/** Marks a record as a failure built with {@link semanticErrorFields}; it adds no serialized field. */
export const semanticErrorOutcome: unique symbol = Symbol("tetral.semantic-error-outcome");
export const failureCauseText: unique symbol = Symbol("tetral.failure-cause-text");

export type TetralSemanticErrorFields = {
  readonly [semanticErrorOutcome]: true;
  readonly [failureCauseText]: string;
  readonly "error.code": string;
};

/**
 * The one way a failure record is built: spread into the record of an event,
 * it gives the record `errorCode` and `cause`. The logger adds the class and
 * safe text the event declares for the code and writes the cause as
 * `error.message`. A code the event does not declare, and a cause that
 * disagrees with the code's declaration, fail a test run; production then
 * writes `diagnostic.invalid_event` for the first and follows the declaration
 * for the second.
 */
export function semanticErrorFields(input: {
  readonly errorCode: string;
  readonly cause: FailureCause;
}): TetralSemanticErrorFields {
  return {
    [semanticErrorOutcome]: true,
    [failureCauseText]: input.cause.text,
    "error.code": input.errorCode,
  };
}
