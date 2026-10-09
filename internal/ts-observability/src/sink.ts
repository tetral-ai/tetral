/** Writable stream subset; listeners are installed once, never per record. */
export interface DiagnosticStream {
  readonly writableLength: number;
  readonly destroyed: boolean;
  write(line: string): boolean;
  on(event: "drain", listener: () => void): unknown;
  on(event: "error", listener: (error: unknown) => void): unknown;
  off(event: "drain", listener: () => void): unknown;
  off(event: "error", listener: (error: unknown) => void): unknown;
}
const streamErrors = new WeakMap<DiagnosticStream, { failures: number; failed: boolean }>();

/**
 * Production stderr adapter. No private queue: after backpressure reject writes
 * until drain. The stream's acceptance ceiling bounds existing buffered bytes.
 * Shutdown never waits for diagnostic flush; stderr remains process-owned.
 */
export function createDiagnosticStreamSink(stream: DiagnosticStream, maxBufferedBytes = 65_536): {
  readonly write: (line: string) => boolean;
  readonly close: () => void;
  readonly stats: () => { readonly dropped: number; readonly failures: number; readonly blocked: boolean };
} {
  let blocked = false, closed = false, dropped = 0, failures = 0;
  let errors = streamErrors.get(stream);
  if (errors === undefined) {
    errors = { failures: 0, failed: false }; const state = errors;
    stream.on("error", () => { state.failures++; state.failed = true; });
    streamErrors.set(stream, state);
  }
  const errorBaseline = errors.failures;
  const drain = (): void => { blocked = false; };
  stream.on("drain", drain);
  return {
    write: (line) => {
      if (closed || errors.failed || stream.destroyed || blocked || stream.writableLength + new TextEncoder().encode(line).byteLength > maxBufferedBytes) { dropped++; return false; }
      try { blocked = !stream.write(line); return true; }
      catch { failures++; dropped++; return false; }
    },
    // Accepted asynchronous writes can still fail after close; retain error handler.
    close: () => { closed = true; stream.off("drain", drain); },
    stats: () => ({ dropped, failures: failures + errors.failures - errorBaseline, blocked }),
  };
}
