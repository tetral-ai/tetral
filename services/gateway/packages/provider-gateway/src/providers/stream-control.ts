/**
 * @packageDocumentation
 *
 * Applies cancellation and liveness deadlines while pulling an asynchronous
 * provider event stream. The controller consumes at most one iterator result at
 * a time, yields values unchanged, and races each pull against caller abort,
 * the applicable first-event or inter-event watchdog, and one absolute overall
 * budget measured from iteration start. Completion, timeout, abort, and consumer
 * exit all request closure of the upstream iterator.
 *
 * Provider turn orchestration calls this module around a
 * `ProviderRequestStreamer`. It reports typed timeout failures through
 * `ProviderStreamTimeoutError`, invokes the supplied abort callback so provider
 * transport can stop, and otherwise depends only on the injected or system
 * clock.
 */
import { ProviderStreamTimeoutError } from "@tetral/gateway-lowering/src/errors.js";
import type { ProviderStreamTimeoutKind } from "@tetral/gateway-lowering/src/errors.js";

/** Defines cancellation, timeout budgets, and optional test-time clock control. */
export interface ProviderStreamControlOptions {
  readonly abortSignal?: AbortSignal | undefined;
  readonly abortOnTimeout?: ((kind: ProviderStreamTimeoutKind) => void) | undefined;
  readonly firstByteTimeoutMs: number;
  readonly interChunkTimeoutMs: number;
  readonly overallTimeoutMs: number;
  readonly clock?: ProviderStreamClock | undefined;
}

/** Provides timer operations used to test stream deadlines deterministically. */
export interface ProviderStreamClock {
  readonly now: () => number;
  readonly setTimeout: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  readonly clearTimeout: (handle: ReturnType<typeof setTimeout>) => void;
}

/**
 * Yields an upstream provider stream unchanged while enforcing cancellation
 * and first-event, inter-event, and absolute overall deadlines.
 *
 * A timeout throws `ProviderStreamTimeoutError` with the winning deadline kind
 * and calls `abortOnTimeout`; caller cancellation throws an abort error. The
 * controller never reads ahead or buffers provider events.
 */
export async function* controlProviderStream<T>(
  events: AsyncIterable<T>,
  options: ProviderStreamControlOptions,
): AsyncGenerator<T> {
  const iterator = events[Symbol.asyncIterator]();
  const clock = options.clock ?? SystemClock;
  const startedAt = clock.now();
  let first = true;
  try {
    while (true) {
      const result = await nextControlled(iterator, {
        abortSignal: options.abortSignal,
        abortOnTimeout: options.abortOnTimeout,
        clock,
        deadline: {
          first,
          startedAt,
          firstByteTimeoutMs: options.firstByteTimeoutMs,
          interChunkTimeoutMs: options.interChunkTimeoutMs,
          overallTimeoutMs: options.overallTimeoutMs,
        },
      });
      if (result.done === true) {
        return;
      }
      first = false;
      yield result.value;
    }
  } finally {
    const returned = iterator.return?.();
    returned?.catch(() => undefined);
  }
}

const SystemClock: ProviderStreamClock = {
  now: () => Date.now(),
  setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
  clearTimeout: (handle) => clearTimeout(handle),
};

interface ControlledNextOptions {
  readonly abortSignal?: AbortSignal | undefined;
  readonly abortOnTimeout?: ((kind: ProviderStreamTimeoutKind) => void) | undefined;
  readonly clock: ProviderStreamClock;
  readonly deadline: ProviderStreamDeadline;
}

interface ProviderStreamDeadline {
  readonly first: boolean;
  readonly startedAt: number;
  readonly firstByteTimeoutMs: number;
  readonly interChunkTimeoutMs: number;
  readonly overallTimeoutMs: number;
}

async function nextControlled<T>(
  iterator: AsyncIterator<T>,
  options: ControlledNextOptions,
): Promise<IteratorResult<T>> {
  options.abortSignal?.throwIfAborted();
  const next = iterator.next();
  next.catch(() => undefined);
  const raceItems: Array<Promise<IteratorResult<T>> | Promise<never>> = [next];
  const timeout = computeTimeout(options.deadline, options.clock);
  let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_resolve, reject) => {
    timeoutHandle = options.clock.setTimeout(() => {
      reject(new ProviderStreamTimeoutError(timeout.kind));
      options.abortOnTimeout?.(timeout.kind);
    }, timeout.delayMs);
  });
  raceItems.push(timeoutPromise);
  let abortListener: (() => void) | undefined;
  const signal = options.abortSignal;
  if (signal !== undefined) {
    const abortPromise = new Promise<never>((_resolve, reject) => {
      abortListener = () => reject(abortError(signal));
      signal.addEventListener("abort", abortListener, { once: true });
    });
    raceItems.push(abortPromise);
  }
  try {
    return await Promise.race(raceItems);
  } finally {
    if (timeoutHandle !== undefined) {
      options.clock.clearTimeout(timeoutHandle);
    }
    if (abortListener !== undefined) {
      signal?.removeEventListener("abort", abortListener);
    }
  }
}

interface ComputedTimeout {
  readonly delayMs: number;
  readonly kind: ProviderStreamTimeoutKind;
}

function computeTimeout(deadline: ProviderStreamDeadline, clock: ProviderStreamClock): ComputedTimeout {
  const elapsedMs = Math.max(0, clock.now() - deadline.startedAt);
  const overallRemainingMs = Math.max(0, deadline.overallTimeoutMs - elapsedMs);
  const chunkTimeoutMs = deadline.first ? deadline.firstByteTimeoutMs : deadline.interChunkTimeoutMs;
  const delayMs = Math.min(chunkTimeoutMs, overallRemainingMs);
  const kind = overallRemainingMs <= chunkTimeoutMs
    ? "overall_timeout"
    : deadline.first ? "first_byte_timeout" : "inter_chunk_timeout";
  return {
    delayMs,
    kind,
  };
}

function abortError(signal: AbortSignal): DOMException {
  if (signal.reason instanceof DOMException && signal.reason.name === "AbortError") {
    return signal.reason;
  }
  return new DOMException("Provider stream aborted.", "AbortError");
}
