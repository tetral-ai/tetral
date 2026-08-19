/**
 * @packageDocumentation
 *
 * Applies cancellation and liveness deadlines while pulling an asynchronous
 * provider event stream. The controller consumes at most one iterator result at
 * a time, yields values unchanged, and races each pull against caller abort,
 * the applicable first-event or inter-event watchdog, a semantic-progress
 * watchdog, and one absolute overall budget measured from iteration start.
 * Completion, timeout, abort, and consumer exit all request closure of the
 * upstream iterator.
 *
 * Provider turn orchestration calls this module around a
 * `ProviderRequestStreamer`. It reports typed timeout failures through
 * `ProviderStreamTimeoutError`, invokes the supplied abort callback so provider
 * transport can stop, and otherwise depends only on the injected or system
 * clock.
 */
import { ProviderStreamTimeoutError } from "@tetral/gateway-lowering/src/errors.js";
import type { ProviderStreamTimeoutKind } from "@tetral/gateway-lowering/src/errors.js";

export type ProviderStreamWatchdogKind =
  | ProviderStreamTimeoutKind
  | "semantic_progress_timeout";

class ProviderSemanticProgressError extends Error {
  constructor() {
    super("Provider stream made no semantic progress.");
    this.name = "ProviderSemanticProgressError";
  }
}

/** Defines cancellation, timeout budgets, and optional test-time clock control. */
export interface ProviderStreamControlOptions {
  readonly abortSignal?: AbortSignal | undefined;
  readonly abortOnTimeout?: ((kind: ProviderStreamWatchdogKind) => void) | undefined;
  readonly firstByteTimeoutMs: number;
  readonly interChunkTimeoutMs: number;
  readonly semanticProgressTimeoutMs: number;
  readonly overallTimeoutMs: number;
  readonly isSemanticProgress: (event: unknown) => boolean;
  readonly transportActivityAt?: (() => number) | undefined;
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
 * and first-event, inter-event, semantic-progress, and absolute overall
 * deadlines. Transport-only markers may keep the inter-event watchdog alive,
 * but only caller-classified content progress re-arms the semantic watchdog.
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
  let lastSemanticProgressAt = startedAt;
  let lastNormalizedEventAt = startedAt;
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
          lastSemanticProgressAt,
          lastNormalizedEventAt,
          transportActivityAt: options.transportActivityAt,
          firstByteTimeoutMs: options.firstByteTimeoutMs,
          interChunkTimeoutMs: options.interChunkTimeoutMs,
          semanticProgressTimeoutMs: options.semanticProgressTimeoutMs,
          overallTimeoutMs: options.overallTimeoutMs,
        },
      });
      if (result.done === true) {
        return;
      }
      first = false;
      lastNormalizedEventAt = clock.now();
      if (options.isSemanticProgress(result.value)) {
        lastSemanticProgressAt = lastNormalizedEventAt;
      }
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
  readonly abortOnTimeout?: ((kind: ProviderStreamWatchdogKind) => void) | undefined;
  readonly clock: ProviderStreamClock;
  readonly deadline: ProviderStreamDeadline;
}

interface ProviderStreamDeadline {
  readonly first: boolean;
  readonly startedAt: number;
  readonly lastSemanticProgressAt: number;
  readonly lastNormalizedEventAt: number;
  readonly transportActivityAt?: (() => number) | undefined;
  readonly firstByteTimeoutMs: number;
  readonly interChunkTimeoutMs: number;
  readonly semanticProgressTimeoutMs: number;
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
  let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_resolve, reject) => {
    const arm = (): void => {
      const timeout = computeTimeout(options.deadline, options.clock);
      timeoutHandle = options.clock.setTimeout(() => {
        const expired = computeTimeout(options.deadline, options.clock);
        if (expired.delayMs > 0) {
          arm();
          return;
        }
        reject(
          expired.kind === "semantic_progress_timeout"
            ? new ProviderSemanticProgressError()
            : new ProviderStreamTimeoutError(expired.kind),
        );
        options.abortOnTimeout?.(expired.kind);
      }, timeout.delayMs);
    };
    arm();
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
  readonly kind: ProviderStreamWatchdogKind;
}

function computeTimeout(deadline: ProviderStreamDeadline, clock: ProviderStreamClock): ComputedTimeout {
  const now = clock.now();
  const elapsedMs = Math.max(0, now - deadline.startedAt);
  const overallRemainingMs = Math.max(0, deadline.overallTimeoutMs - elapsedMs);
  const lastTransportActivityAt = Math.max(
    deadline.lastNormalizedEventAt,
    deadline.transportActivityAt?.() ?? deadline.startedAt,
  );
  const chunkRemainingMs = deadline.first
    ? Math.max(0, deadline.firstByteTimeoutMs - elapsedMs)
    : Math.max(
        0,
        deadline.interChunkTimeoutMs -
          Math.max(0, now - lastTransportActivityAt),
      );
  const semanticRemainingMs = Math.max(
    0,
    deadline.semanticProgressTimeoutMs -
      Math.max(0, now - deadline.lastSemanticProgressAt),
  );
  const delayMs = Math.min(chunkRemainingMs, semanticRemainingMs, overallRemainingMs);
  const kind = overallRemainingMs <= chunkRemainingMs && overallRemainingMs <= semanticRemainingMs
    ? "overall_timeout"
    : chunkRemainingMs <= semanticRemainingMs
      ? deadline.first
        ? "first_byte_timeout"
        : "inter_chunk_timeout"
      : "semantic_progress_timeout";
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
