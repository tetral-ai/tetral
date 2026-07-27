/**
 * Coordinates Runtime Pod bootstrap, readiness, command admission, and bounded shutdown draining.
 *
 * Readiness follows successful ordered bootstrap, commands enter only while the pod is ready and
 * accepting, and shutdown closes admission before requesting local active-run interruption and
 * waiting for the hook plus tracked commands. `createRuntimePodApp` drives startup and shutdown,
 * the HTTP and metrics surfaces inspect
 * lifecycle state, and `RuntimeControlService` submits lease-aware commands. This module calls only
 * injected bootstrap hooks, shutdown hooks, and the structured logger.
 */
import { status } from "@grpc/grpc-js";
import type { RuntimePodConfigResult } from "./config.js";
import type { RuntimePodLogger } from "./logger.js";
import { shutdownFailureLogRecord, startupFailureLogRecord } from "./logger.js";
import { GrpcStatusError } from "./errors.js";

export { GrpcStatusError } from "./errors.js";

/**
 * Ordered startup hooks for the runtime shell, Runtime Core, gRPC server, and auth client.
 */
export interface RuntimePodBootstrap {
  readonly runtime: () => Promise<void>;
  readonly core: () => Promise<void>;
  readonly grpc: () => Promise<void>;
  readonly authClient: () => Promise<void>;
}

/**
 * Runtime-owned active-run interruption and local joining that participate in the bounded command drain.
 */
export interface RuntimePodShutdownHooks {
  readonly shutdownActiveRuns?: () => Promise<void>;
}

/**
 * Shutdown-aware lease supplied to one admitted command.
 *
 * The signal and registered handlers fire only when the shutdown drain times out. The lease exposes
 * an unregister callback for each handler and a check callers can place before publishing local state.
 */
export interface RuntimeCommandLease {
  readonly signal: AbortSignal;
  readonly throwIfAborted: () => void;
  readonly onAbort: (handler: () => void) => () => void;
}

/**
 * Dependencies and optional drain bound used by `RuntimePodLifecycle`.
 */
export interface RuntimePodLifecycleOptions {
  readonly config: RuntimePodConfigResult;
  readonly logger: RuntimePodLogger;
  readonly bootstrap: RuntimePodBootstrap;
  readonly shutdownHooks?: RuntimePodShutdownHooks;
  readonly drainTimeoutMs?: number;
}

interface TrackedCommand<T> {
  readonly promise: Promise<T>;
  readonly fail: (error: GrpcStatusError) => void;
}

/**
 * Owns process-local readiness and the set of commands participating in shutdown drain.
 */
export class RuntimePodLifecycle {
  private readyFlag = false;
  private accepting = false;
  private readonly inFlight = new Set<TrackedCommand<unknown>>();

  constructor(private readonly options: RuntimePodLifecycleOptions) {}

  /** Returns process liveness independently of startup readiness or drain state. */
  health(): { readonly ok: true } {
    return { ok: true };
  }

  /** Returns whether startup completed and command admission remains open. */
  ready(): { readonly ready: boolean } {
    return { ready: this.readyFlag };
  }

  /** Returns the lifecycle gauges consumed by the Runtime Pod metrics endpoint. */
  metricsSnapshot(): {
    readonly ready: boolean;
    readonly accepting: boolean;
    readonly inFlightCommands: number;
  } {
    return {
      ready: this.readyFlag,
      accepting: this.accepting,
      inFlightCommands: this.inFlight.size,
    };
  }

  /**
   * Runs bootstrap hooks in dependency order and opens readiness only after every hook succeeds.
   * Startup failures are sanitized, logged, and represented by a non-ready lifecycle.
   */
  async start(): Promise<void> {
    if (!this.options.config.ok) {
      this.options.logger.error(startupFailureLogRecord(this.options.config.error));
      this.readyFlag = false;
      this.accepting = false;
      return;
    }
    try {
      await this.options.bootstrap.runtime();
      await this.options.bootstrap.core();
      await this.options.bootstrap.grpc();
      await this.options.bootstrap.authClient();
      this.readyFlag = true;
      this.accepting = true;
    } catch (error) {
      this.options.logger.error(startupFailureLogRecord({ kind: "startup_error", message: "runtime pod startup failed", cause: error }));
      this.readyFlag = false;
      this.accepting = false;
    }
  }

  /**
   * Adds an already-running promise to the shutdown drain without providing it an abort signal.
   */
  trackCommand<T>(command: Promise<T>): Promise<T> {
    if (!this.readyFlag || !this.accepting) {
      throw new GrpcStatusError(status.FAILED_PRECONDITION, "runtime pod shutting down");
    }
    let fail: (error: GrpcStatusError) => void = () => undefined;
    const shutdownFailure = new Promise<T>((_resolve, reject) => {
      fail = reject;
    });
    const tracked: TrackedCommand<T> = {
      promise: Promise.race([command, shutdownFailure]),
      fail,
    };
    this.inFlight.add(tracked as TrackedCommand<unknown>);
    void tracked.promise.then(() => {
      this.inFlight.delete(tracked as TrackedCommand<unknown>);
    }, () => {
      this.inFlight.delete(tracked as TrackedCommand<unknown>);
    });
    return tracked.promise;
  }

  /**
   * Admits a command with a lease whose signal and callbacks abort when shutdown exhausts its drain
   * budget, while the returned promise participates in the in-flight count.
   */
  runCommand<T>(command: (lease: RuntimeCommandLease) => Promise<T>): Promise<T> {
    if (!this.readyFlag || !this.accepting) {
      throw new GrpcStatusError(status.FAILED_PRECONDITION, "runtime pod shutting down");
    }
    const controller = new AbortController();
    const abortHandlers = new Set<() => void>();
    const abortError = () => new GrpcStatusError(status.FAILED_PRECONDITION, "runtime pod shutdown drain timed out");
    const lease: RuntimeCommandLease = {
      signal: controller.signal,
      throwIfAborted: () => {
        if (controller.signal.aborted) {
          throw abortError();
        }
      },
      onAbort: (handler) => {
        if (controller.signal.aborted) {
          handler();
          return () => undefined;
        }
        abortHandlers.add(handler);
        return () => {
          abortHandlers.delete(handler);
        };
      },
    };
    let fail: (error: GrpcStatusError) => void = () => undefined;
    const shutdownFailure = new Promise<T>((_resolve, reject) => {
      fail = reject;
    });
    const commandPromise = Promise.resolve().then(() => command(lease));
    const tracked: TrackedCommand<T> = {
      promise: Promise.race([commandPromise, shutdownFailure]),
      fail: (error) => {
        if (!controller.signal.aborted) {
          controller.abort();
          for (const handler of [...abortHandlers]) {
            handler();
          }
          abortHandlers.clear();
        }
        fail(error);
      },
    };
    this.inFlight.add(tracked as TrackedCommand<unknown>);
    void tracked.promise.then(() => {
      this.inFlight.delete(tracked as TrackedCommand<unknown>);
    }, () => {
      this.inFlight.delete(tracked as TrackedCommand<unknown>);
    });
    return tracked.promise;
  }

  /**
   * Closes readiness and admission, asks Runtime Core to interrupt and join active runs locally,
   * and waits for tracked work up to the configured bound before failing outstanding command
   * promises. Durable repair after pod loss belongs to the Bridge job runner, not this hook.
   */
  async shutdown(): Promise<void> {
    this.readyFlag = false;
    this.accepting = false;
    // Shutdown starts Runtime Core's local interrupt-and-join hook, then waits for
    // that promise and tracked commands together under the configured timeout.
    const activeRunSettlement = (this.options.shutdownHooks?.shutdownActiveRuns?.() ?? Promise.resolve()).catch(() => {
      this.options.logger.error(shutdownFailureLogRecord({
        event: "shutdown_active_run_settlement_failed",
        message: "runtime pod shutdown active-run settlement failed",
      }));
    });
    const timeout = sleep(this.options.drainTimeoutMs ?? 5_000).then(() => "timeout" as const);
    const drained = Promise.allSettled([...this.inFlight].map((tracked) => tracked.promise).concat(activeRunSettlement)).then(() => "drained" as const);
    const result = await Promise.race([timeout, drained]);
    if (result === "timeout") {
      for (const tracked of this.inFlight) {
        tracked.fail(new GrpcStatusError(status.FAILED_PRECONDITION, "runtime pod shutdown drain timed out"));
      }
      this.options.logger.error(shutdownFailureLogRecord({
        event: "shutdown_drain_timeout",
        message: "runtime pod shutdown drain timed out",
      }));
    }
  }
}

async function sleep(durationMs: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, durationMs);
  });
}
