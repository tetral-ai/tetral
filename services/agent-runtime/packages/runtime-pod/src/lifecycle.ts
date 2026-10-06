import type { RuntimeShutdownPhase } from "./metrics.js";
/**
 * Coordinates Runtime Pod bootstrap, process registration, readiness, command admission and the
 * bounded checkpoint handoff at shutdown.
 *
 * Readiness opens only after every bootstrap hook succeeds, Bridge registers this boot and commits
 * its ACCEPTING report; heartbeats keep that report fresh. Shutdown closes readiness and admission,
 * asks Runtime Core to quiesce every resident Session to its current-step checkpoint, commits the
 * DRAINING report and releases each checkpointed binding through Bridge within the shared
 * settlement deadline. `createRuntimePodApp` drives startup and shutdown, the HTTP and metrics
 * surfaces inspect lifecycle state, and `RuntimeControlService` submits lease-aware commands. This
 * module calls only injected bootstrap hooks, the quiesce hook, the process registry port and the
 * structured logger.
 */
import type { RuntimeQuiesceOptions } from "@tetral/agent-runtime-core/src/session/session-manager.js";
import type { RuntimeProcessPort } from "./runtime-process.js";
import { bridgeMethodDeadline } from "./bridge-policy.js";
import { retryRuntimeProcessOperation } from "./runtime-process.js";
import { status } from "@grpc/grpc-js";
import type { RuntimePodConfigResult } from "./config.js";
import type { RuntimePodLogger, RuntimePodLogRecord } from "./logger.js";
import {
  runtimeProcessReportFailureLogRecord,
  shutdownFailureLogRecord,
  startupFailureLogRecord,
} from "./logger.js";
import { GrpcStatusError } from "./errors.js";
import { DefaultRuntimeShutdownPolicy } from "./lifecycle-policy.js";

export { GrpcStatusError } from "./errors.js";

type RuntimeReleaseScope = Parameters<RuntimeQuiesceOptions["release"]>[0];

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
 * Runtime Core's cooperative quiesce. It stops admitting the next ordinary model step, lets each
 * resident Session reach its current-step checkpoint (including the permission-reviewer
 * dependencies of already admitted steps) and calls `release` once per checkpointed Session
 * binding. Sessions release independently; the hook settles only after every Session released or
 * failed, and a failed release leaves that Session's binding for fenced loss repair.
 */
export interface RuntimePodShutdownHooks {
  readonly quiesce: (options: RuntimeQuiesceOptions) => Promise<void>;
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
 * Dependencies used by `RuntimePodLifecycle`. Shutdown phase bounds come from the validated
 * configuration's lifecycle policy.
 */
export interface RuntimePodLifecycleOptions {
  readonly observeShutdownPhase?: (phase: RuntimeShutdownPhase, durationMs: number, outcome: "success" | "error" | "timeout") => void;
  readonly runtimeProcess: RuntimeProcessPort;
  readonly config: RuntimePodConfigResult;
  readonly logger: RuntimePodLogger;
  readonly bootstrap: RuntimePodBootstrap;
  readonly shutdownHooks: RuntimePodShutdownHooks;
}

interface TrackedCommand<T> {
  readonly promise: Promise<T>;
  readonly fail: (error: GrpcStatusError) => void;
}

/**
 * Owns process-local readiness and the set of commands participating in shutdown drain.
 */
export class RuntimePodLifecycle {
  private resolveProcessFailure!: (error: GrpcStatusError) => void;
  /** Resolves on permanent process rejection; the executable owns shutdown and exit. */
  readonly processFailure = new Promise<GrpcStatusError>((resolve) => {
    this.resolveProcessFailure = resolve;
  });
  private readyFlag = false;
  private accepting = false;
  private phase: "accepting" | "draining" = "accepting";
  private heartbeatStop: AbortController | undefined;
  private heartbeat: Promise<void> | undefined;
  private stopping: Promise<void> | undefined;
  private lastReportAt = 0;
  private startupComplete = false;
  private processSuperseded = false;
  private freshnessTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly inFlight = new Set<TrackedCommand<unknown>>();

  constructor(private readonly options: RuntimePodLifecycleOptions) {}

  sessionCapacity(): number | undefined {
    return this.options.config.ok
      ? this.options.config.config.maxLocalSessions
      : undefined;
  }

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
   * Runs bootstrap hooks in dependency order, registers this boot with Bridge and opens readiness
   * only after its ACCEPTING report commits. Startup failures are sanitized, logged, and represented
   * by a non-ready lifecycle; a stale-process rejection also resolves `processFailure`.
   */
  async start(): Promise<void> {
    if (
      this.processSuperseded ||
      this.phase !== "accepting" ||
      this.stopping !== undefined
    )
      return;
    if (!this.options.config.ok) {
      try {
        this.options.logger.error(
          startupFailureLogRecord(this.options.config.error),
        );
      } catch {
        /* lifecycle state remains authoritative */
      }
      this.readyFlag = false;
      this.accepting = false;
      return;
    }
    let causeCategory: "dependency_readiness" | "listener" =
      "dependency_readiness";
    try {
      await this.options.bootstrap.runtime();
      await this.options.bootstrap.core();
      causeCategory = "listener";
      await this.options.bootstrap.grpc();
      causeCategory = "dependency_readiness";
      await this.options.bootstrap.authClient();
      const config = this.options.config.config;
      const registrationDeadline = bridgeMethodDeadline(
        config.bridgeMethodPolicies,
        "registerRuntimeProcess",
        Date.now(),
      );
      await retryRuntimeProcessOperation(
        () => this.options.runtimeProcess.register(registrationDeadline),
        registrationDeadline,
      );
      await this.options.runtimeProcess.report(
        "accepting",
        bridgeMethodDeadline(
          config.bridgeMethodPolicies,
          "reportRuntimeProcess",
          Date.now(),
        ),
      );
      if (
        this.phase !== "accepting" ||
        this.stopping !== undefined ||
        this.processSuperseded
      )
        return;
      this.lastReportAt = Date.now();
      this.armProcessFreshness();
      this.startHeartbeat();
      this.recordLifecycle({
        event: "runtime_process_accepting",
        "runtime.process.phase": "accepting",
      });
      this.startupComplete = true;
      this.readyFlag = true;
      this.accepting = true;
    } catch (error) {
      if (
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        error.code === status.FAILED_PRECONDITION
      )
        this.rejectProcess();
      try {
        this.options.logger.error(
          startupFailureLogRecord({
            kind: "startup_error",
            message: "runtime pod startup failed",
            cause: error,
            causeCategory,
          }),
        );
      } catch {
        /* lifecycle state remains authoritative */
      }
      this.readyFlag = false;
      this.accepting = false;
    }
  }

  /**
   * Admits a command with a lease whose signal and callbacks abort when the shutdown settlement
   * deadline expires, while the returned promise participates in the in-flight count.
   */
  runCommand<T>(
    command: (lease: RuntimeCommandLease) => Promise<T>,
  ): Promise<T> {
    if (!this.readyFlag || !this.accepting) {
      throw new GrpcStatusError(
        status.FAILED_PRECONDITION,
        "runtime pod shutting down",
      );
    }
    const controller = new AbortController();
    const abortHandlers = new Set<() => void>();
    const abortError = () =>
      new GrpcStatusError(
        status.FAILED_PRECONDITION,
        "runtime pod shutdown drain timed out",
      );
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
      promise: commandPromise,
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
    void tracked.promise.then(
      () => {
        this.inFlight.delete(tracked as TrackedCommand<unknown>);
      },
      () => {
        this.inFlight.delete(tracked as TrackedCommand<unknown>);
      },
    );
    const result = Promise.race([tracked.promise, shutdownFailure]);
    void result.catch(() => undefined);
    return result;
  }

  /**
   * Closes readiness and admission, then hands off every resident Session at its current-step
   * checkpoint. Each Session releases its binding as soon as it checkpoints, under one operation
   * identity whose committed receipt replays; the release waits for the committed DRAINING report.
   * When the settlement deadline expires, outstanding commands fail and local owners join within the
   * local-join window. A Session whose release did not commit is recorded as an incomplete handoff
   * and keeps its binding for Job Runner's fenced loss repair. The process registry client closes
   * only after every Session and command has settled.
   */
  shutdown(): Promise<void> {
    if (this.stopping !== undefined) return this.stopping;
    this.readyFlag = false;
    this.accepting = false;
    this.phase = "draining";
    this.stopping = this.drain();
    return this.stopping;
  }

  private recordLifecycle(record: RuntimePodLogRecord): void {
    try {
      this.options.logger.info({
        ...record,
        "runtime.process.id": this.options.runtimeProcess.runtimeProcessId,
        component: "runtime-lifecycle",
      });
    } catch {
      /* diagnostics cannot own custody */
    }
  }

  /** Records one Session whose binding release did not commit before its settlement bound. */
  private recordHandoffIncomplete(
    scope: RuntimeReleaseScope,
    operationId: string,
    error: unknown,
  ): void {
    try {
      this.options.logger.error({
        ...shutdownFailureLogRecord({
          event: "runtime_binding_handoff_incomplete",
          message: "Runtime binding release did not commit",
        }),
        "workspace.id": scope.workspaceId,
        "session.id": scope.sessionId,
        "binding.id": scope.bindingId,
        "binding.generation": scope.bindingGeneration,
        "operation.id": operationId,
        "grpc.code": grpcStatusName(error),
        "runtime.process.id": this.options.runtimeProcess.runtimeProcessId,
      });
    } catch {
      /* diagnostics cannot own custody */
    }
  }

  private armProcessFreshness(): void {
    if (this.freshnessTimer !== undefined) clearTimeout(this.freshnessTimer);
    if (!this.options.config.ok) return;
    this.freshnessTimer = setTimeout(
      () => {
        if (
          this.options.config.ok &&
          Date.now() <
            this.lastReportAt +
              this.options.config.config.lifecycle.processFreshnessMs
        )
          return;
        this.readyFlag = false;
        this.accepting = false;
        this.recordLifecycle({
          event: "runtime_process_freshness_expired",
          "runtime.process.freshness_ms": this.options.config.ok
            ? this.options.config.config.lifecycle.processFreshnessMs
            : undefined,
        });
      },
      Math.max(
        0,
        this.lastReportAt +
          this.options.config.config.lifecycle.processFreshnessMs -
          Date.now(),
      ),
    );
  }

  private startHeartbeat(): void {
    if (!this.options.config.ok) return;
    const config = this.options.config.config;
    const controller = new AbortController();
    this.heartbeatStop = controller;
    this.heartbeat = (async () => {
      let failedCount = 0,
        lastFailureLogged = 0;
      while (!controller.signal.aborted) {
        await sleep(config.lifecycle.reportIntervalMs, controller.signal);
        if (controller.signal.aborted) break;
        const reportedPhase = this.phase;
        try {
          await this.options.runtimeProcess.report(
            reportedPhase,
            bridgeMethodDeadline(
              config.bridgeMethodPolicies,
              "reportRuntimeProcess",
              Date.now(),
            ),
          );
          if (controller.signal.aborted || reportedPhase !== this.phase) break;
          if (this.processSuperseded && reportedPhase === "accepting") continue;
          this.lastReportAt = Date.now();
          // Only this still-current boot's ACCEPTING ACK may recover an expired freshness fence.
          // Explicit stale rejection is permanent, and an ACK cannot reopen shutdown admission.
          if (
            reportedPhase === "accepting" &&
            this.startupComplete &&
            this.stopping === undefined &&
            !this.processSuperseded
          ) {
            this.readyFlag = true;
            this.accepting = true;
          }
          if (failedCount > 0) {
            this.recordLifecycle({
              event: "runtime_process_report_recovered",
              "event.kind": "runtime_process_report_recovered",
              "failed.count": failedCount,
            });
            failedCount = 0;
            lastFailureLogged = 0;
          }
          this.armProcessFreshness();
        } catch (error) {
          const code =
            typeof error === "object" && error !== null && "code" in error
              ? error.code
              : undefined;
          if (code === status.FAILED_PRECONDITION) this.rejectProcess();
          if (
            this.processSuperseded ||
            Date.now() - this.lastReportAt >=
              config.lifecycle.processFreshnessMs
          ) {
            this.readyFlag = false;
            this.accepting = false;
          }
          failedCount++;
          if (failedCount === 1 || Date.now() - lastFailureLogged >= 30000) {
            lastFailureLogged = Date.now();
            try {
              this.options.logger.error(
                runtimeProcessReportFailureLogRecord({ failedCount }),
              );
            } catch {
              /* diagnostic isolation */
            }
          }
        }
      }
    })();
  }

  private rejectProcess(): void {
    this.processSuperseded = true;
    this.readyFlag = false;
    this.accepting = false;
    this.resolveProcessFailure(
      new GrpcStatusError(
        status.FAILED_PRECONDITION,
        "Runtime process is stale",
      ),
    );
  }

  private recordShutdownPhase(phase: RuntimeShutdownPhase, started: number, outcome: "success" | "error" | "timeout"): void {
    try { this.options.observeShutdownPhase?.(phase, performance.now()-started, outcome); } catch { /* Metrics cannot change shutdown ownership. */ }
  }

  private async drain(): Promise<void> {
    // A lifecycle whose configuration failed never registered or admitted work; the typed defaults
    // still bound its shutdown.
    const policy = this.options.config.ok
      ? this.options.config.config.lifecycle
      : DefaultRuntimeShutdownPolicy;
    const currentStepDeadline = Date.now() + policy.currentStepTimeoutMs;
    const settlementDeadline = currentStepDeadline + policy.settlementTimeoutMs;
    this.options.runtimeProcess.beginDrain?.({
      currentStepDeadline,
      settlementDeadline,
      settlementAttemptTimeoutMs: policy.settlementAttemptTimeoutMs,
    });
    let resolveDraining!: () => void, rejectDraining!: (error: unknown) => void;
    const draining = new Promise<void>((resolve, reject) => {
      resolveDraining = resolve;
      rejectDraining = reject;
    });
    // Attach before beginning report I/O; an idle Session may already be awaiting this ACK.
    void draining.catch(() => undefined);
    const operations = new Map<string, string>();
    const quiesceStarted = performance.now();
    const coreDrain = this.options.shutdownHooks.quiesce({
      currentStepDeadline,
      settlementDeadline,
      observe: (scope, phase, parentThreadId) =>
        this.recordLifecycle({
          ...(phase === "expired"
            ? shutdownFailureLogRecord({
                event: "runtime_checkpoint_expired",
                message: "Runtime current step deadline exceeded",
              })
            : { event: "runtime_checkpoint_failed_joined" }),
          "workspace.id": scope.workspaceId,
          "session.id": scope.sessionId,
          "thread.id": scope.sessionThreadId,
          "binding.id": scope.bindingId,
          "binding.generation": scope.bindingGeneration,
          "checkpoint.deadline_at": currentStepDeadline,
          "checkpoint.expired": true,
          "parent.thread.id": parentThreadId,
          "reviewer.thread.id":
            parentThreadId === undefined ? undefined : scope.sessionThreadId,
        }),
      ingressJoined: Promise.allSettled(
        [...this.inFlight].map((command) => command.promise),
      ).then(() => undefined),
      release: async (scope, deadline) => {
        this.recordLifecycle({
          event: "runtime_checkpoint_ready",
          "workspace.id": scope.workspaceId,
          "session.id": scope.sessionId,
          "binding.id": scope.bindingId,
          "binding.generation": scope.bindingGeneration,
          "checkpoint.deadline_at": deadline,
        });
        await draining;
        const key = `${scope.workspaceId}/${scope.sessionId}/${scope.bindingId}/${scope.bindingGeneration}`;
        let operationId = operations.get(key);
        if (operationId === undefined) {
          operationId = `rrelease_${crypto.randomUUID()}`;
          operations.set(key, operationId);
        }
        const releaseStarted = performance.now();
        let releaseOutcome: "success" | "error" = "error";
        try {
          await retryRuntimeProcessOperation(async () => {
            const receipt = await this.options.runtimeProcess.release(
              {
                workspaceId: scope.workspaceId,
                sessionId: scope.sessionId,
                bindingId: scope.bindingId,
                bindingGeneration: scope.bindingGeneration,
                operationId: operationId!,
              },
              deadline,
            );
            for (const thread of receipt.threads)
              this.recordLifecycle({
                event: "runtime_binding_handoff_committed",
                "workspace.id": scope.workspaceId,
                "session.id": scope.sessionId,
                "thread.id": thread.sessionThreadId,
                "binding.id": scope.bindingId,
                "binding.generation": scope.bindingGeneration,
                "operation.id": operationId,
                "handoff.id": receipt.handoffId,
                "handoff.disposition":
                  thread.disposition === 1 ? "idle" : "recover",
              });
          }, deadline);
          releaseOutcome = "success";
        } catch (error) {
          // This Session keeps its binding; Job Runner's fenced loss repair settles it after exit.
          this.recordHandoffIncomplete(scope, operationId, error);
          throw error;
        } finally {
          this.recordShutdownPhase("shutdown_release", releaseStarted, releaseOutcome);
        }
      },
    });
    void coreDrain.then(
      () => this.recordShutdownPhase("shutdown_quiesce", quiesceStarted, "success"),
      () => this.recordShutdownPhase("shutdown_quiesce", quiesceStarted, "error"),
    );
    try {
      this.heartbeatStop?.abort();
      await this.heartbeat;
      const reportStarted = performance.now();
      let reportOutcome: "success" | "error" = "error";
      try {
        await retryRuntimeProcessOperation(
          () => this.options.runtimeProcess.report("draining", settlementDeadline),
          settlementDeadline,
        );
        reportOutcome = "success";
      } finally {
        this.recordShutdownPhase("shutdown_report", reportStarted, reportOutcome);
      }
      this.lastReportAt = Date.now();
      this.armProcessFreshness();
      this.startHeartbeat();
      this.recordLifecycle({
        event: "runtime_process_draining",
        "runtime.process.phase": "draining",
      });
      resolveDraining();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const expired = new Promise<"expired">((resolve) => {
        timer = setTimeout(
          () => resolve("expired"),
          Math.max(0, settlementDeadline - Date.now()),
        );
      });
      const joined = Promise.allSettled([
        coreDrain,
        ...[...this.inFlight].map((command) => command.promise),
      ]);
      const result = await Promise.race([joined, expired]);
      if (timer !== undefined) clearTimeout(timer);
      if (result === "expired") {
        for (const command of this.inFlight)
          command.fail(
            new GrpcStatusError(
              status.FAILED_PRECONDITION,
              "runtime pod shutdown drain timed out",
            ),
          );
        // Joining producer bodies follows cancellation; public promise rejection is not ownership transfer.
        const localDeadline = Date.now() + policy.localJoinTimeoutMs;
        const joinStarted = performance.now();
        const joinWindow = new AbortController();
        try {
          await Promise.race([
            joined,
            sleep(Math.max(0, localDeadline - Date.now()), joinWindow.signal),
          ]);
        } finally {
          joinWindow.abort();
        }
        await joined;
        this.recordShutdownPhase("shutdown_local_join",joinStarted,Date.now() > localDeadline ? "timeout" : "success");
        try {
          this.options.logger.error(
            shutdownFailureLogRecord({
              event: "shutdown_drain_timeout",
              message: "runtime pod shutdown drain timed out",
            }),
          );
        } catch {
          /* diagnostic isolation */
        }
        return;
      }
      const failure = result.find((outcome) => outcome.status === "rejected");
      if (failure?.status === "rejected") throw failure.reason;
    } catch (error) {
      rejectDraining(error);
      try {
        this.options.logger.error(
          shutdownFailureLogRecord({
            event: "shutdown_active_run_settlement_failed",
            message: "runtime pod shutdown active-run settlement failed",
          }),
        );
      } catch {
        /* diagnostic isolation */
      }
    } finally {
      await Promise.allSettled([
        coreDrain,
        ...[...this.inFlight].map((command) => command.promise),
      ]);
      this.heartbeatStop?.abort();
      await this.heartbeat;
      if (this.freshnessTimer !== undefined) clearTimeout(this.freshnessTimer);
      await this.options.runtimeProcess.close();
    }
  }
}

async function sleep(durationMs: number, signal?: AbortSignal): Promise<void> {
  await new Promise<void>((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const finish = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, durationMs);
    signal?.addEventListener("abort", finish, { once: true });
  });
}

/** Names the gRPC status of a failed Bridge attempt; local acknowledgement failures carry none. */
function grpcStatusName(error: unknown): string {
  const code =
    typeof error === "object" && error !== null && "code" in error
      ? error.code
      : undefined;
  const name = typeof code === "number" ? status[code] : undefined;
  if (name === undefined) return "Unknown";
  return name
    .toLowerCase()
    .replace(/(?:^|_)([a-z])/g, (_match, letter: string) => letter.toUpperCase());
}
