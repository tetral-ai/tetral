/**
 * @packageDocumentation
 * Single-owner run coordinator (`Runner`) and the Runtime concurrency-primitive
 * discipline every module in this package follows.
 *
 * OWNS:
 *   - make(): a Runner that keeps at most one owner Fiber per scope; concurrent
 *     ensureRunning callers join the active run's Deferred rather than fork a second.
 *
 * STATE MACHINE (Runner):
 *   | state   | meaning                          | writers                | legal transitions                 |
 *   | ------- | -------------------------------- | ---------------------- | --------------------------------- |
 *   | Idle    | no owner fiber                   | make (init), finishRun | Idle -> Running                   |
 *   | Running | one owner forked in the scope;   | ensureRunning (start)  | Running -> Idle (owner exit, via  |
 *   |         | joiners await its Deferred       |                        | finishRun; cancel/scope close)    |
 *   Cancelling a joined waiter never cancels the owner; cancel() or closure of the
 *   owning Scope interrupts it.
 *
 * INVARIANTS (concurrency-primitive discipline, binding across the package):
 *   - Effect wraps every operation with external I/O, a durable-ACK dependency,
 *     structured failure, timeout, or cancellation.
 *   - Fiber is used for explicitly owned lifetimes: one ThreadRun owner per active
 *     ThreadEntry, provider and compaction stream fibers, ToolFiberSets and resumed
 *     approval settlement fibers, child-thread runs, and reviewer trunk/sidecar runs.
 *   - Scope owns every lifetime that must close on interrupt, cleanup, TTL release,
 *     or pod shutdown.
 *   - Deferred is hot in-process coordination only and is NEVER the durable owner of
 *     an external wait; approval, custom-tool, task, child-thread, and cleanup waits
 *     are durable rows first, and hot waiters are disposable accelerators.
 *   - Durable ACKs gate hot-state mutation; durable Bridge/Gateway calls are awaited
 *     Effects, never forked merely to look asynchronous.
 *   - Fiber.fork sites are reviewed architecture surface: each fork has an owning Scope
 *     and a documented join or finalizer path.
 *
 * UPDATE-WITH: services/agent-runtime/packages/core/src/session/session-manager.ts,
 *              services/agent-runtime/packages/core/src/thread-loop/thread-loop.ts,
 *              services/agent-runtime/packages/core/src/tools/tool-scheduler.ts
 *
 * The unit suite currently calls make() directly to exercise this generic coordinator. The
 * returned runner calls Effect fibers, Deferred completion, scoped interruption, and its
 * optional idle and interrupt hooks while keeping those primitives behind a single-owner API.
 */
import { Cause, Deferred, Effect, Exit, Fiber, SynchronizedRef } from "effect";
import type { Scope } from "effect";

/** A scoped single-owner run whose concurrent callers join the same completion. */
export interface Runner<A, E = unknown> {
  readonly ensureRunning: (work: Effect.Effect<A, E>) => Effect.Effect<A, E>;
  readonly cancel: () => Effect.Effect<void>;
}

interface RunnerOptions<A, E> {
  readonly onIdle?: Effect.Effect<void>;
  readonly onInterrupt?: Effect.Effect<A, E>;
}

interface ActiveRun<A, E> {
  readonly id: number;
  readonly done: Deferred.Deferred<A, E | RunnerCancelled>;
  readonly fiber: Fiber.Fiber<A, E>;
}

type RunnerState<A, E> = { readonly _tag: "Idle" } | { readonly _tag: "Running"; readonly run: ActiveRun<A, E> };

class RunnerCancelled {
  readonly _tag = "RunnerCancelled";
}

function isRunnerCancelled(error: unknown): error is RunnerCancelled {
  return error instanceof RunnerCancelled;
}

/** Creates a runner whose owner fiber and cancellation lifetime belong to the supplied scope. */
export function make<A, E = unknown>(scope: Scope.Scope, options: RunnerOptions<A, E> = {}): Runner<A, E> {
  const runnerState = SynchronizedRef.makeUnsafe<RunnerState<A, E>>({ _tag: "Idle" });
  const onIdle = options.onIdle ?? Effect.void;
  const onInterrupt = options.onInterrupt;
  let nextRunId = 0;

  const allocateRunId = (): number => {
    nextRunId += 1;
    return nextRunId;
  };

  const completeRun = (done: Deferred.Deferred<A, E | RunnerCancelled>, exit: Exit.Exit<A, E>): Effect.Effect<void> =>
    Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
      ? Deferred.fail(done, new RunnerCancelled()).pipe(Effect.asVoid)
      : Deferred.done(done, exit).pipe(Effect.asVoid);

  const completeCancelled = (done: Deferred.Deferred<A, E | RunnerCancelled>): Effect.Effect<void> =>
    Deferred.fail(done, new RunnerCancelled()).pipe(Effect.asVoid);

  const awaitDone = (done: Deferred.Deferred<A, E | RunnerCancelled>): Effect.Effect<A, E> =>
    Deferred.await(done).pipe(
      Effect.catchIf(isRunnerCancelled, (error) => onInterrupt ?? Effect.die(error)),
    );

  const finishRun = (runId: number, done: Deferred.Deferred<A, E | RunnerCancelled>, exit: Exit.Exit<A, E>): Effect.Effect<void> =>
    SynchronizedRef.modifyEffect(
      runnerState,
      Effect.fnUntraced(function* (state) {
        if (state._tag === "Running" && state.run.id === runId) {
          yield* onIdle;
          yield* completeRun(done, exit);
          return [undefined, { _tag: "Idle" } as const] as const;
        }
        yield* completeRun(done, exit);
        return [undefined, state] as const;
      }),
    );

  const startRun = (work: Effect.Effect<A, E>, done: Deferred.Deferred<A, E | RunnerCancelled>): Effect.Effect<ActiveRun<A, E>> =>
    Effect.gen(function* () {
      const runId = allocateRunId();
      const fiber = yield* work.pipe(
        Effect.onExit((exit) => finishRun(runId, done, exit)),
        Effect.forkIn(scope),
      );
      return { id: runId, done, fiber };
    });

  const completeIfFiberAlreadyExited = (run: ActiveRun<A, E>): Effect.Effect<void> =>
    Effect.gen(function* () {
      yield* Effect.yieldNow;
      const exit = run.fiber.pollUnsafe();
      if (exit !== undefined) {
        yield* finishRun(run.id, run.done, exit);
      }
    });

  const ensureRunning = (work: Effect.Effect<A, E>): Effect.Effect<A, E> =>
    SynchronizedRef.modifyEffect(
      runnerState,
      Effect.fnUntraced(function* (state) {
        if (state._tag === "Running") {
          return [awaitDone(state.run.done), state] as const;
        }
        const done = yield* Deferred.make<A, E | RunnerCancelled>();
        const run = yield* startRun(work, done);
        return [
          Effect.gen(function* () {
            yield* completeIfFiberAlreadyExited(run);
            return yield* awaitDone(done);
          }),
          { _tag: "Running", run },
        ] as const;
      }),
    ).pipe(Effect.flatten);

  const cancelActiveRun = (run: ActiveRun<A, E>): Effect.Effect<void> =>
    Effect.gen(function* () {
      yield* Effect.withFiber((fiber) =>
        Effect.sync(() => {
          run.fiber.interruptUnsafe(fiber.id);
        }),
      );
      yield* completeCancelled(run.done);
      const exit = yield* Fiber.await(run.fiber);
      yield* finishRun(run.id, run.done, exit);
    });

  const cancel = (): Effect.Effect<void> =>
    SynchronizedRef.modify(runnerState, (state) => {
      if (state._tag === "Idle") {
        return [Effect.void, state] as const;
      }
      return [cancelActiveRun(state.run), state] as const;
    }).pipe(Effect.flatten);

  return { cancel, ensureRunning };
}
