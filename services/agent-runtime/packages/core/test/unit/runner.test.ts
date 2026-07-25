import { describe, expect, test } from "bun:test";
import { Deferred, Effect, Exit, Fiber, Option, Ref, Scope } from "effect";
import * as Runner from "../../src/effect/runner.js";

function signal(deferred: Deferred.Deferred<void>): Effect.Effect<void> {
  return Deferred.succeed(deferred, undefined).pipe(Effect.asVoid);
}

function heldWork(
  starts: Ref.Ref<number>,
  started: Deferred.Deferred<void>,
  release: Deferred.Deferred<void>,
): Effect.Effect<void, string> {
  return Effect.gen(function* () {
    yield* Ref.update(starts, (count) => count + 1);
    yield* signal(started);
    yield* Deferred.await(release);
  });
}

function interruptibleHeldWork(
  starts: Ref.Ref<number>,
  started: Deferred.Deferred<void>,
  interrupted: Ref.Ref<number>,
): Effect.Effect<void, string> {
  return Effect.gen(function* () {
    yield* Ref.update(starts, (count) => count + 1);
    yield* signal(started);
    yield* Effect.never;
  }).pipe(Effect.onInterrupt(() => Ref.update(interrupted, (count) => count + 1)));
}

function failureFromExit(exit: Exit.Exit<void, string>): string | undefined {
  const failure = Exit.findErrorOption(exit);
  return Option.isSome(failure) ? failure.value : undefined;
}

describe("Runner", () => {
  test("ensureRunning from idle starts one work Effect and resolves when controlled work completes", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          const starts = yield* Ref.make(0);
          const idleCount = yield* Ref.make(0);
          const started = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const runner = Runner.make<void, string>(scope, {
            onIdle: Ref.update(idleCount, (count) => count + 1),
            onInterrupt: Effect.void,
          });

          const runFiber = yield* Effect.forkIn(runner.ensureRunning(heldWork(starts, started, release)), scope);
          yield* Deferred.await(started);
          const startsWhileHeld = yield* Ref.get(starts);
          yield* signal(release);
          const exit = yield* Fiber.await(runFiber);

          return {
            exit,
            idleCount: yield* Ref.get(idleCount),
            startsWhileHeld,
            startsAfterCompletion: yield* Ref.get(starts),
          };
        }),
      ),
    );

    expect(Exit.isSuccess(result.exit)).toBe(true);
    expect(result.startsWhileHeld).toBe(1);
    expect(result.startsAfterCompletion).toBe(1);
    expect(result.idleCount).toBe(1);
  });

  test("concurrent ensureRunning calls on the same Runner reuse the active result", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          const starts = yield* Ref.make(0);
          const duplicateStarts = yield* Ref.make(0);
          const started = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const runner = Runner.make<void, string>(scope, { onInterrupt: Effect.void });

          const firstFiber = yield* Effect.forkIn(runner.ensureRunning(heldWork(starts, started, release)), scope);
          yield* Deferred.await(started);
          const secondFiber = yield* Effect.forkIn(
            runner.ensureRunning(Effect.gen(function* () {
              yield* Ref.update(duplicateStarts, (count) => count + 1);
            })),
            scope,
          );
          yield* Effect.yieldNow;
          const duplicateStartsWhileHeld = yield* Ref.get(duplicateStarts);
          yield* signal(release);
          const firstExit = yield* Fiber.await(firstFiber);
          const secondExit = yield* Fiber.await(secondFiber);

          return {
            duplicateStartsWhileHeld,
            firstExit,
            secondExit,
            starts: yield* Ref.get(starts),
          };
        }),
      ),
    );

    expect(result.starts).toBe(1);
    expect(result.duplicateStartsWhileHeld).toBe(0);
    expect(Exit.isSuccess(result.firstExit)).toBe(true);
    expect(Exit.isSuccess(result.secondExit)).toBe(true);
  });

  test("a later ensureRunning after normal completion starts a fresh run and onIdle runs per completion", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          const starts = yield* Ref.make(0);
          const idleCount = yield* Ref.make(0);
          const firstStarted = yield* Deferred.make<void>();
          const firstRelease = yield* Deferred.make<void>();
          const secondStarted = yield* Deferred.make<void>();
          const secondRelease = yield* Deferred.make<void>();
          const runner = Runner.make<void, string>(scope, {
            onIdle: Ref.update(idleCount, (count) => count + 1),
            onInterrupt: Effect.void,
          });

          const firstFiber = yield* Effect.forkIn(runner.ensureRunning(heldWork(starts, firstStarted, firstRelease)), scope);
          yield* Deferred.await(firstStarted);
          yield* signal(firstRelease);
          const firstExit = yield* Fiber.await(firstFiber);

          const secondFiber = yield* Effect.forkIn(runner.ensureRunning(heldWork(starts, secondStarted, secondRelease)), scope);
          yield* Deferred.await(secondStarted);
          const startsDuringSecondRun = yield* Ref.get(starts);
          yield* signal(secondRelease);
          const secondExit = yield* Fiber.await(secondFiber);

          return {
            firstExit,
            idleCount: yield* Ref.get(idleCount),
            secondExit,
            startsDuringSecondRun,
          };
        }),
      ),
    );

    expect(Exit.isSuccess(result.firstExit)).toBe(true);
    expect(Exit.isSuccess(result.secondExit)).toBe(true);
    expect(result.startsDuringSecondRun).toBe(2);
    expect(result.idleCount).toBe(2);
  });

  test("normal work failure propagates to all waiters and still runs onIdle", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          const starts = yield* Ref.make(0);
          const duplicateStarts = yield* Ref.make(0);
          const idleCount = yield* Ref.make(0);
          const started = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const runner = Runner.make<void, string>(scope, {
            onIdle: Ref.update(idleCount, (count) => count + 1),
            onInterrupt: Effect.void,
          });
          const failingWork = heldWork(starts, started, release).pipe(Effect.andThen(Effect.fail("work failed")));

          const firstFiber = yield* Effect.forkIn(runner.ensureRunning(failingWork), scope);
          yield* Deferred.await(started);
          const secondFiber = yield* Effect.forkIn(
            runner.ensureRunning(Effect.gen(function* () {
              yield* Ref.update(duplicateStarts, (count) => count + 1);
            })),
            scope,
          );
          yield* Effect.yieldNow;
          yield* signal(release);
          const firstExit = yield* Fiber.await(firstFiber);
          const secondExit = yield* Fiber.await(secondFiber);

          return {
            duplicateStarts: yield* Ref.get(duplicateStarts),
            firstExit,
            idleCount: yield* Ref.get(idleCount),
            secondExit,
            starts: yield* Ref.get(starts),
          };
        }),
      ),
    );

    expect(result.starts).toBe(1);
    expect(result.duplicateStarts).toBe(0);
    expect(failureFromExit(result.firstExit)).toBe("work failed");
    expect(failureFromExit(result.secondExit)).toBe("work failed");
    expect(result.idleCount).toBe(1);
  });

  test("cancel interrupts held work, runs onInterrupt, resolves waiters with void, and runs onIdle", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          const starts = yield* Ref.make(0);
          const idleCount = yield* Ref.make(0);
          const interruptCount = yield* Ref.make(0);
          const workInterrupts = yield* Ref.make(0);
          const started = yield* Deferred.make<void>();
          const runner = Runner.make<void, string>(scope, {
            onIdle: Ref.update(idleCount, (count) => count + 1),
            onInterrupt: Ref.update(interruptCount, (count) => count + 1),
          });

          const runFiber = yield* Effect.forkIn(runner.ensureRunning(interruptibleHeldWork(starts, started, workInterrupts)), scope);
          yield* Deferred.await(started);
          yield* runner.cancel();
          const exit = yield* Fiber.await(runFiber);

          return {
            exit,
            idleCount: yield* Ref.get(idleCount),
            interruptCount: yield* Ref.get(interruptCount),
            starts: yield* Ref.get(starts),
            workInterrupts: yield* Ref.get(workInterrupts),
          };
        }),
      ),
    );

    expect(result.starts).toBe(1);
    expect(result.workInterrupts).toBe(1);
    expect(result.interruptCount).toBe(1);
    expect(result.idleCount).toBe(1);
    expect(Exit.isSuccess(result.exit)).toBe(true);
  });

  test("cancel directly releases waiters before interrupted work cleanup completes", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          const starts = yield* Ref.make(0);
          const idleCount = yield* Ref.make(0);
          const interruptCount = yield* Ref.make(0);
          const started = yield* Deferred.make<void>();
          const cleanupBlocked = yield* Deferred.make<void>();
          const releaseCleanup = yield* Deferred.make<void>();
          const runner = Runner.make<void, string>(scope, {
            onIdle: Ref.update(idleCount, (count) => count + 1),
            onInterrupt: Ref.update(interruptCount, (count) => count + 1),
          });
          const work = Effect.gen(function* () {
            yield* Ref.update(starts, (count) => count + 1);
            yield* signal(started);
            yield* Effect.never;
          }).pipe(
            Effect.onInterrupt(() =>
              Effect.gen(function* () {
                yield* signal(cleanupBlocked);
                yield* Deferred.await(releaseCleanup);
              }),
            ),
          );

          const runFiber = yield* Effect.forkIn(runner.ensureRunning(work), scope);
          yield* Deferred.await(started);
          const cancelFiber = yield* Effect.forkIn(runner.cancel(), scope);
          yield* Deferred.await(cleanupBlocked);
          const waiterExitBeforeCleanupReleased = yield* Fiber.await(runFiber).pipe(Effect.timeoutOption("50 millis"));
          const idleCountBeforeCleanupReleased = yield* Ref.get(idleCount);
          yield* signal(releaseCleanup);
          const cancelExit = yield* Fiber.await(cancelFiber);
          const finalRunExit = yield* Fiber.await(runFiber);

          return {
            cancelExit,
            finalRunExit,
            idleCount: yield* Ref.get(idleCount),
            idleCountBeforeCleanupReleased,
            interruptCount: yield* Ref.get(interruptCount),
            starts: yield* Ref.get(starts),
            waiterExitBeforeCleanupReleased,
          };
        }),
      ),
    );

    expect(result.starts).toBe(1);
    expect(Option.isSome(result.waiterExitBeforeCleanupReleased)).toBe(true);
    expect(result.idleCountBeforeCleanupReleased).toBe(0);
    expect(result.interruptCount).toBe(1);
    expect(result.idleCount).toBe(1);
    expect(Exit.isSuccess(result.cancelExit)).toBe(true);
    expect(Exit.isSuccess(result.finalRunExit)).toBe(true);
  });

  test("cancel on an idle Runner succeeds without starting work or running callbacks", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          const idleCount = yield* Ref.make(0);
          const interruptCount = yield* Ref.make(0);
          const runner = Runner.make<void, string>(scope, {
            onIdle: Ref.update(idleCount, (count) => count + 1),
            onInterrupt: Ref.update(interruptCount, (count) => count + 1),
          });

          yield* runner.cancel();

          return {
            idleCount: yield* Ref.get(idleCount),
            interruptCount: yield* Ref.get(interruptCount),
          };
        }),
      ),
    );

    expect(result.idleCount).toBe(0);
    expect(result.interruptCount).toBe(0);
  });

  test("ensureRunning with an already closed Runner scope settles without starting work", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const runnerScope = yield* Scope.make();
        const waiterScope = yield* Scope.make();
        const starts = yield* Ref.make(0);
        const interruptCount = yield* Ref.make(0);
        const idleCount = yield* Ref.make(0);
        const runner = Runner.make<void, string>(runnerScope, {
          onIdle: Ref.update(idleCount, (count) => count + 1),
          onInterrupt: Ref.update(interruptCount, (count) => count + 1),
        });
        yield* Scope.close(runnerScope, Exit.void);

        const fiber = yield* Effect.forkIn(
          runner.ensureRunning(Ref.update(starts, (count) => count + 1)),
          waiterScope,
        );
        const settledExit = yield* Fiber.await(fiber).pipe(Effect.timeoutOption("100 millis"));
        yield* runner.cancel();
        yield* Scope.close(waiterScope, Exit.void);

        return {
          idleCount: yield* Ref.get(idleCount),
          interruptCount: yield* Ref.get(interruptCount),
          settledExit,
          starts: yield* Ref.get(starts),
        };
      }),
    );

    expect(result.starts).toBe(0);
    expect(result.interruptCount).toBe(1);
    expect(result.idleCount).toBe(1);
    expect(Option.isSome(result.settledExit)).toBe(true);
    if (Option.isSome(result.settledExit)) {
      expect(Exit.isSuccess(result.settledExit.value)).toBe(true);
    }
  });

  test("does not start a later run before normal completion cleanup finishes", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          const starts = yield* Ref.make(0);
          const idleCount = yield* Ref.make(0);
          const firstStarted = yield* Deferred.make<void>();
          const firstRelease = yield* Deferred.make<void>();
          const firstIdleStarted = yield* Deferred.make<void>();
          const releaseFirstIdle = yield* Deferred.make<void>();
          const secondStarted = yield* Deferred.make<void>();
          const secondRelease = yield* Deferred.make<void>();
          const runner = Runner.make<void, string>(scope, {
            onIdle: Effect.gen(function* () {
              const idleIndex = yield* Ref.modify(idleCount, (count) => [count, count + 1] as const);
              if (idleIndex === 0) {
                yield* signal(firstIdleStarted);
                yield* Deferred.await(releaseFirstIdle);
              }
            }),
            onInterrupt: Effect.void,
          });

          const firstFiber = yield* Effect.forkIn(runner.ensureRunning(heldWork(starts, firstStarted, firstRelease)), scope);
          yield* Deferred.await(firstStarted);
          yield* signal(firstRelease);
          yield* Deferred.await(firstIdleStarted);

          const secondFiber = yield* Effect.forkIn(runner.ensureRunning(heldWork(starts, secondStarted, secondRelease)), scope);
          const secondStartedBeforeFirstIdleReleased = yield* Deferred.await(secondStarted).pipe(Effect.timeoutOption("50 millis"));
          const startsBeforeFirstIdleReleased = yield* Ref.get(starts);
          yield* signal(releaseFirstIdle);
          const firstExit = yield* Fiber.await(firstFiber);
          yield* Deferred.await(secondStarted);
          const startsAfterSecondStart = yield* Ref.get(starts);
          yield* signal(secondRelease);
          const secondExit = yield* Fiber.await(secondFiber);

          return {
            firstExit,
            idleCount: yield* Ref.get(idleCount),
            secondExit,
            secondStartedBeforeFirstIdleReleased: Option.isSome(secondStartedBeforeFirstIdleReleased),
            startsAfterSecondStart,
            startsBeforeFirstIdleReleased,
          };
        }),
      ),
    );

    expect(Exit.isSuccess(result.firstExit)).toBe(true);
    expect(Exit.isSuccess(result.secondExit)).toBe(true);
    expect(result.secondStartedBeforeFirstIdleReleased).toBe(false);
    expect(result.startsBeforeFirstIdleReleased).toBe(1);
    expect(result.startsAfterSecondStart).toBe(2);
    expect(result.idleCount).toBe(2);
  });

  test("does not start a later run before cancellation cleanup finishes", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          const starts = yield* Ref.make(0);
          const idleCount = yield* Ref.make(0);
          const workInterrupts = yield* Ref.make(0);
          const firstStarted = yield* Deferred.make<void>();
          const firstIdleStarted = yield* Deferred.make<void>();
          const releaseFirstIdle = yield* Deferred.make<void>();
          const secondStarted = yield* Deferred.make<void>();
          const secondRelease = yield* Deferred.make<void>();
          const runner = Runner.make<void, string>(scope, {
            onIdle: Effect.gen(function* () {
              const idleIndex = yield* Ref.modify(idleCount, (count) => [count, count + 1] as const);
              if (idleIndex === 0) {
                yield* signal(firstIdleStarted);
                yield* Deferred.await(releaseFirstIdle);
              }
            }),
            onInterrupt: Effect.void,
          });

          const firstFiber = yield* Effect.forkIn(runner.ensureRunning(interruptibleHeldWork(starts, firstStarted, workInterrupts)), scope);
          yield* Deferred.await(firstStarted);
          const cancelFiber = yield* Effect.forkIn(runner.cancel(), scope);
          yield* Deferred.await(firstIdleStarted);

          const secondFiber = yield* Effect.forkIn(runner.ensureRunning(heldWork(starts, secondStarted, secondRelease)), scope);
          const secondStartedBeforeFirstIdleReleased = yield* Deferred.await(secondStarted).pipe(Effect.timeoutOption("50 millis"));
          const startsBeforeFirstIdleReleased = yield* Ref.get(starts);
          yield* signal(releaseFirstIdle);
          const cancelExit = yield* Fiber.await(cancelFiber);
          const firstExit = yield* Fiber.await(firstFiber);
          yield* Deferred.await(secondStarted);
          const startsAfterSecondStart = yield* Ref.get(starts);
          yield* signal(secondRelease);
          const secondExit = yield* Fiber.await(secondFiber);

          return {
            cancelExit,
            firstExit,
            idleCount: yield* Ref.get(idleCount),
            secondExit,
            secondStartedBeforeFirstIdleReleased: Option.isSome(secondStartedBeforeFirstIdleReleased),
            startsAfterSecondStart,
            startsBeforeFirstIdleReleased,
            workInterrupts: yield* Ref.get(workInterrupts),
          };
        }),
      ),
    );

    expect(Exit.isSuccess(result.cancelExit)).toBe(true);
    expect(Exit.isSuccess(result.firstExit)).toBe(true);
    expect(Exit.isSuccess(result.secondExit)).toBe(true);
    expect(result.workInterrupts).toBe(1);
    expect(result.secondStartedBeforeFirstIdleReleased).toBe(false);
    expect(result.startsBeforeFirstIdleReleased).toBe(1);
    expect(result.startsAfterSecondStart).toBe(2);
    expect(result.idleCount).toBe(2);
  });
});
