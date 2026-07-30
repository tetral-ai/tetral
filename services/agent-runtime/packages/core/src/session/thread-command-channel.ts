import { Deferred, Effect, Exit, Fiber, Queue, Scope } from "effect";

/** Raised when work is submitted after the owning ThreadEntry has closed. */
export class ThreadCommandChannelClosed extends Error {
  constructor() {
    super("thread command channel is closed");
    this.name = "ThreadCommandChannelClosed";
  }
}

/** One scoped serialized execution channel owned by a resident ThreadEntry. */
export interface ThreadCommandChannel {
  readonly ownerFiber: Fiber.Fiber<never, never>;
  readonly enqueue: <A, E>(
    command: Effect.Effect<A, E>,
  ) => Effect.Effect<Effect.Effect<A, E | ThreadCommandChannelClosed>, ThreadCommandChannelClosed>;
  readonly submit: <A, E>(command: Effect.Effect<A, E>) => Effect.Effect<A, E | ThreadCommandChannelClosed>;
  readonly close: () => Effect.Effect<void>;
  readonly closed: () => boolean;
  readonly busy: () => boolean;
}

interface QueuedThreadCommand {
  state: "queued" | "running" | "cancelled" | "settled";
  readonly run: Effect.Effect<void>;
}

/**
 * Creates an unbounded semantic command channel. The queue only carries executable
 * Effects; durable state remains in the database and hot state remains on ThreadEntry.
 */
export function makeThreadCommandChannel(): Effect.Effect<ThreadCommandChannel> {
  return Effect.gen(function* () {
    const queue = yield* Queue.unbounded<QueuedThreadCommand>();
    const commandScope = yield* Scope.make();
    const pending = new Set<() => Effect.Effect<boolean>>();
    let isClosed = false;
    let activeCommands = 0;
    const worker = Effect.forever(
      Queue.take(queue).pipe(
        Effect.flatMap((command) =>
          Effect.suspend(() => {
            if (command.state !== "queued") {
              command.state = "settled";
              activeCommands -= 1;
              return Effect.void;
            }
            command.state = "running";
            return command.run.pipe(
              Effect.ensuring(Effect.sync(() => {
                command.state = "settled";
                activeCommands -= 1;
              })),
            );
          })
        ),
      ),
    );
    const ownerFiber = yield* worker.pipe(Effect.forkIn(commandScope));

    const enqueue = <A, E>(
      command: Effect.Effect<A, E>,
    ): Effect.Effect<Effect.Effect<A, E | ThreadCommandChannelClosed>, ThreadCommandChannelClosed> =>
      Effect.gen(function* () {
        if (isClosed) {
          return yield* Effect.fail(new ThreadCommandChannelClosed());
        }
        const result = yield* Deferred.make<A, E | ThreadCommandChannelClosed>();
        const closeSubmission = () => Deferred.fail(result, new ThreadCommandChannelClosed());
        pending.add(closeSubmission);
        const queued: QueuedThreadCommand = {
          state: "queued",
          run: command.pipe(
            Effect.exit,
            Effect.flatMap((exit) => Deferred.done(result, exit)),
            Effect.ensuring(Effect.sync(() => pending.delete(closeSubmission))),
            Effect.asVoid,
          ),
        };
        activeCommands += 1;
        const offered = yield* Queue.offer(queue, queued);
        if (!offered) {
          activeCommands -= 1;
          yield* closeSubmission();
        }
        return Deferred.await(result).pipe(
          Effect.onInterrupt(() => Effect.sync(() => {
            if (queued.state === "queued") {
              queued.state = "cancelled";
            }
          })),
          Effect.ensuring(Effect.sync(() => pending.delete(closeSubmission))),
        );
      });

    const submit = <A, E>(
      command: Effect.Effect<A, E>,
    ): Effect.Effect<A, E | ThreadCommandChannelClosed> =>
      enqueue(command).pipe(Effect.flatMap((awaitResult) => awaitResult));

    const close = (): Effect.Effect<void> =>
      Effect.suspend(() => {
        if (isClosed) {
          return Effect.void;
        }
        isClosed = true;
        return Queue.shutdown(queue).pipe(
          Effect.andThen(Effect.forEach([...pending], (settle) => settle(), { discard: true })),
          Effect.andThen(Fiber.interrupt(ownerFiber)),
          Effect.andThen(Scope.close(commandScope, Exit.void)),
          Effect.exit,
          Effect.asVoid,
        );
      });

    return {
      ownerFiber,
      enqueue,
      submit,
      close,
      closed: () => isClosed,
      busy: () => activeCommands > 0,
    };
  });
}
