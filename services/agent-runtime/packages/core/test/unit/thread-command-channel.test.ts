import { describe, expect, test } from "bun:test";
import { Effect, Fiber } from "effect";
import { makeThreadCommandChannel } from "../../src/session/thread-command-channel.js";

describe("ThreadCommandChannel", () => {
  test("runs semantic commands in enqueue order even when earlier work suspends", async () => {
    const observed: string[] = [];
    const channel = await Effect.runPromise(makeThreadCommandChannel());
    let releaseFirst = (): void => undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });

    const first = Effect.runPromise(channel.submit(
      Effect.promise(async () => {
        observed.push("first:start");
        await firstGate;
        observed.push("first:end");
        return "first";
      }),
    ));
    const second = Effect.runPromise(channel.submit(
      Effect.sync(() => {
        observed.push("second");
        return "second";
      }),
    ));

    await Bun.sleep(0);
    expect(observed).toEqual(["first:start"]);
    releaseFirst();
    expect(await Promise.all([first, second])).toEqual(["first", "second"]);
    expect(observed).toEqual(["first:start", "first:end", "second"]);

    await Effect.runPromise(channel.close());
  });

  test("owns and interrupts its worker Fiber when the thread closes", async () => {
    const channel = await Effect.runPromise(makeThreadCommandChannel());
    expect(channel.closed()).toBe(false);

    await Effect.runPromise(channel.close());

    expect(channel.closed()).toBe(true);
    const exit = await Effect.runPromiseExit(channel.submit(Effect.succeed("late")));
    expect(exit._tag).toBe("Failure");
  });

  test("settles active and queued submissions when the thread closes", async () => {
    const channel = await Effect.runPromise(makeThreadCommandChannel());
    const active = Effect.runPromiseExit(channel.submit(Effect.never));
    const queued = Effect.runPromiseExit(channel.submit(Effect.succeed("queued")));

    await Bun.sleep(0);
    await Effect.runPromise(channel.close());

    const [activeExit, queuedExit] = await Promise.all([active, queued]);
    expect(activeExit._tag).toBe("Failure");
    expect(queuedExit._tag).toBe("Failure");
    expect(String(activeExit)).toContain("ThreadCommandChannelClosed");
    expect(String(queuedExit)).toContain("ThreadCommandChannelClosed");
  });

  test("does not execute a queued command after its submitter is interrupted", async () => {
    const observed: string[] = [];
    const channel = await Effect.runPromise(makeThreadCommandChannel());
    let releaseFirst = (): void => undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });

    const first = Effect.runFork(channel.submit(
      Effect.promise(async () => {
        observed.push("first:start");
        await firstGate;
        observed.push("first:end");
      }),
    ));
    const cancelled = Effect.runFork(channel.submit(
      Effect.sync(() => {
        observed.push("cancelled");
      }),
    ));

    await Bun.sleep(0);
    await Effect.runPromise(Fiber.interrupt(cancelled));
    releaseFirst();
    await Effect.runPromise(Fiber.join(first));
    await Bun.sleep(0);

    expect(observed).toEqual(["first:start", "first:end"]);
    await Effect.runPromise(channel.close());
  });
});
