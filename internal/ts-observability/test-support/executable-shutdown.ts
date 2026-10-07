import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

/**
 * @packageDocumentation
 *
 * Test-only driver for the executable shutdown deadline armed through
 * `runProcessEntry`. Each service owns its fixture, which runs that service's
 * real command and app wiring; this module only spawns the fixture and checks
 * the shared process-boundary contract. The package entry does not export it.
 *
 * A fixture is a Bun script invoked as `<sink> <trigger> <mode> <directory>`:
 * - `sink`: `normal` writes diagnostics to stderr, `silent` drops them and
 *   `throw` makes every diagnostic call throw `synthetic diagnostic sink failure`;
 * - `trigger`: `SIGTERM` or `SIGINT` signals the process, while `finally`
 *   returns from the command's wait so command-finally cleanup starts shutdown;
 * - `mode`: `held` keeps owned work blocked past the deadline and `cooperative`
 *   lets it join after shutdown begins; a service may define further modes;
 * - `directory`: a private temporary directory removed after the run.
 *
 * It writes one `{"event":string,"at":epochMs}` JSON line to stdout per
 * lifecycle event: `shutdown.begin` when shutdown starts, `<owner>.held` and
 * `<owner>.joined` around blocked work, `<dependency>.close` when an owned
 * dependency closes and `handoff.*` if it attempts a durable handoff.
 */

/** One lifecycle event written by a shutdown fixture. */
export interface ShutdownFixtureEvent {
  readonly event: string;
  readonly at: number;
}

/** Observations from one fixture process. */
export interface ShutdownFixtureRun {
  readonly code: number;
  /** Parent clock reading taken once the child's exit was observed. */
  readonly exitedAt: number;
  readonly events: readonly ShutdownFixtureEvent[];
  readonly stderr: string;
  /** Complete fixture output for assertion messages. */
  readonly diagnostic: string;
}

/** Runs one fixture process; past `watchdogMs` it is killed and the run fails. */
export async function runShutdownFixture(
  fixtureUrl: URL,
  run: {
    readonly sink: string;
    readonly trigger: string;
    readonly mode: string;
    readonly watchdogMs: number;
  },
): Promise<ShutdownFixtureRun> {
  const directory = await mkdtemp(join(tmpdir(), "shutdown-child-"));
  const child = Bun.spawn({
    cmd: [
      process.execPath,
      fixtureUrl.pathname,
      run.sink,
      run.trigger,
      run.mode,
      directory,
    ],
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdout = new Response(child.stdout).text();
  const stderr = new Response(child.stderr).text();
  let watchdog: ReturnType<typeof setTimeout> | undefined;
  try {
    const code = await Promise.race([
      child.exited,
      new Promise<never>((_resolve, reject) => {
        watchdog = setTimeout(() => {
          child.kill("SIGKILL");
          reject(
            new Error(`${run.watchdogMs}ms child shutdown watchdog exceeded`),
          );
        }, run.watchdogMs);
      }),
    ]);
    const exitedAt = Date.now();
    const [out, err] = await Promise.all([stdout, stderr]);
    const events = out
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line) as ShutdownFixtureEvent);
    return {
      code,
      exitedAt,
      events,
      stderr: err,
      diagnostic: `stdout=${out} stderr=${err}`,
    };
  } finally {
    if (watchdog !== undefined) clearTimeout(watchdog);
    if (child.exitCode === null) child.kill("SIGKILL");
    await child.exited;
    await rm(directory, { recursive: true, force: true });
  }
}

/** The executable whose shutdown fixture is checked. */
export interface ExecutableShutdownCase {
  /** Service label that starts the test name, such as `Runtime`. */
  readonly receiver: string;
  /** The service's shutdown fixture script. */
  readonly fixtureUrl: URL;
  /** `.joined` events a cooperative run records, all before its first dependency close. */
  readonly expectedJoins: number;
  /** Application shutdown budget, in milliseconds, that the fixture configures. */
  readonly budgetMs: number;
}

const watchdogMs = 15_000;

/**
 * Registers the executable shutdown test. Three concurrent held runs, one per
 * sink and trigger, must exit nonzero at the application deadline without a
 * join, dependency close or handoff; only the normal sink reports the deadline
 * diagnostic. A following cooperative run must exit zero after its expected
 * joins and before dependency close. The budget must leave the 15-second
 * per-run watchdog room for the exit tolerance.
 */
export function testExecutableShutdown({
  receiver,
  fixtureUrl,
  expectedJoins,
  budgetMs,
}: ExecutableShutdownCase): void {
  test(`${receiver} executable exits incomplete shutdown within one application budget without early dependency close`, async () => {
    const cases = [
      { sink: "normal", trigger: "SIGTERM" },
      { sink: "silent", trigger: "SIGINT" },
      { sink: "throw", trigger: "finally" },
    ];
    await Promise.all(
      cases.map(async ({ sink, trigger }) => {
        const result = await runShutdownFixture(fixtureUrl, {
          sink,
          trigger,
          mode: "held",
          watchdogMs,
        });
        expect(result.code, result.diagnostic).toBe(1);
        expect(
          result.events.some((event) => event.event.endsWith(".held")),
        ).toBe(true);
        expect(
          result.events.some((event) => event.event.endsWith(".joined")),
        ).toBe(false);
        expect(
          result.events.some((event) => event.event.endsWith(".close")),
        ).toBe(false);
        expect(
          result.events.some((event) => event.event.startsWith("handoff")),
        ).toBe(false);
        const began = result.events.find(
          (event) => event.event === "shutdown.begin",
        );
        expect(began).toBeDefined();
        expect(result.exitedAt - began!.at).toBeGreaterThanOrEqual(
          budgetMs - 50,
        );
        // Bounded exit, not a second budget: the tolerance covers three concurrent children.
        expect(result.exitedAt - began!.at).toBeLessThan(budgetMs + 2000);
        if (sink === "normal")
          expect(result.stderr).toContain(
            '"event":"workload.shutdown_deadline_exceeded"',
          );
        else expect(result.stderr).toBe("");
        expect(result.stderr).not.toContain(
          "synthetic diagnostic sink failure",
        );
      }),
    );
    const cooperative = await runShutdownFixture(fixtureUrl, {
      sink: "normal",
      trigger: "SIGTERM",
      mode: "cooperative",
      watchdogMs,
    });
    expect(cooperative.code, cooperative.diagnostic).toBe(0);
    const names = cooperative.events.map((event) => event.event);
    const joined = names
      .map((event, index) => (event.endsWith(".joined") ? index : -1))
      .filter((index) => index >= 0);
    const closed = names.findIndex((event) => event.endsWith(".close"));
    expect(joined).toHaveLength(expectedJoins);
    for (const index of joined) expect(closed).toBeGreaterThan(index);
    expect(cooperative.stderr).not.toContain(
      "workload.shutdown_deadline_exceeded",
    );
  }, 20_000);
}
