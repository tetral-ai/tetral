import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("Provider executable exits incomplete shutdown within one application budget without early dependency close", async () => {
  const cases = [
    { sink: "normal", trigger: "SIGTERM" },
    { sink: "silent", trigger: "SIGINT" },
    { sink: "throw", trigger: "finally" },
  ];
  await Promise.all(
    cases.map(async ({ sink, trigger }) => {
      const result = await child(sink, trigger, "held");
      expect(result.code, result.diagnostic).toBe(1);
      expect(result.events.some((event) => event.event.endsWith(".held"))).toBe(
        true,
      );
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
      expect(result.exitedAt - began!.at).toBeGreaterThanOrEqual(4950);
      // Bounded exit, not a second budget: the tolerance covers three concurrent children.
      expect(result.exitedAt - began!.at).toBeLessThan(5000 + 2000);
      if (sink === "normal")
        expect(result.stderr).toContain(
          '"event":"workload.shutdown_deadline_exceeded"',
        );
      else expect(result.stderr).toBe("");
      expect(result.stderr).not.toContain("synthetic diagnostic sink failure");
    }),
  );
  const cooperative = await child("normal", "SIGTERM", "cooperative");
  expect(cooperative.code, cooperative.diagnostic).toBe(0);
  const names = cooperative.events.map((event) => event.event);
  const joined = names
    .map((event, index) => (event.endsWith(".joined") ? index : -1))
    .filter((index) => index >= 0);
  const closed = names.findIndex((event) => event.endsWith(".close"));
  expect(joined).toHaveLength(1);
  for (const index of joined) expect(closed).toBeGreaterThan(index);
  expect(cooperative.stderr).not.toContain(
    "workload.shutdown_deadline_exceeded",
  );
}, 20_000);

async function child(sink: string, trigger: string, mode: string) {
  const directory = await mkdtemp(join(tmpdir(), "shutdown-child-"));
  const process = Bun.spawn({
    cmd: [
      globalThis.process.execPath,
      new URL("../fixtures/shutdown-process.ts", import.meta.url).pathname,
      sink,
      trigger,
      mode,
      directory,
    ],
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdout = new Response(process.stdout).text();
  const stderr = new Response(process.stderr).text();
  let watchdog: ReturnType<typeof setTimeout> | undefined;
  try {
    const code = await Promise.race([
      process.exited,
      new Promise<never>((_resolve, reject) => {
        watchdog = setTimeout(() => {
          process.kill("SIGKILL");
          reject(new Error("15s child shutdown watchdog exceeded"));
        }, 15_000);
      }),
    ]);
    const exitedAt = Date.now();
    const [out, err] = await Promise.all([stdout, stderr]);
    const events = out
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line) as { event: string; at: number });
    return {
      code,
      exitedAt,
      events,
      stderr: err,
      diagnostic: `stdout=${out} stderr=${err}`,
    };
  } finally {
    if (watchdog !== undefined) clearTimeout(watchdog);
    if (process.exitCode === null) process.kill("SIGKILL");
    await process.exited;
    await rm(directory, { recursive: true, force: true });
  }
}
