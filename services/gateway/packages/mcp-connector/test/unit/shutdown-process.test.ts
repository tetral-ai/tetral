import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("MCP executable exits incomplete shutdown within one application budget without early dependency close", async () => {
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
      expect(result.exitedAt - began!.at).toBeLessThan(5500);
      console.info(
        "shutdown_exit " +
          JSON.stringify({
            receiver: "MCP",
            sink,
            trigger,
            exit_code: result.code,
            elapsed_ms: result.exitedAt - began!.at,
            worker_joined: false,
            dependencies_closed: false,
          }),
      );
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

test("default MCP executable client retains raw credential SQL through its configured exit deadline under every sink", async () => {
  await Promise.all(
    [
      { sink: "normal", trigger: "SIGTERM" },
      { sink: "silent", trigger: "SIGINT" },
      { sink: "throw", trigger: "finally" },
    ].map(async ({ sink, trigger }) => {
      const result = await child(sink, trigger, "credential-held");
      expect(result.code, result.diagnostic).toBe(1);
      const names = result.events.map((event) => event.event);
      expect(names).toContain("credential.held");
      expect(names).toContain("worker.returned");
      expect(names).not.toContain("credential.joined");
      expect(names).not.toContain("database.close");
      const began = result.events.find(
        (event) => event.event === "shutdown.begin",
      );
      expect(began).toBeDefined();
      const elapsed = result.exitedAt - began!.at;
      expect(elapsed).toBeGreaterThanOrEqual(1150);
      expect(elapsed).toBeLessThan(1700);
      if (sink === "normal")
        expect(result.stderr).toContain(
          '"event":"workload.shutdown_deadline_exceeded"',
        );
      else expect(result.stderr).toBe("");
      expect(result.stderr).not.toContain("synthetic diagnostic sink failure");
      console.info(
        "credential_shutdown_exit " +
          JSON.stringify({
            receiver: "MCP",
            sink,
            trigger,
            exit_code: result.code,
            elapsed_ms: elapsed,
            credential_joined: false,
            database_closed: false,
          }),
      );
    }),
  );
  const cooperative = await child(
    "normal",
    "SIGTERM",
    "credential-cooperative",
  );
  expect(cooperative.code, cooperative.diagnostic).toBe(0);
  const names = cooperative.events.map((event) => event.event);
  expect(names.filter((event) => event === "database.close")).toHaveLength(1);
  expect(names.indexOf("credential.joined")).toBeGreaterThanOrEqual(0);
  expect(names.indexOf("database.close")).toBeGreaterThan(
    names.indexOf("credential.joined"),
  );
  expect(cooperative.stderr).not.toContain(
    "workload.shutdown_deadline_exceeded",
  );
}, 10_000);

async function child(sink: string, trigger: string, mode: string) {
  const watchdogMs = mode.startsWith("credential-") ? 5000 : 15_000;
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
          reject(new Error(`${watchdogMs}ms child shutdown watchdog exceeded`));
        }, watchdogMs);
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
