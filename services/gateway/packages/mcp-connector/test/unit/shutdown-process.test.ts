import { expect, test } from "bun:test";
import {
  runShutdownFixture,
  testExecutableShutdown,
} from "@tetral/ts-observability/test-support/executable-shutdown";

const fixtureUrl = new URL("../fixtures/shutdown-process.ts", import.meta.url);

testExecutableShutdown({
  receiver: "MCP",
  fixtureUrl,
  expectedJoins: 1,
  // The fixture's 2000 ms drain plus 3000 ms cancellation join.
  budgetMs: 5000,
});

test("default MCP executable client retains raw credential SQL through its configured exit deadline under every sink", async () => {
  await Promise.all(
    [
      { sink: "normal", trigger: "SIGTERM" },
      { sink: "silent", trigger: "SIGINT" },
      { sink: "throw", trigger: "finally" },
    ].map(async ({ sink, trigger }) => {
      const result = await runShutdownFixture(fixtureUrl, {
        sink,
        trigger,
        mode: "credential-held",
        watchdogMs: 5000,
      });
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
      // The 200 ms drain plus 1000 ms join budget, with a bounded-exit tolerance for three children.
      expect(elapsed).toBeLessThan(1200 + 1500);
      if (sink === "normal")
        expect(result.stderr).toContain(
          '"event":"workload.shutdown_deadline_exceeded"',
        );
      else expect(result.stderr).toBe("");
      expect(result.stderr).not.toContain("synthetic diagnostic sink failure");
    }),
  );
  const cooperative = await runShutdownFixture(fixtureUrl, {
    sink: "normal",
    trigger: "SIGTERM",
    mode: "credential-cooperative",
    watchdogMs: 5000,
  });
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
