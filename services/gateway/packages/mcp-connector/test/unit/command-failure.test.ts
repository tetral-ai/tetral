import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { Writable } from "node:stream";
import { createDiagnosticStreamSink } from "@tetral/ts-observability";
import { runMcpConnectorCommand } from "../../src/command.js";
import { createJsonLogger } from "../../src/logger.js";
import { commandEnv, commandFixture, failureSentinel } from "../fixtures/command-process.js";

describe("McpConnector command failure ownership", () => {
  test("listener, wait and cleanup failures preserve errors and attempt each later close once", async () => {
    for (const mode of ["reviewer", "listener", "http_bind", "wait", "wait_both_cleanup", "http", "grpc", "mcp_clients", "database", "both_cleanup"]) {
      const events: string[] = [], lines: string[] = [];
      const fixture = commandFixture(mode, (event) => events.push(event));
      let releases = 0;
      const base = createJsonLogger({ write: (line) => { lines.push(line); } });
      const logger = { ...base, flush: () => { releases++; base.flush(); } };
      await withEnv(async () => {
        try { await runMcpConnectorCommand({ ...fixture.options, logger, registerSignalHandlers: () => undefined }); throw new Error("expected failure"); }
        catch (error) {
          expect(error).toBe(["grpc", "mcp_clients", "database"].includes(mode) ? fixture.laterFailure : fixture.failure);
        }
      });
      const expectedCloses = mode === "reviewer" || mode === "listener" || mode === "http_bind"
        ? ["grpc.close", "mcp_clients.close", "database.close"]
        : ["http.close", "grpc.close", "mcp_clients.close", "database.close"];
      expect(events.filter((event) => event.endsWith(".close"))).toEqual(expectedCloses);
      if (expectedCloses.includes("http.close")) expect(events).toContain("http.ready:false");
      expect(releases).toBe(1);
      const records = lines.map((line) => JSON.parse(line));
      const runPhase = mode === "reviewer" ? "dependency" : mode === "http_bind" ? "listener" : mode.startsWith("wait") ? "wait" : mode;
      const businessFailure = ["reviewer", "listener", "http_bind"].includes(mode) || mode.startsWith("wait");
      expect(records).toContainEqual(expect.objectContaining({
        event: businessFailure ? "workload.command_failed" : "workload.cleanup_failed",
        phase: mode === "both_cleanup" ? "http" : runPhase,
        "error.class": businessFailure ? "process_error" : "cleanup_error",
      }));
      expect(lines.join("")).not.toContain(failureSentinel);
      expect(lines.join("")).not.toContain("PRIVATE_LATER_CLOSE_NON_TOKEN_SENTINEL");
    }
  });

  test("repeated shutdown reuses its failed result and still releases later resources", async () => {
    const events: string[] = [];
    let reentered: Promise<void> | undefined;
    const fixture = commandFixture("http", (event) => {
      events.push(event);
      if (event === "http.close") reentered = shutdown!();
    });
    let shutdown: (() => Promise<void>) | undefined;
    await withEnv(async () => {
      await expect(runMcpConnectorCommand({
        ...fixture.options, logger: { info: () => undefined, error: () => { throw new Error(failureSentinel); } },
        registerSignalHandlers: (close) => { shutdown = close; },
        waitForever: async () => {
          const first = shutdown!();
          expect(events).toContain("http.close");
          expect(reentered).toBe(first);
          const second = shutdown!();
          expect(second).toBe(first);
          await first;
          return undefined as never;
        },
      })).rejects.toBe(fixture.failure);
    });
    expect(events.filter((event) => event.endsWith(".close"))).toEqual(["http.close", "grpc.close", "mcp_clients.close", "database.close"]);
  });

  test("disabled, throwing and real backpressured diagnostics do not extend prompt business cleanup", async () => {
    for (const mode of ["disabled", "throw", "backpressure"]) {
      const events: string[] = [];
      let complete: (() => void) | undefined;
      const stream = new Writable({ highWaterMark: 1, write(_chunk, _encoding, done) { complete = done; } });
      const sink = createDiagnosticStreamSink(stream);
      const logger = mode === "disabled" ? { info: () => undefined, error: () => undefined }
        : mode === "throw" ? { info: () => { throw new Error(failureSentinel); }, error: () => { throw new Error(failureSentinel); }, flush: () => { throw new Error(failureSentinel); } }
          : createJsonLogger({ write: sink.write });
      if (mode === "backpressure") logger.error({ event: "command_backpressure_probe" });
      const fixture = commandFixture("wait", (event) => events.push(event));
      const before = performance.now();
      try {
        await withEnv(async () => { await expect(runMcpConnectorCommand({ ...fixture.options, logger, registerSignalHandlers: () => undefined })).rejects.toBe(fixture.failure); });
        expect(performance.now() - before).toBeLessThan(1_000);
        expect(events.filter((event) => event.endsWith(".close"))).toEqual(["http.close", "grpc.close", "mcp_clients.close", "database.close"]);
        if (mode === "backpressure") { expect(sink.stats().blocked).toBe(true); expect(sink.stats().dropped).toBeGreaterThan(0); }
      } finally { complete?.(); sink.close(); stream.destroy(); }
    }
  });

  test("the production executable config-failure boundary exits with safe JSON instead of a native stack", () => {
    const path = new URL("../../src/command.ts", import.meta.url).pathname;
    const result = spawnSync(process.execPath, [path], {
      encoding: "utf8", timeout: 10_000,
      env: { ...process.env, ...commandEnv(), TETRAL_LOG_LEVEL: failureSentinel },
    });
    expect(result.error).toBeUndefined();
    expect(result.signal).toBeNull();
    expect(result.status).toBe(1);
    expect(result.stderr).not.toContain(failureSentinel);
    expect(result.stderr).not.toContain("Bun v");
    const records = result.stderr.trim().split("\n").map((line) => JSON.parse(line));
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({ level: "error", kind: "config_error" });
  });

  test("actual Bun entry and both signal exits contain raw failures and report nonzero status", () => {
    const path = new URL("../fixtures/command-process.ts", import.meta.url).pathname;
    for (const mode of ["dependency", "listener", "wait", "both_cleanup", "SIGTERM_failure", "SIGINT_failure", "SIGTERM", "SIGINT"]) {
      const result = spawnSync(process.execPath, [path, mode], { encoding: "utf8", timeout: 10_000 });
      expect(result.error).toBeUndefined();
      expect(result.signal).toBeNull();
      expect(result.status).toBe(mode === "SIGTERM" || mode === "SIGINT" ? 0 : 1);
      expect(result.stderr).not.toContain(failureSentinel);
      expect(result.stderr).not.toContain("PRIVATE_LATER_CLOSE_NON_TOKEN_SENTINEL");
      expect(result.stderr).not.toContain("Bun v");
      for (const line of result.stderr.trim().split("\n").filter(Boolean)) expect(() => JSON.parse(line)).not.toThrow();
      const events = result.stdout.trim().split("\n");
      if (mode !== "dependency") {
        expect(events.filter((event) => event === "grpc.close")).toHaveLength(1);
        expect(events.filter((event) => event === "database.close")).toHaveLength(1);
      }
      if (mode.endsWith("_failure")) expect(JSON.parse(result.stderr.trim().split("\n").at(-1)!)).toMatchObject({ event: "workload.cleanup_failed", phase: "http", "error.class": "cleanup_error" });
    }
  }, 30_000);
});

async function withEnv(run: () => Promise<void>): Promise<void> {
  const saved = { ...process.env };
  Object.assign(process.env, commandEnv());
  try { await run(); } finally {
    for (const key of Object.keys(process.env)) delete process.env[key];
    Object.assign(process.env, saved);
  }
}
