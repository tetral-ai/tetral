import { expect, test } from "bun:test";
import { Server, ServerCredentials, status } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceService,
  RuntimeProcessPhase,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { AgentRuntimeBridgeServiceServer } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";

type Case = {
  readonly phase: "startup" | "running";
  readonly sink: "normal" | "silent" | "throw";
  readonly holdInitialReport?: boolean;
};

async function runCase({ phase, sink, holdInitialReport }: Case): Promise<void> {
  const registrations: string[] = [];
  const reported: string[] = [];
  let accepting = 0;
  let staleAt = 0;
  let heldReportCancelled = false;
  let joinHeldReport!: () => void;
  const heldReportJoined = new Promise<void>((resolve) => {
    joinHeldReport = resolve;
  });
  const server = new Server();
  const implementation: Pick<
    AgentRuntimeBridgeServiceServer,
    "registerRuntimeProcess" | "reportRuntimeProcess"
  > = {
    registerRuntimeProcess(call, callback) {
      registrations.push(call.request.runtimeProcessId);
      callback(null, {
        runtimeProcessId: call.request.runtimeProcessId,
        registrationOrder: 1,
        registrationReceipt: "controlled-receipt",
      });
    },
    reportRuntimeProcess(call, callback) {
      reported.push(call.request.runtimeProcessId);
      if (call.request.registrationReceipt !== "controlled-receipt")
        throw new Error("wrong registration proof");
      const isAccepting =
        call.request.phase ===
        RuntimeProcessPhase.RUNTIME_PROCESS_PHASE_ACCEPTING;
      if (isAccepting) accepting++;
      if (holdInitialReport === true && isAccepting) {
        // Keep the first healthy report pending until its real transport is
        // cancelled. No timer chooses this competing startup-failure outcome.
        call.once("cancelled", () => {
          heldReportCancelled = true;
          // The transport owns its deadline outcome. This observer only joins;
          // it must not race the client's deadline with a fabricated remote code.
          joinHeldReport();
        });
      } else if (
        holdInitialReport !== true && (phase === "startup" || accepting > 1)
      ) {
        if (staleAt === 0) staleAt = Date.now();
        callback({
          code: status.FAILED_PRECONDITION,
          message: "controlled retired process",
        });
      } else {
        callback(null, {
          runtimeProcessId: call.request.runtimeProcessId,
          phase: call.request.phase,
          current: true,
        });
      }
    },
  };
  server.addService(AgentRuntimeBridgeServiceService, implementation);
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync(
      "127.0.0.1:0",
      ServerCredentials.createInsecure(),
      (error, port) => (error === null ? resolve(port) : reject(error)),
    ),
  );
  const child = Bun.spawn({
    cmd: [
      process.execPath,
      new URL("../fixtures/stale-process.ts", import.meta.url).pathname,
      `127.0.0.1:${port}`,
      sink,
    ],
    // Deliberately inherited overrides prove the fixture chooses its own normal
    // parsed policy, never the caller's.
    env: {
      ...process.env,
      TETRAL_RUNTIME_REPORT_TIMEOUT_MS: "1",
      TETRAL_RUNTIME_REPORT_INTERVAL_MS: "2",
      TETRAL_RUNTIME_PROCESS_FRESHNESS_MS: "3",
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdout = new Response(child.stdout).text();
  const stderr = new Response(child.stderr).text();
  let watchdog: ReturnType<typeof setTimeout> | undefined;
  try {
    const expired = new Promise<never>((_resolve, reject) => {
      watchdog = setTimeout(() => {
        child.kill("SIGKILL");
        reject(new Error(`15s child watchdog: phase=${phase} sink=${sink}`));
      }, 15_000);
    });
    const code = await Promise.race([child.exited, expired]);
    const elapsed = Date.now() - staleAt;
    if (holdInitialReport === true)
      await Promise.race([heldReportJoined, expired]);
    const [out, err] = await Promise.all([stdout, stderr]);
    const records = out
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line) as {
        event: string;
        phase?: string;
        code?: number;
        report_timeout_ms?: number;
        report_interval_ms?: number;
        freshness_ms?: number;
      });
    const names = records.map((record) => record.event);
    const context = JSON.stringify({
      phase,
      sink,
      report_policy: holdInitialReport ? "held-initial-report" : "normal",
      exit_code: code,
      registrations: registrations.length,
      reports: reported.length,
      accepting,
      stale_observed: staleAt > 0,
      events: names.slice(0, 20),
      statuses: records
        .filter((record) => record.event === "report.returned")
        .map(({ phase, code }) => ({ phase, code })),
    });
    expect(records.length, context).toBeLessThanOrEqual(20);
    expect(code, context).toBe(1);
    expect(registrations, context).toHaveLength(1);
    expect(new Set(reported), context).toEqual(new Set(registrations));
    expect(names.indexOf("core.joined"), context).toBeGreaterThanOrEqual(0);
    expect(names.indexOf("dependency.close"), context).toBeGreaterThan(
      names.indexOf("core.joined"),
    );
    expect(names.indexOf("core.close"), context).toBeGreaterThan(
      names.indexOf("dependency.close"),
    );
    expect(names.includes("process.ready"), context).toBe(
      holdInitialReport !== true && phase === "running",
    );
    if (sink === "normal") expect(err, context).toContain("workload.command_failed");
    else expect(err, context).toBe("");
    expect(err, context).not.toContain("controlled retired process");
    expect(err, context).not.toContain("synthetic diagnostic sink failure");
    const returned = records.filter(
      (record) => record.event === "report.returned" && record.phase === "accepting",
    );
    expect(returned, context).toHaveLength(1);
    expect(records[0], context).toMatchObject({
      event: "process.policy",
      report_timeout_ms: 1000,
      report_interval_ms: 2000,
      freshness_ms: 10000,
    });
    if (holdInitialReport === true) {
      expect(staleAt, context).toBe(0);
      expect(accepting, context).toBe(1);
      expect(heldReportCancelled, context).toBe(true);
      expect(returned[0]?.code, context).toBe(status.DEADLINE_EXCEEDED);
      expect(names.includes("report.ack.accepting"), context).toBe(false);
    } else {
      expect(staleAt, context).toBeGreaterThan(0);
      expect(elapsed, context).toBeLessThan(5500);
      expect(accepting, context).toBe(phase === "startup" ? 1 : 2);
      expect(returned[0]?.code, context).toBe(status.FAILED_PRECONDITION);
      const attempts = names.filter(
        (event) => event === "report.attempt.accepting",
      ).length;
      const healthy = names.filter(
        (event) => event === "report.ack.accepting",
      ).length;
      expect(attempts, context).toBe(phase === "startup" ? 1 : 2);
      expect(healthy, context).toBe(phase === "startup" ? 0 : 1);
      if (phase === "running")
        expect(names.indexOf("report.ack.accepting"), context).toBeLessThan(
          names.indexOf("process.ready"),
        );
    }
  } finally {
    if (watchdog !== undefined) clearTimeout(watchdog);
    if (child.exitCode === null) child.kill("SIGKILL");
    await child.exited;
    await new Promise<void>((resolve) => server.tryShutdown(() => resolve()));
  }
}

test("startup and running stale process rejection exits the actual executable without another registration under every sink", async () => {
  const outcomes = await Promise.allSettled(
    (["startup", "running"] as const).flatMap((phase) =>
      (["normal", "silent", "throw"] as const).map((sink) => runCase({ phase, sink })),
    ),
  );
  // A failure is reported only after all six process/server owners have joined.
  const failures = outcomes.flatMap((outcome) =>
    outcome.status === "rejected" ? [outcome.reason] : [],
  );
  if (failures.length > 0)
    throw new AggregateError(
      failures,
      "stale process cases failed after all owners joined",
    );
}, 20_000);

test("an ordinary initial report deadline exits without satisfying the actual stale-process oracle", async () => {
  await runCase({ phase: "running", sink: "silent", holdInitialReport: true });
}, 20_000);
